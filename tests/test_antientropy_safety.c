/* test_antientropy_safety.c — anti-entropy never wipes durable local rows.
 *
 * Exercises the REAL production decision function tsdb_antientropy_decide()
 * (src/storage/db_cluster.c), which gates the destructive truncate+re-pull in
 * tsdb_cluster_resync_table.  The invariant under test:
 *
 *   anti-entropy must NEVER reduce a node's durable row count for a table
 *   based solely on a peer count comparison.
 *
 * The data-loss bug: peer_table_stats read the count by positional index from
 * a peer that delivered it in a different order, yielding a max_ts-scale value
 * (~1.3e12) as the "count" for a 3.62M-row table.  best_count > local_count
 * with equal max_ts then triggered "middle-gap" → tsdb_truncate_table + full
 * re-pull, deleting durable rows on every reconcile.  The guard must:
 *   - NEVER return FULL_PULL (which truncates) when local has rows;
 *   - reject a timestamp-scale peer count outright;
 *   - still allow the legit TAIL_PULL when the peer genuinely has newer rows;
 *   - still allow FULL_PULL only for an EMPTY local table.
 *
 * NOTE: a true multi-node truncate-on-the-wire regression needs a live cluster
 * (the parent verifies that on the real server).  This pins the exact branch
 * logic that decides whether the wipe happens — the tightest in-process test.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define ASSERT(c) do { if (!(c)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #c, __FILE__, __LINE__); \
    abort(); } } while (0)

static const char *name_of(tsdb_ae_action_t a) {
    switch (a) {
    case TSDB_AE_UP_TO_DATE:  return "UP_TO_DATE";
    case TSDB_AE_TAIL_PULL:   return "TAIL_PULL";
    case TSDB_AE_FULL_PULL:   return "FULL_PULL";
    case TSDB_AE_SKIP_UNSAFE: return "SKIP_UNSAFE";
    }
    return "?";
}

/* local (count, max_ts), peer (count, max_ts) -> action */
static tsdb_ae_action_t decide(uint64_t lc, int64_t lm, uint64_t pc, int64_t pm) {
    tsdb_ae_action_t a = tsdb_antientropy_decide(lc, lm, pc, pm);
    printf("  local=(%llu,%lld) peer=(%llu,%lld) -> %s\n",
           (unsigned long long)lc, (long long)lm,
           (unsigned long long)pc, (long long)pm, name_of(a));
    return a;
}

int main(void) {
    printf("=== test_antientropy_safety ===\n");

    const int64_t MAXTS = 1319487000000LL;   /* shared ns-epoch max_ts */

    /* THE BUG REPRO: 3.62M durable local rows, peer reports a max_ts-scale
     * "count" (count/max_ts swapped on the peer path) at the SAME max_ts.
     * Old code: best_count > local_count && equal max_ts -> truncate + wipe.
     * Guard: must NOT truncate. */
    printf("case 1: bogus timestamp-scale peer count must NOT truncate\n");
    ASSERT(decide(3620864, MAXTS, 1303104000000ULL, MAXTS) == TSDB_AE_SKIP_UNSAFE);

    /* Even a plausible-but-higher peer count at equal max_ts is not a proven
     * superset (the two nodes can hold different rows at the same max_ts) —
     * with local rows present we refuse the destructive path. */
    printf("case 2: higher peer count, equal max_ts, local has rows -> SKIP\n");
    ASSERT(decide(3620864, MAXTS, 3700000, MAXTS) == TSDB_AE_SKIP_UNSAFE);

    /* The classic data-loss shape: peer genuinely has FEWER rows but the count
     * arrived corrupted high — must never shrink local. (Same SKIP path; the
     * point is local rows are preserved regardless of the peer's number.) */
    printf("case 3: peer count corrupt-high while local has more real rows\n");
    ASSERT(decide(15000000, MAXTS, 1303104000000ULL, MAXTS) == TSDB_AE_SKIP_UNSAFE);

    /* Legit tail gap: peer has a strictly newer max_ts -> pull the tail.
     * This path is NON-destructive and must stay enabled. */
    printf("case 4: peer has newer max_ts -> TAIL_PULL (non-destructive)\n");
    ASSERT(decide(1000, 1000000000LL, 2000, 2000000000LL) == TSDB_AE_TAIL_PULL);
    /* Tail pull applies even with a corrupt-high count, since the newer max_ts
     * alone justifies a (bounded, additive) pull — it never truncates. */
    ASSERT(decide(3620864, MAXTS, 1303104000000ULL, MAXTS + 1) == TSDB_AE_TAIL_PULL);

    /* Empty local table (max_ts=0) + peer with real ns-epoch data: the peer's
     * max_ts > 0 = local_max_ts, so this is a non-destructive TAIL_PULL with
     * since_ts=0 (pulls every row).  No truncate needed — even safer than a
     * full re-pull, and the common fresh-node case. */
    printf("case 5: empty local + peer real data -> TAIL_PULL (since_ts=0, safe)\n");
    ASSERT(decide(0, 0, 5000, MAXTS) == TSDB_AE_TAIL_PULL);
    /* FULL_PULL (the only truncating action) is reachable ONLY when local is
     * empty AND max_ts ties AND peer count > 0 — i.e. both at max_ts==0, which
     * never happens with real data but is the documented safe full-pull case. */
    ASSERT(decide(0, 0, 5000, 0) == TSDB_AE_FULL_PULL);

    /* Already converged: equal counts + equal max_ts -> no action. */
    printf("case 6: equal state -> UP_TO_DATE\n");
    ASSERT(decide(5000, MAXTS, 5000, MAXTS) == TSDB_AE_UP_TO_DATE);
    /* Local strictly ahead in count at equal max_ts -> still no shrink. */
    ASSERT(decide(6000, MAXTS, 5000, MAXTS) == TSDB_AE_UP_TO_DATE);

    /* Boundary: exactly at the implausible threshold (1e11) -> SKIP. */
    printf("case 7: count at 1e11 threshold -> SKIP_UNSAFE\n");
    ASSERT(decide(100, MAXTS, 100000000000ULL, MAXTS) == TSDB_AE_SKIP_UNSAFE);
    /* Just below the threshold, with empty local, is a legit FULL_PULL. */
    ASSERT(decide(0, MAXTS, 99999999999ULL, MAXTS) == TSDB_AE_FULL_PULL);

    /* THE CORE INVARIANT, stated directly: for every (peer_count, peer_max_ts)
     * where peer_max_ts <= local_max_ts, a populated local table is NEVER told
     * to FULL_PULL (the only action that truncates).  Sweep a range. */
    printf("case 8: invariant sweep — populated local never gets FULL_PULL\n");
    for (uint64_t pc = 0; pc < 5000000000000ULL; pc += 137000000ULL) {
        tsdb_ae_action_t a = tsdb_antientropy_decide(3620864, MAXTS, pc, MAXTS);
        ASSERT(a != TSDB_AE_FULL_PULL);   /* would truncate durable rows */
        /* equal max_ts means TAIL_PULL is impossible here too. */
        ASSERT(a == TSDB_AE_UP_TO_DATE || a == TSDB_AE_SKIP_UNSAFE);
    }

    printf("PASS test_antientropy_safety\n");
    return 0;
}
