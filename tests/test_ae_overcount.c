/* test_ae_overcount.c — an over-counted replica must become VISIBLE without a
 * false truncate.
 *
 * THE BUG.  tsdb_antientropy_decide returns UP_TO_DATE whenever
 * best_count <= local_count, so a replica holding MORE rows than the authority
 * — a duplicated WRITE_BATCH counted twice — reads as up-to-date and is never
 * corrected.  Count alone cannot tell a DUPLICATE from a replica that is
 * LEGITIMATELY AHEAD (async, QUORUM=0: it took a write the authority has not
 * yet), and truncating the latter would DESTROY real rows — a worse bug.
 *
 * THE FIX under test (the smaller, correct change the task mandates): do NOT
 * auto-repair.  tsdb_ae_overcount_note tracks per-table PERSISTENCE of the
 * suspect condition.  A legitimately-ahead replica clears within a sweep or two
 * (replication catches up -> the streak resets on the first non-suspect sweep);
 * a real over-count persists and the streak climbs past AE_OVERCOUNT_PERSIST_N,
 * at which point the caller emits a metric/log.  This test pins the streak
 * discipline that distinguishes the two.
 */
#include "../src/storage/antientropy.h"

#include <stdio.h>

static int g_fail = 0;
#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

int main(void) {
    printf("=== test_ae_overcount ===\n");

    /* [1] a TRANSIENT ahead-state never reaches the persistence threshold: the
     *     first sweep in which the replica is no longer ahead resets the streak,
     *     so no metric/log ever fires for a legitimately-ahead replica. */
    printf("\n[1] a transient ahead-state resolves and is never flagged\n");
    CHECK(tsdb_ae_overcount_note("t_x", 1) == 1, "suspect sweep 1 -> streak 1");
    CHECK(tsdb_ae_overcount_note("t_x", 1) == 2, "suspect sweep 2 -> streak 2");
    CHECK(tsdb_ae_overcount_note("t_x", 0) == 0,
          "replication caught up -> streak RESETS to 0 (never crossed N=3)");
    CHECK(tsdb_ae_overcount_note("t_x", 1) == 1, "and a later suspect starts over");

    /* [2] a REAL over-count is permanent: the streak climbs monotonically and
     *     crosses the threshold, which is where the caller shouts. */
    printf("\n[2] a persistent over-count crosses the threshold\n");
    long streak = 0;
    for (int i = 1; i <= 5; i++) {
        streak = tsdb_ae_overcount_note("t_y", 1);
        CHECK(streak == i, "suspect sweep %d -> streak %ld", i, streak);
    }
    CHECK(streak >= 3, "a real over-count persists past AE_OVERCOUNT_PERSIST_N=3");

    /* [3] even after crossing the threshold, a non-suspect sweep clears it — the
     *     condition is edge-recoverable, not sticky. */
    printf("\n[3] crossing the threshold is still recoverable\n");
    CHECK(tsdb_ae_overcount_note("t_z", 1) == 1, "");
    CHECK(tsdb_ae_overcount_note("t_z", 1) == 2, "");
    CHECK(tsdb_ae_overcount_note("t_z", 1) == 3, "t_z is now persistent (3)");
    CHECK(tsdb_ae_overcount_note("t_z", 1) == 4, "and keeps climbing (4)");
    CHECK(tsdb_ae_overcount_note("t_z", 0) == 0,
          "one non-suspect sweep clears even an established over-count");

    /* [4] distinct tables track INDEPENDENTLY. */
    printf("\n[4] two tables keep separate streaks\n");
    CHECK(tsdb_ae_overcount_note("t_p", 1) == 1, "t_p sweep 1");
    CHECK(tsdb_ae_overcount_note("t_q", 1) == 1, "t_q sweep 1 (independent)");
    CHECK(tsdb_ae_overcount_note("t_p", 1) == 2, "t_p sweep 2");
    CHECK(tsdb_ae_overcount_note("t_q", 0) == 0, "t_q resets without touching t_p");
    CHECK(tsdb_ae_overcount_note("t_p", 1) == 3, "t_p is unaffected -> 3");

    printf("\n=== test_ae_overcount: %s ===\n", g_fail ? "FAILED" : "ALL PASS");
    return g_fail ? 1 : 0;
}
