/* test_ae_candidates.c — anti-entropy must not bet the whole resync on ONE peer.
 *
 * THE BUG.  tsdb_cluster_resync_table collapsed every alive peer into a single
 * `best` and contacted only that one.  Two peers routinely report the SAME
 * (count, max_ts) while only one of them can actually deliver the rows: a peer
 * on an older binary does not know TSDB_RPC_LOCAL_TABLE_STATS, so the probe
 * falls back to `SELECT count(*), max(ts) FROM <t>`, and hierarchy mirroring
 * turns that into a cluster-scoped query — a peer holding NOTHING answers with
 * the CLUSTER-WIDE aggregate, byte-identical to the holder's numbers.  The
 * follow-up `SELECT * FROM <t> WHERE ts > N` is not a scalar agg, so no
 * coordinator fires and it delivers 0 rows.  The old tie-break kept the FIRST
 * reporter, so gossip insertion order decided the winner; when it was the
 * empty peer the resync returned TSDB_OK having converged nothing — and the
 * startup resync runs exactly once, so it stayed empty for the process life.
 *
 * THE FIX under test: keep every peer's answer, rank them with a TOTAL order,
 * and walk them best-first until one actually raises the local row count.
 *
 * This is the deterministic half — the real production functions
 * (tsdb_antientropy_rank_candidates / tsdb_antientropy_run_candidates from
 * src/storage/db_cluster.c) driven through injected callbacks.  No sockets, no
 * forks, no sleeps, no temp dirs.  The live two-node half is
 * tests/test_ae_phantom_peer.c under `make test-cluster`.
 */
#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/storage/antientropy.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_fail = 0;

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define MAXTS  1743465609999000000LL

/* ---- fake world --------------------------------------------------------- */

/* Models one node's local storage plus the peers it can talk to.
 *
 *   holder      — the only candidate whose attempt actually lands rows
 *   deliver     — how many rows it lands
 *   claim       — what EVERY attempt REPORTS, regardless of what it lands
 *                 (a peer can hand back N rows that all fail to insert)
 *   stats_fail_at — local_stats returns failure on the Nth call (0 = never)
 *   hard_err_on — this node id's attempt returns a transport failure
 *   clock_step  — ms the injected clock advances per reading
 */
typedef struct {
    uint64_t local_count;
    int64_t  local_max_ts;

    uint64_t holder;
    uint64_t deliver;
    int      claim;
    uint64_t hard_err_on;

    int      stats_calls;
    int      stats_fail_at;

    long     now;
    long     clock_step;

    /* recording */
    uint64_t         visited[16];
    tsdb_ae_action_t acts[16];
    int64_t          sinces[16];
    int              nvisited;
} world_t;

static int w_local_stats(void *vctx, uint64_t *count, int64_t *max_ts) {
    world_t *w = (world_t *)vctx;
    w->stats_calls++;
    if (w->stats_fail_at && w->stats_calls == w->stats_fail_at) return -1;
    *count  = w->local_count;
    *max_ts = w->local_max_ts;
    return 0;
}

static long w_now_ms(void *vctx) {
    world_t *w = (world_t *)vctx;
    long t = w->now;
    w->now += w->clock_step;
    return t;
}

static int w_attempt(void *vctx, const tsdb_ae_cand_t *c,
                     tsdb_ae_action_t act, int64_t since)
{
    world_t *w = (world_t *)vctx;
    if (w->nvisited < 16) {
        w->visited[w->nvisited] = c->node_id;
        w->acts[w->nvisited]    = act;
        w->sinces[w->nvisited]  = since;
    }
    w->nvisited++;

    if (c->node_id == w->hard_err_on) return -1;

    if (c->node_id == w->holder && w->deliver > 0) {
        /* Rows land: the local measurement moves. */
        w->local_count += w->deliver;
        if (c->max_ts > w->local_max_ts) w->local_max_ts = c->max_ts;
        return (int)w->deliver;
    }
    /* A phantom: it may still CLAIM it delivered a pile of rows. */
    return w->claim;
}

static void drv_init(tsdb_ae_driver_t *d, world_t *w, long budget_ms) {
    memset(d, 0, sizeof(*d));
    d->ctx         = w;
    d->local_stats = w_local_stats;
    d->attempt     = w_attempt;
    d->now_ms      = w_now_ms;
    d->budget_ms   = budget_ms;
}

static const char *act_name(tsdb_ae_action_t a) {
    switch (a) {
    case TSDB_AE_UP_TO_DATE:  return "UP_TO_DATE";
    case TSDB_AE_TAIL_PULL:   return "TAIL_PULL";
    case TSDB_AE_FULL_PULL:   return "FULL_PULL";
    case TSDB_AE_SKIP_UNSAFE: return "SKIP_UNSAFE";
    }
    return "?";
}

static void print_order(const char *what, const tsdb_ae_cand_t *c, int n) {
    printf("    %s:", what);
    for (int i = 0; i < n; i++) printf(" %llu", (unsigned long long)c[i].node_id);
    printf("\n");
}

/* ids visited, in order, as a comparable string */
static int visited_is(const world_t *w, const uint64_t *want, int n) {
    if (w->nvisited != n) return 0;
    for (int i = 0; i < n; i++) if (w->visited[i] != want[i]) return 0;
    return 1;
}

int main(void) {
    printf("=== test_ae_candidates ===\n");

    /* ---------------------------------------------------------------- [1] */
    printf("\n[1] the order is TOTAL and deterministic\n");
    {
        /* The measured phantom/holder tie: identical (count, max_ts).  The old
         * strict `>` tie-break kept whichever gossip listed first, so the same
         * cluster could pick a different peer on every boot. */
        tsdb_ae_cand_t a[3] = {
            { 7, 10000, MAXTS }, { 3, 10000, MAXTS }, { 9, 10000, MAXTS },
        };
        tsdb_antientropy_rank_candidates(a, 3);
        print_order("ranked", a, 3);
        CHECK(a[0].node_id == 3 && a[1].node_id == 7 && a[2].node_id == 9,
              "identical stats are separated by node_id ASC");

        tsdb_ae_cand_t b[3] = {
            { 9, 10000, MAXTS }, { 7, 10000, MAXTS }, { 3, 10000, MAXTS },
        };
        tsdb_antientropy_rank_candidates(b, 3);
        CHECK(b[0].node_id == 3 && b[1].node_id == 7 && b[2].node_id == 9,
              "a shuffled input ranks to the SAME order (membership order "
              "no longer decides)");
    }

    /* ---------------------------------------------------------------- [2] */
    printf("\n[2] count DOMINATES max_ts — a repairable tail gap stays repairable\n");
    {
        /* Post-downtime shape: H holds the whole history, P was alive for the
         * last two fanned-out writes only.  Ranking max_ts first would put P
         * on top; its 2-row tail pull would move local_max_ts PAST H's, and
         * every later pass would then classify H as SKIP_UNSAFE — 998 rows
         * lost to this node forever.  count first keeps H at rank 0. */
        tsdb_ae_cand_t a[2] = { { 2, 2, MAXTS + 20 }, { 1, 1000, MAXTS + 10 } };
        tsdb_antientropy_rank_candidates(a, 2);
        print_order("ranked", a, 2);
        CHECK(a[0].node_id == 1 && a[0].count == 1000,
              "the full holder outranks a newer-but-tiny peer");
    }

    /* ---------------------------------------------------------------- [3] */
    printf("\n[3] max_ts breaks an equal count; node_id breaks the rest\n");
    {
        tsdb_ae_cand_t a[3] = {
            { 1, 5000, MAXTS }, { 2, 5000, MAXTS + 1 }, { 4, 5000, MAXTS },
        };
        tsdb_antientropy_rank_candidates(a, 3);
        print_order("ranked", a, 3);
        CHECK(a[0].node_id == 2 && a[1].node_id == 1 && a[2].node_id == 4,
              "equal counts order by max_ts DESC then node_id ASC");
    }

    /* ---------------------------------------------------------------- [4] */
    printf("\n[4] degenerate inputs do not crash and change nothing\n");
    {
        tsdb_ae_cand_t one = { 42, 7, MAXTS };
        tsdb_antientropy_rank_candidates(NULL, 0);
        tsdb_antientropy_rank_candidates(NULL, 5);
        tsdb_antientropy_rank_candidates(&one, 0);
        tsdb_antientropy_rank_candidates(&one, 1);
        CHECK(one.node_id == 42 && one.count == 7 && one.max_ts == MAXTS,
              "rank(NULL/0/1) is a no-op");

        tsdb_ae_run_t r;
        tsdb_ae_cand_t c = { 1, 10, MAXTS };
        tsdb_ae_driver_t d; world_t w; memset(&w, 0, sizeof(w));
        drv_init(&d, &w, 0);
        CHECK(tsdb_antientropy_run_candidates(NULL, 1, &d, &r) == TSDB_ERR_INVAL,
              "run(NULL cands) is rejected");
        CHECK(tsdb_antientropy_run_candidates(&c, 0, &d, &r) == TSDB_ERR_INVAL,
              "run(0 cands) is rejected");
        CHECK(tsdb_antientropy_run_candidates(&c, 1, NULL, &r) == TSDB_ERR_INVAL,
              "run(no driver) is rejected");
        CHECK(tsdb_antientropy_run_candidates(&c, 1, &d, NULL) == TSDB_ERR_INVAL,
              "run(no out) is rejected");
        CHECK(w.nvisited == 0, "no candidate was contacted on a bad call");
    }

    /* ---------------------------------------------------------------- [5] */
    printf("\n[5] THE FIX: phantoms are skipped until the real holder is found\n");
    {
        /* Empty local, four peers all reporting the cluster aggregate.
         * Exactly one of them can actually deliver.  The old code contacted
         * ONE peer and gave up. */
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.holder = 5; w.deliver = 10000;

        tsdb_ae_cand_t c[4] = {
            { 3, 10000, MAXTS }, { 7, 10000, MAXTS },
            { 5, 10000, MAXTS }, { 9, 10000, MAXTS },
        };
        tsdb_antientropy_rank_candidates(c, 4);
        print_order("ranked", c, 4);

        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        CHECK(tsdb_antientropy_run_candidates(c, 4, &d, &r) == TSDB_OK, "run ok");

        /* Ranked order is 3,5,7,9 — so exactly 3 then 5, and nobody else. */
        uint64_t want[2] = { 3, 5 };
        printf("    visited:");
        for (int i = 0; i < w.nvisited; i++)
            printf(" %llu", (unsigned long long)w.visited[i]);
        printf("  rows=%llu chosen=%llu\n",
               (unsigned long long)r.rows, (unsigned long long)r.chosen);

        CHECK(r.rows == 10000, "the holder's rows landed (10000)");
        CHECK(r.chosen == 5, "the run names the peer that delivered");
        CHECK(visited_is(&w, want, 2),
              "candidates are visited best-first, stopping at the holder");
        CHECK(w.nvisited == 2 && w.visited[1] == 5,
              "7 and 9 are never contacted once the holder delivered");
        CHECK(r.visited == w.nvisited && r.pulls == w.nvisited,
              "the run reports every visit exactly once");
        CHECK(r.hard_err == 0 && r.budget_hit == 0, "no error, no budget cut");
    }

    /* ---------------------------------------------------------------- [6] */
    printf("\n[6] every candidate a phantom: bounded, no repeats, no progress\n");
    {
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.holder = 0;               /* nobody delivers */

        tsdb_ae_cand_t c[4] = {
            { 3, 10000, MAXTS }, { 7, 10000, MAXTS },
            { 5, 10000, MAXTS }, { 9, 10000, MAXTS },
        };
        tsdb_antientropy_rank_candidates(c, 4);
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 4, &d, &r);

        CHECK(r.rows == 0 && r.chosen == 0, "nothing converged, and it says so");
        CHECK(w.nvisited == 4, "every candidate tried exactly once (no repeats)");
        int dup = 0;
        for (int i = 0; i < w.nvisited; i++)
            for (int j = i + 1; j < w.nvisited; j++)
                if (w.visited[i] == w.visited[j]) dup = 1;
        CHECK(!dup, "no peer is hammered twice in one resync");
        CHECK(r.pulls == 4, "the caller can tell 4 peers claimed rows and none paid");
        CHECK(w.local_count == 0, "local storage is left byte-for-byte alone");
    }

    /* ---------------------------------------------------------------- [7] */
    printf("\n[7] UP_TO_DATE candidates are never contacted at all\n");
    {
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 5000; w.local_max_ts = MAXTS;
        tsdb_ae_cand_t c[2] = { { 1, 5000, MAXTS }, { 2, 4000, MAXTS } };
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 2, &d, &r);
        CHECK(w.nvisited == 0 && r.visited == 0 && r.pulls == 0,
              "a peer with nothing to add costs zero round-trips");
        CHECK(r.rows == 0 && r.hard_err == 0, "and is not an error");
    }

    /* ---------------------------------------------------------------- [8] */
    printf("\n[8] progress is MEASURED, not reported\n");
    {
        /* The delta pull counts rows the peer handed back and discards
         * tsdb_batch_row_end's status, so a peer whose rows all fail to insert
         * (schema drift, an un-internable symbol) "returns 10000" while the
         * table stays empty.  Trusting that number would stop the loop on a
         * peer that converged nothing — the same silent failure, one layer up. */
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.holder = 9; w.deliver = 4242;
        w.claim  = 10000;           /* every phantom lies loudly */

        tsdb_ae_cand_t c[3] = {
            { 3, 10000, MAXTS }, { 7, 10000, MAXTS }, { 9, 10000, MAXTS },
        };
        tsdb_antientropy_rank_candidates(c, 3);
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 3, &d, &r);
        printf("    visited=%d rows=%llu chosen=%llu\n",
               w.nvisited, (unsigned long long)r.rows,
               (unsigned long long)r.chosen);
        CHECK(w.nvisited == 3, "a peer that CLAIMS 10000 but lands 0 does not "
                               "stop the walk");
        CHECK(r.rows == 4242 && r.chosen == 9,
              "the reported rows are the local count delta, not the peer's claim");
    }

    /* ---------------------------------------------------------------- [9] */
    printf("\n[9] the wall budget bounds the retry\n");
    {
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.holder = 0;
        w.clock_step = 20000;       /* each clock reading is 20 s later */

        tsdb_ae_cand_t c[5] = {
            { 1, 10000, MAXTS }, { 2, 10000, MAXTS }, { 3, 10000, MAXTS },
            { 4, 10000, MAXTS }, { 5, 10000, MAXTS },
        };
        tsdb_ae_driver_t d; drv_init(&d, &w, 30000);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 5, &d, &r);
        printf("    visited=%d budget_hit=%d\n", w.nvisited, r.budget_hit);
        CHECK(r.budget_hit == 1, "the loop reports that the budget cut it short");
        CHECK(w.nvisited >= 1, "candidate 0 is always tried in full");
        CHECK(w.nvisited < 5, "a 64-peer cluster cannot serialise 64 x 60 s "
                              "of pulls per table");
    }
    {
        /* budget_ms <= 0 (or no clock) disables it — the loop is still bounded
         * by the candidate count. */
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.clock_step = 1000000;
        tsdb_ae_cand_t c[3] = {
            { 1, 10, MAXTS }, { 2, 10, MAXTS }, { 3, 10, MAXTS },
        };
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 3, &d, &r);
        CHECK(w.nvisited == 3 && r.budget_hit == 0,
              "no budget still terminates after ncand visits");
    }

    /* --------------------------------------------------------------- [10] */
    printf("\n[10] a hard failure moves on; a later peer can still converge\n");
    {
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.holder = 7; w.deliver = 500;
        w.hard_err_on = 3;          /* rank 0 drops the connection */

        tsdb_ae_cand_t c[2] = { { 3, 10000, MAXTS }, { 7, 10000, MAXTS } };
        tsdb_antientropy_rank_candidates(c, 2);
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 2, &d, &r);
        CHECK(r.rows == 500 && r.chosen == 7,
              "a dead first candidate no longer ends the resync");
        CHECK(r.hard_err == 1, "the failure is still reported to the caller");
    }
    {
        /* If NOBODY converges, the hard failure is what the caller sees. */
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.hard_err_on = 3;
        tsdb_ae_cand_t c[1] = { { 3, 10000, MAXTS } };
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 1, &d, &r);
        CHECK(r.rows == 0 && r.hard_err == 1,
              "transport failure with no source found is still an error");
    }

    /* --------------------------------------------------------------- [11] */
    printf("\n[11] an unmeasurable local table stops the loop — never guess\n");
    {
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        w.stats_fail_at = 1;        /* the very first measurement fails */
        tsdb_ae_cand_t c[3] = {
            { 1, 10000, MAXTS }, { 2, 10000, MAXTS }, { 3, 10000, MAXTS },
        };
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 3, &d, &r);
        CHECK(w.nvisited == 0, "no peer is contacted when we cannot measure "
                               "ourselves");
        CHECK(r.hard_err == 1 && r.rows == 0, "and the caller is told");
    }

    /* --------------------------------------------------------------- [12] */
    printf("\n[12] the destructive gate still holds THROUGH the driver\n");
    {
        /* tests/test_antientropy_safety.c pins tsdb_antientropy_decide itself.
         * This is the same invariant one layer up: with the candidate loop in
         * place, a POPULATED local table must never be handed FULL_PULL — the
         * only action that truncates — no matter what any peer reports. */
        int saw_full = 0, sweeps = 0;
        for (uint64_t pc = 0; pc < 5000000000000ULL; pc += 137000000ULL) {
            world_t w; memset(&w, 0, sizeof(w));
            w.local_count = 3620864; w.local_max_ts = MAXTS;
            tsdb_ae_cand_t c[1] = { { 1, pc, MAXTS } };
            tsdb_ae_driver_t d; drv_init(&d, &w, 0);
            tsdb_ae_run_t r;
            tsdb_antientropy_run_candidates(c, 1, &d, &r);
            for (int i = 0; i < w.nvisited; i++)
                if (w.acts[i] == TSDB_AE_FULL_PULL) saw_full = 1;
            sweeps++;
        }
        printf("    swept %d peer counts at equal max_ts\n", sweeps);
        CHECK(!saw_full, "3.6M durable rows are never offered up for truncate");

        /* Same sweep with the peer BEHIND us in time — the input space the
         * candidate list newly makes reachable (the old loop admitted it too,
         * but only ever as the single `best`). */
        saw_full = 0;
        for (uint64_t pc = 0; pc < 5000000000000ULL; pc += 137000000ULL) {
            world_t w; memset(&w, 0, sizeof(w));
            w.local_count = 3620864; w.local_max_ts = MAXTS;
            tsdb_ae_cand_t c[1] = { { 1, pc, MAXTS - 1000000 } };
            tsdb_ae_driver_t d; drv_init(&d, &w, 0);
            tsdb_ae_run_t r;
            tsdb_antientropy_run_candidates(c, 1, &d, &r);
            for (int i = 0; i < w.nvisited; i++)
                if (w.acts[i] == TSDB_AE_FULL_PULL) saw_full = 1;
        }
        CHECK(!saw_full, "a peer BEHIND us in time cannot obtain the truncate "
                         "either");

        /* And an empty local table against real data pulls the tail, which
         * truncates nothing. */
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 0; w.local_max_ts = INT64_MIN;
        tsdb_ae_cand_t c[1] = { { 1, 10000, MAXTS } };
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 1, &d, &r);
        printf("    empty local vs real peer -> %s\n", act_name(w.acts[0]));
        CHECK(w.nvisited == 1 && w.acts[0] == TSDB_AE_TAIL_PULL,
              "an empty replica catches up without a truncate");
    }

    /* --------------------------------------------------------------- [13] */
    printf("\n[13] since_ts comes from a FRESH measurement of this node\n");
    {
        world_t w; memset(&w, 0, sizeof(w));
        w.local_count = 500; w.local_max_ts = MAXTS;
        w.holder = 0;
        tsdb_ae_cand_t c[2] = { { 1, 1000, MAXTS + 10 }, { 2, 900, MAXTS + 5 } };
        tsdb_antientropy_rank_candidates(c, 2);
        tsdb_ae_driver_t d; drv_init(&d, &w, 0);
        tsdb_ae_run_t r;
        tsdb_antientropy_run_candidates(c, 2, &d, &r);
        CHECK(w.nvisited == 2 &&
              w.sinces[0] == MAXTS && w.sinces[1] == MAXTS,
              "a tail pull asks for ts > our CURRENT max_ts");
        CHECK(w.acts[0] == TSDB_AE_TAIL_PULL && w.acts[1] == TSDB_AE_TAIL_PULL,
              "both newer peers are legitimate tail sources");
    }

    printf("\n=== test_ae_candidates: %s ===\n", g_fail ? "FAILED" : "ALL PASS");
    return g_fail ? 1 : 0;
}
