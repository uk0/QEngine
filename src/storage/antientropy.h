/* antientropy.h — anti-entropy pull-candidate selection (storage-internal).
 *
 * tsdb_cluster_resync_table used to collapse every alive peer into a single
 * `best` and contact only that one.  Two peers can report the SAME (count,
 * max_ts) while only one of them can actually hand the rows over:
 *
 *   - a peer on an older binary does not know TSDB_RPC_LOCAL_TABLE_STATS, so
 *     the probe falls back to `SELECT count(*), max(ts) FROM <t>`.  Hierarchy
 *     mirroring makes that a cluster-scoped query, so a peer holding NOTHING
 *     answers with the CLUSTER-WIDE aggregate — byte-identical to the holder's
 *     numbers.  The follow-up `SELECT * FROM <t> WHERE ts > N` is not a scalar
 *     agg, so no coordinator fires and it delivers 0 rows.
 *   - a peer can also lose the table between the probe and the pull, or drop
 *     the connection mid-pull.
 *
 * When the single `best` was one of those, the resync returned TSDB_OK having
 * converged nothing, and the startup resync runs exactly once — so it was
 * permanent.  The fix is to keep EVERY peer's answer, order them, and try them
 * best-first until one actually delivers rows.
 *
 * Declared here rather than in include/tsdb_cluster.h so the retry seam stays
 * an implementation detail while still being unit-testable in-process
 * (tests/test_ae_candidates.c) without sockets, forks or sleeps.
 */
#ifndef TSDB_STORAGE_ANTIENTROPY_H
#define TSDB_STORAGE_ANTIENTROPY_H

#include <stdint.h>

#include "../../include/tsdb_cluster.h"   /* tsdb_ae_action_t */

#ifdef __cplusplus
extern "C" {
#endif

/* One peer's answer to the anti-entropy stats probe. */
typedef struct {
    uint64_t node_id;
    uint64_t count;
    int64_t  max_ts;
} tsdb_ae_cand_t;

/* Order pull candidates best-first, in place: count DESC, max_ts DESC,
 * node_id ASC.
 *
 * count leads deliberately.  It is the key the old single-`best` loop used,
 * and promoting max_ts above it would be a regression: a peer that holds two
 * recent rows would outrank the peer that holds the whole history, the tail
 * pull would move local_max_ts PAST the real holder's, and every later pass
 * would then classify the holder as SKIP_UNSAFE — turning a repairable tail
 * gap into a permanent middle gap.
 *
 * node_id comes last only to make the order TOTAL, so which peer is tried
 * first stops depending on gossip insertion order.  It is not a quality
 * signal: two peers reporting identical stats are indistinguishable here,
 * which is exactly why the driver below retries rather than trusting rank 0.
 */
void tsdb_antientropy_rank_candidates(tsdb_ae_cand_t *cands, int n);

/* Everything the candidate driver needs from the outside world.  Injected so
 * the sequencing can be tested without a cluster; production wires these to
 * tsdb_cluster_local_table_stats / pull_table_delta / a monotonic clock. */
typedef struct {
    void *ctx;

    /* Measure what THIS node currently holds for the table.
     * Returns 0 on success, <0 if the measurement could not be taken — the
     * driver then STOPS rather than act on a guess. */
    int  (*local_stats)(void *ctx, uint64_t *out_count, int64_t *out_max_ts);

    /* Carry out `act` against `cand`.  Return <0 on a hard transport/IO
     * failure for this candidate, >= 0 otherwise.  The value is advisory:
     * real progress is decided by re-measuring local_stats, never by what
     * this reports (a peer can hand back N rows that all fail to insert). */
    int  (*attempt)(void *ctx, const tsdb_ae_cand_t *cand,
                    tsdb_ae_action_t act, int64_t since_ts);

    /* Monotonic milliseconds.  NULL disables the wall budget. */
    long (*now_ms)(void *ctx);

    /* Wall budget for the whole candidate loop, checked at the TOP of each
     * iteration; <= 0 disables it.  The first candidate is therefore always
     * tried in full, and the real bound is budget + one in-flight leg. */
    long budget_ms;
} tsdb_ae_driver_t;

/* What one driver run did. */
typedef struct {
    int      visited;     /* candidates the callback was invoked for        */
    int      pulls;       /* of those, actual fetches (TAIL_PULL/FULL_PULL) */
    uint64_t rows;        /* rows that LANDED (measured local count delta)  */
    uint64_t chosen;      /* node_id that delivered them, 0 if none         */
    int      hard_err;    /* a leg actively failed (transport / measurement)*/
    int      budget_hit;  /* the wall budget cut the loop short             */
} tsdb_ae_run_t;

/* Walk ranked candidates best-first.  For each one: re-measure locally,
 * classify with tsdb_antientropy_decide (unchanged — it remains the sole gate
 * on the destructive path), skip UP_TO_DATE without contacting anybody, else
 * invoke `attempt` and re-measure again.  Stop at the first candidate whose
 * attempt actually raised the local row count.
 *
 * Bounded and terminating by construction: one visit per candidate, no goto,
 * no re-tries, plus the wall budget.  Peers that failed the stats probe never
 * became candidates, so a dead peer is not contacted again here.
 *
 * Returns TSDB_OK (outcome is in *out) or TSDB_ERR_INVAL on bad arguments.
 */
int tsdb_antientropy_run_candidates(const tsdb_ae_cand_t *cands, int ncand,
                                    const tsdb_ae_driver_t *drv,
                                    tsdb_ae_run_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_ANTIENTROPY_H */
