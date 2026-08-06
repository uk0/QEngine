/* test_dedup_ledger.c — the exactness primitive WRITE_BATCH dedup needs.
 *
 * The ledger answers "have I already applied (stream, seq)?".  It must never
 * answer YES for a seq it has not seen: batches are dispatched by independent
 * per-peer workers with NO FIFO guarantee, so seq 7 routinely arrives before
 * seq 5, and a scalar high-water would then report the never-applied seq 5 as
 * already applied and DROP its rows — turning a duplicate-rows bug into a
 * silent missing-rows bug.  That trap is case [2] and it is the reason this
 * structure exists rather than one integer per stream.
 *
 *   [1] in-order          -> frontier tracks, everything seen, replay refused
 *   [2] OUT OF ORDER      -> the gap is NOT seen (a watermark would say it is)
 *   [3] gap filled        -> frontier jumps over the whole absorbed run
 *   [4] window exhausted  -> FAIL CLOSED: refused AND not marked seen
 *   [5] streams isolated  -> one stream's progress never covers another's
 *   [6] restart           -> frontier restores forward-only, never backward
 */

#include "../src/cluster/dedup.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>

static int g_pass = 0, g_fail = 0;

#define CHECK(c, ...) do { \
    if (!(c)) { fprintf(stderr, "FAIL [%s:%d]: ", __FILE__, __LINE__); \
                fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); g_fail++; } \
    else { printf("  ok: "); printf(__VA_ARGS__); printf("\n"); g_pass++; } \
} while (0)

#define S1 0xA1A1A1A1ULL
#define S2 0xB2B2B2B2ULL

int main(void) {
    printf("=== test_dedup_ledger ===\n");

    /* ── [1] in order ─────────────────────────────────────────────────── */
    printf("\n[1] in-order delivery\n");
    {
        tsdb_dedup_ledger_t *l = NULL;
        CHECK(tsdb_dedup_open(8, 64, &l) == TSDB_OK && l, "ledger opens");

        CHECK(tsdb_dedup_seen(l, S1, 1) == 0, "nothing is seen before anything is recorded");
        for (uint64_t s = 1; s <= 5; s++)
            CHECK(tsdb_dedup_record(l, S1, s) == TSDB_OK, "record seq %llu",
                  (unsigned long long)s);
        CHECK(tsdb_dedup_frontier(l, S1) == 5, "frontier is 5 (a contiguous 1..5)");
        CHECK(tsdb_dedup_gap_count(l, S1) == 0, "no out-of-order tail is retained");
        for (uint64_t s = 1; s <= 5; s++)
            CHECK(tsdb_dedup_seen(l, S1, s) == 1, "seq %llu is seen",
                  (unsigned long long)s);
        CHECK(tsdb_dedup_seen(l, S1, 6) == 0, "an unsent seq is NOT seen");

        /* A retry whose ACK was lost — the whole point. */
        CHECK(tsdb_dedup_record(l, S1, 3) == TSDB_ERR_EXISTS,
              "replaying seq 3 is refused as EXISTS (the duplicate is stopped)");
        CHECK(tsdb_dedup_frontier(l, S1) == 5, "and the frontier is unchanged");
        tsdb_dedup_close(l);
    }

    /* ── [2] THE TRAP: out-of-order arrival ───────────────────────────── */
    printf("\n[2] out-of-order arrival — a scalar watermark loses rows here\n");
    {
        tsdb_dedup_ledger_t *l = NULL;
        tsdb_dedup_open(8, 64, &l);

        CHECK(tsdb_dedup_record(l, S1, 1) == TSDB_OK, "seq 1 arrives");
        CHECK(tsdb_dedup_record(l, S1, 7) == TSDB_OK, "seq 7 arrives BEFORE 2..6");

        CHECK(tsdb_dedup_frontier(l, S1) == 1,
              "the frontier stays at 1 — it only advances over a CONTIGUOUS run");
        CHECK(tsdb_dedup_seen(l, S1, 7) == 1, "seq 7 is seen (it really arrived)");
        for (uint64_t s = 2; s <= 6; s++)
            CHECK(tsdb_dedup_seen(l, S1, s) == 0,
                  "seq %llu is NOT seen — a watermark of 7 would have dropped its rows",
                  (unsigned long long)s);

        /* The missing ones must still be applicable when they turn up. */
        CHECK(tsdb_dedup_record(l, S1, 4) == TSDB_OK,
              "the late seq 4 is accepted, not swallowed");
        CHECK(tsdb_dedup_frontier(l, S1) == 1, "frontier still 1 (2 and 3 absent)");
        CHECK(tsdb_dedup_gap_count(l, S1) == 2, "the tail holds exactly {4,7}");
        tsdb_dedup_close(l);
    }

    /* ── [3] filling the gap collapses the tail ───────────────────────── */
    printf("\n[3] filling a gap absorbs the whole contiguous run\n");
    {
        tsdb_dedup_ledger_t *l = NULL;
        tsdb_dedup_open(8, 64, &l);
        /* Everything except 2 arrives, worst-case order. */
        uint64_t order[] = { 5, 3, 1, 6, 4 };
        for (size_t i = 0; i < sizeof(order)/sizeof(order[0]); i++)
            tsdb_dedup_record(l, S1, order[i]);
        CHECK(tsdb_dedup_frontier(l, S1) == 1, "frontier 1 while seq 2 is missing");
        CHECK(tsdb_dedup_gap_count(l, S1) == 4, "tail holds {3,4,5,6}");

        CHECK(tsdb_dedup_record(l, S1, 2) == TSDB_OK, "the missing seq 2 arrives");
        CHECK(tsdb_dedup_frontier(l, S1) == 6,
              "frontier jumps 1 -> 6, absorbing the whole run at once");
        CHECK(tsdb_dedup_gap_count(l, S1) == 0,
              "and the tail is empty — memory is bounded by CONCURRENCY, not uptime");
        for (uint64_t s = 1; s <= 6; s++)
            CHECK(tsdb_dedup_seen(l, S1, s) == 1, "seq %llu still seen after absorb",
                  (unsigned long long)s);
        tsdb_dedup_close(l);
    }

    /* ── [4] window exhausted must FAIL CLOSED ────────────────────────── */
    printf("\n[4] a full out-of-order window refuses instead of forgetting\n");
    {
        tsdb_dedup_ledger_t *l = NULL;
        tsdb_dedup_open(8, /*max_gap*/4, &l);
        /* seq 1 never arrives, so nothing can be absorbed. */
        CHECK(tsdb_dedup_record(l, S1, 2) == TSDB_OK, "seq 2 (gap 1/4)");
        CHECK(tsdb_dedup_record(l, S1, 3) == TSDB_OK, "seq 3 (gap 2/4)");
        CHECK(tsdb_dedup_record(l, S1, 4) == TSDB_OK, "seq 4 (gap 3/4)");
        CHECK(tsdb_dedup_record(l, S1, 5) == TSDB_OK, "seq 5 (gap 4/4, window full)");

        int rc = tsdb_dedup_record(l, S1, 9);
        CHECK(rc == TSDB_ERR_FULL,
              "seq 9 is REFUSED (rc=%d) rather than recorded unreliably", rc);
        CHECK(tsdb_dedup_seen(l, S1, 9) == 0,
              "and 9 is NOT marked seen — the sender's retry can still deliver it");
        CHECK(tsdb_dedup_gap_count(l, S1) == 4, "the window is unchanged");

        /* Draining the gap frees the window again. */
        CHECK(tsdb_dedup_record(l, S1, 1) == TSDB_OK, "the missing seq 1 arrives");
        CHECK(tsdb_dedup_frontier(l, S1) == 5, "frontier absorbs to 5");
        CHECK(tsdb_dedup_record(l, S1, 9) == TSDB_OK,
              "and seq 9 is now accepted — the refusal was backpressure, not loss");
        tsdb_dedup_close(l);
    }

    /* ── [5] streams are independent ──────────────────────────────────── */
    printf("\n[5] one stream's progress never covers another\n");
    {
        tsdb_dedup_ledger_t *l = NULL;
        tsdb_dedup_open(8, 64, &l);
        for (uint64_t s = 1; s <= 10; s++) tsdb_dedup_record(l, S1, s);
        CHECK(tsdb_dedup_frontier(l, S1) == 10, "stream A frontier 10");
        CHECK(tsdb_dedup_frontier(l, S2) == 0,  "stream B frontier still 0");
        CHECK(tsdb_dedup_seen(l, S2, 5) == 0,
              "stream B seq 5 is NOT seen — a DROP+recreate mints a new stream id, "
              "so its seq 1.. must not collide with the old table's history");
        CHECK(tsdb_dedup_record(l, S2, 5) == TSDB_OK, "and B can record its own seq 5");
        CHECK(tsdb_dedup_seen(l, S1, 5) == 1, "A's seq 5 is untouched");
        tsdb_dedup_close(l);
    }

    /* ── [6] restart: frontier restores forward-only ──────────────────── */
    printf("\n[6] a restored frontier only ever moves forward\n");
    {
        tsdb_dedup_ledger_t *l = NULL;
        tsdb_dedup_open(8, 64, &l);
        CHECK(tsdb_dedup_set_frontier(l, S1, 100) == TSDB_OK, "restore frontier 100");
        CHECK(tsdb_dedup_frontier(l, S1) == 100, "frontier is 100");
        CHECK(tsdb_dedup_seen(l, S1, 100) == 1, "everything <= 100 is seen");
        CHECK(tsdb_dedup_record(l, S1, 42) == TSDB_ERR_EXISTS,
              "a pre-restart seq is refused as already applied");

        /* A stale checkpoint must not drag it back and re-admit applied seqs. */
        CHECK(tsdb_dedup_set_frontier(l, S1, 50) == TSDB_OK, "a STALE restore is accepted...");
        CHECK(tsdb_dedup_frontier(l, S1) == 100,
              "...but ignored — the frontier stays 100, never rewinds");
        CHECK(tsdb_dedup_record(l, S1, 60) == TSDB_ERR_EXISTS,
              "so an already-applied seq is still refused, not duplicated");

        /* And a restore that covers an out-of-order tail collapses it. */
        tsdb_dedup_record(l, S1, 105);
        tsdb_dedup_record(l, S1, 106);
        CHECK(tsdb_dedup_gap_count(l, S1) == 2, "tail {105,106} above the frontier");
        CHECK(tsdb_dedup_set_frontier(l, S1, 104) == TSDB_OK, "restore to 104");
        CHECK(tsdb_dedup_frontier(l, S1) == 106,
              "frontier absorbs the now-contiguous tail to 106");
        CHECK(tsdb_dedup_gap_count(l, S1) == 0, "tail collapsed");
        tsdb_dedup_close(l);
    }

    /* ── argument hygiene ─────────────────────────────────────────────── */
    printf("\n[7] arguments\n");
    {
        tsdb_dedup_ledger_t *l = NULL;
        tsdb_dedup_open(2, 4, &l);
        CHECK(tsdb_dedup_record(l, S1, 0) == TSDB_ERR_INVAL, "seq 0 is invalid (seqs start at 1)");
        CHECK(tsdb_dedup_seen(l, S1, 0) == 0, "and seq 0 is never 'seen'");
        CHECK(tsdb_dedup_open(0, 4, &l) == TSDB_ERR_INVAL, "max_streams 0 refused");
        CHECK(tsdb_dedup_open(4, 0, &l) == TSDB_ERR_INVAL, "max_gap 0 refused");
        /* Slot exhaustion must be NOMEM, never a false "seen". */
        tsdb_dedup_ledger_t *t = NULL;
        tsdb_dedup_open(1, 4, &t);
        tsdb_dedup_record(t, 111, 1);
        CHECK(tsdb_dedup_record(t, 222, 1) == TSDB_ERR_NOMEM,
              "no slot left -> NOMEM (refuse), never a silent accept");
        CHECK(tsdb_dedup_seen(t, 222, 1) == 0, "and the unrecorded stream is not seen");
        tsdb_dedup_close(t);
        tsdb_dedup_close(l);
    }

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
