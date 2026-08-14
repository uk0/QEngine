/* test_dedup_ckpt_restart.c — the dedup ledger must survive a restart.
 *
 * The ledger is what stops a retried WRITE_BATCH being applied twice, and an
 * over-count is unrepairable afterwards ("local has more" is indistinguishable
 * from legitimate local writes).  dedup.c already implements a CRC32C'd,
 * atomically-renamed, directory-fsynced checkpoint of the frontier — but until
 * this test nothing outside dedup.c/dedup.h and the ledger unit test ever
 * called it, so production only ever held the ledger in memory and every
 * restart forgot every id it had ever applied.
 *
 * The WAL cannot cover this on its own: a flush TRUNCATES the log, so the redo
 * records that carry the ids are gone the moment the rows become durable.  The
 * checkpoint is exactly the state for those seqs.
 *
 * Only the contiguous FRONTIER is persisted, by design (dedup.h), so the stream
 * here arrives in order — that is what a frontier is, and the out-of-order tail
 * is the WAL's job.
 *
 * Phases:
 *   1. a receiver applies (S1, 1..8) and the node closes cleanly
 *   2. the process ledger is dropped — the memory a restart loses
 *   3. reopening the db must bring those seqs back as SEEN, so the sender's
 *      retry is dropped instead of doubling the rows
 *   4. a seq that was never applied must still be UNSEEN — a fix that simply
 *      marks everything seen would turn duplicate rows into missing rows,
 *      which the dedup design calls out as strictly worse
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/cluster/dedup.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d", _r); } while (0)

static const char *DIR = "/tmp/tsdb_test_dedup_ckpt_restart";
static const uint64_t S1 = 0x5EED0001ULL;

static void rm_tree(const char *p) {
    char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c);
}

/* Mirrors rpc_apply_write_batch: the id is recorded AFTER the batch applied. */
static int record_id(uint64_t stream, uint64_t seq) {
    tsdb_dedup_global_lock();
    tsdb_dedup_ledger_t *l = tsdb_dedup_global();
    int rc = l ? tsdb_dedup_record(l, stream, seq) : TSDB_ERR_INVAL;
    tsdb_dedup_global_unlock();
    return rc;
}

static int id_seen(uint64_t stream, uint64_t seq) {
    tsdb_dedup_global_lock();
    tsdb_dedup_ledger_t *l = tsdb_dedup_global();
    int s = l && tsdb_dedup_seen(l, stream, seq);
    tsdb_dedup_global_unlock();
    return s;
}

/* One committed batch, so the node has real rows behind the recorded ids. */
static void write_one_batch(tsdb_db_t *db, const char *name, int64_t ts0) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, name, &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int r = 0; r < 8; r++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(ts0 + r)));
        OK(tsdb_batch_row_i64(b, 1, r));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

int main(void) {
    printf("=== test_dedup_ckpt_restart ===\n");

    setenv("TSDB_DEDUP", "1", 1);
    rm_tree(DIR);
    tsdb_dedup_global_reset_for_test();

    /* ---- Phase 1: apply eight in-order batches, then close cleanly ---- */
    {
        tsdb_db_t *db = NULL;
        OK(tsdb_open(DIR, &db));
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_INT64     },
        };
        OK(tsdb_create_table(db, "dq", cols, 2, "ts"));

        for (uint64_t q = 1; q <= 8; q++) {
            write_one_batch(db, "dq", 1735689600000000000LL + (int64_t)q * 1000);
            OK(record_id(S1, q));
        }

        ASSERT(id_seen(S1, 8) == 1);
        tsdb_close(db);
        printf("  phase 1: applied (S1,1..8), closed\n");
    }

    /* ---- Phase 2: the restart loses the in-memory ledger ---- */
    tsdb_dedup_global_reset_for_test();
    ASSERT(id_seen(S1, 7) == 0);
    printf("  phase 2: process ledger dropped\n");

    /* ---- Phase 3 + 4: reopen and check what came back ---- */
    {
        tsdb_db_t *db = NULL;
        OK(tsdb_open(DIR, &db));

        int seen7 = id_seen(S1, 7);
        int seen8 = id_seen(S1, 8);
        int seen9 = id_seen(S1, 9);
        printf("  phase 3: after reopen seen(7)=%d seen(8)=%d seen(9)=%d\n",
               seen7, seen8, seen9);

        ASSERT(seen7 == 1);
        ASSERT(seen8 == 1);
        /* Never applied — must not be swallowed. */
        ASSERT(seen9 == 0);

        /* And the retry the ledger exists to stop is refused, not re-applied. */
        ASSERT(record_id(S1, 7) == TSDB_ERR_EXISTS);

        tsdb_close(db);
    }

    tsdb_dedup_global_reset_for_test();
    rm_tree(DIR);
    printf("PASS test_dedup_ckpt_restart\n");
    return 0;
}
