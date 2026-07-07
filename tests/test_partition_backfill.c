/* test_partition_backfill.c — partition backfill converges a middle-gap replica.
 *
 * Two standalone dbs stand in for two replicas of one table:
 *   A (the fuller peer) holds a FULL day-1 partition (1000 rows);
 *   B (the divergent replica) holds a 600-row subset that includes A's final
 *   row, so both report the SAME max_ts with different counts — the exact
 *   middle-gap shape tsdb_antientropy_decide classifies as SKIP_UNSAFE.
 *
 * The backfill core (tsdb_cluster_backfill_partition_from_result) is called
 * directly with A's rows (a tsdb_result_t from a local SELECT — the same
 * shape fedrpc_query returns), no cluster required.  Asserts:
 *   1. the shape really is the SKIP_UNSAFE middle gap;
 *   2. the one-query peer-map QTL (time_bucket + count(*) SAMPLE BY 1d)
 *      yields buckets aligned with the partition boundaries;
 *   3. after the swap B's partition holds all 1000 rows, values / symbols /
 *      sum(val) correct, scratch dir cleaned, and the data survives reopen;
 *   4. the staleness guard aborts (TSDB_ERR_BUSY) when the partition is
 *      written between the caller's snapshot and the swap, table intact;
 *   5. a pull that is not strictly fuller than the live partition is refused
 *      (backfill never shrinks durable data).
 */

#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/storage/db.h"   /* tsdb_db_flush_all */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

#define DIR_A   "/tmp/tsdb_test_pbf_a"
#define DIR_B   "/tmp/tsdb_test_pbf_b"
#define BASE_TS 1750000000000000000LL   /* 2025-06-15T15:06:40Z */
#define STEP_NS 1000000LL               /* 1 ms */
#define DAY_NS  86400000000000LL

static double val_of(int i) { return (double)i + (double)(i % 97) / 100.0; }
static const char *tag_of(int i) {
    static const char *tags[3] = { "h0", "h1", "h2" };
    return tags[i % 3];
}

static tsdb_db_t *open_with_table(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open %s failed\n", dir); exit(1); }
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
        { "tag", TSDB_TYPE_SYMBOL    },
    };
    int rc = tsdb_create_table(db, "t", cols, 3, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create rc=%d\n", rc); exit(1); }
    return db;
}

static void write_rows(tsdb_db_t *db, const int *idx, int n) {
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "open_table\n"); exit(1); }
    tsdb_batch_t *b = NULL;
    if (tsdb_batch_begin(t, &b) != TSDB_OK) { fprintf(stderr, "batch_begin\n"); exit(1); }
    for (int k = 0; k < n; k++) {
        int i = idx[k];
        tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * STEP_NS);
        tsdb_batch_row_f64(b, 1, val_of(i));
        tsdb_batch_row_sym(b, 2, tag_of(i));
        tsdb_batch_row_end(b);
    }
    if (tsdb_batch_commit(b) != TSDB_OK) { fprintf(stderr, "commit\n"); exit(1); }
}

static int64_t q_count(tsdb_db_t *db) {
    tsdb_result_t *r = NULL; int64_t v = -1;
    if (tsdb_query(db, "SELECT count(*) FROM t", &r) == TSDB_OK && r) {
        if (tsdb_result_next(r)) v = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
    }
    return v;
}

static double q_sum(tsdb_db_t *db) {
    tsdb_result_t *r = NULL; double v = -1.0;
    if (tsdb_query(db, "SELECT sum(val) FROM t", &r) == TSDB_OK && r) {
        if (tsdb_result_next(r)) v = tsdb_result_f64(r, 0);
        tsdb_result_free(r);
    }
    return v;
}

/* Fetch (val, tag) of the single row at ts = BASE_TS + i*STEP_NS. */
static int q_row(tsdb_db_t *db, int i, double *out_val, char *out_tag, size_t cap) {
    char qtl[256];
    long long ts = BASE_TS + (long long)i * STEP_NS;
    snprintf(qtl, sizeof(qtl),
             "SELECT val, tag FROM t WHERE ts >= %lld AND ts <= %lld", ts, ts);
    tsdb_result_t *r = NULL;
    int got = 0;
    if (tsdb_query(db, qtl, &r) == TSDB_OK && r) {
        if (tsdb_result_next(r)) {
            *out_val = tsdb_result_f64(r, 0);
            const char *s = tsdb_result_sym(r, 1);
            snprintf(out_tag, cap, "%s", s ? s : "");
            got = 1;
        }
        tsdb_result_free(r);
    }
    return got;
}

/* SELECT the full partition range from db — the same result shape the
 * cluster pull (fedrpc SELECT *) produces. */
static tsdb_result_t *pull_range(tsdb_db_t *db, long long lo, long long hi) {
    char qtl[256];
    snprintf(qtl, sizeof(qtl),
             "SELECT * FROM t WHERE ts >= %lld AND ts <= %lld", lo, hi);
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, qtl, &r) != TSDB_OK || !r) {
        fprintf(stderr, "pull_range query failed\n"); exit(1);
    }
    return r;
}

int main(void) {
    printf("=== test_partition_backfill ===\n");
    rmrf(DIR_A); rmrf(DIR_B);

    long long pstart = BASE_TS - (BASE_TS % DAY_NS);
    long long pend   = pstart + DAY_NS - 1;
    char pname[12];
    {
        time_t secs = (time_t)(pstart / 1000000000LL);
        struct tm tm; gmtime_r(&secs, &tm);
        snprintf(pname, sizeof(pname), "%04d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    }
    printf("  partition %s  [%lld, %lld]\n", pname, pstart, pend);

    tsdb_db_t *A = open_with_table(DIR_A);
    tsdb_db_t *B = open_with_table(DIR_B);

    /* A: full day (i = 0..999).  B: middle gap (i = 0..598 plus 999) —
     * 600 rows, same final row, same max_ts. */
    int idx[1050];
    for (int i = 0; i < 1000; i++) idx[i] = i;
    write_rows(A, idx, 1000);
    int n = 0;
    for (int i = 0; i <= 598; i++) idx[n++] = i;
    idx[n++] = 999;
    write_rows(B, idx, n);

    tsdb_db_flush_all(A);
    tsdb_db_flush_all(B);
    CHECK(q_count(A) == 1000, "A holds the full partition (1000 rows)");
    CHECK(q_count(B) == 600,  "B holds the middle-gap subset (600 rows)");

    printf("\n[1] shape really is the SKIP_UNSAFE middle gap\n");
    {
        long long max_ts = BASE_TS + 999 * STEP_NS;
        CHECK(tsdb_antientropy_decide(600, max_ts, 1000, max_ts)
                  == TSDB_AE_SKIP_UNSAFE,
              "decide(600 vs 1000, equal max_ts) == SKIP_UNSAFE");
    }

    printf("\n[2] one-query peer map: time_bucket buckets align with partitions\n");
    {
        char qtl[256];
        snprintf(qtl, sizeof(qtl),
                 "SELECT time_bucket(ts, %lld), count(*) FROM t SAMPLE BY 1d",
                 (long long)DAY_NS);
        tsdb_result_t *r = NULL;
        CHECK(tsdb_query(A, qtl, &r) == TSDB_OK && r, "peer-map QTL executes");
        int rows = 0; long long bkt = -1, cnt = -1;
        if (r) {
            int ci_b = tsdb_result_col_index_by_name(r, "time_bucket");
            int ci_c = tsdb_result_col_index_by_name(r, "count");
            CHECK(ci_b >= 0 && ci_c >= 0, "peer-map result columns found by name");
            while (tsdb_result_next(r)) {
                if (rows == 0) {
                    bkt = (long long)tsdb_result_ts(r, ci_b);
                    cnt = tsdb_result_i64(r, ci_c);
                }
                rows++;
            }
            tsdb_result_free(r);
        }
        CHECK(rows == 1, "single day => single peer-map bucket");
        CHECK(bkt == pstart, "time_bucket start == partition pstart (alignment)");
        CHECK(cnt == 1000, "peer-map bucket count == partition rows");
    }

    printf("\n[3] backfill converges B's partition to A's copy\n");
    double sum_a = q_sum(A);
    {
        tsdb_result_t *res = pull_range(A, pstart, pend);
        uint64_t written = 0;
        int rc = tsdb_cluster_backfill_partition_from_result(B, "t", pname,
                                                             res, 600, &written);
        tsdb_result_free(res);
        CHECK(rc == TSDB_OK, "backfill swap returns TSDB_OK");
        CHECK(written == 1000, "backfill reports 1000 rows written");
    }
    CHECK(q_count(B) == 1000, "B count(*) == 1000 after swap");
    {
        double sum_b = q_sum(B);
        CHECK(sum_b > sum_a - 1e-6 && sum_b < sum_a + 1e-6,
              "B sum(val) matches A after swap");
        double v = 0; char tag[16] = "";
        CHECK(q_row(B, 700, &v, tag, sizeof(tag)) == 1,
              "middle-gap row 700 now present in B");
        CHECK(v == val_of(700), "row 700 val correct");
        CHECK(strcmp(tag, tag_of(700)) == 0, "row 700 symbol correct (re-interned)");
        struct stat st;
        char scratch[256];
        snprintf(scratch, sizeof(scratch), "%s/t/.aebf_tmp", DIR_B);
        CHECK(stat(scratch, &st) != 0, "scratch build dir cleaned up");
    }

    printf("\n[4] swapped partition survives reopen\n");
    tsdb_close(B);
    B = NULL;
    if (tsdb_open(DIR_B, &B) != TSDB_OK) { fprintf(stderr, "reopen B\n"); exit(1); }
    CHECK(q_count(B) == 1000, "B count(*) == 1000 after reopen");
    {
        double v = 0; char tag[16] = "";
        CHECK(q_row(B, 700, &v, tag, sizeof(tag)) == 1 &&
              v == val_of(700) && strcmp(tag, tag_of(700)) == 0,
              "row 700 still correct after reopen");
    }

    printf("\n[5] staleness guard aborts when the partition is written mid-flight\n");
    {
        /* A grows to 1050 so the pull stays strictly fuller than B (1000). */
        for (int i = 0; i < 50; i++) idx[i] = 1000 + i;
        write_rows(A, idx, 50);
        tsdb_db_flush_all(A);
        CHECK(q_count(A) == 1050, "A grown to 1050 rows");

        /* Caller snapshots B at 1000 rows...                              */
        tsdb_result_t *res = pull_range(A, pstart, pend);
        /* ...then a concurrent writer lands 5 rows + flush in the gap.    */
        for (int i = 0; i < 5; i++) idx[i] = 5000 + i;
        write_rows(B, idx, 5);
        tsdb_db_flush_all(B);
        CHECK(q_count(B) == 1005, "concurrent write flushed (B at 1005)");

        uint64_t written = 0;
        int rc = tsdb_cluster_backfill_partition_from_result(B, "t", pname,
                                                             res, 1000, &written);
        tsdb_result_free(res);
        CHECK(rc == TSDB_ERR_BUSY, "stale snapshot => swap aborts with TSDB_ERR_BUSY");
        CHECK(written == 0, "aborted swap reports 0 rows written");
        CHECK(q_count(B) == 1005, "table intact after aborted swap");
        double v = 0; char tag[16] = "";
        CHECK(q_row(B, 5002, &v, tag, sizeof(tag)) == 1 && v == val_of(5002),
              "concurrent write's rows intact after aborted swap");
        struct stat st;
        char scratch[256];
        snprintf(scratch, sizeof(scratch), "%s/t/.aebf_tmp", DIR_B);
        CHECK(stat(scratch, &st) != 0, "scratch cleaned after aborted swap");
    }

    printf("\n[6] a not-strictly-fuller pull is refused (never shrink)\n");
    {
        /* 600-row prefix of A vs B's live 1005 rows. */
        tsdb_result_t *res = pull_range(A, pstart, BASE_TS + 599 * STEP_NS);
        uint64_t written = 0;
        int rc = tsdb_cluster_backfill_partition_from_result(B, "t", pname,
                                                             res, 1005, &written);
        tsdb_result_free(res);
        CHECK(rc == TSDB_ERR_INVAL, "600-row pull vs 1005 local rows refused");
        CHECK(q_count(B) == 1005, "B untouched by refused pull");
    }

    tsdb_close(A);
    tsdb_close(B);
    rmrf(DIR_A); rmrf(DIR_B);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
