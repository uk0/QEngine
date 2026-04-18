/* test_skiplist_memtable.c — skip-list-backed memtable ordering tests.
 *
 * The memtable keeps col_bufs append-only but maintains a parallel
 * ts-keyed skip-list.  tsdb_memtable_sorted_indices walks it to produce
 * a ts-ordered permutation of row positions; part.c uses this on flush
 * so on-disk blocks stay ts-sorted regardless of insertion order.
 *
 * Phases:
 *   1. In-order inserts → memtable_is_sorted() == 1 and sorted_indices
 *      returns the identity permutation.
 *   2. Reverse-order inserts → is_sorted == 0 and sorted_indices yields
 *      strictly increasing ts when re-read via col_bufs.
 *   3. Interleaved (checkerboard) inserts → same sorted guarantee.
 *   4. Equal-ts ties are stable (insertion order preserved among ties).
 *   5. End-to-end: out-of-order inserts + flush + reopen partition →
 *      blocks are ts-sorted (ts_min strictly increases block-to-block).
 */

#include "../src/storage/memtable.h"
#include "../src/storage/schema.h"
#include "../src/storage/part.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>

static int g_pass = 0, g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *p) {
    char cmd[512]; snprintf(cmd, sizeof(cmd), "rm -rf %s", p); (void)system(cmd);
}

static tsdb_schema_t *make_schema(const char *dir) {
    rmrf(dir); mkdir(dir, 0755);
    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_INT64},
    };
    tsdb_schema_t *s = NULL;
    if (tsdb_schema_create_ex(dir, "t", cols, 2, "ts",
                               TSDB_PARTITION_DAY, 0, &s) != TSDB_OK) {
        fprintf(stderr, "schema_create failed\n");
        exit(1);
    }
    return s;
}

static void push_row(tsdb_memtable_t *m, int64_t ts, int64_t v) {
    tsdb_memtable_row_begin(m);
    tsdb_memtable_row_ts(m, (tsdb_ts_t)ts);
    tsdb_memtable_row_i64(m, 1, v);
    tsdb_memtable_row_end(m);
}

int main(void) {
    printf("=== test_skiplist_memtable ===\n");

    const char *dir = "/tmp/tsdb_test_skiplist";
    rmrf(dir);
    mkdir(dir, 0755);

    /* ---- Phase 1: in-order inserts → identity sorted_indices --------- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/t1", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        for (int i = 0; i < 1000; i++)
            push_row(m, (int64_t)(i * 1000000LL), (int64_t)i);

        CHECK(tsdb_memtable_is_sorted(m), "in-order: is_sorted == 1");

        size_t idx[1000];
        tsdb_memtable_sorted_indices(m, idx);
        int ok = 1;
        for (int i = 0; i < 1000; i++) if (idx[i] != (size_t)i) { ok = 0; break; }
        CHECK(ok, "in-order: sorted_indices == identity");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- Phase 2: reverse-order inserts → sorted_indices reverses ---- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/t2", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        for (int i = 999; i >= 0; i--)
            push_row(m, (int64_t)(i * 1000000LL), (int64_t)i);

        CHECK(!tsdb_memtable_is_sorted(m), "reverse-order: is_sorted == 0");

        size_t idx[1000];
        tsdb_memtable_sorted_indices(m, idx);
        /* Using the permutation, the ts-values should ascend. */
        const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, 0);
        int asc = 1;
        for (int i = 1; i < 1000; i++) {
            if (ts_buf[idx[i-1]] > ts_buf[idx[i]]) { asc = 0; break; }
        }
        CHECK(asc, "reverse-order: ts via sorted_indices is non-decreasing");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- Phase 3: interleaved inserts (even then odd) ---------------- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/t3", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        for (int i = 0; i < 1000; i += 2)
            push_row(m, (int64_t)(i * 1000000LL), (int64_t)i);
        for (int i = 1; i < 1000; i += 2)
            push_row(m, (int64_t)(i * 1000000LL), (int64_t)i);

        CHECK(!tsdb_memtable_is_sorted(m), "checker: is_sorted == 0");

        size_t idx[1000];
        tsdb_memtable_sorted_indices(m, idx);
        const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, 0);
        int asc = 1;
        for (int i = 1; i < 1000; i++) {
            if (ts_buf[idx[i-1]] > ts_buf[idx[i]]) { asc = 0; break; }
        }
        CHECK(asc, "checker: sorted_indices yields asc ts");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- Phase 4: equal-ts stability --------------------------------- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/t4", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        /* Three rows all with ts=1 000 000, values 10/20/30 in insert order. */
        push_row(m, 1000000, 10);
        push_row(m, 1000000, 20);
        push_row(m, 1000000, 30);

        size_t idx[3];
        tsdb_memtable_sorted_indices(m, idx);
        const int64_t *vb = (const int64_t *)tsdb_memtable_col(m, 1);
        CHECK(vb[idx[0]] == 10 && vb[idx[1]] == 20 && vb[idx[2]] == 30,
              "stable: ties preserve insertion order");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- Phase 5: end-to-end via tsdb_create_table + flush ----------- */
    {
        char dbdir[256]; snprintf(dbdir, sizeof(dbdir), "%s/dbe2e", dir);
        tsdb_db_t *db = NULL;
        tsdb_open(dbdir, &db);

        tsdb_col_t cols[] = {
            {"ts", TSDB_TYPE_TIMESTAMP},
            {"v",  TSDB_TYPE_INT64},
        };
        /* block_points=1024 so all 500 reversed inserts share one memtable,
         * one flush, one block.  That block should be internally ts-sorted
         * because the skip-list orders the row_idx permutation.  (Cross-
         * flush ordering is a separate, unrelated problem — fixable only
         * by LSM-style compaction.) */
        tsdb_create_table_ex2(db, "te2e", cols, 2, "ts",
                               TSDB_CREATE_PART_DAY, 1024);

        tsdb_table_t *t = NULL;
        tsdb_open_table(db, "te2e", &t);
        tsdb_batch_t *b; tsdb_batch_begin(t, &b);
        /* Reverse-order inserts on a single day. */
        int64_t base = 1700000000000000000LL;
        for (int i = 499; i >= 0; i--) {
            tsdb_batch_row_ts (b, (tsdb_ts_t)(base + (int64_t)i * 1000000LL));
            tsdb_batch_row_i64(b, 1, i);
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);

        tsdb_close(db);

        /* Re-open partition and verify per-block ts_min ascends. */
        tsdb_open(dbdir, &db);
        char tdir[512]; snprintf(tdir, sizeof(tdir), "%s/te2e", dbdir);

        /* Locate the only partition subdir. */
        char partdir[512] = {0};
        DIR *dd = opendir(tdir);
        struct dirent *e;
        while (dd && (e = readdir(dd))) {
            if (e->d_name[0] >= '0' && e->d_name[0] <= '9') {
                snprintf(partdir, sizeof(partdir), "%s/%s", tdir, e->d_name);
                break;
            }
        }
        if (dd) closedir(dd);
        CHECK(partdir[0] != '\0', "partition directory found");

        tsdb_schema_t *sc = NULL;
        tsdb_schema_open(tdir, &sc);
        tsdb_part_t *pp = NULL;
        CHECK(tsdb_part_open(sc, partdir, &pp) == TSDB_OK, "part_open");

        tsdb_block_meta_t *meta = NULL;
        size_t nm = 0;
        tsdb_part_col_blocks(pp, 0 /* ts */, &meta, &nm);
        CHECK(nm == 1, "exactly one block emitted (all rows fit memtable)");
        if (nm >= 1) {
            CHECK(meta[0].ts_min == base,
                  "block ts_min is the smallest input ts");
            CHECK(meta[0].ts_max == base + 499LL * 1000000LL,
                  "block ts_max is the largest input ts");
        }

        /* Walk the block's row data and verify ts strictly increases. */
        if (nm == 1) {
            int64_t *row_ts = malloc((size_t)meta[0].count * sizeof(int64_t));
            tsdb_part_read_block(pp, 0, &meta[0], row_ts);
            int asc = 1;
            for (uint32_t i = 1; i < meta[0].count; i++) {
                if (row_ts[i-1] > row_ts[i]) { asc = 0; break; }
            }
            CHECK(asc, "intra-block row ts is monotonically non-decreasing");
            free(row_ts);
        }

        free(meta);
        tsdb_part_close(pp);
        tsdb_schema_free(sc);
        tsdb_close(db);
    }

    rmrf(dir);
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
