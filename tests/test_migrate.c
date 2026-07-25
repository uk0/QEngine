/* test_migrate.c — cross-cluster migration SDK: export -> import round trip.
 *
 * Pins the properties the SDK promises:
 *   1. Every row and value survives export+import, byte for byte.
 *   2. Blocks move COMPRESSED — the payload streamed is the on-disk block
 *      size, not the decoded size.
 *   3. Re-importing the same stream is idempotent (no duplicated rows), which
 *      is what makes an interrupted migration resumable.
 *   4. Importing into a table whose schema disagrees is refused, not written
 *      (landing addresses columns by index, so a mismatch would put one
 *      column's bytes in another's file).
 *   5. --rename lands the data under a different table name.
 */
#include "tsdb.h"
#include "tsdb_migrate.h"
#include "../src/storage/db.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define SRC    "/tmp/tsdb_test_migrate_src"
#define DST    "/tmp/tsdb_test_migrate_dst"
#define STREAM "/tmp/tsdb_test_migrate.stream"
#define DAY1   1700000000000000000LL
#define NROWS  40000

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static double val_at(int64_t i) { return (double)(i % 977) * 0.25 - 100.0; }

/* Fold the whole visible result set so a single number pins every value. */
static void scan(tsdb_db_t *db, const char *table, uint64_t *out_h, int64_t *out_n) {
    char q[128]; snprintf(q, sizeof(q), "SELECT ts, v FROM %s", table);
    tsdb_result_t *r = NULL;
    uint64_t h = 1469598103934665603ULL; int64_t n = 0;
    if (tsdb_query(db, q, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            double  v  = tsdb_result_f64(r, 1);
            uint64_t vb; memcpy(&vb, &v, sizeof(vb));
            h = (h ^ (uint64_t)ts) * 1099511628211ULL;
            h = (h ^ vb) * 1099511628211ULL;
            n++;
        }
        tsdb_result_free(r);
    }
    *out_h = h; *out_n = n;
}

static void build_source(void) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(SRC, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, "m", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));
    int64_t i = 0;
    while (i < NROWS) {
        int m = (NROWS - i < 1000) ? (int)(NROWS - i) : 1000;
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int k = 0; k < m; k++) {
            OK(tsdb_batch_row_ts(b, DAY1 + (i + k) * 1000000LL));
            OK(tsdb_batch_row_f64(b, 1, val_at(i + k)));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        i += m;
    }
    OK(tsdb_db_flush_all(db));
    tsdb_close(db);
}

int main(void) {
    printf("=== test_migrate ===\n");
    rm_rf(SRC); rm_rf(DST); remove(STREAM);

    build_source();

    /* ---- export ---- */
    tsdb_db_t *src = NULL;
    OK(tsdb_open(SRC, &src));
    uint64_t src_h; int64_t src_n;
    scan(src, "m", &src_h, &src_n);
    if (src_n != NROWS) FAIL("source has %lld rows, expected %d", (long long)src_n, NROWS);

    tsdb_mig_stats_t exp_st, src_dg;
    int fd = open(STREAM, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) FAIL("open stream for write");
    OK(tsdb_migrate_export(src, "m", fd, NULL, &exp_st));
    close(fd);
    OK(tsdb_migrate_digest(src, "m", &src_dg));
    tsdb_close(src);

    struct stat sb; stat(STREAM, &sb);
    printf("[export] parts=%llu blocks=%llu rows=%llu payload=%llu B  stream=%lld B\n",
           (unsigned long long)exp_st.partitions, (unsigned long long)exp_st.blocks,
           (unsigned long long)exp_st.rows, (unsigned long long)exp_st.payload_bytes,
           (long long)sb.st_size);
    if (exp_st.rows != NROWS) FAIL("exported %llu rows, expected %d",
                                   (unsigned long long)exp_st.rows, NROWS);
    if (exp_st.blocks == 0) FAIL("exported no blocks");

    /* Blocks travel COMPRESSED: 40k rows x 2 cols x 8 B raw = 640 kB. */
    uint64_t raw = (uint64_t)NROWS * 2 * 8;
    printf("[export] payload %llu B vs %llu B raw -> %.2fx compressed\n",
           (unsigned long long)exp_st.payload_bytes, (unsigned long long)raw,
           (double)raw / (double)exp_st.payload_bytes);
    if (exp_st.payload_bytes >= raw)
        FAIL("payload %llu >= raw %llu — blocks were not moved compressed",
             (unsigned long long)exp_st.payload_bytes, (unsigned long long)raw);

    /* ---- import ---- */
    tsdb_db_t *dst = NULL;
    OK(tsdb_open(DST, &dst));
    tsdb_mig_stats_t imp_st;
    fd = open(STREAM, O_RDONLY);
    if (fd < 0) FAIL("open stream for read");
    OK(tsdb_migrate_import(dst, fd, NULL, &imp_st));
    close(fd);
    printf("[import] blocks=%llu landed=%llu rows=%llu\n",
           (unsigned long long)imp_st.blocks,
           (unsigned long long)imp_st.blocks_landed,
           (unsigned long long)imp_st.rows);

    uint64_t dst_h; int64_t dst_n;
    scan(dst, "m", &dst_h, &dst_n);
    printf("[verify] src rows=%lld digest=%llu | dst rows=%lld digest=%llu\n",
           (long long)src_n, (unsigned long long)src_h,
           (long long)dst_n, (unsigned long long)dst_h);
    if (dst_n != src_n) FAIL("row count %lld != %lld", (long long)dst_n, (long long)src_n);
    if (dst_h != src_h) FAIL("value digest differs — data changed in transit");

    tsdb_mig_stats_t dst_dg;
    OK(tsdb_migrate_digest(dst, "m", &dst_dg));
    if (dst_dg.digest != src_dg.digest)
        FAIL("block digest differs: src=%llu dst=%llu",
             (unsigned long long)src_dg.digest, (unsigned long long)dst_dg.digest);
    printf("[verify] block digest matches (%llu)\n", (unsigned long long)src_dg.digest);

    /* ---- idempotence: replay the same stream ---- */
    fd = open(STREAM, O_RDONLY);
    OK(tsdb_migrate_import(dst, fd, NULL, NULL));
    close(fd);
    uint64_t h2; int64_t n2;
    scan(dst, "m", &h2, &n2);
    printf("[replay] rows=%lld (expect %lld)\n", (long long)n2, (long long)src_n);
    if (n2 != src_n) FAIL("re-import duplicated rows: %lld != %lld",
                          (long long)n2, (long long)src_n);
    if (h2 != src_h) FAIL("re-import changed values");

    /* ---- schema mismatch is refused ---- */
    {
        tsdb_col_t bad[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_INT64     },   /* wrong type */
        };
        OK(tsdb_create_table(dst, "clash", bad, 2, "ts"));
        tsdb_mig_opts_t o; memset(&o, 0, sizeof(o));
        o.rename_to = "clash";
        fd = open(STREAM, O_RDONLY);
        int rc = tsdb_migrate_import(dst, fd, &o, NULL);
        close(fd);
        printf("[schema] mismatched import rc=%d (%s)\n", rc, tsdb_errstr(rc));
        if (rc == TSDB_OK) FAIL("import into a mismatched schema must be refused");
    }

    /* ---- rename ---- */
    {
        tsdb_mig_opts_t o; memset(&o, 0, sizeof(o));
        o.rename_to = "renamed";
        fd = open(STREAM, O_RDONLY);
        OK(tsdb_migrate_import(dst, fd, &o, NULL));
        close(fd);
        uint64_t rh; int64_t rn;
        scan(dst, "renamed", &rh, &rn);
        printf("[rename] rows=%lld digest=%llu\n", (long long)rn, (unsigned long long)rh);
        if (rn != src_n || rh != src_h) FAIL("renamed import did not match source");
    }

    tsdb_close(dst);
    rm_rf(SRC); rm_rf(DST); remove(STREAM);
    printf("\n=== test_migrate PASSED ===\n");
    return 0;
}
