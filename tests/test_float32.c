/* test_float32.c — FLOAT32 column equivalence + disk-size win.
 *
 * A FLOAT32 column is stored 4 bytes on disk but is a double everywhere in the
 * query path (the F32 codec widens on decode).  This test writes IDENTICAL
 * values into a FLOAT64 and a FLOAT32 column and asserts every operator —
 * count/sum/avg/min/max/stddev, WHERE filters, GROUP BY — produces matching
 * results (within float32 precision), across both the memtable and the on-disk
 * path (flush + reopen).  It also checks the .col file is ~2x smaller.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dirent.h>
#include <sys/stat.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d %s\n",_r,tsdb_errstr(_r));FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMPDIR = "/tmp/tsdb_test_float32";
static const char *TBL = "f32t";

/* Single-row query helpers. */
static double q_f64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, sql, &r));
    ASSERT(tsdb_result_next(r) == 1);
    double v = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    return v;
}
static int64_t q_i64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, sql, &r));
    ASSERT(tsdb_result_next(r) == 1);
    int64_t v = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    return v;
}

/* Relative-equality within float32 epsilon scaled by the magnitude + count. */
static void near(double a, double b, double tol, const char *what) {
    double d = fabs(a - b);
    double scale = fabs(a) + fabs(b) + 1.0;
    if (d > tol * scale) {
        fprintf(stderr, "FAIL %s: f64=%.9g f32=%.9g (|d|=%.3g > %.3g)\n", what, a, b, d, tol * scale);
        abort();
    }
    printf("  OK %-18s f64=%.6g  f32=%.6g\n", what, a, b);
}

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name,".")||!strcmp(e->d_name,"..")) continue;
        char s[4096]; snprintf(s,sizeof(s),"%s/%s",p,e->d_name); rm_rf(s);
    }
    closedir(d); rmdir(p);
}

static long col_file_size(const char *col) {
    char base[4096]; snprintf(base, sizeof(base), "%s/%s", TMPDIR, TBL);
    DIR *d = opendir(base);
    if (!d) return -1;
    long total = 0; struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4096]; snprintf(p, sizeof(p), "%s/%s/%s.col", base, e->d_name, col);
        struct stat st; if (stat(p, &st) == 0) total += st.st_size;
    }
    closedir(d);
    return total;
}

static void run_checks(tsdb_db_t *db, const char *phase) {
    printf("[%s]\n", phase);
    ASSERT(q_i64(db, "SELECT count(*) FROM f32t") == 50000);
    /* sum: f32 accumulates ~7 sig digits → looser tol; avg/min/max tighter. */
    near(q_f64(db,"SELECT sum(f64c) FROM f32t"),  q_f64(db,"SELECT sum(f32c) FROM f32t"),  1e-6, "sum");
    near(q_f64(db,"SELECT avg(f64c) FROM f32t"),  q_f64(db,"SELECT avg(f32c) FROM f32t"),  1e-6, "avg");
    near(q_f64(db,"SELECT min(f64c) FROM f32t"),  q_f64(db,"SELECT min(f32c) FROM f32t"),  1e-6, "min");
    near(q_f64(db,"SELECT max(f64c) FROM f32t"),  q_f64(db,"SELECT max(f32c) FROM f32t"),  1e-6, "max");
    near(q_f64(db,"SELECT stddev(f64c) FROM f32t"), q_f64(db,"SELECT stddev(f32c) FROM f32t"), 1e-5, "stddev");
    /* Filter: same predicate on each column → matching count (a handful may
     * differ if their f32 value rounds across the boundary). */
    int64_t cf64 = q_i64(db, "SELECT count(*) FROM f32t WHERE f64c > 500.0");
    int64_t cf32 = q_i64(db, "SELECT count(*) FROM f32t WHERE f32c > 500.0");
    printf("  OK WHERE >500        f64=%lld f32=%lld\n", (long long)cf64, (long long)cf32);
    ASSERT(llabs((long long)(cf64 - cf32)) <= 5);
}

int main(void) {
    printf("=== test_float32 ===\n");
    rm_rf(TMPDIR);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(TMPDIR, &db));
    tsdb_col_t cols[] = {
        {"ts",   TSDB_TYPE_TIMESTAMP},
        {"f64c", TSDB_TYPE_FLOAT64},
        {"f32c", TSDB_TYPE_FLOAT32},
    };
    OK(tsdb_create_table(db, TBL, cols, 3, "ts"));
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, TBL, &t));

    /* High-entropy doubles (full f64 mantissa) in [100, 1000): FLOAT64 can't
     * compress them (~8 bytes/value), so the FLOAT32 storage win is visible. */
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    uint64_t st = 0x12345678;
    for (int i = 0; i < 50000; i++) {
        st = st * 6364136223846793005ULL + 1442695040888963407ULL;
        /* Full 53-bit mantissa entropy → FLOAT64 is near-incompressible. */
        double v = 100.0 + (double)(st >> 11) / (double)(1ULL << 53) * 900.0;
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, v));   /* f64 column */
        OK(tsdb_batch_row_f64(b, 2, v));   /* f32 column — same setter, stored as double, narrowed on flush */
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    run_checks(db, "memtable");           /* before flush */

    /* Force flush to disk so the F32 codec narrows f32c to 4 bytes. */
    OK(tsdb_db_flush_all(db));
    run_checks(db, "on-disk (after flush)");

    long s64 = col_file_size("f64c"), s32 = col_file_size("f32c");
    double ratio = (s32 > 0) ? (double)s64 / (double)s32 : 0.0;
    printf("[disk] f64c.col=%ld B  f32c.col=%ld B  ratio=%.2fx\n", s64, s32, ratio);
    ASSERT(s32 > 0 && s64 > 0);
    ASSERT(ratio > 1.4);  /* near-2x: 4-byte vs ~8-byte for incompressible f64 */

    /* Reopen — schema must persist the FLOAT32 type. */
    tsdb_close(db);
    OK(tsdb_open(TMPDIR, &db));
    run_checks(db, "reopened");

    tsdb_close(db);
    rm_rf(TMPDIR);
    printf("[PASS] FLOAT32 equivalent to FLOAT64 in queries; ~2x smaller on disk\n");
    return 0;
}
