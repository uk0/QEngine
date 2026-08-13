/* test_query_silent_partial.c — three query paths that answered WRONG with rc=0.
 *
 * Every case here is a silent wrong answer: no error, no NULL, no short read
 * the caller can notice.  A dashboard charts the number and nobody can tell it
 * apart from the truth.
 *
 * [1] QC-8  scan_plan_build_ex dropped an unopenable partition from the plan.
 *           tsdb_part_open reports an unparseable index as TSDB_ERR_CORRUPT —
 *           data that EXISTS but cannot be read — and the loop swallowed
 *           everything that was not TSDB_ERR_IO / TSDB_ERR_NOMEM, so the
 *           partition simply vanished and count(*) came back short at rc=0.
 *
 * [2] QC-10 `SELECT time_bucket(ts, N), count(*) FROM t GROUP BY <tag>` passed
 *           the GROUP BY validator (it only inspects PROJ_COL) and then hit
 *           agg_write's `default: return`, which stores NOTHING.  The result
 *           buffers come from realloc and are never zeroed, so the bucket
 *           column was whatever the allocator last left there — uninitialised
 *           heap charted as timestamps, rc=0.
 *
 * [3] QC-5  twa() under PARALLEL GROUP BY.  Each worker accumulates its own
 *           slice and gb_merge_into just adds the weighted sums, so every
 *           interval that spans a slice boundary is dropped from the numerator
 *           while the denominator (ts_last - ts_first) still spans the whole
 *           query.  Serial and parallel disagree by orders of magnitude for the
 *           same query.  The same merge also drops the SYMBOL arm of
 *           first()/last() (ts_first_u32 / ts_last_u32 are never copied).
 *
 * Each phase asserts something the BROKEN code cannot produce: an error return
 * where it returned rc=0, or agreement between two execution modes that
 * disagreed.
 */

#include "../include/tsdb.h"
#include "../src/query/exec.h"

#include <dirent.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

extern int tsdb_db_flush_all(tsdb_db_t *db);

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    exit(1); } while (0)
#define OK(rc) do { int _rc = (rc); \
    if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

#define DAY_NS 86400000000000LL
#define BASE_TS (1700000000000000000LL / DAY_NS * DAY_NS)   /* a day boundary */

/* ---- helpers ------------------------------------------------------------ */

static void rm_rf(const char *path) {
    DIR *d = opendir(path);
    if (!d) { remove(path); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", path, e->d_name);
        rm_rf(p);
    }
    closedir(d); rmdir(path);
}

static tsdb_db_t *open_fresh(const char *dir) {
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    return db;
}

/* Find every partition directory below `root` that holds a ts.idx, sorted by
 * path.  Partition dirs are 8-digit (DAY) or 10-digit (HOUR) names. */
static int collect_parts(const char *root, char out[][4096], int cap) {
    DIR *d = opendir(root);
    if (!d) return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) && n < cap) {
        if (e->d_name[0] == '.') continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s", root, e->d_name);
        struct stat st;
        if (stat(p, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        char idx[4200];
        snprintf(idx, sizeof(idx), "%s/ts.idx", p);
        if (access(idx, R_OK) == 0) {
            snprintf(out[n], 4096, "%s", p);
            n++;
        } else {
            n += collect_parts(p, out + n, cap - n);
        }
    }
    closedir(d);
    /* Insertion sort: at most a handful of partitions. */
    for (int i = 1; i < n; i++) {
        char tmp[4096];
        snprintf(tmp, sizeof(tmp), "%s", out[i]);
        int j = i - 1;
        while (j >= 0 && strcmp(out[j], tmp) > 0) {
            snprintf(out[j + 1], 4096, "%s", out[j]);
            j--;
        }
        snprintf(out[j + 1], 4096, "%s", tmp);
    }
    return n;
}

static int64_t count_star(tsdb_db_t *db, int *out_rc) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "SELECT count(*) FROM t", &r);
    *out_rc = rc;
    int64_t v = -1;
    if (rc == TSDB_OK && r) {
        if (tsdb_result_next(r) == 1) v = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
    }
    return v;
}

/* ---- [1] QC-8: an unreadable partition must not vanish from the answer --- */

static void phase_dropped_partition(void) {
    printf("\n[1] QC-8 unopenable partition must not silently vanish\n");
    const char *dir = "/tmp/tsdb_qsp_drop";
    tsdb_db_t *db = open_fresh(dir);

    tsdb_col_t cols[] = {
        {"ts",   TSDB_TYPE_TIMESTAMP},
        {"host", TSDB_TYPE_SYMBOL},
        {"v",    TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 3, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    const int ndays = 3, per_day = 10;
    for (int d = 0; d < ndays; d++) {
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(tbl, &b));
        for (int i = 0; i < per_day; i++) {
            OK(tsdb_batch_row_ts(b, BASE_TS + d * DAY_NS + i * 1000000LL));
            OK(tsdb_batch_row_sym(b, 1, "h1"));
            OK(tsdb_batch_row_f64(b, 2, 1.0));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        OK(tsdb_db_flush_all(db));
    }

    int rc = 0;
    int64_t total = count_star(db, &rc);
    OK(rc);
    printf("  healthy count(*) = %lld (expect %d)\n",
           (long long)total, ndays * per_day);
    ASSERT(total == (int64_t)(ndays * per_day));

    char parts[64][4096];
    int np = collect_parts(dir, parts, 64);
    printf("  partitions on disk: %d\n", np);
    ASSERT(np == ndays);

    /* Damage the MIDDLE partition's ts index header so read_idx_header_ex
     * rejects it; tsdb_part_open then returns TSDB_ERR_CORRUPT. */
    char idx[4200];
    snprintf(idx, sizeof(idx), "%s/ts.idx", parts[1]);
    FILE *f = fopen(idx, "r+b");
    ASSERT(f != NULL);
    unsigned char junk[4] = { 0xDE, 0xAD, 0xBE, 0xEF };
    ASSERT(fwrite(junk, 1, 4, f) == 4);
    fclose(f);
    printf("  corrupted %s\n", idx);

    int64_t after = count_star(db, &rc);
    printf("  after corruption: rc=%d count=%lld\n", rc, (long long)after);
    if (rc == TSDB_OK && after < total) {
        FAIL("silent partial answer: count(*) fell %lld -> %lld with rc=0",
             (long long)total, (long long)after);
    }
    ASSERT(rc != TSDB_OK);
    printf("  ok: the unreadable partition surfaced as rc=%d (%s)\n",
           rc, tsdb_errstr(rc));

    tsdb_close(db);
    rm_rf(dir);
}

/* ---- [2] QC-10: time_bucket() cell in a tag GROUP BY -------------------- */

static void phase_bucket_in_group_by(void) {
    printf("\n[2] QC-10 time_bucket() projection under GROUP BY <tag>\n");
    const char *dir = "/tmp/tsdb_qsp_bucket";
    tsdb_db_t *db = open_fresh(dir);

    tsdb_col_t cols[] = {
        {"ts",   TSDB_TYPE_TIMESTAMP},
        {"host", TSDB_TYPE_SYMBOL},
        {"v",    TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 3, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(tbl, &b));
    for (int i = 0; i < 200; i++) {
        OK(tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * 1000000000LL));
        OK(tsdb_batch_row_sym(b, 1, (i % 2) ? "h1" : "h2"));
        OK(tsdb_batch_row_f64(b, 2, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));

    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db,
        "SELECT time_bucket(ts, 60000000000), count(*) FROM t GROUP BY host", &r);
    printf("  rc=%d\n", rc);
    if (rc == TSDB_OK) {
        /* The cell agg_write never wrote.  Print it so the failure shows the
         * uninitialised bytes rather than only the missing error. */
        while (r && tsdb_result_next(r) == 1) {
            printf("  bucket cell = %lld  count = %lld\n",
                   (long long)tsdb_result_i64(r, 0),
                   (long long)tsdb_result_i64(r, 1));
        }
        if (r) tsdb_result_free(r);
        FAIL("time_bucket() with a tag GROUP BY returned rc=0 and an "
             "unwritten bucket column");
    }
    if (r) tsdb_result_free(r);
    printf("  ok: rejected with rc=%d (%s)\n", rc, tsdb_errstr(rc));

    /* The neighbouring shapes must keep working. */
    OK(tsdb_query(db, "SELECT host, count(*) FROM t GROUP BY host", &r));
    ASSERT(r && tsdb_result_nrows(r) == 2);
    tsdb_result_free(r);
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 60000000000), count(*) FROM t SAMPLE BY 1m", &r));
    ASSERT(r && tsdb_result_nrows(r) > 0);
    tsdb_result_free(r);
    printf("  ok: GROUP BY host and SAMPLE BY 1m still answer\n");

    tsdb_close(db);
    rm_rf(dir);
}

/* ---- [3] QC-5: twa()/first()/last() under parallel GROUP BY ------------- */

/* Read a two-column GROUP BY result into (host -> raw 8-byte cell). */
typedef struct { char host[16]; uint64_t bits; } grp_t;

static int read_groups(tsdb_db_t *db, const char *qtl, grp_t *out, int cap) {
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, qtl, &r));
    ASSERT(r != NULL);
    int n = 0;
    while (tsdb_result_next(r) == 1 && n < cap) {
        const char *h = tsdb_result_sym(r, 0);
        snprintf(out[n].host, sizeof(out[n].host), "%s", h ? h : "?");
        const void *cp = tsdb_result_col_ptr(r, 1);
        ASSERT(cp != NULL);
        memcpy(&out[n].bits, (const uint64_t *)cp + n, 8);
        n++;
    }
    tsdb_result_free(r);
    return n;
}

static uint64_t group_bits(const grp_t *g, int n, const char *host) {
    for (int i = 0; i < n; i++)
        if (!strcmp(g[i].host, host)) return g[i].bits;
    FAIL("group '%s' missing from result", host);
    return 0;
}

static void phase_parallel_ts_agg(void) {
    printf("\n[3] QC-5 twa()/first() must not depend on parallelism\n");
    const char *dir = "/tmp/tsdb_qsp_twa";
    tsdb_db_t *db = open_fresh(dir);

    tsdb_col_t cols[] = {
        {"ts",   TSDB_TYPE_TIMESTAMP},
        {"host", TSDB_TYPE_SYMBOL},
        {"tag",  TSDB_TYPE_SYMBOL},
        {"v",    TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 4, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* One partition per day, one row per host per partition.  Every twa
     * interval therefore spans a partition — and so a worker-slice — boundary,
     * which is exactly what the additive merge drops.  hA is present in every
     * partition; hB only in the second half, so its group is INSERTED by the
     * merge rather than found, which is the case where first()'s symbol arm is
     * lost. */
    const int ndays = 8;
    for (int d = 0; d < ndays; d++) {
        tsdb_batch_t *b = NULL;
        char tag[16];
        snprintf(tag, sizeof(tag), "t%d", d);
        OK(tsdb_batch_begin(tbl, &b));
        OK(tsdb_batch_row_ts(b, BASE_TS + (int64_t)d * DAY_NS));
        OK(tsdb_batch_row_sym(b, 1, "hA"));
        OK(tsdb_batch_row_sym(b, 2, tag));
        OK(tsdb_batch_row_f64(b, 3, 1.0));
        OK(tsdb_batch_row_end(b));
        if (d >= ndays / 2) {
            OK(tsdb_batch_row_ts(b, BASE_TS + (int64_t)d * DAY_NS + 1));
            OK(tsdb_batch_row_sym(b, 1, "hB"));
            OK(tsdb_batch_row_sym(b, 2, tag));
            OK(tsdb_batch_row_f64(b, 3, 2.0));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        OK(tsdb_db_flush_all(db));
    }

    char parts[64][4096];
    int np = collect_parts(dir, parts, 64);
    printf("  partitions on disk: %d\n", np);
    ASSERT(np == ndays);

    const char *q_twa   = "SELECT host, twa(v) FROM t GROUP BY host";
    const char *q_first = "SELECT host, first(tag) FROM t GROUP BY host";

    grp_t ser_twa[8], par_twa[8], ser_first[8], par_first[8];
    int n1, n2, n3, n4;

    tsdb_set_query_parallel(0);
    n1 = read_groups(db, q_twa,   ser_twa,   8);
    n3 = read_groups(db, q_first, ser_first, 8);

    tsdb_set_query_pool_size(8);
    tsdb_set_query_parallel(1);
    n2 = read_groups(db, q_twa,   par_twa,   8);
    n4 = read_groups(db, q_first, par_first, 8);
    tsdb_set_query_parallel(0);

    ASSERT(n1 == 2 && n2 == 2 && n3 == 2 && n4 == 2);

    double sA, pA, sB, pB;
    uint64_t bits;
    bits = group_bits(ser_twa, n1, "hA"); memcpy(&sA, &bits, 8);
    bits = group_bits(par_twa, n2, "hA"); memcpy(&pA, &bits, 8);
    bits = group_bits(ser_twa, n1, "hB"); memcpy(&sB, &bits, 8);
    bits = group_bits(par_twa, n2, "hB"); memcpy(&pB, &bits, 8);

    printf("  twa(v) hA: serial=%.9f parallel=%.9f\n", sA, pA);
    printf("  twa(v) hB: serial=%.9f parallel=%.9f\n", sB, pB);

    /* v is constant per host, so the time-weighted average IS that constant
     * whatever the point spacing. */
    if (fabs(sA - 1.0) > 1e-9) FAIL("serial twa(hA) = %.9f, want 1.0", sA);
    if (fabs(sB - 2.0) > 1e-9) FAIL("serial twa(hB) = %.9f, want 2.0", sB);
    if (fabs(sA - pA) > 1e-9)
        FAIL("twa(hA) depends on parallelism: serial=%.9f parallel=%.9f", sA, pA);
    if (fabs(sB - pB) > 1e-9)
        FAIL("twa(hB) depends on parallelism: serial=%.9f parallel=%.9f", sB, pB);

    uint64_t fsA = group_bits(ser_first, n3, "hA");
    uint64_t fpA = group_bits(par_first, n4, "hA");
    uint64_t fsB = group_bits(ser_first, n3, "hB");
    uint64_t fpB = group_bits(par_first, n4, "hB");
    printf("  first(tag) hA: serial=%llu parallel=%llu\n",
           (unsigned long long)fsA, (unsigned long long)fpA);
    printf("  first(tag) hB: serial=%llu parallel=%llu\n",
           (unsigned long long)fsB, (unsigned long long)fpB);
    if (fsA != fpA)
        FAIL("first(tag) hA depends on parallelism: %llu vs %llu",
             (unsigned long long)fsA, (unsigned long long)fpA);
    if (fsB != fpB)
        FAIL("first(tag) hB depends on parallelism: %llu vs %llu",
             (unsigned long long)fsB, (unsigned long long)fpB);
    printf("  ok: serial and parallel agree\n");

    tsdb_close(db);
    rm_rf(dir);
}

int main(void) {
    printf("=== query silent-partial results ===\n");
    phase_dropped_partition();
    phase_bucket_in_group_by();
    phase_parallel_ts_agg();
    printf("\nALL PASS\n");
    return 0;
}
