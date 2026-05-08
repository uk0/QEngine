/* bench_layout_compress.c — on-disk size measurement for the (d)
 * block-layout reorder.  Runs the same TSBS-shape ingest twice — once
 * with sort_by_tag_col=-1 (default) and once with sort_by_tag_col set
 * to the host column via TSDB_BLOCK_SORT_BY_TAG — and prints per-column
 * .col file sizes plus a delta summary.
 *
 * Usage:
 *   ./build/bench/bench_layout_compress <scale> <ticks>
 *   ./build/bench/bench_layout_compress              (defaults: 100, 4320)
 *
 * scale  — number of distinct hosts (column cardinality of `host`)
 * ticks  — number of timestamps; total rows = scale * ticks
 *
 * Captures the empirical answer to "does tag-sort actually compress
 * better on TSBS-shape data?".  Spoiler from 2026-05-09 measurement:
 * value columns shrink ~7% but ts.col DOD blows up ~800× because
 * within-block ts is no longer monotone.  Net is a wash on canonical
 * TSBS — see docs/tasks/perf-bcd-2026-05-08.md.  This harness exists
 * so future codec changes (e.g. ts.col encoder that survives tag-sort)
 * can be measured the same way before claiming a win.
 */

#include "tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

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

#define DIE(fmt, ...) do { fprintf(stderr, "FATAL: " fmt "\n", ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) DIE("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)

static int64_t file_size(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 ? (int64_t)st.st_size : -1;
}

/* Recursively sum sizes of all *.col files under dir. */
static int64_t total_col_size(const char *dir) {
    int64_t total = 0;
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        struct stat st;
        if (stat(p, &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) total += total_col_size(p);
        else if (S_ISREG(st.st_mode)) {
            const char *ext = strrchr(e->d_name, '.');
            if (ext && strcmp(ext, ".col") == 0) total += (int64_t)st.st_size;
        }
    }
    closedir(d);
    return total;
}

/* Print per-column sizes for the cpu table directory. */
static void dump_col_sizes(const char *db_dir, const char *label) {
    char tdir[4096]; snprintf(tdir, sizeof(tdir), "%s/cpu", db_dir);
    /* TSBS lays out partitions under tdir/<YYYYMMDD>/<col>.col.
     * Aggregate per column name across days. */
    DIR *d = opendir(tdir);
    if (!d) { printf("  %s: no cpu dir\n", label); return; }
    /* Two-pass: first collect (name -> sum) for partitions. */
    typedef struct { char name[64]; int64_t total; } col_agg_t;
    col_agg_t aggs[64]; int naggs = 0;

    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char part_dir[4096]; snprintf(part_dir, sizeof(part_dir), "%s/%s", tdir, e->d_name);
        struct stat st;
        if (stat(part_dir, &st) != 0 || !S_ISDIR(st.st_mode)) continue;

        DIR *pd = opendir(part_dir);
        if (!pd) continue;
        struct dirent *pe;
        while ((pe = readdir(pd))) {
            const char *ext = strrchr(pe->d_name, '.');
            if (!ext || strcmp(ext, ".col") != 0) continue;
            char fp[4096]; snprintf(fp, sizeof(fp), "%s/%s", part_dir, pe->d_name);
            int64_t sz = file_size(fp);
            if (sz < 0) continue;

            char colname[64];
            size_t nlen = (size_t)(ext - pe->d_name);
            if (nlen >= sizeof(colname)) nlen = sizeof(colname) - 1;
            memcpy(colname, pe->d_name, nlen);
            colname[nlen] = '\0';

            int found = -1;
            for (int i = 0; i < naggs; i++) {
                if (strcmp(aggs[i].name, colname) == 0) { found = i; break; }
            }
            if (found < 0 && naggs < 64) {
                strncpy(aggs[naggs].name, colname, sizeof(aggs[naggs].name) - 1);
                aggs[naggs].name[sizeof(aggs[naggs].name) - 1] = '\0';
                aggs[naggs].total = 0;
                found = naggs++;
            }
            if (found >= 0) aggs[found].total += sz;
        }
        closedir(pd);
    }
    closedir(d);

    int64_t grand = 0;
    printf("  %s: per-column .col bytes\n", label);
    for (int i = 0; i < naggs; i++) {
        printf("    %-22s %12lld\n", aggs[i].name, (long long)aggs[i].total);
        grand += aggs[i].total;
    }
    printf("    %-22s %12lld\n", "[TOTAL]", (long long)grand);
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void run_ingest(const char *dir, int scale, int ticks, int sort_on,
                        int64_t *out_total_col, double *out_secs)
{
    rm_rf(dir);
    if (sort_on) setenv("TSDB_BLOCK_SORT_BY_TAG", "hostname", 1);
    else         unsetenv("TSDB_BLOCK_SORT_BY_TAG");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",               TSDB_TYPE_TIMESTAMP},
        {"hostname",         TSDB_TYPE_SYMBOL},
        {"region",           TSDB_TYPE_SYMBOL},
        {"usage_user",       TSDB_TYPE_FLOAT64},
        {"usage_system",     TSDB_TYPE_FLOAT64},
        {"usage_idle",       TSDB_TYPE_FLOAT64},
        {"usage_nice",       TSDB_TYPE_FLOAT64},
        {"usage_iowait",     TSDB_TYPE_FLOAT64},
        {"usage_irq",        TSDB_TYPE_FLOAT64},
        {"usage_softirq",    TSDB_TYPE_FLOAT64},
        {"usage_steal",      TSDB_TYPE_FLOAT64},
        {"usage_guest",      TSDB_TYPE_FLOAT64},
        {"usage_guest_nice", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "cpu", cols, 13, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "cpu", &t));

    static const char *REGIONS[] = {"us-east-1", "us-west-1", "eu-central-1", "ap-northeast-1"};
    char host_buf[16];

    /* Per-host random walk state. */
    double *walk = malloc((size_t)scale * 10 * sizeof(double));
    if (!walk) DIE("OOM walk[]");
    for (int h = 0; h < scale; h++)
        for (int m = 0; m < 10; m++) walk[h * 10 + m] = 50.0;

    uint64_t lcg = 0xC001D00DULL;
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_ts_t tick_ns  = 10LL * 1000000000LL;
    tsdb_ts_t sub_step = tick_ns / (tsdb_ts_t)scale;
    if (sub_step < 1) sub_step = 1;

    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    double t0 = now_sec();

    for (int tk = 0; tk < ticks; tk++) {
        tsdb_ts_t tb = base + (tsdb_ts_t)tk * tick_ns;
        for (int h = 0; h < scale; h++) {
            snprintf(host_buf, sizeof(host_buf), "host_%d", h);
            const char *region = REGIONS[h % 4];

            /* Update each metric's random walk. */
            for (int m = 0; m < 10; m++) {
                lcg = lcg * 6364136223846793005ULL + 1442695040888963407ULL;
                double s = ((double)(lcg >> 32) / (double)UINT32_MAX - 0.5) * 5.0;
                double v = walk[h * 10 + m] + s;
                if (v < 0.0)   v = 0.0;
                if (v > 100.0) v = 100.0;
                walk[h * 10 + m] = v;
            }

            OK(tsdb_batch_row_ts(b, tb + (tsdb_ts_t)h * sub_step));
            OK(tsdb_batch_row_sym(b, 1, host_buf));
            OK(tsdb_batch_row_sym(b, 2, region));
            for (int m = 0; m < 10; m++) {
                OK(tsdb_batch_row_f64(b, 3 + m, walk[h * 10 + m]));
            }
            OK(tsdb_batch_row_end(b));
        }
    }
    OK(tsdb_batch_commit(b));
    *out_secs = now_sec() - t0;

    tsdb_close(db);
    free(walk);

    *out_total_col = total_col_size(dir);
}

int main(int argc, char **argv) {
    int scale = 100;
    int ticks = 4320;  /* 12h × 360 ticks/h */
    if (argc >= 2) scale = atoi(argv[1]);
    if (argc >= 3) ticks = atoi(argv[2]);
    if (scale <= 0 || ticks <= 0) {
        fprintf(stderr, "usage: %s [scale] [ticks]\n", argv[0]);
        return 1;
    }
    int64_t rows = (int64_t)scale * ticks;
    printf("=== bench_layout_compress: scale=%d ticks=%d rows=%lld ===\n",
           scale, ticks, (long long)rows);

    char dir_off[256], dir_on[256];
    snprintf(dir_off, sizeof(dir_off), "/tmp/bench_layout_compress_off");
    snprintf(dir_on,  sizeof(dir_on),  "/tmp/bench_layout_compress_on");

    int64_t off_bytes = 0, on_bytes = 0;
    double  off_sec  = 0,  on_sec  = 0;

    printf("\nrun: off (sort_by_tag_col=-1) ...\n");
    run_ingest(dir_off, scale, ticks, /*sort_on=*/0, &off_bytes, &off_sec);
    printf("  total .col = %lld B  ingest=%.2fs (%.2f Mrows/s)\n",
           (long long)off_bytes, off_sec, (double)rows / off_sec / 1e6);
    dump_col_sizes(dir_off, "off");

    printf("\nrun: on  (sort_by_tag_col=hostname) ...\n");
    run_ingest(dir_on, scale, ticks, /*sort_on=*/1, &on_bytes, &on_sec);
    printf("  total .col = %lld B  ingest=%.2fs (%.2f Mrows/s)\n",
           (long long)on_bytes, on_sec, (double)rows / on_sec / 1e6);
    dump_col_sizes(dir_on, "on");

    printf("\n=== summary ===\n");
    printf("  off total = %lld B  (%.2f B/point across all 13 cols)\n",
           (long long)off_bytes, (double)off_bytes / (double)rows);
    printf("  on  total = %lld B  (%.2f B/point across all 13 cols)\n",
           (long long)on_bytes,  (double)on_bytes  / (double)rows);
    int64_t delta = off_bytes - on_bytes;
    double pct = (double)delta / (double)off_bytes * 100.0;
    printf("  delta     = %lld B  (%.2f%% saved by tag-sort)\n",
           (long long)delta, pct);

    rm_rf(dir_off);
    rm_rf(dir_on);
    unsetenv("TSDB_BLOCK_SORT_BY_TAG");
    return 0;
}
