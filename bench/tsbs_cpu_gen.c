/* tsbs_cpu_gen.c — TSBS cpu-only workload data generator.
 *
 * Usage: tsbs_cpu_gen <db_dir> <scale> <hours>
 *   scale = number of hosts (e.g., 100)
 *   hours = time window in hours (e.g., 4)
 *
 * Generates TSBS-aligned cpu-only data:
 *   - N hosts, 10-second tick interval
 *   - 10 CPU metrics per row using random walk
 *   - Interleaved time ordering: t0:h0..hN, t0+10:h0..hN, ...
 *   - Timestamps strictly ascending via host_id nanosecond offset
 */
#include "tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <stdint.h>

/* ---- helpers ----------------------------------------------------------- */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static uint64_t dir_size(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        struct stat st;
        return (stat(path, &st) == 0) ? (uint64_t)st.st_size : 0;
    }
    uint64_t total = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s", path, e->d_name);
        total += dir_size(p);
    }
    closedir(d);
    return total;
}

/* Box-Muller transform for N(0,1). Uses a static state for efficiency. */
static double randn(void) {
    static int have_spare = 0;
    static double spare;
    if (have_spare) { have_spare = 0; return spare; }
    double u1, u2;
    do { u1 = (double)rand() / ((double)RAND_MAX + 1.0); } while (u1 <= 0.0);
    u2 = (double)rand() / ((double)RAND_MAX + 1.0);
    double mag = sqrt(-2.0 * log(u1));
    spare = mag * cos(2.0 * M_PI * u2);
    have_spare = 1;
    return mag * sin(2.0 * M_PI * u2);
}

static double clamp(double v, double lo, double hi) {
    return v < lo ? lo : (v > hi ? hi : v);
}

/* djb2 hash for region assignment. */
static uint32_t djb2(const char *s) {
    uint32_t h = 5381;
    unsigned char c;
    while ((c = (unsigned char)*s++)) h = h * 33 ^ c;
    return h;
}

/* ---- region table ------------------------------------------------------- */

static const char *REGIONS[4] = {
    "us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1"
};

/* ---- schema: 12 columns (col indices in comments) ----------------------- */
/*
 *  0: ts          TIMESTAMP (designated)
 *  1: hostname    SYMBOL
 *  2: region      SYMBOL
 *  3: usage_user       FLOAT64
 *  4: usage_system     FLOAT64
 *  5: usage_idle       FLOAT64
 *  6: usage_nice       FLOAT64
 *  7: usage_iowait     FLOAT64
 *  8: usage_irq        FLOAT64
 *  9: usage_softirq    FLOAT64
 * 10: usage_steal      FLOAT64
 * 11: usage_guest      FLOAT64
 * 12: usage_guest_nice FLOAT64
 */

#define NCPU_METRICS 10
#define TICK_NS      (10LL * 1000000000LL)   /* 10 seconds in nanoseconds */

/* Per-host, per-metric random walk state. Initial value = 50.0 (mid-range). */
typedef struct {
    double v[NCPU_METRICS];
} host_state_t;

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <db_dir> <scale> <hours>\n", argv[0]);
        return 1;
    }
    const char *db_dir = argv[1];
    int scale  = atoi(argv[2]);
    int hours  = atoi(argv[3]);

    if (scale <= 0 || hours <= 0) {
        fprintf(stderr, "scale and hours must be positive\n");
        return 1;
    }

    int64_t ticks = (int64_t)hours * 3600 / 10;  /* number of 10-second intervals */
    int64_t total_rows = (int64_t)scale * ticks;

    printf("=== TSBS cpu-only generator ===\n");
    printf("scale=%d  hours=%d  ticks=%lld  total_rows=%lld\n",
           scale, hours, (long long)ticks, (long long)total_rows);

    /* Fixed seed for reproducibility. */
    srand(42);

    /* Initialize per-host state. */
    host_state_t *states = malloc((size_t)scale * sizeof(host_state_t));
    if (!states) { fprintf(stderr, "OOM\n"); return 1; }
    for (int h = 0; h < scale; h++) {
        for (int m = 0; m < NCPU_METRICS; m++) {
            states[h].v[m] = 50.0;
        }
    }

    /* Pre-build hostname strings. */
    char **hostnames = malloc((size_t)scale * sizeof(char *));
    if (!hostnames) { fprintf(stderr, "OOM\n"); return 1; }
    for (int h = 0; h < scale; h++) {
        char buf[64];
        snprintf(buf, sizeof(buf), "host_%d", h);
        hostnames[h] = strdup(buf);
    }

    /* Open database. */
    tsdb_db_t *db;
    int rc = tsdb_open(db_dir, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "tsdb_open failed: %s\n", tsdb_errstr(rc));
        return 1;
    }

    /* Create table schema. */
    tsdb_col_t cols[] = {
        {"ts",              TSDB_TYPE_TIMESTAMP},
        {"hostname",        TSDB_TYPE_SYMBOL},
        {"region",          TSDB_TYPE_SYMBOL},
        {"usage_user",      TSDB_TYPE_FLOAT64},
        {"usage_system",    TSDB_TYPE_FLOAT64},
        {"usage_idle",      TSDB_TYPE_FLOAT64},
        {"usage_nice",      TSDB_TYPE_FLOAT64},
        {"usage_iowait",    TSDB_TYPE_FLOAT64},
        {"usage_irq",       TSDB_TYPE_FLOAT64},
        {"usage_softirq",   TSDB_TYPE_FLOAT64},
        {"usage_steal",     TSDB_TYPE_FLOAT64},
        {"usage_guest",     TSDB_TYPE_FLOAT64},
        {"usage_guest_nice",TSDB_TYPE_FLOAT64},
    };
    const int ncols = (int)(sizeof(cols) / sizeof(cols[0]));

    rc = tsdb_create_table(db, "cpu", cols, (size_t)ncols, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) {
        fprintf(stderr, "create_table failed: %s\n", tsdb_errstr(rc));
        tsdb_close(db);
        return 1;
    }

    tsdb_table_t *tbl;
    rc = tsdb_open_table(db, "cpu", &tbl);
    if (rc != TSDB_OK) {
        fprintf(stderr, "open_table failed: %s\n", tsdb_errstr(rc));
        tsdb_close(db);
        return 1;
    }

    /* Base timestamp: 2026-01-01 00:00:00 UTC. */
    tsdb_ts_t base_ts = tsdb_parse_ts("2026-01-01 00:00:00");

    tsdb_batch_t *batch;
    rc = tsdb_batch_begin(tbl, &batch);
    if (rc != TSDB_OK) {
        fprintf(stderr, "batch_begin failed: %s\n", tsdb_errstr(rc));
        tsdb_close(db);
        return 1;
    }

    double t0 = now_sec();
    int64_t rows_written = 0;

    /* Interleaved ordering: for each tick, iterate over all hosts.
     * Timestamps are strictly ascending within and across ticks.
     *
     * sub_step = TICK_NS / scale spreads hosts uniformly within a tick.
     * With this spacing the delta-of-delta at tick boundaries is zero
     * (delta stays constant = sub_step), keeping DOD encoding working for
     * any scale without overflowing the 32-bit DOD fallback range.
     *
     * Effective host separation: sub_step ns (≥ 1 ns for scale ≤ TICK_NS).
     * Effective tick period seen from any single host: exactly TICK_NS.
     */
    tsdb_ts_t sub_step = TICK_NS / (tsdb_ts_t)scale;
    if (sub_step < 1) sub_step = 1; /* safety guard for very large scale */

    for (int64_t tick = 0; tick < ticks; tick++) {
        tsdb_ts_t tick_base = base_ts + tick * TICK_NS;

        for (int h = 0; h < scale; h++) {
            /* Evenly-spaced within tick: constant delta = sub_step across all rows. */
            tsdb_ts_t ts = tick_base + (tsdb_ts_t)h * sub_step;

            /* Update random walk for each metric. */
            for (int m = 0; m < NCPU_METRICS; m++) {
                states[h].v[m] = clamp(states[h].v[m] + randn() * 2.5, 0.0, 100.0);
            }

            /* Region: hash(hostname) % 4. */
            const char *region = REGIONS[djb2(hostnames[h]) % 4];

            tsdb_batch_row_ts(batch, ts);
            tsdb_batch_row_sym(batch, 1, hostnames[h]);
            tsdb_batch_row_sym(batch, 2, region);
            tsdb_batch_row_f64(batch, 3,  states[h].v[0]);  /* usage_user */
            tsdb_batch_row_f64(batch, 4,  states[h].v[1]);  /* usage_system */
            tsdb_batch_row_f64(batch, 5,  states[h].v[2]);  /* usage_idle */
            tsdb_batch_row_f64(batch, 6,  states[h].v[3]);  /* usage_nice */
            tsdb_batch_row_f64(batch, 7,  states[h].v[4]);  /* usage_iowait */
            tsdb_batch_row_f64(batch, 8,  states[h].v[5]);  /* usage_irq */
            tsdb_batch_row_f64(batch, 9,  states[h].v[6]);  /* usage_softirq */
            tsdb_batch_row_f64(batch, 10, states[h].v[7]);  /* usage_steal */
            tsdb_batch_row_f64(batch, 11, states[h].v[8]);  /* usage_guest */
            tsdb_batch_row_f64(batch, 12, states[h].v[9]);  /* usage_guest_nice */
            tsdb_batch_row_end(batch);
            rows_written++;
        }
    }

    rc = tsdb_batch_commit(batch);
    if (rc != TSDB_OK) {
        fprintf(stderr, "batch_commit failed: %s\n", tsdb_errstr(rc));
        tsdb_close(db);
        return 1;
    }

    double t1 = now_sec();
    double elapsed = t1 - t0;
    double rps = (double)rows_written / elapsed;

    tsdb_close(db);

    /* Disk usage. */
    uint64_t disk_bytes = dir_size(db_dir);
    /* Uncompressed: ts(8) + hostname(4) + region(4) + 10xf64(80) = 96 bytes/row estimate */
    uint64_t raw_bytes = (uint64_t)rows_written * 96;

    printf("=== Ingest complete ===\n");
    printf("rows         = %lld\n", (long long)rows_written);
    printf("wall time    = %.3f s\n", elapsed);
    printf("throughput   = %.2f M rows/s  (%.0f rows/s)\n", rps / 1e6, rps);
    printf("on-disk      = %.2f MB  (%.2f KB)\n", disk_bytes / 1e6, disk_bytes / 1e3);
    printf("raw estimate = %.2f MB\n", raw_bytes / 1e6);
    printf("bytes/point  = %.2f B\n", (disk_bytes > 0 && rows_written > 0) ? (double)disk_bytes / (double)rows_written : 0.0);
    if (disk_bytes > 0)
        printf("compression  = %.2fx\n", (double)raw_bytes / (double)disk_bytes);

    free(states);
    for (int h = 0; h < scale; h++) free(hostnames[h]);
    free(hostnames);
    return 0;
}
