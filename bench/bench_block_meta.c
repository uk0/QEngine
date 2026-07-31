/* bench_block_meta.c — cost of fetching a column's block metadata.
 *
 * tsdb_part_col_blocks() mallocs and memcpy's the partition's WHOLE per-column
 * block-meta array.  The scan path calls it once per (block, column), so the
 * bytes copied to walk a partition grow as nb^2 while the rows read grow as nb:
 * ns/row is linear in the block count.  tsdb_part_col_blocks_ref() borrows the
 * array instead — O(1), no allocation.
 *
 * Both variants are exercised in the SAME binary against the SAME partition, so
 * the comparison carries no compiler or layout confound.  Three access patterns
 * are replayed, matching the three converted exec.c call sites and the column
 * counts a representative query gives each:
 *
 *   stats     try_stats_fastpath   1 col   SELECT count(*),min(v),max(v),sum(v)
 *   scan      scan_load_col_block  2 cols  SELECT sum(v) ... WHERE g = 1
 *   row       exec_select row path 4 cols  SELECT ts, v, k, g ...
 *
 * Only the metadata fetch and the ordinal pairing are timed — no block decode —
 * because that is exactly what the change removes.  ns/row divides by the rows
 * the pattern would have covered (nb * rows_per_block), which is the unit the
 * measured study reported.
 *
 * Usage:  bench_block_meta [nb ...]        (default: 125 500 1000 2000 3000)
 *         TSDB_BM_DIR=/path  to place the dataset elsewhere
 */

#include "../include/tsdb.h"
#include "../src/storage/schema.h"
#include "../src/storage/part.h"
#include "../src/core/types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

#define BP        TSDB_BLOCK_POINTS      /* 8192 rows/block */
#define STEP_NS   1000000LL              /* 1 ms */
#define DAY_NS    86400000000000LL
/* Day-ALIGNED base: a day's rows must land in exactly one partition, or the
 * requested block count splits across two and the nb axis stops being the
 * thing that varies.  19675 * DAY_NS. */
#define BASE_TS   1699920000000000000LL
#define REPS      5

static int64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static volatile uint64_t g_sink;

/* Replay one call site's metadata access over every block of the partition.
 * `cols` is the set of columns that site would touch per block. */
static int64_t replay_copy(tsdb_part_t *p, const int *cols, int ncols, size_t nb) {
    uint64_t acc = 0;
    int64_t t0 = now_ns();
    for (size_t b = 0; b < nb; b++) {
        for (int ci = 0; ci < ncols; ci++) {
            tsdb_block_meta_t *m = NULL; size_t n = 0;
            if (tsdb_part_col_blocks(p, cols[ci], &m, &n) != TSDB_OK) continue;
            if (b < n) acc += m[b].count + (uint64_t)m[b].ts_min;
            free(m);
        }
    }
    int64_t t1 = now_ns();
    g_sink += acc;
    return t1 - t0;
}

static int64_t replay_ref(tsdb_part_t *p, const int *cols, int ncols, size_t nb) {
    uint64_t acc = 0;
    int64_t t0 = now_ns();
    for (size_t b = 0; b < nb; b++) {
        for (int ci = 0; ci < ncols; ci++) {
            const tsdb_block_meta_t *m = NULL; size_t n = 0;
            if (tsdb_part_col_blocks_ref(p, cols[ci], &m, &n) != TSDB_OK) continue;
            if (b < n) acc += m[b].count + (uint64_t)m[b].ts_min;
        }
    }
    int64_t t1 = now_ns();
    g_sink += acc;
    return t1 - t0;
}

struct site { const char *name; int cols[4]; int ncols; };
static const struct site SITES[] = {
    { "stats  (try_stats_fastpath, 1 col)", { 1 },          1 },
    { "scan   (scan_load_col_block, 2 col)", { 1, 3 },      2 },
    { "row    (exec_select row path, 4 col)", { 0, 1, 2, 3 }, 4 },
};

static void write_day(tsdb_table_t *t, int day, size_t nb) {
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    int64_t base = BASE_TS + (int64_t)day * DAY_NS;
    size_t rows = nb * BP;
    for (size_t i = 0; i < rows; i++) {
        tsdb_batch_row_ts(b, base + (int64_t)i * STEP_NS);
        tsdb_batch_row_f64(b, 1, (double)(i % 977));
        tsdb_batch_row_i64(b, 2, (int64_t)i);
        tsdb_batch_row_i64(b, 3, (int64_t)(i % 4));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
}

/* Partition dirs sorted ascending — same order the days were written. */
static int list_parts(const char *table_dir, char out[][4096], int cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int n = 0;
    while ((e = readdir(d)) && n < cap) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s", table_dir, e->d_name);
        struct stat st;
        if (stat(p, &st) == 0 && S_ISDIR(st.st_mode)) snprintf(out[n++], 4096, "%s", p);
    }
    closedir(d);
    for (int i = 1; i < n; i++) {
        char tmp[4096]; snprintf(tmp, sizeof(tmp), "%s", out[i]);
        int j = i - 1;
        while (j >= 0 && strcmp(out[j], tmp) > 0) { snprintf(out[j + 1], 4096, "%s", out[j]); j--; }
        snprintf(out[j + 1], 4096, "%s", tmp);
    }
    return n;
}

int main(int argc, char **argv) {
    size_t want[16]; int nwant = 0;
    if (argc > 1) {
        for (int i = 1; i < argc && nwant < 16; i++) want[nwant++] = (size_t)strtoul(argv[i], NULL, 10);
    } else {
        size_t def[] = { 125, 500, 1000, 2000, 3000 };
        for (unsigned i = 0; i < sizeof(def) / sizeof(def[0]); i++) want[nwant++] = def[i];
    }

    const char *dir = getenv("TSDB_BM_DIR");
    if (!dir) dir = "/tmp/tsdb_bench_block_meta";
    char cmd[4200]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);

    printf("=== bench_block_meta ===\n");
    printf("block_points=%d  partitions:", BP);
    for (int i = 0; i < nwant; i++) printf(" %zu blocks (%.2fM rows)", want[i],
                                            (double)want[i] * BP / 1e6);
    printf("\n\n[0] ingest\n");

    int64_t t_ing = now_ns();
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); return 1; }
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_FLOAT64 },
            { "k",  TSDB_TYPE_INT64 },     { "g", TSDB_TYPE_INT64 },
        };
        int rc = tsdb_create_table(db, "t", cols, 4, "ts");
        if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create rc=%d\n", rc); return 1; }
        tsdb_table_t *t = NULL;
        if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "open_table\n"); return 1; }
        for (int i = 0; i < nwant; i++) {
            write_day(t, i, want[i]);
            printf("  day %d: %zu blocks written\n", i, want[i]); fflush(stdout);
        }
        tsdb_close(db);
    }
    printf("  ingest %.1f s\n", (double)(now_ns() - t_ing) / 1e9);

    char table_dir[4096];
    snprintf(table_dir, sizeof(table_dir), "%s/t", dir);
    static char parts[16][4096];
    int nparts = list_parts(table_dir, parts, 16);

    tsdb_schema_t *s = NULL;
    if (tsdb_schema_open(table_dir, &s) != TSDB_OK) { fprintf(stderr, "schema_open\n"); return 1; }

    printf("\n[1] metadata fetch: copy (tsdb_part_col_blocks) vs "
           "borrow (tsdb_part_col_blocks_ref)\n");
    printf("%-40s %8s %12s %12s %9s\n",
           "site", "blocks", "copy ns/row", "ref ns/row", "speedup");

    for (int pi = 0; pi < nparts; pi++) {
        tsdb_part_t *p = NULL;
        if (tsdb_part_open(s, parts[pi], &p) != TSDB_OK) continue;
        const tsdb_block_meta_t *m0 = NULL; size_t nb = 0;
        tsdb_part_col_blocks_ref(p, 0, &m0, &nb);
        size_t rows = 0;
        for (size_t b = 0; b < nb; b++) rows += m0[b].count;

        for (unsigned si = 0; si < sizeof(SITES) / sizeof(SITES[0]); si++) {
            const struct site *st = &SITES[si];
            /* warm */
            replay_copy(p, st->cols, st->ncols, nb);
            replay_ref(p, st->cols, st->ncols, nb);
            int64_t tc = 0, tr = 0;
            for (int r = 0; r < REPS; r++) {
                tc += replay_copy(p, st->cols, st->ncols, nb);
                tr += replay_ref(p, st->cols, st->ncols, nb);
            }
            double denom = (double)rows * REPS;
            printf("%-40s %8zu %12.4f %12.4f %8.1fx\n",
                   st->name, nb, (double)tc / denom, (double)tr / denom,
                   tr > 0 ? (double)tc / (double)tr : 0.0);
            fflush(stdout);
        }
        printf("\n");
        tsdb_part_close(p);
    }
    tsdb_schema_free(s);

    /* ---- [2] end-to-end SQL over the largest partition -------------------- */
    printf("[2] end-to-end SQL (whole table, %d partitions)\n", nparts);
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); return 1; }
        const char *qs[] = {
            "SELECT count(*), min(v), max(v), sum(v) FROM t",
            "SELECT sum(v) FROM t WHERE g = 1",
            "SELECT g, count(*), sum(v) FROM t GROUP BY g",
        };
        for (unsigned qi = 0; qi < sizeof(qs) / sizeof(qs[0]); qi++) {
            tsdb_result_t *r = NULL;
            if (tsdb_query(db, qs[qi], &r) == TSDB_OK && r) tsdb_result_free(r);  /* warm */
            int64_t best = 0;
            for (int rep = 0; rep < 3; rep++) {
                int64_t a = now_ns();
                r = NULL;
                if (tsdb_query(db, qs[qi], &r) == TSDB_OK && r) {
                    while (tsdb_result_next(r)) g_sink++;
                    tsdb_result_free(r);
                }
                int64_t e = now_ns() - a;
                if (!best || e < best) best = e;
            }
            printf("  %-52s %8.2f ms\n", qs[qi], (double)best / 1e6);
            fflush(stdout);
        }
        tsdb_close(db);
    }

    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
    printf("\nsink=%llu\n", (unsigned long long)g_sink);
    return 0;
}
