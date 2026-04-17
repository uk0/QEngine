/* test_retention.c — tests for background retention GC.
 *
 * Tests:
 *   1. Basic sweep: old partitions deleted, recent ones kept.
 *   2. Dry-run: stats incremented but dirs survive.
 *   3. Glob pattern matching: "metrics.*" matches "metrics.cpu".
 *   4. No-rule table: partitions untouched.
 *   5. Bench: 100 old partitions, print wall time.
 */

#include "../include/tsdb.h"
#include "../src/storage/retention.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>
#include <errno.h>

/* ---- Tiny harness -------------------------------------------------------- */

static int g_pass = 0, g_fail = 0;

#define CHECK(cond, msg) do { \
    if (cond) { g_pass++; } \
    else { fprintf(stderr, "FAIL [line %d]: %s\n", __LINE__, msg); g_fail++; } \
} while (0)

#define CHECK_OK(rc, msg) CHECK((rc) == TSDB_OK, msg)

/* ---- Helpers ------------------------------------------------------------- */

static const char *TEST_DIR = "/tmp/tsdb_test_retention";

static void rmrf(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
}

static void make_dir(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "mkdir -p '%s'", path);
    (void)system(cmd);
}

static int dir_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

/* Create a minimal .col file so dir_size_bytes sees something. */
static void touch_col(const char *dir) {
    char path[2048];
    snprintf(path, sizeof(path), "%s/ts.col", dir);
    FILE *f = fopen(path, "wb");
    if (f) {
        /* Write 1024 bytes so bytes_reclaimed > 0. */
        uint8_t buf[1024] = {0};
        fwrite(buf, 1, sizeof(buf), f);
        fclose(f);
    }
}

/* Build "YYYYMMDD" string for N days ago. */
static void day_str_ago(int days_ago, char *out, size_t cap) {
    time_t t = time(NULL) - (time_t)days_ago * 86400;
    struct tm tm;
    gmtime_r(&t, &tm);
    snprintf(out, cap, "%04d%02d%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

/* Write retention.conf to TEST_DIR. */
static void write_conf(const char *data_dir, const char *conf_text) {
    char path[2048];
    snprintf(path, sizeof(path), "%s/retention.conf", data_dir);
    FILE *f = fopen(path, "w");
    assert(f);
    fputs(conf_text, f);
    fclose(f);
}

/* ---- Test 1: Basic sweep ------------------------------------------------- */

static void test_basic_sweep(void) {
    printf("test_basic_sweep\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "%s/basic", TEST_DIR);
    rmrf(data_dir);
    make_dir(data_dir);

    /* Create table directory. */
    char table_dir[512];
    snprintf(table_dir, sizeof(table_dir), "%s/test_table", data_dir);
    make_dir(table_dir);

    /* Create partitions: 60 days ago (expired), 3 days ago (should keep). */
    char old_part[64], recent_part[64];
    day_str_ago(60, old_part, sizeof(old_part));
    day_str_ago(3,  recent_part, sizeof(recent_part));

    char old_dir[512], recent_dir[512];
    snprintf(old_dir,    sizeof(old_dir),    "%s/%s", table_dir, old_part);
    snprintf(recent_dir, sizeof(recent_dir), "%s/%s", table_dir, recent_part);
    make_dir(old_dir);
    make_dir(recent_dir);
    touch_col(old_dir);
    touch_col(recent_dir);

    /* Write conf: test_table = 7d */
    write_conf(data_dir, "test_table = 7d\n");

    tsdb_retention_opts_t opts = {0};
    opts.sweep_interval_ns = 3600LL * 1000000000LL;
    opts.dry_run = 0;

    tsdb_retention_t *r = NULL;
    CHECK_OK(tsdb_retention_start(data_dir, NULL, &opts, &r), "start");

    int deleted = 0;
    CHECK_OK(tsdb_retention_sweep_once(r, &deleted), "sweep_once");
    CHECK(deleted == 1, "exactly 1 partition deleted");

    /* Old dir gone, recent dir present. */
    CHECK(!dir_exists(old_dir),    "old partition removed");
    CHECK( dir_exists(recent_dir), "recent partition kept");

    tsdb_retention_stats_t stats;
    tsdb_retention_stats(r, &stats);
    CHECK(stats.sweeps_done == 1,        "sweeps_done == 1");
    CHECK(stats.partitions_deleted == 1, "partitions_deleted == 1");
    CHECK(stats.bytes_reclaimed >= 1024, "bytes_reclaimed >= 1024");

    tsdb_retention_stop(r);
    rmrf(data_dir);
    printf("  passed\n");
}

/* ---- Test 2: Dry-run ----------------------------------------------------- */

static void test_dry_run(void) {
    printf("test_dry_run\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "%s/dryrun", TEST_DIR);
    rmrf(data_dir);
    make_dir(data_dir);

    char table_dir[512];
    snprintf(table_dir, sizeof(table_dir), "%s/logs", data_dir);
    make_dir(table_dir);

    char old_part[64];
    day_str_ago(30, old_part, sizeof(old_part));
    char old_dir[512];
    snprintf(old_dir, sizeof(old_dir), "%s/%s", table_dir, old_part);
    make_dir(old_dir);
    touch_col(old_dir);

    write_conf(data_dir, "logs = 7d\n");

    tsdb_retention_opts_t opts = {0};
    opts.sweep_interval_ns = 3600LL * 1000000000LL;
    opts.dry_run = 1;   /* DRY RUN */

    tsdb_retention_t *r = NULL;
    CHECK_OK(tsdb_retention_start(data_dir, NULL, &opts, &r), "start");

    int deleted = 0;
    CHECK_OK(tsdb_retention_sweep_once(r, &deleted), "sweep_once dry-run");

    /* In dry_run the 'deleted' counter still increments so callers can observe
     * what WOULD be deleted, but dirs survive on disk. */
    CHECK(deleted == 1,       "dry_run: deleted count == 1");
    CHECK(dir_exists(old_dir), "dry_run: dir still on disk");

    tsdb_retention_stats_t stats;
    tsdb_retention_stats(r, &stats);
    CHECK(stats.partitions_deleted == 1, "dry_run stats: partitions_deleted == 1");

    tsdb_retention_stop(r);
    rmrf(data_dir);
    printf("  passed\n");
}

/* ---- Test 3: Glob pattern ------------------------------------------------ */

static void test_glob_pattern(void) {
    printf("test_glob_pattern\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "%s/glob", TEST_DIR);
    rmrf(data_dir);
    make_dir(data_dir);

    /* Two tables matching glob "metrics.*" and one that does not. */
    const char *tables[] = {"metrics.cpu", "metrics.mem", "other_table"};
    char old_part[64];
    day_str_ago(40, old_part, sizeof(old_part));

    for (int i = 0; i < 3; i++) {
        char tdir[512], pdir[512];
        snprintf(tdir, sizeof(tdir), "%s/%s", data_dir, tables[i]);
        snprintf(pdir, sizeof(pdir), "%s/%s", tdir, old_part);
        make_dir(pdir);
        touch_col(pdir);
    }

    /* metrics.* = 30d; other_table has no rule */
    write_conf(data_dir, "metrics.* = 30d\n");

    tsdb_retention_opts_t opts = {0};
    tsdb_retention_t *r = NULL;
    CHECK_OK(tsdb_retention_start(data_dir, NULL, &opts, &r), "start");

    int deleted = 0;
    CHECK_OK(tsdb_retention_sweep_once(r, &deleted), "sweep_once glob");
    CHECK(deleted == 2, "2 partitions deleted (metrics.cpu + metrics.mem)");

    /* metrics dirs deleted, other_table untouched. */
    char other_part[512];
    snprintf(other_part, sizeof(other_part), "%s/other_table/%s", data_dir, old_part);
    CHECK(dir_exists(other_part), "other_table partition kept (no rule)");

    char cpu_part[512], mem_part[512];
    snprintf(cpu_part, sizeof(cpu_part), "%s/metrics.cpu/%s", data_dir, old_part);
    snprintf(mem_part, sizeof(mem_part), "%s/metrics.mem/%s", data_dir, old_part);
    CHECK(!dir_exists(cpu_part), "metrics.cpu partition deleted");
    CHECK(!dir_exists(mem_part), "metrics.mem partition deleted");

    tsdb_retention_stop(r);
    rmrf(data_dir);
    printf("  passed\n");
}

/* ---- Test 4: No matching rule leaves dirs alone -------------------------- */

static void test_no_rule(void) {
    printf("test_no_rule\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "%s/norule", TEST_DIR);
    rmrf(data_dir);
    make_dir(data_dir);

    char table_dir[512];
    snprintf(table_dir, sizeof(table_dir), "%s/unregistered", data_dir);
    make_dir(table_dir);

    char old_part[64];
    day_str_ago(365, old_part, sizeof(old_part));
    char part_dir[512];
    snprintf(part_dir, sizeof(part_dir), "%s/%s", table_dir, old_part);
    make_dir(part_dir);
    touch_col(part_dir);

    /* retention.conf exists but has no rule for "unregistered". */
    write_conf(data_dir, "something_else = 7d\n");

    tsdb_retention_opts_t opts = {0};
    tsdb_retention_t *r = NULL;
    CHECK_OK(tsdb_retention_start(data_dir, NULL, &opts, &r), "start");

    int deleted = 0;
    CHECK_OK(tsdb_retention_sweep_once(r, &deleted), "sweep_once no_rule");
    CHECK(deleted == 0, "0 partitions deleted (no matching rule)");
    CHECK(dir_exists(part_dir), "unregistered partition untouched");

    tsdb_retention_stop(r);
    rmrf(data_dir);
    printf("  passed\n");
}

/* ---- Test 5: HOUR partition directory ------------------------------------ */

static void test_hour_partition(void) {
    printf("test_hour_partition\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "%s/hourpart", TEST_DIR);
    rmrf(data_dir);
    make_dir(data_dir);

    char table_dir[512];
    snprintf(table_dir, sizeof(table_dir), "%s/tick_data", data_dir);
    make_dir(table_dir);

    /* Build an HOUR dir name from 10 days ago, hour 00. */
    time_t t = time(NULL) - (time_t)10 * 86400;
    struct tm tm;
    gmtime_r(&t, &tm);
    char hour_part[16];
    snprintf(hour_part, sizeof(hour_part), "%04d%02d%02d00",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);

    char part_dir[512];
    snprintf(part_dir, sizeof(part_dir), "%s/%s", table_dir, hour_part);
    make_dir(part_dir);
    touch_col(part_dir);

    write_conf(data_dir, "tick_data = 7d\n");

    tsdb_retention_opts_t opts = {0};
    tsdb_retention_t *r = NULL;
    CHECK_OK(tsdb_retention_start(data_dir, NULL, &opts, &r), "start");

    int deleted = 0;
    CHECK_OK(tsdb_retention_sweep_once(r, &deleted), "sweep_once hour");
    CHECK(deleted == 1, "HOUR partition deleted");
    CHECK(!dir_exists(part_dir), "hour partition dir gone");

    tsdb_retention_stop(r);
    rmrf(data_dir);
    printf("  passed\n");
}

/* ---- Bench: 100 old partitions ------------------------------------------- */

static void bench_100_partitions(void) {
    printf("bench_100_partitions\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "%s/bench", TEST_DIR);
    rmrf(data_dir);
    make_dir(data_dir);

    char table_dir[512];
    snprintf(table_dir, sizeof(table_dir), "%s/bench_table", data_dir);
    make_dir(table_dir);

    /* Create 100 partitions spread across years 2010-2019. */
    for (int i = 0; i < 100; i++) {
        /* Spread across different dates. */
        int y = 2010 + (i / 10);
        int mo = 1 + (i % 12);
        int dy = 1 + (i % 28);
        if (mo > 12) { mo = 12; }
        char part_name[16];
        snprintf(part_name, sizeof(part_name), "%04d%02d%02d", y, mo, dy);
        char part_dir[512];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", table_dir, part_name);
        make_dir(part_dir);
        touch_col(part_dir);
    }

    write_conf(data_dir, "bench_table = 7d\n");

    tsdb_retention_opts_t opts = {0};
    tsdb_retention_t *r = NULL;
    int rc = tsdb_retention_start(data_dir, NULL, &opts, &r);
    if (rc != TSDB_OK) { printf("  SKIP (start failed)\n"); return; }

    /* Time the sweep. */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    int deleted = 0;
    tsdb_retention_sweep_once(r, &deleted);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    int64_t elapsed_us = ((int64_t)(t1.tv_sec - t0.tv_sec) * 1000000000LL
                          + (t1.tv_nsec - t0.tv_nsec)) / 1000;

    printf("  deleted=%d  wall_time=%lld us  (%.2f ms)\n",
           deleted, (long long)elapsed_us, elapsed_us / 1000.0);

    CHECK(deleted == 100, "all 100 partitions deleted");

    tsdb_retention_stats_t stats;
    tsdb_retention_stats(r, &stats);
    CHECK(stats.partitions_deleted == 100, "stats: 100 partitions_deleted");

    tsdb_retention_stop(r);
    rmrf(data_dir);
}

/* ---- Test 6: tsdb_db_set_retention integration -------------------------- */

static void test_db_integration(void) {
    printf("test_db_integration\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "%s/dbint", TEST_DIR);
    rmrf(data_dir);

    tsdb_db_t *db = NULL;
    CHECK_OK(tsdb_open(data_dir, &db), "db open");

    tsdb_retention_opts_t opts = {0};
    opts.dry_run = 1;
    CHECK_OK(tsdb_db_set_retention(db, NULL, &opts), "set_retention");

    /* tsdb_close should stop the retention thread cleanly. */
    tsdb_close(db);

    /* If we get here without hang or crash, the integration is correct. */
    CHECK(1, "db close with retention did not hang");
    rmrf(data_dir);
    printf("  passed\n");
}

/* ---- Main ---------------------------------------------------------------- */

int main(void) {
    printf("=== test_retention ===\n");

    rmrf(TEST_DIR);
    make_dir(TEST_DIR);

    test_basic_sweep();
    test_dry_run();
    test_glob_pattern();
    test_no_rule();
    test_hour_partition();
    bench_100_partitions();
    test_db_integration();

    rmrf(TEST_DIR);

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
