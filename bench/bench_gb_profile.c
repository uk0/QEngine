/* bench_gb_profile.c — run a representative GROUP BY against the
 * existing TSBS dataset to measure where time is spent.
 *
 * Companion to the env-gated `[gb-profile/seq]` line in src/query/exec.c.
 * Invoke with TSDB_GB_PROFILE=1 to see the per-stage breakdown:
 *
 *   TSDB_GB_PROFILE=1 build/bench/bench_gb_profile /tmp/tsbs_db_1k
 *
 * Reports:
 *   wall-clock latency for one full GROUP BY hostname over the
 *   8.64 M-row tsbs_db_1k dataset, plus the engine-side
 *   [gb-profile/seq] line printed by exec.c.
 */

#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <tsbs_db_dir>\n", argv[0]);
        return 1;
    }
    const char *dir = argv[1];

    tsdb_db_t *db;
    if (tsdb_open(dir, &db) != TSDB_OK) {
        fprintf(stderr, "open %s failed\n", dir);
        return 1;
    }

    const struct {
        const char *name;
        const char *sql;
    } qs[] = {
        { "groupby-host-count",
          "SELECT hostname, count(*) FROM cpu GROUP BY hostname" },
        { "groupby-host-avg",
          "SELECT hostname, avg(usage_user) FROM cpu GROUP BY hostname" },
        { "groupby-host-multi-agg",
          "SELECT hostname, count(*), avg(usage_user), "
          "max(usage_system), min(usage_idle) "
          "FROM cpu GROUP BY hostname" },
        { "groupby-region-host (2-key)",
          "SELECT region, hostname, count(*) "
          "FROM cpu GROUP BY region, hostname" },
        { NULL, NULL }
    };

    printf("=== bench_gb_profile  db=%s ===\n", dir);
    printf("(set TSDB_GB_PROFILE=1 to see [gb-profile/seq] breakdown)\n\n");

    for (int i = 0; qs[i].name; i++) {
        /* Warm up — discard first run's cache miss. */
        tsdb_result_t *r = NULL;
        (void)tsdb_query(db, qs[i].sql, &r);
        int rows = 0;
        if (r) {
            while (tsdb_result_next(r)) rows++;
            tsdb_result_free(r); r = NULL;
        }

        double t0 = now_sec();
        if (tsdb_query(db, qs[i].sql, &r) != TSDB_OK || !r) {
            fprintf(stderr, "  %-32s  FAIL\n", qs[i].name);
            continue;
        }
        rows = 0;
        while (tsdb_result_next(r)) rows++;
        double t1 = now_sec();
        tsdb_result_free(r);

        printf("  %-32s  %7d rows  %7.2f ms\n",
               qs[i].name, rows, (t1 - t0) * 1000.0);
    }

    tsdb_close(db);
    return 0;
}
