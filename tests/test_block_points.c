/* test_block_points.c — per-table block granularity (schema v3 field).
 *
 * Validates:
 *   1. tsdb_schema_create_ex clamps block_points into [1024, 8192].
 *   2. The schema round-trips through v3 binary format.
 *   3. tsdb_memtable_is_full fires at the per-table block_points, not at
 *      the global TSDB_BLOCK_POINTS constant.
 *   4. Legacy schemas on disk (written before this field existed) read
 *      back with block_points = TSDB_BLOCK_POINTS.  We simulate a v2
 *      schema by truncating the file past the v2 tail and re-opening.
 */

#include "../src/storage/schema.h"
#include "../src/storage/memtable.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[256]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

int main(void) {
    printf("=== test_block_points ===\n");

    const char *dir = "/tmp/tsdb_test_block_points";
    rmrf(dir);
    mkdir(dir, 0755);

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };

    /* ---- 1. Clamp behaviour ------------------------------------------- */
    {
        tsdb_schema_t *s = NULL;
        /* 0 → default 8192 */
        char d0[256]; snprintf(d0, sizeof(d0), "%s/t_def", dir); mkdir(d0, 0755);
        CHECK(tsdb_schema_create_ex(d0, "t_def", cols, 2, "ts",
                                     TSDB_PARTITION_DAY, 0, &s) == TSDB_OK,
              "schema_create_ex block_points=0 OK");
        CHECK(s && s->block_points == 8192, "block_points=0 resolves to 8192");
        tsdb_schema_free(s);

        /* Below floor — clamps up. */
        char d1[256]; snprintf(d1, sizeof(d1), "%s/t_min", dir); mkdir(d1, 0755);
        CHECK(tsdb_schema_create_ex(d1, "t_min", cols, 2, "ts",
                                     TSDB_PARTITION_DAY, 42, &s) == TSDB_OK,
              "schema_create_ex block_points=42 OK");
        CHECK(s && s->block_points == 1024, "42 clamps up to 1024");
        tsdb_schema_free(s);

        /* Above ceiling — clamps down. */
        char d2[256]; snprintf(d2, sizeof(d2), "%s/t_max", dir); mkdir(d2, 0755);
        CHECK(tsdb_schema_create_ex(d2, "t_max", cols, 2, "ts",
                                     TSDB_PARTITION_DAY, 99999, &s) == TSDB_OK,
              "schema_create_ex block_points=99999 OK");
        CHECK(s && s->block_points == 8192, "99999 clamps down to 8192");
        tsdb_schema_free(s);
    }

    /* ---- 2. Round-trip through v3 binary format ----------------------- */
    {
        char d[256]; snprintf(d, sizeof(d), "%s/t_rt", dir); mkdir(d, 0755);
        tsdb_schema_t *s = NULL;
        CHECK(tsdb_schema_create_ex(d, "t_rt", cols, 2, "ts",
                                     TSDB_PARTITION_DAY, 2048, &s) == TSDB_OK,
              "schema_create_ex block_points=2048");
        tsdb_schema_free(s);
        s = NULL;
        CHECK(tsdb_schema_open(d, &s) == TSDB_OK, "schema_open");
        CHECK(s && s->block_points == 2048, "v3 round-trip preserved 2048");
        tsdb_schema_free(s);
    }

    /* ---- 3. memtable is_full honours the per-table value -------------- */
    {
        char d[256]; snprintf(d, sizeof(d), "%s/t_mt", dir); mkdir(d, 0755);
        tsdb_schema_t *s = NULL;
        CHECK(tsdb_schema_create_ex(d, "t_mt", cols, 2, "ts",
                                     TSDB_PARTITION_DAY, 1024, &s) == TSDB_OK,
              "create t_mt with block_points=1024");
        tsdb_memtable_t *m = NULL;
        CHECK(tsdb_memtable_new(s, &m) == TSDB_OK, "memtable_new");

        /* Fill up to 1023 rows — should not be full. */
        for (int i = 0; i < 1023; i++) {
            tsdb_memtable_row_begin(m);
            tsdb_memtable_row_ts(m, (tsdb_ts_t)(i * 1000000000LL));
            tsdb_memtable_row_i64(m, 1, i);
            tsdb_memtable_row_end(m);
        }
        CHECK(!tsdb_memtable_is_full(m), "is_full=0 at 1023 rows");

        /* One more → full at 1024. */
        tsdb_memtable_row_begin(m);
        tsdb_memtable_row_ts(m, (tsdb_ts_t)(1023LL * 1000000000LL));
        tsdb_memtable_row_i64(m, 1, 1023);
        tsdb_memtable_row_end(m);
        CHECK(tsdb_memtable_is_full(m), "is_full=1 at 1024 rows");

        /* Next begin should return FULL. */
        int rc = tsdb_memtable_row_begin(m);
        CHECK(rc == TSDB_ERR_FULL, "row_begin after cap returns FULL");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- 4. v2 schema on disk reads back with default block_points ---- */
    {
        char d[256]; snprintf(d, sizeof(d), "%s/t_v2", dir); mkdir(d, 0755);
        tsdb_schema_t *s = NULL;
        CHECK(tsdb_schema_create_ex(d, "t_v2", cols, 2, "ts",
                                     TSDB_PARTITION_DAY, 2048, &s) == TSDB_OK,
              "create t_v2");
        tsdb_schema_free(s);

        /* Truncate schema.bin to drop the v3 tail (4 bytes of block_points)
         * and rewrite version=2 at offset 4. */
        char schema_path[512];
        snprintf(schema_path, sizeof(schema_path), "%s/schema.bin", d);
        FILE *f = fopen(schema_path, "rb+");
        CHECK(f != NULL, "open schema.bin for downgrade");
        if (f) {
            /* Read current size. */
            fseek(f, 0, SEEK_END);
            long sz = ftell(f);
            CHECK(sz >= 4, "schema.bin big enough to truncate");
            /* Truncate last 4 bytes. */
            fflush(f);
            ftruncate(fileno(f), sz - 4);
            /* Rewrite version field (offset 4, u16) to 2. */
            fseek(f, 4, SEEK_SET);
            uint16_t v2 = 2;
            fwrite(&v2, 1, 2, f);
            fflush(f);
            fclose(f);
        }

        s = NULL;
        CHECK(tsdb_schema_open(d, &s) == TSDB_OK, "reopen downgraded schema");
        CHECK(s && s->block_points == 8192,
              "v2 schema defaults block_points to 8192");
        tsdb_schema_free(s);
    }

    rmrf(dir);
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
