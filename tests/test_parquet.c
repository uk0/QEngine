/* test_parquet.c — integration tests for the minimal Parquet exporter.
 *
 * Verifies:
 *   1. EXPORT TABLE ... TO PARQUET '<dir>' creates one non-empty file per
 *      on-disk partition.
 *   2. pyarrow can open the emitted file and read the expected row count.
 *   3. Values round-trip byte-exactly for INT64, TIMESTAMP (INT64), FLOAT64,
 *      and SYMBOL (as UTF-8 string).
 *
 * Skip policy: if `python3 -c 'import pyarrow'` fails, the pyarrow-dependent
 * assertions are skipped (but the file-existence check still runs).
 */

#include "../include/tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define ASSERT(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", \
                    #cond, __FILE__, __LINE__); \
            abort(); \
        } \
    } while (0)

#define ASSERT_OK(rc) \
    do { \
        int _rc = (rc); \
        if (_rc != TSDB_OK) { \
            fprintf(stderr, "ASSERT_OK FAILED: %s -> %d (%s) [%s:%d]\n", \
                    #rc, _rc, tsdb_errstr(_rc), __FILE__, __LINE__); \
            abort(); \
        } \
    } while (0)

static int pyarrow_available(void) {
    /* 0 == pyarrow importable; non-zero == unavailable. */
    int rc = system("python3 -c 'import pyarrow' >/dev/null 2>&1");
    return rc == 0;
}

static int file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    return (int)st.st_size;
}

/* Recursively rm -rf — needed before each test run to start clean. */
static void rmrf(const char *path) {
    char cmd[4096];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s' 2>/dev/null", path);
    (void)system(cmd);
}

/* Find the single .parquet file in `dir`. Caller must free if non-NULL. */
static char *find_parquet(const char *dir) {
    DIR *d = opendir(dir);
    if (!d) return NULL;
    struct dirent *ent;
    char *hit = NULL;
    while ((ent = readdir(d)) != NULL) {
        size_t n = strlen(ent->d_name);
        if (n < 8) continue;
        if (strcmp(ent->d_name + n - 8, ".parquet") != 0) continue;
        char full[4096];
        snprintf(full, sizeof(full), "%s/%s", dir, ent->d_name);
        hit = strdup(full);
        break;
    }
    closedir(d);
    return hit;
}

int main(void) {
    /* Use a work dir distinct from the binary's path so `make test` can
     * relink the binary without conflicting with a leftover directory. */
    const char *base = "build/test/test_parquet_data";
    const char *data = "build/test/test_parquet_data/db";
    const char *outd = "build/test/test_parquet_data/out";

    rmrf(base);
    mkdir("build",      0755);
    mkdir("build/test", 0755);
    mkdir(base,         0755);

    printf("[test_parquet] setting up db at %s\n", data);
    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(data, &db));

    tsdb_col_t cols[] = {
        { "ts",     TSDB_TYPE_TIMESTAMP },
        { "sym",    TSDB_TYPE_SYMBOL    },
        { "price",  TSDB_TYPE_FLOAT64   },
        { "volume", TSDB_TYPE_INT64     },
    };
    ASSERT_OK(tsdb_create_table(db, "trades", cols, 4, "ts"));

    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, "trades", &tbl));

    /* Insert 100 rows, all on a single day so we get exactly one partition.
     * Day anchor: 2026-04-18 00:00:00 UTC = 1776643200 seconds since epoch. */
    const int64_t DAY_NS   = 1776643200LL * 1000000000LL;
    const int64_t STEP_NS  = 1000000LL;  /* 1 ms */
    const int     N        = 100;
    const char   *syms[]   = { "AAPL", "GOOG", "MSFT", "AMZN", "TSLA" };
    const int     NSYM     = 5;

    tsdb_batch_t *b = NULL;
    ASSERT_OK(tsdb_batch_begin(tbl, &b));
    for (int i = 0; i < N; i++) {
        ASSERT_OK(tsdb_batch_row_ts(b, DAY_NS + (int64_t)i * STEP_NS));
        ASSERT_OK(tsdb_batch_row_sym(b, 1, syms[i % NSYM]));
        ASSERT_OK(tsdb_batch_row_f64(b, 2, 100.0 + i * 0.125));
        ASSERT_OK(tsdb_batch_row_i64(b, 3, 1000 + i));
        ASSERT_OK(tsdb_batch_row_end(b));
    }
    ASSERT_OK(tsdb_batch_commit(b));

    /* Issue EXPORT via the query layer. */
    mkdir(outd, 0755);
    char qtl[512];
    snprintf(qtl, sizeof(qtl), "EXPORT TABLE trades TO PARQUET '%s'", outd);
    tsdb_result_t *r = NULL;
    int qrc = tsdb_query(db, qtl, &r);
    if (qrc != TSDB_OK) {
        fprintf(stderr, "[test_parquet] query failed rc=%d\n", qrc);
    }
    ASSERT_OK(qrc);
    tsdb_result_free(r);

    /* Verify a .parquet file was produced. */
    char *pq = find_parquet(outd);
    ASSERT(pq != NULL);
    int sz = file_size(pq);
    printf("[test_parquet] exported %s (%d bytes)\n", pq, sz);
    ASSERT(sz > 0);

    /* Check magic bytes: start and end with "PAR1". */
    {
        FILE *f = fopen(pq, "rb");
        ASSERT(f);
        char head[4] = {0}, tail[4] = {0};
        ASSERT(fread(head, 1, 4, f) == 4);
        fseek(f, -4, SEEK_END);
        ASSERT(fread(tail, 1, 4, f) == 4);
        fclose(f);
        ASSERT(memcmp(head, "PAR1", 4) == 0);
        ASSERT(memcmp(tail, "PAR1", 4) == 0);
    }

    tsdb_close(db);

    if (!pyarrow_available()) {
        printf("SKIP: pyarrow not available\n");
        free(pq);
        return 0;
    }

    /* Run a Python script that opens the file and verifies row count,
     * types, and values exactly. */
    char script[8192];
    snprintf(script, sizeof(script),
"python3 - <<'PYEOF' '%s' %d\n"
"import sys, os\n"
"import pyarrow.parquet as pq\n"
"path = sys.argv[1]\n"
"n    = int(sys.argv[2])\n"
"tbl = pq.read_table(path)\n"
"assert tbl.num_rows == n, 'rows=%%d expected %%d' %% (tbl.num_rows, n)\n"
"cols = set(tbl.column_names)\n"
"assert cols == {'ts','sym','price','volume'}, cols\n"
"ts_vals     = tbl.column('ts').to_pylist()\n"
"vol_vals    = tbl.column('volume').to_pylist()\n"
"price_vals  = tbl.column('price').to_pylist()\n"
"sym_vals    = tbl.column('sym').to_pylist()\n"
"DAY_NS = 1776643200*10**9\n"
"STEP   = 10**6\n"
"syms   = ['AAPL','GOOG','MSFT','AMZN','TSLA']\n"
"for i in range(n):\n"
"    assert ts_vals[i]    == DAY_NS + i*STEP,  ('ts',  i, ts_vals[i])\n"
"    assert vol_vals[i]   == 1000 + i,         ('vol', i, vol_vals[i])\n"
"    assert price_vals[i] == 100.0 + i*0.125,  ('px',  i, price_vals[i])\n"
"    assert sym_vals[i]   == syms[i %% 5],     ('sym', i, sym_vals[i])\n"
"print('pyarrow: all %%d rows round-tripped exactly' %% n)\n"
"PYEOF\n",
        pq, N);

    printf("[test_parquet] invoking pyarrow verifier...\n");
    int prc = system(script);
    if (prc != 0) {
        fprintf(stderr, "[test_parquet] pyarrow verification FAILED (exit %d)\n", prc);
        free(pq);
        return 1;
    }

    free(pq);
    printf("[test_parquet] PASS\n");
    return 0;
}
