/* test_migrate_resolve.c — the migration SDK must resolve a table by what is
 * ON DISK, not by what this process happens to have opened.
 *
 * THE BUG.  export, digest and import all resolved the table with
 * tsdb_db_find_table, which walks the db's in-memory table list.  A table that
 * exists on disk but has not been opened in this process is simply absent from
 * that list, so:
 *
 *   1. export/digest of an untouched table returned TSDB_ERR_NOTFOUND, even
 *      though a SELECT on the same handle reads it fine (query opens it).
 *   2. import saw "no such table" and called tsdb_create_table, which
 *      overwrites the target's schema.bin — TSDB_MIG_CREATE_ONLY landed
 *      instead of returning TSDB_ERR_EXISTS, and a MISMATCHED schema was
 *      accepted and destroyed the target's own rows instead of being refused.
 *
 * THE FIX.  Resolve through tsdb_open_table and propagate its rc: TSDB_OK
 * means the table exists, TSDB_ERR_NOTFOUND means it does not, anything else
 * is a real error.
 *
 * Resolving properly makes a RESUMED import find its own previous run's
 * dictionary, so the v2 "target already holds symbols -> refuse" guard had to
 * become "target dictionary DIVERGES -> refuse".  The last case pins that: the
 * same SYMBOL stream must import twice, in one process and across a reopen,
 * without ever being rejected.
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
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define SRC     "/tmp/tsdb_mres_src"
#define DST_EX  "/tmp/tsdb_mres_exists"
#define DST_MM  "/tmp/tsdb_mres_mismatch"
#define DST_RS  "/tmp/tsdb_mres_resume"
#define STREAM  "/tmp/tsdb_mres.stream"

#define T0     1700000000000000000LL
#define NROWS  6000
#define NHOST  2

static const char *HOSTS[NHOST] = { "alpha", "bravo" };

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

static void cleanup(void) {
    rm_rf(SRC); rm_rf(DST_EX); rm_rf(DST_MM); rm_rf(DST_RS); remove(STREAM);
}

static long long count_where(tsdb_db_t *db, const char *q) {
    tsdb_result_t *r = NULL; long long n = -1;
    if (tsdb_query(db, q, &r) == TSDB_OK && r && tsdb_result_next(r) > 0)
        n = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);
    return n;
}

/* Tally rows per tag by DECODING the symbol column, not by predicating on it.
 * Decoding is what the dictionary reconciliation actually has to get right.
 * WHERE host='...' is deliberately avoided here: a symbol predicate over a
 * partition landed by the raw-block applier reads 0 rows even when every value
 * decodes correctly — a separate defect in the predicate path, present at HEAD
 * with or without this change, and not what this test is pinning. */
static void tally_tags(tsdb_db_t *db, long long *per_host, long long *total) {
    for (int h = 0; h < NHOST; h++) per_host[h] = 0;
    *total = 0;
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, "SELECT ts, host FROM m", &r) != TSDB_OK || !r) return;
    while (tsdb_result_next(r) > 0) {
        const char *s = tsdb_result_sym(r, 1);
        (*total)++;
        for (int h = 0; h < NHOST; h++)
            if (s && !strcmp(s, HOSTS[h])) { per_host[h]++; break; }
    }
    tsdb_result_free(r);
}

/* Source: m(ts, host SYMBOL, v), NROWS rows over NHOST tags, flushed. */
static void build_source(void) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(SRC, &db));
    tsdb_col_t cols[] = {
        { "ts",   TSDB_TYPE_TIMESTAMP },
        { "host", TSDB_TYPE_SYMBOL    },
        { "v",    TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, "m", cols, 3, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < NROWS; i++) {
        OK(tsdb_batch_row_ts(b, T0 + (int64_t)i * 1000000LL));
        OK(tsdb_batch_row_sym(b, 1, HOSTS[i % NHOST]));
        OK(tsdb_batch_row_f64(b, 2, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));
    tsdb_close(db);
}

/* A target holding its OWN rows, under `cols`, then closed so nothing in the
 * next process has it open. */
static void build_target(const char *dir, const tsdb_col_t *cols, size_t ncols,
                         double v0) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    OK(tsdb_create_table(db, "m", cols, ncols, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 2000; i++) {
        OK(tsdb_batch_row_ts(b, T0 + (int64_t)i * 1000000LL));
        for (size_t c = 1; c < ncols; c++) {
            if (cols[c].type == TSDB_TYPE_SYMBOL)
                OK(tsdb_batch_row_sym(b, (int)c, "dst-only"));
            else
                OK(tsdb_batch_row_f64(b, (int)c, v0 + (double)i));
        }
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));
    tsdb_close(db);
}

int main(void) {
    printf("=== test_migrate_resolve ===\n");
    cleanup();
    build_source();

    /* ---- 1. export + digest of a table this process never touched ---- */
    tsdb_mig_stats_t exp_st; memset(&exp_st, 0, sizeof(exp_st));
    {
        tsdb_db_t *src = NULL;
        OK(tsdb_open(SRC, &src));          /* no query, no tsdb_open_table */

        int fd = open(STREAM, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) FAIL("open stream for write");
        int rc = tsdb_migrate_export(src, "m", fd, NULL, &exp_st);
        close(fd);
        printf("[cold export] rc=%d (%s) blocks=%llu rows=%llu\n", rc, tsdb_errstr(rc),
               (unsigned long long)exp_st.blocks, (unsigned long long)exp_st.rows);
        if (rc != TSDB_OK)
            FAIL("export of an unopened on-disk table must work, got %d (%s)",
                 rc, tsdb_errstr(rc));
        if (exp_st.rows != NROWS)
            FAIL("cold export moved %llu rows, expected %d",
                 (unsigned long long)exp_st.rows, NROWS);
        tsdb_close(src);
    }
    {
        tsdb_db_t *src = NULL;
        OK(tsdb_open(SRC, &src));
        tsdb_mig_stats_t dg;
        int rc = tsdb_migrate_digest(src, "m", &dg);
        printf("[cold digest] rc=%d (%s) rows=%llu\n", rc, tsdb_errstr(rc),
               (unsigned long long)dg.rows);
        if (rc != TSDB_OK)
            FAIL("digest of an unopened on-disk table must work, got %d (%s)",
                 rc, tsdb_errstr(rc));
        if (dg.rows != exp_st.rows || dg.digest != exp_st.digest)
            FAIL("cold digest disagrees with cold export");

        /* A table that really is absent is still NOTFOUND. */
        int fd = open("/dev/null", O_WRONLY);
        rc = tsdb_migrate_export(src, "nosuchtable", fd, NULL, NULL);
        close(fd);
        printf("[absent]      export rc=%d (%s)\n", rc, tsdb_errstr(rc));
        if (rc != TSDB_ERR_NOTFOUND)
            FAIL("export of an absent table must be NOTFOUND, got %d", rc);
        rc = tsdb_migrate_digest(src, "nosuchtable", &dg);
        if (rc != TSDB_ERR_NOTFOUND)
            FAIL("digest of an absent table must be NOTFOUND, got %d", rc);
        tsdb_close(src);
    }

    /* ---- 2. CREATE_ONLY must see an on-disk table it never opened ---- */
    {
        tsdb_col_t same[] = {
            { "ts",   TSDB_TYPE_TIMESTAMP },
            { "host", TSDB_TYPE_SYMBOL    },
            { "v",    TSDB_TYPE_FLOAT64   },
        };
        build_target(DST_EX, same, 3, 1000.0);

        tsdb_db_t *dst = NULL;
        OK(tsdb_open(DST_EX, &dst));       /* table exists on disk, not open */
        tsdb_mig_opts_t o; memset(&o, 0, sizeof(o));
        o.land = TSDB_MIG_CREATE_ONLY;
        tsdb_mig_stats_t st; memset(&st, 0, sizeof(st));
        int fd = open(STREAM, O_RDONLY);
        int rc = tsdb_migrate_import(dst, fd, &o, &st);
        close(fd);
        printf("[create_only] rc=%d (%s) landed=%llu\n", rc, tsdb_errstr(rc),
               (unsigned long long)st.blocks_landed);
        if (rc != TSDB_ERR_EXISTS)
            FAIL("CREATE_ONLY into an existing on-disk table must be EXISTS, got %d (%s)",
                 rc, tsdb_errstr(rc));
        long long n = count_where(dst, "SELECT count(*) FROM m");
        printf("[create_only] target still has %lld rows\n", n);
        if (n != 2000) FAIL("CREATE_ONLY refusal must not touch the target (%lld rows)", n);
        tsdb_close(dst);
    }

    /* ---- 3. a mismatched schema must be refused, not overwritten ---- */
    {
        tsdb_col_t other[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_FLOAT64   },
        };
        build_target(DST_MM, other, 2, 7.5);

        tsdb_db_t *dst = NULL;
        OK(tsdb_open(DST_MM, &dst));       /* table exists on disk, not open */
        tsdb_mig_stats_t st; memset(&st, 0, sizeof(st));
        int fd = open(STREAM, O_RDONLY);
        int rc = tsdb_migrate_import(dst, fd, NULL, &st);
        close(fd);
        printf("[mismatch]    rc=%d (%s) landed=%llu\n", rc, tsdb_errstr(rc),
               (unsigned long long)st.blocks_landed);
        if (rc != TSDB_ERR_SCHEMA)
            FAIL("import into a 2-col table from a 3-col stream must be SCHEMA, got %d (%s)",
                 rc, tsdb_errstr(rc));
        long long n = count_where(dst, "SELECT count(*) FROM m");
        printf("[mismatch]    target still has %lld rows\n", n);
        if (n != 2000)
            FAIL("refused import destroyed the target's own rows (%lld, expected 2000)", n);
        tsdb_close(dst);
    }

    /* ---- 4. a SYMBOL migration must RESUME, in-process and after reopen --- */
    {
        tsdb_db_t *dst = NULL;
        OK(tsdb_open(DST_RS, &dst));
        tsdb_mig_stats_t a, b;
        int fd = open(STREAM, O_RDONLY);
        int rc = tsdb_migrate_import(dst, fd, NULL, &a);
        close(fd);
        printf("[resume] pass1 rc=%d (%s) landed=%llu\n", rc, tsdb_errstr(rc),
               (unsigned long long)a.blocks_landed);
        OK(rc);
        if (a.blocks_landed == 0) FAIL("first import landed nothing");

        /* Same process, table now open with the dictionary the first pass
         * installed: the v2 guard rejected this as a colliding dictionary. */
        fd = open(STREAM, O_RDONLY);
        rc = tsdb_migrate_import(dst, fd, NULL, &b);
        close(fd);
        printf("[resume] pass2 rc=%d (%s) landed=%llu\n", rc, tsdb_errstr(rc),
               (unsigned long long)b.blocks_landed);
        if (rc != TSDB_OK)
            FAIL("re-importing the same SYMBOL stream must resume, got %d (%s)",
                 rc, tsdb_errstr(rc));
        if (b.blocks_landed != 0)
            FAIL("re-import landed %llu blocks, expected 0",
                 (unsigned long long)b.blocks_landed);
        tsdb_close(dst);

        /* Fresh process view: the table is on disk and not open. */
        OK(tsdb_open(DST_RS, &dst));
        tsdb_mig_stats_t c;
        fd = open(STREAM, O_RDONLY);
        rc = tsdb_migrate_import(dst, fd, NULL, &c);
        close(fd);
        printf("[resume] pass3 rc=%d (%s) landed=%llu\n", rc, tsdb_errstr(rc),
               (unsigned long long)c.blocks_landed);
        if (rc != TSDB_OK)
            FAIL("resume after reopen must work, got %d (%s)", rc, tsdb_errstr(rc));
        if (c.blocks_landed != 0)
            FAIL("resume after reopen landed %llu blocks, expected 0",
                 (unsigned long long)c.blocks_landed);

        long long per_host[NHOST], total = 0;
        tally_tags(dst, per_host, &total);
        if (total != NROWS)
            FAIL("resumed target has %lld rows, expected %d", total, NROWS);
        for (int h = 0; h < NHOST; h++) {
            printf("[resume] %-6s = %lld\n", HOSTS[h], per_host[h]);
            if (per_host[h] != NROWS / NHOST)
                FAIL("tag %s decodes on %lld rows, expected %d",
                     HOSTS[h], per_host[h], NROWS / NHOST);
        }
        tsdb_close(dst);
    }

    cleanup();
    printf("\n=== test_migrate_resolve PASSED ===\n");
    return 0;
}
