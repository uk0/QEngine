/* test_null_semantics.c — a value a line protocol row did not carry is
 * MISSING, not zero, and must not enter an aggregate.
 *
 * Before this test's fix, influx ingest wrote 0 into every column a line did
 * not set, so a sensor that goes quiet was indistinguishable from one
 * reporting zero and dragged avg() toward zero.  The discriminating numbers
 * used throughout are avg(), count(col) and min(): with three rows carrying
 * temp = 10, MISSING, 30 the broken engine answers avg=13.333, count=3,
 * min=0 and the fixed engine answers avg=20, count=2, min=10.  sum() is
 * deliberately NOT used as a discriminator — adding a fabricated zero does
 * not change a sum, so it cannot tell the two engines apart.
 *
 * Covered:
 *  1. Memtable path        — aggregates over unflushed rows.
 *  2. Flushed path         — same query after tsdb_db_flush_all, which is
 *                            also the stats fast path (no WHERE, disk only).
 *  3. Reopened path        — NaN survives compression and the on-disk block.
 *  4. count(*) vs count(col) — rows vs values.
 *  5. A real 0.0 is a value, not a NULL.
 *  6. first() / last()     — report values that exist.
 *  7. GROUP BY             — the per-row aggregate path.
 *  8. tsdb_result_is_null  — reports the missing float and only that.
 *  9. Integer columns      — the stated boundary: still 0, still counted.
 */

#include "../include/tsdb.h"
#include "../src/server/influx_line.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <unistd.h>
#include <sys/stat.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
        g_fail++; \
    } else { \
        printf("PASS: %s\n", msg); \
        g_pass++; \
    } \
} while (0)

#define CHECK_NEAR(got, want, msg) do { \
    double _g = (got), _w = (want); \
    if (!(fabs(_g - _w) < 1e-9)) { \
        fprintf(stderr, "FAIL [%s:%d]: %s (got %.6f, want %.6f)\n", \
                __FILE__, __LINE__, msg, _g, _w); \
        g_fail++; \
    } else { \
        printf("PASS: %s (%.6f)\n", msg, _g); \
        g_pass++; \
    } \
} while (0)

static void rmrf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    if (system(cmd) != 0) { /* nothing to remove is fine */ }
}

/* Run a single-value aggregate query and return column 0 as a double.
 * Returns NAN when the query fails or produced no row — distinguishable
 * from a real result by every assertion below. */
static double q_f64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, sql, &r) != TSDB_OK || !r) {
        if (r) tsdb_result_free(r);
        fprintf(stderr, "  (query failed: %s)\n", sql);
        return NAN;
    }
    double out = NAN;
    if (tsdb_result_next(r)) out = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    return out;
}

static int64_t q_i64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, sql, &r) != TSDB_OK || !r) {
        if (r) tsdb_result_free(r);
        fprintf(stderr, "  (query failed: %s)\n", sql);
        return -1;
    }
    int64_t out = -1;
    if (tsdb_result_next(r)) out = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    return out;
}

/* temp is present on rows 1 and 3 only; hum is present on all three.
 * hum keeps the measurement's column layout stable so the missing temp is
 * genuinely "field absent from the line" rather than "schema not created
 * yet" — the first line creates both columns. */
static const char *BODY =
    "sensor,id=a temp=10.0,hum=50.0 1000000000\n"
    "sensor,id=a hum=51.0 2000000000\n"
    "sensor,id=a temp=30.0,hum=52.0 3000000000\n";

/* ---- The aggregate assertions, run against each storage state ---------- */

static void assert_null_aware(tsdb_db_t *db, const char *stage) {
    char msg[160];

    snprintf(msg, sizeof(msg), "[%s] avg(temp) excludes the missing row", stage);
    CHECK_NEAR(q_f64(db, "SELECT avg(temp) FROM sensor"), 20.0, msg);

    snprintf(msg, sizeof(msg), "[%s] count(temp) counts values, not rows", stage);
    CHECK(q_i64(db, "SELECT count(temp) FROM sensor") == 2, msg);

    snprintf(msg, sizeof(msg), "[%s] count(*) still counts rows", stage);
    CHECK(q_i64(db, "SELECT count(*) FROM sensor") == 3, msg);

    snprintf(msg, sizeof(msg), "[%s] min(temp) is a real value, not 0", stage);
    CHECK_NEAR(q_f64(db, "SELECT min(temp) FROM sensor"), 10.0, msg);

    snprintf(msg, sizeof(msg), "[%s] max(temp)", stage);
    CHECK_NEAR(q_f64(db, "SELECT max(temp) FROM sensor"), 30.0, msg);

    snprintf(msg, sizeof(msg), "[%s] sum(temp)", stage);
    CHECK_NEAR(q_f64(db, "SELECT sum(temp) FROM sensor"), 40.0, msg);

    snprintf(msg, sizeof(msg), "[%s] spread(temp) spans real values", stage);
    CHECK_NEAR(q_f64(db, "SELECT spread(temp) FROM sensor"), 20.0, msg);

    /* hum is never missing: it must be unaffected in every respect. */
    snprintf(msg, sizeof(msg), "[%s] a fully-populated column is untouched", stage);
    CHECK_NEAR(q_f64(db, "SELECT avg(hum) FROM sensor"), 51.0, msg);

    snprintf(msg, sizeof(msg), "[%s] count(hum) == row count", stage);
    CHECK(q_i64(db, "SELECT count(hum) FROM sensor") == 3, msg);
}

/* ---- 1/2/3: memtable, flushed, reopened -------------------------------- */

static void test_across_storage_states(void) {
    char base[256];
    snprintf(base, sizeof(base), "/tmp/tsdb_nullsem_%d", (int)getpid());
    rmrf(base);

    tsdb_db_t *db = NULL;
    CHECK(tsdb_open(base, &db) == TSDB_OK, "open db");
    if (!db) return;

    size_t lines = 0, errors = 0;
    CHECK(tsdb_influx_ingest(db, BODY, strlen(BODY), &lines, &errors) == TSDB_OK,
          "influx ingest ok");
    CHECK(errors == 0, "no ingest errors");

    /* (1) memtable — nothing has been flushed yet. */
    assert_null_aware(db, "memtable");

    /* (2) flushed — disk blocks, and the stats fast path is live here:
     * these queries have no WHERE and every source is a partition. */
    CHECK(tsdb_db_flush_all(db) == TSDB_OK, "flush_all");
    assert_null_aware(db, "flushed");

    /* (3) reopened — proves NaN survived the block codec and the index,
     * not just the in-process memtable. */
    tsdb_close(db);
    db = NULL;
    CHECK(tsdb_open(base, &db) == TSDB_OK, "reopen db");
    if (db) {
        assert_null_aware(db, "reopened");
        tsdb_close(db);
    }
    rmrf(base);
}

/* ---- 5: a stored 0.0 is a value ---------------------------------------- */

static void test_real_zero_is_not_null(void) {
    char base[256];
    snprintf(base, sizeof(base), "/tmp/tsdb_nullsem_zero_%d", (int)getpid());
    rmrf(base);

    tsdb_db_t *db = NULL;
    CHECK(tsdb_open(base, &db) == TSDB_OK, "open db (zero)");
    if (!db) return;

    /* Row 2 reports temp=0.0 explicitly; row 3 omits it.  Only row 3 is a
     * NULL, so count(temp)==2 and avg==(10+0)/2==5.  An implementation that
     * treated 0 as the NULL marker would answer count=1, avg=10. */
    const char *body =
        "z,id=a temp=10.0,hum=1.0 1000000000\n"
        "z,id=a temp=0.0,hum=1.0 2000000000\n"
        "z,id=a hum=1.0 3000000000\n";
    size_t lines = 0, errors = 0;
    CHECK(tsdb_influx_ingest(db, body, strlen(body), &lines, &errors) == TSDB_OK,
          "ingest (zero) ok");
    CHECK(errors == 0, "no ingest errors (zero)");

    CHECK(q_i64(db, "SELECT count(temp) FROM z") == 2,
          "an explicit 0.0 is counted, the omitted field is not");
    CHECK_NEAR(q_f64(db, "SELECT avg(temp) FROM z"), 5.0,
               "an explicit 0.0 is averaged in");
    CHECK_NEAR(q_f64(db, "SELECT min(temp) FROM z"), 0.0,
               "min sees the explicit 0.0");

    tsdb_close(db);
    rmrf(base);
}

/* ---- 6/7/8: first/last, GROUP BY, and the result NULL bit -------------- */

static void test_first_last_groupby_isnull(void) {
    char base[256];
    snprintf(base, sizeof(base), "/tmp/tsdb_nullsem_gb_%d", (int)getpid());
    rmrf(base);

    tsdb_db_t *db = NULL;
    CHECK(tsdb_open(base, &db) == TSDB_OK, "open db (gb)");
    if (!db) return;

    /* The first line of a body defines the auto-created schema, so it must
     * carry every column the later sparse lines omit.
     * id=a: temp missing on the FIRST and LAST row it has, so first()/last()
     *       must reach past them to 20.0 — a broken engine answers 0.
     * id=b: one row, temp present. */
    const char *body =
        "g,id=z temp=1.0,hum=1.0 500000000\n"
        "g,id=a hum=1.0 1000000000\n"
        "g,id=a temp=20.0,hum=1.0 2000000000\n"
        "g,id=a hum=1.0 3000000000\n"
        "g,id=b temp=8.0,hum=1.0 4000000000\n";
    size_t lines = 0, errors = 0;
    CHECK(tsdb_influx_ingest(db, body, strlen(body), &lines, &errors) == TSDB_OK,
          "ingest (gb) ok");
    CHECK(errors == 0, "no ingest errors (gb)");

    /* Second body: every line has the SAME key layout and none of them
     * mentions temp.  That is the hoisted-resolution path in
     * write_rows_columnar (res[d].kind == RES_NONE for a column no row
     * sets), which the mixed-layout body above never reaches. */
    const char *body2 =
        "g,id=c hum=9.0 5000000000\n"
        "g,id=c hum=9.0 6000000000\n";
    CHECK(tsdb_influx_ingest(db, body2, strlen(body2), &lines, &errors) == TSDB_OK,
          "ingest (gb, uniform sparse layout) ok");
    CHECK(q_i64(db, "SELECT count(temp) FROM g WHERE id='c'") == 0,
          "a column no line sets is NULL on every row");
    CHECK(q_i64(db, "SELECT count(*) FROM g WHERE id='c'") == 2,
          "those rows still exist");

    CHECK_NEAR(q_f64(db, "SELECT first(temp) FROM g WHERE id='a'"), 20.0,
               "first(temp) skips the leading NULLs");
    CHECK_NEAR(q_f64(db, "SELECT last(temp) FROM g WHERE id='a'"), 20.0,
               "last(temp) skips the trailing NULLs");

    /* GROUP BY runs the per-row aggregate path (one_bm / one_n), a different
     * route into agg_update_vals than the block path above. */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db,
                  "SELECT id, avg(temp), count(temp) FROM g GROUP BY id", &r);
        CHECK(rc == TSDB_OK && r != NULL, "group-by query ok");
        int seen_a = 0, seen_b = 0, seen_c = 0;
        while (r && tsdb_result_next(r)) {
            const char *id = tsdb_result_sym(r, 0);
            double avg = tsdb_result_f64(r, 1);
            int64_t cnt = tsdb_result_i64(r, 2);
            if (id && strcmp(id, "a") == 0) {
                seen_a = 1;
                CHECK_NEAR(avg, 20.0, "group a: avg over the one real value");
                CHECK(cnt == 1, "group a: count(temp) == 1 of 3 rows");
            } else if (id && strcmp(id, "b") == 0) {
                seen_b = 1;
                CHECK_NEAR(avg, 8.0, "group b: avg");
                CHECK(cnt == 1, "group b: count(temp) == 1");
            } else if (id && strcmp(id, "c") == 0) {
                seen_c = 1;
                CHECK(cnt == 0, "group c: every temp is NULL");
                CHECK(isnan(avg), "group c: avg over no values is NULL, not 0");
            }
        }
        CHECK(seen_a && seen_b && seen_c, "all three groups returned");
        if (r) tsdb_result_free(r);
    }

    /* tsdb_result_is_null: true for the missing float, false for a present
     * one, false for the SYMBOL and TIMESTAMP columns that have no NULL
     * representation at all. */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db,
                  "SELECT ts, id, temp FROM g WHERE id='a'", &r);
        CHECK(rc == TSDB_OK && r != NULL, "row query ok");
        int nrows = 0, nnull = 0, bad = 0;
        while (r && tsdb_result_next(r)) {
            nrows++;
            if (tsdb_result_is_null(r, 0)) bad++;   /* ts   — never NULL */
            if (tsdb_result_is_null(r, 1)) bad++;   /* id   — never NULL */
            if (tsdb_result_is_null(r, 2)) nnull++; /* temp — NULL twice */
        }
        CHECK(nrows == 3, "3 rows for id=a");
        CHECK(nnull == 2, "tsdb_result_is_null reports the 2 missing temps");
        CHECK(bad == 0, "ts and id columns never report NULL");
        if (r) tsdb_result_free(r);
    }

    tsdb_close(db);
    rmrf(base);
}

/* ---- 9: the stated boundary — integers have no NULL -------------------- */

static void test_integer_columns_have_no_null(void) {
    char base[256];
    snprintf(base, sizeof(base), "/tmp/tsdb_nullsem_int_%d", (int)getpid());
    rmrf(base);

    tsdb_db_t *db = NULL;
    CHECK(tsdb_open(base, &db) == TSDB_OK, "open db (int)");
    if (!db) return;

    /* 'n' is an INT64 column (the 'i' suffix).  A 64-bit integer has no
     * spare bit pattern, so a missing one is still stored as 0 and still
     * counted.  This assertion is the honest boundary of the change, not a
     * desired behaviour: it fails the day integer NULLs are implemented,
     * which is exactly when it should be revisited. */
    const char *body =
        "i,id=a n=10i,hum=1.0 1000000000\n"
        "i,id=a hum=1.0 2000000000\n";
    size_t lines = 0, errors = 0;
    CHECK(tsdb_influx_ingest(db, body, strlen(body), &lines, &errors) == TSDB_OK,
          "ingest (int) ok");
    CHECK(errors == 0, "no ingest errors (int)");

    CHECK(q_i64(db, "SELECT count(n) FROM i") == 2,
          "BOUNDARY: count() over an INT64 column still counts the missing row");
    CHECK(q_i64(db, "SELECT min(n) FROM i") == 0,
          "BOUNDARY: min() over an INT64 column still sees the fabricated 0");

    tsdb_close(db);
    rmrf(base);
}

int main(void) {
    printf("=== NULL semantics: a missing value is not zero ===\n");
    test_across_storage_states();
    test_real_zero_is_not_null();
    test_first_last_groupby_isnull();
    test_integer_columns_have_no_null();

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
