/* test_show_introspection.c — SHOW DATABASES / GROUPS / STABLES / TABLES.
 *
 * An operator could not list what was in the database: all three SHOW forms
 * were parse errors, and only LIST existed.  This pins the surface AND the
 * one thing about it that is not cosmetic — its scope.
 *
 *  1. The four SHOW forms parse and execute; so do their IN DATABASE /
 *     IN GROUP / USING filters and lowercase spellings.
 *  2. SHOW carries a cluster-scope claim, so it is restricted to the four
 *     hierarchy nouns: SHOW USERS is a parse error that points at LIST,
 *     because users are not certified across the cluster here.
 *  3. `show` stays a usable identifier — it is matched at statement start,
 *     not added to the keyword table.
 *  4. SHOW returns LIST's columns plus `on_nodes` = "held-by/consulted",
 *     over the same row set.  On a standalone node that is "1/1": one
 *     replica consulted, one holds it.
 *  5. The USING / IN filters actually filter.
 *  6. A peer's listing survives the federation wire WITH ITS STRINGS.
 *     fedrpc shipped SYMBOL columns as bare uint32 codes and left
 *     col_symtab NULL, so tsdb_result_sym() on any decoded result returned
 *     NULL — every name, database and group in a peer's LIST arrived empty.
 *     Nothing caught it because the only pre-existing consumer (the
 *     stable-aggregate scatter) ships numeric partials.  SHOW's peer legs
 *     ride this exact path, so the dictionary is load-bearing for it.
 *  7. The dictionary is additive: a numeric-only result still encodes to
 *     exactly the pre-dictionary byte length, so a stable-aggregate partial
 *     is unchanged on the wire.
 *
 * The multi-replica union, the on_nodes divergence count and the
 * refuse-when-a-node-is-unreachable gate need more than one node and live in
 * tests/test_cluster_show.c (run by `make test-cluster`).
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/query/result_internal.h"
#include "../src/federation/fedagg.h"
#include "../src/federation/fedrpc.h"
#include "../src/cluster/rpc.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); \
} while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

static const char *DIR_ = "/tmp/tsdb_test_show_introspection";

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char s[4096]; snprintf(s, sizeof(s), "%s/%s", p, e->d_name); rm_rf(s);
    }
    closedir(d); rmdir(p);
}

static void run_ddl(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("DDL failed rc=%d (%s): %s", rc, tsdb_errstr(rc), sql);
    if (r) tsdb_result_free(r);
}

static int query_rc(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (r) tsdb_result_free(r);
    return rc;
}

/* Collect column 0 of a result as a sorted, newline-joined string so two
 * listings can be compared as sets. */
static void names_of(tsdb_db_t *db, const char *sql, char *out, size_t cap) {
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, sql, &r));
    ASSERT(r);
    char rows[64][96];
    int  n = 0;
    while (tsdb_result_next(r) == 1 && n < 64) {
        const char *s = tsdb_result_sym(r, 0);
        snprintf(rows[n++], sizeof(rows[0]), "%s", s ? s : "(null)");
    }
    tsdb_result_free(r);
    for (int i = 1; i < n; i++) {
        char tmp[96];
        snprintf(tmp, sizeof(tmp), "%s", rows[i]);
        int j = i - 1;
        while (j >= 0 && strcmp(rows[j], tmp) > 0) {
            snprintf(rows[j + 1], sizeof(rows[0]), "%s", rows[j]);
            j--;
        }
        snprintf(rows[j + 1], sizeof(rows[0]), "%s", tmp);
    }
    out[0] = '\0';
    size_t used = 0;
    for (int i = 0; i < n; i++)
        used += (size_t)snprintf(out + used, cap - used, "%s\n", rows[i]);
}

static size_t count_rows(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, sql, &r));
    ASSERT(r);
    size_t n = 0;
    while (tsdb_result_next(r) == 1) n++;
    tsdb_result_free(r);
    return n;
}

/* ---- 6/7: federation wire ------------------------------------------------ */

/* Legacy (pre-dictionary) encoded size: ncols u8 + per-col (len u8 + name +
 * type u8) + nrows u32 + ncols * nrows * 8. */
static int legacy_encoded_len(tsdb_result_t *r) {
    int n = 1 + 4;
    for (int c = 0; c < r->ncols; c++) n += 1 + (int)strlen(r->col_names[c]) + 1;
    n += r->ncols * (int)r->nrows * 8;
    return n;
}

static void check_fed_symbols(tsdb_db_t *db) {
    tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", db, NULL);
    ASSERT(srv != NULL);
    int port = tsdb_rpc_server_port(srv);
    ASSERT(port > 0);
    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    ASSERT(conn != NULL);

    /* Exactly what a SHOW peer leg sends. */
    tsdb_result_t *r = NULL;
    OK(fedrpc_query_local(conn, "LIST PTABLES", 5000, &r));
    ASSERT(r);
    ASSERT(r->ncols == 6);
    ASSERT(r->nrows == 3);

    int seen_d1 = 0, seen_d2 = 0, seen_p1 = 0;
    while (tsdb_result_next(r) == 1) {
        const char *name   = tsdb_result_sym(r, 0);
        const char *vtable = tsdb_result_sym(r, 1);
        if (!name)
            FAIL("peer listing arrived with a NULL name — symbol codes shipped "
                 "without their strings");
        if (!strcmp(name, "d1")) { seen_d1 = 1; ASSERT(vtable && !strcmp(vtable, "meters")); }
        if (!strcmp(name, "d2")) { seen_d2 = 1; ASSERT(vtable && !strcmp(vtable, "meters")); }
        if (!strcmp(name, "p1")) { seen_p1 = 1; ASSERT(vtable && !strcmp(vtable, "power")); }
    }
    ASSERT(seen_d1 && seen_d2 && seen_p1);
    tsdb_result_free(r);
    printf("  PASS: peer listing keeps its strings across FED_QUERY_LOCAL\n");

    tsdb_rpc_conn_close(conn);
    tsdb_rpc_server_stop(srv);
}

static void check_numeric_encoding_unchanged(void) {
    const char *names[2] = { "count(ts)", "sum(v)" };
    tsdb_type_t types[2] = { TSDB_TYPE_INT64, TSDB_TYPE_FLOAT64 };
    tsdb_result_t *r = fedagg_result_alloc(2, names, types);
    ASSERT(r);
    uint64_t row[2] = { 42, 7 };
    ASSERT(fedagg_result_append(r, row) == 0);

    uint8_t buf[512];
    int n = fedrpc_encode_result(buf, sizeof(buf), r);
    ASSERT(n > 0);
    if (n != legacy_encoded_len(r))
        FAIL("numeric-only partial changed size on the wire: %d vs %d",
             n, legacy_encoded_len(r));

    tsdb_result_t *back = NULL;
    OK(fedrpc_decode_result(buf, (uint32_t)n, &back));
    ASSERT(back && tsdb_result_next(back) == 1);
    ASSERT(tsdb_result_i64(back, 0) == 42);
    tsdb_result_free(back);
    fedagg_result_free(r);
    printf("  PASS: numeric-only partial is byte-length identical\n");
}

int main(void) {
    rm_rf(DIR_);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(DIR_, &db));

    run_ddl(db, "CREATE DATABASE bench");
    run_ddl(db, "CREATE GROUP g_sensors IN DATABASE bench");
    run_ddl(db, "CREATE GROUP g_meters IN DATABASE bench");
    run_ddl(db, "CREATE STABLE meters (ts TIMESTAMP, v FLOAT64) TAGS (loc SYMBOL) "
                "IN DATABASE bench GROUP g_sensors");
    run_ddl(db, "CREATE STABLE power (ts TIMESTAMP, w FLOAT64) TAGS (loc SYMBOL) "
                "IN DATABASE bench GROUP g_meters");
    run_ddl(db, "CREATE TABLE d1 USING meters TAGS ('east')");
    run_ddl(db, "CREATE TABLE d2 USING meters TAGS ('west')");
    run_ddl(db, "CREATE TABLE p1 USING power TAGS ('north')");

    /* ── 1: the four SHOW forms parse and execute ───────────────────────── */
    printf("\n[1] SHOW DATABASES / GROUPS / STABLES / TABLES parse\n");
    {
        static const char *stmts[] = {
            "SHOW DATABASES", "SHOW GROUPS", "SHOW STABLES", "SHOW TABLES",
            "show tables", "Show Stables",
            "SHOW GROUPS IN DATABASE bench",
            "SHOW STABLES IN DATABASE bench",
            "SHOW STABLES IN GROUP g_sensors",
            "SHOW TABLES IN DATABASE bench IN GROUP g_sensors",
            "SHOW TABLES USING meters",
            "SHOW TABLES USING meters IN DATABASE bench",
            "SHOW TABLES;",
        };
        for (size_t i = 0; i < sizeof(stmts) / sizeof(stmts[0]); i++) {
            int rc = query_rc(db, stmts[i]);
            if (rc != TSDB_OK) FAIL("`%s` -> rc=%d (%s)", stmts[i], rc, tsdb_errstr(rc));
        }
        printf("  PASS: %zu SHOW forms accepted\n", sizeof(stmts) / sizeof(stmts[0]));
    }

    /* ── 2: SHOW covers only what it can certify ────────────────────────── */
    printf("\n[2] SHOW is limited to the four hierarchy nouns\n");
    {
        ASSERT(query_rc(db, "SHOW USERS")     == TSDB_ERR_PARSE);
        ASSERT(query_rc(db, "SHOW DEVICES")   == TSDB_ERR_PARSE);
        ASSERT(query_rc(db, "SHOW FUNCTIONS") == TSDB_ERR_PARSE);
        ASSERT(query_rc(db, "SHOW MASTERS")   == TSDB_ERR_PARSE);
        ASSERT(query_rc(db, "SHOW NONSENSE")  == TSDB_ERR_PARSE);
        /* LIST keeps every noun it had. */
        OK(query_rc(db, "LIST USERS"));
        OK(query_rc(db, "LIST DEVICES"));
        OK(query_rc(db, "LIST FUNCTIONS"));
        OK(query_rc(db, "LIST TABLES"));
        printf("  PASS: SHOW rejects the uncertified nouns, LIST keeps them\n");
    }

    /* ── 3: `show` is still an identifier ───────────────────────────────── */
    printf("\n[3] `show` stays usable as a name\n");
    {
        run_ddl(db, "CREATE TABLE t_show (ts TIMESTAMP, show INT64) TIMESTAMP(ts)");
        OK(query_rc(db, "SELECT show FROM t_show"));
        printf("  PASS: `show` still lexes as an identifier\n");
    }

    /* ── 4: SHOW = LIST columns + on_nodes, same rows ───────────────────── */
    printf("\n[4] SHOW adds on_nodes over LIST's row set\n");
    {
        static const struct { const char *show; const char *list; int list_ncols; }
        pairs[] = {
            { "SHOW DATABASES", "LIST DATABASES", 5 },
            { "SHOW GROUPS",    "LIST GROUPS",    8 },
            { "SHOW STABLES",   "LIST STABLES",   6 },
            { "SHOW TABLES",    "LIST TABLES",    6 },
        };
        for (size_t i = 0; i < sizeof(pairs) / sizeof(pairs[0]); i++) {
            tsdb_result_t *r = NULL;
            OK(tsdb_query(db, pairs[i].show, &r));
            ASSERT(r);
            if (tsdb_result_ncols(r) != pairs[i].list_ncols + 1)
                FAIL("`%s` has %d cols, want %d",
                     pairs[i].show, tsdb_result_ncols(r), pairs[i].list_ncols + 1);
            const char *last = tsdb_result_col_name(r, pairs[i].list_ncols);
            if (!last || strcmp(last, "on_nodes") != 0)
                FAIL("`%s` last column is `%s`, want on_nodes",
                     pairs[i].show, last ? last : "(null)");
            size_t nrows = 0;
            while (tsdb_result_next(r) == 1) {
                const char *held = tsdb_result_sym(r, pairs[i].list_ncols);
                if (!held || strcmp(held, "1/1") != 0)
                    FAIL("`%s` row on_nodes=%s, want 1/1 on a standalone node",
                         pairs[i].show, held ? held : "(null)");
                nrows++;
            }
            ASSERT(nrows > 0);
            tsdb_result_free(r);

            char a[4096], b[4096];
            names_of(db, pairs[i].show, a, sizeof(a));
            names_of(db, pairs[i].list, b, sizeof(b));
            if (strcmp(a, b) != 0)
                FAIL("`%s` and `%s` disagree:\n--- SHOW ---\n%s--- LIST ---\n%s",
                     pairs[i].show, pairs[i].list, a, b);
        }
        printf("  PASS: 4 SHOW forms match LIST row-for-row, plus on_nodes\n");
    }

    /* ── 5: the filters filter ──────────────────────────────────────────── */
    printf("\n[5] SHOW filters\n");
    {
        ASSERT(count_rows(db, "SHOW TABLES USING meters") == 2);
        ASSERT(count_rows(db, "SHOW TABLES USING power")  == 1);
        ASSERT(count_rows(db, "SHOW STABLES IN GROUP g_meters") == 1);
        ASSERT(count_rows(db, "SHOW GROUPS IN DATABASE bench") == 2);
        /* An unknown scope is empty, not everything. */
        ASSERT(count_rows(db, "SHOW TABLES IN GROUP nosuchgroup") == 0);
        printf("  PASS: USING / IN DATABASE / IN GROUP narrow the listing\n");
    }

    /* ── 6/7: federation wire ───────────────────────────────────────────── */
    printf("\n[6] peer listing over FED_QUERY_LOCAL\n");
    check_fed_symbols(db);
    printf("\n[7] numeric partials unchanged on the wire\n");
    check_numeric_encoding_unchanged();

    tsdb_close(db);
    rm_rf(DIR_);
    printf("\n=== ALL SHOW INTROSPECTION TESTS PASSED ===\n");
    return 0;
}
