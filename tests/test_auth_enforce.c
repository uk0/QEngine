/* test_auth_enforce.c — enforce RBAC via tsdb_query_auth.
 *
 * Verifies that tsdb_query_auth gates statements against the token's role +
 * grants, while tsdb_query continues to bypass enforcement (permissive MVP
 * model: v1 apps opt in to enforcement by calling the _auth variant).
 *
 *   Phase 1: bootstrap via bypass-path tsdb_query — create admin + normal
 *            user, a table, and obtain tokens for each user.
 *   Phase 2: SELECT via normal-user token without grant -> PERMISSION.
 *   Phase 3: GRANT SELECT ON t TO normal; retry -> OK.
 *   Phase 4: CREATE FUNCTION via normal-user token -> PERMISSION
 *            (ADMIN-only, even with GRANT DDL ON *).  Advisor's
 *            "non-negotiable, day one" requirement.
 *   Phase 5: GRANT DDL ON * TO normal; CREATE FUNCTION still PERMISSION.
 *   Phase 6: CREATE FUNCTION via admin token -> OK.
 *   Phase 7: unknown token -> PERMISSION.
 *   Phase 8: DROP FUNCTION via normal token -> PERMISSION (symmetric).
 *   Phase 9: bypass tsdb_query still works regardless of token state.
 */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); \
} while (0)
#define OK(rc)     do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c)  do { if (!(c)) FAIL("%s", #c); } while (0)
#define EQ_RC(rc, want) do {                                         \
    int _r = (rc);                                                    \
    if (_r != (want)) FAIL("got rc=%d (%s), want %d (%s)",            \
        _r, tsdb_errstr(_r), (want), tsdb_errstr(want));              \
} while (0)

static const char *SO_PATH = "build/test/udf_sample.so";

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

/* Run QTL that expects to succeed on the bypass path. */
static void bypass_ok(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("bypass '%s' rc=%d (%s)", sql, rc, tsdb_errstr(rc));
    if (r) tsdb_result_free(r);
}

/* Run QTL via tsdb_query_auth, return rc + discard result. */
static int auth_run(tsdb_db_t *db, const char *token, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query_auth(db, token, sql, &r);
    if (r) tsdb_result_free(r);
    return rc;
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_auth_enforce";
    rm_rf(dir);

    struct stat st;
    if (stat(SO_PATH, &st) != 0) {
        fprintf(stderr, "SKIP: %s not built. Run `make build/test/udf_sample.so` first.\n", SO_PATH);
        return 0;
    }

    printf("=== tsdb auth-enforcement tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    /* ---- Phase 1 — bootstrap via bypass path --------------------------- */
    printf("\n[1] bootstrap: create users + table\n");
    bypass_ok(db, "CREATE USER root IDENTIFIED BY 'root_pw' ROLE admin;");
    bypass_ok(db, "CREATE USER alice IDENTIFIED BY 'alice_pw';"); /* NORMAL */
    /* QTL has no CREATE TABLE for non-child tables; use the C API. */
    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    char tok_admin[64] = {0};
    char tok_alice[64] = {0};
    OK(tsdb_auth_authenticate(db, "root",  "root_pw",  tok_admin, sizeof(tok_admin)));
    OK(tsdb_auth_authenticate(db, "alice", "alice_pw", tok_alice, sizeof(tok_alice)));
    ASSERT(strlen(tok_admin) == 32);
    ASSERT(strlen(tok_alice) == 32);
    printf("  PASS: tokens issued for admin + normal user\n");

    /* ---- Phase 2 — SELECT as NORMAL without grant ---------------------- */
    printf("\n[2] NORMAL user SELECT without grant -> PERMISSION\n");
    EQ_RC(auth_run(db, tok_alice, "SELECT * FROM t"), TSDB_ERR_PERMISSION);
    printf("  PASS\n");

    /* ---- Phase 3 — GRANT SELECT + retry -------------------------------- */
    printf("\n[3] GRANT SELECT ON t TO alice; retry\n");
    bypass_ok(db, "GRANT SELECT ON t TO alice;");
    OK(auth_run(db, tok_alice, "SELECT * FROM t"));
    printf("  PASS: SELECT now allowed\n");

    /* ---- Phase 4 — CREATE FUNCTION without any grant: ADMIN-only ------- */
    printf("\n[4] NORMAL user CREATE FUNCTION -> PERMISSION (ADMIN-only)\n");
    char sql_create[1024];
    snprintf(sql_create, sizeof(sql_create),
        "CREATE FUNCTION f_double(FLOAT64) RETURNS FLOAT64 "
        "FROM '%s' SYMBOL 'udf_double';", SO_PATH);
    EQ_RC(auth_run(db, tok_alice, sql_create), TSDB_ERR_PERMISSION);
    printf("  PASS\n");

    /* ---- Phase 5 — GRANT DDL ON * still NOT enough --------------------- */
    printf("\n[5] GRANT DDL ON * TO alice; CREATE FUNCTION still PERMISSION\n");
    bypass_ok(db, "GRANT DDL ON * TO alice;");
    EQ_RC(auth_run(db, tok_alice, sql_create), TSDB_ERR_PERMISSION);
    printf("  PASS: DDL grant does not unlock UDF registration\n");

    /* ---- Phase 6 — ADMIN can register a UDF ---------------------------- */
    printf("\n[6] ADMIN token CREATE FUNCTION -> OK\n");
    OK(auth_run(db, tok_admin, sql_create));
    printf("  PASS\n");

    /* ---- Phase 7 — bogus token ----------------------------------------- */
    printf("\n[7] bogus token -> PERMISSION\n");
    EQ_RC(auth_run(db, "deadbeefdeadbeefdeadbeefdeadbeef",
                   "SELECT * FROM t"), TSDB_ERR_PERMISSION);
    EQ_RC(auth_run(db, "", "SELECT * FROM t"), TSDB_ERR_PERMISSION);
    printf("  PASS\n");

    /* ---- Phase 8 — NORMAL user DROP FUNCTION symmetric ----------------- */
    printf("\n[8] NORMAL user DROP FUNCTION -> PERMISSION\n");
    EQ_RC(auth_run(db, tok_alice, "DROP FUNCTION f_double"),
          TSDB_ERR_PERMISSION);
    printf("  PASS\n");

    /* ---- Phase 9 — bypass tsdb_query unchanged ------------------------- */
    printf("\n[9] bypass tsdb_query works without token\n");
    bypass_ok(db, "DROP FUNCTION f_double;");
    bypass_ok(db, "LIST FUNCTIONS;");
    printf("  PASS: bypass path unchanged\n");

    /* ---- Phase 10 — LIST FUNCTIONS is SELECT-on-* ---------------------- */
    printf("\n[10] LIST FUNCTIONS: denied until alice holds SELECT on *\n");
    /* alice has GRANT SELECT ON t only; listing functions uses resource="*"
     * which requires SELECT on "*", not on "t".  Expect PERMISSION. */
    EQ_RC(auth_run(db, tok_alice, "LIST FUNCTIONS;"), TSDB_ERR_PERMISSION);
    /* After GRANT SELECT ON * TO alice, LIST FUNCTIONS becomes allowed. */
    bypass_ok(db, "GRANT SELECT ON * TO alice;");
    OK(auth_run(db, tok_alice, "LIST FUNCTIONS;"));
    printf("  PASS: wildcard SELECT allows LIST FUNCTIONS\n");

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== ALL AUTH-ENFORCE TESTS PASSED ===\n");
    return 0;
}
