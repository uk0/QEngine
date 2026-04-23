/* test_rbac.c — Role-based access control tests.
 *
 * Phases:
 *   1. CREATE USER via QTL, authenticate with correct password
 *   2. Authenticate with wrong password -> TSDB_ERR_PERMISSION
 *   3. GRANT SELECT ON t1 TO alice -> auth_check(t1, SELECT) == TSDB_OK
 *   4. auth_check(t1, INSERT) == TSDB_ERR_PERMISSION
 *   5. auth_check(t2, SELECT) == TSDB_ERR_PERMISSION (wrong resource)
 *   6. GRANT ALL ON * TO alice -> auth_check works on any table/any priv
 *   7. REVOKE ALL ON * first, then GRANT + REVOKE SELECT ON t1
 *   8. ADMIN role -- auto-granted ALL on *
 *   9. Reopen db -> users + grants survive
 *
 * Phase 7 note: the spec says "REVOKE SELECT ON t1 FROM alice -> PERMISSION".
 * To reach that state cleanly after phase 6 (which granted ALL on *),
 * we first REVOKE ALL ON * FROM alice, then GRANT SELECT ON t1, then
 * REVOKE SELECT ON t1.  This keeps REVOKE semantics as "drop matching pair"
 * without inventing explicit-deny rules.
 */

#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* ---- Tiny harness ------------------------------------------------------- */

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do {                                   \
    if (cond) {                                                 \
        g_pass++; printf("PASS: %s\n", msg);                    \
    } else {                                                    \
        g_fail++;                                               \
        fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
    }                                                           \
} while (0)

#define CHECK_OK(rc, msg) CHECK((rc) == TSDB_OK, msg)

/* ---- Setup -------------------------------------------------------------- */

static const char *TEST_DIR = "/tmp/tsdb_test_rbac";

static void rmrf(const char *dir) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", dir);
    (void)system(cmd);
}

static void setup(void) {
    rmrf(TEST_DIR);
    mkdir(TEST_DIR, 0755);
}

/* Run a QTL statement, ignore result body (status row), return rc. */
static int run_qtl(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (r) tsdb_result_free(r);
    return rc;
}

/* ---- Tests -------------------------------------------------------------- */

int main(void) {
    setup();

    tsdb_db_t *db = NULL;
    CHECK_OK(tsdb_open(TEST_DIR, &db), "tsdb_open");
    if (!db) return 1;

    char alice_token[64] = {0};
    char admin_token[64] = {0};

    /* ---- Phase 1: create user via QTL + authenticate ------------------- */
    printf("\n[phase 1] CREATE USER + authenticate\n");
    CHECK_OK(run_qtl(db, "CREATE USER alice IDENTIFIED BY 'secret123';"),
             "CREATE USER alice via QTL");
    int rc = tsdb_auth_authenticate(db, "alice", "secret123",
                                     alice_token, sizeof(alice_token));
    CHECK(rc == TSDB_OK, "authenticate alice with correct password");
    CHECK(strlen(alice_token) == 32, "token is 32 hex chars");

    /* ---- Phase 2: wrong password -> PERMISSION ------------------------- */
    printf("\n[phase 2] wrong password\n");
    char tmp[64] = {0};
    rc = tsdb_auth_authenticate(db, "alice", "WRONG", tmp, sizeof(tmp));
    CHECK(rc == TSDB_ERR_PERMISSION, "wrong password -> TSDB_ERR_PERMISSION");

    rc = tsdb_auth_authenticate(db, "nosuch", "WRONG", tmp, sizeof(tmp));
    CHECK(rc == TSDB_ERR_PERMISSION, "unknown user -> TSDB_ERR_PERMISSION");

    /* ---- Phase 3: GRANT SELECT ON t1 TO alice -------------------------- */
    printf("\n[phase 3] GRANT SELECT ON t1 TO alice\n");
    CHECK_OK(run_qtl(db, "GRANT SELECT ON t1 TO alice;"),
             "GRANT SELECT ON t1 TO alice via QTL");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_SELECT, "t1");
    CHECK(rc == TSDB_OK, "auth_check(alice, SELECT, t1) == TSDB_OK");

    /* ---- Phase 4: INSERT not granted ----------------------------------- */
    printf("\n[phase 4] INSERT not granted\n");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_INSERT, "t1");
    CHECK(rc == TSDB_ERR_PERMISSION,
          "auth_check(alice, INSERT, t1) -> PERMISSION");

    /* ---- Phase 5: wrong resource --------------------------------------- */
    printf("\n[phase 5] wrong resource\n");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_SELECT, "t2");
    CHECK(rc == TSDB_ERR_PERMISSION,
          "auth_check(alice, SELECT, t2) -> PERMISSION (wrong resource)");

    /* ---- Phase 6: GRANT ALL ON * TO alice ------------------------------ */
    printf("\n[phase 6] GRANT ALL ON * TO alice\n");
    CHECK_OK(run_qtl(db, "GRANT ALL ON * TO alice;"),
             "GRANT ALL ON * TO alice via QTL");

    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_SELECT, "t_any");
    CHECK(rc == TSDB_OK, "ALL on * -> SELECT on any table ok");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_INSERT, "t_any");
    CHECK(rc == TSDB_OK, "ALL on * -> INSERT on any table ok");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_DDL, "t_any");
    CHECK(rc == TSDB_OK, "ALL on * -> DDL on any table ok");

    /* ---- Phase 7: REVOKE SELECT ON t1 FROM alice ----------------------- */
    printf("\n[phase 7] REVOKE ALL ON *, then GRANT + REVOKE SELECT ON t1\n");
    /* First undo the wildcard grant so the remaining grants are narrow. */
    CHECK_OK(run_qtl(db, "REVOKE ALL ON * FROM alice;"),
             "REVOKE ALL ON * FROM alice");
    /* Also remove the existing narrow grant on t1 so later REVOKE starts
     * from a known state. */
    CHECK_OK(run_qtl(db, "REVOKE SELECT ON t1 FROM alice;"),
             "REVOKE SELECT ON t1 FROM alice (clean slate)");
    /* Now grant SELECT ON t1 fresh. */
    CHECK_OK(run_qtl(db, "GRANT SELECT ON t1 TO alice;"),
             "GRANT SELECT ON t1 TO alice");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_SELECT, "t1");
    CHECK(rc == TSDB_OK, "pre-revoke: SELECT on t1 ok");
    /* Revoke it. */
    CHECK_OK(run_qtl(db, "REVOKE SELECT ON t1 FROM alice;"),
             "REVOKE SELECT ON t1 FROM alice");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_SELECT, "t1");
    CHECK(rc == TSDB_ERR_PERMISSION,
          "after REVOKE: SELECT on t1 -> PERMISSION");

    /* ---- Phase 8: ADMIN role ------------------------------------------- */
    printf("\n[phase 8] ADMIN role\n");
    /* Use a fresh name; the default admin ('root') is auto-seeded on
     * tsdb_open so we cannot re-CREATE it here. */
    CHECK_OK(run_qtl(db, "CREATE USER admin1 IDENTIFIED BY 'pw' ROLE ADMIN;"),
             "CREATE USER admin1 ROLE ADMIN");
    rc = tsdb_auth_authenticate(db, "admin1", "pw",
                                 admin_token, sizeof(admin_token));
    CHECK(rc == TSDB_OK, "authenticate admin1");
    rc = tsdb_auth_check(db, admin_token, TSDB_PRIV_SELECT, "random_t");
    CHECK(rc == TSDB_OK, "ADMIN -> SELECT on random_t ok (implicit ALL on *)");
    rc = tsdb_auth_check(db, admin_token, TSDB_PRIV_DDL, "foo");
    CHECK(rc == TSDB_OK, "ADMIN -> DDL on foo ok");
    rc = tsdb_auth_check(db, admin_token, TSDB_PRIV_INSERT, "bar");
    CHECK(rc == TSDB_OK, "ADMIN -> INSERT on bar ok");

    /* ---- Enforcement stub flag ----------------------------------------- */
    printf("\n[extra] tsdb_auth_enforce flag\n");
    /* Default: auth_enforce = false -> enforce always returns OK. */
    rc = tsdb_auth_enforce(db, "bogus_token", TSDB_PRIV_SELECT, "anywhere");
    CHECK(rc == TSDB_OK, "enforce OFF -> bogus token passes");
    tsdb_auth_set_enforce(db, true);
    rc = tsdb_auth_enforce(db, "bogus_token", TSDB_PRIV_SELECT, "anywhere");
    CHECK(rc == TSDB_ERR_PERMISSION,
          "enforce ON -> bogus token -> PERMISSION");
    tsdb_auth_set_enforce(db, false);

    /* Also verify ALTER USER SET PASSWORD works. */
    printf("\n[extra] ALTER USER SET PASSWORD\n");
    CHECK_OK(run_qtl(db, "ALTER USER alice SET PASSWORD 'newsecret';"),
             "ALTER USER alice SET PASSWORD");
    rc = tsdb_auth_authenticate(db, "alice", "secret123", tmp, sizeof(tmp));
    CHECK(rc == TSDB_ERR_PERMISSION, "old password rejected after alter");
    rc = tsdb_auth_authenticate(db, "alice", "newsecret",
                                 alice_token, sizeof(alice_token));
    CHECK(rc == TSDB_OK, "new password accepted after alter");

    /* LIST USERS should return a result set. */
    printf("\n[extra] LIST USERS returns rows\n");
    tsdb_result_t *lu = NULL;
    CHECK_OK(tsdb_query(db, "LIST USERS;", &lu), "LIST USERS query");
    CHECK(lu != NULL, "LIST USERS result not null");
    if (lu) {
        int nrow = 0;
        while (tsdb_result_next(lu) > 0) nrow++;
        CHECK(nrow >= 2, "LIST USERS returns at least 2 rows (alice+root)");
        tsdb_result_free(lu);
    }

    /* ---- Phase 9: reopen -> users + grants survive --------------------- */
    printf("\n[phase 9] close + reopen\n");
    tsdb_close(db);
    db = NULL;
    CHECK_OK(tsdb_open(TEST_DIR, &db), "tsdb_open (reopen)");
    /* After reopen, tokens are invalidated; must re-authenticate. */
    rc = tsdb_auth_authenticate(db, "alice", "newsecret",
                                 alice_token, sizeof(alice_token));
    CHECK(rc == TSDB_OK, "post-reopen: authenticate alice with newsecret");

    /* The SELECT ON t1 grant was revoked before close — stays revoked. */
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_SELECT, "t1");
    CHECK(rc == TSDB_ERR_PERMISSION,
          "post-reopen: revoked SELECT on t1 stays revoked");

    /* Grant a fresh SELECT, close, reopen, verify it persists. */
    CHECK_OK(run_qtl(db, "GRANT INSERT ON tx TO alice;"),
             "post-reopen: GRANT INSERT ON tx TO alice");
    tsdb_close(db);
    CHECK_OK(tsdb_open(TEST_DIR, &db), "reopen #2");
    rc = tsdb_auth_authenticate(db, "alice", "newsecret",
                                 alice_token, sizeof(alice_token));
    CHECK(rc == TSDB_OK, "reopen #2: authenticate alice");
    rc = tsdb_auth_check(db, alice_token, TSDB_PRIV_INSERT, "tx");
    CHECK(rc == TSDB_OK, "reopen #2: INSERT on tx survives reopen");

    /* ADMIN role also survives reopen. */
    rc = tsdb_auth_authenticate(db, "admin1", "pw",
                                 admin_token, sizeof(admin_token));
    CHECK(rc == TSDB_OK, "reopen #2: authenticate admin1 (ADMIN)");
    rc = tsdb_auth_check(db, admin_token, TSDB_PRIV_DDL, "anything");
    CHECK(rc == TSDB_OK, "reopen #2: admin1 ADMIN -> DDL on anything");

    /* Drop user + verify. */
    CHECK_OK(run_qtl(db, "DROP USER alice;"), "DROP USER alice");
    rc = tsdb_auth_authenticate(db, "alice", "newsecret",
                                 alice_token, sizeof(alice_token));
    CHECK(rc == TSDB_ERR_PERMISSION,
          "after DROP: alice authenticate -> PERMISSION");

    tsdb_close(db);
    rmrf(TEST_DIR);

    printf("\n--- test_rbac summary ---\n");
    printf("PASS: %d\n", g_pass);
    printf("FAIL: %d\n", g_fail);
    return g_fail == 0 ? 0 : 1;
}
