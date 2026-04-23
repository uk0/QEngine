/* test_auth_wire.c — end-to-end TCP auth flow.
 *
 * In-process server (same pattern as test_server.c: no fork).  Uses the
 * server-side proto helpers (tsdb_proto_send / tsdb_proto_recv) directly
 * so the test is independent of cli/tsdb_wire.c.
 *
 * Phases:
 *   1. require_auth=true:   QUERY before AUTH_LOGIN  → TSDB_MT_ERROR/PERMISSION
 *   2. AUTH_LOGIN(admin)    → TSDB_MT_AUTH_OK + 32-byte hex token
 *   3. QUERY (same conn)    → HDR/ROWS (authenticated success)
 *   4. New conn, wrong pw   → ERROR/PERMISSION, state cleared
 *   5. AUTH_LOGIN(normal)   → OK; CREATE FUNCTION rejected (ADMIN-only)
 *   6. require_auth=false:  QUERY without AUTH_LOGIN → HDR (bypass path kept)
 */

#include "../src/server/server.h"
#include "../src/server/proto.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); g_fail++; } \
    else         { printf("PASS: %s\n", msg); g_pass++; } \
} while (0)

static int client_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    for (int i = 0; i < 50; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) return fd;
        if (errno == ECONNREFUSED) { usleep(20000); continue; }
        break;
    }
    close(fd);
    return -1;
}

/* Send AUTH_LOGIN frame with <user> + <pass>. */
static int send_login(int fd, uint64_t req_id, const char *user, const char *pass) {
    uint8_t buf[300];
    uint8_t ulen = (uint8_t)strlen(user);
    uint8_t plen = (uint8_t)strlen(pass);
    size_t  off  = 0;
    buf[off++] = ulen; memcpy(buf + off, user, ulen); off += ulen;
    buf[off++] = plen; memcpy(buf + off, pass, plen); off += plen;
    return tsdb_proto_send(fd, TSDB_MT_AUTH_LOGIN, 0, req_id, buf, off);
}

/* Send QUERY frame with raw QTL (no u16 prefix — matches test_server.c's path). */
static int send_query(int fd, uint64_t req_id, const char *qtl) {
    return tsdb_proto_send(fd, TSDB_MT_QUERY, 0, req_id,
                            (const uint8_t *)qtl, strlen(qtl));
}

/* Minimal well-formed WRITE_BATCH payload: just the table name; the server's
 * auth gate fires before it reads the columnar body, so the rest can be empty. */
static int send_write_stub(int fd, uint64_t req_id, const char *table) {
    uint8_t buf[64];
    uint8_t tnlen = (uint8_t)strlen(table);
    buf[0] = tnlen;
    memcpy(buf + 1, table, tnlen);
    return tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, req_id,
                           buf, (size_t)(1 + tnlen));
}

/* Minimal DROP_TABLE payload: just the table name bytes. */
static int send_drop_table(int fd, uint64_t req_id, const char *table) {
    return tsdb_proto_send(fd, TSDB_MT_DROP_TABLE, 0, req_id,
                           (const uint8_t *)table, strlen(table));
}

static int recv_frame(int fd, tsdb_frame_hdr_t *hdr, uint8_t **payload) {
    return tsdb_proto_recv(fd, hdr, payload);
}

/* Decode error payload: first 4 bytes LE = rc. */
static int payload_err_rc(const uint8_t *p) {
    if (!p) return 0;
    int32_t rc;
    memcpy(&rc, p, 4);
    return (int)rc;
}

/* ---- Server bootstrap ---------------------------------------------------- */

static tsdb_db_t     *g_db  = NULL;
static tsdb_server_t *g_srv = NULL;
static char           g_dir[128];

static void rmrf(const char *dir) { char cmd[512]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd); }

static int start_server(bool require_auth) {
    snprintf(g_dir, sizeof(g_dir), "/tmp/tsdb_test_auth_wire_%d", (int)getpid());
    rmrf(g_dir);

    int rc = tsdb_open(g_dir, &g_db);
    if (rc != TSDB_OK) return rc;

    /* Bootstrap admin + normal user via bypass. 'root' is auto-seeded
     * by tsdb_open, so only an ALTER is needed to reset its password. */
    tsdb_result_t *r = NULL;
    tsdb_query(g_db, "ALTER USER root SET PASSWORD 'root_pw';", &r);
    if (r) tsdb_result_free(r); r = NULL;
    tsdb_query(g_db, "CREATE USER alice IDENTIFIED BY 'alice_pw';", &r);
    if (r) tsdb_result_free(r); r = NULL;

    /* Create a table for SELECT testing. */
    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_INT64},
    };
    tsdb_create_table(g_db, "t", cols, 2, "ts");

    /* Alice needs SELECT grant on t for the read test. */
    tsdb_query(g_db, "GRANT SELECT ON t TO alice;", &r);
    if (r) tsdb_result_free(r);

    tsdb_server_opts_t opts = {
        .bind_addr    = "127.0.0.1:0",
        .max_conns    = 32,
        .db           = g_db,
        .require_auth = require_auth,
    };
    rc = tsdb_server_start(&opts, &g_srv);
    if (rc != TSDB_OK) return rc;
    usleep(20000);
    return 0;
}

static void stop_server(void) {
    if (g_srv) { tsdb_server_stop(g_srv); g_srv = NULL; }
    if (g_db)  { tsdb_close(g_db); g_db = NULL; }
    rmrf(g_dir);
}

/* ---- Phase 1 + 2 + 3 ----------------------------------------------------- */

static void phase_deny_without_login(int port) {
    printf("\n[1] require_auth=true: QUERY before LOGIN -> ERROR/PERMISSION\n");
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;

    send_query(fd, 1, "SELECT * FROM t");

    tsdb_frame_hdr_t hdr;
    uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_ERROR, "type == ERROR");
    CHECK(payload_err_rc(pl) == TSDB_ERR_PERMISSION, "rc == PERMISSION");
    free(pl);
    close(fd);
}

static void phase_login_then_query(int port) {
    printf("\n[2+3] AUTH_LOGIN(root) -> AUTH_OK; QUERY -> HDR\n");
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;

    send_login(fd, 10, "root", "root_pw");
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_AUTH_OK, "type == AUTH_OK");
    CHECK(hdr.payload_len == 32, "token is 32 bytes");
    /* Token must be lowercase hex. */
    int all_hex = (pl != NULL);
    for (uint32_t i = 0; pl && i < hdr.payload_len; i++) {
        char c = (char)pl[i];
        int h = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        if (!h) { all_hex = 0; break; }
    }
    CHECK(all_hex, "token is lowercase hex");
    free(pl); pl = NULL;

    /* Query on the same conn should now work. */
    send_query(fd, 11, "SELECT * FROM t");
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_QUERY_RESULT_HDR, "QUERY answered with HDR");
    free(pl);
    close(fd);
}

/* ---- Phase 4 ------------------------------------------------------------- */

static void phase_wrong_password(int port) {
    printf("\n[4] wrong password -> ERROR/PERMISSION + state cleared\n");
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;

    send_login(fd, 20, "root", "WRONG");
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_ERROR, "type == ERROR");
    CHECK(payload_err_rc(pl) == TSDB_ERR_PERMISSION, "rc == PERMISSION");
    free(pl); pl = NULL;

    /* Try QUERY: must still be denied (no token installed). */
    send_query(fd, 21, "SELECT * FROM t");
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_ERROR, "QUERY still denied after failed login");
    free(pl);
    close(fd);
}

/* ---- Phase 5 ------------------------------------------------------------- */

static void phase_normal_cannot_create_function(int port) {
    printf("\n[5] NORMAL user via wire: CREATE FUNCTION -> PERMISSION\n");
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;

    send_login(fd, 30, "alice", "alice_pw");
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_AUTH_OK, "alice LOGIN OK");
    free(pl); pl = NULL;

    /* alice has SELECT on t; SELECT should succeed. */
    send_query(fd, 31, "SELECT * FROM t");
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_QUERY_RESULT_HDR, "alice SELECT t OK");
    /* drain to FIN so the server doesn't block on the next request */
    while (!(hdr.flags & TSDB_FLAG_FIN)) {
        free(pl); pl = NULL;
        if (recv_frame(fd, &hdr, &pl) != TSDB_OK) break;
    }
    free(pl); pl = NULL;

    /* alice tries CREATE FUNCTION — must be denied (ADMIN-only). */
    send_query(fd, 32,
        "CREATE FUNCTION foo(INT64) RETURNS INT64 "
        "FROM '/tmp/nope.so' SYMBOL 'x'");
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_ERROR, "alice CREATE FUNCTION -> ERROR");
    CHECK(payload_err_rc(pl) == TSDB_ERR_PERMISSION, "rc == PERMISSION");
    free(pl);
    close(fd);
}

/* ---- Phase 7 + 8 — gate covers WRITE_BATCH / DROP_TABLE too -------------- */

static void phase_write_drop_gated(int port) {
    printf("\n[7] WRITE_BATCH before LOGIN -> ERROR/PERMISSION\n");
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;

    send_write_stub(fd, 70, "t");
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_ERROR, "WRITE_BATCH denied");
    CHECK(payload_err_rc(pl) == TSDB_ERR_PERMISSION, "rc == PERMISSION");
    free(pl); pl = NULL;
    close(fd);

    printf("\n[8] DROP_TABLE before LOGIN -> ERROR/PERMISSION\n");
    fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;
    send_drop_table(fd, 80, "t");
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_ERROR, "DROP_TABLE denied");
    CHECK(payload_err_rc(pl) == TSDB_ERR_PERMISSION, "rc == PERMISSION");
    free(pl);
    close(fd);
}

/* ---- Phase 6 ------------------------------------------------------------- */

static void phase_bypass_mode(void) {
    printf("\n[6] require_auth=false: QUERY without LOGIN works\n");
    /* Restart with require_auth=false. */
    stop_server();
    int rc = start_server(false);
    CHECK(rc == 0, "server restarted with require_auth=false");
    if (rc != 0) return;
    int port = tsdb_server_port(g_srv);

    int fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;
    send_query(fd, 40, "SELECT * FROM t");
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl);
    CHECK(hdr.type == TSDB_MT_QUERY_RESULT_HDR, "bypass QUERY answered with HDR");
    free(pl);
    close(fd);
}

int main(void) {
    printf("=== test_auth_wire ===\n");

    if (start_server(true) != 0) {
        fprintf(stderr, "server start failed\n");
        return 1;
    }
    int port = tsdb_server_port(g_srv);
    printf("server on port %d (require_auth=true)\n", port);

    phase_deny_without_login(port);
    phase_login_then_query(port);
    phase_wrong_password(port);
    phase_normal_cannot_create_function(port);
    phase_write_drop_gated(port);
    phase_bypass_mode();

    stop_server();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
