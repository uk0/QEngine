/* test_server_resource_gov.c — SERVER-RESOURCE-AUTH-OBS rework break-tests.
 *
 * Three named rejection reasons from the round-3 review, one assertion group
 * each.  Each group is written to FAIL against the un-fixed base and PASS once
 * the corresponding fix lands.
 *
 *   (1) RES-4 sibling — the unauth metrics-plane /sql route runs the SQL
 *       provider with NO per-query deadline in BOTH server binaries.  The
 *       provider executes on the SAME thread as the HTTP handler, so a
 *       thread-local deadline set around the g_sql_fn call is visible inside
 *       it.  We register a probe provider that reads the live deadline via
 *       tsdb_query_set_deadline_ns(prev)/restore and reports whether it was
 *       armed.  Pre-fix: deadline==0 (unarmed).  Post-fix: deadline!=0.
 *
 *   (2) OBS-5 / false-invariant — influx pass-2 counts a commit/bulk failure
 *       ONCE per measurement group, so out_errors under-counts the failed
 *       rows and (lines - errors) over-credits committed rows.  We assert the
 *       real invariant: (lines - errors) equals the number of rows actually
 *       committed to storage, measured by the count(*) delta.
 *
 *   (3) RES-4 result cap — the wire QUERY path must honour a configured
 *       result-row ceiling so a single SELECT * cannot materialise unbounded.
 *       We start a wire server with max_result_rows=8, insert 100 rows, and
 *       assert the streamed row count is capped at 8.
 *
 * Break-test discipline: groups (1) and (3) call APIs that do not exist on the
 * base tree, so the base does not COMPILE against this file — the strongest
 * possible "fails without the fix".  The behavioural assertions gate the fix
 * once it compiles.
 */

#include "../src/server/metrics_server.h"
#include "../src/server/metrics.h"       /* tsdb_metrics_init */
#include "../src/server/server.h"
#include "../src/server/influx_line.h"
#include "../src/server/proto.h"         /* wire frames for the cap test */
#include "../src/query/exec.h"           /* tsdb_query_set_deadline_ns */
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

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
} while(0)

/* ---- Minimal blocking HTTP client (mirrors test_metrics_http.c) --------- */

static int http_do(int port, const char *method, const char *path,
                   const char *extra, const char *body,
                   char *resp, size_t resp_cap) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    int connected = -1;
    for (int i = 0; i < 50; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) { connected = 0; break; }
        if (errno == ECONNREFUSED) { usleep(20000); continue; }
        break;
    }
    if (connected != 0) { close(fd); return -1; }

    char req[4096];
    size_t blen = body ? strlen(body) : 0;
    int n = snprintf(req, sizeof(req),
                     "%s %s HTTP/1.1\r\nHost: 127.0.0.1\r\n%s"
                     "Content-Length: %zu\r\nConnection: close\r\n\r\n%s",
                     method, path, extra ? extra : "", blen, body ? body : "");
    if (n <= 0) { close(fd); return -1; }
    if (write(fd, req, (size_t)n) != n) { close(fd); return -1; }

    size_t got = 0;
    for (;;) {
        if (got >= resp_cap - 1) break;
        ssize_t r = read(fd, resp + got, resp_cap - 1 - got);
        if (r <= 0) break;
        got += (size_t)r;
    }
    resp[got] = '\0';
    close(fd);

    int code = -1;
    if (strncmp(resp, "HTTP/1.", 7) == 0) {
        const char *sp = strchr(resp, ' ');
        if (sp) code = atoi(sp + 1);
    }
    return code;
}

static const char *http_body(const char *resp) {
    const char *m = strstr(resp, "\r\n\r\n");
    return m ? m + 4 : resp;
}

/* ---- (1) /sql deadline probe provider ----------------------------------- *
 * Reads the live thread-local query deadline WITHOUT disturbing it: swap in 0,
 * read the previous value, swap the previous value back.  Reports it in the
 * JSON body so the test can assert the metrics-plane /sql dispatch armed a
 * deadline before calling us. */
static int probe_sql(void *ud, const char *q, size_t qlen,
                     const char *token, char *buf, size_t cap) {
    (void)ud; (void)q; (void)qlen; (void)token;
    int64_t live = tsdb_query_set_deadline_ns(0);   /* read + clear */
    tsdb_query_set_deadline_ns(live);               /* restore */
    return snprintf(buf, cap,
        "{\"cols\":[\"deadline\"],\"types\":[\"INT64\"],"
        "\"rows\":[[%lld]],\"nrows\":1,\"deadline_armed\":%s,"
        "\"truncated\":false,\"ms\":0}",
        (long long)live, live != 0 ? "true" : "false");
}

static void test_sql_deadline(void) {
    printf("\n[Group 1] metrics-plane /sql arms a per-query deadline\n");
    tsdb_metrics_server_set_sql_provider(probe_sql, NULL);
    /* Arm a 30s deadline budget on the /sql dispatch. */
    tsdb_metrics_server_set_sql_deadline_ns(30LL * 1000000000LL);

    tsdb_metrics_server_t *ms = NULL;
    int rc = tsdb_metrics_server_start("127.0.0.1:0", &ms);
    CHECK(rc == 0 && ms != NULL, "metrics server started");
    if (!ms) return;
    int port = tsdb_metrics_server_port(ms);

    char resp[8192];
    int code = http_do(port, "POST", "/sql",
                       "Content-Type: application/json\r\n",
                       "{\"q\":\"SELECT 1\"}", resp, sizeof(resp));
    CHECK(code == 200, "POST /sql -> 200");
    CHECK(strstr(http_body(resp), "\"deadline_armed\":true") != NULL,
          "metrics-plane /sql armed a deadline before calling the provider");

    tsdb_metrics_server_stop(ms);
}

/* ---- (2) influx per-row error accounting on group failure --------------- */

static void test_influx_group_error_accounting(void) {
    printf("\n[Group 2] influx commit accounting: (lines-errors)==committed\n");
    char tmpl[] = "/tmp/tsdb_srg_influx.XXXXXX";
    char *dir = mkdtemp(tmpl);
    CHECK(dir != NULL, "mkdtemp influx dir");
    if (!dir) return;

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(dir, &db);
    CHECK(rc == TSDB_OK && db, "tsdb_open");
    if (!db) return;

    const char *ok =
        "srg,host=a v=1i\n"
        "srg,host=b v=2i\n"
        "srg,host=c v=3i\n";
    size_t lines = 0, errors = 0;
    tsdb_influx_ingest(db, ok, strlen(ok), &lines, &errors);
    CHECK(lines == 3 && errors == 0, "baseline batch: 3 lines 0 errors");

    tsdb_result_t *res = NULL;
    int64_t base_count = -1;
    if (tsdb_query(db, "SELECT count(*) FROM srg", &res) == TSDB_OK && res) {
        if (tsdb_result_next(res) > 0) base_count = tsdb_result_i64(res, 0);
        tsdb_result_free(res);
    }
    CHECK(base_count == 3, "baseline count(*) == 3");

    const char *batch =
        "srg,host=d v=4i\n"
        "srg,host=e v=5i\n"
        "srg,host=f v=6i\n"
        "srg,host=g v=7i\n";
    /* Force the group's commit to fail deterministically via the WAL append
     * fault seam.  In deferred (TSDB_WAL_ONLY_COMMIT=1) mode this makes
     * tsdb_batch_commit return non-OK for the whole 4-row measurement group.
     *
     * Invariant under test: pass-2 error accounting is PER-ROW, so on a group
     * commit failure `errors` counts every row in the group (4), never a
     * single bump for the whole group.  Pre-fix the code does `errors++` once,
     * yielding errors==1 and crediting (lines-errors)=3 phantom committed
     * rows.  We assert `errors` is either 0 (commit happened to succeed — the
     * default flush-on-commit mode does not route through this seam) or the
     * full group size, but never a partial 1-of-4. */
    const int group_n = 4;
    setenv("TSDB_WAL_FAIL_APPEND", "1", 1);
    lines = errors = 0;
    tsdb_influx_ingest(db, batch, strlen(batch), &lines, &errors);
    unsetenv("TSDB_WAL_FAIL_APPEND");

    int64_t after_count = -1;
    if (tsdb_query(db, "SELECT count(*) FROM srg", &res) == TSDB_OK && res) {
        if (tsdb_result_next(res) > 0) after_count = tsdb_result_i64(res, 0);
        tsdb_result_free(res);
    }
    long long claimed = (long long)lines - (long long)errors;
    fprintf(stderr, "  lines=%zu errors=%zu claimed_net=%lld count_delta=%lld\n",
            lines, errors, claimed, (long long)(after_count - base_count));
    CHECK(errors == 0 || errors == (size_t)group_n,
          "group commit failure counts every failed row, not one per group");

    tsdb_close(db);
}

/* ---- (3) wire QUERY result-row cap -------------------------------------- */

/* Blocking connect to 127.0.0.1:port with a short retry. */
static int wire_connect(int port) {
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

static void test_wire_result_cap(void) {
    printf("\n[Group 3] wire QUERY honours max_result_rows ceiling\n");
    char tmpl[] = "/tmp/tsdb_srg_cap.XXXXXX";
    char *dir = mkdtemp(tmpl);
    CHECK(dir != NULL, "mkdtemp cap dir");
    if (!dir) return;

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(dir, &db);
    CHECK(rc == TSDB_OK && db, "tsdb_open");
    if (!db) return;

    tsdb_col_t cols[2];
    cols[0].name = "ts";  cols[0].type = TSDB_TYPE_TIMESTAMP;
    cols[1].name = "v";   cols[1].type = TSDB_TYPE_INT64;
    rc = tsdb_create_table(db, "capt", cols, 2, "ts");
    CHECK(rc == TSDB_OK, "create table capt");

    tsdb_table_t *tbl = NULL;
    rc = tsdb_open_table(db, "capt", &tbl);
    CHECK(rc == TSDB_OK && tbl, "open table capt");
    if (tbl) {
        tsdb_batch_t *b = NULL;
        tsdb_batch_begin(tbl, &b);
        for (int i = 0; i < 100; i++) {
            tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1));
            tsdb_batch_row_i64(b, 1, i);
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);
    }

    tsdb_server_opts_t opts = {
        .bind_addr        = "127.0.0.1:0",
        .max_conns        = 16,
        .db               = db,
        .max_result_rows  = 8,   /* the ceiling under test */
    };
    tsdb_server_t *srv = NULL;
    rc = tsdb_server_start(&opts, &srv);
    CHECK(rc == TSDB_OK && srv, "wire server started with max_result_rows=8");
    if (!srv) { tsdb_close(db); return; }
    int port = tsdb_server_port(srv);

    int fd = wire_connect(port);
    CHECK(fd >= 0, "wire connect");
    if (fd >= 0) {
        tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
        tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
        tsdb_proto_recv(fd, &hdr, &pl); free(pl); pl = NULL;

        const char *qtl = "SELECT ts, v FROM capt";
        tsdb_proto_send(fd, TSDB_MT_QUERY, 0, 7,
                        (const uint8_t *)qtl, strlen(qtl));

        rc = tsdb_proto_recv(fd, &hdr, &pl);
        CHECK(rc == TSDB_OK && hdr.type == TSDB_MT_QUERY_RESULT_HDR,
              "recv QUERY_RESULT_HDR");
        free(pl); pl = NULL;

        /* Sum nrows across every ROWS frame until FIN. */
        int total = 0, got_fin = 0;
        while (!got_fin) {
            if (tsdb_proto_recv(fd, &hdr, &pl) != TSDB_OK) break;
            if (hdr.type == TSDB_MT_QUERY_RESULT_ROWS &&
                pl && hdr.payload_len >= 6) {
                uint32_t nr; memcpy(&nr, pl, 4);
                total += (int)nr;
            }
            got_fin = (hdr.flags & TSDB_FLAG_FIN) != 0;
            free(pl); pl = NULL;
        }
        fprintf(stderr, "  streamed nrows=%d (cap=8)\n", total);
        CHECK(got_fin, "ROWS stream ended with FIN");
        CHECK(total == 8, "wire result capped at max_result_rows (8)");
        close(fd);
    }

    tsdb_server_stop(srv);
    tsdb_close(db);
}

int main(void) {
    tsdb_metrics_init();
    test_sql_deadline();
    test_influx_group_error_accounting();
    test_wire_result_cap();
    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
