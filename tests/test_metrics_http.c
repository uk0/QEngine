/* test_metrics_http.c — HTTP-level tests for the dashboard API surface
 * served by src/server/metrics_server.c.
 *
 * Until now nothing exercised the metrics_server HTTP endpoints in-process
 * (test_server.c covers the binary wire protocol; test_metrics.c covers the
 * metric registry).  This harness starts a real tsdb_metrics_server on an
 * ephemeral port, registers stub providers for the dashboard endpoints, and
 * drives each route with a tiny blocking HTTP client — asserting the status
 * line and the key response fields the dashboard (dashboard/src/api.ts)
 * depends on.
 *
 * Regression anchor: POST /retention/sweep and GET /audit used to crash the
 * node with a stack overflow (oversized stack buffers in do_sweep / the audit
 * handler) because the handler runs on a small-stack pthread.  The retention
 * provider here calls the REAL tsdb_retention_sweep_once() against a temp
 * data dir so the formerly-crashing path runs end-to-end through HTTP; if it
 * regressed, this test would crash instead of asserting.
 */

#include "../src/server/metrics_server.h"
#include "../src/server/metrics.h"
#include "../src/storage/retention.h"
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

/* ---- Minimal blocking HTTP client --------------------------------------- */

/* Perform one HTTP request to 127.0.0.1:port and capture the full response.
 * `extra` is appended into the request (e.g. an extra header line ending in
 * CRLF, or a Cookie); pass "" for none.  `body` is sent after the headers
 * with a matching Content-Length when non-empty.  Returns the numeric status
 * code (e.g. 200), or -1 on transport failure.  The full response (headers +
 * body) is copied into resp[0..resp_cap-1] NUL-terminated. */
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
                     "%s %s HTTP/1.1\r\n"
                     "Host: 127.0.0.1\r\n"
                     "%s"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n%s",
                     method, path, extra ? extra : "", blen,
                     body ? body : "");
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

    /* Parse "HTTP/1.1 <code> ...". */
    int code = -1;
    if (strncmp(resp, "HTTP/1.", 7) == 0) {
        const char *sp = strchr(resp, ' ');
        if (sp) code = atoi(sp + 1);
    }
    return code;
}

/* Return a pointer to the start of the response body (after CRLFCRLF). */
static const char *http_body(const char *resp) {
    const char *m = strstr(resp, "\r\n\r\n");
    return m ? m + 4 : resp;
}

/* ---- Stub providers ----------------------------------------------------- */

static int stub_cluster(void *ud, char *buf, size_t cap) {
    (void)ud;
    return snprintf(buf, cap,
        "{\"local_id\":\"node-test\",\"nodes\":[{\"id\":\"node-test\","
        "\"addr\":\"127.0.0.1:1\",\"state\":\"ALIVE\",\"role\":\"master\"}]}");
}
static int stub_tree(void *ud, char *buf, size_t cap) {
    (void)ud;
    return snprintf(buf, cap,
        "{\"db\":{\"name\":\"testdb\"},\"tables\":[],\"databases\":[],"
        "\"vtables\":[],\"ptables\":[],\"groups\":[]}");
}
static int stub_raft(void *ud, char *buf, size_t cap) {
    (void)ud;
    return snprintf(buf, cap,
        "{\"self_id\":\"node-test\",\"role\":\"leader\",\"current_term\":1}");
}
static int stub_sql(void *ud, const char *q, size_t qlen,
                    const char *token, char *buf, size_t cap) {
    (void)ud; (void)token;
    /* Echo a fixed result shape — enough to validate the JSON envelope. */
    if (qlen == 0) return snprintf(buf, cap, "{\"error\":\"empty query\"}");
    return snprintf(buf, cap,
        "{\"cols\":[\"n\"],\"types\":[\"INT64\"],\"rows\":[[1]],"
        "\"nrows\":1,\"truncated\":false,\"ms\":0}");
}
/* Audit provider: returns two JSONL records so the handler's array-rewrap
 * path (and its 1 MiB scratch buffer) is exercised. */
static int stub_audit(void *ud, int max_rows, char *buf, size_t cap) {
    (void)ud; (void)max_rows;
    return snprintf(buf, cap,
        "{\"ts\":\"t1\",\"user\":\"root\",\"event\":\"AUTH\",\"action\":\"LOGIN\","
        "\"object\":\"\",\"result\":0,\"detail\":\"\"}\n"
        "{\"ts\":\"t2\",\"user\":\"root\",\"event\":\"DDL\",\"action\":\"CREATE\","
        "\"object\":\"t\",\"result\":0,\"detail\":\"\"}\n");
}
static int stub_pitr(void *ud, int64_t target_ns) {
    (void)ud; (void)target_ns;
    return 0; /* 0 partitions removed, success */
}

/* Retention provider that runs the REAL sweep against a temp dir — this is
 * the exact path that overflowed the handler-thread stack pre-fix. */
static tsdb_retention_t *g_ret = NULL;
static int real_retention_sweep(void *ud) {
    (void)ud;
    if (!g_ret) return -1;
    int deleted = 0;
    int rc = tsdb_retention_sweep_once(g_ret, &deleted);
    return rc == TSDB_OK ? deleted : -1;
}

/* ---- Tests -------------------------------------------------------------- */

int main(void) {
    tsdb_metrics_init();

    /* Temp data dir for the real retention sweep. */
    char tmpl[] = "/tmp/tsdb_http_test.XXXXXX";
    char *dir = mkdtemp(tmpl);
    CHECK(dir != NULL, "mkdtemp for retention data dir");
    if (!dir) return 1;
    if (tsdb_retention_start(dir, NULL, NULL, &g_ret) != TSDB_OK) {
        fprintf(stderr, "FAIL: tsdb_retention_start\n");
        return 1;
    }

    /* Register every dashboard provider. */
    tsdb_metrics_server_set_data_dir(dir);
    tsdb_metrics_server_set_cluster_provider(stub_cluster, NULL);
    tsdb_metrics_server_set_tree_provider(stub_tree, NULL);
    tsdb_metrics_server_set_raft_provider(stub_raft, NULL);
    tsdb_metrics_server_set_sql_provider(stub_sql, NULL);
    tsdb_metrics_server_set_audit_provider(stub_audit, NULL);
    tsdb_metrics_server_set_pitr_provider(stub_pitr, NULL);
    tsdb_metrics_server_set_retention_sweep_provider(real_retention_sweep, NULL);

    /* Bind on an ephemeral port (auth left disabled: TSDB_DASHBOARD_AUTH unset,
     * which is the metrics_server default, so we exercise the route handlers
     * directly without the cookie gate). */
    tsdb_metrics_server_t *ms = NULL;
    int rc = tsdb_metrics_server_start("127.0.0.1:0", &ms);
    CHECK(rc == 0 && ms != NULL, "metrics server started on ephemeral port");
    if (!ms) return 1;
    int port = tsdb_metrics_server_port(ms);
    CHECK(port > 0, "metrics server bound to a real port");

    char resp[1 << 16];
    int code;

    /* GET /health */
    code = http_do(port, "GET", "/health", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /health -> 200");
    CHECK(strstr(http_body(resp), "\"status\":\"ok\"") != NULL,
          "GET /health body has status:ok");
    CHECK(strstr(http_body(resp), "\"uptime_s\"") != NULL,
          "GET /health body has uptime_s");

    /* GET /metrics (Prometheus text) */
    code = http_do(port, "GET", "/metrics", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /metrics -> 200");
    CHECK(strstr(resp, "# TYPE") != NULL, "GET /metrics has Prometheus TYPE lines");

    /* GET /raft */
    code = http_do(port, "GET", "/raft", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /raft -> 200");
    CHECK(strstr(http_body(resp), "\"role\":\"leader\"") != NULL,
          "GET /raft body has role:leader");

    /* GET /cluster */
    code = http_do(port, "GET", "/cluster", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /cluster -> 200");
    CHECK(strstr(http_body(resp), "\"nodes\"") != NULL,
          "GET /cluster body has nodes[]");

    /* GET /tree */
    code = http_do(port, "GET", "/tree", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /tree -> 200");
    CHECK(strstr(http_body(resp), "\"databases\"") != NULL,
          "GET /tree body has databases[]");

    /* POST /sql */
    code = http_do(port, "POST", "/sql",
                   "Content-Type: application/json\r\n",
                   "{\"q\":\"SELECT 1\"}", resp, sizeof(resp));
    CHECK(code == 200, "POST /sql -> 200");
    CHECK(strstr(http_body(resp), "\"cols\"") != NULL,
          "POST /sql body has cols[] result shape");

    /* GET /audit — exercises the (formerly 1 MiB stack) audit buffer path. */
    code = http_do(port, "GET", "/audit?n=10", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /audit -> 200");
    CHECK(strstr(http_body(resp), "\"rows\":[") != NULL,
          "GET /audit body has rows[]");
    CHECK(strstr(http_body(resp), "\"nrows\":2") != NULL,
          "GET /audit rewrapped both JSONL records (nrows:2)");

    /* POST /retention/sweep — the regression anchor: real sweep through HTTP.
     * If do_sweep regressed to a stack-resident conf this would SIGBUS the
     * test process instead of returning a clean 200. */
    code = http_do(port, "POST", "/retention/sweep", "", NULL,
                   resp, sizeof(resp));
    CHECK(code == 200, "POST /retention/sweep -> 200 (no stack-overflow crash)");
    CHECK(strstr(http_body(resp), "\"ok\":true") != NULL,
          "POST /retention/sweep body has ok:true");
    CHECK(strstr(http_body(resp), "\"partitions_deleted\":0") != NULL,
          "POST /retention/sweep deleted 0 on empty data dir");

    /* GET /retention/sweep must be rejected — the sweep mutates state. */
    code = http_do(port, "GET", "/retention/sweep", "", NULL,
                   resp, sizeof(resp));
    CHECK(code == 405, "GET /retention/sweep -> 405 Method Not Allowed");
    CHECK(strstr(http_body(resp), "use POST") != NULL,
          "GET /retention/sweep body hints use POST");

    /* POST /pitr?ts=... */
    code = http_do(port, "POST", "/pitr?ts=9000000000000000000", "", NULL,
                   resp, sizeof(resp));
    CHECK(code == 200, "POST /pitr -> 200");
    CHECK(strstr(http_body(resp), "\"partitions_removed\":0") != NULL,
          "POST /pitr body has partitions_removed");

    /* GET /pitr must be rejected (method guard). */
    code = http_do(port, "GET", "/pitr?ts=1", "", NULL, resp, sizeof(resp));
    CHECK(code == 405, "GET /pitr -> 405 Method Not Allowed");

    /* POST /pitr with no ts -> 400. */
    code = http_do(port, "POST", "/pitr", "", NULL, resp, sizeof(resp));
    CHECK(code == 400, "POST /pitr without ts -> 400 Bad Request");

    /* GET /logout always 302 -> /login (clears cookie). */
    code = http_do(port, "GET", "/logout", "", NULL, resp, sizeof(resp));
    CHECK(code == 302, "GET /logout -> 302");

    /* Unknown path -> not a dashboard route.  Served as 404 (or the embedded
     * dashboard for "/"), but an arbitrary unknown path should 404. */
    code = http_do(port, "GET", "/nope-not-a-route", "", NULL,
                   resp, sizeof(resp));
    CHECK(code == 404, "GET /nope-not-a-route -> 404");

    tsdb_metrics_server_stop(ms);
    tsdb_retention_stop(g_ret);
    g_ret = NULL;

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
