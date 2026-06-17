/* test_standalone_parity.c — management-plane parity for the standalone
 * tsdb-server.
 *
 * The cluster node (src/cluster/tsdb_node_main.c) registers the full set of
 * dashboard providers, but the standalone server (cli/tsdb_server_main.c)
 * historically wired only /sql — so /retention/sweep, /audit, /pitr and
 * /catalog/check returned 503 "provider not registered" in single-node mode.
 *
 * Commit 1 factored tsdb_server_wire_metrics_providers() to close that gap.
 * This test exercises the REAL helper (the cli object is linked in with its
 * main() renamed out of the way — see the Makefile rule): it opens a temp
 * data dir, wires the providers, starts a real metrics server on an ephemeral
 * 127.0.0.1 port, and asserts the management endpoints return NON-ERROR
 * (not 503 / not "provider not registered").  The HTTP client + body helpers
 * are copied from tests/test_metrics_http.c.
 */

#include "../src/server/metrics_server.h"
#include "../src/server/metrics.h"
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

/* The production helper from cli/tsdb_server_main.c (non-static).  The cli
 * translation unit is linked into this test with -Dmain=... so only its
 * main() is renamed; tsdb_server_wire_metrics_providers stays callable. */
void tsdb_server_wire_metrics_providers(tsdb_db_t *db);

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

/* ---- Minimal blocking HTTP client (copied from test_metrics_http.c) ----- */

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

/* ---- Test --------------------------------------------------------------- */

int main(void) {
    tsdb_metrics_init();

    char tmpl[] = "/tmp/tsdb_parity_test.XXXXXX";
    char *dir = mkdtemp(tmpl);
    CHECK(dir != NULL, "mkdtemp for standalone data dir");
    if (!dir) return 1;

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(dir, &db);
    CHECK(rc == TSDB_OK && db != NULL, "tsdb_open(temp data dir)");
    if (rc != TSDB_OK || !db) return 1;

    /* Tell the dashboard about the data dir (so /cluster's standalone
     * fallback works), then wire the full management plane via the SAME
     * helper main() calls.  Pre-fix the standalone server never wired any
     * of these. */
    tsdb_metrics_server_set_data_dir(dir);
    tsdb_server_wire_metrics_providers(db);

    tsdb_metrics_server_t *ms = NULL;
    rc = tsdb_metrics_server_start("127.0.0.1:0", &ms);
    CHECK(rc == 0 && ms != NULL, "metrics server started on ephemeral port");
    if (!ms) return 1;
    int port = tsdb_metrics_server_port(ms);
    CHECK(port > 0, "metrics server bound to a real port");

    char resp[1 << 16];
    int code;

    /* Sanity: the always-on endpoint works. */
    code = http_do(port, "GET", "/health", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /health -> 200");

    /* KEY parity asserts — each of these returned 503 in standalone pre-fix
     * because no provider was registered.  Now they must succeed. */

    /* POST /retention/sweep — provider now wired + GC armed. */
    code = http_do(port, "POST", "/retention/sweep", "", NULL,
                   resp, sizeof(resp));
    CHECK(code == 200, "POST /retention/sweep -> 200 (provider wired)");
    CHECK(strstr(http_body(resp), "provider not registered") == NULL,
          "POST /retention/sweep not 'provider not registered'");
    CHECK(strstr(http_body(resp), "\"ok\":true") != NULL,
          "POST /retention/sweep body has ok:true");

    /* GET /audit — provider now wired (0 rows on a fresh dir is fine). */
    code = http_do(port, "GET", "/audit?n=10", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /audit -> 200 (provider wired)");
    CHECK(strstr(http_body(resp), "provider not registered") == NULL,
          "GET /audit not 'provider not registered'");
    CHECK(strstr(http_body(resp), "\"rows\":[") != NULL,
          "GET /audit body has rows[]");

    /* POST /pitr?ts=<ns> — provider now wired. */
    code = http_do(port, "POST", "/pitr?ts=9000000000000000000", "", NULL,
                   resp, sizeof(resp));
    CHECK(code == 200, "POST /pitr -> 200 (provider wired)");
    CHECK(strstr(http_body(resp), "provider not registered") == NULL,
          "POST /pitr not 'provider not registered'");
    CHECK(strstr(http_body(resp), "\"partitions_removed\":") != NULL,
          "POST /pitr body has partitions_removed");

    /* GET /catalog/check — provider now wired. */
    code = http_do(port, "GET", "/catalog/check", "", NULL, resp, sizeof(resp));
    CHECK(code == 200, "GET /catalog/check -> 200 (provider wired)");
    CHECK(strstr(http_body(resp), "provider not registered") == NULL,
          "GET /catalog/check not 'provider not registered'");

    tsdb_metrics_server_stop(ms);
    tsdb_close(db);

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
