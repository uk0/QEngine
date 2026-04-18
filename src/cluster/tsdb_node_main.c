/* tsdb_node_main.c — CLI entry point for a standalone cluster node.
 *
 * Usage:
 *   tsdb_node --data-dir /tmp/node1 --rpc 127.0.0.1:28081 \
 *             [--seeds 127.0.0.1:28080,127.0.0.1:28082]
 *
 * Starts a tsdb cluster node and blocks until SIGTERM/SIGINT.
 */

/* Must include tsdb.h via full path since src/cluster/ is not include root. */
#include "../../include/tsdb.h"
#include "../../include/tsdb_cluster.h"
#include "../server/server.h"
#include "../server/metrics_server.h"
#include "../server/metrics.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

static volatile int g_running = 1;

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --data-dir <dir> --rpc <host:port>\n"
        "       [--seeds <seed,...>] [--bind <host:port>]\n"
        "       [--metrics-bind <host:port>]\n"
        "\n"
        "  --data-dir       Local storage directory (created if absent)\n"
        "  --rpc            TCP RPC bind address (e.g. 0.0.0.0:28081)\n"
        "  --seeds          Comma-separated gossip seed addresses\n"
        "  --bind           Wire-protocol client bind (default 0.0.0.0:28090)\n"
        "                   — exposes the same API as tsdb-server.\n"
        "  --metrics-bind   /metrics + /health + / dashboard bind\n"
        "                   (default 0.0.0.0:28094)\n"
        "\n"
        "Gossip UDP port = RPC port - 1.\n",
        prog);
}

/* cluster_json_cb — provide /cluster HTTP payload from the live cluster
 * view.  Registered with metrics_server so the dashboard topology panel
 * shows real gossip membership instead of the standalone fallback. */
static int cluster_json_cb(void *ud, char *buf, size_t cap) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return 0;
    int n = tsdb_cluster_stats(db, buf, cap);
    return n > 0 ? n : 0;
}

int main(int argc, char **argv) {
    const char *data_dir     = NULL;
    const char *rpc_addr     = NULL;
    const char *seeds        = NULL;
    const char *client_bind  = getenv("TSDB_BIND");
    const char *metrics_bind = getenv("TSDB_METRICS_BIND");

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--rpc") == 0 && i + 1 < argc) {
            rpc_addr = argv[++i];
        } else if (strcmp(argv[i], "--seeds") == 0 && i + 1 < argc) {
            seeds = argv[++i];
        } else if (strcmp(argv[i], "--bind") == 0 && i + 1 < argc) {
            client_bind = argv[++i];
        } else if (strcmp(argv[i], "--metrics-bind") == 0 && i + 1 < argc) {
            metrics_bind = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    if (!data_dir || !rpc_addr) {
        usage(argv[0]);
        return 1;
    }
    /* Sensible defaults so `docker run` with just seeds works. */
    if (!client_bind  || !*client_bind)  client_bind  = "0.0.0.0:28090";
    if (!metrics_bind || !*metrics_bind) metrics_bind = "0.0.0.0:28094";

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    tsdb_db_t *db = NULL;
    int rc = tsdb_open_cluster(data_dir, rpc_addr, seeds, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "tsdb_open_cluster failed: %s\n", tsdb_errstr(rc));
        return 1;
    }

    printf("[node] cluster opened  rpc=%s  data=%s\n", rpc_addr, data_dir);
    fflush(stdout);

    /* Wait for cluster to form. */
    tsdb_cluster_wait_ready(db, 1, 5000);

    char stats_buf[4096];
    tsdb_cluster_stats(db, stats_buf, sizeof(stats_buf));
    printf("[node] cluster view: %s\n", stats_buf);
    fflush(stdout);

    /* Start the wire-protocol client server so external clients (Go SDK,
     * JDBC, CLI) can reach this node the same way they reach tsdb-server. */
    tsdb_metrics_init();
    tsdb_server_opts_t sopts = {
        .bind_addr    = client_bind,
        .max_conns    = 1024,
        .db           = db,
    };
    tsdb_server_t *srv = NULL;
    rc = tsdb_server_start(&sopts, &srv);
    if (rc != TSDB_OK) {
        fprintf(stderr, "tsdb_server_start(%s) failed: %s\n",
                client_bind, tsdb_errstr(rc));
        tsdb_close(db);
        return 1;
    }
    printf("[node] client bind=%s\n", client_bind);

    /* /metrics + /health + dashboard + /cluster — latter wired to the
     * live cluster view via the provider callback. */
    tsdb_metrics_server_t *ms = NULL;
    tsdb_metrics_server_set_data_dir(data_dir);
    tsdb_metrics_server_set_cluster_provider(cluster_json_cb, db);
    rc = tsdb_metrics_server_start(metrics_bind, &ms);
    if (rc == 0 && ms) {
        printf("[node] metrics  bind=%s\n", metrics_bind);
    } else {
        fprintf(stderr, "[node] metrics server start(%s) failed\n", metrics_bind);
    }
    fflush(stdout);

    /* Main loop: print stats every 5s until signal. */
    int tick = 0;
    while (g_running) {
        sleep(1);
        if (++tick % 5 == 0) {
            tsdb_cluster_stats(db, stats_buf, sizeof(stats_buf));
            printf("[node] alive=%d  %s\n",
                   tsdb_cluster_alive_count(db), stats_buf);
            fflush(stdout);
        }
    }

    printf("[node] shutting down...\n");
    if (ms)  tsdb_metrics_server_stop(ms);
    if (srv) tsdb_server_stop(srv);
    tsdb_close(db);
    printf("[node] done.\n");
    return 0;
}
