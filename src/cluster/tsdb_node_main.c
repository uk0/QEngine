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
        "Usage: %s --data-dir <dir> --rpc <host:port> [--seeds <seed,...>]\n"
        "\n"
        "  --data-dir   Local storage directory (created if absent)\n"
        "  --rpc        TCP RPC bind address (e.g. 127.0.0.1:28081)\n"
        "  --seeds      Comma-separated gossip seed addresses\n"
        "\n"
        "Gossip UDP port = RPC port - 1.\n",
        prog);
}

int main(int argc, char **argv) {
    const char *data_dir  = NULL;
    const char *rpc_addr  = NULL;
    const char *seeds     = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--rpc") == 0 && i + 1 < argc) {
            rpc_addr = argv[++i];
        } else if (strcmp(argv[i], "--seeds") == 0 && i + 1 < argc) {
            seeds = argv[++i];
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

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    tsdb_db_t *db = NULL;
    int rc = tsdb_open_cluster(data_dir, rpc_addr, seeds, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "tsdb_open_cluster failed: %s\n", tsdb_errstr(rc));
        return 1;
    }

    printf("[node] listening rpc=%s  data=%s\n", rpc_addr, data_dir);
    fflush(stdout);

    /* Wait for cluster to form. */
    tsdb_cluster_wait_ready(db, 1, 5000);

    char stats_buf[4096];
    tsdb_cluster_stats(db, stats_buf, sizeof(stats_buf));
    printf("[node] cluster view: %s\n", stats_buf);
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
    tsdb_close(db);
    printf("[node] done.\n");
    return 0;
}
