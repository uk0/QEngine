/* tsdb_server_main.c — standalone tsdb TCP server daemon.
 *
 * Usage:
 *   tsdb-server --data-dir /var/lib/tsdb --bind 0.0.0.0:28090
 *               [--cluster-bind :28080] [--seeds host:port,...]
 *
 * Listens for incoming client connections on the wire protocol v1 port.
 * SIGINT / SIGTERM → graceful shutdown.
 */

#include "../src/server/server.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

/* ---- Global for signal handler ------------------------------------------ */
static volatile int g_quit = 0;
static tsdb_server_t *g_srv = NULL;

static void on_signal(int sig) {
    (void)sig;
    g_quit = 1;
}

/* ---- Argument parsing ----------------------------------------------------- */
static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --data-dir <dir> [--bind addr:port] [--cluster-bind addr:port]\n"
        "          [--seeds host:port,...]\n"
        "\n"
        "Options:\n"
        "  --data-dir DIR       Data directory (required)\n"
        "  --bind ADDR          Listen address (default 0.0.0.0:28090)\n"
        "  --cluster-bind ADDR  Cluster RPC bind address (not implemented in v1)\n"
        "  --seeds LIST         Comma-separated cluster seed addresses\n",
        prog);
}

int main(int argc, char **argv) {
    const char *data_dir     = NULL;
    const char *bind_addr    = "0.0.0.0:28090";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--data-dir") == 0 && i+1 < argc)
            data_dir = argv[++i];
        else if (strcmp(argv[i], "--bind") == 0 && i+1 < argc)
            bind_addr = argv[++i];
        else if (strcmp(argv[i], "--cluster-bind") == 0 && i+1 < argc)
            i++;  /* accepted, ignored in v1 */
        else if (strcmp(argv[i], "--seeds") == 0 && i+1 < argc)
            i++;  /* accepted, ignored in v1 */
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            usage(argv[0]); return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]); return 1;
        }
    }

    if (!data_dir) {
        fprintf(stderr, "Error: --data-dir is required\n");
        usage(argv[0]);
        return 1;
    }

    /* ---- Open database --------------------------------------------------- */
    tsdb_db_t *db = NULL;
    int rc = tsdb_open(data_dir, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "Failed to open database at '%s': %s\n",
                data_dir, tsdb_errstr(rc));
        return 1;
    }

    /* ---- Install signal handlers ---------------------------------------- */
    struct sigaction sa = {0};
    sa.sa_handler = on_signal;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    /* ---- Start server ---------------------------------------------------- */
    tsdb_server_opts_t opts = {
        .bind_addr    = bind_addr,
        .max_conns    = 1024,
        .write_workers = 4,
        .db           = db,
    };

    tsdb_server_t *srv = NULL;
    rc = tsdb_server_start(&opts, &srv);
    if (rc != TSDB_OK) {
        fprintf(stderr, "Failed to start server on '%s': %s\n",
                bind_addr, tsdb_errstr(rc));
        tsdb_close(db);
        return 1;
    }
    g_srv = srv;

    fprintf(stdout, "tsdb-server listening on port %d, data-dir=%s\n",
            tsdb_server_port(srv), data_dir);
    fflush(stdout);

    /* ---- Main loop ------------------------------------------------------- */
    while (!g_quit) {
        sleep(1);
    }

    /* ---- Graceful shutdown ----------------------------------------------- */
    fprintf(stdout, "\nShutting down...\n");
    tsdb_server_stop(srv);
    g_srv = NULL;
    tsdb_close(db);
    fprintf(stdout, "Done.\n");
    return 0;
}
