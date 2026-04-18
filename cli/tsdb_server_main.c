/* tsdb_server_main.c — standalone tsdb TCP server daemon.
 *
 * Usage:
 *   tsdb-server [--config /etc/tsd.conf] [overrides...]
 *
 * Overrides (mirror tsd.conf keys):
 *   --data-dir PATH        primary data directory
 *   --bind ADDR            client listen address
 *   --log-level LVL        error|warn|info|debug|trace
 *   --debug FLAG[,FLAG...] enable feature-flag debug output
 *   --show-config          print the resolved config and exit
 *
 * Any option in tsd.conf can also be set via TSDB_<UPPER_KEY>= env.
 * SIGINT / SIGTERM — graceful shutdown.
 * SIGHUP          — reload log_level, debug_flags, and other
 *                    [reloadable] options from the same config file.
 */

#define _POSIX_C_SOURCE 200809L

#include "../src/server/server.h"
#include "../src/server/config.h"
#include "../src/server/log.h"
#include "../src/server/influx_line.h"
#include "../src/server/metrics.h"
#include "../src/server/metrics_server.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

/* ─── signal state ─────────────────────────────────────────────────────── */

static volatile sig_atomic_t g_quit    = 0;
static volatile sig_atomic_t g_reload  = 0;
static tsdb_server_t        *g_srv     = NULL;
static char                  g_cfg_path[TSDB_CFG_MAX_PATH];

static void on_signal(int sig) {
    if (sig == SIGHUP) g_reload = 1;
    else               g_quit   = 1;
}

/* ─── usage ────────────────────────────────────────────────────────────── */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [--config PATH] [options]\n"
        "\n"
        "Options (override tsd.conf):\n"
        "  --config PATH          path to tsd.conf (default: /etc/tsd.conf / ./tsd.conf)\n"
        "  --data-dir DIR         primary data directory (required if not in config)\n"
        "  --bind ADDR            client listen address (e.g. 0.0.0.0:28090)\n"
        "  --log-level LVL        error | warn | info | debug | trace\n"
        "  --debug FLAGS          comma-separated debug feature flags\n"
        "  --tls-cert PATH        PEM server certificate (enables TLS)\n"
        "  --tls-key  PATH        PEM server private key  (required with --tls-cert)\n"
        "  --tls-ca   PATH        PEM CA bundle for mutual TLS client auth (optional)\n"
        "  --show-config          dump effective config and exit\n"
        "  --help                 show this help\n"
        "\n"
        "Environment: any tsd.conf key may be overridden by TSDB_<UPPER_KEY>=\n"
        "Signals:     SIGINT/SIGTERM graceful shutdown, SIGHUP reload log options\n",
        prog);
}

/* ─── main ─────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    const char *cfg_arg           = NULL;
    const char *override_data_dir = NULL;
    const char *override_bind     = NULL;
    const char *override_level    = NULL;
    const char *override_debug    = NULL;
    const char *override_tls_cert = NULL;
    const char *override_tls_key  = NULL;
    const char *override_tls_ca   = NULL;
    int show_config = 0;

    for (int i = 1; i < argc; i++) {
        if      (!strcmp(argv[i], "--config")    && i + 1 < argc) cfg_arg           = argv[++i];
        else if (!strcmp(argv[i], "--data-dir")  && i + 1 < argc) override_data_dir = argv[++i];
        else if (!strcmp(argv[i], "--bind")      && i + 1 < argc) override_bind     = argv[++i];
        else if (!strcmp(argv[i], "--log-level") && i + 1 < argc) override_level    = argv[++i];
        else if (!strcmp(argv[i], "--debug")     && i + 1 < argc) override_debug    = argv[++i];
        else if (!strcmp(argv[i], "--show-config"))               show_config       = 1;
        /* Accept legacy flags and silently ignore (tsd.conf handles cluster). */
        else if (!strcmp(argv[i], "--cluster-bind") && i + 1 < argc) i++;
        else if (!strcmp(argv[i], "--seeds")        && i + 1 < argc) i++;
        else if (!strcmp(argv[i], "--tls-cert") && i + 1 < argc) override_tls_cert = argv[++i];
        else if (!strcmp(argv[i], "--tls-key")  && i + 1 < argc) override_tls_key  = argv[++i];
        else if (!strcmp(argv[i], "--tls-ca")   && i + 1 < argc) override_tls_ca   = argv[++i];
        else if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) { usage(argv[0]); return 0; }
        else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            usage(argv[0]); return 2;
        }
    }

    /* 1. Defaults. */
    tsdb_config_t cfg;
    tsdb_config_defaults(&cfg);

    /* 2. Locate and load config file (silent if missing). */
    char *cfg_path = tsdb_config_locate(cfg_arg);
    char  err[256] = {0};
    int   rc = 0;
    if (cfg_path) {
        snprintf(g_cfg_path, sizeof(g_cfg_path), "%s", cfg_path);
        rc = tsdb_config_load(&cfg, cfg_path, err, sizeof(err));
        if (rc == -2) {
            fprintf(stderr, "[tsd.conf] parse errors: %s\n", err);
        }
        free(cfg_path);
    }

    /* 3. Env overrides. */
    tsdb_config_apply_env(&cfg);

    /* 4. Command-line overrides (highest priority). */
    if (override_data_dir) snprintf(cfg.data_dir,  sizeof(cfg.data_dir),  "%s", override_data_dir);
    if (override_bind)     snprintf(cfg.bind,       sizeof(cfg.bind),       "%s", override_bind);
    if (override_tls_cert) snprintf(cfg.tls_cert,   sizeof(cfg.tls_cert),   "%s", override_tls_cert);
    if (override_tls_key)  snprintf(cfg.tls_key,    sizeof(cfg.tls_key),    "%s", override_tls_key);
    if (override_tls_ca)   snprintf(cfg.tls_ca,     sizeof(cfg.tls_ca),     "%s", override_tls_ca);
    /* Recompute derived flag. */
    cfg.tls_enabled = (cfg.tls_cert[0] != '\0' && cfg.tls_key[0] != '\0');
    if (override_level) {
        if      (!strcmp(override_level, "error")) cfg.log_level = TSDB_LOG_ERROR;
        else if (!strcmp(override_level, "warn"))  cfg.log_level = TSDB_LOG_WARN;
        else if (!strcmp(override_level, "info"))  cfg.log_level = TSDB_LOG_INFO;
        else if (!strcmp(override_level, "debug")) cfg.log_level = TSDB_LOG_DEBUG;
        else if (!strcmp(override_level, "trace")) cfg.log_level = TSDB_LOG_TRACE;
    }
    if (override_debug) {
        for (int i = 0; i < cfg.n_debug_flags; i++) free(cfg.debug_flags[i]);
        cfg.n_debug_flags = 0;
        char buf[512]; snprintf(buf, sizeof(buf), "%s", override_debug);
        char *tok = strtok(buf, ",");
        while (tok && cfg.n_debug_flags < TSDB_CFG_MAX_DEBUG_FLAGS) {
            while (*tok == ' ') tok++;
            cfg.debug_flags[cfg.n_debug_flags++] = strdup(tok);
            tok = strtok(NULL, ",");
        }
    }

    if (show_config) {
        tsdb_config_dump(&cfg);
        tsdb_config_free(&cfg);
        return 0;
    }

    if (!cfg.data_dir[0]) {
        fprintf(stderr, "error: data_dir not set (use --data-dir or tsd.conf)\n");
        tsdb_config_free(&cfg);
        return 1;
    }

    /* 5. Initialise logging. */
    tsdb_log_init(&cfg);
    TSDB_LOG_INFO("main", "tsdb-server starting data_dir=%s bind=%s",
                  cfg.data_dir, cfg.bind);
    if (g_cfg_path[0])
        TSDB_LOG_INFO("main", "loaded config: %s", g_cfg_path);
    else
        TSDB_LOG_INFO("main", "no tsd.conf found — using defaults");
    for (int i = 0; i < cfg.n_data_dirs; i++)
        TSDB_LOG_INFO("main", "extra data dir [%d] = %s", i, cfg.data_dirs[i]);

    /* 6. Open DB and start server. */
    tsdb_metrics_init();
    tsdb_db_t *db = NULL;
    rc = tsdb_open(cfg.data_dir, &db);
    if (rc == TSDB_OK && db && cfg.block_points > 0) {
        /* Deployment-level knob: subsequently-created tables inherit this
         * block size.  Clamped internally into [1024, 8192]. */
        tsdb_db_set_default_block_points(db, cfg.block_points);
    }
    if (rc != TSDB_OK) {
        TSDB_LOG_ERROR("main", "tsdb_open(%s) failed: %s",
                       cfg.data_dir, tsdb_errstr(rc));
        tsdb_log_shutdown();
        tsdb_config_free(&cfg);
        return 1;
    }

    tsdb_server_opts_t opts = {
        .bind_addr     = cfg.bind,
        .max_conns     = cfg.max_conns,
        .write_workers = cfg.write_workers ? cfg.write_workers : 4,
        .db            = db,
        .tls_cert      = cfg.tls_cert[0] ? cfg.tls_cert : NULL,
        .tls_key       = cfg.tls_key[0]  ? cfg.tls_key  : NULL,
        .tls_ca        = cfg.tls_ca[0]   ? cfg.tls_ca   : NULL,
        .require_auth  = cfg.require_auth,
    };
    rc = tsdb_server_start(&opts, &g_srv);
    if (rc != TSDB_OK) {
        TSDB_LOG_ERROR("main", "server_start failed: %s", tsdb_errstr(rc));
        tsdb_close(db);
        tsdb_log_shutdown();
        tsdb_config_free(&cfg);
        return 1;
    }
    TSDB_LOG_INFO("main", "listening on %s (port=%d)",
                  cfg.bind, tsdb_server_port(g_srv));

    /* Start Prometheus metrics HTTP endpoint if configured. */
    tsdb_metrics_server_t *ms = NULL;
    if (cfg.metrics_bind[0]) {
        rc = tsdb_metrics_server_start(cfg.metrics_bind, &ms);
        if (rc != 0) {
            TSDB_LOG_ERROR("main", "metrics server start(%s) failed", cfg.metrics_bind);
        } else {
            TSDB_LOG_INFO("main", "metrics endpoint on %s (port=%d)",
                          cfg.metrics_bind, tsdb_metrics_server_port(ms));
        }
    }

    /* Start InfluxDB Line Protocol HTTP endpoint if configured. */
    if (cfg.influx_bind[0]) {
        rc = tsdb_influx_http_start(cfg.influx_bind, db);
        if (rc != 0) {
            TSDB_LOG_ERROR("main", "influx http start(%s) failed", cfg.influx_bind);
        } else {
            TSDB_LOG_INFO("main", "influx LP endpoint on %s", cfg.influx_bind);
        }
    }

    /* Install signals. */
    struct sigaction sa = {0};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGHUP,  &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    /* 7. Main loop. */
    while (!g_quit) {
        if (g_reload) {
            g_reload = 0;
            TSDB_LOG_INFO("main", "SIGHUP — reloading logging options");
            tsdb_config_t new_cfg;
            tsdb_config_defaults(&new_cfg);
            if (g_cfg_path[0])
                tsdb_config_load(&new_cfg, g_cfg_path, NULL, 0);
            tsdb_config_apply_env(&new_cfg);
            tsdb_log_init(&new_cfg);
            tsdb_config_free(&new_cfg);
        }
        struct timespec ts = { 0, 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    /* 8. Graceful shutdown. */
    TSDB_LOG_INFO("main", "shutting down");
    if (ms) tsdb_metrics_server_stop(ms);
    tsdb_influx_http_stop();
    tsdb_server_stop(g_srv);
    tsdb_close(db);
    tsdb_log_shutdown();
    tsdb_config_free(&cfg);
    return 0;
}
