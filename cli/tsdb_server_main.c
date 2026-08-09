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
#include "../src/storage/db.h"   /* tsdb_db_set_group_commit, retention/audit/pitr/catalog/backup hooks */
#include "../src/storage/compaction.h"
#include "../src/storage/retention.h"
#include "../src/catalog/audit.h"
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
#include <signal.h>
#include <time.h>
#include <unistd.h>

/* ─── signal state ─────────────────────────────────────────────────────── */

static volatile sig_atomic_t g_quit    = 0;
static volatile sig_atomic_t g_reload  = 0;
static tsdb_server_t        *g_srv     = NULL;
static char                  g_cfg_path[TSDB_CFG_MAX_PATH];

/* ─── /sql provider (standalone) ────────────────────────────────────────
 *
 * Pre-fix the standalone tsdb-server didn't register a SQL provider,
 * so /sql returned "sql provider not installed".  Cluster mode wires
 * its own callback in tsdb_node_main.c with proxy-to-master logic
 * that doesn't apply here — write a minimal per-query callback
 * focused on the standalone path: execute, JSON-format, return.
 *
 * Caught by tests/e2e/backup_restore.sh phase 7 — the sidecar that
 * boots on a /backup tarball is a standalone tsdb-server, and every
 * SELECT count(*) returned the provider-missing error. */

static int srv_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int)(ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL);
}

/* Minimal JSON-string escape — enough for column names + error
 * messages.  Caller sizes `out` to ≥ 4× `s` length to be safe. */
static void srv_json_escape(char *out, size_t cap, const char *s) {
    size_t w = 0;
    for (const char *p = s; *p && w + 8 < cap; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '"' || c == '\\') { out[w++] = '\\'; out[w++] = (char)c; }
        else if (c == '\n')        { out[w++] = '\\'; out[w++] = 'n'; }
        else if (c == '\t')        { out[w++] = '\\'; out[w++] = 't'; }
        else if (c < 0x20)         { w += snprintf(out + w, cap - w, "\\u%04x", c); }
        else                       { out[w++] = (char)c; }
    }
    if (w < cap) out[w] = '\0';
    else if (cap > 0) out[cap - 1] = '\0';
}

static const char *srv_type_name(tsdb_type_t t) {
    switch (t) {
    case TSDB_TYPE_TIMESTAMP: return "TIMESTAMP";
    case TSDB_TYPE_INT64:     return "INT64";
    case TSDB_TYPE_FLOAT64:   return "FLOAT64";
    case TSDB_TYPE_SYMBOL:    return "SYMBOL";
    default:                  return "UNKNOWN";
    }
}

static int server_main_sql_exec_cb(void *ud, const char *q, size_t qlen,
                                    const char *token, char *buf, size_t cap)
{
    (void)token;  /* standalone bypasses RBAC unless require_auth is wired
                   * via the wire path; the metrics-server /sql route is
                   * protected by the dashboard cookie when enabled. */
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return snprintf(buf, cap, "{\"error\":\"db not ready\"}");

    char *stmt = (char *)malloc(qlen + 1);
    if (!stmt) return snprintf(buf, cap, "{\"error\":\"oom\"}");
    memcpy(stmt, q, qlen);
    stmt[qlen] = '\0';

    int t0 = srv_now_ms();
    tsdb_result_t *res = NULL;
    int rc = tsdb_query(db, stmt, &res);
    free(stmt);
    if (rc != 0 || !res) {
        const char *e = tsdb_errstr(rc);
        char esc[256]; srv_json_escape(esc, sizeof(esc), e ? e : "unknown");
        int w = snprintf(buf, cap,
                         "{\"error\":\"%s\",\"ms\":%d}",
                         esc, srv_now_ms() - t0);
        return w;
    }

    int ncols = tsdb_result_ncols(res);
    int w = snprintf(buf, cap, "{\"cols\":[");
    for (int c = 0; c < ncols && (size_t)w < cap - 128; c++) {
        const char *n = tsdb_result_col_name(res, c);
        char esc[128]; srv_json_escape(esc, sizeof(esc), n ? n : "");
        w += snprintf(buf + w, cap - (size_t)w, "%s\"%s\"",
                      c > 0 ? "," : "", esc);
    }
    w += snprintf(buf + w, cap - (size_t)w, "],\"types\":[");
    for (int c = 0; c < ncols && (size_t)w < cap - 64; c++) {
        w += snprintf(buf + w, cap - (size_t)w, "%s\"%s\"",
                      c > 0 ? "," : "", srv_type_name(tsdb_result_col_type(res, c)));
    }
    w += snprintf(buf + w, cap - (size_t)w, "],\"rows\":[");
    int nrows = 0, truncated = 0;
    while (tsdb_result_next(res) > 0) {
        if ((size_t)w > cap - 4096) { truncated = 1; break; }
        w += snprintf(buf + w, cap - (size_t)w, "%s[", nrows == 0 ? "" : ",");
        for (int c = 0; c < ncols && (size_t)w < cap - 256; c++) {
            tsdb_type_t t = tsdb_result_col_type(res, c);
            switch (t) {
            case TSDB_TYPE_TIMESTAMP:
                w += snprintf(buf + w, cap - (size_t)w, "%s%lld",
                              c > 0 ? "," : "", (long long)tsdb_result_ts(res, c));
                break;
            case TSDB_TYPE_INT64:
                w += snprintf(buf + w, cap - (size_t)w, "%s%lld",
                              c > 0 ? "," : "", (long long)tsdb_result_i64(res, c));
                break;
            case TSDB_TYPE_FLOAT64:
                w += snprintf(buf + w, cap - (size_t)w, "%s%g",
                              c > 0 ? "," : "", tsdb_result_f64(res, c));
                break;
            case TSDB_TYPE_SYMBOL: {
                const char *s = tsdb_result_sym(res, c);
                char esc[256]; srv_json_escape(esc, sizeof(esc), s ? s : "");
                w += snprintf(buf + w, cap - (size_t)w, "%s\"%s\"",
                              c > 0 ? "," : "", esc);
                break;
            }
            default:
                w += snprintf(buf + w, cap - (size_t)w, "%snull", c > 0 ? "," : "");
                break;
            }
        }
        w += snprintf(buf + w, cap - (size_t)w, "]");
        nrows++;
    }
    tsdb_result_free(res);
    w += snprintf(buf + w, cap - (size_t)w,
                  "],\"nrows\":%d,\"truncated\":%s,\"ms\":%d}",
                  nrows, truncated ? "true" : "false", srv_now_ms() - t0);
    return w;
}

/* ─── management-plane trampolines (standalone) ─────────────────────────
 *
 * The cluster node (src/cluster/tsdb_node_main.c) registers a full set of
 * dashboard providers; the standalone server historically wired only /sql,
 * so /retention/sweep, /audit, /pitr and the dashboard login returned
 * "provider not registered" (503) in single-node mode.  These trampolines
 * mirror the node ones but bind to the standalone `db` from tsdb_open. */

static int srv_retention_sweep_trampoline(void *ud) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    struct tsdb_retention *r = tsdb_db_retention(db);
    if (!r) return -1;
    int deleted = 0;
    int rc = tsdb_retention_sweep_once(r, &deleted);
    return rc == TSDB_OK ? deleted : -1;
}

static int srv_audit_tail_trampoline(void *ud, int max_rows,
                                     char *buf, size_t cap) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    tsdb_audit_t *a = tsdb_db_audit(db);
    if (!a) return 0;
    return tsdb_audit_tail(a, max_rows, buf, cap);
}

static int srv_pitr_trim_trampoline(void *ud, int64_t ts_ns) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    int removed = 0;
    int rc = tsdb_pitr_trim_to(db, ts_ns, &removed);
    return rc == TSDB_OK ? removed : -1;
}

static int srv_auth_login_trampoline(void *ud, const char *user,
                                     const char *pass, char *out, size_t cap) {
    return tsdb_auth_authenticate((tsdb_db_t *)ud, user, pass, out, cap);
}

static int srv_auth_check_trampoline(void *ud, const char *token) {
    return tsdb_auth_check((tsdb_db_t *)ud, token, TSDB_PRIV_SELECT, "*");
}

/* Register the full management-plane provider set on the metrics server,
 * bound to the standalone `db`.  Mirrors the cluster node's wiring so a
 * dockerized single-node ships the same dashboard surface.  Non-static so
 * the parity test can drive these endpoints without booting a cluster. */
void tsdb_server_wire_metrics_providers(tsdb_db_t *db) {
    /* Retention GC: arm the background sweeper (honours
     * <data_dir>/retention.conf), then expose a manual /retention/sweep.
     * TSDB_RETENTION_SWEEP_MS / TSDB_RETENTION_DRY_RUN mirror the node. */
    tsdb_retention_opts_t ropts = { .sweep_interval_ns = 0, .dry_run = 0 };
    const char *sweep_ms = getenv("TSDB_RETENTION_SWEEP_MS");
    if (sweep_ms && *sweep_ms) {
        long long ms = atoll(sweep_ms);
        if (ms > 0) ropts.sweep_interval_ns = (int64_t)ms * 1000000LL;
    }
    const char *dry = getenv("TSDB_RETENTION_DRY_RUN");
    if (dry && *dry && *dry != '0') ropts.dry_run = 1;
    tsdb_db_set_retention(db, NULL, &ropts);
    tsdb_metrics_server_set_retention_sweep_provider(
        srv_retention_sweep_trampoline, db);

    /* /audit?n=N — tail the append-only audit log. */
    tsdb_metrics_server_set_audit_provider(srv_audit_tail_trampoline, db);

    /* POST /pitr?ts=<ns> — point-in-time recovery trim. */
    tsdb_metrics_server_set_pitr_provider(srv_pitr_trim_trampoline, db);

    /* GET /catalog/check — read-only catalog consistency report. */
    tsdb_metrics_server_set_catalog_check_provider(
        (tsdb_catalog_check_fn)tsdb_catalog_check, db);

    /* Backup-manifest emitter invoked by /backup before tarring. */
    tsdb_metrics_server_set_backup_manifest_provider(
        (tsdb_backup_manifest_fn)tsdb_backup_emit_manifest_file, db);

    /* Dashboard auth (only enforced when TSDB_DASHBOARD_AUTH=1). */
    tsdb_metrics_server_set_auth_provider(
        srv_auth_login_trampoline,
        srv_auth_check_trampoline,
        db);

    /* /tree has no standalone provider in libtsdb — the cluster node's
     * tree_json_cb reaches into the node manager, which doesn't exist in
     * single-node mode.  Left unwired; /tree returns its built-in empty
     * shape, which the dashboard tolerates. */
}

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
        "  --metrics-bind ADDR    metrics + dashboard HTTP listen address (e.g. 0.0.0.0:28094)\n"
        "  --influx-bind  ADDR    InfluxDB line-protocol HTTP listen address (e.g. 0.0.0.0:28092)\n"
        "  --show-config          dump effective config and exit\n"
        "  --help                 show this help\n"
        "\n"
        "Environment: any tsd.conf key may be overridden by TSDB_<UPPER_KEY>=\n"
        "Signals:     SIGINT/SIGTERM graceful shutdown, SIGHUP reload log options\n",
        prog);
}

/* ─── main ─────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    /* Ignore SIGPIPE so a client disconnect mid-write cannot kill us. */
    signal(SIGPIPE, SIG_IGN);

    const char *cfg_arg             = NULL;
    const char *override_data_dir   = NULL;
    const char *override_bind       = NULL;
    const char *override_level      = NULL;
    const char *override_debug      = NULL;
    const char *override_tls_cert   = NULL;
    const char *override_tls_key    = NULL;
    const char *override_tls_ca     = NULL;
    const char *override_metrics_bind = NULL;
    const char *override_influx_bind  = NULL;
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
        else if (!strcmp(argv[i], "--metrics-bind") && i + 1 < argc) override_metrics_bind = argv[++i];
        else if (!strcmp(argv[i], "--influx-bind")  && i + 1 < argc) override_influx_bind  = argv[++i];
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
    if (override_metrics_bind) snprintf(cfg.metrics_bind, sizeof(cfg.metrics_bind), "%s", override_metrics_bind);
    if (override_influx_bind)  snprintf(cfg.influx_bind,  sizeof(cfg.influx_bind),  "%s", override_influx_bind);
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

    /* Bridge tsd.conf data_dirs → engine striping.  The engine only reads
     * extra data dirs from the TSDB_DATA_DIRS env var (db.c); without this
     * the config-file key is parsed but silently ignored.  overwrite=0
     * keeps an explicitly exported env var authoritative. */
    if (cfg.n_data_dirs > 0 && !getenv("TSDB_DATA_DIRS")) {
        char dirs[16384];
        size_t off = 0;
        dirs[0] = '\0';
        for (int i = 0; i < cfg.n_data_dirs; i++) {
            int k = snprintf(dirs + off, sizeof(dirs) - off, "%s%s",
                             off ? "," : "", cfg.data_dirs[i]);
            if (k < 0 || (size_t)k >= sizeof(dirs) - off) break;
            off += (size_t)k;
        }
        if (off > 0)
            setenv("TSDB_DATA_DIRS", dirs, 0);
    }

    /* 6. Open DB and start server. */
    tsdb_metrics_init();
    tsdb_db_t *db = NULL;
    rc = tsdb_open(cfg.data_dir, &db);
    if (rc == TSDB_OK && db && cfg.block_points > 0) {
        /* Deployment-level knob: subsequently-created tables inherit this
         * block size.  Clamped internally into [1024, 8192]. */
        tsdb_db_set_default_block_points(db, cfg.block_points);
    }
    /* Group-commit: coalesce N independent WAL fsyncs into one (3-5x small-
     * batch ingest on spinning disks).  Standalone is single-node with no
     * replica to recover an un-fsync'd tail from, so — unlike the cluster
     * node, which defaults it ON because replication covers the window — we
     * default it OFF here to preserve strict per-commit durability.  Opt in
     * with TSDB_GROUP_COMMIT_US=<window-us> (e.g. 1000 for a 1ms window). */
    if (rc == TSDB_OK && db) {
        const char *gc_us = getenv("TSDB_GROUP_COMMIT_US");
        int64_t window_us = (gc_us && *gc_us) ? atoll(gc_us) : 0;
        if (window_us > 0) {
            tsdb_db_set_group_commit(db, window_us * 1000);  /* us -> ns */
            TSDB_LOG_INFO("main", "group commit window: %lldus", (long long)window_us);
        }
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
        /* RES-4: bridge the configured per-query deadline and result-row
         * ceiling into the wire server.  Both were parsed into tsd.conf but
         * never reached opts, so a runaway or unbounded SELECT had no bound. */
        .request_timeout_ns = cfg.request_timeout_ns,
        .max_result_rows    = cfg.max_result_rows,
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
        /* Tell the metrics endpoint about the data dir so /cluster's
         * standalone fallback can report disk capacity + auto-derived
         * weighted-hashring vn count. */
        tsdb_metrics_server_set_data_dir(cfg.data_dir);
        /* Wire the SQL provider so the dashboard's /sql endpoint works
         * in standalone mode too.  Pre-fix only the cluster-node main
         * registered it, so a tsdb-server running TSDB_ROLE=server (or
         * a backup_restore.sh sidecar) returned "sql provider not
         * installed" for every /sql POST. */
        tsdb_metrics_server_set_sql_provider(server_main_sql_exec_cb, db);
        /* RES-4 sibling: the metrics-plane /sql route is a second, unauth
         * query entry-point.  Give it the same deadline the wire plane uses so
         * a runaway SELECT here can't pin a handler thread either. */
        tsdb_metrics_server_set_sql_deadline_ns(cfg.request_timeout_ns);
        /* Bring the rest of the management plane (retention sweep, audit,
         * PITR, catalog-check, backup-manifest, dashboard auth) to parity
         * with the cluster node so single-node ships the same dashboard. */
        tsdb_server_wire_metrics_providers(db);
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

    /* Background compaction: OFF by default; enable with TSDB_COMPACTION=1.
     * Merges size-tiered flush blocks into larger cold blocks (better
     * dict/symbol ratios, fewer files).  Only touches partitions idle >60s;
     * preserves every row, so count/sum/scan results are unchanged. */
    tsdb_compactor_t *cpt = NULL;
    {
        const char *en = getenv("TSDB_COMPACTION");
        if (en && en[0] == '1') {
            const char *mb = getenv("TSDB_COMPACTION_MIN_BLOCKS");
            const char *iv = getenv("TSDB_COMPACTION_INTERVAL_MS");
            const char *wt = getenv("TSDB_COMPACTION_THREADS");
            tsdb_compactor_opts_t copts;
            memset(&copts, 0, sizeof(copts));
            copts.min_blocks_to_compact = (mb && *mb) ? atoi(mb) : 0; /* 0 → 16 */
            copts.interval_ns = (iv && *iv) ? (int64_t)atoll(iv) * 1000000LL : 0; /* 0 → 5s */
            copts.worker_threads = (wt && *wt) ? atoi(wt) : 0; /* 0 → 1 */
            if (tsdb_compactor_start(db, &copts, &cpt) == TSDB_OK)
                TSDB_LOG_INFO("main", "compaction enabled (min_blocks=%d threads=%d)",
                              copts.min_blocks_to_compact ? copts.min_blocks_to_compact : 16,
                              copts.worker_threads ? copts.worker_threads : 1);
            else
                TSDB_LOG_ERROR("main", "compaction start failed; continuing without");
        }
    }

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
    if (cpt) tsdb_compactor_stop(cpt);   /* join workers before closing db */
    if (ms) tsdb_metrics_server_stop(ms);
    tsdb_influx_http_stop();
    tsdb_server_stop(g_srv);
    tsdb_close(db);
    tsdb_log_shutdown();
    tsdb_config_free(&cfg);
    return 0;
}
