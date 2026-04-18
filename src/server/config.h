/* config.h — tsd.conf parser and defaults.
 *
 * Usage:
 *   tsdb_config_t cfg;
 *   tsdb_config_defaults(&cfg);
 *   tsdb_config_load(&cfg, "/etc/tsd.conf");   // optional; silent if missing
 *   tsdb_config_apply_env(&cfg);                // TSDB_* env overrides
 *
 * After a successful load + apply_env, cfg is ready to feed into
 * tsdb_server_start(). Ownership is simple — all strings live in cfg itself
 * (fixed-size buffers or heap allocations freed by tsdb_config_free).
 */
#ifndef TSDB_SERVER_CONFIG_H
#define TSDB_SERVER_CONFIG_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#define TSDB_CFG_MAX_PATH      4096
#define TSDB_CFG_MAX_DATA_DIRS   16
#define TSDB_CFG_MAX_SEEDS       32
#define TSDB_CFG_MAX_DEBUG_FLAGS 16

/* Log level ordinals (low = terse). */
typedef enum {
    TSDB_LOG_ERROR = 0,
    TSDB_LOG_WARN  = 1,
    TSDB_LOG_INFO  = 2,
    TSDB_LOG_DEBUG = 3,
    TSDB_LOG_TRACE = 4
} tsdb_log_level_t;

/* Log sink. */
typedef enum {
    TSDB_SINK_STDERR = 0,
    TSDB_SINK_FILE   = 1,
    TSDB_SINK_SYSLOG = 2,
    TSDB_SINK_BOTH   = 3
} tsdb_log_sink_t;

typedef enum {
    TSDB_PLACE_ROUND_ROBIN = 0,
    TSDB_PLACE_LEAST_USED  = 1,
    TSDB_PLACE_HASH        = 2
} tsdb_placement_t;

typedef enum {
    TSDB_WAL_FSYNC_COMMIT = 0,
    TSDB_WAL_FSYNC_PERIODIC = 1,
    TSDB_WAL_FSYNC_NONE   = 2
} tsdb_wal_policy_t;

typedef struct {
    /* -- server -- */
    char     bind[128];
    int      max_conns;
    int      write_workers;
    char     auth_token[128];
    int64_t  request_timeout_ns;

    /* -- storage -- */
    char     data_dir[TSDB_CFG_MAX_PATH];
    char    *data_dirs[TSDB_CFG_MAX_DATA_DIRS];
    int      n_data_dirs;
    tsdb_placement_t placement;
    tsdb_wal_policy_t wal_policy;
    int64_t  wal_sync_interval_ns;
    int      block_points;
    char     default_partition[8];       /* "day" / "hour" */
    char     default_codec[32];
    uint64_t max_partition_bytes;
    int64_t  default_retention_ns;
    int64_t  retention_sweep_every_ns;

    /* -- memtable -- */
    int      shards;
    int      flush_rows_threshold;
    int64_t  flush_interval_ns;

    /* -- query -- */
    int      query_workers;
    bool     query_parallel;
    bool     query_zone_prune;
    bool     query_block_skip;
    uint64_t max_result_rows;

    /* -- cluster -- */
    bool     cluster_enabled;
    uint64_t node_id;
    char     cluster_gossip_bind[128];
    char     cluster_rpc_bind[128];
    char    *cluster_seeds[TSDB_CFG_MAX_SEEDS];
    int      n_cluster_seeds;
    int      cluster_replica_factor;
    int      cluster_write_quorum;
    bool     cluster_rawblock;
    int64_t  gossip_interval_ns;
    int      gossip_suspect_after;
    int64_t  gossip_dead_after_ns;
    bool     autobalance_enabled;
    int64_t  autobalance_interval_ns;
    double   autobalance_alpha_writes;
    double   autobalance_beta_storage;
    double   autobalance_dampen;

    /* -- federation -- */
    char     fed_config[TSDB_CFG_MAX_PATH];

    /* -- logging / debug -- */
    tsdb_log_level_t log_level;
    tsdb_log_sink_t  log_sink;
    char     log_file[TSDB_CFG_MAX_PATH];
    char     log_format[8];              /* "text" / "json" */
    char    *debug_flags[TSDB_CFG_MAX_DEBUG_FLAGS];
    int      n_debug_flags;
    int64_t  slow_query_threshold_ns;
    char     cpu_profile_path[TSDB_CFG_MAX_PATH];

    /* -- runtime -- */
    char     user[64];
    char     group[64];
    char     pidfile[TSDB_CFG_MAX_PATH];
    int64_t  shutdown_grace_ns;
    bool     allow_core;

    /* -- InfluxDB Line Protocol HTTP endpoint -- */
    char     influx_bind[128];  /* "host:port" e.g. "0.0.0.0:28091"; empty = disabled */

    /* -- Prometheus metrics HTTP endpoint -- */
    char     metrics_bind[128]; /* "host:port" e.g. "0.0.0.0:28094"; empty = disabled */

    /* -- TLS -- */
    char     tls_cert[TSDB_CFG_MAX_PATH];   /* server certificate PEM */
    char     tls_key[TSDB_CFG_MAX_PATH];    /* server private key PEM */
    char     tls_ca[TSDB_CFG_MAX_PATH];     /* optional CA bundle for mTLS */
    bool     tls_enabled;                   /* computed: tls_cert + tls_key non-empty */

    /* -- dev toggles -- */
    char     cpu_level[16];
    char     force_codec[16];
    int64_t  fault_flush_delay_ns;
    int64_t  fault_rpc_delay_ns;

    /* Parse diagnostics. */
    char     loaded_from[TSDB_CFG_MAX_PATH];
    int      parse_errors;
} tsdb_config_t;

/* Populate cfg with all defaults. Always succeeds. */
void tsdb_config_defaults(tsdb_config_t *cfg);

/* Free all heap-allocated members (data_dirs[], cluster_seeds[], debug_flags[]).
 * Does not free cfg itself. Safe to call on a zero-initialised struct. */
void tsdb_config_free(tsdb_config_t *cfg);

/* Load from path. Returns:
 *   0  success
 *  -1  file not found (cfg left with defaults, loaded_from="")
 *  -2  parse errors (cfg.parse_errors > 0)
 *  -3  I/O error
 *
 * Never prints to stderr itself — caller decides logging. Any syntax errors
 * are counted in cfg->parse_errors and a short message is placed in
 * errbuf (if non-NULL, up to errcap-1 bytes, NUL-terminated). */
int tsdb_config_load(tsdb_config_t *cfg, const char *path,
                     char *errbuf, size_t errcap);

/* Apply TSDB_* environment variable overrides. Called after load to let
 * operators force a value from the shell / systemd unit / container env. */
void tsdb_config_apply_env(tsdb_config_t *cfg);

/* Locate the best config path given an optional explicit one.
 * Search order documented in tsd.conf; returns newly-strdup'd path or NULL.
 * Caller owns the string. */
char *tsdb_config_locate(const char *explicit_path);

/* Dump current config to stderr in a human-readable form.
 * Intended for `tsdb-server --show-config`. */
void tsdb_config_dump(const tsdb_config_t *cfg);

#endif /* TSDB_SERVER_CONFIG_H */
