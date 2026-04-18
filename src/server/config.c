/* config.c — tsd.conf parser + defaults + env overrides. */

#include "config.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/stat.h>

/* ─── small helpers ─────────────────────────────────────────────────────── */

static char *str_trim(char *s) {
    while (*s && isspace((unsigned char)*s)) s++;
    if (!*s) return s;
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) end--;
    *end = 0;
    return s;
}

/* Strip an inline '#' / ';' comment that is not inside a double-quoted run. */
static void strip_inline_comment(char *s) {
    int inq = 0;
    for (char *p = s; *p; p++) {
        if (*p == '"') inq = !inq;
        else if (!inq && (*p == '#' || *p == ';')) { *p = 0; return; }
    }
}

/* Unquote: if value is enclosed in matching '"', strip them and decode \" \\ \n. */
static void str_unquote(char *s) {
    size_t n = strlen(s);
    if (n < 2 || s[0] != '"' || s[n-1] != '"') return;
    size_t j = 0;
    for (size_t i = 1; i < n - 1; i++) {
        char c = s[i];
        if (c == '\\' && i + 1 < n - 1) {
            char nxt = s[++i];
            if (nxt == 'n') c = '\n';
            else if (nxt == 't') c = '\t';
            else c = nxt;  /* \" \\ — literal */
        }
        s[j++] = c;
    }
    s[j] = 0;
}

static int parse_bool(const char *v, bool *out) {
    if (!strcasecmp(v, "true")  || !strcasecmp(v, "yes")
        || !strcasecmp(v, "on") || !strcmp(v, "1")) { *out = true;  return 0; }
    if (!strcasecmp(v, "false") || !strcasecmp(v, "no")
        || !strcasecmp(v, "off") || !strcmp(v, "0")) { *out = false; return 0; }
    return -1;
}

/* Parse integer with k/m/g/t suffix (multiplier 1024), supports 0x prefix. */
static int parse_int64(const char *v, int64_t *out) {
    char *end = NULL;
    errno = 0;
    long long base = strtoll(v, &end, 0);
    if (errno || end == v) return -1;
    while (*end && isspace((unsigned char)*end)) end++;
    int64_t mult = 1;
    if (*end) {
        char c = (char)tolower((unsigned char)*end);
        switch (c) {
        case 'k': mult = 1024LL; break;
        case 'm': mult = 1024LL * 1024LL; break;
        case 'g': mult = 1024LL * 1024LL * 1024LL; break;
        case 't': mult = 1024LL * 1024LL * 1024LL * 1024LL; break;
        default:  return -1;
        }
        end++;
        while (*end && isspace((unsigned char)*end)) end++;
        if (*end) return -1;
    }
    *out = (int64_t)base * mult;
    return 0;
}

/* Parse duration "1h30m", "500ms", "100ns", "2d". Returns nanoseconds. */
static int parse_duration_ns(const char *v, int64_t *out_ns) {
    int64_t total = 0;
    const char *p = v;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        char *end = NULL;
        long long num = strtoll(p, &end, 10);
        if (end == p || num < 0) return -1;
        /* suffix */
        while (*end && isspace((unsigned char)*end)) end++;
        int64_t mult = 0;
        if (end[0] == 'n' && end[1] == 's')            { mult = 1;                 end += 2; }
        else if (end[0] == 'u' && end[1] == 's')       { mult = 1000;              end += 2; }
        else if (end[0] == 'm' && end[1] == 's')       { mult = 1000000;           end += 2; }
        else if (end[0] == 's' && !isalpha((unsigned char)end[1])) { mult = 1000000000LL; end += 1; }
        else if (end[0] == 'm' && !isalpha((unsigned char)end[1])) { mult = 60LL * 1000000000LL; end += 1; }
        else if (end[0] == 'h' && !isalpha((unsigned char)end[1])) { mult = 3600LL * 1000000000LL; end += 1; }
        else if (end[0] == 'd' && !isalpha((unsigned char)end[1])) { mult = 86400LL * 1000000000LL; end += 1; }
        else return -1;
        total += (int64_t)num * mult;
        p = end;
    }
    if (total == 0 && strcmp(v, "0") != 0) return -1;
    *out_ns = total;
    return 0;
}

/* Split a comma-separated list into newly-malloc'd strings, up to cap.
 * Returns the number of tokens (≤ cap). */
static int split_csv(const char *src, char **out, int cap) {
    int n = 0;
    const char *p = src;
    while (*p && n < cap) {
        while (*p && (*p == ',' || isspace((unsigned char)*p))) p++;
        if (!*p) break;
        const char *start = p;
        while (*p && *p != ',') p++;
        size_t len = (size_t)(p - start);
        /* trim trailing spaces in token */
        while (len > 0 && isspace((unsigned char)start[len-1])) len--;
        if (len == 0) continue;
        char *tok = malloc(len + 1);
        if (!tok) return n;
        memcpy(tok, start, len);
        tok[len] = 0;
        out[n++] = tok;
    }
    return n;
}

/* ─── defaults ──────────────────────────────────────────────────────────── */

void tsdb_config_defaults(tsdb_config_t *cfg) {
    memset(cfg, 0, sizeof(*cfg));

    /* server */
    snprintf(cfg->bind, sizeof(cfg->bind), "0.0.0.0:28090");
    cfg->max_conns          = 1024;
    cfg->write_workers      = 0;
    cfg->require_auth       = false;
    cfg->request_timeout_ns = 30LL * 1000000000LL;

    /* storage */
    snprintf(cfg->data_dir, sizeof(cfg->data_dir), "/var/lib/tsdb");
    cfg->placement               = TSDB_PLACE_LEAST_USED;
    cfg->wal_policy              = TSDB_WAL_FSYNC_PERIODIC;
    cfg->wal_sync_interval_ns    = 1000000000LL;
    cfg->block_points            = 8192;
    snprintf(cfg->default_partition, sizeof(cfg->default_partition), "day");
    snprintf(cfg->default_codec,     sizeof(cfg->default_codec),     "chimp128+lz");
    cfg->max_partition_bytes     = 0;
    cfg->default_retention_ns    = 0;
    cfg->retention_sweep_every_ns = 3600LL * 1000000000LL;

    /* memtable */
    cfg->shards                 = 16;
    cfg->flush_rows_threshold   = 8192;
    cfg->flush_interval_ns      = 1000000000LL;

    /* query */
    cfg->query_workers          = 0;
    cfg->query_parallel         = true;
    cfg->query_zone_prune       = true;
    cfg->query_block_skip       = true;
    cfg->max_result_rows        = 10ULL * 1000ULL * 1000ULL;

    /* cluster */
    cfg->cluster_enabled        = false;
    snprintf(cfg->cluster_gossip_bind, sizeof(cfg->cluster_gossip_bind),
             "0.0.0.0:28080");
    snprintf(cfg->cluster_rpc_bind,    sizeof(cfg->cluster_rpc_bind),
             "0.0.0.0:28081");
    cfg->cluster_replica_factor = 3;
    cfg->cluster_write_quorum   = 2;
    cfg->cluster_rawblock       = true;
    cfg->gossip_interval_ns     = 500000000LL;
    cfg->gossip_suspect_after   = 3;
    cfg->gossip_dead_after_ns   = 10LL * 1000000000LL;
    cfg->autobalance_enabled    = true;
    cfg->autobalance_interval_ns= 30LL * 1000000000LL;
    cfg->autobalance_alpha_writes  = 0.6;
    cfg->autobalance_beta_storage  = 0.4;
    cfg->autobalance_dampen        = 0.5;

    /* logging */
    cfg->log_level = TSDB_LOG_INFO;
    cfg->log_sink  = TSDB_SINK_STDERR;
    snprintf(cfg->log_format, sizeof(cfg->log_format), "text");
    cfg->slow_query_threshold_ns = 500LL * 1000000LL;

    /* runtime */
    cfg->shutdown_grace_ns = 10LL * 1000000000LL;
    cfg->allow_core        = false;
}

/* ─── free ──────────────────────────────────────────────────────────────── */

static void free_str_array(char **arr, int n) {
    for (int i = 0; i < n; i++) { free(arr[i]); arr[i] = NULL; }
}

void tsdb_config_free(tsdb_config_t *cfg) {
    if (!cfg) return;
    free_str_array(cfg->data_dirs,     cfg->n_data_dirs);
    free_str_array(cfg->cluster_seeds, cfg->n_cluster_seeds);
    free_str_array(cfg->debug_flags,   cfg->n_debug_flags);
    cfg->n_data_dirs = cfg->n_cluster_seeds = cfg->n_debug_flags = 0;
}

/* ─── key dispatch ──────────────────────────────────────────────────────── */

static int kv_apply(tsdb_config_t *cfg, const char *key, const char *val,
                    char *errbuf, size_t errcap) {
    /* Helpers that write to errbuf if provided. */
    #define BAD(fmt, ...) do { \
        if (errbuf && errcap) snprintf(errbuf, errcap, fmt, ##__VA_ARGS__); \
        return -1; \
    } while (0)

    /* -- server -- */
    if (!strcmp(key, "bind"))                { snprintf(cfg->bind, sizeof(cfg->bind), "%s", val); return 0; }
    if (!strcmp(key, "influx_bind"))         { snprintf(cfg->influx_bind, sizeof(cfg->influx_bind), "%s", val); return 0; }
    if (!strcmp(key, "metrics_bind"))        { snprintf(cfg->metrics_bind, sizeof(cfg->metrics_bind), "%s", val); return 0; }
    if (!strcmp(key, "max_conns"))           { int64_t x; if (parse_int64(val, &x)<0) BAD("max_conns: bad int"); cfg->max_conns=(int)x; return 0; }
    if (!strcmp(key, "write_workers"))       { int64_t x; if (parse_int64(val, &x)<0) BAD("write_workers: bad int"); cfg->write_workers=(int)x; return 0; }
    if (!strcmp(key, "auth_token"))          { snprintf(cfg->auth_token, sizeof(cfg->auth_token), "%s", val); return 0; }
    if (!strcmp(key, "require_auth"))        { if (parse_bool(val,&cfg->require_auth)<0) BAD("require_auth"); return 0; }
    if (!strcmp(key, "request_timeout"))     { if (parse_duration_ns(val, &cfg->request_timeout_ns)<0) BAD("request_timeout: bad duration"); return 0; }

    /* -- storage -- */
    if (!strcmp(key, "data_dir"))            { snprintf(cfg->data_dir, sizeof(cfg->data_dir), "%s", val); return 0; }
    if (!strcmp(key, "data_dirs")) {
        free_str_array(cfg->data_dirs, cfg->n_data_dirs);
        cfg->n_data_dirs = split_csv(val, cfg->data_dirs, TSDB_CFG_MAX_DATA_DIRS);
        return 0;
    }
    if (!strcmp(key, "data_dirs_strategy")) {
        if      (!strcasecmp(val, "round_robin")) cfg->placement = TSDB_PLACE_ROUND_ROBIN;
        else if (!strcasecmp(val, "least_used"))  cfg->placement = TSDB_PLACE_LEAST_USED;
        else if (!strcasecmp(val, "hash"))        cfg->placement = TSDB_PLACE_HASH;
        else BAD("data_dirs_strategy: unknown '%s'", val);
        return 0;
    }
    if (!strcmp(key, "wal_policy")) {
        if      (!strcasecmp(val, "fsync_on_commit")) cfg->wal_policy = TSDB_WAL_FSYNC_COMMIT;
        else if (!strcasecmp(val, "fsync_periodic"))  cfg->wal_policy = TSDB_WAL_FSYNC_PERIODIC;
        else if (!strcasecmp(val, "none"))            cfg->wal_policy = TSDB_WAL_FSYNC_NONE;
        else BAD("wal_policy: unknown '%s'", val);
        return 0;
    }
    if (!strcmp(key, "wal_sync_interval"))   { if (parse_duration_ns(val, &cfg->wal_sync_interval_ns)<0) BAD("wal_sync_interval"); return 0; }
    if (!strcmp(key, "block_points"))        { int64_t x; if (parse_int64(val,&x)<0) BAD("block_points"); cfg->block_points=(int)x; return 0; }
    if (!strcmp(key, "default_partition")) {
        if (strcasecmp(val,"day") && strcasecmp(val,"hour")) BAD("default_partition: 'day' or 'hour'");
        snprintf(cfg->default_partition,sizeof(cfg->default_partition),"%s",val); return 0;
    }
    if (!strcmp(key, "default_codec"))       { snprintf(cfg->default_codec,sizeof(cfg->default_codec),"%s",val); return 0; }
    if (!strcmp(key, "max_partition_bytes")) { int64_t x; if (parse_int64(val,&x)<0) BAD("max_partition_bytes"); cfg->max_partition_bytes=(uint64_t)x; return 0; }
    if (!strcmp(key, "default_retention"))   { if (parse_duration_ns(val,&cfg->default_retention_ns)<0) BAD("default_retention"); return 0; }
    if (!strcmp(key, "retention_sweep_every")) { if (parse_duration_ns(val,&cfg->retention_sweep_every_ns)<0) BAD("retention_sweep_every"); return 0; }

    /* -- memtable -- */
    if (!strcmp(key, "shards"))              { int64_t x; if (parse_int64(val,&x)<0) BAD("shards"); cfg->shards=(int)x; return 0; }
    if (!strcmp(key, "flush_rows_threshold")) { int64_t x; if (parse_int64(val,&x)<0) BAD("flush_rows_threshold"); cfg->flush_rows_threshold=(int)x; return 0; }
    if (!strcmp(key, "flush_interval"))      { if (parse_duration_ns(val,&cfg->flush_interval_ns)<0) BAD("flush_interval"); return 0; }

    /* -- query -- */
    if (!strcmp(key, "query_workers"))       { int64_t x; if (parse_int64(val,&x)<0) BAD("query_workers"); cfg->query_workers=(int)x; return 0; }
    if (!strcmp(key, "query_parallel"))      { if (parse_bool(val,&cfg->query_parallel)<0) BAD("query_parallel"); return 0; }
    if (!strcmp(key, "query_zone_prune"))    { if (parse_bool(val,&cfg->query_zone_prune)<0) BAD("query_zone_prune"); return 0; }
    if (!strcmp(key, "query_block_skip"))    { if (parse_bool(val,&cfg->query_block_skip)<0) BAD("query_block_skip"); return 0; }
    if (!strcmp(key, "max_result_rows"))     { int64_t x; if (parse_int64(val,&x)<0) BAD("max_result_rows"); cfg->max_result_rows=(uint64_t)x; return 0; }

    /* -- cluster -- */
    if (!strcmp(key, "cluster_enabled"))     { if (parse_bool(val,&cfg->cluster_enabled)<0) BAD("cluster_enabled"); return 0; }
    if (!strcmp(key, "node_id"))             { int64_t x; if (parse_int64(val,&x)<0) BAD("node_id"); cfg->node_id=(uint64_t)x; return 0; }
    if (!strcmp(key, "cluster_gossip_bind")) { snprintf(cfg->cluster_gossip_bind,sizeof(cfg->cluster_gossip_bind),"%s",val); return 0; }
    if (!strcmp(key, "cluster_rpc_bind"))    { snprintf(cfg->cluster_rpc_bind,sizeof(cfg->cluster_rpc_bind),"%s",val); return 0; }
    if (!strcmp(key, "cluster_seeds"))       {
        free_str_array(cfg->cluster_seeds, cfg->n_cluster_seeds);
        cfg->n_cluster_seeds = split_csv(val, cfg->cluster_seeds, TSDB_CFG_MAX_SEEDS);
        return 0;
    }
    if (!strcmp(key, "cluster_replica_factor")) { int64_t x; if (parse_int64(val,&x)<0) BAD("cluster_replica_factor"); cfg->cluster_replica_factor=(int)x; return 0; }
    if (!strcmp(key, "cluster_write_quorum"))   { int64_t x; if (parse_int64(val,&x)<0) BAD("cluster_write_quorum");   cfg->cluster_write_quorum=(int)x;   return 0; }
    if (!strcmp(key, "cluster_rawblock"))       { if (parse_bool(val,&cfg->cluster_rawblock)<0) BAD("cluster_rawblock"); return 0; }
    if (!strcmp(key, "gossip_interval"))        { if (parse_duration_ns(val,&cfg->gossip_interval_ns)<0) BAD("gossip_interval"); return 0; }
    if (!strcmp(key, "gossip_suspect_after"))   { int64_t x; if (parse_int64(val,&x)<0) BAD("gossip_suspect_after"); cfg->gossip_suspect_after=(int)x; return 0; }
    if (!strcmp(key, "gossip_dead_after"))      { if (parse_duration_ns(val,&cfg->gossip_dead_after_ns)<0) BAD("gossip_dead_after"); return 0; }
    if (!strcmp(key, "autobalance_enabled"))    { if (parse_bool(val,&cfg->autobalance_enabled)<0) BAD("autobalance_enabled"); return 0; }
    if (!strcmp(key, "autobalance_interval"))   { if (parse_duration_ns(val,&cfg->autobalance_interval_ns)<0) BAD("autobalance_interval"); return 0; }
    if (!strcmp(key, "autobalance_alpha_writes")) { cfg->autobalance_alpha_writes  = strtod(val,NULL); return 0; }
    if (!strcmp(key, "autobalance_beta_storage")) { cfg->autobalance_beta_storage  = strtod(val,NULL); return 0; }
    if (!strcmp(key, "autobalance_dampen"))      { cfg->autobalance_dampen        = strtod(val,NULL); return 0; }

    /* -- federation -- */
    if (!strcmp(key, "fed_config"))          { snprintf(cfg->fed_config,sizeof(cfg->fed_config),"%s",val); return 0; }

    /* -- logging -- */
    if (!strcmp(key, "log_level")) {
        if      (!strcasecmp(val,"error")) cfg->log_level=TSDB_LOG_ERROR;
        else if (!strcasecmp(val,"warn"))  cfg->log_level=TSDB_LOG_WARN;
        else if (!strcasecmp(val,"info"))  cfg->log_level=TSDB_LOG_INFO;
        else if (!strcasecmp(val,"debug")) cfg->log_level=TSDB_LOG_DEBUG;
        else if (!strcasecmp(val,"trace")) cfg->log_level=TSDB_LOG_TRACE;
        else BAD("log_level: unknown '%s'", val);
        return 0;
    }
    if (!strcmp(key, "log_sink")) {
        if      (!strcasecmp(val,"stderr")) cfg->log_sink=TSDB_SINK_STDERR;
        else if (!strcasecmp(val,"file"))   cfg->log_sink=TSDB_SINK_FILE;
        else if (!strcasecmp(val,"syslog")) cfg->log_sink=TSDB_SINK_SYSLOG;
        else if (!strcasecmp(val,"both"))   cfg->log_sink=TSDB_SINK_BOTH;
        else BAD("log_sink: unknown '%s'", val);
        return 0;
    }
    if (!strcmp(key, "log_file"))            { snprintf(cfg->log_file,sizeof(cfg->log_file),"%s",val); return 0; }
    if (!strcmp(key, "log_format"))          { snprintf(cfg->log_format,sizeof(cfg->log_format),"%s",val); return 0; }
    if (!strcmp(key, "debug_flags")) {
        free_str_array(cfg->debug_flags, cfg->n_debug_flags);
        /* Accept comma or space separators by normalising spaces to commas. */
        char buf[1024];
        snprintf(buf, sizeof(buf), "%s", val);
        for (char *p = buf; *p; p++) if (*p == ' ') *p = ',';
        cfg->n_debug_flags = split_csv(buf, cfg->debug_flags, TSDB_CFG_MAX_DEBUG_FLAGS);
        return 0;
    }
    if (!strcmp(key, "slow_query_threshold")){ if (parse_duration_ns(val,&cfg->slow_query_threshold_ns)<0) BAD("slow_query_threshold"); return 0; }
    if (!strcmp(key, "cpu_profile_path"))    { snprintf(cfg->cpu_profile_path,sizeof(cfg->cpu_profile_path),"%s",val); return 0; }

    /* -- runtime -- */
    if (!strcmp(key, "user"))                { snprintf(cfg->user,sizeof(cfg->user),"%s",val); return 0; }
    if (!strcmp(key, "group"))               { snprintf(cfg->group,sizeof(cfg->group),"%s",val); return 0; }
    if (!strcmp(key, "pidfile"))             { snprintf(cfg->pidfile,sizeof(cfg->pidfile),"%s",val); return 0; }
    if (!strcmp(key, "shutdown_grace"))      { if (parse_duration_ns(val,&cfg->shutdown_grace_ns)<0) BAD("shutdown_grace"); return 0; }
    if (!strcmp(key, "allow_core"))          { if (parse_bool(val,&cfg->allow_core)<0) BAD("allow_core"); return 0; }

    /* -- TLS -- */
    if (!strcmp(key, "tls_cert")) { snprintf(cfg->tls_cert,sizeof(cfg->tls_cert),"%s",val); return 0; }
    if (!strcmp(key, "tls_key"))  { snprintf(cfg->tls_key, sizeof(cfg->tls_key), "%s",val); return 0; }
    if (!strcmp(key, "tls_ca"))   { snprintf(cfg->tls_ca,  sizeof(cfg->tls_ca),  "%s",val); return 0; }

    /* -- dev -- */
    if (!strcmp(key, "cpu_level"))           { snprintf(cfg->cpu_level,sizeof(cfg->cpu_level),"%s",val); return 0; }
    if (!strcmp(key, "force_codec"))         { snprintf(cfg->force_codec,sizeof(cfg->force_codec),"%s",val); return 0; }
    if (!strcmp(key, "fault_flush_delay"))   { if (parse_duration_ns(val,&cfg->fault_flush_delay_ns)<0) BAD("fault_flush_delay"); return 0; }
    if (!strcmp(key, "fault_rpc_delay"))     { if (parse_duration_ns(val,&cfg->fault_rpc_delay_ns)<0) BAD("fault_rpc_delay"); return 0; }

    BAD("unknown key '%s'", key);
    #undef BAD
}

/* ─── load ──────────────────────────────────────────────────────────────── */

int tsdb_config_load(tsdb_config_t *cfg, const char *path,
                     char *errbuf, size_t errcap) {
    if (errbuf && errcap) errbuf[0] = 0;
    if (!path || !*path) return -1;

    FILE *f = fopen(path, "r");
    if (!f) return -1;   /* missing file is not an error */

    snprintf(cfg->loaded_from, sizeof(cfg->loaded_from), "%s", path);

    char  line[4096];
    int   lineno = 0;
    int   errors = 0;
    char  first_err[256];
    first_err[0] = 0;

    while (fgets(line, sizeof(line), f)) {
        lineno++;

        strip_inline_comment(line);
        char *s = str_trim(line);
        if (!*s) continue;

        /* Section header [name] — parsed but flat-namespaced. */
        if (s[0] == '[') continue;

        char *eq = strchr(s, '=');
        if (!eq) {
            if (errors == 0)
                snprintf(first_err, sizeof(first_err),
                         "line %d: expected key = value", lineno);
            errors++;
            continue;
        }
        *eq = 0;
        char *key = str_trim(s);
        char *val = str_trim(eq + 1);
        str_unquote(val);

        char ekv[128];
        if (kv_apply(cfg, key, val, ekv, sizeof(ekv)) < 0) {
            if (errors == 0) snprintf(first_err, sizeof(first_err),
                                      "line %d: %s", lineno, ekv);
            errors++;
        }
    }

    fclose(f);
    cfg->parse_errors = errors;

    /* Compute derived field: TLS enabled when both cert and key are set. */
    cfg->tls_enabled = (cfg->tls_cert[0] != '\0' && cfg->tls_key[0] != '\0');

    if (errors > 0) {
        if (errbuf && errcap) snprintf(errbuf, errcap, "%s", first_err);
        return -2;
    }
    return 0;
}

/* ─── env overrides ─────────────────────────────────────────────────────── */

/* Helper: look up "TSDB_<UPPER(key)>" env. Dots and dashes map to '_'. */
static const char *env_for(const char *key, char *buf, size_t cap) {
    snprintf(buf, cap, "TSDB_");
    size_t i = strlen(buf);
    for (const char *p = key; *p && i < cap - 1; p++) {
        char c = (char)toupper((unsigned char)*p);
        if (c == '.' || c == '-') c = '_';
        buf[i++] = c;
    }
    buf[i] = 0;
    return getenv(buf);
}

void tsdb_config_apply_env(tsdb_config_t *cfg) {
    static const char *keys[] = {
        "bind","influx_bind","metrics_bind","max_conns","write_workers","auth_token","require_auth","request_timeout",
        "data_dir","data_dirs","data_dirs_strategy","wal_policy","wal_sync_interval",
        "block_points","default_partition","default_codec","max_partition_bytes",
        "default_retention","retention_sweep_every","shards","flush_rows_threshold",
        "flush_interval","query_workers","query_parallel","query_zone_prune",
        "query_block_skip","max_result_rows","cluster_enabled","node_id",
        "cluster_gossip_bind","cluster_rpc_bind","cluster_seeds",
        "cluster_replica_factor","cluster_write_quorum","cluster_rawblock",
        "gossip_interval","gossip_suspect_after","gossip_dead_after",
        "autobalance_enabled","autobalance_interval","autobalance_alpha_writes",
        "autobalance_beta_storage","autobalance_dampen","fed_config",
        "log_level","log_sink","log_file","log_format","debug_flags",
        "slow_query_threshold","cpu_profile_path","user","group","pidfile",
        "shutdown_grace","allow_core","cpu_level","force_codec",
        "fault_flush_delay","fault_rpc_delay",
        "tls_cert","tls_key","tls_ca",
        NULL
    };
    char envname[128];
    for (int i = 0; keys[i]; i++) {
        const char *v = env_for(keys[i], envname, sizeof(envname));
        if (v) (void)kv_apply(cfg, keys[i], v, NULL, 0);
    }
    /* Recompute derived TLS flag after env overrides. */
    cfg->tls_enabled = (cfg->tls_cert[0] != '\0' && cfg->tls_key[0] != '\0');
}

/* ─── locate ────────────────────────────────────────────────────────────── */

static int file_exists(const char *p) {
    struct stat st;
    return (p && *p && stat(p, &st) == 0 && S_ISREG(st.st_mode));
}

char *tsdb_config_locate(const char *explicit_path) {
    if (explicit_path && *explicit_path && file_exists(explicit_path))
        return strdup(explicit_path);

    const char *env = getenv("TSDB_CONFIG");
    if (env && file_exists(env)) return strdup(env);

    if (file_exists("./tsd.conf")) return strdup("./tsd.conf");
    if (file_exists("/etc/tsd.conf")) return strdup("/etc/tsd.conf");
    return NULL;
}

/* ─── dump ──────────────────────────────────────────────────────────────── */

static const char *place_name(tsdb_placement_t p) {
    switch (p) {
    case TSDB_PLACE_ROUND_ROBIN: return "round_robin";
    case TSDB_PLACE_LEAST_USED:  return "least_used";
    case TSDB_PLACE_HASH:        return "hash";
    default:                     return "?";
    }
}
static const char *wal_name(tsdb_wal_policy_t p) {
    switch (p) {
    case TSDB_WAL_FSYNC_COMMIT:   return "fsync_on_commit";
    case TSDB_WAL_FSYNC_PERIODIC: return "fsync_periodic";
    case TSDB_WAL_FSYNC_NONE:     return "none";
    default: return "?";
    }
}
static const char *level_name(tsdb_log_level_t l) {
    switch (l) {
    case TSDB_LOG_ERROR: return "error";
    case TSDB_LOG_WARN:  return "warn";
    case TSDB_LOG_INFO:  return "info";
    case TSDB_LOG_DEBUG: return "debug";
    case TSDB_LOG_TRACE: return "trace";
    default: return "?";
    }
}
static const char *sink_name(tsdb_log_sink_t s) {
    switch (s) {
    case TSDB_SINK_STDERR: return "stderr";
    case TSDB_SINK_FILE:   return "file";
    case TSDB_SINK_SYSLOG: return "syslog";
    case TSDB_SINK_BOTH:   return "both";
    default: return "?";
    }
}

void tsdb_config_dump(const tsdb_config_t *cfg) {
    fprintf(stderr, "# tsd.conf effective config (loaded_from=%s)\n",
            cfg->loaded_from[0] ? cfg->loaded_from : "<defaults>");
    fprintf(stderr, "bind                   = %s\n", cfg->bind);
    fprintf(stderr, "influx_bind            = %s\n", cfg->influx_bind[0] ? cfg->influx_bind : "(disabled)");
    fprintf(stderr, "metrics_bind           = %s\n", cfg->metrics_bind[0] ? cfg->metrics_bind : "(disabled)");
    fprintf(stderr, "max_conns              = %d\n", cfg->max_conns);
    fprintf(stderr, "write_workers          = %d\n", cfg->write_workers);
    fprintf(stderr, "data_dir               = %s\n", cfg->data_dir);
    if (cfg->n_data_dirs > 0) {
        fprintf(stderr, "data_dirs              =");
        for (int i = 0; i < cfg->n_data_dirs; i++)
            fprintf(stderr, " %s%s", cfg->data_dirs[i], i+1<cfg->n_data_dirs?",":"");
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "data_dirs_strategy     = %s\n", place_name(cfg->placement));
    fprintf(stderr, "wal_policy             = %s\n", wal_name(cfg->wal_policy));
    fprintf(stderr, "block_points           = %d\n", cfg->block_points);
    fprintf(stderr, "default_partition      = %s\n", cfg->default_partition);
    fprintf(stderr, "default_codec          = %s\n", cfg->default_codec);
    fprintf(stderr, "shards                 = %d\n", cfg->shards);
    fprintf(stderr, "flush_rows_threshold   = %d\n", cfg->flush_rows_threshold);
    fprintf(stderr, "query_workers          = %d\n", cfg->query_workers);
    fprintf(stderr, "query_parallel         = %s\n", cfg->query_parallel ? "true":"false");
    fprintf(stderr, "query_zone_prune       = %s\n", cfg->query_zone_prune ? "true":"false");
    fprintf(stderr, "cluster_enabled        = %s\n", cfg->cluster_enabled ? "true":"false");
    if (cfg->cluster_enabled) {
        fprintf(stderr, "cluster_gossip_bind    = %s\n", cfg->cluster_gossip_bind);
        fprintf(stderr, "cluster_rpc_bind       = %s\n", cfg->cluster_rpc_bind);
        if (cfg->n_cluster_seeds > 0) {
            fprintf(stderr, "cluster_seeds          =");
            for (int i = 0; i < cfg->n_cluster_seeds; i++)
                fprintf(stderr, " %s%s", cfg->cluster_seeds[i], i+1<cfg->n_cluster_seeds?",":"");
            fprintf(stderr, "\n");
        }
        fprintf(stderr, "cluster_replica_factor = %d\n", cfg->cluster_replica_factor);
        fprintf(stderr, "cluster_write_quorum   = %d\n", cfg->cluster_write_quorum);
    }
    fprintf(stderr, "log_level              = %s\n", level_name(cfg->log_level));
    fprintf(stderr, "log_sink               = %s\n", sink_name(cfg->log_sink));
    if (cfg->n_debug_flags > 0) {
        fprintf(stderr, "debug_flags            =");
        for (int i = 0; i < cfg->n_debug_flags; i++)
            fprintf(stderr, " %s", cfg->debug_flags[i]);
        fprintf(stderr, "\n");
    }
}
