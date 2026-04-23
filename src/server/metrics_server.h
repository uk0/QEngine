/* metrics_server.h — Minimal HTTP server exposing GET /metrics in
 * Prometheus text exposition format (version 0.0.4).
 *
 * Runs on a separate port from the wire-protocol server (default 28094).
 * Only serves GET /metrics; all other requests receive 404.
 *
 * Usage:
 *   tsdb_metrics_server_t *ms;
 *   tsdb_metrics_server_start("0.0.0.0:28094", &ms);
 *   ...
 *   tsdb_metrics_server_stop(ms);
 */
#ifndef TSDB_SERVER_METRICS_SERVER_H
#define TSDB_SERVER_METRICS_SERVER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_metrics_server tsdb_metrics_server_t;

/*
 * Start the metrics HTTP listener.
 * bind_addr: "host:port", e.g. "0.0.0.0:28094". NULL → disabled (returns 0, *out=NULL).
 * Returns 0 on success (or bind_addr==NULL), negative on error.
 */
int  tsdb_metrics_server_start(const char *bind_addr, tsdb_metrics_server_t **out);

/* Stop and free the server. Safe to call with NULL. */
void tsdb_metrics_server_stop(tsdb_metrics_server_t *ms);

/* Actual bound port (useful when port was 0). */
int  tsdb_metrics_server_port(const tsdb_metrics_server_t *ms);

/* ---- /cluster provider hook --------------------------------------------- *
 * By default GET /cluster returns a synthetic single-node JSON so a plain
 * tsdb-server still reports sensible topology data.  Cluster-aware code
 * (src/cluster/node.c, tsdb_node_main.c) can install a provider that
 * returns the true membership + autobalance snapshot.
 *
 * Provider signature:
 *   len = fn(userdata, buf, cap);
 * must write a NUL-unterminated JSON body into buf[0..cap-1] and return
 * the number of bytes written (0 on failure).
 */
typedef int (*tsdb_cluster_json_fn)(void *userdata, char *buf, size_t cap);

void tsdb_metrics_server_set_cluster_provider(tsdb_cluster_json_fn fn,
                                               void *userdata);

/* Optional data directory override used by the default /cluster response
 * (so it can report total_bytes / free_bytes from statvfs). */
void tsdb_metrics_server_set_data_dir(const char *path);

/* ---- /tree provider hook ------------------------------------------------- *
 * Returns a JSON tree describing this node's catalog for the dashboard's
 * left-hand navigator.  Concrete shape is provider-defined; the dashboard
 * expects:
 *   { "db": "…",
 *     "tables": [{"name":"…","ncols":N,"nrows_hint":N,
 *                 "is_stable":bool,"children":[{"name":"<device>"}]}] }
 */
typedef int (*tsdb_tree_json_fn)(void *userdata, char *buf, size_t cap);
void tsdb_metrics_server_set_tree_provider(tsdb_tree_json_fn fn,
                                            void *userdata);

/* ---- /sql provider hook -------------------------------------------------- *
 * Runs a single SQL / QTL statement and writes a JSON result body.  The
 * dashboard POSTs {"q":"<sql>"} and renders {"cols":[…], "types":[…],
 * "rows":[[…]], "nrows":N, "truncated":bool, "ms":X} on success or
 * {"error":"…"} on failure.  Caller (metrics_server) handles the HTTP
 * envelope; the provider only produces the body bytes.
 *
 * Returns bytes written; negative on failure, 0 → fallback to built-in
 * "disabled" body.
 */
/* token: hex session cookie from Cookie header, or "" if unauthenticated /
 * auth disabled.  Provider uses it to enforce RBAC via tsdb_query_auth. */
typedef int (*tsdb_sql_exec_fn)(void *userdata,
                                const char *q, size_t qlen,
                                const char *token,
                                char *buf, size_t cap);
void tsdb_metrics_server_set_sql_provider(tsdb_sql_exec_fn fn,
                                           void *userdata);

/* ---- /retention/sweep provider hook ------------------------------------- *
 * Synchronous trigger for the retention GC.  Invoked by the
 * /retention/sweep HTTP endpoint.  Returns the number of partitions
 * deleted (or -1 on failure).  Runs in the HTTP request thread so
 * keep it bounded. */
typedef int (*tsdb_retention_sweep_fn)(void *userdata);
void tsdb_metrics_server_set_retention_sweep_provider(
    tsdb_retention_sweep_fn fn, void *userdata);

/* ---- /audit provider hook ---------------------------------------------- *
 * Returns the last `max_rows` JSONL records from the audit log into buf,
 * returning the number of bytes written (0 = empty, negative = error).
 * The HTTP handler wraps them as `{"rows":[...]}` for the dashboard. */
typedef int (*tsdb_audit_tail_fn)(void *userdata, int max_rows,
                                    char *buf, size_t cap);
void tsdb_metrics_server_set_audit_provider(tsdb_audit_tail_fn fn,
                                              void *userdata);

/* ---- Dashboard login hooks --------------------------------------------- *
 * When configured (TSDB_DASHBOARD_AUTH=1), the server gates every route
 * that isn't in the "always public" set (/health, /metrics, /login,
 * /logout) behind a session cookie.  The auth provider is responsible
 * for turning (user, password) into a hex session token and for
 * validating a previously-issued token.  Returning TSDB_OK = permit;
 * anything else = redirect to /login.  Tokens are issued and checked
 * through the auth module in src/catalog/user.c so the usual RBAC
 * grant tables stay authoritative. */
typedef int (*tsdb_auth_login_fn)(void *userdata,
                                    const char *user,
                                    const char *password,
                                    char *out_token, size_t cap);
typedef int (*tsdb_auth_check_fn)(void *userdata, const char *token);
void tsdb_metrics_server_set_auth_provider(tsdb_auth_login_fn login,
                                             tsdb_auth_check_fn check,
                                             void *userdata);

/* ---- /raft provider hook ------------------------------------------------- *
 * Returns a JSON snapshot of the local Raft state machine for the
 * acceptance harness and the topology dashboard.  Expected shape:
 *   { "self_id":"…", "role":"follower|candidate|leader",
 *     "current_term":N, "leader_id":"…",
 *     "commit_index":N, "last_applied":N, "last_index":N }
 *
 * Returns bytes written; 0 → server serves an empty {} for nodes that
 * don't run raft (role=data, or TSDB_CONSENSUS=fanout).
 */
typedef int (*tsdb_raft_json_fn)(void *userdata, char *buf, size_t cap);
void tsdb_metrics_server_set_raft_provider(tsdb_raft_json_fn fn,
                                            void *userdata);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_SERVER_METRICS_SERVER_H */
