/* server.h — TCP event-loop server for tsdb wire protocol v1.
 *
 * Model: one accept thread + thread-per-connection workers (POSIX, non-C++).
 * Cross-platform: macOS kqueue and Linux epoll/poll both supported via
 * blocking per-connection I/O — the OS scheduler is the event loop.
 */
#ifndef TSDB_SERVER_SERVER_H
#define TSDB_SERVER_SERVER_H

#include <stdbool.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque server handle. */
typedef struct tsdb_server tsdb_server_t;

typedef struct {
    const char *bind_addr;     /* e.g. "0.0.0.0:28090"; NULL → "0.0.0.0:28090" */
    int         max_conns;     /* soft cap (default 1024) */
    int         write_workers; /* reserved for future pooled writes; ignored now */
    tsdb_db_t  *db;            /* underlying DB — must remain valid for server lifetime */
    /* TLS (optional — all three NULL/0 = plaintext mode) */
    const char *tls_cert;      /* PEM server certificate file path */
    const char *tls_key;       /* PEM server private key file path */
    const char *tls_ca;        /* PEM CA bundle for mTLS client auth (may be NULL) */
    /* When true, the server rejects QUERY / WRITE_BATCH / DDL frames that
     * arrive before the connection has authenticated via TSDB_MT_AUTH_LOGIN.
     * When false (default, legacy), unauthenticated frames run in bypass
     * mode through tsdb_query — matches pre-RBAC behaviour. */
    bool        require_auth;
    /* Per-query deadline applied to QUERY frames.  0 (default) → no
     * deadline.  When > 0, handle_query sets a thread-local deadline
     * = clock_monotonic + this value before calling tsdb_query, and the
     * executor aborts at the next block boundary with TSDB_ERR_TIMEOUT
     * once exceeded.  Bounds the runaway-SELECT case so a single bad
     * query can't pin a connection thread forever. */
    int64_t     request_timeout_ns;
    /* Result-row ceiling applied to QUERY frames.  0 (default) → unlimited.
     * When > 0, handle_query stops streaming after this many rows and sets
     * FIN, so a single unbounded SELECT * cannot materialise the whole table
     * into memory / onto the wire and OOM-kill the process.  The client sees
     * a normal (truncated) result rather than an error or a hang. */
    uint64_t    max_result_rows;
} tsdb_server_opts_t;

/*
 * Start server.
 * Returns TSDB_OK on success; *out is populated.
 * Spawns an accept thread; per-connection threads created on demand.
 */
int  tsdb_server_start(const tsdb_server_opts_t *opts, tsdb_server_t **out);

/*
 * Graceful stop.
 * Signals accept thread to exit, closes listen fd, waits for accept thread.
 * Active connections are allowed to drain their current request; after that
 * their sockets are closed.
 */
void tsdb_server_stop(tsdb_server_t *s);

/* Return the actual bound port (useful when bind_addr had port 0). */
int  tsdb_server_port(tsdb_server_t *s);

/* ---- Stats --------------------------------------------------------------- */

typedef struct {
    uint64_t conns;        /* currently active connections */
    uint64_t rows_written; /* lifetime rows committed */
    uint64_t queries;      /* lifetime query requests */
    uint64_t subs_active;  /* active SUBSCRIBE sessions */
    uint64_t bytes_in;
    uint64_t bytes_out;
} tsdb_server_stats_t;

void tsdb_server_stats(tsdb_server_t *s, tsdb_server_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_SERVER_SERVER_H */
