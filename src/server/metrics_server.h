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

#ifdef __cplusplus
}
#endif

#endif /* TSDB_SERVER_METRICS_SERVER_H */
