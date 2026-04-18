/* metrics.h — Prometheus-format self-telemetry for QEngine.
 *
 * Provides a fixed global registry of counters, gauges, and histograms.
 * All update functions are lock-free (atomic) for counters/gauges; histograms
 * use a per-metric spinlock so bucket increments are consistent.
 *
 * Render: tsdb_metrics_render() serialises everything to the Prometheus text
 * exposition format (version 0.0.4).  Caller frees the returned buffer.
 *
 * Usage:
 *   tsdb_metrics_init();                        // once at startup
 *   tsdb_metric_inc("qengine_connections_total");
 *   tsdb_metric_add("qengine_rows_written_total", n);
 *   tsdb_metric_gauge_set("qengine_memtable_rows", rows);
 *   tsdb_metric_observe("qengine_query_duration_ms", 4.7);
 *   size_t len;
 *   char *text = tsdb_metrics_render(&len);     // caller frees
 */
#ifndef TSDB_SERVER_METRICS_H
#define TSDB_SERVER_METRICS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Initialise the registry.  Safe to call multiple times (idempotent). */
void tsdb_metrics_init(void);

/* ---- Counter -------------------------------------------------------------- */

/* Increment named counter by 1. */
void tsdb_metric_inc(const char *name);

/* Add val to named counter. */
void tsdb_metric_add(const char *name, uint64_t val);

/* ---- Gauge --------------------------------------------------------------- */

/* Set named gauge to val. */
void tsdb_metric_gauge_set(const char *name, double val);

/* Increment gauge by 1 (convenience). */
void tsdb_metric_gauge_inc(const char *name);

/* Decrement gauge by 1 (convenience). */
void tsdb_metric_gauge_dec(const char *name);

/* ---- Histogram ----------------------------------------------------------- */

/* Observe a single value (ms for duration, rows for batch size). */
void tsdb_metric_observe(const char *name, double val);

/* ---- Rendering ----------------------------------------------------------- */

/*
 * Render all registered metrics to Prometheus text format.
 * Returns a heap-allocated, NUL-terminated string; caller must free().
 * If out_len is non-NULL, *out_len is set to the string length (excl. NUL).
 * Returns NULL on allocation failure.
 */
char *tsdb_metrics_render(size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_SERVER_METRICS_H */
