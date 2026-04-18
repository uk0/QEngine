/* promql.h — PromQL subset translation layer for tsdb.
 *
 * Translates a limited subset of PromQL expressions into QTL (tsdb Query
 * Text Language) strings, executes them against a tsdb_db_t, and formats
 * the results as Prometheus-compatible JSON responses.
 *
 * Supported PromQL subset:
 *   metric_name                          → SELECT ts, value FROM metric_name ORDER BY ts
 *   metric_name{k="v"[,k2="v2"...]}     → ... WHERE k='v' [AND k2='v2'] ORDER BY ts
 *   rate(metric[Xm])                     → SELECT ts, derivative(value) FROM metric ORDER BY ts
 *   avg_over_time(metric[Xm])            → SELECT ts, avg(value) FROM metric SAMPLE BY Xm
 *   sum(metric)                          → SELECT sum(value) FROM metric
 *   sum by (label) (metric)              → SELECT label, sum(value) FROM metric GROUP BY label
 *   avg(metric)                          → SELECT avg(value) FROM metric
 *   count(metric)                        → SELECT count(*) FROM metric
 *
 * NOTE on rate(): PromQL rate() divides by window duration; we approximate
 * with derivative() (instantaneous slope). This is a known semantic
 * difference, documented intentionally.
 *
 * Unsupported expressions return NULL from tsdb_promql_compile() with an
 * error message in errbuf.
 */
#ifndef TSDB_SERVER_PROMQL_H
#define TSDB_SERVER_PROMQL_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Compile a PromQL expression to an equivalent QTL query string.
 * metric_name → table name; labels → WHERE; rate/sum support basic forms.
 *
 * Returns a newly malloc'd QTL string (caller frees), or NULL on unsupported.
 * On NULL, errbuf (if non-NULL) is filled with a NUL-terminated message.
 *
 * start_ns / end_ns / step_ns are for future range-aware compilation;
 * currently unused but accepted for API compatibility.
 */
char *tsdb_promql_compile(const char *promql,
                          tsdb_ts_t start_ns,
                          tsdb_ts_t end_ns,
                          int64_t step_ns,
                          char *errbuf, size_t errcap);

/*
 * Run a compiled QTL against db, then format the result as a Prometheus
 * range-query JSON response:
 *
 *   {"status":"success","data":{"resultType":"matrix","result":[
 *     {"metric":{"__name__":"cpu","host":"A"},"values":[[ts,"val"],...]}
 *   ]}}
 *
 * metric_name is the original metric name (used as __name__ in the response).
 * group_label, if non-NULL, is the column name used for series grouping.
 *
 * Returns a newly malloc'd JSON string (caller frees), or NULL on error.
 * *out_len is set to the byte length (not including NUL) on success.
 */
char *tsdb_promql_format_result(tsdb_db_t *db,
                                const char *qtl,
                                const char *metric_name,
                                const char *group_label,
                                size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_SERVER_PROMQL_H */
