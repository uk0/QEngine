/* influx_line.h — InfluxDB Line Protocol parser + ingest for tsdb.
 *
 * Line protocol format:
 *   measurement[,tag_key=tag_val...] field_key=field_val[,field_key=field_val...] [unix_ns]
 *
 * Escape sequences in measurement/tag keys and values, field keys:
 *   \,  \=  \ (space)  \\
 * Escape sequences in field string values (inside double quotes):
 *   \"  \\
 * Field type detection:
 *   "..."       -> SYMBOL  (string)
 *   42i or 42u  -> INT64
 *   t/T/true/TRUE/f/F/false/FALSE -> INT64 (bool: 1 or 0)
 *   42.0 / 42   -> FLOAT64 (plain integer without 'i' is float per LP spec)
 */
#ifndef TSDB_SERVER_INFLUX_LINE_H
#define TSDB_SERVER_INFLUX_LINE_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum tags/fields per line. */
#define TSDB_INFLUX_MAX_TAGS   64
#define TSDB_INFLUX_MAX_FIELDS 64

/* Field value types as returned by parser. */
typedef enum {
    TSDB_INFLUX_FLOAT  = 0,   /* FLOAT64 */
    TSDB_INFLUX_INT    = 1,   /* INT64 */
    TSDB_INFLUX_BOOL   = 2,   /* INT64 (0/1) */
    TSDB_INFLUX_STRING = 3    /* SYMBOL */
} tsdb_influx_ftype_t;

/* A single parsed field value. */
typedef union {
    double   f64;
    int64_t  i64;
} tsdb_influx_val_t;

/*
 * Parse a single line-protocol line.
 *
 * All output string pointers point into `scratch` (caller provides, size >= len+64).
 * Returns TSDB_OK on success, TSDB_ERR_PARSE on malformed input.
 *
 * out_measurement  -> NUL-terminated measurement name (table name)
 * out_tag_keys[]   -> NUL-terminated tag key strings   (up to TSDB_INFLUX_MAX_TAGS)
 * out_tag_vals[]   -> NUL-terminated tag value strings
 * out_ntags        -> number of tags parsed
 * out_field_keys[] -> NUL-terminated field key strings (up to TSDB_INFLUX_MAX_FIELDS)
 * out_field_vals[] -> parsed values (union by type)
 * out_field_types[]-> tsdb_influx_ftype_t for each field
 * out_field_strs[] -> string value pointer (only valid for TSDB_INFLUX_STRING)
 * out_nfields      -> number of fields parsed
 * out_ts           -> nanosecond timestamp (server time if absent in line)
 */
int tsdb_influx_line_parse(const char *line, size_t len,
                            char *scratch, size_t scap,
                            char **out_measurement,
                            char **out_tag_keys,   char **out_tag_vals,   int *out_ntags,
                            char **out_field_keys,
                            tsdb_influx_val_t *out_field_vals,
                            tsdb_influx_ftype_t *out_field_types,
                            char **out_field_strs,
                            int *out_nfields,
                            tsdb_ts_t *out_ts);

/*
 * Ingest a batch of Line Protocol lines into db.
 *
 * body     - raw LP text (may contain multiple newline-separated lines)
 * n        - byte length of body
 * out_lines  - total lines processed (including errors)
 * out_errors - lines skipped due to parse/schema errors
 *
 * Returns TSDB_OK always (individual line errors are counted, not fatal).
 * Tables are created automatically on first encounter.
 */
int tsdb_influx_ingest(tsdb_db_t *db, const char *body, size_t n,
                        size_t *out_lines, size_t *out_errors);

/*
 * Start a minimal HTTP server for the InfluxDB write endpoint.
 *
 * bind_addr  - "host:port", e.g. "0.0.0.0:28091"
 * db         - shared database (must remain valid for server lifetime)
 *
 * Returns 0 on success (spawns background accept thread).
 * Returns -1 on bind/listen error.
 *
 * Stop with tsdb_influx_http_stop().
 */
int tsdb_influx_http_start(const char *bind_addr, tsdb_db_t *db);
void tsdb_influx_http_stop(void);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_SERVER_INFLUX_LINE_H */
