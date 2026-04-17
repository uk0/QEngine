/* tsdb.h — public API for tsdb
 *
 * Single-machine, C11, column-oriented time-series database.
 * API is stable across minor versions; ABI is not.
 */
#ifndef TSDB_H
#define TSDB_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_VERSION_MAJOR 0
#define TSDB_VERSION_MINOR 1
#define TSDB_VERSION_PATCH 0

/* Error codes. 0 == success. Negative on error. */
typedef enum {
    TSDB_OK              =  0,
    TSDB_ERR_INVAL       = -1,
    TSDB_ERR_NOMEM       = -2,
    TSDB_ERR_IO          = -3,
    TSDB_ERR_CORRUPT     = -4,
    TSDB_ERR_NOTFOUND    = -5,
    TSDB_ERR_EXISTS      = -6,
    TSDB_ERR_FULL        = -7,
    TSDB_ERR_OVERFLOW    = -8,
    TSDB_ERR_UNSUPPORTED = -9,
    TSDB_ERR_PARSE       = -10,
    TSDB_ERR_SCHEMA      = -11,
    TSDB_ERR_INTERNAL    = -12
} tsdb_err_t;

/* Column types */
typedef enum {
    TSDB_TYPE_TIMESTAMP = 1, /* int64 nanoseconds since epoch */
    TSDB_TYPE_INT64     = 2,
    TSDB_TYPE_FLOAT64   = 3,
    TSDB_TYPE_SYMBOL    = 4  /* dictionary-encoded string */
} tsdb_type_t;

/* Timestamps: nanoseconds since Unix epoch. */
typedef int64_t tsdb_ts_t;

#define TSDB_TS_MIN ((tsdb_ts_t)INT64_MIN + 1)
#define TSDB_TS_MAX ((tsdb_ts_t)INT64_MAX)

/* Opaque handle types */
typedef struct tsdb_db       tsdb_db_t;
typedef struct tsdb_table    tsdb_table_t;
typedef struct tsdb_batch    tsdb_batch_t;
typedef struct tsdb_result   tsdb_result_t;

/* Column schema entry */
typedef struct {
    const char  *name;
    tsdb_type_t  type;
} tsdb_col_t;

/* Database open / close.
 * data_dir: root directory; created if missing.
 */
int tsdb_open(const char *data_dir, tsdb_db_t **out);
void tsdb_close(tsdb_db_t *db);

/* Partition granularity chosen at CREATE TABLE time.
 * Fixed for the lifetime of the table. DAY is the default.
 * HOUR improves block-skip selectivity 24x for queries that hit a
 * narrow time range, at the cost of more subdirectories on disk. */
typedef enum {
    TSDB_CREATE_PART_DAY  = 0,
    TSDB_CREATE_PART_HOUR = 1
} tsdb_create_partition_t;

/* Table lifecycle */
int tsdb_create_table(tsdb_db_t *db,
                      const char *name,
                      const tsdb_col_t *cols, size_t ncols,
                      const char *ts_col);

/* Extended variant with partition granularity option. */
int tsdb_create_table_ex(tsdb_db_t *db,
                         const char *name,
                         const tsdb_col_t *cols, size_t ncols,
                         const char *ts_col,
                         tsdb_create_partition_t partition);

int tsdb_open_table(tsdb_db_t *db, const char *name, tsdb_table_t **out);
int tsdb_drop_table(tsdb_db_t *db, const char *name);

/* Batch insert
 * Typical pattern:
 *   tsdb_batch_begin(tbl, &b);
 *   tsdb_batch_row_ts(b, ts);
 *   tsdb_batch_row_i64(b, col_idx, v);
 *   tsdb_batch_row_f64(b, col_idx, v);
 *   tsdb_batch_row_sym(b, col_idx, s);
 *   tsdb_batch_row_end(b);
 *   ... more rows ...
 *   tsdb_batch_commit(b);
 */
int  tsdb_batch_begin(tsdb_table_t *tbl, tsdb_batch_t **out);
int  tsdb_batch_row_ts(tsdb_batch_t *b, tsdb_ts_t ts);
int  tsdb_batch_row_i64(tsdb_batch_t *b, int col_idx, int64_t v);
int  tsdb_batch_row_f64(tsdb_batch_t *b, int col_idx, double v);
int  tsdb_batch_row_sym(tsdb_batch_t *b, int col_idx, const char *s);
int  tsdb_batch_row_end(tsdb_batch_t *b);
int  tsdb_batch_commit(tsdb_batch_t *b);
void tsdb_batch_discard(tsdb_batch_t *b);

/* Query: QTL text → result iterator. */
int tsdb_query(tsdb_db_t *db, const char *qtl, tsdb_result_t **out);
void tsdb_result_free(tsdb_result_t *r);

/* Result row iteration. Returns 1 if a row was read, 0 at end, < 0 on error. */
int tsdb_result_ncols(tsdb_result_t *r);
const char  *tsdb_result_col_name(tsdb_result_t *r, int i);
tsdb_type_t  tsdb_result_col_type(tsdb_result_t *r, int i);
int          tsdb_result_next(tsdb_result_t *r);
tsdb_ts_t    tsdb_result_ts(tsdb_result_t *r, int col);
int64_t      tsdb_result_i64(tsdb_result_t *r, int col);
double       tsdb_result_f64(tsdb_result_t *r, int col);
const char  *tsdb_result_sym(tsdb_result_t *r, int col);
bool         tsdb_result_is_null(tsdb_result_t *r, int col);

/* Utilities */
const char *tsdb_errstr(int err);
const char *tsdb_version(void);
tsdb_ts_t   tsdb_now_ns(void);
tsdb_ts_t   tsdb_parse_ts(const char *s); /* ISO-8601 or "YYYY-MM-DD HH:MM:SS.nnn" */

#ifdef __cplusplus
}
#endif

#endif /* TSDB_H */
