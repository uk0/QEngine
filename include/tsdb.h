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
#define TSDB_VERSION_MINOR 2
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
    TSDB_ERR_INTERNAL    = -12,
    TSDB_ERR_PERMISSION  = -13,
    TSDB_ERR_BUSY        = -14, /* a prior in-flight operation must
                                   commit before this one can start
                                   (e.g. a Raft membership change is
                                   still in the leader's uncommitted
                                   tail — we serialise ADD/REMOVE
                                   MASTER one at a time rather than
                                   run joint consensus) */
    TSDB_ERR_TIMEOUT     = -15, /* per-query deadline elapsed; the
                                   executor aborted at a block boundary
                                   so partial work is discarded.  Set
                                   via tsdb_query_set_deadline() before
                                   the call; server.c populates it from
                                   request_timeout_ns. */
    TSDB_ERR_INDETERMINATE = -16 /* a Raft propose timed out while the
                                    appended entry MAY still be replicated
                                    and committed later (we could not prove
                                    it un-replicated, so we did NOT truncate
                                    it — Leader Append-Only).  The caller
                                    must treat the operation as of-unknown-
                                    outcome and retry idempotently rather
                                    than assume failure. */
} tsdb_err_t;

/* Special positive return from on_replicate hooks: the cluster has
 * accepted the rows on the owner replicas and the local node is NOT
 * an owner — flush_and_clear_ex should drop the local copy instead
 * of persisting to disk.  Phase β.2 sharded-storage signal. */
#define TSDB_SKIP_LOCAL 1

/* Column types */
typedef enum {
    TSDB_TYPE_TIMESTAMP = 1, /* int64 nanoseconds since epoch */
    TSDB_TYPE_INT64     = 2,
    TSDB_TYPE_FLOAT64   = 3,
    TSDB_TYPE_SYMBOL    = 4, /* dictionary-encoded string */
    TSDB_TYPE_FLOAT32   = 5  /* 4-byte on disk, double in memory/query — halves
                              * disk for the float column when 32-bit precision
                              * is enough.  The user opts in per column. */
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

/* Hot-reopen of the in-memory catalog from disk.
 *
 * Used when an external agent (e.g. the Raft InstallSnapshot receive
 * path) has atomically swapped the catalog `.log` files under
 * <data_dir>/catalog/ and the live catalog hashmap needs to re-reflect
 * the new on-disk state without a process restart.
 *
 * Opens a fresh catalog from disk, swaps it into the db under the
 * db-level lock, then closes the old one.  Briefly serialises against
 * other db-level operations but does not abort in-flight queries — a
 * concurrent query whose catalog pointer was already resolved via
 * tsdb_db_catalog() will finish against the old catalog; subsequent
 * lookups see the new state.
 *
 * Returns TSDB_OK on success, TSDB_ERR_IO or TSDB_ERR_NOMEM if the
 * fresh catalog fails to open (in which case the existing in-memory
 * catalog is left untouched). */
int tsdb_db_reload_catalog(tsdb_db_t *db);

/* Set the default block_points that subsequently-created tables will use
 * when the caller does not specify one.  Out-of-range values are silently
 * clamped into [1024, 8192]; 0 resets to the library default (8192).
 * Has no effect on tables already created. */
void tsdb_db_set_default_block_points(tsdb_db_t *db, int block_points);

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

/* Fullest variant: partition granularity + per-table block_points.
 * block_points == 0 resolves to the db's default (tsdb_db_set_default_block_points
 * or the library default TSDB_BLOCK_POINTS).  Values outside [1024, 8192] are
 * silently clamped. */
int tsdb_create_table_ex2(tsdb_db_t *db,
                           const char *name,
                           const tsdb_col_t *cols, size_t ncols,
                           const char *ts_col,
                           tsdb_create_partition_t partition,
                           int block_points);

int tsdb_open_table(tsdb_db_t *db, const char *name, tsdb_table_t **out);
int tsdb_drop_table(tsdb_db_t *db, const char *name);

/*
 * Add a column to an existing table.
 *
 * Semantics:
 *   - In-memory rows are flushed to disk first.
 *   - The new column is appended at the end (highest col index).
 *   - Rows in previously-flushed partitions (no data file for the new
 *     column) read back as zero/0.0/empty-symbol during subsequent queries.
 *   - New inserts after this call MUST provide a value for the new column
 *     via tsdb_batch_row_{i64,f64,sym}.
 *
 * Returns TSDB_OK, TSDB_ERR_NOTFOUND, TSDB_ERR_EXISTS (same-name column),
 * or negative error.
 */
int tsdb_alter_table_add_column(tsdb_db_t *db, const char *table_name,
                                 const char *col_name, tsdb_type_t col_type);

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

/* Authenticated query: parses <qtl>, authorizes it against <token>'s
 * grants/role, then executes.  Differs from tsdb_query in that it ALWAYS
 * enforces RBAC regardless of db->auth_enforce — callers that want bypass
 * behavior should use tsdb_query directly.
 *
 * Privilege mapping:
 *   SELECT                     → TSDB_PRIV_SELECT on FROM table
 *   LIST_GROUPS/DEVICES/FNS    → TSDB_PRIV_SELECT on "*"
 *   CREATE/DROP/ALTER stable   → TSDB_PRIV_DDL    on target (or "*")
 *   CREATE/DROP/LIST USER,
 *   GRANT / REVOKE,
 *   ALTER USER PASSWORD,
 *   CREATE / DROP FUNCTION     → ADMIN role only (grant bitmask insufficient)
 *
 * Returns TSDB_ERR_PERMISSION if the token is unknown or lacks the required
 * privilege; otherwise matches tsdb_query semantics. */
int tsdb_query_auth(tsdb_db_t  *db,
                     const char *token,
                     const char *qtl,
                     tsdb_result_t **out);

void tsdb_result_free(tsdb_result_t *r);

/* Result row iteration. Returns 1 if a row was read, 0 at end, < 0 on error. */
int tsdb_result_ncols(tsdb_result_t *r);
const char  *tsdb_result_col_name(tsdb_result_t *r, int i);
tsdb_type_t  tsdb_result_col_type(tsdb_result_t *r, int i);
/* First column whose name contains `substr` (case-insensitive), or -1.
 * Lets callers read a named column by index regardless of column order. */
int          tsdb_result_col_index_by_name(tsdb_result_t *r, const char *substr);
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

/* ---- RBAC (role-based access control) -----------------------------------
 *
 * User records are persisted in <data_dir>/catalog/users.log.  Passwords are
 * stored as PBKDF2-HMAC-SHA256 hashes (10000 rounds) with per-user 16-byte
 * random salts.
 *
 * Enforcement is opt-in.  The executor does NOT consult auth by default;
 * callers invoke tsdb_auth_check (or tsdb_auth_enforce) explicitly to guard
 * their own entrypoints.  Flip db->auth_enforce via tsdb_auth_set_enforce
 * when auto-consulting via tsdb_auth_enforce is desired.
 */

#define TSDB_PRIV_SELECT 0x01
#define TSDB_PRIV_INSERT 0x02
#define TSDB_PRIV_DDL    0x04
#define TSDB_PRIV_ALL    0x07

/* Authenticate username/password; on success copy a 32-char hex token + NUL
 * into out_token (token_cap must be >= 33).
 *   Returns TSDB_OK or TSDB_ERR_PERMISSION (bad creds / unknown user),
 *   TSDB_ERR_INVAL (bad args). */
int tsdb_auth_authenticate(tsdb_db_t  *db,
                            const char *username,
                            const char *password,
                            char       *out_token,
                            size_t      token_cap);

/* Check whether <token> carries <privilege> on <resource>.  <privilege> is a
 * bitmask; a single call can check multiple bits (all must be granted).
 * <resource> is a table name or "*" meaning any table.
 *   Returns TSDB_OK if granted, TSDB_ERR_PERMISSION otherwise. */
int tsdb_auth_check(tsdb_db_t  *db,
                     const char *token,
                     int         privilege,
                     const char *resource);

/* Toggle the db-level auto-enforce flag consumed by tsdb_auth_enforce.
 * Enforcement into tsdb_query/TCP server is not yet wired — this flag
 * is present so opt-in callers can short-circuit cheaply. */
void tsdb_auth_set_enforce(tsdb_db_t *db, bool enforce);

/* Opt-in enforcement stub: returns TSDB_OK when db->auth_enforce is false;
 * otherwise delegates to tsdb_auth_check. */
int tsdb_auth_enforce(tsdb_db_t  *db,
                       const char *token,
                       int         privilege,
                       const char *resource);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_H */
