/* audit.h — Append-only audit log for security-relevant operations.
 *
 * Records every privileged event so an operator can reconstruct
 * after-the-fact "who did what when":
 *   - AUTH login / login failure
 *   - GRANT / REVOKE
 *   - DDL (CREATE / DROP / ALTER on database / group / table / user / fn)
 *   - BACKUP / RETENTION SWEEP triggers
 *
 * Format: one JSON object per line (JSONL) at
 *   <data_dir>/catalog/audit.log
 *
 * Each record looks like:
 *   {"ts":"2026-04-23T03:30:00.123Z","user":"alice",
 *    "event":"DDL","action":"CREATE TABLE","object":"ops",
 *    "result":0,"detail":""}
 *
 * The log is only ever appended to — compaction / rotation is the
 * operator's responsibility via log rotate or logrotate(8).
 *
 * All public functions are thread-safe.  On open failure the module
 * degrades to a silent no-op (log file cannot be created) so the
 * running server never refuses to start because of audit I/O errors.
 */
#ifndef TSDB_CATALOG_AUDIT_H
#define TSDB_CATALOG_AUDIT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_audit tsdb_audit_t;

/* Open (or create) the audit log under <data_dir>/catalog/audit.log.
 * Returns 0 on success, negative on I/O failure.  *out is non-NULL
 * on success. */
int  tsdb_audit_open(const char *data_dir, tsdb_audit_t **out);

/* Close and flush.  Safe with NULL. */
void tsdb_audit_close(tsdb_audit_t *a);

/* Write one audit record.  Any field may be NULL → emitted as "".
 *   event  : short category tag ("AUTH" | "DDL" | "GRANT" | "REVOKE" | ...)
 *   action : specific operation ("LOGIN" | "CREATE TABLE" | ...)
 *   object : target identifier (user / table / database name)
 *   result : 0 for success, negative tsdb error code on failure
 *   detail : optional free-form explanation (remote IP, error text, ...)
 *
 * Returns 0 on success; negative if the log cannot be written.  A
 * single record is never truncated: too-long values are capped at
 * TSDB_AUDIT_FIELD_MAX bytes each. */
int  tsdb_audit_write(tsdb_audit_t *a,
                      const char  *user,
                      const char  *event,
                      const char  *action,
                      const char  *object,
                      int          result,
                      const char  *detail);

/* Tail the last `max_rows` entries into buf (newline-separated JSONL).
 * Returns bytes written on success, 0 if log is empty, negative on I/O.
 * NOTE: this is an O(file) scan — fine for a dashboard tail of a
 * few hundred lines, too slow for gigabyte-sized logs. */
int  tsdb_audit_tail(tsdb_audit_t *a, int max_rows,
                     char *buf, size_t cap);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CATALOG_AUDIT_H */
