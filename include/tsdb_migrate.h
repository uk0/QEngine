/* tsdb_migrate.h — low-level cross-cluster bulk data movement.
 *
 * Streams a table's COMPRESSED column blocks between clusters without ever
 * decoding them: export reads each block's compressed payload straight out of
 * the partition's .col file, import writes it back verbatim.  Nothing is
 * decompressed and nothing is re-encoded, so a migration costs roughly a file
 * copy plus framing rather than a full decode/re-encode of every value.
 *
 * The stream is self-describing — it carries the schema, so the target can
 * create the table itself — and it is a plain byte stream, so it works over a
 * file, a pipe, a socket or ssh:
 *
 *     tsdb_migrate_export(src_db, "cpu", out_fd, NULL, &st);
 *     tsdb_migrate_import(dst_db, in_fd,  NULL, &st);
 *
 * IDEMPOTENCE.  Import lands blocks through the same path replication uses,
 * which skips a block whose (ts_min, count) already matches the tail of the
 * target's index.  Re-running an interrupted migration therefore resumes
 * rather than duplicating, and that is also what makes the resume cursor
 * safe: it is an optimisation, not a correctness requirement.
 *
 * WHAT THIS IS NOT.  Phase 1 is single-table and offline with respect to the
 * source partitions it is reading: blocks appended to the source while an
 * export runs are simply not in that stream (run it again to pick them up).
 * It does not move the WAL, so un-flushed rows are not included — flush first
 * if you need them.
 */
#ifndef TSDB_MIGRATE_H
#define TSDB_MIGRATE_H

#include "tsdb.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_MIG_MAGIC    0x314D5354u   /* "TSM1" */
/* v2 adds the SYMBOL dictionary section.  v1 streams are rejected: they carry
 * dictionary CODES with no dictionary, so a SYMBOL column imported from one
 * decodes against the target's own (empty) table and every tag reads back
 * wrong — measured as 0 rows for every tag value. */
#define TSDB_MIG_VERSION  2u

/* What to do when the target already holds the table. */
typedef enum {
    /* Create it if absent; if present, require the schema to match and let the
     * idempotent block landing skip whatever is already there.  Default. */
    TSDB_MIG_CREATE_OR_RESUME = 0,
    /* Refuse if the table already exists on the target. */
    TSDB_MIG_CREATE_ONLY      = 1
} tsdb_mig_land_t;

typedef struct {
    tsdb_mig_land_t land;
    /* Rename the table on the way in; NULL/"" keeps the source name. */
    const char     *rename_to;
} tsdb_mig_opts_t;

typedef struct {
    uint64_t partitions;
    uint64_t blocks;       /* block records written / read                   */
    uint64_t blocks_landed;/* import only: records that actually wrote       */
    uint64_t rows;         /* summed block row counts                        */
    uint64_t payload_bytes;/* compressed block bytes, excluding framing      */
    uint64_t digest;       /* order-independent checksum, see below          */
} tsdb_mig_stats_t;

/*
 * Export `table` from `db` as a migration stream written to `fd`.
 *
 * `opts` is accepted for symmetry and may be NULL (there is nothing to
 * configure on the export side in this version).  `out` may be NULL.
 *
 * Returns TSDB_OK, TSDB_ERR_NOTFOUND if the table does not exist ON DISK —
 * whether this process has opened it yet is irrelevant — or an IO/NOMEM
 * error.  A short write is an error; the stream is not resumable mid-record,
 * only mid-migration.
 */
int tsdb_migrate_export(tsdb_db_t *db, const char *table, int fd,
                        const tsdb_mig_opts_t *opts, tsdb_mig_stats_t *out);

/*
 * Import a migration stream from `fd` into `db`, creating the table from the
 * schema carried in the stream when needed.  `opts` may be NULL for defaults.
 * `out` may be NULL.
 *
 * "The target already holds the table" means it exists ON DISK, not that this
 * process has it open.
 *
 * Returns TSDB_OK, TSDB_ERR_CORRUPT on a malformed/truncated stream,
 * TSDB_ERR_SCHEMA when the target's existing schema disagrees with the
 * stream's — or when a SYMBOL column's dictionary does (the codes in the
 * blocks would then name different strings; an identical dictionary, which is
 * what a resumed migration finds, is fine) — TSDB_ERR_EXISTS under
 * TSDB_MIG_CREATE_ONLY, or an IO/NOMEM error.
 */
int tsdb_migrate_import(tsdb_db_t *db, int fd,
                        const tsdb_mig_opts_t *opts, tsdb_mig_stats_t *out);

/*
 * Digest of a table's on-disk blocks, for checking a migration landed.
 *
 * Folds (partition, column, ts_min, ts_max, count) of every block, order
 * independently, so source and target agree even though their blocks may sit
 * at different file offsets.  It deliberately does NOT hash the compressed
 * bytes: a target that re-compacted is still correct, and this stays cheap
 * (index-only, no .col reads).  Equal digests plus equal row counts is the
 * check `tsdb_migrate_export`/`import` report in tsdb_mig_stats_t.digest.
 */
int tsdb_migrate_digest(tsdb_db_t *db, const char *table,
                        tsdb_mig_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_MIGRATE_H */
