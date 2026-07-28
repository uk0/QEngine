/* migrate_internal.h — pieces of the migration stream shared with restore.c.
 *
 * The backup/restore path reads the SAME stream format the migration SDK
 * writes (see migrate.c's header comment for the layout), so the framing
 * constants and the dictionary reconciliation live here rather than being
 * copied — a second, subtly-different copy of the "do these two dictionaries
 * agree" rule is exactly how a SYMBOL column ends up decoding against the
 * wrong table and reading back as zero rows.
 */
#ifndef TSDB_STORAGE_MIGRATE_INTERNAL_H
#define TSDB_STORAGE_MIGRATE_INTERNAL_H

#include "schema.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Fixed-width name field in the stream header (table name and column names). */
#define TSDB_MIG_NAME_BYTES 64

/*
 * Merge a streamed .sym dictionary into column `ci`'s LIVE symtab.
 *
 * Codes are assigned densely from 0 in intern order, so two dictionaries agree
 * exactly when one is a prefix of the other: the shared prefix must match
 * string for string, the tail is interned (intern appends, so string N lands
 * at code N and the prefix property is preserved).  One disagreeing code means
 * the dictionaries were built independently — refuse, because landing blocks
 * that carry the other side's codes would silently retag every row.
 *
 * Interns in place so a reader that snapshotted schema->cols[ci].symtab keeps
 * a live pointer.  Persists the merged dictionary to <dir>/<col>.sym.
 *
 * Returns TSDB_OK, TSDB_ERR_CORRUPT for a malformed dictionary, or
 * TSDB_ERR_SCHEMA when the two dictionaries disagree.
 */
int tsdb_migrate_symtab_adopt(tsdb_schema_t *s, int ci,
                              const uint8_t *bytes, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_MIGRATE_INTERNAL_H */
