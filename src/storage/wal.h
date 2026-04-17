/* wal.h — Write-Ahead Log.
 *
 * Each record is: [crc32 u32][len u32][payload u8[len]]
 * File path: <db_dir>/wal/<table_name>.log
 * fsync is explicit via tsdb_wal_sync().
 */
#ifndef TSDB_STORAGE_WAL_H
#define TSDB_STORAGE_WAL_H

#include "../../include/tsdb.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_wal tsdb_wal_t;

/*
 * Open (or create) a WAL file for the given table.
 * db_dir/wal/<table_name>.log is created if absent.
 */
int  tsdb_wal_open(const char *db_dir, const char *table_name, tsdb_wal_t **out);

/* Close the WAL file. Does not sync or truncate. */
void tsdb_wal_close(tsdb_wal_t *w);

/*
 * Append a record to the WAL.
 * rec/n is the raw payload. CRC is computed internally.
 */
int  tsdb_wal_append(tsdb_wal_t *w, const void *rec, size_t n);

/* fsync the WAL file to durable storage. */
int  tsdb_wal_sync(tsdb_wal_t *w);

/* Truncate the WAL file to zero (called after successful memtable flush). */
int  tsdb_wal_truncate(tsdb_wal_t *w);

/*
 * Replay all records in the WAL file.
 * For each record the callback is invoked with the payload.
 * Returns TSDB_OK, or TSDB_ERR_CORRUPT if a record has bad CRC, or
 * any non-zero value returned by the callback.
 */
int tsdb_wal_replay(const char *db_dir, const char *table_name,
                    int (*cb)(const void *rec, size_t n, void *ctx), void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_WAL_H */
