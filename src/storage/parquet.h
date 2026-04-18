/* parquet.h — minimal Apache Parquet v1 exporter (MVP).
 *
 * Scope (strict MVP, intentionally narrow):
 *   - One file per storage partition.
 *   - Single row group per file; single data page per column.
 *   - Encoding: PLAIN only. No dictionary, no RLE-dict, no DELTA.
 *   - Compression: UNCOMPRESSED only.
 *   - Physical types:
 *       TSDB_TYPE_TIMESTAMP  -> Parquet INT64        (nanoseconds, no logical type)
 *       TSDB_TYPE_INT64      -> Parquet INT64
 *       TSDB_TYPE_FLOAT64    -> Parquet DOUBLE
 *       TSDB_TYPE_SYMBOL     -> Parquet BYTE_ARRAY   (UTF-8, converted_type=UTF8)
 *   - All columns are REQUIRED (repetition_type = 0); no definition/repetition
 *     level bytes are written in data pages.
 *
 * The footer's FileMetaData is emitted using hand-rolled Thrift-compact
 * protocol encoding; no external Thrift dependency.
 *
 * File structure:
 *     "PAR1"  (magic)
 *     for each column:
 *         [PageHeader thrift-compact][PLAIN-encoded values]
 *     FileMetaData (thrift-compact)
 *     u32 LE FileMetaData length
 *     "PAR1"  (magic)
 */
#ifndef TSDB_STORAGE_PARQUET_H
#define TSDB_STORAGE_PARQUET_H

#include "part.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Export one partition to a single Parquet file.
 *
 * part     - opened partition handle (see tsdb_part_open)
 * out_path - destination file path (parent directory must exist)
 *
 * A partition with zero rows produces no file and returns TSDB_OK.
 *
 * Returns TSDB_OK, or TSDB_ERR_* on failure.
 */
int tsdb_part_export_parquet(tsdb_part_t *part, const char *out_path);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_PARQUET_H */
