/* fedrpc.h — federation RPC client.
 *
 * Extends the cluster RPC with a FEDERATION_QUERY call type.
 * Re-uses the same wire format as cluster/rpc.h but with rpc_id = 8.
 *
 * Query payload (request):
 *   qtl_len  u16
 *   qtl      [qtl_len bytes]  (null-terminated QTL string)
 *
 * Query payload (response = ACK with body):
 *   ncols    u8
 *   per-col:
 *     name_len u8
 *     name     [name_len]
 *     type     u8            (tsdb_type_t)
 *   nrows    u32
 *   column-major data: for each col, nrows × 8-byte little-endian values
 *                      (timestamps/int64/float64 all 8 bytes; SYMBOL = 4 bytes
 *                       upcast to 8 — but SYMBOL not used in federation tests)
 *
 * Error payload (ERR type): 4-byte int32_t error code.
 */
#ifndef TSDB_FEDRPC_H
#define TSDB_FEDRPC_H

#include "../../include/tsdb.h"
#include "../cluster/rpc.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* TSDB_RPC_FED_QUERY = 8 is now defined in cluster/rpc.h */

/*
 * Send a federation query to a single cluster coordinator.
 * conn     — open RPC connection to coordinator.
 * qtl      — query string.
 * timeout_ms — 0 = use default (30 000 ms).
 * out      — on success, caller-owned result (free with tsdb_result_free).
 *
 * Returns TSDB_OK or negative error code.
 * TSDB_ERR_IO = timeout / network failure.
 */
int fedrpc_query(tsdb_rpc_conn_t *conn,
                 const char *qtl,
                 int timeout_ms,
                 tsdb_result_t **out);

/*
 * Encode a federation query result into buf.
 * Returns bytes written or -1 if buffer too small.
 */
int fedrpc_encode_result(uint8_t *buf, uint32_t cap,
                         tsdb_result_t *r);

/*
 * Decode a federation query result from buf.
 * Returns TSDB_OK on success; caller frees *out with fedagg_result_free().
 */
int fedrpc_decode_result(const uint8_t *buf, uint32_t len,
                         tsdb_result_t **out);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_FEDRPC_H */
