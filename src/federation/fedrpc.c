/* fedrpc.c — federation RPC: send/receive query results over the cluster wire.
 *
 * Uses tsdb_rpc_call_recv() from cluster/rpc.c with type TSDB_RPC_FED_QUERY.
 * The server side (adding a handler in rpc.c) is done by extending the
 * cluster's connection_handler to recognise type 8.
 */

#include "fedrpc.h"
#include "fedagg.h"
#include "../query/result_internal.h"
#include "../../include/tsdb.h"
#include "../cluster/rpc.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/socket.h>
#include <poll.h>
#include <errno.h>

#define FED_DEFAULT_TIMEOUT_MS 30000
#define FED_MAX_RESULT_BUF     (64 * 1024 * 1024)  /* 64 MB */
#define FED_MAX_COLS           128

/* ---- Encode result -------------------------------------------------------- */

int fedrpc_encode_result(uint8_t *buf, uint32_t cap, tsdb_result_t *r) {
    if (!buf || !r) return -1;

    uint8_t *p   = buf;
    uint8_t *end = buf + cap;

#define NEED(n) if (p + (n) > end) return -1

    /* ncols u8 */
    NEED(1);
    *p++ = (uint8_t)(r->ncols < 255 ? r->ncols : 255);
    int ncols = (r->ncols < 255) ? r->ncols : 255;

    /* column schema */
    for (int c = 0; c < ncols; c++) {
        const char *name = r->col_names ? r->col_names[c] : "";
        uint8_t nlen = (uint8_t)(name ? strlen(name) : 0);
        NEED(1 + nlen + 1);
        *p++ = nlen;
        if (nlen > 0) { memcpy(p, name, nlen); p += nlen; }
        *p++ = (uint8_t)(r->col_types ? r->col_types[c] : TSDB_TYPE_INT64);
    }

    /* nrows u32 */
    uint32_t nrows = (uint32_t)(r->nrows);
    NEED(4);
    memcpy(p, &nrows, 4); p += 4;

    /* column-major data: each col is nrows * 8 bytes */
    for (int c = 0; c < ncols; c++) {
        size_t sz = (size_t)nrows * 8;
        NEED((int)sz);
        if (r->col_data && r->col_data[c]) {
            memcpy(p, r->col_data[c], sz);
        } else {
            memset(p, 0, sz);
        }
        p += sz;
    }

#undef NEED
    return (int)(p - buf);
}

/* ---- Decode result -------------------------------------------------------- */

int fedrpc_decode_result(const uint8_t *buf, uint32_t len, tsdb_result_t **out) {
    if (!buf || !out) return TSDB_ERR_INVAL;

    const uint8_t *p   = buf;
    const uint8_t *end = buf + len;

#define NEED(n) if (p + (n) > end) return TSDB_ERR_CORRUPT

    NEED(1);
    int ncols = (int)*p++;

    char      col_names_buf[FED_MAX_COLS][128];
    tsdb_type_t col_types[FED_MAX_COLS];
    if (ncols > FED_MAX_COLS) return TSDB_ERR_CORRUPT;

    for (int c = 0; c < ncols; c++) {
        NEED(1);
        uint8_t nlen = *p++;
        NEED(nlen + 1);
        int cn = nlen < 127 ? nlen : 127;
        memcpy(col_names_buf[c], p, cn);
        col_names_buf[c][cn] = '\0';
        p += nlen;
        col_types[c] = (tsdb_type_t)*p++;
    }

    NEED(4);
    uint32_t nrows;
    memcpy(&nrows, p, 4); p += 4;

    const char *names[FED_MAX_COLS];
    for (int c = 0; c < ncols; c++) names[c] = col_names_buf[c];

    tsdb_result_t *r = fedagg_result_alloc(ncols, names, col_types);
    if (!r) return TSDB_ERR_NOMEM;

    /* Read column data. */
    for (int c = 0; c < ncols; c++) {
        size_t sz = (size_t)nrows * 8;
        NEED((int)sz);
        /* Ensure capacity. */
        if (r->cap_rows < nrows) {
            void *np = realloc(r->col_data[c], nrows * 8);
            if (!np) { fedagg_result_free(r); return TSDB_ERR_NOMEM; }
            r->col_data[c] = np;
        }
        memcpy(r->col_data[c], p, sz);
        p += sz;
    }
    r->nrows    = nrows;
    r->cap_rows = nrows;
    r->cur      = -1;

#undef NEED
    *out = r;
    return TSDB_OK;
}

/* ---- Send query, receive encoded result ---------------------------------- */

int fedrpc_query(tsdb_rpc_conn_t *conn, const char *qtl,
                 int timeout_ms, tsdb_result_t **out)
{
    if (!conn || !qtl || !out) return TSDB_ERR_INVAL;
    if (timeout_ms <= 0) timeout_ms = FED_DEFAULT_TIMEOUT_MS;

    /* Build payload: u16 len + qtl string. */
    size_t qlen = strlen(qtl);
    if (qlen > 65535) return TSDB_ERR_INVAL;

    size_t payload_len = 2 + qlen;
    uint8_t *payload = malloc(payload_len);
    if (!payload) return TSDB_ERR_NOMEM;

    uint16_t qlen16 = (uint16_t)qlen;
    memcpy(payload, &qlen16, 2);
    memcpy(payload + 2, qtl, qlen);

    /* Allocate response buffer dynamically. */
    uint8_t *resp = malloc(FED_MAX_RESULT_BUF);
    if (!resp) { free(payload); return TSDB_ERR_NOMEM; }

    uint32_t resp_len = 0;
    int rc = tsdb_rpc_call_recv(conn,
                                TSDB_RPC_FED_QUERY,
                                payload, (uint32_t)payload_len,
                                resp, (uint32_t)FED_MAX_RESULT_BUF,
                                &resp_len);
    free(payload);

    if (rc != TSDB_OK) {
        free(resp);
        return rc;
    }

    /* Decode result. */
    rc = fedrpc_decode_result(resp, resp_len, out);
    free(resp);
    return rc;
}
