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

/* ---- Optional trailing SYMBOL dictionary ---------------------------------
 *
 * A SYMBOL cell holds a uint32 code that only means anything next to the
 * symbol table that minted it, and the original wire format shipped the
 * codes without the table.  fedagg_result_alloc leaves col_symtab NULL, so
 * tsdb_result_sym() on a decoded result returned NULL for EVERY symbol
 * column — the numbers arrived, the strings did not.  Nothing noticed
 * because the only production consumer (the stable-aggregate scatter)
 * ships pure numeric partials.
 *
 * The dictionary is appended AFTER the column data and only when at least
 * one column is TSDB_TYPE_SYMBOL and carries a symtab:
 *
 *   u32   FED_SYMDICT_MAGIC
 *   per column, in column order:
 *     u8  present            (0 = not a symbol column, or no symtab)
 *     if present:
 *       u32 nentries         (distinct codes used by rows [0,nrows), ascending)
 *       nentries x { u32 code, u16 len, len bytes }   (no NUL)
 *
 * Additive by construction: a numeric-only result encodes byte-for-byte as
 * before, and a decoder that stops after the column data ignores the tail.
 * Fields use native-endian memcpy, matching the nrows/schema encoding
 * already in this file. */
#define FED_SYMDICT_MAGIC 0x314D5953u  /* 'S','Y','M','1' */

/* ---- Encode result -------------------------------------------------------- */

/* Distinct sort key helper for the dictionary builder. */
static int fed_cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/* 1 when r has at least one SYMBOL column with a symbol table behind it. */
static int fed_has_symbol_dict(const tsdb_result_t *r) {
    if (!r->col_types || !r->col_symtab) return 0;
    for (int c = 0; c < r->ncols; c++) {
        if (r->col_types[c] == TSDB_TYPE_SYMBOL && r->col_symtab[c]) return 1;
    }
    return 0;
}

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

    /* Trailing symbol dictionary — see FED_SYMDICT_MAGIC above. */
    if (fed_has_symbol_dict(r)) {
        uint32_t magic = FED_SYMDICT_MAGIC;
        NEED(4);
        memcpy(p, &magic, 4); p += 4;

        for (int c = 0; c < ncols; c++) {
            tsdb_symtab_t *st = (r->col_types[c] == TSDB_TYPE_SYMBOL)
                                ? r->col_symtab[c] : NULL;
            NEED(1);
            if (!st || !r->col_data || !r->col_data[c]) { *p++ = 0; continue; }
            *p++ = 1;

            /* Distinct codes actually used by rows [0,nrows). */
            uint64_t *codes = NULL;
            uint32_t  nuniq = 0;
            if (nrows > 0) {
                codes = (uint64_t *)malloc((size_t)nrows * 8);
                if (!codes) return -1;
                memcpy(codes, r->col_data[c], (size_t)nrows * 8);
                qsort(codes, nrows, 8, fed_cmp_u64);
                for (uint32_t i = 0; i < nrows; i++) {
                    if (i == 0 || codes[i] != codes[nuniq - 1]) codes[nuniq++] = codes[i];
                }
            }
            if (p + 4 > end) { free(codes); return -1; }
            memcpy(p, &nuniq, 4); p += 4;

            for (uint32_t i = 0; i < nuniq; i++) {
                uint32_t code = (uint32_t)codes[i];
                const char *s = tsdb_symtab_str(st, code);
                size_t slen = s ? strlen(s) : 0;
                if (slen > 65535) slen = 65535;
                uint16_t l16 = (uint16_t)slen;
                if (p + 4 + 2 + slen > end) { free(codes); return -1; }
                memcpy(p, &code, 4); p += 4;
                memcpy(p, &l16, 2);  p += 2;
                if (slen) { memcpy(p, s, slen); p += slen; }
            }
            free(codes);
        }
    }

#undef NEED
    return (int)(p - buf);
}

/* ---- Decode result -------------------------------------------------------- */

/* Hand a freshly created symtab to the result so tsdb_result_free()/
 * fedagg_result_free() release it with the rest of the result. */
static int fed_result_own_symtab(tsdb_result_t *r, tsdb_symtab_t *st) {
    tsdb_symtab_t **np = (tsdb_symtab_t **)realloc(
        r->owned_symtabs, (size_t)(r->n_owned_symtabs + 1) * sizeof(*np));
    if (!np) return -1;
    r->owned_symtabs = np;
    r->owned_symtabs[r->n_owned_symtabs++] = st;
    return 0;
}

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

    /* Trailing symbol dictionary, when the sender emitted one.  Absent for
     * numeric-only results and for peers built before the dictionary
     * existed: those keep the historical behaviour (col_symtab stays NULL,
     * tsdb_result_sym returns NULL) rather than failing the decode. */
    if (p + 4 <= end) {
        uint32_t magic;
        memcpy(&magic, p, 4);
        if (magic == FED_SYMDICT_MAGIC) {
            p += 4;
            for (int c = 0; c < ncols; c++) {
                if (p + 1 > end) { fedagg_result_free(r); return TSDB_ERR_CORRUPT; }
                uint8_t present = *p++;
                if (!present) continue;
                if (p + 4 > end) { fedagg_result_free(r); return TSDB_ERR_CORRUPT; }
                uint32_t nent;
                memcpy(&nent, p, 4); p += 4;
                /* Distinct codes cannot outnumber the rows that use them.
                 * Without this an entry count read off a damaged frame sizes
                 * two allocations directly. */
                if (nent > nrows) { fedagg_result_free(r); return TSDB_ERR_CORRUPT; }

                tsdb_symtab_t *st = NULL;
                if (tsdb_symtab_new(&st) != TSDB_OK || !st) {
                    fedagg_result_free(r); return TSDB_ERR_NOMEM;
                }
                if (fed_result_own_symtab(r, st) != 0) {
                    tsdb_symtab_free(st);
                    fedagg_result_free(r); return TSDB_ERR_NOMEM;
                }
                r->col_symtab[c] = st;

                /* wire code -> local code, ascending in wire code so the
                 * per-row remap below can binary-search it. */
                uint32_t *wire = NULL, *local = NULL;
                if (nent > 0) {
                    wire  = (uint32_t *)malloc((size_t)nent * 4);
                    local = (uint32_t *)malloc((size_t)nent * 4);
                    if (!wire || !local) {
                        free(wire); free(local);
                        fedagg_result_free(r); return TSDB_ERR_NOMEM;
                    }
                }
                for (uint32_t i = 0; i < nent; i++) {
                    if (p + 6 > end) {
                        free(wire); free(local);
                        fedagg_result_free(r); return TSDB_ERR_CORRUPT;
                    }
                    uint32_t code; uint16_t slen;
                    memcpy(&code, p, 4); p += 4;
                    memcpy(&slen, p, 2); p += 2;
                    if (p + slen > end) {
                        free(wire); free(local);
                        fedagg_result_free(r); return TSDB_ERR_CORRUPT;
                    }
                    uint32_t lc = tsdb_symtab_intern_n(st, (const char *)p, slen);
                    p += slen;
                    if (lc == TSDB_SYMBOL_INVALID) {
                        free(wire); free(local);
                        fedagg_result_free(r); return TSDB_ERR_NOMEM;
                    }
                    if (i > 0 && code <= wire[i - 1]) {
                        /* Sender emits ascending distinct codes; anything
                         * else would break the binary search below. */
                        free(wire); free(local);
                        fedagg_result_free(r); return TSDB_ERR_CORRUPT;
                    }
                    wire[i] = code; local[i] = lc;
                }

                for (uint32_t row = 0; row < nrows; row++) {
                    uint32_t code = (uint32_t)((uint64_t *)r->col_data[c])[row];
                    uint32_t lo = 0, hi = nent, hit = TSDB_SYMBOL_INVALID;
                    while (lo < hi) {
                        uint32_t mid = lo + (hi - lo) / 2;
                        if (wire[mid] == code) { hit = local[mid]; break; }
                        if (wire[mid] < code) lo = mid + 1; else hi = mid;
                    }
                    if (hit == TSDB_SYMBOL_INVALID) {
                        free(wire); free(local);
                        fedagg_result_free(r); return TSDB_ERR_CORRUPT;
                    }
                    ((uint64_t *)r->col_data[c])[row] = hit;
                }
                free(wire); free(local);
            }
        }
    }

#undef NEED
    *out = r;
    return TSDB_OK;
}

/* ---- Send query, receive encoded result ---------------------------------- */

/* Shared core for FED_QUERY / FED_QUERY_LOCAL: identical payload and
 * result wire format, only the RPC type (and timeout enforcement)
 * differs.  hard_timeout != 0 arms a per-call socket deadline via
 * tsdb_rpc_call_recv_to; 0 keeps the historical blocking behaviour of
 * fedrpc_query (its timeout_ms was never wired to the socket). */
static int fedrpc_query_type(tsdb_rpc_conn_t *conn, int rpc_type,
                             const char *qtl, int timeout_ms,
                             int hard_timeout, tsdb_result_t **out)
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
    int rc = hard_timeout
        ? tsdb_rpc_call_recv_to(conn, (tsdb_rpc_type_t)rpc_type,
                                payload, (uint32_t)payload_len,
                                resp, (uint32_t)FED_MAX_RESULT_BUF,
                                &resp_len, timeout_ms)
        : tsdb_rpc_call_recv(conn, (tsdb_rpc_type_t)rpc_type,
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

int fedrpc_query(tsdb_rpc_conn_t *conn, const char *qtl,
                 int timeout_ms, tsdb_result_t **out)
{
    return fedrpc_query_type(conn, TSDB_RPC_FED_QUERY, qtl, timeout_ms,
                             0 /* keep historical blocking semantics */, out);
}

int fedrpc_query_local(tsdb_rpc_conn_t *conn, const char *qtl,
                       int timeout_ms, tsdb_result_t **out)
{
    return fedrpc_query_type(conn, TSDB_RPC_FED_QUERY_LOCAL, qtl, timeout_ms,
                             1 /* scatter leg: bounded wait */, out);
}
