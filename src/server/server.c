/* server.c — TCP server for tsdb wire protocol v1.
 *
 * Threading model:
 *   - One accept thread: poll() on listen fd, spawns per-connection thread.
 *   - Per-connection thread: blocking read_all / write_all (tsdb_proto_recv/send).
 *     This keeps the per-connection state machine simple and deterministic.
 *   - Subscription fan-out: a global subscription list protected by a mutex.
 *     After each tsdb_batch_commit(), the write path walks the list and sends
 *     SUB_EVENT frames to matching subscribers (coalesced with 10 ms timer).
 *
 * POSIX-only; no platform-specific event loop needed for correctness.
 */

#include "tls.h"    /* must come before proto.h to expose tsdb_io_t */
#include "server.h"
#include "proto.h"
#include "metrics.h"
#include "../../include/tsdb.h"
#include "../storage/db.h"
#include "../storage/schema.h"
#include "../query/result_internal.h"  /* peek+rewind status-row results */
#include "../query/exec.h"              /* tsdb_query_set_deadline_ns */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

/* ---- Subscription -------------------------------------------------------- */

#define SUB_TABLE_MAX  64
#define SUBS_MAX       256

/*
 * Optional equality filter: match rows where col_name equals filter_val
 * (string comparison for SYMBOL; decimal parse for INT64).
 * If filter_col is empty, no filtering is applied.
 */
typedef struct tsdb_sub {
    uint64_t        sub_id;
    char            table[64];
    int             conn_fd;    /* -1 if removed */
    uint64_t        req_id;
    pthread_mutex_t lock;       /* protects conn_fd */
    /* Optional row filter (equality on one column). */
    char            filter_col[64];  /* column name, empty = no filter */
    char            filter_val[256]; /* expected value as string */
} tsdb_sub_t;

typedef struct {
    tsdb_sub_t      subs[SUBS_MAX];
    int             nsubs;
    pthread_mutex_t lock;
    atomic_uint_fast64_t next_sub_id;
} tsdb_sub_list_t;

static void sub_list_init(tsdb_sub_list_t *sl) {
    memset(sl, 0, sizeof(*sl));
    pthread_mutex_init(&sl->lock, NULL);
    for (int i = 0; i < SUBS_MAX; i++)
        pthread_mutex_init(&sl->subs[i].lock, NULL);
    atomic_store(&sl->next_sub_id, 1);
}

static void sub_list_destroy(tsdb_sub_list_t *sl) {
    pthread_mutex_lock(&sl->lock);
    for (int i = 0; i < SUBS_MAX; i++)
        pthread_mutex_destroy(&sl->subs[i].lock);
    pthread_mutex_unlock(&sl->lock);
    pthread_mutex_destroy(&sl->lock);
}

static uint64_t sub_list_add(tsdb_sub_list_t *sl, const char *table,
                              int conn_fd, uint64_t req_id,
                              const char *filter_col, const char *filter_val) {
    pthread_mutex_lock(&sl->lock);

    int slot = -1;

    if (sl->nsubs < SUBS_MAX) {
        /* Append to array first — avoids scanning for uninitialized slots
         * that have conn_fd == 0 (not -1) from the initial memset. */
        slot = sl->nsubs;
    } else {
        /* Array full: scan for a tombstone (sub removed after nsubs hit cap). */
        for (int i = 0; i < SUBS_MAX; i++) {
            if (sl->subs[i].conn_fd < 0 && sl->subs[i].sub_id == 0) {
                slot = i;
                break;
            }
        }
    }

    if (slot == -1) { pthread_mutex_unlock(&sl->lock); return 0; }

    uint64_t id = atomic_fetch_add(&sl->next_sub_id, 1);
    tsdb_sub_t *s = &sl->subs[slot];
    s->sub_id  = id;
    s->conn_fd = conn_fd;
    s->req_id  = req_id;
    snprintf(s->table,      sizeof(s->table),      "%s", table);
    snprintf(s->filter_col, sizeof(s->filter_col), "%s", filter_col ? filter_col : "");
    snprintf(s->filter_val, sizeof(s->filter_val), "%s", filter_val ? filter_val : "");
    if (slot == sl->nsubs) sl->nsubs++;
    pthread_mutex_unlock(&sl->lock);
    return id;
}

static void sub_list_remove_by_id(tsdb_sub_list_t *sl, uint64_t sub_id) {
    pthread_mutex_lock(&sl->lock);
    for (int i = 0; i < sl->nsubs; i++) {
        if (sl->subs[i].sub_id == sub_id) {
            pthread_mutex_lock(&sl->subs[i].lock);
            sl->subs[i].conn_fd = -1;
            sl->subs[i].sub_id  = 0;
            pthread_mutex_unlock(&sl->subs[i].lock);
            break;
        }
    }
    pthread_mutex_unlock(&sl->lock);
}

static void sub_list_remove_by_fd(tsdb_sub_list_t *sl, int conn_fd) {
    pthread_mutex_lock(&sl->lock);
    for (int i = 0; i < sl->nsubs; i++) {
        if (sl->subs[i].conn_fd == conn_fd) {
            pthread_mutex_lock(&sl->subs[i].lock);
            sl->subs[i].conn_fd = -1;
            sl->subs[i].sub_id  = 0;
            pthread_mutex_unlock(&sl->subs[i].lock);
        }
    }
    pthread_mutex_unlock(&sl->lock);
}

/* Snapshot type for a matched subscriber during fanout. */
typedef struct {
    int      fd;
    uint64_t req_id;
    char     filter_col[64];
    char     filter_val[256];
} sub_snap_t;

/*
 * Snapshot all active subscriptions for this table.
 * Holds sl->lock only for the snapshot; no I/O under lock.
 * Returns number of matched subs (up to SUBS_MAX).
 */
static int sub_list_snapshot(tsdb_sub_list_t *sl, const char *table,
                               sub_snap_t *out, int cap) {
    int count = 0;
    pthread_mutex_lock(&sl->lock);
    for (int i = 0; i < sl->nsubs && count < cap; i++) {
        tsdb_sub_t *s = &sl->subs[i];
        if (s->conn_fd < 0) continue;
        if (strncmp(s->table, table, sizeof(s->table) - 1) != 0) continue;

        /* Grab fd + req_id atomically under per-sub lock. */
        pthread_mutex_lock(&s->lock);
        int fd = s->conn_fd;
        uint64_t req_id = s->req_id;
        pthread_mutex_unlock(&s->lock);

        if (fd < 0) continue;
        out[count].fd     = fd;
        out[count].req_id = req_id;
        snprintf(out[count].filter_col, 64,  "%s", s->filter_col);
        snprintf(out[count].filter_val, 256, "%s", s->filter_val);
        count++;
    }
    pthread_mutex_unlock(&sl->lock);
    return count;
}

/*
 * Build a structured SUB_EVENT payload and fan it out to matching subscribers.
 *
 * SUB_EVENT payload layout (little-endian):
 *   [table_len u8]  [table utf8]
 *   [nrows u32]
 *   [ncols u16]
 *   for each col: [name_len u8][name utf8][type u8]
 *   for each col: [nrows * 8 bytes, columnar]
 *
 * Per-subscriber column filter: if filter_col is non-empty, only fans out
 * if at least one row in the batch matches filter_val for that column.
 * For SYMBOL columns: exact string match.
 * For INT64 columns: decimal parse and compare.
 * Unresolved columns: filter ignored (all rows pass).
 */
typedef struct {
    const char    *col_name;
    uint8_t        col_type;    /* tsdb_type_t */
    const uint8_t *data;        /* nrows * 8 bytes */
} fanout_col_t;

static int fanout_row_matches(const fanout_col_t *cols, int ncols, int row,
                               const char *filter_col, const char *filter_val) {
    if (!filter_col || filter_col[0] == '\0') return 1; /* no filter */
    for (int c = 0; c < ncols; c++) {
        if (strcmp(cols[c].col_name, filter_col) != 0) continue;
        const uint8_t *ptr = cols[c].data + (size_t)row * 8;
        if (cols[c].col_type == TSDB_TYPE_INT64) {
            int64_t v;
            memcpy(&v, ptr, 8);
            char tmp[32];
            snprintf(tmp, sizeof(tmp), "%lld", (long long)v);
            return strcmp(tmp, filter_val) == 0;
        }
        /* For SYMBOL or other types: compare first 8 bytes as a string length field.
         * Symbol data in the write-batch is encoded as sym_id (4 bytes); we can't
         * resolve sym strings here without the symtab.  Treat as int compare. */
        int64_t v;
        memcpy(&v, ptr, 8);
        char tmp[32];
        snprintf(tmp, sizeof(tmp), "%lld", (long long)v);
        return strcmp(tmp, filter_val) == 0;
    }
    return 1; /* column not found — let it pass */
}

static void sub_list_fanout_event(tsdb_sub_list_t *sl, const char *table,
                                   const fanout_col_t *cols, int ncols,
                                   uint32_t nrows) {
    sub_snap_t snaps[SUBS_MAX];
    int nsubs = sub_list_snapshot(sl, table, snaps, SUBS_MAX);
    if (nsubs == 0) return;

    /* Build SUB_EVENT payload once (before per-sub filter check). */
    size_t tlen   = strlen(table);
    size_t hdr_sz = 1 + tlen + 4 + 2; /* table_len + table + nrows + ncols */
    for (int c = 0; c < ncols; c++)
        hdr_sz += 1 + strlen(cols[c].col_name) + 1; /* name_len + name + type */
    size_t data_sz = (size_t)ncols * (size_t)nrows * 8;
    size_t total   = hdr_sz + data_sz;

    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) return;

    uint8_t *p = buf;
    /* table name */
    uint8_t tlen8 = (uint8_t)(tlen < 255 ? tlen : 255);
    *p++ = tlen8;
    memcpy(p, table, tlen8); p += tlen8;
    /* nrows */
    memcpy(p, &nrows, 4); p += 4;
    /* ncols */
    uint16_t nc16 = (uint16_t)ncols;
    memcpy(p, &nc16, 2); p += 2;
    /* column headers */
    for (int c = 0; c < ncols; c++) {
        size_t cnl = strlen(cols[c].col_name);
        uint8_t cnl8 = (uint8_t)(cnl < 255 ? cnl : 255);
        *p++ = cnl8;
        memcpy(p, cols[c].col_name, cnl8); p += cnl8;
        *p++ = cols[c].col_type;
    }
    /* columnar data */
    for (int c = 0; c < ncols; c++) {
        memcpy(p, cols[c].data, (size_t)nrows * 8);
        p += (size_t)nrows * 8;
    }

    /* Fan out to each subscriber — I/O happens outside any lock. */
    for (int s = 0; s < nsubs; s++) {
        /* Apply per-subscriber filter: check if any row matches. */
        if (snaps[s].filter_col[0] != '\0') {
            int match = 0;
            for (uint32_t r = 0; r < nrows && !match; r++) {
                match = fanout_row_matches(cols, ncols, (int)r,
                                           snaps[s].filter_col,
                                           snaps[s].filter_val);
            }
            if (!match) continue;
        }
        /* Best-effort send — connection handler cleans up on next recv error. */
        tsdb_proto_send(snaps[s].fd, TSDB_MT_SUB_EVENT, TSDB_FLAG_STREAM,
                        snaps[s].req_id, buf, total);
    }
    free(buf);
}

/* ---- Write serializer ---------------------------------------------------- */
/*
 * The underlying tsdb_batch API is NOT thread-safe for concurrent writes
 * to the same table (memtable has no internal concurrency control).
 * We serialize per table using a small hash table of mutexes.
 */
#define WRITE_LOCK_BUCKETS  64

typedef struct {
    pthread_mutex_t mu;
    char            table[64];  /* locked table name, empty if free */
} write_lock_entry_t;

typedef struct {
    write_lock_entry_t entries[WRITE_LOCK_BUCKETS];
    pthread_mutex_t    meta;  /* protects table-name assignment */
} write_lock_pool_t;

static void write_lock_pool_init(write_lock_pool_t *p) {
    memset(p, 0, sizeof(*p));
    pthread_mutex_init(&p->meta, NULL);
    for (int i = 0; i < WRITE_LOCK_BUCKETS; i++)
        pthread_mutex_init(&p->entries[i].mu, NULL);
}

static void write_lock_pool_destroy(write_lock_pool_t *p) {
    for (int i = 0; i < WRITE_LOCK_BUCKETS; i++)
        pthread_mutex_destroy(&p->entries[i].mu);
    pthread_mutex_destroy(&p->meta);
}

/* Simple djb2-style hash. */
static unsigned write_lock_hash(const char *s) {
    unsigned h = 5381;
    while (*s) h = h * 33 ^ (unsigned char)*s++;
    return h % WRITE_LOCK_BUCKETS;
}

static void write_lock_acquire(write_lock_pool_t *p, const char *table) {
    /* Use hash-based bucket — two tables can collide (serialized together),
     * but that's safe and only costs throughput, not correctness. */
    unsigned idx = write_lock_hash(table);
    pthread_mutex_lock(&p->entries[idx].mu);
}

static void write_lock_release(write_lock_pool_t *p, const char *table) {
    unsigned idx = write_lock_hash(table);
    pthread_mutex_unlock(&p->entries[idx].mu);
}

/* ---- Server struct -------------------------------------------------------- */

/* Multi-accept: N listener sockets bound to the same port via
 * SO_REUSEPORT, each with its own accept thread.  Mirrors the Influx
 * HTTP path (commit b419c3d) — wire-protocol benches at 8+ concurrent
 * SDK writers used to saturate a single accept queue at ~186 k r/s. */
#define TSDB_WIRE_MAX_ACCEPT 32

struct tsdb_server {
    int                 listen_fds[TSDB_WIRE_MAX_ACCEPT];
    int                 n_listeners;
    int                 port;
    tsdb_db_t          *db;
    tsdb_server_opts_t  opts;

    pthread_t           accept_threads[TSDB_WIRE_MAX_ACCEPT];
    volatile int        running;

    tsdb_sub_list_t     subs;
    write_lock_pool_t   write_locks;

    /* TLS context (NULL when running in plaintext mode). */
    tsdb_tls_ctx_t     *tls_ctx;

    /* Atomic stats */
    atomic_uint_fast64_t stat_conns;
    atomic_uint_fast64_t stat_rows_written;
    atomic_uint_fast64_t stat_queries;
    atomic_uint_fast64_t stat_subs_active;
    atomic_uint_fast64_t stat_bytes_in;
    atomic_uint_fast64_t stat_bytes_out;
};

/* ---- Per-connection context ---------------------------------------------- */

typedef struct {
    tsdb_server_t   *srv;
    int              fd;
    tsdb_tls_conn_t *tls;  /* NULL = plaintext */
} conn_ctx_t;

/* ---- Payload helpers ------------------------------------------------------ */

/* Encode a simple status response (OK or ERROR). */
static int send_ok(tsdb_io_t *io, uint64_t req_id) {
    return tsdb_proto_send_io(io, TSDB_MT_HELLO_OK,
                              TSDB_FLAG_FIN, req_id, NULL, 0);
}

static int send_write_ack(tsdb_io_t *io, uint64_t req_id, uint32_t nrows) {
    uint8_t buf[4];
    buf[0] = (uint8_t)(nrows);
    buf[1] = (uint8_t)(nrows >> 8);
    buf[2] = (uint8_t)(nrows >> 16);
    buf[3] = (uint8_t)(nrows >> 24);
    return tsdb_proto_send_io(io, TSDB_MT_WRITE_ACK,
                              TSDB_FLAG_FIN, req_id, buf, 4);
}

static int send_error(tsdb_io_t *io, uint64_t req_id, int err_code, const char *msg) {
    size_t mlen = msg ? strlen(msg) : 0;
    size_t total = 4 + mlen;
    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) return TSDB_ERR_NOMEM;
    buf[0] = (uint8_t)(err_code);
    buf[1] = (uint8_t)((err_code) >> 8);
    buf[2] = (uint8_t)((err_code) >> 16);
    buf[3] = (uint8_t)((err_code) >> 24);
    if (mlen > 0) memcpy(buf + 4, msg, mlen);
    int rc = tsdb_proto_send_io(io, TSDB_MT_ERROR, TSDB_FLAG_FIN, req_id, buf, total);
    free(buf);
    return rc;
}

/* ---- Auth gate (shared by WRITE/DDL/SUBSCRIBE) --------------------------- */
/*
 * Returns TSDB_OK if the request may proceed, TSDB_ERR_PERMISSION otherwise.
 * On denial, sends TSDB_MT_ERROR with the supplied err_msg and returns the
 * error code — the caller should propagate it back to the dispatch loop.
 *
 * Behaviour matrix:
 *   require_auth=true,  no token → deny "auth required"
 *   require_auth=false, no token → allow (legacy bypass)
 *   token present → delegate to tsdb_auth_check(token, priv, resource).
 *     A bogus / revoked token, or a valid token lacking priv on resource,
 *     returns TSDB_ERR_PERMISSION.
 */
static int auth_gate(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                      int priv, const char *resource)
{
    int have_token = (io->auth_token[0] != '\0');
    if (!have_token) {
        if (srv->opts.require_auth) {
            tsdb_metric_inc("qengine_auth_denied_total");
            send_error(io, req_id, TSDB_ERR_PERMISSION,
                       "auth required: send AUTH_LOGIN first");
            return TSDB_ERR_PERMISSION;
        }
        return TSDB_OK;   /* legacy bypass */
    }
    int rc = tsdb_auth_check(srv->db, io->auth_token, priv, resource);
    if (rc != TSDB_OK) {
        tsdb_metric_inc("qengine_auth_denied_total");
        send_error(io, req_id, TSDB_ERR_PERMISSION, "permission denied");
        return TSDB_ERR_PERMISSION;
    }
    return TSDB_OK;
}

/* ---- WRITE_BATCH decoder ------------------------------------------------- */
/*
 * Wire columnar payload (from wire-protocol.md):
 *   [table_name_len u8] [table_name utf8]
 *   [ncols u16]        [nrows u32]
 *   for each column:
 *     [col_name_len u8] [col_name utf8]
 *     [col_type u8]
 *     [codec u8]            -- currently only CODEC_RAW (0) supported
 *     [compressed_size u32]
 *     [compressed bytes]
 */
#define TSDB_CODEC_RAW  0

static int handle_write_batch(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                               const uint8_t *payload, uint32_t plen) {
    int fd = io->fd;
    const uint8_t *p = payload;
    const uint8_t *end = payload + plen;

#define NEED(n) if (p + (n) > end) return send_error(io, req_id, TSDB_ERR_INVAL, "short payload")

    NEED(1);
    uint8_t tnlen = *p++;
    NEED(tnlen);
    char table_name[256];
    int copy = (tnlen < (int)sizeof(table_name) - 1) ? tnlen : (int)sizeof(table_name) - 1;
    memcpy(table_name, p, copy);
    table_name[copy] = '\0';
    p += tnlen;

    /* RBAC gate: require_auth + INSERT-on-table. */
    if (auth_gate(srv, io, req_id, TSDB_PRIV_INSERT, table_name) != TSDB_OK)
        return TSDB_ERR_PERMISSION;

    NEED(6);
    uint16_t ncols;
    memcpy(&ncols, p, 2); p += 2;
    uint32_t nrows;
    memcpy(&nrows, p, 4); p += 4;

    if (ncols == 0 || ncols > TSDB_MAX_COLS || nrows == 0 || nrows > 16*1024*1024)
        return send_error(io, req_id, TSDB_ERR_INVAL, "bad ncols/nrows");

    /* Stack usage at TSDB_MAX_COLS=256: col_names 64KiB + col_types 256B +
     * col_data 2KiB + col_sizes 1KiB ≈ 67KiB, well inside the 8MiB pthread
     * default.  Bump to heap if TSDB_MAX_COLS ever crosses 1024. */
    char            col_names[TSDB_MAX_COLS][256];
    uint8_t         col_types[TSDB_MAX_COLS];
    const uint8_t  *col_data [TSDB_MAX_COLS];
    uint32_t        col_sizes[TSDB_MAX_COLS];

    for (int c = 0; c < ncols; c++) {
        NEED(1);
        uint8_t cnlen = *p++;
        NEED(cnlen);
        int cn = (cnlen < 255) ? cnlen : 255;
        memcpy(col_names[c], p, cn);
        col_names[c][cn] = '\0';
        p += cnlen;

        NEED(3);
        col_types[c] = *p++;  /* tsdb_type_t */
        uint8_t codec = *p++;
        (void)codec;  /* only RAW supported; others could decompress here */

        NEED(4);
        uint32_t csz;
        memcpy(&csz, p, 4); p += 4;
        col_sizes[c] = csz;

        NEED(csz);
        col_data[c] = p;
        p += csz;
    }
#undef NEED

    /* Acquire per-table write lock — tsdb_batch API is not thread-safe
     * for concurrent writes to the same table. */
    write_lock_acquire(&srv->write_locks, table_name);

    /* Open the table (under write lock so open + begin are atomic). */
    tsdb_table_t *tbl = NULL;
    if (tsdb_open_table(srv->db, table_name, &tbl) != TSDB_OK || !tbl) {
        write_lock_release(&srv->write_locks, table_name);
        return send_error(io, req_id, TSDB_ERR_NOTFOUND, "table not found");
    }

    /* Also take the table's own batch lock (batch_mu).  The pool lock above
     * only serialises wire writers against each other; the cluster RPC
     * receiver, the influx path and the anti-entropy re-pull all serialise
     * on batch_mu instead.  Without joining that domain, a wire write can
     * append to the memtable while one of those callers is mid-flush, and
     * tsdb_part_flush_ex's sorted-permutation walk runs past the row count
     * it snapshotted — heap corruption under repair + live-write load. */
    tsdb_table_lock_write(tbl);

    tsdb_batch_t *batch = NULL;
    if (tsdb_batch_begin(tbl, &batch) != TSDB_OK) {
        tsdb_table_unlock_write(tbl);
        write_lock_release(&srv->write_locks, table_name);
        return send_error(io, req_id, TSDB_ERR_INTERNAL, "batch_begin failed");
    }

    /* Find timestamp column index. */
    int ts_ci = -1;
    for (int c = 0; c < ncols; c++) {
        if (col_types[c] == TSDB_TYPE_TIMESTAMP) { ts_ci = c; break; }
    }

    /* Sanity-check column sizes.
     *
     * SYMBOL columns now carry a length-prefix wire format that mirrors
     * the cluster RPC WRITE_BATCH receiver:
     *
     *   [u32 total_bytes] [u16 len_0] [bytes_0] … [u16 len_n-1] [bytes_n-1]
     *
     * `csz` for a SYMBOL column is `4 + total_bytes`, NOT a stride*nrows
     * product.  The legacy 4-byte sym-id encoding never worked end-to-end
     * (the receiver fabricated "sym_%u" strings the client never set),
     * so this is a clean upgrade — no real client used the old shape.
     * INT64/FLOAT64/TIMESTAMP keep the 8-byte stride. */
    for (int c = 0; c < ncols; c++) {
        if (col_types[c] == TSDB_TYPE_SYMBOL) {
            if (col_sizes[c] < 4) {
                tsdb_batch_discard(batch);
                tsdb_table_unlock_write(tbl);
                write_lock_release(&srv->write_locks, table_name);
                return send_error(io, req_id, TSDB_ERR_INVAL, "symbol col too short");
            }
            uint32_t total = (uint32_t)col_data[c][0]
                | ((uint32_t)col_data[c][1] <<  8)
                | ((uint32_t)col_data[c][2] << 16)
                | ((uint32_t)col_data[c][3] << 24);
            if (total + 4 > col_sizes[c]) {
                tsdb_batch_discard(batch);
                tsdb_table_unlock_write(tbl);
                write_lock_release(&srv->write_locks, table_name);
                return send_error(io, req_id, TSDB_ERR_INVAL, "symbol total > csz");
            }
        } else {
            if (col_sizes[c] < 8 * nrows) {
                tsdb_batch_discard(batch);
                tsdb_table_unlock_write(tbl);
                write_lock_release(&srv->write_locks, table_name);
                return send_error(io, req_id, TSDB_ERR_INVAL, "col data too short");
            }
        }
    }

    /* Bulk columnar append — same fast path the cluster RPC receiver
     * uses since b4c1461.  Per-row dispatch is gone; the wire payload
     * already maps onto memtable_append_bulk's expected layout. */
    int write_err = TSDB_OK;
    {
        const void *col_arrs[TSDB_MAX_COLS];
        int         col_types_arr[TSDB_MAX_COLS];
        int         n_data = 0;
        for (int c = 0; c < ncols; c++) {
            if (c == ts_ci) continue;
            col_arrs[n_data]      = col_data[c];
            col_types_arr[n_data] = col_types[c];
            n_data++;
        }
        const int64_t *ts_arr = (ts_ci >= 0)
            ? (const int64_t *)col_data[ts_ci]
            : NULL;
        int64_t *ts_synth = NULL;
        if (!ts_arr) ts_synth = calloc(nrows, sizeof(int64_t));
        write_err = tsdb_batch_append_bulk(batch,
                                           ts_arr ? ts_arr : ts_synth,
                                           col_arrs, col_types_arr,
                                           n_data, (size_t)nrows);
        free(ts_synth);
    }

    if (write_err != TSDB_OK) {
        tsdb_batch_discard(batch);
        tsdb_table_unlock_write(tbl);
        write_lock_release(&srv->write_locks, table_name);
        return send_error(io, req_id, write_err, "append_bulk failed");
    }

    write_err = tsdb_batch_commit(batch);
    tsdb_table_unlock_write(tbl);
    write_lock_release(&srv->write_locks, table_name);

    if (write_err != TSDB_OK)
        return send_error(io, req_id, write_err, "batch_commit failed");

    atomic_fetch_add(&srv->stat_rows_written, nrows);
    tsdb_metric_add("qengine_rows_written_total", nrows);
    tsdb_metric_add("qengine_bytes_written_total", plen);
    tsdb_metric_observe("qengine_ingest_batch_size", (double)nrows);

    /* Fan-out subscriptions with structured SUB_EVENT payload.
     *
     * SUB_EVENT requires 8 bytes per value in every column.  Fixed-width
     * cols (INT64, FLOAT64, TIMESTAMP) point directly into col_data.
     * SYMBOL cols arrive as the variable-length wire format described
     * above; we walk the per-row records and write the locally-interned
     * symbol code (resolved against the receiver's symtab) into a
     * scratch 8-byte buffer.  That code is what subscribers can pass
     * back to tsdb_symtab_str() if they share the same db handle. */
    {
        fanout_col_t fcols[TSDB_MAX_COLS];
        uint8_t *sym_scratch[TSDB_MAX_COLS];
        memset(sym_scratch, 0, sizeof(sym_scratch));
        int fanout_ok = 1;
        tsdb_table_internal_t *ti = tsdb_db_find_table(srv->db, table_name);
        tsdb_schema_t *sch = ti ? tsdb_tbl_schema(ti) : NULL;

        for (int c = 0; c < ncols && fanout_ok; c++) {
            fcols[c].col_name = col_names[c];
            fcols[c].col_type = (uint8_t)col_types[c];
            if (col_types[c] != TSDB_TYPE_SYMBOL) {
                fcols[c].data = col_data[c];
                continue;
            }
            sym_scratch[c] = (uint8_t *)calloc(nrows, 8);
            if (!sym_scratch[c]) { fanout_ok = 0; break; }
            const uint8_t *p_sym = col_data[c] + 4;  /* skip [u32 total] */
            const uint8_t *end_sym = col_data[c] + col_sizes[c];
            tsdb_symtab_t *st = NULL;
            if (sch) {
                int sci = -1;
                for (int j = 0; j < sch->ncols; j++)
                    if (strcmp(sch->cols[j].name, col_names[c]) == 0) { sci = j; break; }
                if (sci >= 0) st = sch->cols[sci].symtab;
            }
            for (uint32_t r = 0; r < nrows && p_sym + 2 <= end_sym; r++) {
                uint16_t l16; memcpy(&l16, p_sym, 2); p_sym += 2;
                if (p_sym + l16 > end_sym) break;
                char sbuf[260];
                int len = l16 < 256 ? l16 : 255;
                memcpy(sbuf, p_sym, len); sbuf[len] = '\0';
                p_sym += l16;
                uint64_t code = 0;
                if (st) code = (uint64_t)tsdb_symtab_intern(st, sbuf);
                memcpy(sym_scratch[c] + r * 8, &code, 8);
            }
            fcols[c].data = sym_scratch[c];
        }

        if (fanout_ok) {
            sub_list_fanout_event(&srv->subs, table_name, fcols, ncols, nrows);
        }

        for (int c = 0; c < ncols; c++) {
            if (sym_scratch[c]) free(sym_scratch[c]);
        }
    }

    return send_write_ack(io, req_id, nrows);
}

/* ---- CREATE_TABLE decoder ------------------------------------------------ */
/*
 * Payload:
 *   [table_name_len u8] [table_name]
 *   [ts_col_len u8]     [ts_col_name]
 *   [ncols u8]
 *   for each column:
 *     [name_len u8] [name] [type u8]
 */
static int handle_create_table(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                                const uint8_t *payload, uint32_t plen) {
    int fd = io->fd; (void)fd;
    const uint8_t *p = payload;
    const uint8_t *end = payload + plen;

#define NEED2(n) if (p + (n) > end) return send_error(io, req_id, TSDB_ERR_INVAL, "short payload")

    NEED2(1); uint8_t tnlen = *p++;
    NEED2(tnlen);
    char tname[256]; int tc = (tnlen<255)?tnlen:255;
    memcpy(tname, p, tc); tname[tc]='\0'; p+=tnlen;

    /* RBAC gate: require_auth + DDL privilege on target table. */
    if (auth_gate(srv, io, req_id, TSDB_PRIV_DDL, tname) != TSDB_OK)
        return TSDB_ERR_PERMISSION;

    NEED2(1); uint8_t tslen = *p++;
    NEED2(tslen);
    char ts_col[256]; int tsc=(tslen<255)?tslen:255;
    memcpy(ts_col, p, tsc); ts_col[tsc]='\0'; p+=tslen;

    NEED2(1); uint16_t ncols = *p++;
    /* Wire still carries 1 byte, so the hard upper bound is 255 until the
     * protocol adds a 2-byte ncols field; TSDB_MAX_COLS (256) is the
     * storage engine's ceiling.  Clamp to min of the two. */
    uint16_t ncols_limit = (TSDB_MAX_COLS < 255) ? (uint16_t)TSDB_MAX_COLS : 255;
    if (ncols == 0 || ncols > ncols_limit)
        return send_error(io, req_id, TSDB_ERR_INVAL, "bad ncols");

    tsdb_col_t cols[TSDB_MAX_COLS];
    char col_name_storage[TSDB_MAX_COLS][256];

    for (int c = 0; c < ncols; c++) {
        NEED2(1); uint8_t cnlen = *p++;
        NEED2(cnlen+1);
        int cn = (cnlen<255)?cnlen:255;
        memcpy(col_name_storage[c], p, cn);
        col_name_storage[c][cn]='\0'; p+=cnlen;
        uint8_t ctype = *p++;
        cols[c].name = col_name_storage[c];
        cols[c].type = (tsdb_type_t)ctype;
    }
#undef NEED2

    int rc = tsdb_create_table(srv->db, tname, cols, ncols, ts_col);
    if (rc == TSDB_ERR_EXISTS) rc = TSDB_OK; /* idempotent */
    if (rc != TSDB_OK)
        return send_error(io, req_id, rc, "create_table failed");

    /* Send HELLO_OK (reuse as generic OK) with FIN. */
    return tsdb_proto_send_io(io, TSDB_MT_HELLO_OK, TSDB_FLAG_FIN, req_id, NULL, 0);
}

/* ---- DROP_TABLE ---------------------------------------------------------- */
static int handle_drop_table(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                              const uint8_t *payload, uint32_t plen) {
    int fd = io->fd; (void)fd;
    if (!payload || plen == 0)
        return send_error(io, req_id, TSDB_ERR_INVAL, "missing table name");
    char tname[256];
    int n = (plen<255)?(int)plen:255;
    memcpy(tname, payload, n); tname[n]='\0';

    if (auth_gate(srv, io, req_id, TSDB_PRIV_DDL, tname) != TSDB_OK)
        return TSDB_ERR_PERMISSION;

    int rc = tsdb_drop_table(srv->db, tname);
    if (rc != TSDB_OK)
        return send_error(io, req_id, rc, "drop_table failed");
    return tsdb_proto_send_io(io, TSDB_MT_HELLO_OK, TSDB_FLAG_FIN, req_id, NULL, 0);
}

/* ---- QUERY --------------------------------------------------------------- */
/*
 * Payload: QTL string (UTF-8, no null terminator required).
 * Response:
 *   QUERY_RESULT_HDR (ncols, col names+types)
 *   QUERY_RESULT_ROWS × N (columnar chunks, up to 4096 rows each)
 *   final QUERY_RESULT_ROWS with TSDB_FLAG_FIN set (or HDR with FIN if 0 rows)
 */
#define QUERY_CHUNK_ROWS 4096

/*
 * Emit one QUERY_RESULT_ROWS chunk.
 *
 * Wire layout (per chunk):
 *   [nrows u32] [ncols u16]
 *   per column c, by ColType[c] (reader already has types from HDR):
 *     fixed (TS/INT64/FLOAT64): nrows * 8 bytes        (8-byte stride)
 *     SYMBOL:                   [u32 blocklen]          (length-prefixed
 *                               [u16 len][bytes] * nrows   variable block)
 *
 * Numeric columns keep the original fixed-stride encoding byte-for-byte;
 * only SYMBOL columns switch to the length-prefixed form.  Pre-2026-05 the
 * SYMBOL path wrote strlen() into an 8-byte cell (a never-finished stub —
 * see git history), so binary-wire clients only ever saw a symbol's *length*
 * in query results.  This carries the actual string.
 */
static int emit_query_chunk(tsdb_io_t *io, uint64_t req_id, int fin,
                            int ncols, const uint8_t *coltype,
                            uint8_t **numbuf,
                            uint8_t **symbuf, const size_t *symused,
                            int chunk_rows)
{
    size_t csz = 6;
    for (int c = 0; c < ncols; c++) {
        if (coltype[c] == TSDB_TYPE_SYMBOL) csz += 4 + symused[c];
        else                                csz += (size_t)chunk_rows * 8;
    }
    uint8_t *body = (uint8_t *)malloc(csz);
    if (!body) return TSDB_ERR_NOMEM;
    uint8_t *cp = body;
    uint32_t cr32 = (uint32_t)chunk_rows; memcpy(cp, &cr32, 4); cp += 4;
    uint16_t nc16 = (uint16_t)ncols;      memcpy(cp, &nc16, 2); cp += 2;
    for (int c = 0; c < ncols; c++) {
        if (coltype[c] == TSDB_TYPE_SYMBOL) {
            uint32_t bl = (uint32_t)symused[c];
            memcpy(cp, &bl, 4); cp += 4;
            if (bl) { memcpy(cp, symbuf[c], bl); cp += bl; }
        } else {
            if (chunk_rows > 0) memcpy(cp, numbuf[c], (size_t)chunk_rows * 8);
            cp += (size_t)chunk_rows * 8;
        }
    }
    int rc = tsdb_proto_send_io(io, TSDB_MT_QUERY_RESULT_ROWS,
                                fin ? TSDB_FLAG_FIN : 0, req_id, body, csz);
    free(body);
    return rc;
}

/* Append one symbol string to a growable per-column buffer as [u16 len][bytes].
 * Returns 0 on success, -1 on OOM.  Strings longer than 65535 are truncated. */
static int sym_append(uint8_t **buf, size_t *used, size_t *cap, const char *s) {
    size_t slen = s ? strlen(s) : 0;
    if (slen > 65535) slen = 65535;
    size_t need = *used + 2 + slen;
    if (need > *cap) {
        size_t ncap = *cap ? *cap * 2 : 4096;
        while (ncap < need) ncap *= 2;
        uint8_t *nb = (uint8_t *)realloc(*buf, ncap);
        if (!nb) return -1;
        *buf = nb; *cap = ncap;
    }
    uint16_t l16 = (uint16_t)slen;
    memcpy(*buf + *used, &l16, 2); *used += 2;
    if (slen) { memcpy(*buf + *used, s, slen); *used += slen; }
    return 0;
}

/* ---- AUTH_LOGIN handler -------------------------------------------------- */
/*
 * Payload: [u8 ulen][user bytes][u8 plen][password bytes]
 * On success, populates io->auth_token[33] and replies TSDB_MT_AUTH_OK with
 * the 32-char hex token as payload.
 * On failure, sends ERROR with TSDB_ERR_PERMISSION.
 */
static int handle_auth_login(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                              const uint8_t *payload, uint32_t plen)
{
    const uint8_t *p   = payload;
    const uint8_t *end = payload + plen;

    if (p + 1 > end) return send_error(io, req_id, TSDB_ERR_INVAL, "auth: short");
    uint8_t ulen = *p++;
    if (p + ulen > end) return send_error(io, req_id, TSDB_ERR_INVAL, "auth: user short");
    char user[64] = {0};
    size_t uc = ulen < sizeof(user) - 1 ? ulen : sizeof(user) - 1;
    memcpy(user, p, uc);
    p += ulen;

    if (p + 1 > end) return send_error(io, req_id, TSDB_ERR_INVAL, "auth: pass len short");
    uint8_t plen8 = *p++;
    if (p + plen8 > end) return send_error(io, req_id, TSDB_ERR_INVAL, "auth: pass short");
    char pass[128] = {0};
    size_t pc = plen8 < sizeof(pass) - 1 ? plen8 : sizeof(pass) - 1;
    memcpy(pass, p, pc);

    char tok[33] = {0};
    int rc = tsdb_auth_authenticate(srv->db, user, pass, tok, sizeof(tok));
    if (rc != TSDB_OK) {
        /* Zero any previous token — a failed login must not leave stale state. */
        memset(io->auth_token, 0, sizeof(io->auth_token));
        return send_error(io, req_id, TSDB_ERR_PERMISSION, "invalid credentials");
    }

    /* Persist on the connection state; surface in metric. */
    memcpy(io->auth_token, tok, sizeof(io->auth_token));
    tsdb_metric_inc("qengine_auth_logins_total");

    return tsdb_proto_send_io(io, TSDB_MT_AUTH_OK, TSDB_FLAG_FIN,
                              req_id, tok, 32);
}

static int handle_query(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                        const uint8_t *payload, uint32_t plen) {
    int fd = io->fd; (void)fd;

    /* cli/tsdb_client.c prefixes QTL with a u16 little-endian length.
     * Legacy callers (test_server.c, some SDKs) send the raw bytes. Detect
     * and strip the u16 prefix when it matches (plen - 2). */
    const uint8_t *q_bytes = payload;
    uint32_t       q_plen  = plen;
    if (plen >= 2) {
        uint16_t declared = (uint16_t)payload[0] | ((uint16_t)payload[1] << 8);
        if ((uint32_t)declared + 2 == plen) {
            q_bytes = payload + 2;
            q_plen  = declared;
        }
    }

    char qtl[4096] = {0};
    int qlen = (q_plen < sizeof(qtl) - 1) ? (int)q_plen : (int)sizeof(qtl) - 1;
    memcpy(qtl, q_bytes, qlen);

    /* Auth gate — require_auth rejects queries sent before TSDB_MT_AUTH_LOGIN
     * establishes a token; in bypass mode an empty token falls through to
     * the non-authenticated tsdb_query (legacy behaviour). */
    int have_token = (io->auth_token[0] != '\0');
    if (srv->opts.require_auth && !have_token) {
        tsdb_metric_inc("qengine_auth_denied_total");
        return send_error(io, req_id, TSDB_ERR_PERMISSION,
                           "auth required: send AUTH_LOGIN before QUERY");
    }

    /* Time the query for histogram metric. */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Per-query deadline.  Pre-fix request_timeout_ns was configured
     * in opts but nothing read it — runaway SELECTs ran forever and
     * pinned a connection thread.  Now we set a thread-local deadline
     * the executor checks at block boundaries, then clear it after
     * the call.  Save+restore the prior value so nested queries (auth
     * stack, raft re-entry) don't lose their own deadlines. */
    int64_t prev_deadline = 0;
    if (srv->opts.request_timeout_ns > 0) {
        int64_t deadline = (int64_t)t0.tv_sec * 1000000000LL + t0.tv_nsec
                         + srv->opts.request_timeout_ns;
        prev_deadline = tsdb_query_set_deadline_ns(deadline);
    }

    tsdb_result_t *res = NULL;
    int rc = have_token
        ? tsdb_query_auth(srv->db, io->auth_token, qtl, &res)
        : tsdb_query(srv->db, qtl, &res);

    if (srv->opts.request_timeout_ns > 0)
        tsdb_query_set_deadline_ns(prev_deadline);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed_ms = (double)(t1.tv_sec - t0.tv_sec) * 1e3 +
                        (double)(t1.tv_nsec - t0.tv_nsec) * 1e-6;

    tsdb_metric_inc("qengine_queries_total");
    tsdb_metric_observe("qengine_query_duration_ms", elapsed_ms);

    if (rc != TSDB_OK || !res) {
        tsdb_metric_inc("qengine_query_errors_total");
        return send_error(io, req_id, rc ? rc : TSDB_ERR_INTERNAL, "query failed");
    }

    atomic_fetch_add(&srv->stat_queries, 1);

    int ncols = tsdb_result_ncols(res);

    /* Status-row pre-check: tsdb_query returns TSDB_OK with a single-row,
     * single-column "status" SYMBOL result for a class of soft errors:
     *   - "ERR: not raft leader (...)"
     *   - "ERR: raft propose failed (timeout or IO)"
     *   - "ERR: stepped down mid-propose (...)"
     *   - "ERR: <pre-validation message>" (e.g. "stable not found")
     *
     * The status-row form was chosen so the HTTP /sql renderer can show
     * the message inline without leaking the result on rc != 0.  But the
     * wire SDK only treats MsgError frames as errors, so a status-row
     * "ERR:..." comes back to clients (Go SDK, JDBC, bench scripts) as a
     * SUCCESS — silently swallowing real failures.  Bench evidence:
     * stress-200m -ptables 5000 saw ~75% of CREATE TABLE statements fail
     * silently this way under Raft propose pressure.
     *
     * Detect the shape, decode the message, and convert to MsgError.
     * Reaches into tsdb_result_internal to peek+rewind cur because the
     * public API has no nrows accessor or rewind primitive. */
    if (ncols == 1 && res->nrows == 1) {
        const char *cn = tsdb_result_col_name(res, 0);
        tsdb_type_t ct = tsdb_result_col_type(res, 0);
        if (cn && strcmp(cn, "status") == 0 && ct == TSDB_TYPE_SYMBOL) {
            ssize_t saved_cur = res->cur;
            res->cur = 0;
            const char *msg = tsdb_result_sym(res, 0);
            res->cur = saved_cur;
            if (msg && strncmp(msg, "ERR:", 4) == 0) {
                tsdb_metric_inc("qengine_query_errors_total");
                int rc_send = send_error(io, req_id,
                                          TSDB_ERR_INTERNAL, msg);
                tsdb_result_free(res);
                return rc_send;
            }
        }
    }

    /* ---- Send QUERY_RESULT_HDR ------------------------------------------ */
    /* Encode: [ncols u16] for each: [name_len u8][name][type u8] */
    {
        size_t hdr_sz = 2;
        for (int i = 0; i < ncols; i++) {
            const char *cn = tsdb_result_col_name(res, i);
            hdr_sz += 1 + (cn ? strlen(cn) : 0) + 1;
        }
        uint8_t *hbuf = (uint8_t *)malloc(hdr_sz);
        if (!hbuf) { tsdb_result_free(res); return TSDB_ERR_NOMEM; }
        uint8_t *hp = hbuf;
        hp[0] = (uint8_t)(ncols); hp[1] = (uint8_t)(ncols >> 8); hp += 2;
        for (int i = 0; i < ncols; i++) {
            const char *cn = tsdb_result_col_name(res, i);
            uint8_t nlen = cn ? (uint8_t)strlen(cn) : 0;
            *hp++ = nlen;
            if (nlen) { memcpy(hp, cn, nlen); hp += nlen; }
            *hp++ = (uint8_t)tsdb_result_col_type(res, i);
        }
        /* If result has 0 rows, set FIN on HDR. */
        /* We don't know yet — send HDR without FIN, then check. */
        rc = tsdb_proto_send_io(io, TSDB_MT_QUERY_RESULT_HDR, 0, req_id,
                             hbuf, hdr_sz);
        free(hbuf);
        if (rc != TSDB_OK) { tsdb_result_free(res); return rc; }
    }

    /* ---- Stream rows in chunks ------------------------------------------ */
    /* Per-column scratch: numeric cols use a fixed 8-byte-stride buffer;
     * SYMBOL cols accumulate a growable length-prefixed block.  Reader
     * disambiguates by ColType (already sent in the HDR). */
    size_t col_buf_cap = (size_t)QUERY_CHUNK_ROWS * 8;
    uint8_t  *coltype  = (uint8_t *)calloc((size_t)ncols ? (size_t)ncols : 1, 1);
    uint8_t **cbuf     = (uint8_t **)calloc((size_t)ncols ? (size_t)ncols : 1, sizeof(uint8_t *));
    uint8_t **symbuf   = (uint8_t **)calloc((size_t)ncols ? (size_t)ncols : 1, sizeof(uint8_t *));
    size_t   *symused  = (size_t *)calloc((size_t)ncols ? (size_t)ncols : 1, sizeof(size_t));
    size_t   *symcap   = (size_t *)calloc((size_t)ncols ? (size_t)ncols : 1, sizeof(size_t));
    if (!coltype || !cbuf || !symbuf || !symused || !symcap) {
        free(coltype); free(cbuf); free(symbuf); free(symused); free(symcap);
        tsdb_result_free(res);
        return TSDB_ERR_NOMEM;
    }
    int alloc_ok = 1;
    for (int i = 0; i < ncols; i++) {
        coltype[i] = (uint8_t)tsdb_result_col_type(res, i);
        if (coltype[i] != TSDB_TYPE_SYMBOL) {
            cbuf[i] = (uint8_t *)malloc(col_buf_cap);
            if (!cbuf[i]) { alloc_ok = 0; break; }
        }
    }
    if (!alloc_ok) {
        for (int j = 0; j < ncols; j++) { free(cbuf[j]); free(symbuf[j]); }
        free(coltype); free(cbuf); free(symbuf); free(symused); free(symcap);
        tsdb_result_free(res);
        return TSDB_ERR_NOMEM;
    }

    int chunk_rows = 0;
    int any_rows = 0;
    int row_rc;

    while ((row_rc = tsdb_result_next(res)) == 1) {
        any_rows = 1;
        for (int c = 0; c < ncols; c++) {
            switch (coltype[c]) {
            case TSDB_TYPE_TIMESTAMP: {
                int64_t v = tsdb_result_ts(res, c);
                memcpy(cbuf[c] + (size_t)chunk_rows * 8, &v, 8); break;
            }
            case TSDB_TYPE_INT64: {
                int64_t v = tsdb_result_i64(res, c);
                memcpy(cbuf[c] + (size_t)chunk_rows * 8, &v, 8); break;
            }
            case TSDB_TYPE_FLOAT64:
            case TSDB_TYPE_FLOAT32: {
                /* FLOAT32 is an 8-byte double everywhere in the query path; the
                 * executor normally relabels result columns to FLOAT64, but
                 * handle the raw type here too so an unnormalised FLOAT32 column
                 * never emits an uninitialised cell to the wire. */
                double v = tsdb_result_f64(res, c);
                memcpy(cbuf[c] + (size_t)chunk_rows * 8, &v, 8); break;
            }
            case TSDB_TYPE_SYMBOL: {
                const char *s = tsdb_result_sym(res, c);
                if (sym_append(&symbuf[c], &symused[c], &symcap[c], s) != 0) {
                    row_rc = -1;  /* OOM — bail the stream loop */
                }
                break;
            }
            }
        }
        if (row_rc < 0) break;
        chunk_rows++;

        if (chunk_rows == QUERY_CHUNK_ROWS) {
            rc = emit_query_chunk(io, req_id, /*fin*/0, ncols, coltype,
                                  cbuf, symbuf, symused, chunk_rows);
            if (rc != TSDB_OK) break;
            chunk_rows = 0;
            for (int c = 0; c < ncols; c++) symused[c] = 0;
        }
    }

    /* Final chunk carries FIN (covers the partial-chunk and zero-row cases). */
    emit_query_chunk(io, req_id, /*fin*/1, ncols, coltype,
                     cbuf, symbuf, symused, chunk_rows);

    for (int i = 0; i < ncols; i++) { free(cbuf[i]); free(symbuf[i]); }
    free(coltype); free(cbuf); free(symbuf); free(symused); free(symcap);
    tsdb_result_free(res);
    (void)any_rows;
    return TSDB_OK;
}

/* ---- SUBSCRIBE / UNSUBSCRIBE -------------------------------------------- */

/*
 * SUBSCRIBE payload format (v1):
 *   [table_len u8]   [table utf8]
 *   [filter_len u16] [filter utf8]   -- "col_name=value" or empty
 *
 * Backward compat: if payload is just a plain table name string (no filter_len),
 * detect by checking whether plen == 1 + table_len (old format).
 */
static int handle_subscribe(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                             const uint8_t *payload, uint32_t plen) {
    int fd = io->fd;
    char table[256]      = {0};
    char filter_col[64]  = {0};
    char filter_val[256] = {0};

    if (plen == 0) {
        /* No table name — subscribe to all? Unsupported; use empty table = all. */
    } else if (plen >= 1) {
        const uint8_t *p   = payload;
        const uint8_t *end = payload + plen;

        uint8_t tlen = *p++;
        if (p + tlen > end) tlen = (uint8_t)(end - p);
        int tc = (tlen < 255) ? tlen : 255;
        memcpy(table, p, tc);
        p += tlen;

        /* Check for filter_len field (new format). */
        if (p + 2 <= end) {
            uint16_t flen;
            memcpy(&flen, p, 2); p += 2;
            if (flen > 0 && p + flen <= end) {
                /* Parse "col_name=value" filter string. */
                char filter_str[320] = {0};
                int fl = (flen < 319) ? flen : 319;
                memcpy(filter_str, p, fl);
                char *eq = strchr(filter_str, '=');
                if (eq) {
                    *eq = '\0';
                    snprintf(filter_col, sizeof(filter_col), "%s", filter_str);
                    snprintf(filter_val, sizeof(filter_val), "%s", eq + 1);
                }
            }
        } else if (p == end && tlen > 0) {
            /* Old format: payload was just the raw table string (no length prefix).
             * Treat as plain name already copied above. */
        }
    }

    /* RBAC gate: subscription reads data — need SELECT on the table. */
    if (auth_gate(srv, io, req_id, TSDB_PRIV_SELECT, table[0] ? table : "*")
        != TSDB_OK)
        return TSDB_ERR_PERMISSION;

    uint64_t sub_id = sub_list_add(&srv->subs, table, fd, req_id,
                                    filter_col, filter_val);
    if (sub_id == 0)
        return send_error(io, req_id, TSDB_ERR_FULL, "too many subscriptions");
    atomic_fetch_add(&srv->stat_subs_active, 1);

    /* Acknowledge with sub_id (8 bytes LE). */
    uint8_t ack[8];
    ack[0]=(uint8_t)(sub_id);     ack[1]=(uint8_t)(sub_id>>8);
    ack[2]=(uint8_t)(sub_id>>16); ack[3]=(uint8_t)(sub_id>>24);
    ack[4]=(uint8_t)(sub_id>>32); ack[5]=(uint8_t)(sub_id>>40);
    ack[6]=(uint8_t)(sub_id>>48); ack[7]=(uint8_t)(sub_id>>56);
    return tsdb_proto_send_io(io, TSDB_MT_HELLO_OK, 0, req_id, ack, 8);
}

static int handle_unsubscribe(tsdb_server_t *srv, tsdb_io_t *io, uint64_t req_id,
                               const uint8_t *payload, uint32_t plen) {
    int fd = io->fd;
    if (plen >= 8) {
        uint64_t sub_id;
        memcpy(&sub_id, payload, 8);
        sub_list_remove_by_id(&srv->subs, sub_id);
        uint64_t cur = atomic_load(&srv->stat_subs_active);
        if (cur > 0) atomic_fetch_sub(&srv->stat_subs_active, 1);
    }
    return tsdb_proto_send_io(io, TSDB_MT_HELLO_OK, TSDB_FLAG_FIN, req_id, NULL, 0);
}

/* ---- Main connection handler --------------------------------------------- */

static void *connection_handler(void *arg) {
    conn_ctx_t *ctx = (conn_ctx_t *)arg;
    tsdb_server_t   *srv = ctx->srv;
    int              fd  = ctx->fd;
    tsdb_tls_conn_t *tls = ctx->tls;
    free(ctx);

    /* Build IO abstraction — dispatches through TLS when tls != NULL. */
    tsdb_io_t io_obj = { .fd = fd, .tls = tls };
    tsdb_io_t *io = &io_obj;

    atomic_fetch_add(&srv->stat_conns, 1);
    tsdb_metric_inc("qengine_connections_total");
    tsdb_metric_gauge_inc("qengine_connections_active");

    tsdb_frame_hdr_t hdr;
    uint8_t *payload = NULL;

    for (;;) {
        int rc = tsdb_proto_recv_io(io, &hdr, &payload);
        if (rc != TSDB_OK) {
            /* Bad magic or closed connection: just disconnect. */
            break;
        }

        atomic_fetch_add(&srv->stat_bytes_in,
                         TSDB_PROTO_HDR_SIZE + hdr.payload_len + 4);

        switch ((tsdb_msg_type_t)hdr.type) {

        case TSDB_MT_HELLO:
            /* Version negotiation: accept any ver ≤ TSDB_PROTO_VER. */
            if (hdr.ver > TSDB_PROTO_VER) {
                send_error(io, hdr.req_id, TSDB_ERR_UNSUPPORTED, "version too high");
                free(payload);
                goto done;
            }
            send_ok(io, hdr.req_id);
            break;

        case TSDB_MT_PING:
            tsdb_proto_send_io(io, TSDB_MT_PING, TSDB_FLAG_PONG | TSDB_FLAG_FIN,
                               hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_AUTH_LOGIN:
            handle_auth_login(srv, io, hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_CREATE_TABLE:
            handle_create_table(srv, io, hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_DROP_TABLE:
            handle_drop_table(srv, io, hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_WRITE_BATCH:
            handle_write_batch(srv, io, hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_QUERY:
            handle_query(srv, io, hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_SUBSCRIBE:
            handle_subscribe(srv, io, hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_UNSUBSCRIBE:
            handle_unsubscribe(srv, io, hdr.req_id, payload, hdr.payload_len);
            break;

        case TSDB_MT_CLUSTER_STATS:
        case TSDB_MT_CREATE_GROUP:
        case TSDB_MT_LIST_GROUPS:
        case TSDB_MT_DROP_GROUP:
        case TSDB_MT_CREATE_DEVICE:
        case TSDB_MT_LIST_DEVICES:
        case TSDB_MT_DROP_DEVICE:
        case TSDB_MT_SCHEMA:
            /* Stub: ACK */
            tsdb_proto_send_io(io, TSDB_MT_HELLO_OK, TSDB_FLAG_FIN,
                               hdr.req_id, NULL, 0);
            break;

        default:
            send_error(io, hdr.req_id, TSDB_ERR_UNSUPPORTED, "unknown message type");
            break;
        }

        free(payload);
        payload = NULL;
    }

done:
    free(payload);
    sub_list_remove_by_fd(&srv->subs, fd);
    /* Close TLS connection (sends close_notify + closes fd).
     * For plain connections close the fd directly. */
    if (tls)
        tsdb_tls_close(tls);
    else
        close(fd);
    atomic_fetch_sub(&srv->stat_conns, 1);
    tsdb_metric_gauge_dec("qengine_connections_active");
    return NULL;
}

/* ---- Accept loop --------------------------------------------------------- */

typedef struct {
    tsdb_server_t *srv;
    int            fd;
} accept_arg_t;

static void *accept_loop(void *arg) {
    accept_arg_t *aa = (accept_arg_t *)arg;
    tsdb_server_t *srv = aa->srv;
    int my_fd = aa->fd;
    free(aa);

    while (srv->running) {
        struct pollfd pfd = { my_fd, POLLIN, 0 };
        int r = poll(&pfd, 1, 200 /* ms */);
        if (r <= 0) continue;
        if (!(pfd.revents & POLLIN)) continue;

        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(my_fd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }

        /* Disable Nagle for lower latency. */
        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        conn_ctx_t *ctx = (conn_ctx_t *)malloc(sizeof(*ctx));
        if (!ctx) { close(cfd); continue; }
        ctx->srv = srv;
        ctx->fd  = cfd;
        ctx->tls = NULL;

        /* TLS handshake (synchronous, blocks here before the handler thread). */
        if (srv->tls_ctx) {
            if (tsdb_tls_server_wrap(srv->tls_ctx, cfd, &ctx->tls) != 0) {
                /* Handshake failed (plaintext probe, bad cert, etc.) */
                free(ctx);
                close(cfd);
                continue;
            }
        }

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, connection_handler, ctx) != 0) {
            if (ctx->tls) tsdb_tls_close(ctx->tls);
            else          close(cfd);
            free(ctx);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

/* ---- Public API ---------------------------------------------------------- */

static int parse_addr(const char *addr, char *host_out, size_t hcap, int *port_out) {
    if (!addr) { snprintf(host_out, hcap, "0.0.0.0"); *port_out = 28090; return 0; }
    const char *colon = strrchr(addr, ':');
    if (!colon) return -1;
    size_t hlen = (size_t)(colon - addr);
    if (hlen >= hcap) return -1;
    memcpy(host_out, addr, hlen);
    host_out[hlen] = '\0';
    *port_out = atoi(colon + 1);
    return 0;
}

int tsdb_server_start(const tsdb_server_opts_t *opts, tsdb_server_t **out) {
    if (!opts || !opts->db || !out) return TSDB_ERR_INVAL;

    char host[256] = "0.0.0.0";
    int  port = 28090;
    if (opts->bind_addr)
        parse_addr(opts->bind_addr, host, sizeof(host), &port);

    /* TSDB_WIRE_ACCEPT_THREADS — N listener sockets via SO_REUSEPORT,
     * each with its own accept thread.  Default 4. */
    int n_listeners = 4;
    {
        const char *e = getenv("TSDB_WIRE_ACCEPT_THREADS");
        if (e && *e) n_listeners = atoi(e);
        if (n_listeners < 1) n_listeners = 1;
        if (n_listeners > TSDB_WIRE_MAX_ACCEPT) n_listeners = TSDB_WIRE_MAX_ACCEPT;
    }

    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_aton(host, &sa.sin_addr) == 0) return TSDB_ERR_INVAL;

    int first_lfd = -1;
    int listen_fds[TSDB_WIRE_MAX_ACCEPT];
    int nl = 0;
    for (int i = 0; i < n_listeners; i++) {
        int lfd = socket(AF_INET, SOCK_STREAM, 0);
        if (lfd < 0) goto fail_open;

        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
        setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

        if (bind(lfd, (struct sockaddr *)&sa, sizeof(sa)) < 0) { close(lfd); goto fail_open; }
        if (listen(lfd, 256) < 0) { close(lfd); goto fail_open; }

        listen_fds[nl++] = lfd;
        if (first_lfd < 0) first_lfd = lfd;
        continue;

    fail_open:
        for (int j = 0; j < nl; j++) close(listen_fds[j]);
        return TSDB_ERR_IO;
    }

    /* Read back actual port (important when port==0). */
    struct sockaddr_in bound = {0};
    socklen_t blen = sizeof(bound);
    getsockname(first_lfd, (struct sockaddr *)&bound, &blen);

    tsdb_server_t *srv = (tsdb_server_t *)calloc(1, sizeof(*srv));
    if (!srv) { for (int j = 0; j < nl; j++) close(listen_fds[j]); return TSDB_ERR_NOMEM; }

    for (int i = 0; i < nl; i++) srv->listen_fds[i] = listen_fds[i];
    srv->n_listeners = nl;
    srv->port        = ntohs(bound.sin_port);
    srv->db          = opts->db;
    srv->opts        = *opts;
    srv->running     = 1;

    atomic_init(&srv->stat_conns, 0);
    atomic_init(&srv->stat_rows_written, 0);
    atomic_init(&srv->stat_queries, 0);
    atomic_init(&srv->stat_subs_active, 0);
    atomic_init(&srv->stat_bytes_in, 0);
    atomic_init(&srv->stat_bytes_out, 0);

    sub_list_init(&srv->subs);
    write_lock_pool_init(&srv->write_locks);
    srv->tls_ctx = NULL;

    /* Initialise TLS context if cert + key are provided. */
    if (opts->tls_cert && opts->tls_cert[0] &&
        opts->tls_key  && opts->tls_key[0]) {
        if (tsdb_tls_available()) {
            if (tsdb_tls_server_ctx(opts->tls_cert, opts->tls_key,
                                    opts->tls_ca, &srv->tls_ctx) != 0) {
                sub_list_destroy(&srv->subs);
                write_lock_pool_destroy(&srv->write_locks);
                for (int i = 0; i < srv->n_listeners; i++) close(srv->listen_fds[i]);
                free(srv);
                return TSDB_ERR_INTERNAL;
            }
            fprintf(stderr, "[tls] server TLS enabled (cert=%s)\n", opts->tls_cert);
        } else {
            fprintf(stderr, "[tls] WARNING: TLS requested but no TLS backend compiled in\n");
        }
    }

    int spawned = 0;
    for (int i = 0; i < srv->n_listeners; i++) {
        accept_arg_t *aa = (accept_arg_t *)malloc(sizeof(*aa));
        if (!aa) goto fail_spawn;
        aa->srv = srv; aa->fd = srv->listen_fds[i];
        if (pthread_create(&srv->accept_threads[i], NULL, accept_loop, aa) != 0) {
            free(aa);
            goto fail_spawn;
        }
        spawned++;
    }

    fprintf(stderr, "[server] wire endpoint listening on %s:%d (%d accept threads)\n",
            host, srv->port, srv->n_listeners);
    *out = srv;
    return TSDB_OK;

fail_spawn:
    srv->running = 0;
    for (int i = 0; i < spawned; i++) {
        shutdown(srv->listen_fds[i], SHUT_RDWR);
        pthread_join(srv->accept_threads[i], NULL);
    }
    for (int i = 0; i < srv->n_listeners; i++) close(srv->listen_fds[i]);
    tsdb_tls_free(srv->tls_ctx);
    sub_list_destroy(&srv->subs);
    write_lock_pool_destroy(&srv->write_locks);
    free(srv);
    return TSDB_ERR_INTERNAL;
}

void tsdb_server_stop(tsdb_server_t *s) {
    if (!s) return;
    s->running = 0;
    /* Interrupt the poll()s by shutting down the listen fds.
     * Each accept loop will see POLLERR/POLLHUP and exit. */
    for (int i = 0; i < s->n_listeners; i++)
        shutdown(s->listen_fds[i], SHUT_RDWR);
    for (int i = 0; i < s->n_listeners; i++)
        pthread_join(s->accept_threads[i], NULL);
    for (int i = 0; i < s->n_listeners; i++)
        close(s->listen_fds[i]);
    tsdb_tls_free(s->tls_ctx);
    sub_list_destroy(&s->subs);
    write_lock_pool_destroy(&s->write_locks);
    free(s);
}

int tsdb_server_port(tsdb_server_t *s) {
    return s ? s->port : -1;
}

void tsdb_server_stats(tsdb_server_t *s, tsdb_server_stats_t *out) {
    if (!s || !out) return;
    out->conns        = atomic_load(&s->stat_conns);
    out->rows_written = atomic_load(&s->stat_rows_written);
    out->queries      = atomic_load(&s->stat_queries);
    out->subs_active  = atomic_load(&s->stat_subs_active);
    out->bytes_in     = atomic_load(&s->stat_bytes_in);
    out->bytes_out    = atomic_load(&s->stat_bytes_out);
}
