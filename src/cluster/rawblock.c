/* rawblock.c — Raw block replication (primary encode-once, replica write-verbatim). */

#include "rawblock.h"
#include "replica.h"
#include "node.h"
#include "rpc.h"
#include "../storage/db.h"
#include "../storage/schema.h"
#include "../storage/part.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>

/* ---- Extern helpers -------------------------------------------------------- */

extern int tsdb_mkdir_p(const char *path);
extern const char *tsdb_db_data_dir(tsdb_db_t *db);
extern tsdb_table_internal_t *tsdb_db_find_table(tsdb_db_t *db, const char *name);
extern tsdb_schema_t *tsdb_tbl_schema(tsdb_table_internal_t *t);

/* ---- LE helpers ------------------------------------------------------------ */

static inline void rb_put_u8(uint8_t *p, uint8_t v)    { p[0] = v; }
static inline void rb_put_u16(uint8_t *p, uint16_t v)  { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static inline void rb_put_u32(uint8_t *p, uint32_t v)  {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8);
    p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static inline void rb_put_i64(uint8_t *p, int64_t v) {
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; i++) { p[i] = (uint8_t)(u & 0xFF); u >>= 8; }
}
static inline uint8_t  rb_get_u8(const uint8_t *p)  { return p[0]; }
static inline uint16_t rb_get_u16(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }
static inline uint32_t rb_get_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static inline int64_t  rb_get_i64(const uint8_t *p) {
    uint64_t u = 0;
    for (int i = 7; i >= 0; i--) u = (u << 8) | p[i];
    return (int64_t)u;
}

/* ---- Serialize / parse ----------------------------------------------------- */

int tsdb_rawblock_serialize(const tsdb_rawblock_push_t *r,
                             uint8_t **buf, size_t *len)
{
    if (!r || !buf || !len) return TSDB_ERR_INVAL;

    uint8_t tlen = (uint8_t)strnlen(r->table, 63);
    /* 1 + tlen + 4 + 2 + 1 + 2 + 4 + 8 + 8 + 4 + block_bytes_len */
    size_t total = 1 + tlen + 4 + 2 + 1 + 2 + 4 + 8 + 8 + 4 + r->block_bytes_len;

    uint8_t *p = malloc(total);
    if (!p) return TSDB_ERR_NOMEM;

    uint8_t *w = p;
    rb_put_u8(w, tlen); w++;
    memcpy(w, r->table, tlen); w += tlen;
    rb_put_u32(w, r->part_day); w += 4;
    rb_put_u16(w, r->col_idx);  w += 2;
    rb_put_u8(w, r->codec);     w++;
    rb_put_u16(w, r->flags);    w += 2;
    rb_put_u32(w, r->count);    w += 4;
    rb_put_i64(w, r->ts_min);   w += 8;
    rb_put_i64(w, r->ts_max);   w += 8;
    rb_put_u32(w, r->block_bytes_len); w += 4;
    if (r->block_bytes_len > 0 && r->block_bytes)
        memcpy(w, r->block_bytes, r->block_bytes_len);

    *buf = p;
    *len = total;
    return TSDB_OK;
}

int tsdb_rawblock_parse(const uint8_t *buf, size_t len,
                         tsdb_rawblock_push_t *out)
{
    if (!buf || !out) return TSDB_ERR_INVAL;

    const uint8_t *p = buf;
    const uint8_t *end = buf + len;

#define RB_NEED(n) if (p + (n) > end) return TSDB_ERR_CORRUPT

    RB_NEED(1);
    uint8_t tlen = rb_get_u8(p); p++;
    if (tlen > 63) return TSDB_ERR_CORRUPT;
    RB_NEED(tlen);
    memcpy(out->table, p, tlen);
    out->table[tlen] = '\0';
    p += tlen;

    RB_NEED(4); out->part_day = rb_get_u32(p); p += 4;
    RB_NEED(2); out->col_idx  = rb_get_u16(p); p += 2;
    RB_NEED(1); out->codec    = rb_get_u8(p);  p++;
    RB_NEED(2); out->flags    = rb_get_u16(p); p += 2;
    RB_NEED(4); out->count    = rb_get_u32(p); p += 4;
    RB_NEED(8); out->ts_min   = rb_get_i64(p); p += 8;
    RB_NEED(8); out->ts_max   = rb_get_i64(p); p += 8;
    RB_NEED(4); out->block_bytes_len = rb_get_u32(p); p += 4;
    RB_NEED(out->block_bytes_len);
    out->block_bytes = (uint8_t *)p; /* points into caller's buf */

#undef RB_NEED
    return TSDB_OK;
}

/* ---- BlockHeader helpers (mirrors part.c) ---------------------------------- */

#define RAWBLK_HDR_SIZE 32u
#define RAWBLK_MAGIC    0x314B4C42u /* "BLK1" */

static void rawblk_write_header(uint8_t *hdr,
                                 uint8_t codec, uint16_t flags,
                                 uint32_t count,
                                 int64_t ts_min, int64_t ts_max,
                                 uint32_t data_size)
{
    rb_put_u32(hdr + 0,  RAWBLK_MAGIC);
    rb_put_u8 (hdr + 4,  codec);
    rb_put_u8 (hdr + 5,  0);
    rb_put_u16(hdr + 6,  flags);
    rb_put_u32(hdr + 8,  count);
    rb_put_i64(hdr + 12, ts_min);
    rb_put_i64(hdr + 20, ts_max);
    rb_put_u32(hdr + 28, data_size);
}

/* ---- IDX helpers (mirrors part.c) ----------------------------------------- */

#define RAWBLK_IDX_MAGIC    0x31584449u /* "IDX1" */
#define RAWBLK_IDX_VER      1
#define RAWBLK_IDX_HDR_SIZE 20u
#define RAWBLK_IDX_ENT_SIZE 40u

static void rawblk_write_idx_header(uint8_t *buf, uint32_t count, uint64_t total_rows) {
    rb_put_u32(buf + 0,  RAWBLK_IDX_MAGIC);
    rb_put_u32(buf + 4,  count);
    rb_put_u16(buf + 8,  RAWBLK_IDX_VER);
    rb_put_u16(buf + 10, 0);
    /* total_rows as u64 LE */
    for (int i = 0; i < 8; i++) buf[12+i] = (uint8_t)((total_rows >> (i*8)) & 0xFF);
}

static void rawblk_write_idx_entry(uint8_t *buf,
                                   uint64_t offset, uint32_t size, uint32_t count,
                                   int64_t ts_min, int64_t ts_max)
{
    /* offset u64 */
    for (int i = 0; i < 8; i++) buf[0+i] = (uint8_t)(((uint64_t)offset >> (i*8)) & 0xFF);
    rb_put_u32(buf + 8,  size);
    rb_put_u32(buf + 12, count);
    rb_put_i64(buf + 16, ts_min);
    rb_put_i64(buf + 24, ts_max);
    /* reserved */
    memset(buf + 32, 0, 8);
}

/* ---- Apply on replica ------------------------------------------------------ */

int tsdb_rawblock_apply(tsdb_db_t *db, const tsdb_rawblock_push_t *r)
{
    if (!db || !r) return TSDB_ERR_INVAL;

    const char *data_dir = tsdb_db_data_dir(db);
    if (!data_dir) return TSDB_ERR_INTERNAL;

    /* Resolve column name from schema. */
    tsdb_table_internal_t *tbl = tsdb_db_find_table(db, r->table);
    if (!tbl) return TSDB_ERR_NOTFOUND;

    tsdb_schema_t *schema = tsdb_tbl_schema(tbl);
    if (!schema) return TSDB_ERR_INTERNAL;
    if (r->col_idx >= (uint16_t)schema->ncols) return TSDB_ERR_INVAL;

    const char *col_name = schema->cols[r->col_idx].name;

    /* Build partition directory path from YYYYMMDD. */
    char day_str[9];
    snprintf(day_str, sizeof(day_str), "%08u", r->part_day);

    char part_dir[4096];
    snprintf(part_dir, sizeof(part_dir), "%s/%s/%s",
             data_dir, r->table, day_str);

    if (tsdb_mkdir_p(part_dir) < 0) return TSDB_ERR_IO;

    char col_path[4096];
    char idx_path[4096];
    snprintf(col_path, sizeof(col_path), "%s/%s.col", part_dir, col_name);
    snprintf(idx_path, sizeof(idx_path), "%s/%s.idx", part_dir, col_name);

    /* --- Idempotency: skip if last idx entry matches --- */
    {
        FILE *idx_r = fopen(idx_path, "rb");
        if (idx_r) {
            uint8_t hdr[RAWBLK_IDX_HDR_SIZE];
            if (fread(hdr, 1, RAWBLK_IDX_HDR_SIZE, idx_r) == RAWBLK_IDX_HDR_SIZE
                && rb_get_u32(hdr) == RAWBLK_IDX_MAGIC) {
                uint32_t cnt = rb_get_u32(hdr + 4);
                if (cnt > 0) {
                    /* Seek to last entry. */
                    long off = (long)(RAWBLK_IDX_HDR_SIZE + (uint64_t)(cnt - 1) * RAWBLK_IDX_ENT_SIZE);
                    if (fseek(idx_r, off, SEEK_SET) == 0) {
                        uint8_t ent[RAWBLK_IDX_ENT_SIZE];
                        if (fread(ent, 1, RAWBLK_IDX_ENT_SIZE, idx_r) == RAWBLK_IDX_ENT_SIZE) {
                            uint32_t e_size  = rb_get_u32(ent + 8);
                            uint32_t e_count = rb_get_u32(ent + 12);
                            int64_t  e_tmin  = rb_get_i64(ent + 16);
                            int64_t  e_tmax  = rb_get_i64(ent + 24);
                            if (e_size == r->block_bytes_len &&
                                e_count == r->count &&
                                e_tmin  == r->ts_min &&
                                e_tmax  == r->ts_max) {
                                fclose(idx_r);
                                return TSDB_OK; /* already applied */
                            }
                        }
                    }
                }
            }
            fclose(idx_r);
        }
    }

    /* --- Append to .col file --- */
    FILE *col_fp = fopen(col_path, "ab");
    if (!col_fp) return TSDB_ERR_IO;

    fseek(col_fp, 0, SEEK_END);
    long col_offset = ftell(col_fp);
    if (col_offset < 0) { fclose(col_fp); return TSDB_ERR_IO; }

    /* Write 32-byte BlockHeader. */
    uint8_t hdr[RAWBLK_HDR_SIZE];
    rawblk_write_header(hdr, r->codec, r->flags, r->count,
                        r->ts_min, r->ts_max, r->block_bytes_len);

    if (fwrite(hdr, 1, RAWBLK_HDR_SIZE, col_fp) != RAWBLK_HDR_SIZE) {
        fclose(col_fp); return TSDB_ERR_IO;
    }
    if (r->block_bytes_len > 0 && r->block_bytes) {
        if (fwrite(r->block_bytes, 1, r->block_bytes_len, col_fp) != r->block_bytes_len) {
            fclose(col_fp); return TSDB_ERR_IO;
        }
    }
    fflush(col_fp);
    fclose(col_fp);

    /* --- Rewrite .idx --- */
    /* Read existing entries. */
    uint8_t *old_entries = NULL;
    uint32_t old_count   = 0;
    uint64_t old_total   = 0;

    {
        FILE *idx_r = fopen(idx_path, "rb");
        if (idx_r) {
            uint8_t ih[RAWBLK_IDX_HDR_SIZE];
            if (fread(ih, 1, RAWBLK_IDX_HDR_SIZE, idx_r) == RAWBLK_IDX_HDR_SIZE
                && rb_get_u32(ih) == RAWBLK_IDX_MAGIC) {
                old_count = rb_get_u32(ih + 4);
                /* total_rows */
                uint64_t tr = 0;
                for (int i = 7; i >= 0; i--) tr = (tr << 8) | ih[12+i];
                old_total = tr;
                if (old_count > 0) {
                    old_entries = malloc((size_t)old_count * RAWBLK_IDX_ENT_SIZE);
                    if (old_entries) {
                        if (fread(old_entries, 1,
                                  (size_t)old_count * RAWBLK_IDX_ENT_SIZE, idx_r)
                            != (size_t)old_count * RAWBLK_IDX_ENT_SIZE) {
                            free(old_entries); old_entries = NULL; old_count = 0;
                        }
                    }
                }
            }
            fclose(idx_r);
        }
    }

    FILE *idx_w = fopen(idx_path, "wb");
    if (!idx_w) { free(old_entries); return TSDB_ERR_IO; }

    uint32_t new_count = old_count + 1;
    uint64_t new_total = old_total + r->count;

    uint8_t ih[RAWBLK_IDX_HDR_SIZE];
    rawblk_write_idx_header(ih, new_count, new_total);
    fwrite(ih, 1, RAWBLK_IDX_HDR_SIZE, idx_w);

    if (old_count > 0 && old_entries)
        fwrite(old_entries, 1, (size_t)old_count * RAWBLK_IDX_ENT_SIZE, idx_w);

    /* New entry. */
    uint8_t ent[RAWBLK_IDX_ENT_SIZE];
    rawblk_write_idx_entry(ent, (uint64_t)col_offset,
                           r->block_bytes_len, r->count,
                           r->ts_min, r->ts_max);
    fwrite(ent, 1, RAWBLK_IDX_ENT_SIZE, idx_w);

    fflush(idx_w);
    fclose(idx_w);
    free(old_entries);
    return TSDB_OK;
}

/* ---- Replicate (primary fan-out) ------------------------------------------ */

/* Per-replica send state. */
typedef struct {
    tsdb_replica_mgr_t *rmgr;
    tsdb_node_id_t      node_id;
    uint8_t            *payload;
    uint32_t            payload_len;
    volatile int       *ack_count;
    pthread_mutex_t    *ack_lock;
} rawblk_send_arg_t;

static void evict_rawblk_conn(tsdb_replica_mgr_t *rmgr, tsdb_node_id_t nid) {
    /* We reuse replica_mgr infrastructure; connection eviction is handled by
     * the existing pool logic — just mark it dirty by closing. */
    (void)rmgr; (void)nid;
    /* Best-effort: reconnect on next call. */
}

static void *rawblk_send_thread(void *arg) {
    rawblk_send_arg_t *sa = (rawblk_send_arg_t *)arg;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(sa->rmgr, sa->node_id);
    if (conn) {
        int rc = tsdb_rpc_call(conn, TSDB_RPC_RAW_BLOCK_PUSH,
                               sa->payload, sa->payload_len);
        if (rc == TSDB_OK) {
            pthread_mutex_lock(sa->ack_lock);
            (*sa->ack_count)++;
            pthread_mutex_unlock(sa->ack_lock);
        } else {
            evict_rawblk_conn(sa->rmgr, sa->node_id);
        }
    }
    free(sa);
    return NULL;
}

int tsdb_rawblock_replicate(tsdb_cluster_t *c,
                             const char *table, uint32_t day, uint16_t col_idx,
                             const tsdb_block_meta_t *meta,
                             const uint8_t *block_bytes, size_t block_len,
                             int quorum_w)
{
    if (!c || !table || !meta) return TSDB_ERR_INVAL;

    /* Build push struct. */
    tsdb_rawblock_push_t r;
    memset(&r, 0, sizeof(r));
    strncpy(r.table, table, 63);
    r.part_day        = day;
    r.col_idx         = col_idx;
    r.codec           = meta->codec;
    r.flags           = meta->flags;
    r.count           = meta->count;
    r.ts_min          = meta->ts_min;
    r.ts_max          = meta->ts_max;
    r.block_bytes_len = (uint32_t)block_len;
    r.block_bytes     = (uint8_t *)block_bytes;

    /* Serialize once. */
    uint8_t *payload = NULL;
    size_t   plen    = 0;
    if (tsdb_rawblock_serialize(&r, &payload, &plen) != TSDB_OK)
        return TSDB_ERR_INTERNAL;

    /* Gather ALIVE peers (excluding self). */
    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr(c);
    tsdb_node_info_t nodes[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(mgr, nodes, TSDB_CLUSTER_MAX_NODES);

    tsdb_node_id_t local_id = tsdb_cluster_local_id(c);
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);

    volatile int ack_count = 1; /* local write = 1 */
    pthread_mutex_t ack_lock;
    pthread_mutex_init(&ack_lock, NULL);

    pthread_t tids[TSDB_CLUSTER_MAX_NODES];
    int nthreads = 0;

    for (int i = 0; i < n && nthreads < TSDB_CLUSTER_MAX_NODES; i++) {
        if (nodes[i].id == local_id || nodes[i].state != TSDB_NODE_ALIVE)
            continue;

        rawblk_send_arg_t *sa = malloc(sizeof(*sa));
        if (!sa) continue;
        sa->rmgr        = rmgr;
        sa->node_id     = nodes[i].id;
        sa->payload     = payload;
        sa->payload_len = (uint32_t)plen;
        sa->ack_count   = &ack_count;
        sa->ack_lock    = &ack_lock;

        if (pthread_create(&tids[nthreads], NULL, rawblk_send_thread, sa) == 0)
            nthreads++;
        else
            free(sa);
    }

    for (int i = 0; i < nthreads; i++)
        pthread_join(tids[i], NULL);

    pthread_mutex_destroy(&ack_lock);
    free(payload);

    return (ack_count >= quorum_w) ? TSDB_OK : TSDB_ERR_IO;
}
