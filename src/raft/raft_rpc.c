/* raft_rpc.c — encoders / decoders for RequestVote + AppendEntries
 * and the server-side dispatcher hook called from rpc.c.
 *
 * Wire layout (all LE, packed, no padding):
 *
 *   RequestVote req  (32 B)   term | candidate_id | last_log_index | last_log_term
 *   RequestVote resp  (9 B)   term | vote_granted
 *
 *   AppendEntries req (variable):
 *     term | leader_id | prev_log_index | prev_log_term | leader_commit | n_entries
 *     [n_entries × (index | term | type | payload_len | payload[])]
 *   AppendEntries resp (17 B) term | success | match_index
 *
 * Entries repeat the log-file format (minus the self-check magic)
 * so the decoder can hand them straight to raft_log_append.
 */

#include "raft_rpc.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- Small endian-safe helpers ------------------------------------------ */

static inline void put_u32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static inline void put_u64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }
static inline uint32_t get_u32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint64_t get_u64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }

#define REQ_VOTE_SIZE   (8u + 8u + 8u + 8u)       /* 32 */
#define RESP_VOTE_SIZE  (8u + 1u)                 /*  9 */
#define APPEND_HDR_SIZE (8u + 8u + 8u + 8u + 8u + 4u) /* 44 */
#define APPEND_ENTRY_HDR_SIZE (8u + 8u + 4u + 4u) /* 24 per entry header */
#define RESP_APPEND_SIZE (8u + 1u + 8u)           /* 17 */

/* ---- RequestVote -------------------------------------------------------- */

int tsdb_raft_encode_req_vote(uint8_t *buf, size_t cap,
                               const tsdb_raft_req_vote_t *in)
{
    if (!buf || !in || cap < REQ_VOTE_SIZE) return -1;
    put_u64(buf +  0, in->term);
    put_u64(buf +  8, in->candidate_id);
    put_u64(buf + 16, in->last_log_index);
    put_u64(buf + 24, in->last_log_term);
    return (int)REQ_VOTE_SIZE;
}

int tsdb_raft_decode_req_vote(const uint8_t *buf, size_t len,
                               tsdb_raft_req_vote_t *out)
{
    if (!buf || !out || len < REQ_VOTE_SIZE) return -1;
    out->term           = get_u64(buf +  0);
    out->candidate_id   = get_u64(buf +  8);
    out->last_log_index = get_u64(buf + 16);
    out->last_log_term  = get_u64(buf + 24);
    return 0;
}

int tsdb_raft_encode_resp_vote(uint8_t *buf, size_t cap,
                                const tsdb_raft_resp_vote_t *in)
{
    if (!buf || !in || cap < RESP_VOTE_SIZE) return -1;
    put_u64(buf, in->term);
    buf[8] = in->vote_granted ? 1 : 0;
    return (int)RESP_VOTE_SIZE;
}

int tsdb_raft_decode_resp_vote(const uint8_t *buf, size_t len,
                                tsdb_raft_resp_vote_t *out)
{
    if (!buf || !out || len < RESP_VOTE_SIZE) return -1;
    out->term         = get_u64(buf);
    out->vote_granted = buf[8] ? 1 : 0;
    return 0;
}

/* ---- AppendEntries ------------------------------------------------------ */

size_t tsdb_raft_append_buf_cap(uint32_t n_entries, uint32_t max_payload) {
    return (size_t)APPEND_HDR_SIZE
         + (size_t)n_entries * ((size_t)APPEND_ENTRY_HDR_SIZE + (size_t)max_payload);
}

int tsdb_raft_encode_req_append(uint8_t *buf, size_t cap,
                                 const tsdb_raft_req_append_t *in)
{
    if (!buf || !in) return -1;
    if (cap < APPEND_HDR_SIZE) return -1;

    put_u64(buf +  0, in->term);
    put_u64(buf +  8, in->leader_id);
    put_u64(buf + 16, in->prev_log_index);
    put_u64(buf + 24, in->prev_log_term);
    put_u64(buf + 32, in->leader_commit);
    put_u32(buf + 40, in->n_entries);

    size_t off = APPEND_HDR_SIZE;
    for (uint32_t i = 0; i < in->n_entries; i++) {
        const tsdb_raft_entry_t *e = &in->entries[i];
        if (off + APPEND_ENTRY_HDR_SIZE + e->payload_len > cap) return -1;
        put_u64(buf + off +  0, e->index);
        put_u64(buf + off +  8, e->term);
        put_u32(buf + off + 16, e->type);
        put_u32(buf + off + 20, e->payload_len);
        if (e->payload_len > 0) {
            if (!e->payload) return -1;
            memcpy(buf + off + APPEND_ENTRY_HDR_SIZE, e->payload, e->payload_len);
        }
        off += (size_t)APPEND_ENTRY_HDR_SIZE + e->payload_len;
    }
    return (int)off;
}

int tsdb_raft_decode_req_append(const uint8_t *buf, size_t len,
                                 tsdb_raft_req_append_t *out)
{
    if (!buf || !out || len < APPEND_HDR_SIZE) return -1;
    out->term           = get_u64(buf +  0);
    out->leader_id      = get_u64(buf +  8);
    out->prev_log_index = get_u64(buf + 16);
    out->prev_log_term  = get_u64(buf + 24);
    out->leader_commit  = get_u64(buf + 32);
    out->n_entries      = get_u32(buf + 40);
    out->entries        = NULL;

    if (out->n_entries == 0) return 0;

    /* Cap on entry count to block a hostile "n_entries=2^31" crash. */
    if (out->n_entries > 65535) return -1;

    out->entries = calloc(out->n_entries, sizeof(tsdb_raft_entry_t));
    if (!out->entries) return -1;

    size_t off = APPEND_HDR_SIZE;
    for (uint32_t i = 0; i < out->n_entries; i++) {
        if (off + APPEND_ENTRY_HDR_SIZE > len) goto bad;
        tsdb_raft_entry_t *e = &out->entries[i];
        e->index       = get_u64(buf + off +  0);
        e->term        = get_u64(buf + off +  8);
        e->type        = get_u32(buf + off + 16);
        e->payload_len = get_u32(buf + off + 20);
        off += APPEND_ENTRY_HDR_SIZE;

        if (e->payload_len > 0) {
            if (e->payload_len > (1u << 20)) goto bad;
            if (off + e->payload_len > len)  goto bad;
            e->payload = malloc(e->payload_len);
            if (!e->payload) goto bad;
            memcpy(e->payload, buf + off, e->payload_len);
            off += e->payload_len;
        }
    }
    return 0;

bad:
    tsdb_raft_free_req_append(out);
    return -1;
}

void tsdb_raft_free_req_append(tsdb_raft_req_append_t *r) {
    if (!r || !r->entries) return;
    for (uint32_t i = 0; i < r->n_entries; i++) free(r->entries[i].payload);
    free(r->entries);
    r->entries = NULL;
    r->n_entries = 0;
}

int tsdb_raft_encode_resp_append(uint8_t *buf, size_t cap,
                                  const tsdb_raft_resp_append_t *in)
{
    if (!buf || !in || cap < RESP_APPEND_SIZE) return -1;
    put_u64(buf,     in->term);
    buf[8]       = in->success ? 1 : 0;
    put_u64(buf + 9, in->match_index);
    return (int)RESP_APPEND_SIZE;
}

int tsdb_raft_decode_resp_append(const uint8_t *buf, size_t len,
                                  tsdb_raft_resp_append_t *out)
{
    if (!buf || !out || len < RESP_APPEND_SIZE) return -1;
    out->term        = get_u64(buf);
    out->success     = buf[8] ? 1 : 0;
    out->match_index = get_u64(buf + 9);
    return 0;
}

/* ---- InstallSnapshot codecs --------------------------------------------
 *
 * Wire layout (all LE, no padding):
 *   req  (hdr 36 B + data):
 *     term u64 | leader_id u64 | last_idx u64 | last_term u64 |
 *     data_len u32 | data[data_len]
 *   resp (8 B): term u64
 */

#define INSTALL_HDR_SIZE (8u + 8u + 8u + 8u + 4u) /* 36 */
#define RESP_INSTALL_SIZE 8u

int tsdb_raft_encode_req_install(uint8_t *buf, size_t cap,
                                  const tsdb_raft_req_install_t *in)
{
    if (!buf || !in) return -1;
    size_t need = INSTALL_HDR_SIZE + (size_t)in->data_len;
    if (need > cap) return -1;
    put_u64(buf +  0, in->term);
    put_u64(buf +  8, in->leader_id);
    put_u64(buf + 16, in->last_included_index);
    put_u64(buf + 24, in->last_included_term);
    put_u32(buf + 32, in->data_len);
    if (in->data_len > 0) {
        if (!in->data) return -1;
        memcpy(buf + INSTALL_HDR_SIZE, in->data, in->data_len);
    }
    return (int)need;
}

int tsdb_raft_decode_req_install(const uint8_t *buf, size_t len,
                                  tsdb_raft_req_install_t *out)
{
    if (!buf || !out || len < INSTALL_HDR_SIZE) return -1;
    out->term                = get_u64(buf +  0);
    out->leader_id           = get_u64(buf +  8);
    out->last_included_index = get_u64(buf + 16);
    out->last_included_term  = get_u64(buf + 24);
    out->data_len            = get_u32(buf + 32);
    if ((size_t)INSTALL_HDR_SIZE + out->data_len > len) return -1;
    /* Data points into the caller's RPC buffer — caller must copy if
     * needed past the request lifetime. */
    out->data = out->data_len > 0 ? (buf + INSTALL_HDR_SIZE) : NULL;
    return 0;
}

int tsdb_raft_encode_resp_install(uint8_t *buf, size_t cap,
                                   const tsdb_raft_resp_install_t *in)
{
    if (!buf || !in || cap < RESP_INSTALL_SIZE) return -1;
    put_u64(buf, in->term);
    return (int)RESP_INSTALL_SIZE;
}

int tsdb_raft_decode_resp_install(const uint8_t *buf, size_t len,
                                   tsdb_raft_resp_install_t *out)
{
    if (!buf || !out || len < RESP_INSTALL_SIZE) return -1;
    out->term = get_u64(buf);
    return 0;
}

/* ---- Handler registration + dispatch ------------------------------------ */

static pthread_mutex_t             g_h_lock = PTHREAD_MUTEX_INITIALIZER;
static tsdb_raft_on_req_vote_fn    g_vote_fn    = NULL;
static tsdb_raft_on_req_append_fn  g_append_fn  = NULL;
static tsdb_raft_on_req_install_fn g_install_fn = NULL;
static void                       *g_h_ud       = NULL;

void tsdb_raft_rpc_set_handlers(tsdb_raft_on_req_vote_fn    vote_fn,
                                 tsdb_raft_on_req_append_fn  append_fn,
                                 tsdb_raft_on_req_install_fn install_fn,
                                 void *ud)
{
    pthread_mutex_lock(&g_h_lock);
    g_vote_fn    = vote_fn;
    g_append_fn  = append_fn;
    g_install_fn = install_fn;
    g_h_ud       = ud;
    pthread_mutex_unlock(&g_h_lock);
}

int tsdb_raft_rpc_handle_install(const uint8_t *req_payload, uint32_t req_len,
                                  uint8_t *resp_buf, uint32_t resp_cap,
                                  uint32_t *resp_len)
{
    if (!resp_len) return -1;
    *resp_len = 0;

    tsdb_raft_req_install_t req = {0};
    if (tsdb_raft_decode_req_install(req_payload, req_len, &req) != 0) return -1;

    pthread_mutex_lock(&g_h_lock);
    tsdb_raft_on_req_install_fn fn = g_install_fn;
    void *ud = g_h_ud;
    pthread_mutex_unlock(&g_h_lock);
    if (!fn) return -1;

    tsdb_raft_resp_install_t resp = {0};
    if (fn(ud, &req, &resp) != 0) return -1;
    int n = tsdb_raft_encode_resp_install(resp_buf, resp_cap, &resp);
    if (n < 0) return -1;
    *resp_len = (uint32_t)n;
    return 0;
}

int tsdb_raft_rpc_handle_vote(const uint8_t *req_payload, uint32_t req_len,
                               uint8_t *resp_buf, uint32_t resp_cap,
                               uint32_t *resp_len)
{
    if (!resp_len) return -1;
    *resp_len = 0;

    tsdb_raft_req_vote_t req;
    if (tsdb_raft_decode_req_vote(req_payload, req_len, &req) != 0) return -1;

    pthread_mutex_lock(&g_h_lock);
    tsdb_raft_on_req_vote_fn fn = g_vote_fn;
    void *ud = g_h_ud;
    pthread_mutex_unlock(&g_h_lock);
    if (!fn) return -1;

    tsdb_raft_resp_vote_t resp = {0};
    if (fn(ud, &req, &resp) != 0) return -1;

    int n = tsdb_raft_encode_resp_vote(resp_buf, resp_cap, &resp);
    if (n < 0) return -1;
    *resp_len = (uint32_t)n;
    return 0;
}

int tsdb_raft_rpc_handle_append(const uint8_t *req_payload, uint32_t req_len,
                                 uint8_t *resp_buf, uint32_t resp_cap,
                                 uint32_t *resp_len)
{
    if (!resp_len) return -1;
    *resp_len = 0;

    tsdb_raft_req_append_t req = {0};
    if (tsdb_raft_decode_req_append(req_payload, req_len, &req) != 0) return -1;

    pthread_mutex_lock(&g_h_lock);
    tsdb_raft_on_req_append_fn fn = g_append_fn;
    void *ud = g_h_ud;
    pthread_mutex_unlock(&g_h_lock);

    int rc = -1;
    if (fn) {
        tsdb_raft_resp_append_t resp = {0};
        if (fn(ud, &req, &resp) == 0) {
            int n = tsdb_raft_encode_resp_append(resp_buf, resp_cap, &resp);
            if (n >= 0) {
                *resp_len = (uint32_t)n;
                rc = 0;
            }
        }
    }
    tsdb_raft_free_req_append(&req);
    return rc;
}
