/* raft_rpc.h — on-wire payloads for the two Raft-specific RPC types
 * (TSDB_RPC_RAFT_REQUEST_VOTE, TSDB_RPC_RAFT_APPEND_ENTRIES) plus the
 * response bodies the server returns inside an ACK frame.
 *
 * Splitting these into a dedicated header keeps the generic RPC
 * framework (rpc.h) unaware of Raft semantics.  The raft state
 * machine (raft.c) owns request origination and response parsing;
 * the RPC server (rpc.c) calls a registered dispatcher to hand
 * arriving RequestVote / AppendEntries messages off to the state
 * machine.
 *
 * All integers are little-endian, same convention as the rest of the
 * cluster wire protocol.  Node ids are u64; signed types are avoided
 * because Raft term/index are naturally unsigned.
 */
#ifndef TSDB_RAFT_RPC_H
#define TSDB_RAFT_RPC_H

#include <stdint.h>
#include <stddef.h>

#include "raft_log.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ---- RequestVote ---------------------------------------------------------
 *
 * Candidate asks peer "am I fit to be leader?"  The peer grants a vote
 * iff:
 *   - candidate's term is >= our current term (we bump if strictly >);
 *   - we haven't already voted this term, or voted for the same candidate;
 *   - candidate's log is at least as up-to-date as ours (§5.4.1).
 */
typedef struct {
    uint64_t term;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
} tsdb_raft_req_vote_t;

typedef struct {
    uint64_t term;
    uint8_t  vote_granted;  /* 0 = no, 1 = yes */
} tsdb_raft_resp_vote_t;

int tsdb_raft_encode_req_vote (uint8_t *buf, size_t cap,
                                const tsdb_raft_req_vote_t *in);
int tsdb_raft_decode_req_vote (const uint8_t *buf, size_t len,
                                tsdb_raft_req_vote_t *out);

int tsdb_raft_encode_resp_vote(uint8_t *buf, size_t cap,
                                const tsdb_raft_resp_vote_t *in);
int tsdb_raft_decode_resp_vote(const uint8_t *buf, size_t len,
                                tsdb_raft_resp_vote_t *out);

/* ---- AppendEntries -------------------------------------------------------
 *
 * Heartbeat (entries=0) or log replication.  `prev_log_index` /
 * `prev_log_term` are the consistency check: the follower accepts only
 * if its log already has that entry.  Conflicting entries from
 * prev_log_index+1 onward are overwritten by `entries`.
 */
typedef struct {
    uint64_t term;
    uint64_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    uint32_t n_entries;
    tsdb_raft_entry_t *entries;   /* caller-owned; payloads point into
                                      caller-supplied buffers */
} tsdb_raft_req_append_t;

typedef struct {
    uint64_t term;
    uint8_t  success;
    /* Hint used by the leader to speed up nextIndex rollback.  On
     * success it equals prev_log_index + n_entries; on failure it
     * reports the follower's last_log_index. */
    uint64_t match_index;
} tsdb_raft_resp_append_t;

/* Upper bound on bytes needed to encode an AppendEntries request
 * carrying up to `n` entries of up to `p` payload bytes each. */
size_t tsdb_raft_append_buf_cap(uint32_t n_entries, uint32_t max_payload);

int tsdb_raft_encode_req_append(uint8_t *buf, size_t cap,
                                 const tsdb_raft_req_append_t *in);
/* Decoder populates *out.  Entries are allocated with malloc() and
 * must be freed via tsdb_raft_free_req_append. */
int tsdb_raft_decode_req_append(const uint8_t *buf, size_t len,
                                 tsdb_raft_req_append_t *out);
void tsdb_raft_free_req_append(tsdb_raft_req_append_t *r);

int tsdb_raft_encode_resp_append(uint8_t *buf, size_t cap,
                                  const tsdb_raft_resp_append_t *in);
int tsdb_raft_decode_resp_append(const uint8_t *buf, size_t len,
                                  tsdb_raft_resp_append_t *out);

/* ---- InstallSnapshot -----------------------------------------------------
 *
 * Sent when a follower's next_index is already below the leader's
 * snap_index — i.e. the entries the follower needs were compacted
 * away.  The leader streams the snapshot body in 64 KB chunks (§7 of
 * the thesis); `offset` names the starting byte of the current chunk
 * within the full body, and `done==1` on the final chunk.  Chunks are
 * sent sequentially with wait-for-ack so the follower can buffer
 * partials in one receive file; the on-disk BODY format produced by
 * the snapshot-write callback (num_files | name | content_len |
 * content) is intentionally unchanged.
 */
typedef struct {
    uint64_t term;
    uint64_t leader_id;
    uint64_t last_included_index;
    uint64_t last_included_term;
    uint32_t offset;              /* byte offset of this chunk in body */
    uint8_t  done;                /* 1 = final chunk, 0 = more coming */
    uint32_t data_len;            /* bytes of data in THIS chunk */
    const uint8_t *data;          /* borrowed from caller buffer */
} tsdb_raft_req_install_t;

typedef struct {
    uint64_t term;
} tsdb_raft_resp_install_t;

int tsdb_raft_encode_req_install(uint8_t *buf, size_t cap,
                                  const tsdb_raft_req_install_t *in);
int tsdb_raft_decode_req_install(const uint8_t *buf, size_t len,
                                  tsdb_raft_req_install_t *out);
int tsdb_raft_encode_resp_install(uint8_t *buf, size_t cap,
                                   const tsdb_raft_resp_install_t *in);
int tsdb_raft_decode_resp_install(const uint8_t *buf, size_t len,
                                   tsdb_raft_resp_install_t *out);

/* ---- Server-side dispatcher ---------------------------------------------
 *
 * The RPC server, upon receiving REQUEST_VOTE / APPEND_ENTRIES, calls
 * the registered handler.  Handler fills the response struct; the
 * server encodes and replies.  If no handler is registered (role=data
 * or raft disabled) the server responds with TSDB_RPC_ERR.
 *
 * The handler runs on the RPC thread — keep it short or hand off.
 */
typedef int (*tsdb_raft_on_req_vote_fn)(void *ud,
                                        const tsdb_raft_req_vote_t *req,
                                        tsdb_raft_resp_vote_t *resp);
typedef int (*tsdb_raft_on_req_append_fn)(void *ud,
                                          const tsdb_raft_req_append_t *req,
                                          tsdb_raft_resp_append_t *resp);
typedef int (*tsdb_raft_on_req_install_fn)(void *ud,
                                           const tsdb_raft_req_install_t *req,
                                           tsdb_raft_resp_install_t *resp);
/* PreVote uses the same payload struct as RequestVote — it's the same
 * question minus the persistence. */
typedef int (*tsdb_raft_on_req_pre_vote_fn)(void *ud,
                                             const tsdb_raft_req_vote_t *req,
                                             tsdb_raft_resp_vote_t *resp);

void tsdb_raft_rpc_set_handlers(tsdb_raft_on_req_vote_fn    vote_fn,
                                 tsdb_raft_on_req_append_fn  append_fn,
                                 tsdb_raft_on_req_install_fn install_fn,
                                 tsdb_raft_on_req_pre_vote_fn pre_vote_fn,
                                 void *ud);

/* Called by rpc.c when a RAFT_* frame arrives.  Returns 0 if the
 * response was filled in (resp buffer usable up to *resp_len), or -1
 * if the handler is unregistered / decoding failed. */
int tsdb_raft_rpc_handle_vote  (const uint8_t *req_payload, uint32_t req_len,
                                uint8_t *resp_buf, uint32_t resp_cap,
                                uint32_t *resp_len);
int tsdb_raft_rpc_handle_append(const uint8_t *req_payload, uint32_t req_len,
                                uint8_t *resp_buf, uint32_t resp_cap,
                                uint32_t *resp_len);
int tsdb_raft_rpc_handle_install(const uint8_t *req_payload, uint32_t req_len,
                                  uint8_t *resp_buf, uint32_t resp_cap,
                                  uint32_t *resp_len);
int tsdb_raft_rpc_handle_pre_vote(const uint8_t *req_payload, uint32_t req_len,
                                   uint8_t *resp_buf, uint32_t resp_cap,
                                   uint32_t *resp_len);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_RAFT_RPC_H */
