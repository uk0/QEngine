/* test_raft_rpc.c — wire-format round-trip for Raft RPCs.
 *
 * The goal here is narrow: confirm every field survives encode →
 * decode with the exact bytes on the wire, including zero-length
 * edge cases (heartbeat = AppendEntries with n_entries=0) and the
 * tamper path where a truncated buffer surfaces as a decode error.
 */

#include "../src/raft/raft_rpc.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PASS(desc) printf("PASS: %s\n", desc)

static void t_vote(void) {
    printf("\n[1] RequestVote round-trip\n");
    tsdb_raft_req_vote_t in  = { .term = 42,
                                  .candidate_id   = 0xDEADBEEFCAFE,
                                  .last_log_index = 1234,
                                  .last_log_term  = 41 };
    uint8_t buf[64];
    int n = tsdb_raft_encode_req_vote(buf, sizeof(buf), &in);
    assert(n == 32);

    tsdb_raft_req_vote_t out;
    assert(tsdb_raft_decode_req_vote(buf, (size_t)n, &out) == 0);
    assert(out.term == in.term);
    assert(out.candidate_id == in.candidate_id);
    assert(out.last_log_index == in.last_log_index);
    assert(out.last_log_term == in.last_log_term);

    /* Truncated input fails cleanly. */
    assert(tsdb_raft_decode_req_vote(buf, 16, &out) == -1);

    /* Response round-trip. */
    tsdb_raft_resp_vote_t rin = { .term = 42, .vote_granted = 1 };
    n = tsdb_raft_encode_resp_vote(buf, sizeof(buf), &rin);
    assert(n == 9);
    tsdb_raft_resp_vote_t rout;
    assert(tsdb_raft_decode_resp_vote(buf, (size_t)n, &rout) == 0);
    assert(rout.term == 42 && rout.vote_granted == 1);
    PASS("RequestVote req+resp + truncation");
}

static void t_append_heartbeat(void) {
    printf("\n[2] AppendEntries heartbeat (n_entries=0)\n");
    tsdb_raft_req_append_t in = {
        .term = 7, .leader_id = 1,
        .prev_log_index = 5, .prev_log_term = 4,
        .leader_commit = 3, .n_entries = 0, .entries = NULL
    };
    uint8_t buf[128];
    int n = tsdb_raft_encode_req_append(buf, sizeof(buf), &in);
    assert(n == 44); /* 5*8 + 4 */

    tsdb_raft_req_append_t out = {0};
    assert(tsdb_raft_decode_req_append(buf, (size_t)n, &out) == 0);
    assert(out.term == 7 && out.leader_id == 1);
    assert(out.prev_log_index == 5 && out.prev_log_term == 4);
    assert(out.leader_commit == 3);
    assert(out.n_entries == 0);
    assert(out.entries == NULL);
    tsdb_raft_free_req_append(&out);
    PASS("heartbeat body matches");
}

static void t_append_with_entries(void) {
    printf("\n[3] AppendEntries with 2 entries\n");
    uint8_t p1[] = "CREATE DATABASE foo";
    uint8_t p2[] = "CREATE TABLE t (ts TIMESTAMP, v FLOAT64) TIMESTAMP(ts)";
    tsdb_raft_entry_t ents[2] = {
        { .index = 10, .term = 3, .type = 1,
          .payload_len = (uint32_t)strlen((char*)p1), .payload = p1 },
        { .index = 11, .term = 3, .type = 1,
          .payload_len = (uint32_t)strlen((char*)p2), .payload = p2 },
    };
    tsdb_raft_req_append_t in = {
        .term = 3, .leader_id = 2,
        .prev_log_index = 9, .prev_log_term = 3,
        .leader_commit = 9, .n_entries = 2, .entries = ents
    };

    size_t need = tsdb_raft_append_buf_cap(2, 128);
    uint8_t *buf = malloc(need);
    int n = tsdb_raft_encode_req_append(buf, need, &in);
    assert(n > 0);

    tsdb_raft_req_append_t out = {0};
    assert(tsdb_raft_decode_req_append(buf, (size_t)n, &out) == 0);
    assert(out.n_entries == 2);
    assert(out.entries[0].index == 10 && out.entries[0].term == 3);
    assert(out.entries[0].payload_len == strlen((char*)p1));
    assert(memcmp(out.entries[0].payload, p1, strlen((char*)p1)) == 0);
    assert(out.entries[1].index == 11);
    assert(out.entries[1].payload_len == strlen((char*)p2));
    assert(memcmp(out.entries[1].payload, p2, strlen((char*)p2)) == 0);

    tsdb_raft_free_req_append(&out);
    free(buf);
    PASS("two entries survive encode/decode");

    /* Truncation after header should fail. */
    buf = malloc(need);
    n = tsdb_raft_encode_req_append(buf, need, &in);
    tsdb_raft_req_append_t bad = {0};
    assert(tsdb_raft_decode_req_append(buf, 44 + 5, &bad) == -1);
    tsdb_raft_free_req_append(&bad);
    free(buf);
    PASS("truncated AppendEntries returns -1");
}

static void t_resp_append(void) {
    printf("\n[4] AppendEntries response round-trip\n");
    tsdb_raft_resp_append_t in = {
        .term = 77, .success = 1, .match_index = 12345
    };
    uint8_t buf[32];
    int n = tsdb_raft_encode_resp_append(buf, sizeof(buf), &in);
    assert(n == 17);
    tsdb_raft_resp_append_t out;
    assert(tsdb_raft_decode_resp_append(buf, (size_t)n, &out) == 0);
    assert(out.term == 77 && out.success == 1 && out.match_index == 12345);
    PASS("append response round-trip");
}

int main(void) {
    t_vote();
    t_append_heartbeat();
    t_append_with_entries();
    t_resp_append();
    printf("\n=== all raft_rpc codec tests passed ===\n");
    return 0;
}
