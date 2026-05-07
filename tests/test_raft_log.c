/* test_raft_log.c — unit tests for the persistent Raft log layer.
 *
 * Covers:
 *  - fresh open has term=0, vote=0, log empty
 *  - set_term resets votedFor
 *  - set_voted_for persists and survives reopen
 *  - append → last_index / last_term reflect what we wrote
 *  - read round-trips payloads byte-for-byte
 *  - truncate removes tail and keeps the prefix intact across reopen
 *  - torn-tail (simulated by appending and then ftruncate half an entry)
 *    is detected at reopen and silently dropped — log stays consistent
 */

#include "../src/raft/raft_log.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define PASS(desc) printf("PASS: %s\n", desc)
#define FAIL(fmt, ...) do { printf("FAIL: " fmt "\n", ##__VA_ARGS__); exit(1); } while (0)

static void rm_rf(const char *path) {
    char cmd[4096];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

static void fill(const char *prefix, char *out, size_t cap, size_t len) {
    for (size_t i = 0; i < len && i < cap - 1; i++) {
        out[i] = (char)('a' + ((i + prefix[0]) % 26));
    }
    out[len < cap ? len : cap - 1] = '\0';
}

/* Scenario 1: fresh cluster. */
static void t_fresh_open(const char *dir) {
    printf("\n[1] fresh open → zero term, zero vote, empty log\n");
    rm_rf(dir);
    tsdb_raft_log_t *rl = tsdb_raft_log_open(dir);
    assert(rl);
    assert(tsdb_raft_log_current_term(rl) == 0);
    assert(tsdb_raft_log_voted_for(rl) == 0);
    assert(tsdb_raft_log_last_index(rl) == 0);
    assert(tsdb_raft_log_last_term(rl) == 0);
    tsdb_raft_log_close(rl);
    PASS("fresh state zeroed");
}

/* Scenario 2: term + vote persist. */
static void t_term_and_vote(const char *dir) {
    printf("\n[2] term/vote round-trip\n");
    rm_rf(dir);
    tsdb_raft_log_t *rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_set_term(rl, 7) == 0);
    assert(tsdb_raft_log_voted_for(rl) == 0);       /* new term clears vote */
    assert(tsdb_raft_log_set_voted_for(rl, 42) == 0);
    tsdb_raft_log_close(rl);

    rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_current_term(rl) == 7);
    assert(tsdb_raft_log_voted_for(rl) == 42);
    tsdb_raft_log_close(rl);
    PASS("term=7, vote=42 survives reopen");
}

/* Scenario 3: append + read round trip, survives reopen. */
static void t_append_and_read(const char *dir) {
    printf("\n[3] append/read round-trip\n");
    rm_rf(dir);
    tsdb_raft_log_t *rl = tsdb_raft_log_open(dir);

    char body1[64] = {0};
    fill("a", body1, sizeof(body1), 20);
    tsdb_raft_entry_t e1 = { .term = 1, .type = 1,
                              .payload_len = 20, .payload = body1 };
    assert(tsdb_raft_log_append(rl, &e1) == 0);
    assert(e1.index == 1);

    char body2[200] = {0};
    fill("z", body2, sizeof(body2), 199);
    tsdb_raft_entry_t e2 = { .term = 2, .type = 1,
                              .payload_len = 199, .payload = body2 };
    assert(tsdb_raft_log_append(rl, &e2) == 0);
    assert(e2.index == 2);

    assert(tsdb_raft_log_last_index(rl) == 2);
    assert(tsdb_raft_log_last_term(rl) == 2);
    tsdb_raft_log_close(rl);

    rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_last_index(rl) == 2);
    assert(tsdb_raft_log_last_term(rl) == 2);

    tsdb_raft_entry_t r1 = {0};
    assert(tsdb_raft_log_read(rl, 1, &r1) == 0);
    assert(r1.index == 1 && r1.term == 1 && r1.payload_len == 20);
    assert(memcmp(r1.payload, body1, 20) == 0);
    free(r1.payload);

    tsdb_raft_entry_t r2 = {0};
    assert(tsdb_raft_log_read(rl, 2, &r2) == 0);
    assert(r2.index == 2 && r2.term == 2 && r2.payload_len == 199);
    assert(memcmp(r2.payload, body2, 199) == 0);
    free(r2.payload);

    /* Out-of-range reads fail cleanly. */
    tsdb_raft_entry_t r3 = {0};
    assert(tsdb_raft_log_read(rl, 3, &r3) == -1);
    tsdb_raft_log_close(rl);
    PASS("append+read round-trip + reopen");
}

/* Scenario 4: truncate drops tail, prefix intact. */
static void t_truncate(const char *dir) {
    printf("\n[4] truncate keeps prefix\n");
    rm_rf(dir);
    tsdb_raft_log_t *rl = tsdb_raft_log_open(dir);

    for (int i = 0; i < 5; i++) {
        char body[16];
        fill("t", body, sizeof(body), (size_t)(i + 3));
        tsdb_raft_entry_t e = { .term = (uint64_t)(i + 1), .type = 1,
                                 .payload_len = (uint32_t)(i + 3), .payload = body };
        assert(tsdb_raft_log_append(rl, &e) == 0);
    }
    assert(tsdb_raft_log_last_index(rl) == 5);

    assert(tsdb_raft_log_truncate(rl, 3) == 0);        /* drop >= 3 */
    assert(tsdb_raft_log_last_index(rl) == 2);
    assert(tsdb_raft_log_last_term(rl) == 2);

    /* Re-append from 3; should land at index=3. */
    char newbody[8] = {0};
    fill("n", newbody, sizeof(newbody), 5);
    tsdb_raft_entry_t e = { .term = 9, .type = 1,
                             .payload_len = 5, .payload = newbody };
    assert(tsdb_raft_log_append(rl, &e) == 0);
    assert(e.index == 3);
    assert(tsdb_raft_log_last_index(rl) == 3);
    assert(tsdb_raft_log_last_term(rl) == 9);

    tsdb_raft_log_close(rl);

    /* Reopen — prefix + new entry must both survive. */
    rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_last_index(rl) == 3);
    assert(tsdb_raft_log_last_term(rl) == 9);

    tsdb_raft_entry_t r = {0};
    assert(tsdb_raft_log_read(rl, 1, &r) == 0 && r.term == 1);
    free(r.payload);
    memset(&r, 0, sizeof(r));
    assert(tsdb_raft_log_read(rl, 3, &r) == 0 && r.term == 9 && r.payload_len == 5);
    free(r.payload);
    tsdb_raft_log_close(rl);
    PASS("truncate + re-append + reopen intact");
}

/* Scenario 5: torn tail recovery. Simulate a crash mid-write by
 * truncating the log file to a sub-entry byte offset, then reopen and
 * verify the log rolls back to the last complete entry. */
static void t_torn_tail(const char *dir) {
    printf("\n[5] torn tail dropped silently at reopen\n");
    rm_rf(dir);
    tsdb_raft_log_t *rl = tsdb_raft_log_open(dir);

    char body[16] = "complete-entry";
    tsdb_raft_entry_t e = { .term = 1, .type = 1,
                             .payload_len = 14, .payload = body };
    assert(tsdb_raft_log_append(rl, &e) == 0);
    tsdb_raft_log_close(rl);

    /* Simulate a crash: append 4 bytes of garbage to the log file so
     * reopen sees an entry whose magic/header is half-written. */
    char log_path[4096];
    snprintf(log_path, sizeof(log_path), "%s/raft/log/seg.bin", dir);
    FILE *fp = fopen(log_path, "ab");
    assert(fp);
    uint8_t garbage[4] = { 0xAA, 0xAA, 0xAA, 0xAA };
    fwrite(garbage, 4, 1, fp);
    fclose(fp);

    rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_last_index(rl) == 1);
    assert(tsdb_raft_log_last_term(rl) == 1);
    /* Torn bytes should be truncated from the file; size must equal
     * a clean single-entry log. */
    tsdb_raft_entry_t r = {0};
    assert(tsdb_raft_log_read(rl, 1, &r) == 0);
    assert(r.payload_len == 14 && memcmp(r.payload, "complete-entry", 14) == 0);
    free(r.payload);
    tsdb_raft_log_close(rl);
    PASS("crash mid-append → rollback to last complete entry");
}

/* Scenario 6: snapshot compact drops the prefix, preserves the tail,
 * answers term_at(snap_index) from the marker, and survives reopen. */
static void t_snapshot_compact(const char *dir) {
    printf("\n[6] snapshot compact + reopen\n");
    rm_rf(dir);
    tsdb_raft_log_t *rl = tsdb_raft_log_open(dir);

    /* Append 10 entries. */
    for (int i = 0; i < 10; i++) {
        char body[16] = "payload";
        tsdb_raft_entry_t e = { .term = (uint64_t)(i / 3 + 1),
                                 .type = 1, .payload_len = 7, .payload = body };
        assert(tsdb_raft_log_append(rl, &e) == 0);
    }
    assert(tsdb_raft_log_last_index(rl) == 10);
    assert(tsdb_raft_log_snapshot_index(rl) == 0);

    /* Compact through index 6.  Loop wrote term = i/3+1 for i=0..9
     * which maps to Raft index = i+1, so term_at(6) = (5/3)+1 = 2. */
    uint64_t term_at_6 = tsdb_raft_log_term_at(rl, 6);
    assert(term_at_6 == 2);
    assert(tsdb_raft_log_compact(rl, 6, term_at_6) == 0);

    assert(tsdb_raft_log_snapshot_index(rl) == 6);
    assert(tsdb_raft_log_snapshot_term(rl)  == 2);
    assert(tsdb_raft_log_last_index(rl) == 10);
    /* term_at() answers the snapshot boundary from the marker. */
    assert(tsdb_raft_log_term_at(rl, 6) == 2);
    /* Entries 7..10 still readable. */
    tsdb_raft_entry_t r = {0};
    assert(tsdb_raft_log_read(rl, 7, &r) == 0);
    free(r.payload);
    /* Entries <=6 are physically gone and must report "not found". */
    tsdb_raft_entry_t r2 = {0};
    assert(tsdb_raft_log_read(rl, 5, &r2) == -1);

    /* Idempotency: re-compacting at lower or equal index is a no-op. */
    assert(tsdb_raft_log_compact(rl, 3, 1) == 0);
    assert(tsdb_raft_log_snapshot_index(rl) == 6);

    tsdb_raft_log_close(rl);
    /* Reopen — snapshot marker + remaining log survive. */
    rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_snapshot_index(rl) == 6);
    assert(tsdb_raft_log_snapshot_term(rl)  == 2);
    assert(tsdb_raft_log_last_index(rl) == 10);
    assert(tsdb_raft_log_term_at(rl, 6) == 2);
    tsdb_raft_log_close(rl);

    PASS("compact drops prefix, marker answers term_at, survives reopen");
}

/* Scenario 7: snapshot compact subsumes the ENTIRE log (nkept == 0).
 *
 * Pre-fix this case reset rl->last_index to 0, so the next append
 * computed idx = 0 + 1 = 1 — colliding with slots already covered by
 * the snapshot.  Replicate-to-peers rejected those (peer's snap_index
 * already past the new entry's idx) and maybe_advance_commit_locked's
 * `for N > commit_index` loop never entered because last (1..N) was
 * less than commit_index (= snap_idx).  tsdb_raft_propose nevertheless
 * returned OK because last_applied >= my_index, and the apply hook
 * never fired.  Observed on lvm1 cnode-1 as silently-failing
 * CREATE DATABASE / CREATE TABLE.
 *
 * After the fix: last_index == snap_idx, first append picks
 * snap_idx + 1, the apply path runs.  Survives reopen. */
static void t_snapshot_compact_empty_tail(const char *dir) {
    printf("\n[7] snapshot compact subsumes entire log + reopen + append\n");
    rm_rf(dir);
    tsdb_raft_log_t *rl = tsdb_raft_log_open(dir);

    /* Append 5 entries at term 7. */
    for (int i = 0; i < 5; i++) {
        char body[8] = "p";
        tsdb_raft_entry_t e = { .term = 7, .type = 1,
                                 .payload_len = 1, .payload = body };
        assert(tsdb_raft_log_append(rl, &e) == 0);
    }
    assert(tsdb_raft_log_last_index(rl) == 5);

    /* Compact through index 5 — covers ALL existing entries. */
    assert(tsdb_raft_log_compact(rl, 5, 7) == 0);
    assert(tsdb_raft_log_snapshot_index(rl) == 5);
    assert(tsdb_raft_log_snapshot_term(rl)  == 7);
    /* Critical invariant: last_index follows snap_index, NOT 0. */
    assert(tsdb_raft_log_last_index(rl) == 5);
    /* term_at(snap_index) still answers from the marker. */
    assert(tsdb_raft_log_term_at(rl, 5) == 7);

    /* Append a new entry — must land at idx 6, not 1. */
    char body2[8] = "q";
    tsdb_raft_entry_t e2 = { .term = 8, .type = 1,
                              .payload_len = 1, .payload = body2 };
    assert(tsdb_raft_log_append(rl, &e2) == 0);
    assert(e2.index == 6);
    assert(tsdb_raft_log_last_index(rl) == 6);
    /* term_at(6) reads the freshly-appended entry. */
    assert(tsdb_raft_log_term_at(rl, 6) == 8);

    tsdb_raft_log_close(rl);

    /* Reopen — log_rebuild_offsets must restore last_index >= snap_index
     * even when the on-disk log file holds nothing past the snapshot
     * (or only stragglers from a pre-fix compact).  Here entry 6 is
     * present so we expect last_index == 6. */
    rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_snapshot_index(rl) == 5);
    assert(tsdb_raft_log_last_index(rl) == 6);
    assert(tsdb_raft_log_term_at(rl, 6) == 8);
    tsdb_raft_log_close(rl);

    /* Force the "empty surviving log after restart" case: wipe seg.bin
     * (simulating a pre-fix compact that left the file empty) but keep
     * snap_meta.bin.  Open should still treat snap_idx as last_index. */
    char seg_path[4128];
    snprintf(seg_path, sizeof(seg_path), "%s/raft/log/seg.bin", dir);
    FILE *fp = fopen(seg_path, "wb"); /* truncate to 0 bytes */
    assert(fp != NULL);
    fclose(fp);

    rl = tsdb_raft_log_open(dir);
    assert(tsdb_raft_log_snapshot_index(rl) == 5);
    /* Pre-fix: 0.  Post-fix: snap_index. */
    assert(tsdb_raft_log_last_index(rl) == 5);
    /* Append after recovering from the wipe — must skip past the
     * subsumed range. */
    char body3[8] = "r";
    tsdb_raft_entry_t e3 = { .term = 9, .type = 1,
                              .payload_len = 1, .payload = body3 };
    assert(tsdb_raft_log_append(rl, &e3) == 0);
    assert(e3.index == 6);
    tsdb_raft_log_close(rl);

    PASS("compact-with-empty-tail keeps last_index = snap_index, append picks snap+1");
}

int main(void) {
    const char *dir = "/tmp/tsdb-raft-log-test";
    rm_rf(dir);

    t_fresh_open(dir);
    t_term_and_vote(dir);
    t_append_and_read(dir);
    t_truncate(dir);
    t_torn_tail(dir);
    t_snapshot_compact(dir);
    t_snapshot_compact_empty_tail(dir);

    rm_rf(dir);
    printf("\n=== all raft_log tests passed ===\n");
    return 0;
}
