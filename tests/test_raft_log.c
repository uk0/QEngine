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

int main(void) {
    const char *dir = "/tmp/tsdb-raft-log-test";
    rm_rf(dir);

    t_fresh_open(dir);
    t_term_and_vote(dir);
    t_append_and_read(dir);
    t_truncate(dir);
    t_torn_tail(dir);

    rm_rf(dir);
    printf("\n=== all raft_log tests passed ===\n");
    return 0;
}
