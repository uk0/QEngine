/* test_wb_nrows_bound.c — a WRITE_BATCH row count must be backed by payload
 * bytes.  Remote, and unauthenticated by default: the payload arrives from a
 * peer socket (rpc.c WRITE_BATCH / WRITE_BATCH_LZ / FED_INGEST).
 *
 * THE BUG.  tsdb_rpc_decode_write_batch lifts nrows off the wire as 4 raw bytes
 * and hands it on with no relation to how much payload follows.  The apply
 * path's per-column sizing loop bounds where each column STARTS and ENDS, never
 * that a column holds nrows entries: a SYMBOL column's [u32 total] only has to
 * lie inside the frame.  So a frame may declare a million rows behind 20000
 * entries.  tsdb_batch_append_bulk then walks it a block at a time, and the
 * memtable it fills is FLUSHED to a partition before the walk runs out of
 * entries — at which point the batch is refused.  The sender is told ERR while
 * the already-flushed chunks stay in the table; the engine says so itself:
 *   "batch discarded AFTER a mid-batch flush; rows already persisted to a
 *    partition cannot be rolled back"
 * A 40 KB frame therefore injects 16384 rows of fabricated content (ts 0, empty
 * symbol) into any table on the node, under a write the node reported as failed.
 *
 * THE FIX.  The decoder computes the minimum bytes one row can cost in the
 * columnar body — 8 for a fixed-width column, 2 for a SYMBOL column's shortest
 * entry — and refuses a frame whose remaining bytes cannot cover nrows of them.
 * Nothing reaches the apply path, so nothing is half-applied.
 *
 * WHAT THIS PINS (one table per case, so each assertion stands alone):
 *   [A] the 40 KB / 1000000-row frame is refused with NOTHING committed.
 *       Unfixed: applied=0 AND rows=16384.
 *   [B] no columns at all — no row costs a byte, so no row count is provable.
 *       NOTE this one is green on the unfixed tree too (see the case comment);
 *       it pins the rule, it is not a break case.
 *   [C] positive control: a well-formed batch still applies.
 */

#include "tsdb.h"
#include "../src/cluster/rpc.h"

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FATAL(fmt, ...) do { fprintf(stderr, "FATAL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, __VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FATAL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define TDIR "/tmp/tsdb_test_wb_nrows"
#define BASE 1735689600000000000LL          /* 2025-01-01 UTC ns */

static int g_fail = 0;

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        chmod(q, 0700);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static void put_u16(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static void put_u32(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}

/* Frame a WRITE_BATCH payload around a caller-supplied columnar body:
 *   [u8 tnlen][table][u32 nrows][u8 ncols][ncols type bytes][body]
 * malloc'd to EXACTLY the framed size, so a read past the body is a heap
 * overflow a sanitizer reports rather than a quiet neighbour read. */
static uint8_t *frame(const char *table, uint32_t nrows,
                      int ncols, const int *types,
                      const uint8_t *body, size_t body_len, uint32_t *out_len)
{
    size_t tn = strlen(table);
    size_t len = 1 + tn + 4 + 1 + (size_t)ncols + body_len;
    uint8_t *p = (uint8_t *)malloc(len);
    if (!p) { fprintf(stderr, "oom\n"); exit(1); }
    size_t o = 0;
    p[o++] = (uint8_t)tn;
    memcpy(p + o, table, tn); o += tn;
    put_u32(p + o, nrows); o += 4;
    p[o++] = (uint8_t)ncols;
    for (int c = 0; c < ncols; c++) p[o++] = (uint8_t)types[c];
    if (body_len) memcpy(p + o, body, body_len);
    *out_len = (uint32_t)len;
    return p;
}

static long long count_rows(tsdb_db_t *db, const char *table) {
    char q[128];
    snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
    tsdb_result_t *r = NULL;
    long long n = -1;
    if (tsdb_query(db, q, &r) == TSDB_OK && r) {
        if (tsdb_result_next(r) > 0) {
            int ci = tsdb_result_col_index_by_name(r, "count");
            if (ci < 0) ci = 0;
            n = (long long)tsdb_result_i64(r, ci);
        }
        tsdb_result_free(r);
    }
    return n;
}

static void make_table(tsdb_db_t *db, const char *name) {
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "tag", TSDB_TYPE_SYMBOL } };
    OK(tsdb_create_table(db, name, cols, 2, "ts"));
}

/* argv[1] optionally names ONE case ("A".."C") so a case can be run alone. */
int main(int argc, char **argv) {
    const char *only = (argc > 1) ? argv[1] : NULL;
#define WANT(c) (!only || !strcmp(only, (c)))

    printf("=== test_wb_nrows_bound ===\n");
    rm_rf(TDIR);
    setenv("TSDB_IDLE_FLUSH", "0", 1);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    make_table(db, "ta");
    make_table(db, "tb");
    make_table(db, "tc");

    int types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_SYMBOL };

    /* [A] one SYMBOL column carrying 20000 empty entries, nrows says a million.
     * Every byte of the frame is accounted for; only the ROW COUNT is a lie. */
    if (WANT("A")) {
        size_t nent = 20000;
        size_t blen = 4 + 2 * nent;
        uint8_t *body = (uint8_t *)calloc(1, blen);
        if (!body) { fprintf(stderr, "oom\n"); exit(1); }
        put_u32(body, (uint32_t)(2 * nent));    /* entries are [u16 len = 0] */
        int sym_only[1] = { TSDB_TYPE_SYMBOL };
        uint32_t plen = 0;
        uint8_t *p = frame("ta", 1000000, 1, sym_only, body, blen, &plen);
        int applied = tsdb_rpc_apply_write_batch_for_test(db, p, plen);
        long long rows = count_rows(db, "ta");
        CHECK(applied == 0 && rows == 0,
              "[A] %u-byte frame declaring 1000000 rows refused with NOTHING "
              "committed (applied=%d want 0, rows=%lld want 0)",
              plen, applied, rows);
        free(p); free(body);
    }

    /* [B] no columns at all, so no row costs a single byte.  NOTE — the unfixed
     * tree also refuses this one, not by any bound but because the memtable
     * rejects a wire batch carrying fewer data columns than the schema
     * (memtable.c: `data_idx >= ncols_data`).  It pins the rule; it is not the
     * break case. */
    if (WANT("B")) {
        uint32_t plen = 0;
        uint8_t *p = frame("tb", 0x7FFFFFFFu, 0, NULL, NULL, 0, &plen);
        int applied = tsdb_rpc_apply_write_batch_for_test(db, p, plen);
        long long rows = count_rows(db, "tb");
        CHECK(applied == 0 && rows == 0,
              "[B] %u-byte frame declaring INT32_MAX rows with no columns "
              "refused (applied=%d want 0, rows=%lld want 0)",
              plen, applied, rows);
        free(p);
    }

    /* [C] positive control: two well-formed rows still apply, so the bound is
     * not "reject everything". */
    if (WANT("C")) {
        uint8_t body[2 * 8 + 4 + 10];
        for (int i = 0; i < 2; i++) {
            int64_t ts = BASE + i;
            memcpy(body + i * 8, &ts, 8);
        }
        put_u32(body + 16, 10);
        put_u16(body + 20, 3); memcpy(body + 22, "aaa", 3);
        put_u16(body + 25, 3); memcpy(body + 27, "bbb", 3);
        uint32_t plen = 0;
        uint8_t *p = frame("tc", 2, 2, types, body, sizeof(body), &plen);
        int applied = tsdb_rpc_apply_write_batch_for_test(db, p, plen);
        long long rows = count_rows(db, "tc");
        CHECK(applied == 1 && rows == 2,
              "[C] well-formed 2-row batch still applies (applied=%d want 1, "
              "rows=%lld want 2)", applied, rows);
        free(p);
    }

    tsdb_close(db);
    rm_rf(TDIR);
#undef WANT
    printf(g_fail ? "\n=== FAILED (%d) ===\n" : "\n=== PASSED ===\n", g_fail);
    return g_fail ? 1 : 0;
}
