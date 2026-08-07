/* test_write_batch_bounds.c — a WRITE_BATCH frame must never make the receiver
 * read outside it.  Remote, unauthenticated: the payload arrives from a peer
 * socket (rpc.c WRITE_BATCH / WRITE_BATCH_LZ / FED_INGEST).
 *
 * THE BUG.  Two holes, one per layer:
 *
 *  1. rpc.c's per-column sizing loop read the SYMBOL column's [u32 total]
 *     header before checking the header was inside the payload, and then did
 *     `boff += 4 + (int)total`.  For total > INT_MAX that conversion is
 *     NEGATIVE, so boff SHRANK and the `boff > payload_len` bound passed.  The
 *     bound was also the wrong reference: boff is an offset into the columnar
 *     body, compared against the length of the WHOLE payload — so it tolerated
 *     an overshoot of exactly the header ([u8 tnlen][table][u32 nrows][u8
 *     ncols][types]), which the sender controls via the table name.
 *
 *  2. db.c's tsdb_batch_append_bulk then walked the symbol column
 *     (`memcpy(&l16, cur, 2); cur += 2 + l16;`) with NO end at all, so an
 *     entry length could march the cursor off the end of the frame.
 *
 * THE FIX.  rpc.c bounds every read against the payload in size_t arithmetic
 * (the shape write_batch_incarnation already used), and db.c walks within the
 * column's own declared [u32 total] — which the fixed rpc.c has by then proved
 * lies inside the payload.  Neither layer alone is enough: case C below passes
 * rpc.c's sizing and is stopped only by db.c's bound.
 *
 * RUN IT UNDER ASan.  Every case asserts the post-fix contract — the frame is
 * REJECTED and not one row lands — which is deterministic in any build.  The
 * memory error itself is only deterministic under a sanitizer: in a plain build
 * the out-of-bounds read returns whatever follows the allocation, which may or
 * may not trip a downstream check, so a plain run can pass VACUOUSLY on the
 * unfixed tree.  scripts/asan-build.sh && scripts/asan-run.sh
 * test_write_batch_bounds is the run that proves memory safety.
 */

#include "tsdb.h"
#include "../src/cluster/rpc.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define TDIR "/tmp/tsdb_test_wb_bounds"
#define BASE 1735689600000000000LL          /* 2025-01-01 UTC ns */

/* 57 chars: fits the receiver's char[64] table_name, and makes the frame header
 * long enough that an offset bounded by payload_len alone overshoots the
 * columnar body by 57 bytes. */
#define LONGNAME "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

static int g_fail = 0;

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

/*
 * Frame a WRITE_BATCH payload around a caller-supplied columnar body:
 *   [u8 tnlen][table][u32 nrows][u8 ncols][ncols type bytes][body]
 * malloc'd to EXACTLY the framed size — a walk that runs one byte past the
 * body is then a heap overflow the sanitizer reports, not a quiet read of
 * whatever the allocator happens to keep next door.
 */
static uint8_t *frame(const char *table, uint32_t nrows,
                      int ncols, const int *types,
                      const uint8_t *body, size_t body_len, uint32_t *out_len)
{
    size_t tn = strlen(table);
    size_t len = 1 + tn + 4 + 1 + (size_t)ncols + body_len;
    uint8_t *p = (uint8_t *)malloc(len);
    if (!p) FAIL("oom");
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

static int count_rows(tsdb_db_t *db, const char *table) {
    char q[256];
    snprintf(q, sizeof(q), "SELECT ts FROM %s", table);
    tsdb_result_t *r = NULL;
    int n = 0;
    if (tsdb_query(db, q, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

/* A malformed frame must be refused (ACK=0) and leave the table untouched. */
static void expect_rejected(tsdb_db_t *db, const char *table, const char *what,
                            uint8_t *payload, uint32_t plen, int rows_before)
{
    int applied = tsdb_rpc_apply_write_batch_for_test(db, payload, plen);
    int rows = count_rows(db, table);
    if (applied != 0 || rows != rows_before) {
        fprintf(stderr, "FAIL %s:%d: %s: applied=%d (want 0) rows=%d (want %d)\n",
                __FILE__, __LINE__, what, applied, rows, rows_before);
        g_fail++;
    } else {
        printf("  ok: %s rejected, %d rows (unchanged)\n", what, rows);
    }
    free(payload);
}

/* argv[1] optionally names ONE case ("A".."D").  A sanitizer report ABORTS the
 * process at the first hit, so proving the unfixed tree wrong in each case
 * needs each case runnable on its own. */
int main(int argc, char **argv) {
    const char *only = (argc > 1) ? argv[1] : NULL;
#define WANT(c) (!only || !strcmp(only, (c)))

    printf("=== test_write_batch_bounds ===\n");
    rm_rf(TDIR);
    setenv("TSDB_IDLE_FLUSH", "0", 1);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "tag", TSDB_TYPE_SYMBOL } };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    OK(tsdb_create_table(db, LONGNAME, cols, 2, "ts"));

    int types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_SYMBOL };

    /* [A] the symbol column's declared size, read as a signed int, is NEGATIVE.
     * Body: 64 ts values, then a bare [u32 total=0xFFFFFFF0] and nothing else.
     * `boff += 4 + (int)total` moves boff BACKWARDS by 12, so the sizing bound
     * is satisfied and the column pointer is handed on — with 0 payload bytes
     * behind it.  db.c then reads the first entry's [u16 len] one byte past the
     * end of the frame. */
    if (WANT("A")) {
        uint8_t body[64 * 8 + 4];
        memset(body, 0, sizeof(body));
        for (int i = 0; i < 64; i++) {
            int64_t ts = BASE + i;
            memcpy(body + i * 8, &ts, 8);
        }
        put_u32(body + 64 * 8, 0xFFFFFFF0u);
        uint32_t plen = 0;
        uint8_t *p = frame("t", 64, 2, types, body, sizeof(body), &plen);
        expect_rejected(db, "t", "[A] symbol total 0xFFFFFFF0", p, plen, 0);
    }

    /* [B] the frame ENDS after the ts column: the symbol column's [u32 total]
     * header is not in it at all.  boff (64) never exceeds payload_len (129)
     * because payload_len also counts the 65-byte header, so the unfixed sizing
     * loop reads those 4 bytes from past the end of the frame. */
    if (WANT("B")) {
        uint8_t body[8 * 8];
        for (int i = 0; i < 8; i++) {
            int64_t ts = BASE + i;
            memcpy(body + i * 8, &ts, 8);
        }
        uint32_t plen = 0;
        uint8_t *p = frame(LONGNAME, 8, 2, types, body, sizeof(body), &plen);
        expect_rejected(db, LONGNAME, "[B] symbol header past frame end", p, plen, 0);
    }

    /* [C] a column whose entries outrun its OWN declared size: total=8 bytes of
     * payload, but the first entry claims 200.  rpc.c's sizing loop accepts
     * this frame — every column is inside the payload — so it is db.c's walk,
     * and only that, standing between the entry length and a read 200 bytes
     * past the frame. */
    if (WANT("C")) {
        uint8_t body[2 * 8 + 4 + 8];
        for (int i = 0; i < 2; i++) {
            int64_t ts = BASE + i;
            memcpy(body + i * 8, &ts, 8);
        }
        put_u32(body + 16, 8);              /* column payload: 8 bytes */
        put_u16(body + 20, 200);            /* first entry claims 200 */
        memcpy(body + 22, "abcdef", 6);
        uint32_t plen = 0;
        uint8_t *p = frame("t", 2, 2, types, body, sizeof(body), &plen);
        expect_rejected(db, "t", "[C] entry outruns its column", p, plen, 0);
    }

    /* [D] positive control: a well-formed symbol batch still applies.  Without
     * this the bound could be "reject everything" and the cases above would
     * still pass. */
    if (WANT("D")) {
        uint8_t body[2 * 8 + 4 + 10];
        for (int i = 0; i < 2; i++) {
            int64_t ts = BASE + 100 + i;
            memcpy(body + i * 8, &ts, 8);
        }
        put_u32(body + 16, 10);
        put_u16(body + 20, 3); memcpy(body + 22, "aaa", 3);
        put_u16(body + 25, 3); memcpy(body + 27, "bbb", 3);
        uint32_t plen = 0;
        uint8_t *p = frame("t", 2, 2, types, body, sizeof(body), &plen);
        int applied = tsdb_rpc_apply_write_batch_for_test(db, p, plen);
        int rows = count_rows(db, "t");
        if (applied != 1 || rows != 2) {
            fprintf(stderr, "FAIL %s:%d: [D] well-formed batch: applied=%d (want 1) "
                    "rows=%d (want 2)\n", __FILE__, __LINE__, applied, rows);
            g_fail++;
        } else {
            printf("  ok: [D] well-formed symbol batch applied, %d rows\n", rows);
        }
        free(p);
    }

    tsdb_close(db);
    rm_rf(TDIR);
#undef WANT
    printf(g_fail ? "\n=== FAILED (%d) ===\n" : "\n=== PASSED ===\n", g_fail);
    return g_fail ? 1 : 0;
}
