/* test_idx_append_crash.c — the append-only .idx publish, proved by killing a
 * process at every step of it.
 *
 * col_writer_close publishes a column's manifest by appending the new entries
 * at the end of the entry array and then rewriting the fixed-size header in
 * place: the entries do not exist until the header's `count` names them, the
 * same publish-last discipline the flush already uses for the ts column.  There
 * is no temp file and no rename, so unlike the full-rewrite publish it MUTATES
 * THE LIVE MANIFEST — every intermediate state is something a crash can leave
 * on disk, and every one of them has to read back safely.
 *
 * The states, and why each is safe, are argued on col_idx_append_publish in
 * src/storage/part.c.  This file does not re-argue them; it produces them with
 * a real process death (TSDB_TEST_CRASH_IDX_APPEND=<step>, which _exit()s
 * inside the publish) and reads them back from a fresh process.
 *
 * THE HOLE THIS PINS.  The dangerous state is not a missing entry, it is a
 * PARTIAL one.  idx_recover_header_size() repairs a "mongrel" idx — a file
 * whose version byte implies a 40-byte header while its entries actually start
 * at 48 — by testing which header size makes (file_size - header) an exact
 * multiple of the entry size.  A V3 idx with a torn 8-byte tail has EXACTLY
 * that length, so the repair fires on a healthy file and relocates the whole
 * entry array by 8 bytes: every block then decodes from a garbage offset.  That
 * is the same shape as the incident where compaction read V4 indexes as V3 and
 * shredded 95% of rows.  A torn tail and a real mongrel are indistinguishable
 * by length, so the writer must never produce one — which is why the publish
 * pre-extends the file to its final length before writing any entry.
 *
 * THE DECISIVE KILL is "header_mid": the process dies with the header WRITTEN
 * but not yet fsynced, which is the durable state any writeback in that window
 * (jbd2 commit, dirty_expire, memory pressure) followed by a power loss would
 * leave.  That is the point where `count` becomes visible, and it is the only
 * kill that can tell a header published as ONE write from one published field
 * by field: with count and max_seq in separate writes, ts.idx names the new
 * blocks while its redo checkpoint still points before them, reopen replays
 * records the partition already holds, and the re-flush appends a SECOND copy —
 * measured at SELECT v = 600 for 500 durable rows, TSDB_OK, permanent across
 * reopens.  A kill placed AFTER the fsync (the "header" case below) cannot see
 * that state at all: it only ever produces the fully published one.
 *
 * Asserts:
 *   [A] the publish really is an in-place append — the .idx inode does not
 *       change across a flush and the file grows by exactly one entry.  Without
 *       this the rest of the file could pass against a temp+rename publish.
 *   [B] after a kill at pre_extend / entries / header_mid / header, on a non-ts
 *       column and on the ts column: the surviving .idx is a whole number of
 *       entries past its header (the hole, closed), never names more entries
 *       than it holds, leaves no .tmp behind, and reads back EXACTLY the rows
 *       the header declares — no loss, and no row twice — counted on the VALUE
 *       column, since a relocated entry array still answers count(ts) correctly
 *       while every value read fails to pair.
 *   [C] the partition heals: a clean flush after the crash lands the missing
 *       batch, with no loss and no duplication.
 *   [D] the two tail shapes are not interchangeable — a whole-entry tail (what
 *       the publish can leave) reads every row; an 8-byte tail (what a publish
 *       without the pre-extension would leave) does not, on the V3 header the
 *       default durability mode writes.
 */

#include "tsdb.h"

#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern int tsdb_db_flush_all(tsdb_db_t *db);

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

/* On-disk idx layout, decoded here rather than through the engine so the test
 * pins the FORMAT and not the engine's own reading of it. */
#define IDX_MAGIC        0x31584449u
#define IDX_ENTRY_SIZE   88u
#define IDX_HDR_V3       40u
#define IDX_HDR_V4       48u

#define ROWS   100                              /* rows per batch = 1 block   */
#define NBATCH 8                                /* batches the child attempts */
#define DAY1   (1735689600LL * 1000000000LL)    /* 2025-01-01 UTC ns          */
#define PART   "20250101"

static const char *DIR_ = "/tmp/tsdb_test_idx_append_crash";

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

static off_t file_size(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? st.st_size : -1;
}

static uint32_t rd_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t rd_u16(const uint8_t *p) {
    return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

/* Decode an idx header.  Returns the header size implied by its VERSION byte
 * (40 for V3, 48 for V4), or 0 if the file is absent/short/not an idx. */
static unsigned idx_probe(const char *path, uint32_t *out_count,
                          uint16_t *out_version, uint32_t *out_entry_size)
{
    if (out_count) *out_count = 0;
    if (out_version) *out_version = 0;
    if (out_entry_size) *out_entry_size = 0;
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    uint8_t h[IDX_HDR_V4];
    size_t n = fread(h, 1, sizeof(h), f);
    fclose(f);
    if (n < IDX_HDR_V3 || rd_u32(h) != IDX_MAGIC) return 0;
    uint16_t ver = rd_u16(h + 8);
    if (ver != 3 && ver != 4) return 0;
    if (out_count)      *out_count      = rd_u32(h + 4);
    if (out_version)    *out_version    = ver;
    if (out_entry_size) *out_entry_size = rd_u16(h + 36);
    return (ver == 4) ? IDX_HDR_V4 : IDX_HDR_V3;
}

/* The column-count stamp at header bytes [10..11] (part.h), read RAW rather
 * than through tsdb_part_idx_ncols so this cannot be satisfied by a decoder
 * that agrees with a broken writer.  -1 = the file is not a readable idx. */
static int idx_ncols_raw(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    uint8_t h[IDX_HDR_V4];
    size_t n = fread(h, 1, sizeof(h), f);
    fclose(f);
    if (n < IDX_HDR_V3 || rd_u32(h) != IDX_MAGIC) return -1;
    return (int)rd_u16(h + 10);
}

static void col_paths(char idx[2][4096], char tmp[2][4200]) {
    snprintf(idx[0], 4096, "%s/x/%s/v.idx",  DIR_, PART);
    snprintf(idx[1], 4096, "%s/x/%s/ts.idx", DIR_, PART);
    snprintf(tmp[0], 4200, "%s.tmp", idx[0]);
    snprintf(tmp[1], 4200, "%s.tmp", idx[1]);
}

/* Write ROWS rows for batch `b`; v == the global row index, so a value range
 * query names exactly the rows a given prefix of batches produced. */
static int write_batch(tsdb_db_t *db, int b) {
    tsdb_table_t *t = NULL;
    int rc = tsdb_open_table(db, "x", &t);
    if (rc != TSDB_OK) return rc;
    tsdb_batch_t *bt = NULL;
    rc = tsdb_batch_begin(t, &bt);
    if (rc != TSDB_OK) return rc;
    for (int i = 0; i < ROWS; i++) {
        int64_t g = (int64_t)b * ROWS + i;
        if ((rc = tsdb_batch_row_ts(bt, DAY1 + g * 1000000LL)) != TSDB_OK ||
            (rc = tsdb_batch_row_i64(bt, 1, g))                != TSDB_OK ||
            (rc = tsdb_batch_row_end(bt))                      != TSDB_OK) {
            tsdb_batch_discard(bt);
            return rc;
        }
    }
    rc = tsdb_batch_commit(bt);
    if (rc != TSDB_OK) return rc;
    /* One published block per batch is this test's premise, not something it
     * may discover: a no-op under flush-on-commit (empty memtable), the actual
     * partition write under deferred flush. */
    return tsdb_db_flush_all(db);
}

static int count_rows(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int n = 0;
    if (tsdb_query(db, sql, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

/* ---- child processes ----------------------------------------------------
 * Every write happens in a forked child.  The parent stays a pure reader so
 * the injection can never be disarmed by state it inherited, and so each
 * "after the crash" read is genuinely a fresh process. */

/* Build `nb` batches into a fresh db, then _exit(0) with no tsdb_close. */
static void child_build(int nb, const char *crash_spec) {
    if (crash_spec) setenv("TSDB_TEST_CRASH_IDX_APPEND", crash_spec, 1);
    tsdb_db_t *db = NULL;
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    if (tsdb_open(DIR_, &db) != TSDB_OK)                     _exit(90);
    if (tsdb_create_table(db, "x", cols, 2, "ts") != TSDB_OK &&
        tsdb_open_table(db, "x", NULL) != TSDB_OK)           _exit(91);
    for (int b = 0; b < nb; b++)
        if (write_batch(db, b) != TSDB_OK)                   _exit(92);
    _exit(0);
}

/* Append one more batch and close CLEANLY — the heal step. */
static void child_heal(int b) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(DIR_, &db) != TSDB_OK) _exit(90);
    if (write_batch(db, b) != TSDB_OK)   _exit(92);
    tsdb_close(db);
    _exit(0);
}

/* Reopen and report counts through a file (row counts exceed an exit status).
 * Two value windows, because a total alone cannot tell "the in-flight batch
 * replayed" (correct) from "a durable batch came back twice" (duplication). */
static void child_count(const char *out, int64_t lo, int64_t hi,
                        int64_t lo2, int64_t hi2) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(DIR_, &db) != TSDB_OK) _exit(90);
    char sql[256];
    int nv = count_rows(db, "SELECT v FROM x");
    int nt = count_rows(db, "SELECT ts FROM x");
    snprintf(sql, sizeof(sql),
             "SELECT v FROM x WHERE v >= %lld AND v < %lld",
             (long long)lo, (long long)hi);
    int nr = count_rows(db, sql);
    snprintf(sql, sizeof(sql),
             "SELECT v FROM x WHERE v >= %lld AND v < %lld",
             (long long)lo2, (long long)hi2);
    int nr2 = count_rows(db, sql);
    tsdb_close(db);
    FILE *f = fopen(out, "w");
    if (!f) _exit(93);
    fprintf(f, "%d %d %d %d\n", nv, nt, nr, nr2);
    fclose(f);
    _exit(0);
}

static pid_t spawn(void (*fn)(void)) {
    fflush(NULL);
    pid_t p = fork();
    if (p < 0) FAIL("fork: %s", strerror(errno));
    if (p == 0) { fn(); _exit(99); }
    return p;
}

static int reap(pid_t p) {
    int st = 0;
    if (waitpid(p, &st, 0) != p || !WIFEXITED(st))
        FAIL("child %d did not exit normally", (int)p);
    return WEXITSTATUS(st);
}

/* fn-pointer trampolines (spawn takes no args). */
static int          g_nb;
static const char  *g_spec;
static int          g_heal_b;
static char         g_out[4096];
static int64_t      g_lo, g_hi, g_lo2, g_hi2;
static void tramp_build(void) { child_build(g_nb, g_spec); }
static void tramp_heal(void)  { child_heal(g_heal_b); }
static void tramp_count(void) { child_count(g_out, g_lo, g_hi, g_lo2, g_hi2); }

static void read_counts(const char *path, int *nv, int *nt, int *nr, int *nr2) {
    FILE *f = fopen(path, "r");
    if (!f) FAIL("counts file %s missing", path);
    if (fscanf(f, "%d %d %d %d", nv, nt, nr, nr2) != 4)
        FAIL("counts file %s corrupt", path);
    fclose(f);
}

/* Reopen in a fresh process and return (SELECT v, SELECT ts, two ranged
 * SELECT v).  Pass hi2 == lo2 when only one window is of interest. */
static void counts_after(int *nv, int *nt, int *nr, int *nr2,
                         int64_t lo, int64_t hi, int64_t lo2, int64_t hi2) {
    snprintf(g_out, sizeof(g_out), "/tmp/tsdb_test_idx_append_counts.txt");
    unlink(g_out);
    g_lo = lo; g_hi = hi; g_lo2 = lo2; g_hi2 = hi2;
    int rc = reap(spawn(tramp_count));
    if (rc != 0) FAIL("reader child failed with %d", rc);
    read_counts(g_out, nv, nt, nr, nr2);
    unlink(g_out);
}

/* ===== [A] the publish is an in-place append ============================== */

static void phase_a_in_place(void) {
    rm_rf(DIR_);
    tsdb_db_t *db = NULL;
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_open(DIR_, &db));
    OK(tsdb_create_table(db, "x", cols, 2, "ts"));

    char idx[2][4096], tmp[2][4200];
    col_paths(idx, tmp);

    OK(write_batch(db, 0));                 /* creates the idx (full rewrite) */
    OK(write_batch(db, 1));                 /* first append publish           */

    struct stat s1[2];
    uint32_t c1[2];
    int      n1[2];
    for (int i = 0; i < 2; i++) {
        if (stat(idx[i], &s1[i]) != 0) FAIL("[A] %s missing", idx[i]);
        unsigned h = idx_probe(idx[i], &c1[i], NULL, NULL);
        if (!h) FAIL("[A] %s is not a V3/V4 idx", idx[i]);
        ASSERT(c1[i] == 2);
        /* The column-count stamp, after one full rewrite (batch 0) and one
         * append publish (batch 1).  The schema has exactly 2 columns, so
         * every publish of either shape must leave a 2 here. */
        n1[i] = idx_ncols_raw(idx[i]);
        if (n1[i] != 2)
            FAIL("[A] %s stamps ncols=%d, expected 2 — a publish already "
                 "erased the column-count stamp", idx[i], n1[i]);
    }

    OK(write_batch(db, 2));                 /* the flush under test           */

    for (int i = 0; i < 2; i++) {
        struct stat s2;
        if (stat(idx[i], &s2) != 0) FAIL("[A] %s vanished", idx[i]);
        uint32_t c2 = 0, esz = 0;
        uint16_t ver = 0;
        unsigned h = idx_probe(idx[i], &c2, &ver, &esz);
        printf("[A] %s: ino %llu -> %llu, %lld -> %lld bytes, count %u -> %u "
               "(V%u, entry=%u)\n", idx[i],
               (unsigned long long)s1[i].st_ino, (unsigned long long)s2.st_ino,
               (long long)s1[i].st_size, (long long)s2.st_size,
               c1[i], c2, ver, esz);
        /* A temp+rename publish replaces the inode.  This one must not. */
        if (s2.st_ino != s1[i].st_ino)
            FAIL("[A] %s changed inode — the publish is still a rename, so the "
                 "rest of this file would be testing the wrong write path",
                 idx[i]);
        ASSERT(c2 == c1[i] + 1);
        ASSERT(s2.st_size == s1[i].st_size + (off_t)IDX_ENTRY_SIZE);
        ASSERT(esz == IDX_ENTRY_SIZE);
        ASSERT((uint64_t)s2.st_size == (uint64_t)h + (uint64_t)c2 * IDX_ENTRY_SIZE);
        if (file_size(tmp[i]) >= 0) FAIL("[A] leftover %s", tmp[i]);
        /* The append publish REWRITES THE WHOLE HEADER in one write, so it
         * re-stamps bytes [10..11] rather than leaving them alone.  Losing the
         * stamp here would not fail any count assertion above — it would make
         * part_col_absence_is_late_add() fall back to the shape rule alone and
         * zero-fill a column whose write was actually lost, which is the exact
         * fabrication a landed fix removed.  Pin it. */
        int n2 = idx_ncols_raw(idx[i]);
        printf("[A] %s: ncols stamp %d -> %d (expect 2 -> 2)\n",
               idx[i], n1[i], n2);
        if (n2 != 2 || n2 != n1[i])
            FAIL("[A] %s: the in-place publish left ncols=%d (was %d, schema "
                 "has 2 columns) — a whole-header rewrite must re-stamp "
                 "[10..11], not erase it", idx[i], n2, n1[i]);
    }
    tsdb_close(db);
    printf("[A] OK — in-place append, one 88-byte entry per flush, no rename\n");
}

/* ===== [B]+[C] kill at each step, on each column ========================== */

static void phase_b_crash(const char *step, int nth, const char *what) {
    rm_rf(DIR_);

    char spec[64];
    snprintf(spec, sizeof(spec), "%s:%d", step, nth);
    g_nb = NBATCH; g_spec = spec;
    int rc = reap(spawn(tramp_build));

    /* The kill has to have happened, at the step we asked for.  A child that
     * exits 0 ran to completion and this case tested nothing.  The mapping must
     * track the IDX_CRASH_* order in src/storage/part.c — the child exits
     * 70 + step, and a stale mapping here reads as "the injection did not
     * fire" rather than as a test bug. */
    int want = 70 + (!strcmp(step, "pre_extend") ? 1
                     : !strcmp(step, "entries")    ? 2
                     : !strcmp(step, "header_mid") ? 3 : 4);
    printf("[B:%s@%s] child exit=%d (expect %d)\n", step, what, rc, want);
    if (rc != want)
        FAIL("[B:%s@%s] the injection did not fire (exit %d) — the publish "
             "never reached that step", step, what, rc);

    char idx[2][4096], tmp[2][4200];
    col_paths(idx, tmp);

    uint32_t ts_count = 0;
    for (int i = 0; i < 2; i++) {
        uint32_t cnt = 0, esz = 0;
        uint16_t ver = 0;
        unsigned h = idx_probe(idx[i], &cnt, &ver, &esz);
        off_t sz = file_size(idx[i]);
        if (!h) FAIL("[B:%s@%s] %s unreadable after the kill", step, what, idx[i]);
        printf("[B:%s@%s] %s: V%u count=%u size=%lld tail=%lld\n",
               step, what, idx[i], ver, cnt, (long long)sz,
               (long long)((uint64_t)sz - h - (uint64_t)cnt * IDX_ENTRY_SIZE));
        ASSERT(esz == IDX_ENTRY_SIZE);
        ASSERT(cnt >= 2);                       /* a real partition, not flush #1 */
        /* THE HOLE, CLOSED: whatever the kill interrupted, the entry array is a
         * whole number of entries.  A sub-entry tail here is read as a V3/V4
         * mongrel and relocates every entry by 8 bytes. */
        if (((uint64_t)sz - h) % IDX_ENTRY_SIZE != 0)
            FAIL("[B:%s@%s] %s has a %llu-byte sub-entry tail — that length is "
                 "indistinguishable from a V3/V4 mongrel and the reader will "
                 "relocate the whole entry array",
                 step, what, idx[i],
                 (unsigned long long)(((uint64_t)sz - h) % IDX_ENTRY_SIZE));
        /* The header never names entries the file does not hold. */
        ASSERT((uint64_t)sz >= (uint64_t)h + (uint64_t)cnt * IDX_ENTRY_SIZE);
        if (file_size(tmp[i]) >= 0)
            FAIL("[B:%s@%s] leftover %s", step, what, tmp[i]);
        if (i == 1) ts_count = cnt;
    }

    /* ts.idx is the partition's block enumerator, so it names exactly the
     * batches a reader must see; anything the redo log replays comes on top. */
    int64_t durable = (int64_t)ts_count * ROWS;
    int nv = 0, nt = 0, nr = 0, nin = 0;
    counts_after(&nv, &nt, &nr, &nin, 0, durable, durable, durable + ROWS);
    printf("[B:%s@%s] after crash + reopen: SELECT v=%d SELECT ts=%d "
           "v in [0,%lld)=%d in-flight batch=%d (ts.idx declares %u blocks)\n",
           step, what, nv, nt, (long long)durable, nr, nin, ts_count);
    /* Counted on the VALUE column as well as ts: a relocated entry array keeps
     * answering count(ts) correctly while every value read fails to pair. */
    ASSERT(nv == nt);                        /* columns not desynced         */
    ASSERT(nv % ROWS == 0);                  /* never half a batch           */
    /* EXACTLY the durable rows, each ONCE.  Too few is loss; too many is the
     * duplication a header that published `count` without `max_seq` causes:
     * ts.idx names the new blocks while the redo checkpoint still points before
     * them, reopen replays records the partition already holds, and the
     * re-flush appends a second copy of the same blocks — measured at 600 rows
     * for 500 durable ones, TSDB_OK, permanent.  The "header_mid" kill above is
     * placed exactly there. */
    if (nr != (int)durable)
        FAIL("[B:%s@%s] %d rows in [0,%lld) for %lld durable rows — %s",
             step, what, nr, (long long)durable, (long long)durable,
             nr > (int)durable ? "rows came back DUPLICATED (count published "
                                 "without its max_seq checkpoint?)"
                               : "published rows were LOST");
    /* And nothing beyond them but the in-flight batch, whole or absent. */
    if (nin != 0 && nin != ROWS)
        FAIL("[B:%s@%s] in-flight batch read back as %d rows (expect 0 or %d)",
             step, what, nin, ROWS);
    ASSERT(nv == (int)durable + nin);        /* exact: no row from anywhere else */

    /* [C] heal: a clean flush after the crash lands, without loss or dup. */
    g_heal_b = NBATCH + 1;
    int hrc = reap(spawn(tramp_heal));
    if (hrc != 0) FAIL("[C:%s@%s] heal child failed with %d", step, what, hrc);

    int nv2 = 0, nt2 = 0, nr2 = 0, nd2 = 0;
    counts_after(&nv2, &nt2, &nr2, &nd2, (int64_t)(NBATCH + 1) * ROWS,
                 (int64_t)(NBATCH + 2) * ROWS, 0, durable);
    printf("[C:%s@%s] after heal: SELECT v=%d (expect %d) healed batch=%d "
           "(expect %d) pre-crash rows=%d (expect %lld)\n",
           step, what, nv2, nv + ROWS, nr2, ROWS, nd2, (long long)durable);
    ASSERT(nt2 == nv2);
    ASSERT(nv2 == nv + ROWS);                /* exactly one batch more       */
    ASSERT(nr2 == ROWS);                     /* and it is the one we wrote   */
    ASSERT(nd2 == (int)durable);             /* the heal duplicated nothing  */

    printf("[B:%s@%s] OK\n", step, what);
}

/* ===== [D] a whole-entry tail and an 8-byte tail are not the same file ==== */

static void append_zeros(const char *path, size_t n) {
    FILE *f = fopen(path, "ab");
    if (!f) FAIL("[D] open %s", path);
    unsigned char z[IDX_ENTRY_SIZE];
    memset(z, 0, sizeof(z));
    if (fwrite(z, 1, n, f) != n) FAIL("[D] append %zu bytes to %s", n, path);
    fclose(f);
}

static void phase_d_tail_shapes(size_t tail) {
    rm_rf(DIR_);
    g_nb = 5; g_spec = NULL;
    int rc = reap(spawn(tramp_build));
    if (rc != 0) FAIL("[D] build child failed with %d", rc);

    char idx[2][4096], tmp[2][4200];
    col_paths(idx, tmp);

    uint32_t cnt = 0;
    uint16_t ver = 0;
    unsigned h = idx_probe(idx[0], &cnt, &ver, NULL);
    if (!h) FAIL("[D] v.idx unreadable");
    ASSERT(cnt == 5);

    append_zeros(idx[0], tail);              /* v.idx only: ts stays intact */

    int64_t all = (int64_t)cnt * ROWS;
    int nv = 0, nt = 0, nr = 0, nr2 = 0;
    counts_after(&nv, &nt, &nr, &nr2, 0, all, 0, 0);
    printf("[D] V%u v.idx + %zu-byte tail: SELECT v=%d SELECT ts=%d "
           "(intact answer is %lld)\n", ver, tail, nv, nt, (long long)all);

    if (tail == IDX_ENTRY_SIZE) {
        /* What the publish CAN leave: pre-extended, entries not yet written.
         * The header still names `cnt`, so every published row reads back. */
        ASSERT(nv == (int)all);
        ASSERT(nt == (int)all);
    } else if (ver == 3) {
        /* What a publish WITHOUT the pre-extension would leave on a V3 idx:
         * length == 40 + cnt*88 + 8 is EXACTLY the mongrel signature, so
         * idx_recover_header_size relocates the entry array to offset 48, every
         * entry decodes from a garbage offset, and the partition collapses —
         * observed here as 5 published blocks reading back as 0 rows.  One
         * healthy column, one torn byte count, the whole partition gone: that
         * is the shape of the incident this pre-extension exists to prevent.
         * If a future change lets the recovery tell the two apart, this
         * assertion fires and should be revisited; pre-extending is what makes
         * the question moot. */
        if (nv >= (int)all)
            FAIL("[D] an 8-byte tail read as intact — the two tail shapes are "
                 "supposed to be indistinguishable by length, so re-check "
                 "idx_recover_header_size before relaxing the pre-extension");
    } else {
        /* V4 (48-byte header): 48 + cnt*88 + 8 fits neither candidate, so the
         * recovery does not fire and the tail is simply ignored.  The hole is
         * V3-shaped; the writer pre-extends unconditionally rather than
         * depending on which header a partition happens to carry. */
        ASSERT(nv == (int)all);
        ASSERT(nt == (int)all);
    }
}

int main(void) {
    setvbuf(stdout, NULL, _IOLBF, 0);

    phase_a_in_place();

    /* Per flush the writer publishes every non-ts column and then ts, so the
     * n-th append publish alternates v, ts, v, ts, ...  Odd n lands on the
     * value column, even n on ts — the visibility marker. */
    phase_b_crash("pre_extend", 7, "v");
    phase_b_crash("pre_extend", 8, "ts");
    phase_b_crash("entries",    7, "v");
    phase_b_crash("entries",    8, "ts");
    phase_b_crash("header_mid", 7, "v");
    phase_b_crash("header_mid", 8, "ts");
    phase_b_crash("header",     7, "v");
    phase_b_crash("header",     8, "ts");

    phase_d_tail_shapes(IDX_ENTRY_SIZE);
    phase_d_tail_shapes(8);

    rm_rf(DIR_);
    printf("\n=== idx append-publish crash TESTS PASSED ===\n");
    return 0;
}
