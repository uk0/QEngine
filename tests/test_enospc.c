/* test_enospc.c — disk-full / write-failure durability contract.
 *
 * THE CONTRACT (one rule, two modes):
 *   A commit that returns TSDB_OK means the rows are on stable storage and
 *   come back after a crash with no graceful close.  A write that cannot get
 *   them there must return a clean error and must leave every live file
 *   byte-identical — never a partial publish, never a silent success.
 *
 * The engine has two durability modes and they put that ack at DIFFERENT
 * writes, so a disk-full injection reaches them at different calls:
 *
 *   flush-on-commit (default)   tsdb_batch_commit runs the partition flush
 *                               itself; rows are acked once tsdb_part_flush_ex
 *                               publishes the ts.idx manifest via atomic
 *                               temp+rename.  The COMMIT is the write.
 *
 *   deferred flush              TSDB_WAL_ONLY_COMMIT=1, and auto-enabled at
 *                               open for a rotational/sata iopolicy — i.e. the
 *                               cluster's configuration.  tsdb_batch_commit
 *                               appends a redo record to <data_dir>/wal/ and
 *                               fsyncs it; the rows reach a partition later on
 *                               a size/idle/close flush and are rebuilt from
 *                               the redo log by redo_recover_table on reopen.
 *                               The WAL APPEND is the write that must fail on
 *                               a full disk (phase [11]); the partition write
 *                               is the later flush, which must report its own
 *                               failure rather than swallow it (phases [2a],
 *                               [6], [7]).
 *
 * Every phase below therefore asserts against "the call that writes the
 * partition in this mode", obtained by following each commit with
 * tsdb_db_flush_all() where the mode defers it — a no-op under flush-on-commit,
 * since the memtable is already empty.  Phase [10] pins the two modes to the
 * same promise from the outside: it crashes a child process and asserts the
 * row count implied by what that child was ACKED.
 *
 * This test forces the partition flush to fail (by making the data/table/
 * partition dir read-only, so col_writer_open's fopen / mkdir / idx
 * temp+rename hit EACCES — the same clean errno path ENOSPC takes) and
 * asserts:
 *   (a) batch_commit returns a clean error (no crash / abort),
 *   (b) data written BEFORE the failure is still fully readable,
 *   (c) the failed-flush rows linger in the memtable and a graceful close
 *       drains them exactly once (no loss, no duplication),
 *   (d) the data survives close + reopen (partition reload),
 *   (e) a FAILED flush leaves the partition .col byte-identical — the
 *       col_writer_abort / failed-close rollback removes the orphan block a
 *       partial append would otherwise leak (regression guard for the fix in
 *       src/storage/part.c).
 *
 * Phases [7]-[9] extend the same contract to the paths a SHORT WRITE reaches,
 * injected with RLIMIT_FSIZE (portable POSIX, no root, no loopback device;
 * errno EFBIG takes the same clean path as the EACCES above).  A short write
 * is observable ONLY at the fwrite return / ferror(): afterwards fflush,
 * fsync, fclose and rename all report success, so an unchecked chain publishes
 * truncated bytes and returns TSDB_OK.
 *   [7] src/storage/part.c col_writer_close — the .idx publish is a FULL
 *       rewrite (header + every OLD entry + the new ones).  A short write
 *       there drops manifest entries for ALREADY-durable blocks while the
 *       header still declares the full count, and the false TSDB_OK lets
 *       flush_and_clear_locked clear the memtable AND truncate the WAL.
 *   [8] src/storage/compaction.c — a per-column failure must abort the WHOLE
 *       partition swap.  Every column of a partition shares ONE block layout,
 *       so swapping only the columns that produced leaves ts compacted and a
 *       value column on the old layout.  That is not an obvious outage:
 *       count(*) is served from ts and keeps answering correctly while every
 *       value read fails the (ts_min,count) pairing — which is why the
 *       assertions here scan the VALUE column, not count(*).
 *   [9] src/storage/db.c delwm_bump — fopen(_delwm,"wb") truncated the live
 *       watermark before writing, so a failed write left a 0-byte file that
 *       reads back as "no delete ever happened" and anti-entropy stops
 *       re-asserting the delete.
 *
 * Phases [10]-[11] cover the ack itself rather than a single write:
 *  [10] acked => durable, from OUTSIDE the process: a child commits, is told
 *       yes or no, then _exit()s without tsdb_close (so nothing is flushed on
 *       the way out), and the parent asserts the row count that answer
 *       implies.  Runs in both modes and names neither.
 *  [11] src/storage/db.c redo_log_locked / wal.c — deferred flush only, since
 *       flush-on-commit never takes this path.  A commit whose redo record
 *       cannot be made durable must FAIL, and must leave the log
 *       byte-identical: commit_seq is only advanced on success, so a record
 *       left live carries a seq the next commit re-uses, and replay applies
 *       both — duplicating every row they overlap on.
 *
 * Skips gracefully (exit 0) if run as root, since root bypasses the
 * read-only permission check and the injection would not fire.
 */

#include "tsdb.h"
#include "../src/storage/db.h"           /* tsdb_delete_range, delwm_load */
#include "../src/storage/compaction.h"   /* phase [8] drives the compactor */

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utime.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

/* Deferred-flush mode (TSDB_WAL_ONLY_COMMIT=1, or auto-enabled for a
 * rotational/sata iopolicy).  Read from the DB, not from getenv, because the
 * iopolicy can turn it on with no env var set — and because the mode is the
 * PREMISE of each phase, never its assertion: if the engine regressed the
 * assertions below must fail, not quietly select the other branch. */
static int g_deferred = 0;

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        /* restore perms so rm_rf can descend even after we chmod'd things */
        chmod(q, 0700);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* Commit one batch of `n` rows starting at base_ts/base_v.  Returns the
 * batch_commit (or earlier) rc — does NOT abort on failure so the caller
 * can assert on the error. */
static int try_insert(tsdb_db_t *db, const char *tbl, int n,
                      int64_t base_ts, int64_t base_v) {
    tsdb_table_t *t = NULL;
    int rc = tsdb_open_table(db, tbl, &t);
    if (rc != TSDB_OK) return rc;
    tsdb_batch_t *b = NULL;
    rc = tsdb_batch_begin(t, &b);
    if (rc != TSDB_OK) return rc;
    for (int i = 0; i < n; i++) {
        rc = tsdb_batch_row_ts(b, base_ts + (int64_t)i);
        if (rc != TSDB_OK) { tsdb_batch_discard(b); return rc; }
        rc = tsdb_batch_row_i64(b, 1, base_v + (int64_t)i);
        if (rc != TSDB_OK) { tsdb_batch_discard(b); return rc; }
        rc = tsdb_batch_row_end(b);
        if (rc != TSDB_OK) { tsdb_batch_discard(b); return rc; }
    }
    return tsdb_batch_commit(b);   /* may flush -> may fail cleanly */
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

static off_t file_size(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? st.st_size : -1;
}

/* FNV-1a over a whole file.  0 means "unreadable" (never a valid digest here,
 * since every file we digest is asserted non-empty first). */
static uint64_t file_digest(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    uint64_t h = 1469598103934665603ULL;
    unsigned char buf[8192];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        for (size_t i = 0; i < n; i++)
            h = (h ^ buf[i]) * 1099511628211ULL;
    fclose(f);
    return h;
}

/* Lower ONLY rlim_cur.  Lowering rlim_max is irreversible for an unprivileged
 * process and would wedge every later phase (including the drain-on-close
 * assertions), so the hard limit is left alone. */
static void fsize_cap(rlim_t cap, struct rlimit *saved) {
    if (getrlimit(RLIMIT_FSIZE, saved) != 0) FAIL("getrlimit(RLIMIT_FSIZE)");
    struct rlimit r = *saved;
    r.rlim_cur = cap;
    if (setrlimit(RLIMIT_FSIZE, &r) != 0) FAIL("setrlimit(RLIMIT_FSIZE, %lld)",
                                               (long long)cap);
}
static void fsize_uncap(const struct rlimit *saved) {
    if (setrlimit(RLIMIT_FSIZE, saved) != 0) FAIL("setrlimit restore");
}

/* Back-date a partition dir: the compactor skips anything touched in the last
 * 60 s, so without this phase [8] is a silent no-op. */
static void backdate(const char *part_dir) {
    struct stat st;
    if (stat(part_dir, &st) != 0) FAIL("stat %s", part_dir);
    struct utimbuf ut = { st.st_atime, st.st_mtime - 120 };
    if (utime(part_dir, &ut) != 0) FAIL("utime %s", part_dir);
}

/* ===== Phase [8] scaffolding: compaction under a per-column failure ======== */

#define P8_BLK    40                        /* commits -> blocks per column   */
#define P8_ROWS   200                       /* rows per commit                */
#define P8_TOTAL  (P8_BLK * P8_ROWS)        /* 8000                           */
#define P8_DAY    "20260101"
#define P8_BASE   (1767225600LL * 1000000000LL)   /* 2026-01-01 UTC */

/* Full-entropy 52-bit mantissa under a fixed exponent: always finite, but
 * incompressible, so the FLOAT64 column stays an order of magnitude larger
 * than the ts column.  That size gap is what lets one cap fail the value
 * column's .tmp while the ts column's .tmp is written in full. */
static double p8_val(int i) {
    uint64_t x = (uint64_t)i * 6364136223846793005ULL + 1442695040888963407ULL;
    x ^= x >> 33; x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ULL; x ^= x >> 33;
    x = (x & 0x000FFFFFFFFFFFFFULL) | 0x4000000000000000ULL;
    double d;
    memcpy(&d, &x, sizeof(d));
    return d;
}

static void p8_seed(const char *dbdir, tsdb_db_t **out_db) {
    rm_rf(dbdir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dbdir, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, "c", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "c", &t));
    int i = 0;
    for (int b = 0; b < P8_BLK; b++) {
        tsdb_batch_t *bt = NULL;
        OK(tsdb_batch_begin(t, &bt));
        for (int r = 0; r < P8_ROWS; r++, i++) {
            OK(tsdb_batch_row_ts(bt, P8_BASE + (int64_t)i * 1000000LL));
            OK(tsdb_batch_row_f64(bt, 1, p8_val(i)));
            OK(tsdb_batch_row_end(bt));
        }
        OK(tsdb_batch_commit(bt));
        /* One on-disk block per commit — that is what gives the compactor
         * P8_BLK blocks to merge, and it is the phase's precondition, not
         * something it may discover.  Under flush-on-commit the commit already
         * published it and this is a no-op on an empty memtable; under
         * deferred flush the commit only fsynced a redo record, so drain it. */
        OK(tsdb_db_flush_all(db));
    }
    *out_db = db;
}

static void p8_paths(const char *dbdir, char pd[4096], char f[4][4096]) {
    snprintf(pd, 4096, "%s/c/%s", dbdir, P8_DAY);
    snprintf(f[0], 4096, "%s/ts.col", pd);
    snprintf(f[1], 4096, "%s/ts.idx", pd);
    snprintf(f[2], 4096, "%s/v.col",  pd);
    snprintf(f[3], 4096, "%s/v.idx",  pd);
}

static int p8_compact(tsdb_db_t *db) {
    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns           = 10000000LL;
    opts.worker_threads        = -1;   /* manual: no background thread */
    tsdb_compactor_t *c = NULL;
    OK(tsdb_compactor_start(db, &opts, &c));
    int rc = tsdb_compactor_run_once(c);
    tsdb_compactor_stop(c);
    return rc;
}

/* Compact an identical, UNCAPPED copy so the torn-tail cap can be derived from
 * the real compacted size instead of hard-coded (a hard-coded cap silently
 * decays into a no-op the moment the codec changes). */
static off_t p8_probe_compacted_vcol(void) {
    const char *pdir = "/tmp/tsdb_test_enospc_c_probe";
    tsdb_db_t *db = NULL;
    p8_seed(pdir, &db);
    char pd[4096], f[4][4096];
    p8_paths(pdir, pd, f);
    backdate(pd);
    OK(p8_compact(db));
    off_t s = file_size(f[2]);
    tsdb_close(db);
    rm_rf(pdir);
    return s;
}

/*
 * One phase-[8] injection.  Exactly one of these is used per case:
 *   cap             — RLIMIT_FSIZE while the compactor runs (0 = none)
 *   chmod_zero      — make the value .col unreadable (a non-ENOSPC resource
 *                     failure; compact_column_file's open() used to swallow it
 *                     and report TSDB_OK with produced=0)
 *   poke_total_rows — rewrite v.idx's header total_rows (offset 12, u64 LE) so
 *                     the per-block counts sum past it and the re-encode takes
 *                     its count-overflow bail.  That bail returns TSDB_OK with
 *                     produced=0, so the return code says nothing at all —
 *                     only the eligibility flag distinguishes it from a
 *                     legitimate no-op.  tsdb_part_open reads this field and
 *                     never uses it, so the poke is invisible to readers.
 *
 * Contract in every case: an aborted compaction leaves ALL FOUR live files
 * byte-identical, and the partition still reads back every value.
 */
static void phase8_case(const char *label, rlim_t cap, int chmod_zero,
                        uint64_t poke_total_rows) {
    const char *pdir = "/tmp/tsdb_test_enospc_cmp";
    tsdb_db_t *db = NULL;
    p8_seed(pdir, &db);

    char pd[4096], f[4][4096];
    p8_paths(pdir, pd, f);
    static const char *fname[4] = { "ts.col", "ts.idx", "v.col", "v.idx" };

    if (poke_total_rows) {
        FILE *pf = fopen(f[3], "r+b");
        if (!pf) FAIL("[8:%s] open v.idx", label);
        unsigned char le[8];
        for (int k = 0; k < 8; k++) le[k] = (unsigned char)(poke_total_rows >> (8 * k));
        if (fseek(pf, 12, SEEK_SET) != 0 || fwrite(le, 1, 8, pf) != 8)
            FAIL("[8:%s] poke v.idx total_rows", label);
        fclose(pf);
    }

    off_t    sz[4];
    uint64_t dg[4];
    for (int i = 0; i < 4; i++) {
        sz[i] = file_size(f[i]);
        dg[i] = file_digest(f[i]);
        if (sz[i] <= 0 || dg[i] == 0) FAIL("[8:%s] %s missing/empty", label, fname[i]);
    }
    if (sz[2] <= sz[0])
        FAIL("[8:%s] v.col (%lld) is not larger than ts.col (%lld) — the "
             "injection cannot discriminate; make the values less compressible",
             label, (long long)sz[2], (long long)sz[0]);
    backdate(pd);

    struct rlimit saved;
    if (chmod_zero && chmod(f[2], 0000) != 0) FAIL("[8:%s] chmod 000 v.col", label);
    if (cap) fsize_cap(cap, &saved);
    int rc = p8_compact(db);
    if (cap) fsize_uncap(&saved);
    if (chmod_zero && chmod(f[2], 0600) != 0) FAIL("[8:%s] chmod 600 v.col", label);

    printf("[8:%s] compact rc=%d; ts.col %lld->%lld ts.idx %lld->%lld "
           "v.col %lld->%lld v.idx %lld->%lld\n", label, rc,
           (long long)sz[0], (long long)file_size(f[0]),
           (long long)sz[1], (long long)file_size(f[1]),
           (long long)sz[2], (long long)file_size(f[2]),
           (long long)sz[3], (long long)file_size(f[3]));

    for (int i = 0; i < 4; i++) {
        if (file_digest(f[i]) != dg[i])
            FAIL("[8:%s] %s CHANGED by an aborted compaction (%lld -> %lld bytes)"
                 " — the partition's columns are now on different block layouts",
                 label, fname[i], (long long)sz[i], (long long)file_size(f[i]));
    }
    /* The aborted swap must also leave no .tmp pair behind. */
    for (int i = 0; i < 4; i++) {
        char tmp[4200];
        snprintf(tmp, sizeof(tmp), "%s.tmp", f[i]);
        if (file_size(tmp) >= 0) FAIL("[8:%s] leftover %s.tmp", label, fname[i]);
    }

    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(pdir, &db));
    /* MUST scan the VALUE column.  A desynced partition still answers
     * count(ts) with the right number (it is served from the ts column) while
     * every value read fails the (ts_min,count) pairing — a count-only
     * assertion is green on the broken build and worthless. */
    int vrows = count_rows(db, "SELECT v FROM c");
    int trows = count_rows(db, "SELECT ts FROM c");
    printf("[8:%s] after reopen: SELECT v -> %d rows, SELECT ts -> %d rows "
           "(expect %d both)\n", label, vrows, trows, P8_TOTAL);
    ASSERT(vrows == P8_TOTAL);
    ASSERT(trows == P8_TOTAL);
    tsdb_close(db);
    rm_rf(pdir);
    printf("[8:%s] OK\n", label);
}

/* ===== Phase [9]: the delete watermark survives a failed bump ============== */

static void phase9_delwm(void) {
    const char *pdir = "/tmp/tsdb_test_enospc_delwm";
    rm_rf(pdir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(pdir, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "d", cols, 2, "ts"));

    const int64_t D1 = 1735689600LL * 1000000000LL;   /* 2025-01-01 */
    const int64_t DAY = 86400LL * 1000000000LL;
    OK(try_insert(db, "d", 50, D1,           10));
    OK(try_insert(db, "d", 50, D1 + DAY,     20));
    OK(try_insert(db, "d", 50, D1 + 2 * DAY, 30));
    /* tsdb_delete_range drops partition DIRS, so the three days have to BE on
     * disk for the first delete to be a real delete and not a watermark bump
     * over nothing.  No-op under flush-on-commit. */
    OK(tsdb_db_flush_all(db));

    int removed = 0;
    OK(tsdb_delete_range(db, "d", D1 + DAY, /*op_lt*/1, /*inclusive*/0, &removed));
    int64_t wm1 = tsdb_table_delwm_load(db, "d");
    char wmp[4096];
    snprintf(wmp, sizeof(wmp), "%s/d/_delwm", pdir);
    printf("[9] first delete removed %d partition(s); _delwm size=%lld wm=%lld\n",
           removed, (long long)file_size(wmp), (long long)wm1);
    ASSERT(wm1 == D1 + DAY);
    ASSERT(file_size(wmp) == 8);

    /* Now fail the SECOND bump.  tsdb_delete_range itself only rm_rf's whole
     * partition dirs (no writes), so a zero file-size cap isolates the one
     * write that matters: delwm_bump's. */
    struct rlimit saved;
    fsize_cap(0, &saved);
    int rc = tsdb_delete_range(db, "d", D1 + 2 * DAY, 1, 0, &removed);
    fsize_uncap(&saved);

    off_t   sz  = file_size(wmp);
    int64_t wm2 = tsdb_table_delwm_load(db, "d");
    printf("[9] failed bump (rc=%d): _delwm size=%lld wm=%lld (expect 8 / %lld)\n",
           rc, (long long)sz, (long long)wm2, (long long)wm1);
    /* The old watermark must SURVIVE — a 0-byte _delwm reads back as "no
     * delete ever happened", which stops anti-entropy re-asserting the delete
     * and lets a peer that missed it resurrect the rows cluster-wide. */
    ASSERT(sz == 8);
    ASSERT(wm2 >= wm1);

    tsdb_close(db);
    rm_rf(pdir);
    printf("[9] OK\n");
}

/* ===== Phase [10]: acked => durable, observed from outside the process =====
 *
 * The two modes make an ack durable by different means — flush-on-commit
 * publishes the partition manifest before returning, deferred flush fsyncs the
 * redo record before returning — but they promise the same thing, and this
 * phase states it without naming either one.
 *
 * A child writes one batch that must succeed, then a second one under a
 * read-only table dir, then _exit()s.  No tsdb_close, so nothing is flushed on
 * the way out, no memtable is drained and no WAL is truncated: whatever the
 * parent reads back is exactly what the ack had already made durable.  The
 * child reports the SECOND commit's answer through its exit status and the
 * parent asserts the row count that answer implies — acked rows must ALL be
 * there, refused rows must not be.  Under deferred flush that is the proof
 * that a commit acked over an unwritable partition dir was not a lie; under
 * flush-on-commit it is the proof that the refusal was not a silent loss of
 * the earlier durable batch.
 */
static void phase10_crash_durability(void) {
    const char *pdir = "/tmp/tsdb_test_enospc_crash";
    rm_rf(pdir);

    const int64_t D   = 1735689600LL * 1000000000LL;   /* 2025-01-01 */
    const int64_t DAY = 86400LL * 1000000000LL;
    const int     NC  = 500;

    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/x", pdir);

    fflush(NULL);                       /* no buffered output into the child */
    pid_t pid = fork();
    if (pid < 0) FAIL("[10] fork");
    if (pid == 0) {
        tsdb_db_t *c = NULL;
        tsdb_col_t cc[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_INT64     },
        };
        if (tsdb_open(pdir, &c) != TSDB_OK)                    _exit(90);
        if (tsdb_create_table(c, "x", cc, 2, "ts") != TSDB_OK) _exit(91);
        if (try_insert(c, "x", NC, D, 1) != TSDB_OK)           _exit(92);
        if (chmod(tdir, 0500) != 0)                            _exit(93);
        int rc = try_insert(c, "x", NC, D + DAY, 2);
        /* CRASH HERE: no tsdb_close — no final flush, no WAL truncate. */
        _exit(rc == TSDB_OK ? 0 : 1);
    }

    int st = 0;
    if (waitpid(pid, &st, 0) != pid || !WIFEXITED(st)) FAIL("[10] child died");
    int acked = WEXITSTATUS(st);
    if (acked > 1) FAIL("[10] child setup failed at step %d", acked);
    if (chmod(tdir, 0700) != 0) FAIL("[10] chmod 0700 %s", tdir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(pdir, &db));
    int got  = count_rows(db, "SELECT v FROM x");
    int want = (acked == 0) ? 2 * NC : NC;
    printf("[10] child %s the 2nd commit; after crash + reopen count=%d "
           "(expect %d)\n", acked == 0 ? "ACKED" : "REFUSED", got, want);
    ASSERT(got == want);
    tsdb_close(db);
    rm_rf(pdir);
    printf("[10] OK\n");
}

/* ===== Phase [11]: a commit that cannot log its rows must refuse ============
 *
 * Deferred flush moves the ack onto <data_dir>/wal/<table>.log, and nothing
 * else in this file reaches a write failure THERE — phases [2]/[6]/[7] all
 * land on partition writes, which a wal-only commit never performs.  A full
 * disk hits the log first, and the commit must refuse rather than ack a record
 * that never made it to stable storage.
 *
 * Injection: RLIMIT_FSIZE pinned PAST the log's current length but short of
 * one more record, so the next append is a genuine SHORT write (EFBIG — the
 * same clean errno path as ENOSPC) rather than a refused one: the header
 * lands, the payload does not.  A cap at exactly the current length would be
 * rejected whole by the kernel and would never exercise the rollback.
 * Asserts (a) the commit fails and (b) the log is left byte-identical.  (b) is
 * not cosmetic: redo_log_locked only advances commit_seq after a successful
 * append+fsync, so a record left live carries a seq the NEXT commit re-uses,
 * and replay applies both records — duplicating every row they overlap on.
 *
 * Flush-on-commit does not run this path at all (its first write is the
 * partition, pinned by [7]); the phase reports that instead of asserting on a
 * code path the mode does not have.
 */
static void phase11_wal_full(void) {
    if (!g_deferred) {
        printf("[11] flush-on-commit: a commit performs no wal-only append; "
               "the same rule on its first write is phase [7].\n");
        return;
    }

    const char *pdir = "/tmp/tsdb_test_enospc_walfull";
    rm_rf(pdir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(pdir, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "w", cols, 2, "ts"));

    const int64_t D = 1767225600LL * 1000000000LL;   /* 2026-01-01 UTC */
    const int     NW = 100;
    OK(try_insert(db, "w", NW, D, 1));

    char wp[4096];
    snprintf(wp, sizeof(wp), "%s/wal/w.log", pdir);
    off_t    wsz = file_size(wp);
    uint64_t wdg = file_digest(wp);
    /* Loudly, not quietly: if an acked wal-only commit ever stops writing the
     * log, the cap below would cap nothing and the phase would pass for the
     * wrong reason — and the ack would have had no durable record behind it. */
    if (wsz <= 0 || wdg == 0)
        FAIL("[11] an acked wal-only commit left %s empty — nothing backs the ack",
             wp);
    printf("[11] redo log after one acked commit: %lld bytes\n", (long long)wsz);

    /* Second commit logs a record of the same shape, so half of one lands
     * inside the cap and half does not. */
    off_t cap = wsz + wsz / 2;
    struct rlimit saved;
    fsize_cap((rlim_t)cap, &saved);
    int rc = try_insert(db, "w", NW, D + NW, 2);
    fsize_uncap(&saved);

    printf("[11] commit with the redo log capped at %lld returned rc=%d (%s); "
           "log %lld -> %lld bytes\n", (long long)cap, rc, tsdb_errstr(rc),
           (long long)wsz, (long long)file_size(wp));
    ASSERT(rc != TSDB_OK);                 /* never ack an unlogged commit */
    ASSERT(file_digest(wp) == wdg);        /* no half record left under a
                                            * seq the next commit re-uses  */

    /* Not wedged: with room again the rows commit, and the refused batch is
     * still in the memtable, so a clean close drains all three exactly once. */
    OK(try_insert(db, "w", NW, D + 2 * NW, 3));
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(pdir, &db));
    int n = count_rows(db, "SELECT v FROM w");
    printf("[11] after close + reopen count=%d (expect %d)\n", n, 3 * NW);
    ASSERT(n == 3 * NW);
    tsdb_close(db);
    rm_rf(pdir);
    printf("[11] OK\n");
}

int main(void) {
    if (geteuid() == 0) {
        printf("test_enospc: running as root — read-only injection cannot fire, "
               "SKIP (covered by the Docker loopback test).\n");
        return 0;
    }

    /* RLIMIT_FSIZE raises SIGXFSZ (default: kill) on the offending write.
     * Ignoring it turns the same event into a short write + EFBIG, which is
     * exactly the errno path a real ENOSPC takes. */
    signal(SIGXFSZ, SIG_IGN);

    const char *dir = "/tmp/tsdb_test_enospc";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    g_deferred = tsdb_db_wal_only_commit(db);
    printf("[0] durability mode: %s\n", g_deferred
           ? "deferred flush (commit = redo append + fsync)"
           : "flush-on-commit (commit = partition publish)");

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    /* ---- Phase 1: write N rows on one day, commit, verify durable. ---- */
    const int64_t DAY1 = 1735689600LL * 1000000000LL; /* 2025-01-01 UTC */
    const int N1 = 500;
    OK(try_insert(db, "t", N1, DAY1, 100));
    int pre = count_rows(db, "SELECT v FROM t");
    printf("[1] committed N1=%d, count=%d\n", N1, pre);
    ASSERT(pre == N1);

    /* ---- Phase 2: make the TABLE dir read-only, attempt another commit. ----
     * Partition dirs live at <data_dir>/<table>/<YYYYMMDD>, so chmod the
     * table dir (not the data root).  The next commit targets a DIFFERENT
     * day, so the flush must mkdir a brand-new partition dir under a 0500
     * table dir -> EACCES — the same clean errno path an ENOSPC mid-flush
     * takes (col_writer_open's fopen / tsdb_mkdir_p both fail with errno).
     *
     * WHICH call has to report that depends on where the mode puts the ack,
     * and this injection blocks only the partition write:
     *   flush-on-commit — the commit IS the partition write, so the COMMIT
     *                     must fail;
     *   deferred flush  — the commit is a redo append + fsync under
     *                     <data_dir>/wal/, which a read-only TABLE dir does
     *                     not touch.  The rows really are on stable storage,
     *                     so TSDB_OK is honest, not a lie — phase [10] kills
     *                     a process mid-flight to prove they come back.  The
     *                     partition write is the deferred flush, and THAT is
     *                     what must not swallow the error: tsdb_db_flush_all
     *                     returning TSDB_OK here is the same lie, one call
     *                     later, and it is the only "put my rows on disk now"
     *                     entry point there is.
     * Same rule either way: the call that writes the partition fails, and no
     * call claims a write it did not perform. */
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", dir, "t");
    if (chmod(tbl_dir, 0500) != 0) FAIL("chmod 0500 on table dir failed");

    const int64_t DAY2 = DAY1 + 86400LL * 1000000000LL;
    const int N2 = 500;
    int rc_fail = try_insert(db, "t", N2, DAY2, 200);
    printf("[2] commit under read-only table dir returned rc=%d (%s)\n",
           rc_fail, tsdb_errstr(rc_fail));
    if (!g_deferred) {
        ASSERT(rc_fail != TSDB_OK);   /* clean error, no crash/abort */
    } else {
        ASSERT(rc_fail == TSDB_OK);   /* redo record fsynced: honest ack */
        int rc_flush = tsdb_db_flush_all(db);
        printf("[2a] deferred flush under read-only table dir returned "
               "rc=%d (%s)\n", rc_flush, tsdb_errstr(rc_flush));
        ASSERT(rc_flush != TSDB_OK);  /* the partition write must be reported */
    }

    /* ---- Phase 3: no corruption — prior data intact, failed rows linger. ----
     * On a failed flush, flush_and_clear_locked does NOT clear the memtable
     * (rc != TSDB_OK), so the N2 rows survive in RAM and stay visible to a
     * live SELECT.  They are NOT yet durable — that's the safe behaviour
     * (no silent loss; the next successful commit re-flushes them).  The
     * critical correctness property is that the EARLIER durable N1 rows are
     * untouched and nothing is corrupted: count == N1 + N2, never < N1,
     * never garbage.  (Under deferred flush both batches sit in the memtable
     * and both are redo-durable; the count and the reason it must hold are
     * the same.) */
    int mid = count_rows(db, "SELECT v FROM t");
    printf("[3] after failed flush, live count=%d (N1=%d + retained N2=%d)\n",
           mid, N1, N2);
    ASSERT(mid >= N1);            /* prior durable data never lost */
    ASSERT(mid == N1 + N2);       /* failed-flush rows retained in memtable */

    /* ---- Phase 4: graceful close drains the retained memtable. ----
     * tsdb_close() flushes pending memtable rows, so once writes are possible
     * again a clean close+reopen surfaces N1 + N2 — the failed-flush rows are
     * NOT lost, they were retried on close.  (The un-acked-rows-lost boundary
     * applies only to a hard CRASH with no graceful close — under
     * flush-on-commit nothing replays those rows, under deferred flush the
     * redo log does; phase [10] pins both.)  The correctness property here:
     * exactly N1 + N2, no duplication, no corruption. */
    if (chmod(tbl_dir, 0700) != 0) FAIL("chmod 0700 on table dir failed");
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    int durable = count_rows(db, "SELECT v FROM t");
    printf("[4] after restore + graceful close + reopen, count=%d (expect %d)\n",
           durable, N1 + N2);
    ASSERT(durable == N1 + N2);   /* close drained the retained rows; none lost/dup'd */

    /* ---- Phase 5: table not wedged — fresh writes still succeed durably. ---- */
    const int64_t DAY3 = DAY2 + 86400LL * 1000000000LL;
    const int N3 = 300;
    OK(try_insert(db, "t", N3, DAY3, 5000));
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    int post = count_rows(db, "SELECT v FROM t");
    printf("[5] after fresh write + reopen, count=%d (expect %d)\n",
           post, N1 + N2 + N3);
    ASSERT(post == N1 + N2 + N3);

    /* ---- Phase 6: regression guard for the partial-flush .col rollback. ----
     * Fail an APPEND into an EXISTING partition: blocking the partition dir
     * lets col_writer_open's fopen("ab") on the existing .col succeed (and a
     * block gets appended + fflush'd), but col_writer_close's idx temp+rename
     * fails.  Pre-fix, the appended block stayed in the .col as an orphan with
     * no idx entry — dead bytes that accumulate under sustained disk-full
     * retries (consuming the very space that's scarce).  The col_writer_abort
     * / failed-close rollback now truncate the .col back to its pre-flush
     * length, so a FAILED flush leaves the partition byte-identical.
     *
     * Deterministic check: the v.col file size must be unchanged across the
     * failed append. */
    tsdb_table_t *t6 = NULL;
    OK(tsdb_open_table(db, "t", &t6));   /* ensure 20250101 exists on disk */
    char vcol[4096];
    snprintf(vcol, sizeof(vcol), "%s/t/20250101/v.col", dir);
    off_t sz_before = file_size(vcol);
    printf("[6a] v.col size before failed append = %lld\n", (long long)sz_before);
    ASSERT(sz_before > 0);               /* partition really is on disk */

    char p1_dir[4096];
    snprintf(p1_dir, sizeof(p1_dir), "%s/t/20250101", dir);
    const int64_t DAY1b = DAY1 + 5000;   /* same day as N1, distinct ts */
    const int N4 = 300;
    if (chmod(p1_dir, 0500) != 0) FAIL("chmod 0500 on partition dir failed");
    int rc_app = try_insert(db, "t", N4, DAY1b, 90000);   /* append -> flush fails */
    if (g_deferred) {
        /* The .col append this phase guards happens in the flush, and under
         * deferred flush the commit is not it — the commit only fsynced a redo
         * record, which the read-only PARTITION dir does not touch.  Drive the
         * partition write and assert on that instead; the .col rollback
         * requirement below is identical. */
        ASSERT(rc_app == TSDB_OK);
        rc_app = tsdb_db_flush_all(db);
    }
    if (chmod(p1_dir, 0700) != 0) FAIL("chmod 0700 on partition dir failed");
    off_t sz_after = file_size(vcol);
    printf("[6b] failed append rc=%d (%s); v.col size after = %lld (expect == %lld)\n",
           rc_app, tsdb_errstr(rc_app), (long long)sz_after, (long long)sz_before);
    ASSERT(rc_app != TSDB_OK);            /* clean error */
    ASSERT(sz_after == sz_before);        /* FIX: failed flush left .col byte-identical */

    /* The failed rows linger in the memtable; a graceful close flushes them
     * exactly once.  Verify no loss and no duplication after reopen. */
    OK(tsdb_open_table(db, "t", &t6));
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    int after = count_rows(db, "SELECT v FROM t");
    int n4_rows = count_rows(db, "SELECT v FROM t WHERE v >= 90000 AND v < 90300");
    printf("[6c] after close+reopen: total=%d (expect %d), N4-range=%d (expect %d)\n",
           after, N1 + N2 + N3 + N4, n4_rows, N4);
    ASSERT(after == N1 + N2 + N3 + N4);   /* drained once, no loss, no dup */
    ASSERT(n4_rows == N4);

    /* ---- Phase 7: a SHORT .idx write must not publish a truncated manifest.
     * The idx publish rewrites the whole manifest (header + every OLD entry +
     * this flush's new ones), so a short write there drops entries for blocks
     * that were already durable while the header keeps declaring the full
     * count.  Pre-fix nothing observed it — fflush, fsync, fclose and rename
     * all returned success after the short fwrite — so col_writer_close
     * returned TSDB_OK and flush_and_clear_locked cleared the memtable and
     * truncated the WAL, losing acked rows from disk, RAM and WAL at once.
     *
     * Injection: build one partition whose .idx is much larger than its .col,
     * then cap RLIMIT_FSIZE between them.  The .col append fits; the .idx
     * rewrite does not.  The cap is DERIVED from the real file sizes, and the
     * phase fails loudly rather than degrading into a no-op if the window is
     * empty. */
    const int K7 = 400, B7 = 20;
    tsdb_col_t cols7[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t7", cols7, 2, "ts"));
    const int64_t D7 = 1767225600LL * 1000000000LL;   /* 2026-01-01 UTC */
    for (int k = 0; k < K7; k++) {
        OK(try_insert(db, "t7", B7, D7 + (int64_t)k * B7, 1));
        /* One manifest entry per commit is the whole injection: it is what
         * makes the .idx outgrow the .col so a cap can land between them.
         * No-op under flush-on-commit (empty memtable); under deferred flush
         * the commit published nothing, so drain it. */
        OK(tsdb_db_flush_all(db));
    }

    char p7[4][4096];
    snprintf(p7[0], sizeof(p7[0]), "%s/t7/20260101/ts.col", dir);
    snprintf(p7[1], sizeof(p7[1]), "%s/t7/20260101/v.col",  dir);
    snprintf(p7[2], sizeof(p7[2]), "%s/t7/20260101/ts.idx", dir);
    snprintf(p7[3], sizeof(p7[3]), "%s/t7/20260101/v.idx",  dir);
    off_t max_col = file_size(p7[0]) > file_size(p7[1])
                    ? file_size(p7[0]) : file_size(p7[1]);
    off_t min_idx = file_size(p7[2]) < file_size(p7[3])
                    ? file_size(p7[2]) : file_size(p7[3]);
    printf("[7a] %d blocks: max(.col)=%lld min(.idx)=%lld\n",
           K7, (long long)max_col, (long long)min_idx);
    /* Self-calibrating, and loudly so: if the window ever closes (fewer blocks
     * per commit, a wider idx entry, ...) the phase FAILS instead of quietly
     * degrading into a no-op that passes for the wrong reason. */
    if (max_col <= 0 || min_idx <= 0 || max_col + 4096 >= min_idx)
        FAIL("[7] cap window empty (max .col=%lld, min .idx=%lld) — raise K7",
             (long long)max_col, (long long)min_idx);

    uint64_t d_ts = file_digest(p7[2]), d_v = file_digest(p7[3]);
    ASSERT(d_ts != 0 && d_v != 0);

    struct rlimit saved7;
    fsize_cap((rlim_t)(max_col + 4096), &saved7);
    int rc7 = try_insert(db, "t7", B7, D7 + (int64_t)K7 * B7, 1);
    if (g_deferred && rc7 == TSDB_OK) {
        /* Deferred flush: the redo record is far below the cap, so the commit
         * legitimately acks; the .idx rewrite this phase is about happens in
         * the flush.  Assert on the call that performs it. */
        rc7 = tsdb_db_flush_all(db);
    }
    fsize_uncap(&saved7);

    printf("[7b] commit under FSIZE cap=%lld returned rc=%d (%s); "
           "ts.idx %lld->%lld v.idx %lld->%lld\n",
           (long long)(max_col + 4096), rc7, tsdb_errstr(rc7),
           (long long)min_idx, (long long)file_size(p7[2]),
           (long long)min_idx, (long long)file_size(p7[3]));
    ASSERT(rc7 != TSDB_OK);                    /* never a silent success */
    ASSERT(file_digest(p7[2]) == d_ts);        /* manifest untouched... */
    ASSERT(file_digest(p7[3]) == d_v);         /* ...for BOTH columns   */

    int live7 = count_rows(db, "SELECT v FROM t7");
    printf("[7c] live count=%d (expect %d durable + %d retained)\n",
           live7, K7 * B7, B7);
    ASSERT(live7 == K7 * B7 + B7);             /* failed rows retained in RAM */

    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    int post7 = count_rows(db, "SELECT v FROM t7");
    printf("[7d] after close+reopen: count=%d (expect %d)\n",
           post7, K7 * B7 + B7);
    ASSERT(post7 == K7 * B7 + B7);             /* nothing lost, nothing dup'd */

    tsdb_close(db);
    db = NULL;

    /* ---- Phase 8: an aborted compaction leaves the partition untouched. ---- */
    {
        off_t s = p8_probe_compacted_vcol();
        printf("[8] uncapped compaction produces v.col=%lld bytes\n",
               (long long)s);
        ASSERT(s > 8192);
        /* (a) the value column's .tmp fails mid-write (checked safe_write); */
        phase8_case("cap-mid",  (rlim_t)(4096 + s / 2), 0, 0);
        /* (b) only the FINAL buffered tail is cut — the failure lands in the
         *     fflush that used to be discarded, so a truncated .col.tmp was
         *     renamed over live data under a COMPLETE .idx.tmp; */
        phase8_case("cap-tail", (rlim_t)(s - 1),        0, 0);
        /* (c) the value .col cannot be opened at all — a resource failure that
         *     compact_column_file reported as TSDB_OK/produced=0, so keying
         *     the abort on rc alone leaves it reachable; */
        phase8_case("unreadable", 0, 1, 0);
        /* (d) the value column takes its count-overflow bail: TSDB_OK, no .tmp
         *     pair, nothing in the return code — only the eligibility flag
         *     tells this apart from a column that was simply below threshold. */
        phase8_case("bail", 0, 0, (uint64_t)(P8_ROWS * 3));
    }

    rm_rf(dir);

    /* ---- Phase 9: a failed delete-watermark bump keeps the OLD watermark. -- */
    phase9_delwm();

    /* ---- Phase 10: what the engine ACKED is what survives a crash. -------- */
    phase10_crash_durability();

    /* ---- Phase 11: a commit that cannot log its rows must refuse. --------- */
    phase11_wal_full();

    printf("\n=== ENOSPC / write-failure durability TESTS PASSED ===\n");
    return 0;
}
