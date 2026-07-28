/* test_dup_ts_readpaths.c — the three read paths resolve a column's block
 * independently, so a fix that only corrects one leaves the other two serving
 * another block's bytes.
 *
 * All three ask the same non-unique question — "which entry of this column has
 * (ts_min, count) equal to the ts block's?" — and all three take the FIRST
 * answer:
 *
 *   scan             the per-column decode loop, once in tsdb_par_scan_task and
 *                    again in the serial executor      (src/query/exec.c)
 *   stats fast path  try_stats_fastpath                (src/query/exec.c)
 *   bloom            bloom_can_skip_block              (src/query/exec.c)
 *
 * Equal timestamps are accepted and kept in insertion order, and a flush splits
 * a partition into independent block_points chunks, so a single repeated
 * timestamp spanning more than one chunk produces genuinely distinct blocks with
 * identical (ts_min, ts_max, count).  Every one of them resolves to the first.
 *
 * What that costs each path:
 *
 *   scan / stats  a later block is answered with block zero's values, so the row
 *                 count stays right and every value is wrong.  rc is TSDB_OK.
 *   bloom         a later block is tested against block ZERO's bloom filter, so
 *                 a symbol that lives only in that later block is "definitely
 *                 not here" and the block is dropped before it is ever read.
 *                 The query returns no rows at all.
 *   ALTER         the sentinel padding that keeps an added column aligned is
 *                 itself duplicate-keyed, and the real blocks written after the
 *                 ALTER are too — so the added column reads its legitimate zeros
 *                 for the old rows and then the WRONG non-zero block for the new
 *                 ones.
 *
 * Scan and stats are asserted against each other as well as against the truth:
 * they must agree AND both be right.  Two paths that agree on a wrong answer is
 * still a wrong answer; two paths that disagree is a second defect.  Fixing only
 * the scan turns case [1] from "both wrong" into "the two paths disagree", and
 * leaves case [2] completely unchanged — which is the whole reason this file
 * exercises the three separately.
 *
 * Process model.  TSDB_DISABLE_STATS_FASTPATH is latched into a static on first
 * use (src/query/exec.c), so it cannot be flipped inside one process.  Every
 * unit of work below therefore runs in a fork()ed child that sets its own
 * environment before opening anything, and the parent never opens a database or
 * runs a query, so no child can inherit a latched decision.  TSDB_WAL_ONLY_COMMIT
 * is sampled at tsdb_open (src/storage/db.c) — the child sets it before opening,
 * so both durability modes get a real, non-vacuous assertion.  TSDB_QUERY_PARALLEL
 * is re-read per query, so one child can cover both executors.
 *
 * tsdb_open + tsdb_close costs a flat ~1s regardless of data volume, so each
 * child runs a whole list of queries against one open handle.
 */
#include "../include/tsdb.h"
#include "../src/query/exec.h"      /* tsdb_bloom_stats_skipped/total */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

#define DIR_AGG   "/tmp/tsdb_dup_readpaths_agg"
#define DIR_SYM   "/tmp/tsdb_dup_readpaths_sym"
#define DIR_ALT   "/tmp/tsdb_dup_readpaths_alt"

#define BLK   8192                              /* == TSDB_BLOCK_POINTS */
#define NBLK  3
#define TS0   1700000000000000000LL             /* 2023-11-14T22:13:20Z */
#define TS1   (TS0 + 3600000000000LL)           /* +1h — same DAY partition */

#define MAXQ  12

static int g_fail;

#define CHECK(cond, ...) do {                                                 \
    if (!(cond)) { printf("FAIL " __VA_ARGS__); printf("\n"); g_fail++; }     \
} while (0)

static long long tri(long long n) { return n * (n + 1) / 2; }

static void rm_dir(const char *d) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", d);
    if (system(cmd)) { /* best effort */ }
}

/* ---- child protocol ------------------------------------------------------ */

enum { SEED_NONE = 0, SEED_AGG, SEED_SYM, SEED_ALTER };

typedef struct {
    char sql[176];
    int  par_off;              /* TSDB_QUERY_PARALLEL=0 for this query */
} qspec_t;

typedef struct {
    const char *dir;
    int         wal_only;      /* TSDB_WAL_ONLY_COMMIT — sampled at tsdb_open */
    int         fp_off;        /* TSDB_DISABLE_STATS_FASTPATH — latched, so
                                * one value per child process */
    int         seed;          /* SEED_* — SEED_NONE means "run the queries" */
    int         nq;
    qspec_t     q[MAXQ];
} job_t;

typedef struct {
    int       rc;
    long long rows;
    long long sum0;            /* sum of result column 0 as i64 */
    long long sum1;            /* sum of result column 1 as i64 (0 if absent) */
    long long first0;          /* column 0 of the first row — the aggregate */
    long long bloom_skipped;   /* from the most recent serial SELECT */
    long long bloom_total;
} res_t;

typedef struct {
    int   ok;                  /* child reached the end without a setup error */
    int   nq;
    res_t q[MAXQ];
} batch_t;

static void job_q(job_t *j, int par_off, const char *fmt, ...) {
    if (j->nq >= MAXQ) { printf("FAIL MAXQ too small\n"); g_fail++; return; }
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(j->q[j->nq].sql, sizeof(j->q[j->nq].sql), fmt, ap);
    va_end(ap);
    j->q[j->nq].par_off = par_off;
    j->nq++;
}

/* ---- fixtures ------------------------------------------------------------ */

/* Every fixture writes chunks of exactly BLK rows and commits each chunk.  That
 * yields one block per chunk in BOTH durability modes: with flush-on-commit the
 * commit flushes BLK rows, and with the deferred mode the memtable is full at
 * block_points == BLK rows so the next chunk's first row_ts flushes it. */
static int wr_open(const char *dir, tsdb_db_t **db, const char *name,
                   const tsdb_col_t *cols, size_t ncols, tsdb_table_t **tbl)
{
    if (tsdb_open(dir, db) != TSDB_OK) return 0;
    if (tsdb_create_table(*db, name, cols, ncols, "ts") != TSDB_OK) return 0;
    if (tsdb_open_table(*db, name, tbl) != TSDB_OK) return 0;
    return 1;
}

/* [fixture] t(ts, v): NBLK chunks, EVERY row at TS0, v = 1..NBLK*BLK. */
static void seed_agg(const char *dir, batch_t *out) {
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    tsdb_db_t *db = NULL; tsdb_table_t *t = NULL;
    if (!wr_open(dir, &db, "t", cols, 2, &t)) { if (db) tsdb_close(db); return; }
    for (int blk = 0; blk < NBLK; blk++) {
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) { tsdb_close(db); return; }
        for (int k = 0; k < BLK; k++) {
            tsdb_batch_row_ts(b, TS0);
            tsdb_batch_row_i64(b, 1, (long long)blk * BLK + k + 1);
            tsdb_batch_row_end(b);
        }
        if (tsdb_batch_commit(b) != TSDB_OK) { tsdb_close(db); return; }
    }
    tsdb_close(db);                     /* close flushes the tail */
    out->ok = 1;
}

/* One table of the symbol fixture: a distinct SYMBOL per chunk, so 'ccc' exists
 * ONLY in the third block and is genuinely absent from block zero.
 *   dup=1  every chunk stamped TS0 — all three blocks key-identical
 *   dup=0  one timestamp per chunk — the control, same data, unambiguous. */
static int seed_sym_tbl(tsdb_db_t *db, const char *tbl, int dup) {
    static const char *SYMS[NBLK] = { "aaa", "bbb", "ccc" };
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP },
                          { "s",  TSDB_TYPE_SYMBOL },
                          { "v",  TSDB_TYPE_INT64 } };
    if (tsdb_create_table(db, tbl, cols, 3, "ts") != TSDB_OK) return 0;
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, tbl, &t) != TSDB_OK) return 0;
    for (int blk = 0; blk < NBLK; blk++) {
        int64_t ts = dup ? TS0 : TS0 + (int64_t)blk * 1000000LL;
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) return 0;
        for (int k = 0; k < BLK; k++) {
            tsdb_batch_row_ts(b, ts);
            tsdb_batch_row_sym(b, 1, SYMS[blk]);
            tsdb_batch_row_i64(b, 2, (long long)blk * BLK + k + 1);
            tsdb_batch_row_end(b);
        }
        if (tsdb_batch_commit(b) != TSDB_OK) return 0;
    }
    return 1;
}

/* [fixture] bt (duplicate timestamps) and ct (the control) side by side, so a
 * single open serves both and the control is guaranteed to have been built by
 * the same code in the same durability mode. */
static void seed_sym(const char *dir, batch_t *out) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) return;
    if (!seed_sym_tbl(db, "bt", 1)) { tsdb_close(db); return; }
    if (!seed_sym_tbl(db, "ct", 0)) { tsdb_close(db); return; }
    tsdb_close(db);
    out->ok = 1;
}

/* [fixture] altdup(ts, v), then ALTER ADD w, then MORE rows carrying real w
 * values:
 *
 *   blocks 0,1  ts=TS0  v=1..2*BLK           w does not exist yet
 *   ALTER TABLE altdup ADD COLUMN w INT64
 *   blocks 2,3  ts=TS1  v=2*BLK+1..4*BLK     w=1..2*BLK
 *
 * Both halves are duplicate-keyed runs.  w's block-meta array becomes two ALTER
 * sentinels (keyed TS0/BLK, legitimately zero) followed by two real blocks
 * (keyed TS1/BLK), so the ambiguity is present in the padding AND in the real
 * data — and the two must not be confused for each other. */
static void seed_alter(const char *dir, batch_t *out) {
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    tsdb_db_t *db = NULL; tsdb_table_t *t = NULL;
    if (!wr_open(dir, &db, "altdup", cols, 2, &t)) { if (db) tsdb_close(db); return; }
    for (int blk = 0; blk < 2; blk++) {
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) { tsdb_close(db); return; }
        for (int k = 0; k < BLK; k++) {
            tsdb_batch_row_ts(b, TS0);
            tsdb_batch_row_i64(b, 1, (long long)blk * BLK + k + 1);
            tsdb_batch_row_end(b);
        }
        if (tsdb_batch_commit(b) != TSDB_OK) { tsdb_close(db); return; }
    }
    tsdb_close(db); db = NULL;

    /* ALTER resolves through the open-table registry, so hold the table. */
    if (tsdb_open(dir, &db) != TSDB_OK) return;
    tsdb_table_t *th = NULL;
    if (tsdb_open_table(db, "altdup", &th) != TSDB_OK) { tsdb_close(db); return; }
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "ALTER TABLE altdup ADD COLUMN w INT64;", &r);
    if (r) tsdb_result_free(r);
    if (rc != TSDB_OK) { tsdb_close(db); return; }

    t = NULL;
    if (tsdb_open_table(db, "altdup", &t) != TSDB_OK) { tsdb_close(db); return; }
    for (int blk = 0; blk < 2; blk++) {
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) { tsdb_close(db); return; }
        for (int k = 0; k < BLK; k++) {
            long long i = (long long)blk * BLK + k + 1;
            tsdb_batch_row_ts(b, TS1);
            tsdb_batch_row_i64(b, 1, 2LL * BLK + i);   /* v continues */
            tsdb_batch_row_i64(b, 2, i);               /* w restarts at 1 */
            tsdb_batch_row_end(b);
        }
        if (tsdb_batch_commit(b) != TSDB_OK) { tsdb_close(db); return; }
    }
    tsdb_close(db);
    out->ok = 1;
}

/* ---- child + harness ----------------------------------------------------- */

static void child_body(const job_t *j, batch_t *out) {
    if (j->wal_only) setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    else             unsetenv("TSDB_WAL_ONLY_COMMIT");
    if (j->fp_off)   setenv("TSDB_DISABLE_STATS_FASTPATH", "1", 1);
    else             unsetenv("TSDB_DISABLE_STATS_FASTPATH");

    switch (j->seed) {
        case SEED_AGG:   seed_agg(j->dir, out);   return;
        case SEED_SYM:   seed_sym(j->dir, out);   return;
        case SEED_ALTER: seed_alter(j->dir, out); return;
        default: break;
    }

    tsdb_db_t *db = NULL;
    if (tsdb_open(j->dir, &db) != TSDB_OK) return;
    out->nq = j->nq;
    for (int i = 0; i < j->nq; i++) {
        /* Re-read per query by the executor, so it can vary inside a child. */
        if (j->q[i].par_off) setenv("TSDB_QUERY_PARALLEL", "0", 1);
        else                 unsetenv("TSDB_QUERY_PARALLEL");

        res_t *o = &out->q[i];
        tsdb_result_t *r = NULL;
        o->rc = tsdb_query(db, j->q[i].sql, &r);
        if (o->rc == TSDB_OK && r) {
            int nc = tsdb_result_ncols(r);
            while (tsdb_result_next(r) > 0) {
                if (o->rows == 0) o->first0 = tsdb_result_i64(r, 0);
                o->sum0 += tsdb_result_i64(r, 0);
                if (nc > 1) o->sum1 += tsdb_result_i64(r, 1);
                o->rows++;
            }
        }
        if (r) tsdb_result_free(r);
        o->bloom_skipped = (long long)tsdb_bloom_stats_skipped();
        o->bloom_total   = (long long)tsdb_bloom_stats_total();
    }
    tsdb_close(db);
    out->ok = 1;
}

/* Run one job in a fresh process; the results come back over a pipe. */
static void run_job(const job_t *j, batch_t *out) {
    memset(out, 0, sizeof(*out));
    int fds[2];
    if (pipe(fds) != 0) { printf("FAIL pipe\n"); g_fail++; return; }
    pid_t pid = fork();
    if (pid < 0) { close(fds[0]); close(fds[1]); printf("FAIL fork\n"); g_fail++; return; }
    if (pid == 0) {
        close(fds[0]);
        batch_t b;
        memset(&b, 0, sizeof(b));
        child_body(j, &b);
        if (write(fds[1], &b, sizeof(b)) != (ssize_t)sizeof(b)) { /* parent reports */ }
        close(fds[1]);
        _exit(0);
    }
    close(fds[1]);
    size_t got = 0;
    while (got < sizeof(*out)) {
        ssize_t n = read(fds[0], (char *)out + got, sizeof(*out) - got);
        if (n <= 0) break;
        got += (size_t)n;
    }
    close(fds[0]);
    int st = 0;
    waitpid(pid, &st, 0);
    if (got != sizeof(*out) || !out->ok) {
        printf("FAIL child died or failed setup (dir=%s seed=%d wal=%d fp_off=%d "
               "nq=%d)\n", j->dir, j->seed, j->wal_only, j->fp_off, j->nq);
        g_fail++;
        out->ok = 0;
    }
}

static void seed_in_child(const char *dir, int seed, int wal_only) {
    rm_dir(dir);
    job_t j;
    memset(&j, 0, sizeof(j));
    j.dir = dir; j.seed = seed; j.wal_only = wal_only;
    batch_t b;
    run_job(&j, &b);
}

/* ---- [1] scan vs stats fast path ---------------------------------------- */

/* Four legs per aggregate: {parallel, serial} x {stats allowed, stats forced
 * off}.  try_stats_fastpath is reached from BOTH executors, so a fix that only
 * corrects one of them shows up here as two legs disagreeing. */
static void t_scan_vs_stats(int wal_only) {
    printf("\n[1] scan vs stats fast path — %d blocks, ONE timestamp  (wal_only=%d)\n",
           NBLK, wal_only);
    seed_in_child(DIR_AGG, SEED_AGG, wal_only);

    static const struct { const char *sql; long long want; } Q[] = {
        { "SELECT count(*) FROM t", (long long)BLK * NBLK },
        { "SELECT min(v) FROM t",   1 },
        { "SELECT max(v) FROM t",   (long long)BLK * NBLK },
        { "SELECT sum(v) FROM t",   0 },   /* 0 => tri(BLK*NBLK), filled below */
    };
    const int NQ = (int)(sizeof(Q) / sizeof(Q[0]));
    static const char *LEG[4] = { "par+stats", "par+scan ", "ser+stats", "ser+scan " };

    /* Legs 0,2 share one child (stats allowed); legs 1,3 share the other
     * (stats forced off).  Within a child the parallel switch is per query. */
    batch_t out[2];
    for (int fp_off = 0; fp_off < 2; fp_off++) {
        job_t j;
        memset(&j, 0, sizeof(j));
        j.dir = DIR_AGG; j.wal_only = wal_only; j.fp_off = fp_off;
        for (int par_off = 0; par_off < 2; par_off++)
            for (int qi = 0; qi < NQ; qi++)
                job_q(&j, par_off, "%s", Q[qi].sql);
        run_job(&j, &out[fp_off]);
    }
    if (!out[0].ok || !out[1].ok) return;

    for (int qi = 0; qi < NQ; qi++) {
        long long want = Q[qi].want ? Q[qi].want : tri((long long)BLK * NBLK);
        /* leg index: 0 par+stats, 1 par+scan, 2 ser+stats, 3 ser+scan */
        const res_t *L[4] = { &out[0].q[qi], &out[1].q[qi],
                              &out[0].q[NQ + qi], &out[1].q[NQ + qi] };
        int all_ok = 1;
        for (int leg = 0; leg < 4; leg++) {
            if (L[leg]->rc != TSDB_OK || L[leg]->rows != 1) {
                printf("FAIL [1] wal=%d %s %s: rc=%d rows=%lld\n",
                       wal_only, Q[qi].sql, LEG[leg], L[leg]->rc, L[leg]->rows);
                g_fail++;
                all_ok = 0;
            }
        }
        if (!all_ok) continue;
        printf("  %-24s want=%-10lld  par+stats=%-10lld par+scan=%-10lld "
               "ser+stats=%-10lld ser+scan=%lld\n",
               Q[qi].sql, want, L[0]->first0, L[1]->first0,
               L[2]->first0, L[3]->first0);
        for (int leg = 1; leg < 4; leg++)
            CHECK(L[leg]->first0 == L[0]->first0,
                  "[1] wal=%d %s: %s=%lld disagrees with %s=%lld — the stats fast "
                  "path and the scan must never resolve to different blocks",
                  wal_only, Q[qi].sql, LEG[leg], L[leg]->first0,
                  LEG[0], L[0]->first0);
        for (int leg = 0; leg < 4; leg++)
            CHECK(L[leg]->first0 == want,
                  "[1] wal=%d %s %s = %lld want %lld — a repeated timestamp made "
                  "distinct blocks share (ts_min,count) and block zero served them all",
                  wal_only, Q[qi].sql, LEG[leg], L[leg]->first0, want);
    }
}

/* ---- [2] bloom ----------------------------------------------------------- */

/* One symbol per block.  'ccc' is genuinely absent from block zero, so testing
 * block two against block ZERO's bloom filter says "definitely not here" and
 * drops the one block that holds every matching row.
 *
 * These are non-aggregate SELECTs on purpose: bloom_can_skip_block lives in the
 * serial executor, and an aggregate over more than one source takes the parallel
 * path instead, where the bloom pre-check never runs at all. */
static void t_bloom(int wal_only) {
    printf("\n[2] bloom — a SYMBOL that lives only in a later identical-key block"
           "  (wal_only=%d)\n", wal_only);
    seed_in_child(DIR_SYM, SEED_SYM, wal_only);

    job_t j;
    memset(&j, 0, sizeof(j));
    j.dir = DIR_SYM; j.wal_only = wal_only;
    job_q(&j, 0, "SELECT v FROM ct WHERE s = 'ccc'");   /* control */
    job_q(&j, 0, "SELECT v FROM bt WHERE s = 'ccc'");
    job_q(&j, 0, "SELECT v FROM bt WHERE s = 'aaa'");
    batch_t out;
    run_job(&j, &out);
    if (!out.ok) return;

    long long want_ccc = tri((long long)BLK * NBLK) - tri(2LL * BLK);
    long long want_aaa = tri((long long)BLK);

    /* Control: same data, one timestamp per block, so pairing is unambiguous.
     * This must PASS on HEAD — it proves the 64-bit bloom is sparse enough to
     * prune here, so the failure below is the pairing and not the filter. */
    const res_t *c = &out.q[0];
    printf("  control ct (distinct ts), s='ccc': rows=%lld (want %d)  sum=%lld "
           "(want %lld)  bloom skipped=%lld/%lld  rc=%d\n",
           c->rows, BLK, c->sum0, want_ccc, c->bloom_skipped, c->bloom_total, c->rc);
    CHECK(c->rc == TSDB_OK, "[2] wal=%d control rc=%d", wal_only, c->rc);
    CHECK(c->rows == BLK, "[2] wal=%d control rows=%lld want %d",
          wal_only, c->rows, BLK);
    CHECK(c->sum0 == want_ccc, "[2] wal=%d control sum=%lld want %lld",
          wal_only, c->sum0, want_ccc);
    CHECK(c->bloom_total > 0,
          "[2] wal=%d control examined %lld blocks — the bloom pre-check never ran, "
          "so the assertions below would be vacuous", wal_only, c->bloom_total);
    CHECK(c->bloom_skipped > 0,
          "[2] wal=%d control skipped %lld blocks — the filter pruned nothing, so a "
          "'not skipped' assertion below proves nothing", wal_only, c->bloom_skipped);

    /* The real thing: three blocks with identical (ts_min, ts_max, count). */
    const res_t *r = &out.q[1];
    printf("  dup ts bt, s='ccc': rows=%lld (want %d)  sum=%lld (want %lld)  "
           "bloom skipped=%lld/%lld  rc=%d\n",
           r->rows, BLK, r->sum0, want_ccc, r->bloom_skipped, r->bloom_total, r->rc);
    CHECK(r->rc == TSDB_OK, "[2] wal=%d rc=%d", wal_only, r->rc);
    CHECK(r->bloom_total > 0, "[2] wal=%d bloom pre-check never ran (total=%lld)",
          wal_only, r->bloom_total);
    CHECK(r->bloom_skipped < r->bloom_total,
          "[2] wal=%d all %lld blocks were bloom-skipped — block two was tested "
          "against block ZERO's filter, which has no 'ccc'",
          wal_only, r->bloom_total);
    CHECK(r->rows == BLK,
          "[2] wal=%d s='ccc' returned %lld rows, want %d — the only block holding "
          "'ccc' was dropped before it was ever read", wal_only, r->rows, BLK);
    CHECK(r->sum0 == want_ccc, "[2] wal=%d s='ccc' sum=%lld want %lld",
          wal_only, r->sum0, want_ccc);

    /* The other direction: 'aaa' IS in block zero, so nothing is bloom-skipped
     * and every block then decodes block zero's symbols — three blocks' worth of
     * rows all claiming to be 'aaa'. */
    const res_t *a = &out.q[2];
    printf("  dup ts bt, s='aaa': rows=%lld (want %d)  sum=%lld (want %lld)  "
           "bloom skipped=%lld/%lld  rc=%d\n",
           a->rows, BLK, a->sum0, want_aaa, a->bloom_skipped, a->bloom_total, a->rc);
    CHECK(a->rc == TSDB_OK, "[2] wal=%d 'aaa' rc=%d", wal_only, a->rc);
    CHECK(a->rows == BLK,
          "[2] wal=%d s='aaa' returned %lld rows, want %d — later blocks decoded "
          "block zero's symbol column and every row matched", wal_only, a->rows, BLK);
    CHECK(a->sum0 == want_aaa, "[2] wal=%d s='aaa' sum=%lld want %lld",
          wal_only, a->sum0, want_aaa);
}

/* ---- [3] ALTER with NON-ZERO data after it ------------------------------- */

static void t_alter_nonzero(int wal_only) {
    printf("\n[3] ALTER-added column with real values in later blocks  (wal_only=%d)\n",
           wal_only);
    seed_in_child(DIR_ALT, SEED_ALTER, wal_only);

    long long half    = 2LL * BLK;
    long long want_v0 = tri(half);                    /* v over the pre-ALTER rows  */
    long long want_v1 = tri(4LL * BLK) - tri(half);   /* v over the post-ALTER rows */
    long long want_w  = tri(half);                    /* w over the whole table     */

    batch_t out[2];
    for (int fp_off = 0; fp_off < 2; fp_off++) {
        job_t j;
        memset(&j, 0, sizeof(j));
        j.dir = DIR_ALT; j.wal_only = wal_only; j.fp_off = fp_off;
        job_q(&j, 0, "SELECT v, w FROM altdup WHERE ts = %lld", (long long)TS0);
        job_q(&j, 0, "SELECT v, w FROM altdup WHERE ts = %lld", (long long)TS1);
        job_q(&j, 0, "SELECT sum(w) FROM altdup");
        run_job(&j, &out[fp_off]);
    }
    if (!out[0].ok || !out[1].ok) return;

    /* Rows written BEFORE the ALTER: v is real, w is legitimately zero. */
    const res_t *old = &out[0].q[0];
    printf("  pre-ALTER rows (ts=TS0):  rows=%lld (want %lld)  sum(v)=%lld (want %lld)"
           "  sum(w)=%lld (want 0)  rc=%d\n",
           old->rows, half, old->sum0, want_v0, old->sum1, old->rc);
    CHECK(old->rc == TSDB_OK, "[3] wal=%d pre-ALTER rc=%d", wal_only, old->rc);
    CHECK(old->rows == half, "[3] wal=%d pre-ALTER rows=%lld want %lld",
          wal_only, old->rows, half);
    CHECK(old->sum0 == want_v0,
          "[3] wal=%d pre-ALTER sum(v)=%lld want %lld — the duplicate-keyed run "
          "served block zero twice", wal_only, old->sum0, want_v0);
    CHECK(old->sum1 == 0,
          "[3] wal=%d pre-ALTER sum(w)=%lld want 0 — a column added after these rows "
          "were written is legitimately zero for them, and the sentinel padding must "
          "not be mistaken for real data", wal_only, old->sum1);

    /* Rows written AFTER the ALTER: both columns carry real, distinct values. */
    const res_t *nw = &out[0].q[1];
    printf("  post-ALTER rows (ts=TS1): rows=%lld (want %lld)  sum(v)=%lld (want %lld)"
           "  sum(w)=%lld (want %lld)  rc=%d\n",
           nw->rows, half, nw->sum0, want_v1, nw->sum1, want_w, nw->rc);
    CHECK(nw->rc == TSDB_OK, "[3] wal=%d post-ALTER rc=%d", wal_only, nw->rc);
    CHECK(nw->rows == half, "[3] wal=%d post-ALTER rows=%lld want %lld",
          wal_only, nw->rows, half);
    CHECK(nw->sum0 == want_v1, "[3] wal=%d post-ALTER sum(v)=%lld want %lld",
          wal_only, nw->sum0, want_v1);
    CHECK(nw->sum1 == want_w,
          "[3] wal=%d post-ALTER sum(w)=%lld want %lld — the second post-ALTER block "
          "shares (ts_min,count) with the first and was answered with its values",
          wal_only, nw->sum1, want_w);

    /* Whole-table aggregate over the added column, both legs.  The ALTER
     * sentinels carry no stats, so this also checks that a sentinel is never
     * mistaken for a stats-bearing block. */
    for (int leg = 0; leg < 2; leg++) {
        const res_t *s = &out[leg].q[2];
        printf("  sum(w) over whole table [%s]: %lld (want %lld) rows=%lld rc=%d\n",
               leg ? "scan " : "stats", s->first0, want_w, s->rows, s->rc);
        CHECK(s->rc == TSDB_OK && s->rows == 1,
              "[3] wal=%d sum(w) leg=%d rc=%d rows=%lld", wal_only, leg, s->rc, s->rows);
        CHECK(s->first0 == want_w, "[3] wal=%d sum(w) [%s] = %lld want %lld",
              wal_only, leg ? "scan" : "stats", s->first0, want_w);
    }
    CHECK(out[0].q[2].first0 == out[1].q[2].first0,
          "[3] wal=%d sum(w): stats=%lld disagrees with scan=%lld", wal_only,
          out[0].q[2].first0, out[1].q[2].first0);
}

int main(void) {
    printf("=== dup-ts read paths: scan / stats fast path / bloom / ALTER ===\n");
    for (int wal_only = 0; wal_only <= 1; wal_only++) {
        printf("\n---- TSDB_WAL_ONLY_COMMIT=%d ----\n", wal_only);
        t_scan_vs_stats(wal_only);
        t_bloom(wal_only);
        t_alter_nonzero(wal_only);
    }
    rm_dir(DIR_AGG); rm_dir(DIR_SYM); rm_dir(DIR_ALT);
    printf("\n=== %s ===\n", g_fail ? "FAILED" : "all dup-ts read-path tests passed");
    return g_fail ? 1 : 0;
}
