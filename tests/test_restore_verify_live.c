/* test_restore_verify_live.c — tsdb_restore_verify against a database that is
 * NOT a frozen copy of the set it is being checked against.
 *
 * Three properties, all of which the round-2 verify broke the moment it started
 * force-draining every table the manifest names:
 *
 * [Y] A LIVE SOURCE IS NOT A CORRUPT SET.  Verify used to answer
 *     TSDB_ERR_CORRUPT for an INTACT set the moment the source took one more
 *     write — measured rows_backup=2000 vs rows_target=2700 -> rc=-4 — and set
 *     blocks_mismatched=1 on the way out, a mismatch it had not found and could
 *     not have found.  An operator asking "did my data survive?" during an
 *     incident must not be told CORRUPT because the node kept ingesting.  The
 *     rc now answers CONTAINMENT and the equality answer rides beside it in
 *     target_rel.
 *
 * [Z] A DIAGNOSTIC MUST NOT ARM A REFUSAL, AND MUST NOT REPLICATE.  The drain
 *     published blocks with the TARGET's (ts_min,count) boundaries, which is
 *     exactly the state tsdb_restore_run's re-run gate refuses on: a verify
 *     turned a restore that returns TSDB_OK into one that returns
 *     TSDB_ERR_UNSUPPORTED.  It also fired the cluster replication hook, so on
 *     a cluster the check shipped rows.  Both are measured against a control
 *     that runs the same restore without the verify.
 *
 *     RUN TWICE: once on a target nobody re-encoded, and once after the
 *     database's OWN compactor has merged the partition.  The bound that fixes
 *     the force-drain was `dg.rows < manifest.rows || dg.blocks <
 *     manifest.blocks`, and the second disjunct is true of every partition the
 *     compactor has touched — fewer blocks, same rows.  tsdb_node_main starts
 *     the compactor on a 5 s timer, so "the compactor has touched it" is the
 *     steady state of every partition nobody is writing: the drain fired on
 *     healthy nodes, and an intact set was reported TSDB_ERR_CORRUPT / BEHIND
 *     with a blocks_missing that was a difference of totals, not a measurement.
 *     A memtable and a redo log move ROWS; the block count is not an axis
 *     either of them can be short on.
 *
 * [V] ...AND THE DRAIN MUST STILL FIRE WHEN THE TARGET IS BEHIND.  The bound
 *     that fixes [Z] must not re-open the false alarm the drain exists for: a
 *     node that died with its acked rows in the redo log and nothing in its
 *     partitions digests 0 against a manifest of 1000.  When it does fire the
 *     flush MUST replicate: a memtable flushes exactly once, so a drain with
 *     skip_replicate=1 would not stop the check replicating, it would delete
 *     those rows from the replication stream for good.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define DAY0   1700000000000000000LL
#define NBASE  2000
#define NMORE  700

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* Recursive copy of dirs + regular files.  [V] needs a byte-for-byte second
 * copy of a data dir whose rows are all still in its redo log. */
static void cp_tree(const char *src, const char *dst) {
    struct stat st;
    if (stat(src, &st) != 0) FAIL("cp_tree: stat %s: %s", src, strerror(errno));
    if (S_ISDIR(st.st_mode)) {
        if (mkdir(dst, 0755) != 0 && errno != EEXIST)
            FAIL("cp_tree: mkdir %s: %s", dst, strerror(errno));
        DIR *d = opendir(src);
        if (!d) FAIL("cp_tree: opendir %s", src);
        struct dirent *e;
        while ((e = readdir(d))) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
            char a[4096], b[4096];
            snprintf(a, sizeof(a), "%s/%s", src, e->d_name);
            snprintf(b, sizeof(b), "%s/%s", dst, e->d_name);
            cp_tree(a, b);
        }
        closedir(d);
        return;
    }
    if (!S_ISREG(st.st_mode)) return;
    int in = open(src, O_RDONLY);
    if (in < 0) FAIL("cp_tree: open %s", src);
    int out = open(dst, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (out < 0) { close(in); FAIL("cp_tree: create %s", dst); }
    char b[65536]; ssize_t r;
    while ((r = read(in, b, sizeof(b))) > 0) {
        ssize_t off = 0;
        while (off < r) {
            ssize_t w = write(out, b + off, (size_t)(r - off));
            if (w <= 0) { close(in); close(out); FAIL("cp_tree: write %s", dst); }
            off += w;
        }
    }
    close(in); close(out);
}

/* Flip a byte inside the FIRST block's compressed payload of <table>/<part>/
 * <col>.col — the 32-byte BlockHeader precedes it, so no metadata moves and the
 * index level stays blind by design.  Only the deep level's byte-compare and
 * CRC see it. */
static int corrupt_first_block(const char *data_dir, const char *table,
                               const char *col)
{
    char tbl[4096];
    snprintf(tbl, sizeof(tbl), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl);
    if (!d) return -1;
    char part[32] = {0};
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t k = strlen(e->d_name);
        if (k != 8 && k != 10) continue;
        int all = 1;
        for (size_t i = 0; i < k; i++)
            if (e->d_name[i] < '0' || e->d_name[i] > '9') all = 0;
        if (all && (!part[0] || strcmp(e->d_name, part) < 0))
            snprintf(part, sizeof(part), "%s", e->d_name);
    }
    closedir(d);
    if (!part[0]) return -1;

    char cp[4400];
    snprintf(cp, sizeof(cp), "%s/%s/%s/%s.col", data_dir, table, part, col);
    int fd = open(cp, O_RDWR);
    if (fd < 0) return -1;
    uint8_t b = 0;
    if (pread(fd, &b, 1, 40) != 1) { close(fd); return -1; }
    b ^= 0xFF;
    if (pwrite(fd, &b, 1, 40) != 1) { close(fd); return -1; }
    fsync(fd); close(fd);
    return 0;
}

/* on_replicate counter — the only witness that says whether a check shipped
 * rows to peers.  Registered with tsdb_db_set_hooks and no raw-block hook, so
 * flush_and_clear_locked's row-level path is the one that fires. */
static int g_repl;
static int repl_hook(void *ud, tsdb_db_t *db, const char *table,
                     tsdb_schema_t *s, tsdb_memtable_t *m) {
    (void)ud; (void)db; (void)table; (void)s; (void)m;
    g_repl++;
    return TSDB_OK;
}

static void write_rows(tsdb_table_t *t, int from, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = from; i < from + n; i++) {
        OK(tsdb_batch_row_ts(b, DAY0 + (int64_t)i * 1000000LL));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_i64(b, 2, i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

static tsdb_db_t *fresh_db(const char *dir, tsdb_table_t **out_t) {
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_FLOAT64   },
        { "n",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "m", cols, 3, "ts"));
    OK(tsdb_open_table(db, "m", out_t));
    return db;
}

/* Find table 'm' in a verify report. */
static const tsdb_restore_verify_table_t *row_m(const tsdb_restore_verify_t *v) {
    for (int i = 0; i < v->ntables; i++)
        if (!strcmp(v->tables[i].table, "m")) return &v->tables[i];
    return NULL;
}

static void print_row(const char *tag, int rc, const tsdb_restore_verify_t *v) {
    const tsdb_restore_verify_table_t *r = row_m(v);
    printf("%s rc=%d (%s) nfailed=%d", tag, rc, tsdb_errstr(rc), v->nfailed);
    if (r)
        printf("  m: rc=%d rel=%s rows_backup=%llu rows_target=%llu "
               "missing=%llu mismatched=%llu drained=%d",
               r->rc, tsdb_restore_target_rel_name(r->target_rel),
               (unsigned long long)r->rows_backup,
               (unsigned long long)r->rows_target,
               (unsigned long long)r->blocks_missing,
               (unsigned long long)r->blocks_mismatched, r->drained);
    printf("\n");
}

/* ---- [Y] a live source is not a corrupt set ------------------------------ */

#define Y_DIR "/tmp/tsdb_vlive_y"
#define Y_BK  "/tmp/tsdb_vlive_y_bk"

static void case_live_source(void) {
    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(Y_DIR, &t);

    write_rows(t, 0, NBASE);
    rm_rf(Y_BK);
    OK(tsdb_backup_create(db, Y_BK, NULL));

    /* The node keeps ingesting, the way a node under an incident does.  The
     * explicit flush puts the extra rows on DISK so the case behaves the same
     * in both durability modes — under deferred flush they would otherwise sit
     * in the memtable and the target would still read EQUAL. */
    write_rows(t, NBASE, NMORE);
    OK(tsdb_table_flush(db, "m"));

    for (int deep = 0; deep <= 1; deep++) {
        tsdb_restore_verify_t v;
        memset(&v, 0, sizeof(v));
        int rc = tsdb_restore_verify(db, Y_BK,
                                     deep ? TSDB_RESTORE_VERIFY_DEEP
                                          : TSDB_RESTORE_VERIFY_INDEX, &v);
        print_row(deep ? "[Y] verify(deep) " : "[Y] verify(index)", rc, &v);

        const tsdb_restore_verify_table_t *r = row_m(&v);
        if (!r) FAIL("[Y] the %s verify did not report table 'm'",
                     deep ? "deep" : "index");
        if (r->rows_target <= r->rows_backup)
            FAIL("[Y] setup is not testing anything: rows_target=%llu is not "
                 "ahead of rows_backup=%llu",
                 (unsigned long long)r->rows_target,
                 (unsigned long long)r->rows_backup);
        if (rc != TSDB_OK || r->rc != TSDB_OK)
            FAIL("[Y] an INTACT set was reported %s on a source that kept "
                 "ingesting: %s verify rc=%d table rc=%d, rows_backup=%llu "
                 "rows_target=%llu.  'The node has more rows than the set' is "
                 "not set corruption",
                 tsdb_errstr(rc), deep ? "deep" : "index", rc, r->rc,
                 (unsigned long long)r->rows_backup,
                 (unsigned long long)r->rows_target);
        if (r->target_rel != TSDB_RESTORE_TARGET_AHEAD)
            FAIL("[Y] %s verify reported target_rel=%s, want ahead — the "
                 "equality answer is what an operator reads to see the node "
                 "moved on", deep ? "deep" : "index",
                 tsdb_restore_target_rel_name(r->target_rel));
        if (r->blocks_missing || r->blocks_mismatched)
            FAIL("[Y] %s verify claimed missing=%llu mismatched=%llu against a "
                 "set whose every block is still on disk",
                 deep ? "deep" : "index",
                 (unsigned long long)r->blocks_missing,
                 (unsigned long long)r->blocks_mismatched);
        if (r->drained)
            FAIL("[Y] %s verify DRAINED a target that already held everything "
                 "the set carries — a check must not write to a database it "
                 "has no question about", deep ? "deep" : "index");
        tsdb_restore_verify_free(&v);
    }

    /* AHEAD MUST NOT BECOME A BLIND SPOT.  Making "the target holds more" a
     * non-failure is only safe if the level that CAN prove containment still
     * does.  Rot one block the set carries and re-ask: the index level stays
     * blind (no metadata moved) and reports ahead, the deep level must still
     * fail.  Without this, "AHEAD -> rc=TSDB_OK" is a licence to certify a
     * damaged set on any node that kept ingesting. */
    tsdb_close(db);
    if (corrupt_first_block(Y_DIR, "m", "v") != 0)
        FAIL("[Y] could not corrupt a block");
    OK(tsdb_open(Y_DIR, &db));

    {
        tsdb_restore_verify_t vi;
        memset(&vi, 0, sizeof(vi));
        int irc = tsdb_restore_verify(db, Y_BK, TSDB_RESTORE_VERIFY_INDEX, &vi);
        print_row("[Y] rot index    ", irc, &vi);
        if (irc != TSDB_OK)
            FAIL("[Y] the index level is supposed to be blind to payload rot — "
                 "a flipped payload byte moves no block metadata");
        tsdb_restore_verify_free(&vi);

        tsdb_restore_verify_t vd;
        memset(&vd, 0, sizeof(vd));
        int drc = tsdb_restore_verify(db, Y_BK, TSDB_RESTORE_VERIFY_DEEP, &vd);
        print_row("[Y] rot deep     ", drc, &vd);
        const tsdb_restore_verify_table_t *r = row_m(&vd);
        if (!r) FAIL("[Y] rot deep verify did not report table 'm'");
        if (r->target_rel != TSDB_RESTORE_TARGET_AHEAD)
            FAIL("[Y] rot deep verify reported target_rel=%s, want ahead",
                 tsdb_restore_target_rel_name(r->target_rel));
        if (drc == TSDB_OK || r->rc == TSDB_OK)
            FAIL("[Y] a set whose block was rotted on an AHEAD target was "
                 "certified clean (verify rc=%d table rc=%d) — 'the target has "
                 "more rows' must not switch off the containment check",
                 drc, r->rc);
        if (r->blocks_mismatched + r->blocks_missing == 0)
            FAIL("[Y] rot deep verify failed without naming a bad block");
        tsdb_restore_verify_free(&vd);
    }

    tsdb_close(db);
    printf("[Y] an intact set on a still-ingesting source verifies clean at "
           "both levels; a rotted one on the SAME ahead source still fails "
           "deep\n");
}

/* ---- [Z] a diagnostic must not arm a refusal, and must not replicate ----- */

/* One commit is one block per column, and the compactor only rewrites a column
 * carrying at least COMPACT_THRESHOLD_DEFAULT (16) of them. */
#define ZCHUNKS 20

/* <dir>/<table>/<lowest partition>. */
static void first_part_dir(const char *dir, const char *table,
                           char *out, size_t cap) {
    char tbl[4096];
    snprintf(tbl, sizeof(tbl), "%s/%s", dir, table);
    DIR *d = opendir(tbl);
    if (!d) FAIL("first_part_dir: opendir %s", tbl);
    char best[64] = {0};
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t k = strlen(e->d_name);
        if (k != 8 && k != 10) continue;
        int all = 1;
        for (size_t i = 0; i < k; i++)
            if (e->d_name[i] < '0' || e->d_name[i] > '9') all = 0;
        if (all && (!best[0] || strcmp(e->d_name, best) < 0))
            snprintf(best, sizeof(best), "%s", e->d_name);
    }
    closedir(d);
    if (!best[0]) FAIL("first_part_dir: no partition under %s", tbl);
    snprintf(out, cap, "%s/%s", tbl, best);
}

/* The database's OWN compactor, stock options, driven synchronously — the same
 * thing the background thread does on its 5 s timer. */
static void run_compactor(tsdb_db_t *db, const char *dir) {
    char part[4200];
    first_part_dir(dir, "m", part, sizeof(part));

    /* The compactor skips partitions touched in the last 60 s ("still hot").
     * Age this one so the pass behaves the way it does on any partition the
     * operator is not actively writing — i.e. all of them, a minute later. */
    struct timeval tv[2];
    tv[0].tv_sec = time(NULL) - 3600; tv[0].tv_usec = 0;
    tv[1] = tv[0];
    if (utimes(part, tv) != 0) FAIL("[Z] utimes(%s): %s", part, strerror(errno));

    tsdb_compactor_opts_t co;
    memset(&co, 0, sizeof(co));
    /* DEFAULT threshold (16) — no test-only tuning. */
    co.worker_threads = -1;                 /* manual: no background thread */
    tsdb_compactor_t *c = NULL;
    OK(tsdb_compactor_start(db, &co, &c));
    OK(tsdb_compactor_run_once(c));
    tsdb_compactor_stats_t cs;
    tsdb_compactor_stats(c, &cs);
    tsdb_compactor_stop(c);
    printf("[Z] compactor: cols_rewritten=%llu parts_merged=%llu\n",
           (unsigned long long)cs.compactions_done,
           (unsigned long long)cs.parts_merged);
    if (cs.compactions_done == 0)
        FAIL("[Z] the compactor rewrote nothing — the compacted half of this "
             "case would be identical to the other half and prove nothing");
}

/* with_verify == 0 is the control: the same database, the same restore, no
 * verify in between.  Whatever it returns is what the verify must not change.
 * compact != 0 runs the database's own compactor over the restored layout
 * first, so the target holds every row the set carries in FEWER blocks. */
static int case_arms_refusal(int with_verify, int compact, int *out_repl) {
    char dir[64], bk[64];
    snprintf(dir, sizeof(dir), "/tmp/tsdb_vlive_z%d%d", compact, with_verify);
    snprintf(bk,  sizeof(bk),  "/tmp/tsdb_vlive_z%d%d_bk", compact, with_verify);

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(dir, &t);
    for (int k = 0; k < ZCHUNKS; k++) {
        write_rows(t, k * (NBASE / ZCHUNKS), NBASE / ZCHUNKS);
        OK(tsdb_table_flush(db, "m"));      /* one block per column, per chunk */
    }
    rm_rf(bk);
    OK(tsdb_backup_create(db, bk, NULL));

    /* Acked, in the memtable, NOT on disk — the deferred-flush steady state.
     * The re-run gate reads the idx, so it does not see these and the restore
     * below is a legitimate no-op re-run: every block of the stream is already
     * on disk and gets skipped. */
    write_rows(t, NBASE, NMORE);

    if (compact) run_compactor(db, dir);

    g_repl = 0;
    tsdb_db_set_hooks(db, repl_hook, NULL, NULL);

    if (with_verify) {
        tsdb_restore_verify_t v;
        memset(&v, 0, sizeof(v));
        int vrc = tsdb_restore_verify(db, bk, TSDB_RESTORE_VERIFY_DEEP, &v);
        print_row(compact ? "[Z] verify(deep,compacted)" : "[Z] verify(deep)      ",
                  vrc, &v);
        const tsdb_restore_verify_table_t *r = row_m(&v);
        if (!r) FAIL("[Z] verify did not report table 'm'");
        if (r->drained)
            FAIL("[Z] verify DRAINED a target holding every row the set carries "
                 "(%llu on disk against a manifest of %llu)%s.  A drain moves "
                 "ROWS out of a memtable and there are none missing — it wrote "
                 "to the database, fired replication, and published block "
                 "boundaries a later tsdb_restore_run refuses on",
                 (unsigned long long)r->rows_target,
                 (unsigned long long)r->rows_backup,
                 compact ? ", in fewer BLOCKS because the compactor merged them"
                         : "");
        if (r->target_rel == TSDB_RESTORE_TARGET_BEHIND)
            FAIL("[Z] verify called an intact target BEHIND (missing=%llu) — "
                 "%s", (unsigned long long)r->blocks_missing,
                 compact ? "the compactor merged its blocks, it did not lose any"
                         : "it holds every row and block the set carries");
        if (vrc != TSDB_OK || r->rc != TSDB_OK)
            FAIL("[Z] verify of an intact set on a live source rc=%d (%s) "
                 "table rc=%d rel=%s", vrc, tsdb_errstr(vrc), r->rc,
                 tsdb_restore_target_rel_name(r->target_rel));
        if (compact && r->target_rel == TSDB_RESTORE_TARGET_EQUAL)
            FAIL("[Z] the compacted target still folds EQUAL to the set — the "
                 "compaction did not change the layout, so this half of the "
                 "case is vacuous");
        tsdb_restore_verify_free(&v);
    }
    *out_repl = g_repl;

    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rrc = tsdb_restore_run(db, bk, &rep);
    printf("[Z] restore_run %-18s rc=%d (%s) nfailed=%d%s\n",
           with_verify ? "after a verify" : "with NO verify", rrc,
           tsdb_errstr(rrc), rep.nfailed, compact ? "  (compacted)" : "");
    tsdb_restore_report_free(&rep);

    tsdb_db_set_hooks(db, NULL, NULL, NULL);
    tsdb_close(db);
    rm_rf(dir); rm_rf(bk);
    return rrc;
}

/* ---- [V] the drain must still fire when the target is behind ------------- */

#define V_SRC "/tmp/tsdb_vlive_v_src"
#define V_TGT "/tmp/tsdb_vlive_v_tgt"
#define V_BK  "/tmp/tsdb_vlive_v_bk"

/* Commit NBASE rows and die without closing: acked (WAL fsync + memtable) and
 * nothing in a partition. */
static void child_write_and_die(void) {
    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(V_SRC, &t);
    write_rows(t, 0, NBASE);
    fflush(NULL);
    _exit(0);
}

static void case_behind_target_is_drained(void) {
    rm_rf(V_SRC); rm_rf(V_TGT); rm_rf(V_BK);

    fflush(NULL);            /* the child must not re-emit this run's output */
    pid_t pid = fork();
    if (pid == 0) { child_write_and_die(); _exit(1); }
    int st = 0; waitpid(pid, &st, 0);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
        FAIL("[V] writer child did not exit cleanly (status %d)", st);

    /* A second node in exactly that state: rows in the redo log, partitions
     * empty.  Taking the backup below flushes V_SRC, so the copy has to be
     * made first. */
    cp_tree(V_SRC, V_TGT);

    {
        tsdb_db_t *src = NULL;
        OK(tsdb_open(V_SRC, &src));
        OK(tsdb_backup_create(src, V_BK, NULL));
        tsdb_close(src);
    }

    tsdb_db_t *tgt = NULL;
    OK(tsdb_open(V_TGT, &tgt));          /* table 'm' NOT opened by this call */
    g_repl = 0;
    tsdb_db_set_hooks(tgt, repl_hook, NULL, NULL);

    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int rc = tsdb_restore_verify(tgt, V_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    print_row("[V] verify(deep)", rc, &v);
    const tsdb_restore_verify_table_t *r = row_m(&v);
    if (!r) FAIL("[V] verify did not report table 'm'");

    if (!r->drained)
        FAIL("[V] verify did NOT drain a target holding 0 on-disk rows against "
             "a manifest of %llu — the crash-recovery false alarm is back: a "
             "node whose acked rows are all in its redo log would be reported "
             "%s", (unsigned long long)r->rows_backup, tsdb_errstr(rc));
    if (rc != TSDB_OK || r->rc != TSDB_OK)
        FAIL("[V] a node whose rows are durable in its redo log was reported "
             "rc=%d (%s), table rc=%d rel=%s rows_target=%llu", rc,
             tsdb_errstr(rc), r->rc,
             tsdb_restore_target_rel_name(r->target_rel),
             (unsigned long long)r->rows_target);
    if (r->rows_target != r->rows_backup)
        FAIL("[V] after the drain the target holds %llu rows against the set's "
             "%llu", (unsigned long long)r->rows_target,
             (unsigned long long)r->rows_backup);
    if (r->blocks_missing || r->blocks_mismatched)
        FAIL("[V] deep verify after the drain: missing=%llu mismatched=%llu",
             (unsigned long long)r->blocks_missing,
             (unsigned long long)r->blocks_mismatched);

    /* The drain is a real flush and a memtable flushes exactly ONCE.  If it
     * ever runs with skip_replicate=1 these rows leave the memtable without
     * ever being offered to a peer — the check would not "stop replicating",
     * it would delete them from the replication stream for good. */
    if (g_repl == 0)
        FAIL("[V] the drain flushed %llu rows without firing on_replicate — a "
             "memtable flushes once, so those rows are now gone from the "
             "replication stream for good",
             (unsigned long long)r->rows_target);
    printf("[V] the drain fired (on_replicate x%d) and the behind target "
           "verified clean\n", g_repl);

    tsdb_restore_verify_free(&v);
    tsdb_db_set_hooks(tgt, NULL, NULL, NULL);
    tsdb_close(tgt);
}

int main(void) {
    printf("=== test_restore_verify_live ===\n");

    /* [Y] runs in whatever mode the harness selected, so both are covered. */
    case_live_source();

    /* [Z] and [V] are ABOUT the deferred-flush steady state — rows acked into a
     * memtable and not on disk — so they pin the mode the cluster runs. */
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);

    for (int compact = 0; compact <= 1; compact++) {
        int repl_control = 0, repl_probe = 0;
        int rc_control = case_arms_refusal(0, compact, &repl_control);
        int rc_probe   = case_arms_refusal(1, compact, &repl_probe);
        printf("[Z] %scompacted: on_replicate during the verify: %d "
               "(control window: %d)\n", compact ? "" : "un-",
               repl_probe, repl_control);

        if (rc_probe != rc_control)
            FAIL("[Z] running a verify CHANGED the restore's answer: "
                 "rc=%d (%s) without it, rc=%d (%s) with it.  A diagnostic that "
                 "arms a refusal tells the operator during the restore",
                 rc_control, tsdb_errstr(rc_control),
                 rc_probe, tsdb_errstr(rc_probe));
        /* Un-compacted, the re-run is the documented no-op and must return OK.
         * Compacted, tsdb_restore_run refuses on the rewritten block
         * boundaries — that refusal is the COMPACTOR's, and the only thing
         * under test here is that the verify did not become a second cause of
         * it (rc_probe == rc_control above). */
        if (!compact && rc_control != TSDB_OK)
            FAIL("[Z] the control restore itself failed rc=%d (%s) — the case "
                 "is not testing what it claims",
                 rc_control, tsdb_errstr(rc_control));
        if (repl_probe != 0)
            FAIL("[Z] the verify fired on_replicate %d time(s) on a target that "
                 "already held everything the set carries — a check shipped "
                 "rows to peers", repl_probe);
    }

    case_behind_target_is_drained();

    printf("\n=== test_restore_verify_live PASSED ===\n");
    return 0;
}
