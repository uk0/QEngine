/* test_ae_dup_converge.c — the anti-entropy row digest must never report a
 * bucket the repair cannot close.
 *
 * THE BUG (REPL-5).  Detection and repair disagree about what a bucket IS.
 *   - merkle.c fingerprints a MULTISET: tsdb_rowdigest_from_result folds EVERY
 *     row into (count, hsum, hxor), and tsdb_rowdigest_diff calls a bucket
 *     divergent when any of the three differs.
 *   - db_cluster.c repairs by SET membership: tsdb_ae_merge_result_dedup skips
 *     any peer row whose content hash this node already holds — once, no matter
 *     how many copies the peer has.
 * So two replicas differing only by a DUPLICATED row can never converge.  The
 * counts stay unequal, the digest keeps naming the bucket, and every 30 s sweep
 * re-pulls the whole bucket from the peer and inserts nothing.  Forever.
 *
 * A duplicated row is ordinary data here, not corruption: rows have no primary
 * key, so a tick store legitimately holds the same (ts, values) twice, and a
 * replica that missed one of the two copies is genuinely a row short.
 *
 * THE FIX.  The merge consumes local copies by MULTIPLICITY: a peer row is
 * skipped only while this node still has an unmatched copy of that exact
 * content, so a bucket gains exactly peer_multiplicity - local_multiplicity
 * rows.  That is the fixed point the multiset digest compares against, in both
 * directions (the replica with fewer copies gains them; the one with more
 * inserts nothing and keeps them).
 *
 * WHAT THIS PINS (two local DBs in one process, one standing in for the peer —
 * the real production helpers, no cluster, no sockets):
 *   [1] premise: the digests DIFFER when the only difference is a duplicate.
 *   [2] BREAK: merging the peer's bucket converges the digests in one round.
 *       Unfixed: 0 rows inserted and the bucket is still divergent — and stays
 *       divergent after a second and third sweep.
 *   [3] the reverse merge inserts nothing and never deletes the duplicate.
 *   [4] a merge that has converged is idempotent, and a genuinely missing
 *       DISTINCT row is still inserted exactly once (the property the
 *       content-dedup merge already had must survive).
 */

#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/cluster/merkle.h"
#include "../src/storage/db.h"          /* tsdb_db_flush_all */

#include <dirent.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int g_fail = 0;

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
    fprintf(stderr, "FATAL %s:%d rc=%d (%s)\n", __FILE__, __LINE__, _r,   \
            tsdb_errstr(_r)); exit(1); } } while (0)

#define BASE 1700000000000000000LL   /* a multiple of 100, so SPAN=100 aligns */
#define SPAN 100LL
#define NROWS 40

static const char *TAG(int i) {
    static const char *t[4] = { "alpha", "beta", "gamma", "delta" };
    return t[i & 3];
}

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

static tsdb_db_t *open_fresh(const char *dir) {
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "v",   TSDB_TYPE_INT64     },
        { "tag", TSDB_TYPE_SYMBOL    },
    };
    OK(tsdb_create_table(db, "t", cols, 3, "ts"));
    return db;
}

/* Append one row (ts = BASE + i, v, TAG(i)).  Called twice with the same
 * arguments it produces two byte-identical rows — a legal duplicate. */
static void add_row(tsdb_db_t *db, int i, int64_t v) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *bat = NULL;
    OK(tsdb_batch_begin(t, &bat));
    OK(tsdb_batch_row_ts(bat, BASE + i));
    OK(tsdb_batch_row_i64(bat, 1, v));
    OK(tsdb_batch_row_sym(bat, 2, TAG(i)));
    OK(tsdb_batch_row_end(bat));
    OK(tsdb_batch_commit(bat));
}

static void fill(tsdb_db_t *db, int n) {
    for (int i = 0; i < n; i++) add_row(db, i, i);
}

static void digest(tsdb_db_t *db, tsdb_rowdigest_bucket_t **v, size_t *n) {
    OK(tsdb_cluster_local_table_digest(db, "t", SPAN, INT64_MIN, INT64_MAX, v, n));
}

/* Divergent bucket count between the two DBs' digests. */
static size_t divergence(tsdb_db_t *a, tsdb_db_t *b) {
    tsdb_rowdigest_bucket_t *va = NULL, *vb = NULL;
    size_t na = 0, nb = 0;
    digest(a, &va, &na);
    digest(b, &vb, &nb);
    int64_t divs[64]; size_t nd = 0;
    OK(tsdb_rowdigest_diff(va, na, vb, nb, divs, 64, &nd));
    free(va); free(vb);
    return nd;
}

static uint64_t row_count(tsdb_db_t *db) {
    uint64_t c = 0; int64_t m = 0;
    OK(tsdb_cluster_local_table_stats(db, "t", &c, &m));
    return c;
}

/* Merge src's copy of [bstart,bend] into dst with the REAL production helpers;
 * src stands in for the peer.  Returns rows inserted. */
static int merge_bucket(tsdb_db_t *dst, tsdb_db_t *src,
                        int64_t bstart, int64_t bend) {
    uint64_t *lset = NULL; size_t ln = 0;
    OK(tsdb_ae_local_bucket_hashes(dst, "t", bstart, bend, &lset, &ln));

    char qtl[256];
    snprintf(qtl, sizeof(qtl),
             "SELECT * FROM t WHERE ts >= %lld AND ts <= %lld",
             (long long)bstart, (long long)bend);
    tsdb_result_t *res = NULL;
    OK(tsdb_query(src, qtl, &res));

    int inserted = 0;
    OK(tsdb_ae_merge_result_dedup(dst, "t", res, lset, ln, &inserted));
    tsdb_result_free(res);
    free(lset);
    return inserted;
}

int main(void) {
    printf("=== test_ae_dup_converge ===\n");
    setenv("TSDB_IDLE_FLUSH", "0", 1);

    tsdb_db_t *peer = open_fresh("/tmp/tsdb_ae_dup_peer");
    tsdb_db_t *node = open_fresh("/tmp/tsdb_ae_dup_node");

    fill(peer, NROWS);
    fill(node, NROWS);
    /* The peer holds row 7 TWICE — same ts, same v, same tag. */
    add_row(peer, 7, 7);
    OK(tsdb_db_flush_all(peer));
    OK(tsdb_db_flush_all(node));

    int64_t bstart = BASE, bend = BASE + SPAN - 1;

    printf("\n[1] the duplicate alone makes the digests diverge\n");
    CHECK(row_count(peer) == NROWS + 1 && row_count(node) == NROWS,
          "peer holds %llu rows, node holds %llu (the peer's extra copy of "
          "row 7 is a real, distinct row)",
          (unsigned long long)row_count(peer), (unsigned long long)row_count(node));
    CHECK(divergence(peer, node) == 1,
          "exactly 1 divergent bucket reported (%zu)", divergence(peer, node));

    printf("\n[2] one merge round must CLOSE what the digest reported\n");
    int ins = merge_bucket(node, peer, bstart, bend);
    size_t nd = divergence(peer, node);
    CHECK(ins == 1 && nd == 0,
          "merging the peer's bucket inserted %d row (want 1) and left %zu "
          "divergent buckets (want 0)", ins, nd);
    CHECK(row_count(node) == NROWS + 1,
          "the node now holds both copies (%llu, want %d)",
          (unsigned long long)row_count(node), NROWS + 1);

    /* Two more sweeps: an unconverged bucket re-pulls forever, so a repair that
     * did not converge shows up here as repeated zero-insert rounds that never
     * clear the divergence. */
    int ins2 = merge_bucket(node, peer, bstart, bend);
    int ins3 = merge_bucket(node, peer, bstart, bend);
    CHECK(ins2 == 0 && ins3 == 0 && divergence(peer, node) == 0,
          "the next two sweeps insert nothing and the bucket stays converged "
          "(ins2=%d ins3=%d)", ins2, ins3);

    printf("\n[3] the reverse merge adds nothing and removes nothing\n");
    uint64_t before = row_count(peer);
    int rins = merge_bucket(peer, node, bstart, bend);
    CHECK(rins == 0 && row_count(peer) == before,
          "peer pulled %d rows (want 0) and still holds %llu (want %llu) — the "
          "duplicate is never deleted", rins,
          (unsigned long long)row_count(peer), (unsigned long long)before);

    printf("\n[4] a genuinely missing DISTINCT row is still inserted once\n");
    add_row(peer, 11, 999999);                 /* new content, same bucket */
    OK(tsdb_db_flush_all(peer));
    CHECK(divergence(peer, node) == 1, "the new row re-diverges the bucket");
    int dins = merge_bucket(node, peer, bstart, bend);
    CHECK(dins == 1 && divergence(peer, node) == 0,
          "the distinct row is inserted exactly once (%d) and the bucket "
          "converges again", dins);
    CHECK(row_count(node) == row_count(peer),
          "both replicas hold %llu rows — converged, never over-counted",
          (unsigned long long)row_count(node));

    tsdb_close(peer);
    tsdb_close(node);
    rm_rf("/tmp/tsdb_ae_dup_peer");
    rm_rf("/tmp/tsdb_ae_dup_node");

    printf(g_fail ? "\n=== FAILED (%d) ===\n" : "\n=== PASSED ===\n", g_fail);
    return g_fail ? 1 : 0;
}
