/* test_ae_rowdigest.c — anti-entropy row-range digest: the middle-hole net.
 *
 * THE BUG.  tsdb_antientropy_decide compares only (count, max_ts): a peer with
 * best_max_ts <= local_max_ts and best_count <= local_count reads UP_TO_DATE.
 * So two replicas that each miss a DIFFERENT interior batch — EQUAL count,
 * EQUAL max_ts, different content — both believe they are current and neither
 * repairs the other.  A silent middle hole, permanent: no count(*) or max(ts)
 * a node can compute distinguishes the two, so nothing ever revisits it.
 *
 * THE FIX.  A row-range digest keyed on ROW CONTENT, not block identity: rows
 * are bucketed by a ts window and each bucket carries an order-independent
 * (count, hsum, hxor) multiset fingerprint of its rows' canonical (ts, values).
 * A LOCAL_TABLE_DIGEST RPC exchanges it; a mismatch at EQUAL (count,max_ts)
 * NAMES the divergent bucket and the node content-dedup merges the peer's copy
 * of that range (never a truncate).  A re-blocked replica (compacted vs not)
 * holding the SAME rows produces the SAME digest, so re-blocking is not a false
 * divergence.
 *
 * WHAT THIS PINS (single process; the digest core + repair in-process, then a
 * real loopback RPC server + client for the wire exchange and the old-peer
 * fallback — no forks, no sleeps, no cluster):
 *
 *   [1] DETECTION.  Two replicas equal on (count, max_ts) but differing in one
 *       interior row: tsdb_antientropy_decide says UP_TO_DATE (the blind spot),
 *       yet the digests DIFFER and name exactly the bucket that holds the row —
 *       at EQUAL per-bucket count (a content-only difference).
 *   [2] INVARIANCE.  The same rows in different block layouts (1 block vs many,
 *       and after a real compaction) produce byte-identical digests, and the
 *       memtable path is counted (unflushed == flushed).
 *   [3] REPAIR.  A content-dedup bucket merge inserts ONLY the rows a replica
 *       lacks (not the shared ones), never deletes, and drives both replicas to
 *       the union — the digests then MATCH and neither over-counts.
 *   [4] RPC.  The digest survives the wire intact, a peer's digest diffs against
 *       ours to name the hole, and a peer too old for the opcode degrades to
 *       TSDB_ERR_UNSUPPORTED (the caller then falls back to count/max_ts).
 *   [5] WIRE + COLLISION.  serialize/deserialize round-trips, and two different
 *       single-row buckets never share a fingerprint.
 */
#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/cluster/rpc.h"
#include "../src/cluster/merkle.h"
#include "../src/storage/db.h"          /* tsdb_db_flush_all */
#include "../src/storage/compaction.h"  /* tsdb_compactor_run_once */

#include <arpa/inet.h>
#include <dirent.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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

/* Insert rows [0,n) as `nflush` equal batches, flushing after each (when
 * nflush > 1) so the partition ends with several blocks per column — a
 * different block layout for the same rows.  diff_i >= 0 overrides that row's
 * v (to forge a same-ts, different-content interior row). */
static void fill_blocks(tsdb_db_t *db, int n, int nflush,
                        int diff_i, int64_t diff_v) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    int per = (n + nflush - 1) / nflush;
    for (int b = 0; b < nflush; b++) {
        int lo = b * per, hi = lo + per;
        if (hi > n) hi = n;
        if (lo >= hi) break;
        tsdb_batch_t *bat = NULL;
        OK(tsdb_batch_begin(t, &bat));
        for (int i = lo; i < hi; i++) {
            int64_t v = (i == diff_i) ? diff_v : i;
            OK(tsdb_batch_row_ts(bat, BASE + i));
            OK(tsdb_batch_row_i64(bat, 1, v));
            OK(tsdb_batch_row_sym(bat, 2, TAG(i)));
            OK(tsdb_batch_row_end(bat));
        }
        OK(tsdb_batch_commit(bat));
        if (nflush > 1) OK(tsdb_db_flush_all(db));
    }
}

static void digest(tsdb_db_t *db, tsdb_rowdigest_bucket_t **v, size_t *n) {
    OK(tsdb_cluster_local_table_digest(db, "t", SPAN, INT64_MIN, INT64_MAX, v, n));
}

/* Insert ONE extra interior row — a distinct value at an already-present
 * interior ts (a duplicate ts is a legal, distinct row in a tick store) — so a
 * replica ends exactly one row AHEAD of its peer WITHOUT moving max_ts: the
 * count-unequal-at-equal-max_ts shape a replication drop leaves. */
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

static int dv_equal(const tsdb_rowdigest_bucket_t *a, size_t na,
                    const tsdb_rowdigest_bucket_t *b, size_t nb) {
    if (na != nb) return 0;
    for (size_t i = 0; i < na; i++)
        if (a[i].bstart != b[i].bstart || a[i].count != b[i].count ||
            a[i].hsum != b[i].hsum || a[i].hxor != b[i].hxor)
            return 0;
    return 1;
}

static const tsdb_rowdigest_bucket_t *
find_b(const tsdb_rowdigest_bucket_t *v, size_t n, int64_t bs) {
    for (size_t i = 0; i < n; i++) if (v[i].bstart == bs) return &v[i];
    return NULL;
}

static uint64_t row_count(tsdb_db_t *db) {
    uint64_t c = 0; int64_t m = 0;
    OK(tsdb_cluster_local_table_stats(db, "t", &c, &m));
    return c;
}

/* ---- in-process repair helpers ------------------------------------------ */

/* Merge db_src's copy of [bstart,bend] into db_dst with the REAL production
 * dedup (tsdb_ae_local_bucket_hashes + tsdb_ae_merge_result_dedup); db_src
 * stands in for the peer.  Returns rows inserted. */
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

/* ---- old-peer stub (models a binary that predates opcode 25) ------------- */

typedef struct { int listen_fd; } stub_t;

static void *stub_unsupported(void *ud) {
    stub_t *s = (stub_t *)ud;
    int cfd = accept(s->listen_fd, NULL, NULL);
    if (cfd < 0) return NULL;
    for (;;) {
        uint8_t hdr[TSDB_RPC_HDR_SIZE];
        size_t got = 0;
        while (got < sizeof(hdr)) {
            ssize_t k = read(cfd, hdr + got, sizeof(hdr) - got);
            if (k <= 0) { close(cfd); return NULL; }
            got += (size_t)k;
        }
        uint32_t req_id, plen;
        memcpy(&req_id, hdr + 6, 4);
        memcpy(&plen,   hdr + 10, 4);
        uint8_t *pl = plen ? (uint8_t *)malloc(plen) : NULL;
        for (uint32_t off = 0; off < plen; ) {
            ssize_t k = read(cfd, pl + off, plen - off);
            if (k <= 0) { free(pl); close(cfd); return NULL; }
            off += (uint32_t)k;
        }
        free(pl);
        /* Exactly what this build's dispatch `default:` sends for an opcode it
         * does not implement — the unambiguous "too old" signal. */
        uint8_t frame[TSDB_RPC_HDR_SIZE];
        int fn = tsdb_rpc_encode(frame, sizeof(frame),
                                 TSDB_RPC_ERR_UNSUPPORTED, req_id, NULL, 0);
        if (fn <= 0) { close(cfd); return NULL; }
        for (int off = 0; off < fn; ) {
            ssize_t k = write(cfd, frame + off, (size_t)(fn - off));
            if (k <= 0) { close(cfd); return NULL; }
            off += (int)k;
        }
    }
}

static int stub_start(stub_t *s, pthread_t *th) {
    memset(s, 0, sizeof(*s));
    s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (s->listen_fd < 0) return -1;
    int one = 1;
    setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(s->listen_fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) return -1;
    if (listen(s->listen_fd, 4) != 0) return -1;
    socklen_t sl = sizeof(sa);
    if (getsockname(s->listen_fd, (struct sockaddr *)&sa, &sl) != 0) return -1;
    if (pthread_create(th, NULL, stub_unsupported, s) != 0) return -1;
    return ntohs(sa.sin_port);
}

static tsdb_rpc_conn_t *dial(int port) {
    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    return tsdb_rpc_connect(addr, 2000);
}

/* Fetch + deserialize a peer's digest over the wire. */
static int fetch_digest(tsdb_rpc_conn_t *conn,
                        tsdb_rowdigest_bucket_t **out, size_t *out_n) {
    uint8_t *rbuf = malloc(TSDB_ROWDIGEST_WIRE_MAX);
    if (!rbuf) return TSDB_ERR_NOMEM;
    uint32_t rlen = 0;
    int rc = tsdb_rpc_local_table_digest(conn, "t", SPAN, INT64_MIN, INT64_MAX,
                                         rbuf, TSDB_ROWDIGEST_WIRE_MAX, &rlen);
    if (rc == TSDB_OK) rc = tsdb_rowdigest_deserialize(rbuf, rlen, out, out_n);
    free(rbuf);
    return rc;
}

int main(void) {
    printf("=== test_ae_rowdigest ===\n");

    /* ── [1] DETECTION: the middle-hole blind spot ───────────────────────── */
    printf("\n[1] equal (count,max_ts), one interior row differs -> digests name it\n");
    tsdb_db_t *dbA = open_fresh("/tmp/tsdb_rd_A");
    tsdb_db_t *dbB = open_fresh("/tmp/tsdb_rd_B");
    fill_blocks(dbA, 1000, 1, -1, 0);          /* correct */
    fill_blocks(dbB, 1000, 1, 500, 99999);     /* row 500 has v=99999 */

    {
        uint64_t ca = 0, cb = 0; int64_t ma = 0, mb = 0;
        OK(tsdb_cluster_local_table_stats(dbA, "t", &ca, &ma));
        OK(tsdb_cluster_local_table_stats(dbB, "t", &cb, &mb));
        CHECK(ca == cb && ma == mb,
              "the two replicas TIE on (count,max_ts): (%llu,%lld)==(%llu,%lld)",
              (unsigned long long)ca, (long long)ma,
              (unsigned long long)cb, (long long)mb);
        CHECK(tsdb_antientropy_decide(ca, ma, cb, mb) == TSDB_AE_UP_TO_DATE,
              "the (count,max_ts)-only decide() is BLIND: reads UP_TO_DATE");

        tsdb_rowdigest_bucket_t *dva = NULL, *dvb = NULL; size_t na = 0, nb = 0;
        digest(dbA, &dva, &na);
        digest(dbB, &dvb, &nb);
        CHECK(na == 10 && nb == 10, "10 buckets each (1000 rows / SPAN 100)");

        int64_t divs[32]; size_t ndiv = 0;
        OK(tsdb_rowdigest_diff(dva, na, dvb, nb, divs, 32, &ndiv));
        CHECK(ndiv == 1, "exactly ONE bucket diverges (got %zu)", ndiv);
        CHECK(ndiv == 1 && divs[0] == BASE + 500,
              "and it is the bucket that holds ts=BASE+500 (got %lld)",
              ndiv ? (long long)divs[0] : -1);

        const tsdb_rowdigest_bucket_t *ba = find_b(dva, na, BASE + 500);
        const tsdb_rowdigest_bucket_t *bb = find_b(dvb, nb, BASE + 500);
        CHECK(ba && bb && ba->count == bb->count && ba->count == 100,
              "the divergent bucket has EQUAL per-bucket count (100) — a "
              "content-only hole the count probe cannot see");
        CHECK(ba && bb && (ba->hsum != bb->hsum || ba->hxor != bb->hxor),
              "yet its content fingerprint differs");
        free(dva); free(dvb);
    }

    /* ── [2] INVARIANCE under re-blocking + the memtable path ────────────── */
    printf("\n[2] same rows, different block layout -> identical digest\n");
    {
        tsdb_db_t *one  = open_fresh("/tmp/tsdb_rd_1blk");
        tsdb_db_t *many = open_fresh("/tmp/tsdb_rd_5blk");
        fill_blocks(one,  1000, 1, -1, 0);     /* one batch, one block          */
        fill_blocks(many, 1000, 5, -1, 0);     /* five flushed batches, 5 blocks */

        tsdb_rowdigest_bucket_t *d1 = NULL, *d5 = NULL; size_t n1 = 0, n5 = 0;
        digest(one, &d1, &n1);
        digest(many, &d5, &n5);
        CHECK(dv_equal(d1, n1, d5, n5),
              "1-block and 5-block layouts of the same rows digest identically");

        /* Real compaction must not move the digest either. */
        tsdb_compactor_opts_t opts;
        memset(&opts, 0, sizeof(opts));
        opts.min_blocks_to_compact = 2;
        opts.interval_ns = 5000000000LL;
        opts.worker_threads = 1;
        tsdb_compactor_t *cpt = NULL;
        OK(tsdb_compactor_start(many, &opts, &cpt));
        OK(tsdb_compactor_run_once(cpt));
        tsdb_compactor_stop(cpt);

        tsdb_rowdigest_bucket_t *d5c = NULL; size_t n5c = 0;
        digest(many, &d5c, &n5c);
        CHECK(dv_equal(d5, n5, d5c, n5c),
              "a compaction re-blocks the partition but leaves the digest byte-"
              "identical");
        free(d1); free(d5); free(d5c);
        tsdb_close(one); tsdb_close(many);
        rm_rf("/tmp/tsdb_rd_1blk"); rm_rf("/tmp/tsdb_rd_5blk");

        /* Memtable path: unflushed rows are counted, so an unflushed replica
         * and a flushed one holding the same rows still match (apples-to-apples
         * across the deferred-flush boundary). */
        tsdb_db_t *mem = open_fresh("/tmp/tsdb_rd_mem");
        tsdb_db_t *dsk = open_fresh("/tmp/tsdb_rd_dsk");
        fill_blocks(mem, 300, 1, -1, 0);       /* left in the memtable (deferred) */
        fill_blocks(dsk, 300, 1, -1, 0);
        OK(tsdb_db_flush_all(dsk));             /* forced to a partition          */
        tsdb_rowdigest_bucket_t *dm = NULL, *dd = NULL; size_t nm = 0, nd = 0;
        digest(mem, &dm, &nm);
        digest(dsk, &dd, &nd);
        CHECK(dv_equal(dm, nm, dd, nd),
              "unflushed (memtable) digest == flushed digest for the same rows");
        free(dm); free(dd);
        tsdb_close(mem); tsdb_close(dsk);
        rm_rf("/tmp/tsdb_rd_mem"); rm_rf("/tmp/tsdb_rd_dsk");
    }

    /* ── [3] REPAIR: content-dedup merge closes the gap to the UNION ─────── */
    printf("\n[3] a dedup bucket merge inserts only the missing row, then converges\n");
    {
        int64_t bstart = BASE + 500, bend = BASE + 599;

        /* dbA correct, dbB has row 500 = 99999 (from section [1]).  Merge dbB's
         * bucket into dbA: the 99 shared rows dedup out, only the one differing
         * row is inserted. */
        int insA = merge_bucket(dbA, dbB, bstart, bend);
        CHECK(insA == 1,
              "merging the peer's 100-row bucket inserts ONLY the 1 row we "
              "lacked (got %d) — no duplication of the 99 shared rows", insA);
        CHECK(row_count(dbA) == 1001, "dbA grew by exactly 1 (to %llu)",
              (unsigned long long)row_count(dbA));

        /* Mirror half: dbB pulls dbA's bucket.  Both end at the union. */
        int insB = merge_bucket(dbB, dbA, bstart, bend);
        CHECK(insB == 1, "the mirror merge also inserts exactly 1 (got %d)", insB);
        CHECK(row_count(dbB) == 1001, "dbB grew to %llu",
              (unsigned long long)row_count(dbB));

        tsdb_rowdigest_bucket_t *dva = NULL, *dvb = NULL; size_t na = 0, nb = 0;
        digest(dbA, &dva, &na);
        digest(dbB, &dvb, &nb);
        int64_t divs[32]; size_t ndiv = 0;
        OK(tsdb_rowdigest_diff(dva, na, dvb, nb, divs, 32, &ndiv));
        CHECK(ndiv == 0,
              "after the bidirectional merge the digests MATCH (gap closed, "
              "converged to the union) — divergent buckets now %zu", ndiv);
        CHECK(row_count(dbA) == row_count(dbB) && row_count(dbA) == 1001,
              "and both hold exactly the union (1001), never over-counted");

        /* A second merge is a no-op — the dedup makes repair idempotent. */
        CHECK(merge_bucket(dbA, dbB, bstart, bend) == 0,
              "re-merging an already-converged bucket inserts nothing");
        free(dva); free(dvb);
    }
    tsdb_close(dbA); tsdb_close(dbB);
    rm_rf("/tmp/tsdb_rd_A"); rm_rf("/tmp/tsdb_rd_B");

    /* ── [4] RPC exchange + old-peer fallback ────────────────────────────── */
    printf("\n[4] LOCAL_TABLE_DIGEST over the wire, and the mixed-version degrade\n");
    {
        tsdb_db_t *srvA = open_fresh("/tmp/tsdb_rd_srvA");
        tsdb_db_t *srvB = open_fresh("/tmp/tsdb_rd_srvB");
        fill_blocks(srvA, 1000, 1, -1, 0);
        fill_blocks(srvB, 1000, 1, 500, 99999);

        tsdb_rpc_server_t *sA = tsdb_rpc_server_new("127.0.0.1:0", srvA, NULL);
        tsdb_rpc_server_t *sB = tsdb_rpc_server_new("127.0.0.1:0", srvB, NULL);
        if (!sA || !sB) { fprintf(stderr, "FATAL: rpc server\n"); return 1; }
        tsdb_rpc_conn_t *cA = dial(tsdb_rpc_server_port(sA));
        tsdb_rpc_conn_t *cB = dial(tsdb_rpc_server_port(sB));
        if (!cA || !cB) { fprintf(stderr, "FATAL: dial\n"); return 1; }

        /* The wire answer for srvA equals srvA's own locally-computed digest. */
        tsdb_rowdigest_bucket_t *local = NULL, *wire = NULL; size_t nl = 0, nw = 0;
        digest(srvA, &local, &nl);
        CHECK(fetch_digest(cA, &wire, &nw) == TSDB_OK,
              "the digest RPC is answered (opcode 25 wired)");
        CHECK(dv_equal(local, nl, wire, nw),
              "the digest survives the wire byte-for-byte");

        /* srvB's digest, diffed against ours, names the hole across the wire. */
        tsdb_rowdigest_bucket_t *wireB = NULL; size_t nwB = 0;
        CHECK(fetch_digest(cB, &wireB, &nwB) == TSDB_OK, "peer B answers too");
        int64_t divs[32]; size_t ndiv = 0;
        OK(tsdb_rowdigest_diff(local, nl, wireB, nwB, divs, 32, &ndiv));
        CHECK(ndiv == 1 && divs[0] == BASE + 500,
              "the over-the-wire diff names bucket BASE+500 (ndiv=%zu)", ndiv);
        free(local); free(wire); free(wireB);

        tsdb_rpc_conn_close(cA); tsdb_rpc_conn_close(cB);
        tsdb_rpc_server_stop(sA); tsdb_rpc_server_stop(sB);
        tsdb_close(srvA); tsdb_close(srvB);
        rm_rf("/tmp/tsdb_rd_srvA"); rm_rf("/tmp/tsdb_rd_srvB");

        /* Old peer: a binary without opcode 25 answers ERR_UNSUPPORTED, which
         * the client reports as TSDB_ERR_UNSUPPORTED so anti-entropy degrades
         * to the count/max_ts decision instead of erroring. */
        stub_t st; pthread_t th;
        int sp = stub_start(&st, &th);
        if (sp < 0) { fprintf(stderr, "FATAL: stub\n"); return 1; }
        tsdb_rpc_conn_t *sc = dial(sp);
        if (!sc) { fprintf(stderr, "FATAL: dial stub\n"); return 1; }
        uint8_t rbuf[64]; uint32_t rlen = 0;
        int rc = tsdb_rpc_local_table_digest(sc, "t", SPAN, INT64_MIN, INT64_MAX,
                                             rbuf, sizeof(rbuf), &rlen);
        CHECK(rc == TSDB_ERR_UNSUPPORTED,
              "a pre-opcode peer degrades to TSDB_ERR_UNSUPPORTED (rc=%d)", rc);
        tsdb_rpc_conn_close(sc);
        pthread_join(th, NULL);
        close(st.listen_fd);
    }

    /* ── [5] WIRE round-trip + per-row collision resistance ──────────────── */
    printf("\n[5] serialize/deserialize round-trip and fingerprint separation\n");
    {
        tsdb_rowdigest_bucket_t in[3] = {
            { BASE,        100, 0xdeadbeefULL,       0x1234ULL },
            { BASE + SPAN,  50, 0xfeedface0000ULL,   0xabcdULL },
            { BASE + 900,    1, 0x0ULL,              0xffffffffffffffffULL },
        };
        uint8_t buf[512];
        int w = tsdb_rowdigest_serialize(in, 3, buf, sizeof(buf));
        CHECK(w == 4 + 3 * 32, "serialize wrote the expected %d bytes", w);
        tsdb_rowdigest_bucket_t *out = NULL; size_t on = 0;
        OK(tsdb_rowdigest_deserialize(buf, (uint32_t)w, &out, &on));
        CHECK(on == 3 && dv_equal(in, 3, out, on),
              "deserialize reproduces the vector exactly");
        free(out);

        /* A body that claims more buckets than it carries is rejected. */
        tsdb_rowdigest_bucket_t *bad = NULL; size_t bn = 0;
        uint32_t lie = 99; memcpy(buf, &lie, 4);
        CHECK(tsdb_rowdigest_deserialize(buf, (uint32_t)w, &bad, &bn) == TSDB_ERR_CORRUPT,
              "a short/over-claimed body is rejected, not half-parsed");

        /* Two single-row buckets differing only in one value must not collide. */
        tsdb_db_t *x = open_fresh("/tmp/tsdb_rd_x");
        tsdb_db_t *y = open_fresh("/tmp/tsdb_rd_y");
        fill_blocks(x, 1, 1, -1, 0);           /* one row, v=0 */
        fill_blocks(y, 1, 1, 0, 777);          /* one row, v=777 */
        tsdb_rowdigest_bucket_t *dx = NULL, *dy = NULL; size_t nx = 0, ny = 0;
        digest(x, &dx, &nx);
        digest(y, &dy, &ny);
        CHECK(nx == 1 && ny == 1 && dx[0].count == dy[0].count &&
              (dx[0].hsum != dy[0].hsum || dx[0].hxor != dy[0].hxor),
              "same count, one differing value -> different fingerprint");
        free(dx); free(dy);
        tsdb_close(x); tsdb_close(y);
        rm_rf("/tmp/tsdb_rd_x"); rm_rf("/tmp/tsdb_rd_y");
    }

    /* ── [6] REPAIR the count-UNEQUAL divergence the count/max_ts probe strands ─
     *
     * Cases [1..3] are the equal-count middle hole.  The other shape a
     * replication drop leaves is UNEQUAL count at EQUAL max_ts: one replica
     * holds an interior row the other lacks, so the newest ts still ties but the
     * counts differ.  decide() heals NEITHER side — the deficit node REFUSES
     * (SKIP_UNSAFE: a higher peer count at equal max_ts is never truncate-and-
     * repulled) and the surplus node reads UP_TO_DATE — which is why stress_t /
     * phole sat permanently split on the live cluster.  The row-range digest is
     * the only path that heals it: the merge is count-agnostic and insert-only,
     * so it converges to the union in BOTH directions and never truncates a
     * legitimately-ahead replica.  This is the divergence the widened eqpeers
     * capture (db_cluster.c: `pmax == local_max_ts`, no longer count-gated) now
     * routes into the digest verify. */
    printf("\n[6] count-UNEQUAL at equal max_ts -> decide() strands it, digest heals\n");
    {
        int64_t bstart = BASE + 500, bend = BASE + 599;
        tsdb_db_t *lo = open_fresh("/tmp/tsdb_rd_lo");   /* deficit replica */
        tsdb_db_t *hi = open_fresh("/tmp/tsdb_rd_hi");   /* surplus replica */
        fill_blocks(lo, 1000, 1, -1, 0);
        fill_blocks(hi, 1000, 1, -1, 0);
        add_row(hi, 500, 88888);            /* hi ends ONE interior row ahead   */

        uint64_t clo = 0, chi = 0; int64_t mlo = 0, mhi = 0;
        OK(tsdb_cluster_local_table_stats(lo, "t", &clo, &mlo));
        OK(tsdb_cluster_local_table_stats(hi, "t", &chi, &mhi));
        CHECK(clo == 1000 && chi == 1001 && mlo == mhi,
              "counts DIFFER (%llu vs %llu) at EQUAL max_ts (%lld) — a drop, not a tail",
              (unsigned long long)clo, (unsigned long long)chi, (long long)mlo);

        /* The count/max_ts decision strands BOTH sides — the whole motivation. */
        CHECK(tsdb_antientropy_decide(clo, mlo, chi, mhi) == TSDB_AE_SKIP_UNSAFE,
              "deficit node REFUSES (SKIP_UNSAFE): higher peer count at equal max_ts");
        CHECK(tsdb_antientropy_decide(chi, mhi, clo, mlo) == TSDB_AE_UP_TO_DATE,
              "surplus node reads UP_TO_DATE — neither side heals via count/max_ts");

        /* The digest names exactly the one divergent bucket. */
        tsdb_rowdigest_bucket_t *dlo = NULL, *dhi = NULL; size_t nlo = 0, nhi = 0;
        digest(lo, &dlo, &nlo);
        digest(hi, &dhi, &nhi);
        int64_t divs[32]; size_t ndiv = 0;
        OK(tsdb_rowdigest_diff(dlo, nlo, dhi, nhi, divs, 32, &ndiv));
        CHECK(ndiv == 1 && divs[0] == bstart,
              "the digest names exactly bucket BASE+500 (ndiv=%zu)", ndiv);
        free(dlo); free(dhi);

        /* Surplus direction FIRST — the safety half: hi (1001) pulls the deficit
         * peer's strictly-smaller bucket (100 rows).  Every row dedups out, so
         * NOTHING inserts and — critically — hi is NOT truncated to match the
         * smaller peer.  A merge that truncated a legitimately-ahead replica (the
         * old data-loss bug's shape) would drop hi to 1000 here. */
        int ins_hi = merge_bucket(hi, lo, bstart, bend);
        CHECK(ins_hi == 0 && row_count(hi) == 1001,
              "surplus node inserts nothing and NEVER truncates (ins=%d, still %llu)",
              ins_hi, (unsigned long long)row_count(hi));

        /* Deficit direction: lo (100 in-bucket) pulls hi's 101, dedups the 100
         * shared, inserts ONLY the 1 it lacked -> lo reaches the union. */
        int ins_lo = merge_bucket(lo, hi, bstart, bend);
        CHECK(ins_lo == 1 && row_count(lo) == 1001,
              "deficit node pulls exactly the 1 missing interior row (ins=%d, now %llu)",
              ins_lo, (unsigned long long)row_count(lo));

        /* Converged: digests MATCH, both hold the union, neither over-counts. */
        tsdb_rowdigest_bucket_t *dlo2 = NULL, *dhi2 = NULL; size_t nlo2 = 0, nhi2 = 0;
        digest(lo, &dlo2, &nlo2);
        digest(hi, &dhi2, &nhi2);
        ndiv = 0;
        OK(tsdb_rowdigest_diff(dlo2, nlo2, dhi2, nhi2, divs, 32, &ndiv));
        CHECK(ndiv == 0 && row_count(lo) == row_count(hi) && row_count(lo) == 1001,
              "after the bidirectional merge both converge to the union (1001), divs=%zu",
              ndiv);
        free(dlo2); free(dhi2);
        tsdb_close(lo); tsdb_close(hi);
        rm_rf("/tmp/tsdb_rd_lo"); rm_rf("/tmp/tsdb_rd_hi");
    }

    if (g_fail) {
        printf("\n=== test_ae_rowdigest FAILED (%d) ===\n", g_fail);
        return 1;
    }
    printf("\n=== test_ae_rowdigest PASSED ===\n");
    return 0;
}
