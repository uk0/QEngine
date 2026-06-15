/* test_wire_count_disk.c — L4 regression for task #174.
 *
 * Reproduces the LAST layer: `SELECT count(*)` returns an EMPTY result set
 * (0 result rows) for data that is on disk across many blocks, even though the
 * on-disk block metadata (count/ts_min/ts_max) is correct.
 *
 * Two write paths feed the failing scan:
 *   A. WRITE_BATCH wire path -> memtable -> flush -> multi-block partition.
 *   B. Raw-block REPLICATION path (cluster non-owner node): a peer's flushed
 *      blocks are captured and re-applied one-by-one via tsdb_rawblock_apply.
 *      With many blocks the replica's per-column idx is rebuilt N times, one
 *      entry per apply, and the blocks can arrive per-column INTERLEAVED in an
 *      order different from the flush writer's "ts column last" ordering.
 *
 * The pre-existing test_server.c writes 100 rows (one block, served from the
 * memtable) and test_rawblock.c applies a single block and byte-compares files
 * — neither reads back a MULTI-block partition via count(*).  This test does:
 * after each write path it asserts count(*) returns exactly ONE row whose value
 * equals the rows written (never an empty set, never a wrong value), and that a
 * full row scan returns every row.
 */

#include "../src/server/server.h"
#include "../src/server/proto.h"
#include "../src/cluster/rawblock.h"
#include "../src/storage/db.h"
#include "../src/catalog/stable.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); g_fail++; } \
    else { printf("PASS: %s\n", msg); g_pass++; } \
} while(0)

#define BLOCK_PTS  1024                 /* small so flushes happen early */
#define NBLOCKS    7                     /* enough blocks to expose ordering */
#define NROWS      (NBLOCKS * BLOCK_PTS) /* 7168 -> exactly 7 disk blocks/col */
#define TS_BASE    1743465600000000000LL /* 2025-04-01T00:00:00Z, ns */

static const char *TCOLS[3] = { "ts", "price", "qty" };

/* ---- wire helpers (mirrors test_server.c) -------------------------------- */

static int client_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET; sa.sin_port = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    for (int i = 0; i < 50; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) return fd;
        if (errno == ECONNREFUSED) { usleep(20000); continue; }
        break;
    }
    close(fd); return -1;
}

static int recv_frame(int fd, tsdb_frame_hdr_t *hdr, uint8_t **out) {
    return tsdb_proto_recv(fd, hdr, out);
}

/* WRITE_BATCH payload for (ts TIMESTAMP, price FLOAT64, qty INT64). */
static size_t build_write_batch(uint8_t *buf, const char *tname,
                                int nrows, int64_t ts_base) {
    uint8_t *p = buf;
    uint8_t tnl = (uint8_t)strlen(tname);
    *p++ = tnl; memcpy(p, tname, tnl); p += tnl;
    uint16_t nc = 3; memcpy(p, &nc, 2); p += 2;
    uint32_t nr = (uint32_t)nrows; memcpy(p, &nr, 4); p += 4;
    *p++ = 2; memcpy(p, "ts", 2); p += 2; *p++ = TSDB_TYPE_TIMESTAMP; *p++ = 0;
    { uint32_t csz = (uint32_t)(nrows * 8); memcpy(p, &csz, 4); p += 4; }
    for (int i = 0; i < nrows; i++) { int64_t ts = ts_base + (int64_t)i*1000000LL; memcpy(p, &ts, 8); p += 8; }
    *p++ = 5; memcpy(p, "price", 5); p += 5; *p++ = TSDB_TYPE_FLOAT64; *p++ = 0;
    { uint32_t csz = (uint32_t)(nrows * 8); memcpy(p, &csz, 4); p += 4; }
    for (int i = 0; i < nrows; i++) { double v = 100.0 + (double)(i%1000)*0.01; memcpy(p, &v, 8); p += 8; }
    *p++ = 3; memcpy(p, "qty", 3); p += 3; *p++ = TSDB_TYPE_INT64; *p++ = 0;
    { uint32_t csz = (uint32_t)(nrows * 8); memcpy(p, &csz, 4); p += 4; }
    for (int i = 0; i < nrows; i++) { int64_t v = (int64_t)(i + 1); memcpy(p, &v, 8); p += 8; }
    return (size_t)(p - buf);
}

/* `SELECT count(*) FROM t` over the wire.  Returns count value; *out_nrows =
 * number of RESULT ROWS (1 healthy, 0 = empty-set bug); *out_err = ERROR seen. */
static int64_t wire_count_star(int port, const char *table,
                               int *out_nrows, int *out_err) {
    *out_nrows = -1; *out_err = 0;
    int fd = client_connect(port);
    if (fd < 0) return -1;
    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;
    char qtl[128]; snprintf(qtl, sizeof(qtl), "SELECT count(*) FROM %s", table);
    tsdb_proto_send(fd, TSDB_MT_QUERY, 0, 4, (const uint8_t *)qtl, strlen(qtl));
    if (recv_frame(fd, &hdr, &pl) != TSDB_OK) { close(fd); return -1; }
    if (hdr.type == TSDB_MT_ERROR) { *out_err = 1; free(pl); close(fd); return -1; }
    free(pl); pl = NULL;
    int64_t count_val = -1; int nrows_total = 0; int got_fin = 0;
    while (!got_fin) {
        if (recv_frame(fd, &hdr, &pl) != TSDB_OK) break;
        if (hdr.type == TSDB_MT_ERROR) { *out_err = 1; free(pl); pl = NULL; break; }
        if (pl && hdr.payload_len >= 6) {
            uint32_t nr; memcpy(&nr, pl, 4); nrows_total += (int)nr;
            if (nr >= 1 && hdr.payload_len >= 6 + 8) memcpy(&count_val, pl + 6, 8);
        }
        got_fin = (hdr.flags & TSDB_FLAG_FIN) != 0;
        free(pl); pl = NULL;
    }
    *out_nrows = nrows_total;
    close(fd); return count_val;
}

/* In-process count(*): returns count; *out_nrows = result-row count. */
static int64_t inproc_count_star(tsdb_db_t *db, const char *table,
                                 int *out_nrows, int *out_rc) {
    char qtl[128]; snprintf(qtl, sizeof(qtl), "SELECT count(*) FROM %s", table);
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, qtl, &r);
    *out_rc = rc; *out_nrows = -1;
    if (rc != TSDB_OK) return -1;
    int nrows = 0; int64_t cv = -1;
    while (tsdb_result_next(r) == 1) { if (nrows == 0) cv = tsdb_result_i64(r, 0); nrows++; }
    *out_nrows = nrows; tsdb_result_free(r);
    return cv;
}

/* In-process full row scan: returns number of rows seen. */
static int64_t inproc_row_scan(tsdb_db_t *db, const char *table) {
    char qtl[128]; snprintf(qtl, sizeof(qtl), "SELECT ts FROM %s", table);
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, qtl, &r) != TSDB_OK) return -1;
    int64_t n = 0; while (tsdb_result_next(r) == 1) n++;
    tsdb_result_free(r); return n;
}

/* ===== Part A: WRITE_BATCH -> flush -> disk -> count(*) (wire + in-proc) ==== */
static void test_write_batch_path(void) {
    printf("\n--- Part A: WRITE_BATCH flush path ---\n");
    char dir[256]; snprintf(dir, sizeof(dir), "/tmp/tsdb_wcd_wb_%d", (int)getpid());

    tsdb_db_t *db = NULL;
    CHECK(tsdb_open(dir, &db) == TSDB_OK, "A: tsdb_open");
    tsdb_col_t cols[] = { {"ts",TSDB_TYPE_TIMESTAMP},{"price",TSDB_TYPE_FLOAT64},{"qty",TSDB_TYPE_INT64} };
    CHECK(tsdb_create_table_ex2(db,"trades",cols,3,"ts",TSDB_CREATE_PART_DAY,BLOCK_PTS)==TSDB_OK,
          "A: create table (block_points=1024)");

    tsdb_server_opts_t opts = { .bind_addr="127.0.0.1:0", .max_conns=64, .write_workers=0, .db=db };
    tsdb_server_t *srv = NULL;
    CHECK(tsdb_server_start(&opts,&srv)==TSDB_OK, "A: server_start");
    int port = tsdb_server_port(srv); usleep(20000);

    {
        int fd = client_connect(port);
        tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
        tsdb_frame_hdr_t hdr; uint8_t *pl=NULL; recv_frame(fd,&hdr,&pl); free(pl); pl=NULL;
        uint8_t *wbuf = malloc(8u*1024*1024);
        size_t wsz = build_write_batch(wbuf, "trades", NROWS, TS_BASE);
        CHECK(tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, 3, wbuf, wsz)==TSDB_OK, "A: send WRITE_BATCH");
        free(wbuf);
        CHECK(recv_frame(fd,&hdr,&pl)==TSDB_OK && hdr.type==TSDB_MT_WRITE_ACK, "A: WRITE_ACK");
        free(pl); close(fd);
    }
    { char c[512]; snprintf(c,sizeof(c),"ls -d %s/trades/20*/ >/dev/null 2>&1",dir);
      CHECK(system(c)==0, "A: on-disk partition exists (flushed)"); }

    int nr=-1, rcq=-1; int64_t cv = inproc_count_star(db,"trades",&nr,&rcq);
    printf("  A in-proc: rc=%d rows=%d count=%lld\n", rcq, nr, (long long)cv);
    CHECK(rcq==TSDB_OK && nr==1 && cv==NROWS, "A: in-proc count(*) == NROWS");

    int wn=-1, we=0; int64_t wc = wire_count_star(port,"trades",&wn,&we);
    printf("  A wire: rows=%d err=%d count=%lld\n", wn, we, (long long)wc);
    CHECK(we==0 && wn==1 && wc==NROWS, "A: wire count(*) == NROWS (not empty set)");

    tsdb_server_stop(srv); tsdb_close(db);
    { char c[300]; snprintf(c,sizeof(c),"rm -rf %s",dir); (void)system(c); }
}

/* ===== Part B: raw-block replication -> multi-block replica -> count(*) ===== */

typedef struct {
    char table[64]; uint32_t part_day; uint16_t col_idx;
    tsdb_block_meta_t meta; uint8_t *bytes; size_t bytes_len;
} cap_blk_t;
#define CAP_MAX 256
typedef struct { cap_blk_t b[CAP_MAX]; int n; } cap_ctx_t;

static int cap_hook(void *ud, tsdb_db_t *db, const char *table, uint32_t day,
                    uint16_t col, const tsdb_block_meta_t *meta,
                    const uint8_t *bytes, size_t blen) {
    (void)db; cap_ctx_t *c = (cap_ctx_t *)ud;
    if (c->n >= CAP_MAX) return TSDB_OK;
    cap_blk_t *e = &c->b[c->n++];
    snprintf(e->table, sizeof(e->table), "%s", table);
    e->part_day = day; e->col_idx = col; e->meta = *meta;
    e->bytes = malloc(blen ? blen : 1);
    memcpy(e->bytes, bytes, blen); e->bytes_len = blen;
    return TSDB_OK;
}

/* Apply captured block i to dst via the replication entry point. */
static int apply_cap(tsdb_db_t *dst, const cap_blk_t *e) {
    tsdb_rawblock_push_t r; memset(&r, 0, sizeof(r));
    snprintf(r.table, sizeof(r.table), "%s", e->table);
    r.part_day=e->part_day; r.col_idx=e->col_idx;
    r.codec=e->meta.codec; r.flags=e->meta.flags; r.count=e->meta.count;
    r.ts_min=e->meta.ts_min; r.ts_max=e->meta.ts_max;
    r.stats_min=e->meta.stats_min; r.stats_max=e->meta.stats_max;
    r.stats_sum=e->meta.stats_sum; r.stats_first=e->meta.stats_first;
    r.stats_last=e->meta.stats_last; r.stats_flags=e->meta.stats_flags;
    r.block_bytes_len=(uint32_t)e->bytes_len; r.block_bytes=e->bytes;
    return tsdb_rawblock_apply(dst, &r);
}

/* order: 0 = capture order (per-column grouped, ts last), 1 = interleaved
 * (ts/price/qty per block, mimics network arrival from a streaming sender). */
static void test_rawblock_path(int order, const char *label) {
    printf("\n--- Part B: raw-block replication (%s) ---\n", label);
    char src_dir[256], dst_dir[256];
    snprintf(src_dir,sizeof(src_dir),"/tmp/tsdb_wcd_rb_src_%d_%d",(int)getpid(),order);
    snprintf(dst_dir,sizeof(dst_dir),"/tmp/tsdb_wcd_rb_dst_%d_%d",(int)getpid(),order);

    tsdb_db_t *src=NULL,*dst=NULL;
    CHECK(tsdb_open(src_dir,&src)==TSDB_OK, "B: open src");
    CHECK(tsdb_open(dst_dir,&dst)==TSDB_OK, "B: open dst");
    tsdb_col_t cols[] = { {"ts",TSDB_TYPE_TIMESTAMP},{"price",TSDB_TYPE_FLOAT64},{"qty",TSDB_TYPE_INT64} };
    CHECK(tsdb_create_table_ex2(src,"trades",cols,3,"ts",TSDB_CREATE_PART_DAY,BLOCK_PTS)==TSDB_OK,"B: src create");
    /* replica registers the table locally (catalog-sync analogue) so apply
     * can resolve the schema. */
    CHECK(tsdb_create_table_local(dst,"trades",cols,3,"ts")==TSDB_OK,"B: dst create_local");

    cap_ctx_t ctx; memset(&ctx,0,sizeof(ctx));
    tsdb_db_set_raw_block_hook(src, cap_hook, &ctx);

    /* Write NROWS to src -> flush emits NBLOCKS blocks per column. */
    tsdb_table_t *tbl=NULL; CHECK(tsdb_open_table(src,"trades",&tbl)==TSDB_OK,"B: src open table");
    tsdb_batch_t *b=NULL; tsdb_batch_begin(tbl,&b);
    for (int i=0;i<NROWS;i++) {
        tsdb_batch_row_ts(b, TS_BASE + (int64_t)i*1000000LL);
        tsdb_batch_row_f64(b, 1, 100.0 + (double)(i%1000)*0.01);
        tsdb_batch_row_i64(b, 2, (int64_t)(i+1));
        tsdb_batch_row_end(b);
    }
    CHECK(tsdb_batch_commit(b)==TSDB_OK, "B: src commit");
    printf("  B captured %d raw blocks (expect %d cols x %d blocks)\n",
           ctx.n, 3, NBLOCKS);
    CHECK(ctx.n == 3*NBLOCKS, "B: captured 3*NBLOCKS raw blocks");

    /* Apply to dst. */
    int applied_ok = 1;
    if (order == 0) {
        for (int i=0;i<ctx.n;i++) if (apply_cap(dst,&ctx.b[i])!=TSDB_OK) applied_ok=0;
    } else {
        /* Interleave by block index: for each of NBLOCKS, apply that block for
         * every column (ts,price,qty), mimicking a per-block streaming sender
         * whose ts column is NOT guaranteed to land last. */
        for (int blk=0; blk<NBLOCKS && applied_ok; blk++) {
            for (int col=0; col<3 && applied_ok; col++) {
                /* find captured entry for (col_idx==col, blk-th occurrence) */
                int seen=0;
                for (int i=0;i<ctx.n;i++) {
                    if (ctx.b[i].col_idx==(uint16_t)col) {
                        if (seen==blk) { if (apply_cap(dst,&ctx.b[i])!=TSDB_OK) applied_ok=0; break; }
                        seen++;
                    }
                }
            }
        }
    }
    CHECK(applied_ok, "B: all raw blocks applied OK");

    /* Read back on the replica via a FRESH plain open (what a wire handler does
     * on a data-bearing node).  This is the cluster condition. */
    tsdb_close(dst); dst=NULL;
    tsdb_db_t *dst2=NULL;
    CHECK(tsdb_open(dst_dir,&dst2)==TSDB_OK, "B: reopen dst");

    int64_t rs = inproc_row_scan(dst2,"trades");
    int nr=-1, rcq=-1; int64_t cv = inproc_count_star(dst2,"trades",&nr,&rcq);
    printf("  B replica: SELECT ts -> %lld | count(*) rc=%d rows=%d val=%lld\n",
           (long long)rs, rcq, nr, (long long)cv);
    CHECK(rs == NROWS, "B: replica full row scan == NROWS");
    CHECK(rcq==TSDB_OK, "B: replica count(*) rc==OK (no error)");
    CHECK(nr==1, "B: replica count(*) returned exactly 1 row (not empty set)");
    CHECK(cv==NROWS, "B: replica count(*) == NROWS");

    tsdb_close(src); if (dst2) tsdb_close(dst2);
    for (int i=0;i<ctx.n;i++) free(ctx.b[i].bytes);
    { char c[300]; snprintf(c,sizeof(c),"rm -rf %s %s",src_dir,dst_dir); (void)system(c); }
}

/* ===== Part C: stable-mirror self-child, multi-block, successful open ====== */
/* A plain table is also a childless super-table.  A bare count(*) routes to
 * exec_stable_select, which recovers the same-named storage as the sole child.
 * With a MULTI-block partition the per-child merge must still yield 1 row. */
static void test_stable_selfchild_multiblock(void) {
    printf("\n--- Part C: stable-mirror self-child, multi-block ---\n");
    char dir[256]; snprintf(dir,sizeof(dir),"/tmp/tsdb_wcd_sc_%d",(int)getpid());
    tsdb_db_t *db=NULL; CHECK(tsdb_open(dir,&db)==TSDB_OK,"C: open");
    tsdb_col_t cols[] = { {"ts",TSDB_TYPE_TIMESTAMP},{"price",TSDB_TYPE_FLOAT64},{"qty",TSDB_TYPE_INT64} };
    CHECK(tsdb_create_table_ex2(db,"trades",cols,3,"ts",TSDB_CREATE_PART_DAY,BLOCK_PTS)==TSDB_OK,"C: create");

    tsdb_table_t *tbl=NULL; tsdb_open_table(db,"trades",&tbl);
    tsdb_batch_t *b=NULL; tsdb_batch_begin(tbl,&b);
    for (int i=0;i<NROWS;i++){ tsdb_batch_row_ts(b,TS_BASE+(int64_t)i*1000000LL);
        tsdb_batch_row_f64(b,1,(double)(i%1000)); tsdb_batch_row_i64(b,2,(int64_t)(i+1));
        tsdb_batch_row_end(b);}
    CHECK(tsdb_batch_commit(b)==TSDB_OK,"C: commit");
    /* close+reopen so the read goes through the stable fallback (no live handle). */
    tsdb_close(db); db=NULL; CHECK(tsdb_open(dir,&db)==TSDB_OK,"C: reopen");

    int nr=-1,rcq=-1; int64_t cv=inproc_count_star(db,"trades",&nr,&rcq);
    printf("  C: count(*) rc=%d rows=%d val=%lld\n", rcq, nr, (long long)cv);
    CHECK(rcq==TSDB_OK,"C: count(*) rc==OK");
    CHECK(nr==1,"C: count(*) returned exactly 1 row (not empty set)");
    CHECK(cv==NROWS,"C: count(*) == NROWS");

    tsdb_close(db); { char c[300]; snprintf(c,sizeof(c),"rm -rf %s",dir); (void)system(c); }
}

/* ===== Part D: force the SLOW column-read scan path (suspect #2) =========== */
/* TSDB_DISABLE_STATS_FASTPATH=1 forces count(*) through tsdb_par_scan_task's
 * per-source column load, which RE-reads the ts column's blocks and matches by
 * (ts_min,count) then tsdb_part_read_block — the exact code the task flags as
 * suspect #2.  Exercise it against a multi-block partition. */
static void test_slowpath_multiblock(void) {
    printf("\n--- Part D: forced slow column-scan path (multi-block) ---\n");
    setenv("TSDB_DISABLE_STATS_FASTPATH","1",1);
    char dir[256]; snprintf(dir,sizeof(dir),"/tmp/tsdb_wcd_sp_%d",(int)getpid());
    tsdb_db_t *db=NULL; CHECK(tsdb_open(dir,&db)==TSDB_OK,"D: open");
    tsdb_col_t cols[] = { {"ts",TSDB_TYPE_TIMESTAMP},{"price",TSDB_TYPE_FLOAT64},{"qty",TSDB_TYPE_INT64} };
    CHECK(tsdb_create_table_ex2(db,"trades",cols,3,"ts",TSDB_CREATE_PART_DAY,BLOCK_PTS)==TSDB_OK,"D: create");

    tsdb_table_t *tbl=NULL; tsdb_open_table(db,"trades",&tbl);
    tsdb_batch_t *b=NULL; tsdb_batch_begin(tbl,&b);
    for (int i=0;i<NROWS;i++){ tsdb_batch_row_ts(b,TS_BASE+(int64_t)i*1000000LL);
        tsdb_batch_row_f64(b,1,(double)(i%1000)); tsdb_batch_row_i64(b,2,(int64_t)(i+1));
        tsdb_batch_row_end(b);}
    CHECK(tsdb_batch_commit(b)==TSDB_OK,"D: commit");
    tsdb_close(db); db=NULL; CHECK(tsdb_open(dir,&db)==TSDB_OK,"D: reopen");

    int nr=-1,rcq=-1; int64_t cv=inproc_count_star(db,"trades",&nr,&rcq);
    printf("  D: count(*) rc=%d rows=%d val=%lld\n", rcq, nr, (long long)cv);
    CHECK(rcq==TSDB_OK,"D: count(*) rc==OK (slow path no error)");
    CHECK(nr==1,"D: count(*) returned exactly 1 row");
    CHECK(cv==NROWS,"D: count(*) == NROWS (slow path)");

    tsdb_close(db); unsetenv("TSDB_DISABLE_STATS_FASTPATH");
    { char c[300]; snprintf(c,sizeof(c),"rm -rf %s",dir); (void)system(c); }
}

/* ===== Part E: exact REPLICA state — stable mirror from catalog-sync +
 *               plain table from schema-sync (suppress_hook) + raw-block data ==
 *
 * On a cluster non-owner node the plain table is provisioned via a
 * suppress_hook=1 path (SCHEMA_SYNC / raft replay), which does NOT auto-register
 * the stable mirror; the mirror arrives SEPARATELY via catalog-sync of the
 * originator's stables.log.  The partition .col/.idx arrive via raw-block
 * replication.  This builds that exact three-source state and queries count(*)
 * over the WIRE on the LIVE node — the precise server condition. */
static void test_replica_state_wire(void) {
    printf("\n--- Part E: exact replica state (stable-sync + schema-sync + raw blocks), WIRE ---\n");
    char src_dir[256], dst_dir[256];
    snprintf(src_dir,sizeof(src_dir),"/tmp/tsdb_wcd_e_src_%d",(int)getpid());
    snprintf(dst_dir,sizeof(dst_dir),"/tmp/tsdb_wcd_e_dst_%d",(int)getpid());

    /* --- source: produce NBLOCKS raw blocks per column --- */
    tsdb_db_t *src=NULL; CHECK(tsdb_open(src_dir,&src)==TSDB_OK,"E: open src");
    tsdb_col_t cols[] = { {"ts",TSDB_TYPE_TIMESTAMP},{"price",TSDB_TYPE_FLOAT64},{"qty",TSDB_TYPE_INT64} };
    CHECK(tsdb_create_table_ex2(src,"lt_0",cols,3,"ts",TSDB_CREATE_PART_DAY,BLOCK_PTS)==TSDB_OK,"E: src create");
    cap_ctx_t ctx; memset(&ctx,0,sizeof(ctx));
    tsdb_db_set_raw_block_hook(src, cap_hook, &ctx);
    tsdb_table_t *tbl=NULL; tsdb_open_table(src,"lt_0",&tbl);
    tsdb_batch_t *b=NULL; tsdb_batch_begin(tbl,&b);
    for (int i=0;i<NROWS;i++){ tsdb_batch_row_ts(b,TS_BASE+(int64_t)i*1000000LL);
        tsdb_batch_row_f64(b,1,(double)(i%1000)); tsdb_batch_row_i64(b,2,(int64_t)(i+1));
        tsdb_batch_row_end(b);}
    CHECK(tsdb_batch_commit(b)==TSDB_OK,"E: src commit");
    CHECK(ctx.n==3*NBLOCKS,"E: captured 3*NBLOCKS raw blocks");

    /* --- replica node: build the three independent pieces --- */
    tsdb_db_t *dst=NULL; CHECK(tsdb_open(dst_dir,&dst)==TSDB_OK,"E: open dst");

    /* (1) schema-sync analogue: plain table via suppress_hook path (NO auto
     *     stable mirror — exactly what a replica's SCHEMA_SYNC receiver does). */
    CHECK(tsdb_create_table_local(dst,"lt_0",cols,3,"ts")==TSDB_OK,"E: dst schema-sync (create_local)");
    tsdb_catalog_t *cat = tsdb_db_catalog(dst);
    CHECK(cat!=NULL,"E: dst catalog");
    CHECK(!tsdb_stable_exists(cat,"lt_0"),"E: create_local did NOT auto-register stable mirror");

    /* (2) catalog-sync analogue: register the stable mirror INDEPENDENTLY, like
     *     receiving the originator's stables.log entry. */
    {
        tsdb_stable_t st; memset(&st,0,sizeof(st));
        snprintf(st.name,sizeof(st.name),"%s","lt_0");
        st.ncols=3; st.ts_col_idx=0; st.ntag_cols=0; st.created_at=1;
        const char *nm[3]={"ts","price","qty"};
        tsdb_type_t ty[3]={TSDB_TYPE_TIMESTAMP,TSDB_TYPE_FLOAT64,TSDB_TYPE_INT64};
        for (int i=0;i<3;i++){ snprintf(st.cols[i].name,sizeof(st.cols[i].name),"%s",nm[i]); st.cols[i].type=ty[i]; }
        CHECK(tsdb_stable_create(cat,&st)==TSDB_OK,"E: catalog-sync stable mirror");
    }
    CHECK(tsdb_stable_exists(cat,"lt_0"),"E: lt_0 is now a stable mirror");

    /* (3) raw-block replication: apply all captured blocks. */
    int applied_ok=1;
    for (int i=0;i<ctx.n;i++) if (apply_cap(dst,&ctx.b[i])!=TSDB_OK) applied_ok=0;
    CHECK(applied_ok,"E: raw blocks applied");

    /* Restart the replica so all three pieces reload from disk (stables.log,
     * schema.bin, partition files) — the steady-state of a live cluster node. */
    tsdb_close(dst); dst=NULL;
    CHECK(tsdb_open(dst_dir,&dst)==TSDB_OK,"E: reopen replica");
    cat = tsdb_db_catalog(dst);
    CHECK(tsdb_stable_exists(cat,"lt_0"),"E: stable mirror survived restart");

    /* Start a wire server on the LIVE replica db and query count(*) over it. */
    tsdb_server_opts_t opts = { .bind_addr="127.0.0.1:0", .max_conns=64, .write_workers=0, .db=dst };
    tsdb_server_t *srv=NULL; CHECK(tsdb_server_start(&opts,&srv)==TSDB_OK,"E: server_start");
    int port = tsdb_server_port(srv); usleep(20000);

    /* In-process first (same db handle the server uses). */
    int nr=-1,rcq=-1; int64_t cv=inproc_count_star(dst,"lt_0",&nr,&rcq);
    int64_t rs = inproc_row_scan(dst,"lt_0");
    printf("  E in-proc: SELECT ts -> %lld | count(*) rc=%d rows=%d val=%lld\n",
           (long long)rs, rcq, nr, (long long)cv);
    CHECK(rs==NROWS,"E: in-proc row scan == NROWS");
    CHECK(rcq==TSDB_OK && nr==1 && cv==NROWS,"E: in-proc count(*) == NROWS (not empty set)");

    /* Wire — the exact server symptom. */
    int wn=-1,we=0; int64_t wc=wire_count_star(port,"lt_0",&wn,&we);
    printf("  E wire: rows=%d err=%d count=%lld\n", wn, we, (long long)wc);
    CHECK(we==0,"E: wire count(*) no ERROR");
    CHECK(wn==1,"E: wire count(*) returned exactly 1 row (NOT empty set)");
    CHECK(wc==NROWS,"E: wire count(*) == NROWS");

    tsdb_server_stop(srv); tsdb_close(src); tsdb_close(dst);
    for (int i=0;i<ctx.n;i++) free(ctx.b[i].bytes);
    { char c[300]; snprintf(c,sizeof(c),"rm -rf %s %s",src_dir,dst_dir); (void)system(c); }
}

/* ===== Part F: stable mirror + multi-block partition + schema.bin GONE =====
 *
 * The dc29571 L3 recovery rebuilds the local schema from the stable record when
 * the self-open fails but storage exists.  test_stable_open_fail.c only exercises
 * this with ONE block (5000 rows < 8192).  This drives the recovery with a
 * MULTI-block partition and asserts count(*) returns the rows (recovery works)
 * — never an empty result set. */
static void test_stable_recovery_multiblock(void) {
    printf("\n--- Part F: stable recovery, schema.bin gone, multi-block ---\n");
    char dir[256]; snprintf(dir,sizeof(dir),"/tmp/tsdb_wcd_f_%d",(int)getpid());

    tsdb_db_t *db=NULL; CHECK(tsdb_open(dir,&db)==TSDB_OK,"F: open");
    tsdb_col_t cols[] = { {"ts",TSDB_TYPE_TIMESTAMP},{"price",TSDB_TYPE_FLOAT64},{"qty",TSDB_TYPE_INT64} };
    CHECK(tsdb_create_table_ex2(db,"lt_0",cols,3,"ts",TSDB_CREATE_PART_DAY,BLOCK_PTS)==TSDB_OK,"F: create");
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    CHECK(tsdb_stable_exists(cat,"lt_0"),"F: lt_0 mirrored as stable");

    tsdb_table_t *tbl=NULL; tsdb_open_table(db,"lt_0",&tbl);
    tsdb_batch_t *b=NULL; tsdb_batch_begin(tbl,&b);
    for (int i=0;i<NROWS;i++){ tsdb_batch_row_ts(b,TS_BASE+(int64_t)i*1000000LL);
        tsdb_batch_row_f64(b,1,(double)(i%1000)); tsdb_batch_row_i64(b,2,(int64_t)(i+1));
        tsdb_batch_row_end(b);}
    CHECK(tsdb_batch_commit(b)==TSDB_OK,"F: commit");
    tsdb_close(db); db=NULL;

    /* Replica-like damage: remove schema.bin, keep multi-block partition. */
    char sp[512]; snprintf(sp,sizeof(sp),"%s/lt_0/schema.bin",dir);
    CHECK(remove(sp)==0,"F: removed schema.bin");

    CHECK(tsdb_open(dir,&db)==TSDB_OK,"F: reopen");
    cat = tsdb_db_catalog(db);
    CHECK(tsdb_stable_exists(cat,"lt_0"),"F: stable survived restart");

    int nr=-1,rcq=-1; int64_t cv=inproc_count_star(db,"lt_0",&nr,&rcq);
    printf("  F: count(*) rc=%d rows=%d val=%lld\n", rcq, nr, (long long)cv);
    /* The acceptance bar from task #174: NEVER an empty result set for on-disk
     * data.  Recovery should return the rows; a surfaced error is also non-empty
     * (acceptable), but a silent 0-row TSDB_OK is the bug. */
    int empty_set = (rcq==TSDB_OK && nr==0);
    CHECK(!empty_set, "F: NOT an empty result set (no silent 0-row lie)");
    if (rcq==TSDB_OK) CHECK(nr==1 && cv==NROWS, "F: recovery returned the full count");

    tsdb_close(db); { char c[300]; snprintf(c,sizeof(c),"rm -rf %s",dir); (void)system(c); }
}

int main(void) {
    printf("=== test_wire_count_disk: multi-block count(*) on disk (task #174 L4) ===\n");
    test_write_batch_path();
    test_rawblock_path(0, "capture order (ts last)");
    test_rawblock_path(1, "interleaved per-block (ts NOT last)");
    test_stable_selfchild_multiblock();
    test_slowpath_multiblock();
    test_replica_state_wire();
    test_stable_recovery_multiblock();
    printf("\n=== test_wire_count_disk: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
