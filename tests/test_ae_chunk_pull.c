/* test_ae_chunk_pull.c — anti-entropy must be able to repair a table whose
 * full contents do not fit in ONE federation result frame (REPL-4).
 *
 * THE BUG.  pull_table_delta issued a single un-chunked
 * `SELECT * FROM t WHERE ts > X` (X = INT64_MIN for an empty local table,
 * i.e. the whole table in one query).  Both wire ends cap a federation
 * result at 64 MB — the serving peer encodes into a fixed 64 MB buffer and
 * replies TSDB_RPC_ERR when the result does not fit, and the client
 * refuses a truncated frame — so a table whose SELECT * exceeds 64 MB
 * (~800K rows at 10 columns, ~130K rows at 65) could NEVER be pulled.
 * Every 30 s sweep re-ran the same doomed query, re-materializing the
 * whole table on the healthy peer each time, forever.
 *
 * THE INVARIANT.  Anti-entropy repair progress must not depend on the
 * repaired table fitting in one RPC frame: the pull walks the ts range in
 * bounded sub-range chunks (splitting guided by cheap peer-side range
 * counts, with bisection on an oversized response as the safety net), and
 * each merged chunk is durable before the next is fetched — a retry
 * resumes instead of restarting.
 *
 * Single process, no forks: the HOLDER is a plain db behind a real RPC
 * server on 127.0.0.1:0; the PULLER is a real cluster node that learns
 * the holder through a direct membership upsert (no gossip needed).
 *
 *   [1] A 135,000-row x 65-column table (~70 MB on the wire, over the
 *       64 MB frame cap) converges onto an empty peer via
 *       tsdb_cluster_resync_table: exact row count, exact max_ts.
 *   [2] Healthy write path end-to-end after the fix: a table written on
 *       the cluster node with the holder ALIVE replicates normally and
 *       the flush that fanned it out reports success (guards the REPL-1
 *       remote-ACK gate against breaking the ACKing-peer path).
 */
#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"
#include "../src/cluster/cluster.h"
#include "../src/cluster/node.h"
#include "../src/cluster/rpc.h"
#include "../src/storage/db.h"    /* tsdb_table_flush */

#include <dirent.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
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

#define DIR_HOLDER "/tmp/tsdb_test_ae_chunk_holder"
#define DIR_PULLER "/tmp/tsdb_test_ae_chunk_puller"
#define RPC_ADDR   "127.0.0.1:28475"
#define HOLDER_ID  ((tsdb_node_id_t)7777ULL)

#define TABLE   "big"
#define NVCOLS  64                       /* + ts = 65 cols, 520 B/row wire */
#define NROWS   135000                   /* 135000 * 520 = 70.2 MB > 64 MB */
#define BASE    1700000000000000000LL
#define STEP    1000000LL                /* 1 ms */

extern struct tsdb_cluster *tsdb_db_cluster(tsdb_db_t *db);

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

static void make_big_table(tsdb_db_t *db) {
    tsdb_col_t cols[1 + NVCOLS];
    static char names[NVCOLS][8];
    cols[0].name = "ts";
    cols[0].type = TSDB_TYPE_TIMESTAMP;
    for (int i = 0; i < NVCOLS; i++) {
        snprintf(names[i], sizeof(names[i]), "c%02d", i);
        cols[1 + i].name = names[i];
        cols[1 + i].type = TSDB_TYPE_INT64;
    }
    OK(tsdb_create_table(db, TABLE, cols, 1 + NVCOLS, "ts"));
}

static void fill_big_table(tsdb_db_t *db) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, TABLE, &t));
    int done = 0;
    while (done < NROWS) {
        int n = NROWS - done > 15000 ? 15000 : NROWS - done;
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < n; i++) {
            int row = done + i;
            OK(tsdb_batch_row_ts(b, BASE + (int64_t)row * STEP));
            for (int cidx = 0; cidx < NVCOLS; cidx++)
                OK(tsdb_batch_row_i64(b, 1 + cidx, (int64_t)row + cidx));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        done += n;
    }
}

static void upsert_holder(tsdb_cluster_t *c, int port, uint64_t version) {
    tsdb_node_info_t info;
    memset(&info, 0, sizeof(info));
    info.id = HOLDER_ID;
    snprintf(info.addr, sizeof(info.addr), "127.0.0.1:%d", port);
    snprintf(info.gossip_addr, sizeof(info.gossip_addr), "127.0.0.1:1");
    info.state   = TSDB_NODE_ALIVE;
    info.version = version;
    tsdb_node_manager_upsert(tsdb_cluster_node_mgr(c), &info);
}

int main(void) {
    printf("=== test_ae_chunk_pull ===\n");
    rm_rf(DIR_HOLDER);
    rm_rf(DIR_PULLER);
    unsetenv("TSDB_CONFIG");
    unsetenv("TSDB_REPLICATION_QUORUM");
    unsetenv("TSDB_SHARD_REPLICA_N");
    unsetenv("TSDB_AE_ROW_DIGEST");
    setenv("TSDB_FANOUT_WAIT_TIMEOUT_MS", "3000", 1);

    /* ---- holder: plain db + real RPC server, 135K rows ------------------ */
    tsdb_db_t *dbh = NULL;
    OK(tsdb_open(DIR_HOLDER, &dbh));
    make_big_table(dbh);
    fill_big_table(dbh);
    {
        uint64_t hc = 0; int64_t hm = 0;
        OK(tsdb_cluster_local_table_stats(dbh, TABLE, &hc, &hm));
        printf("  holder: %" PRIu64 " rows, max_ts=%lld\n", hc, (long long)hm);
        if (hc != NROWS) { fprintf(stderr, "FATAL: holder fill\n"); return 1; }
    }
    tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", dbh, NULL);
    if (!srv) { fprintf(stderr, "FATAL: rpc server\n"); return 1; }
    int holder_port = tsdb_rpc_server_port(srv);

    /* ---- puller: real cluster node, empty table -------------------------- */
    tsdb_db_t *dbp = NULL;
    OK(tsdb_open_cluster(DIR_PULLER, RPC_ADDR, "", &dbp));
    make_big_table(dbp);    /* no peers known yet -> no schema fanout wait */
    tsdb_cluster_t *c = tsdb_db_cluster(dbp);
    if (!c) { fprintf(stderr, "FATAL: no cluster handle\n"); return 1; }
    upsert_holder(c, holder_port, 1);

    printf("\n[1] resync pulls a >64MB table in chunks\n");
    {
        int pulled = 0;
        long t0 = time(NULL);
        int rc = tsdb_cluster_resync_table(dbp, TABLE, &pulled);
        uint64_t lc = 0; int64_t lm = 0;
        OK(tsdb_cluster_local_table_stats(dbp, TABLE, &lc, &lm));
        printf("  resync rc=%d pulled=%d local=(%" PRIu64 ", %lld) in %lds\n",
               rc, pulled, lc, (long long)lm, (long)(time(NULL) - t0));
        CHECK(lc == NROWS,
              "puller converged to %d rows (got %" PRIu64 ")", NROWS, lc);
        CHECK(lm == BASE + (int64_t)(NROWS - 1) * STEP,
              "puller max_ts matches the holder's newest row");
    }

    printf("\n[2] healthy replication path still works after the quorum gate\n");
    {
        upsert_holder(c, holder_port, 2);   /* re-assert ALIVE for phase 2 */
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_INT64     },
        };
        OK(tsdb_create_table(dbp, "pos", cols, 2, "ts"));
        tsdb_table_t *t = NULL;
        OK(tsdb_open_table(dbp, "pos", &t));
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < 100; i++) {
            OK(tsdb_batch_row_ts(b, BASE + i));
            OK(tsdb_batch_row_i64(b, 1, i));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        upsert_holder(c, holder_port, 3);
        int frc = tsdb_table_flush(dbp, "pos");
        CHECK(frc == TSDB_OK, "flush with an ACKing peer succeeds (rc=%d)", frc);

        uint64_t hc = 0; int64_t hm = 0;
        for (int i = 0; i < 30; i++) {
            OK(tsdb_cluster_local_table_stats(dbh, "pos", &hc, &hm));
            if (hc >= 100) break;
            struct timespec sl = { 0, 100000000L };
            nanosleep(&sl, NULL);
        }
        CHECK(hc == 100, "holder received the replicated rows (got %" PRIu64 ")", hc);
    }

    tsdb_close_cluster(dbp);
    tsdb_rpc_server_stop(srv);
    tsdb_close(dbh);
    rm_rf(DIR_HOLDER);
    rm_rf(DIR_PULLER);

    printf("\n%s\n", g_fail ? "FAILED" : "OK");
    return g_fail ? 1 : 0;
}
