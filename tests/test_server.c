/* test_server.c — end-to-end tests for tsdb TCP wire-protocol server.
 *
 * Strategy: fork() a child that opens a DB and starts the server.
 *           Parent connects directly with socket()/connect(), sends frames
 *           hand-crafted with tsdb_proto_send(), and verifies responses.
 *
 * Tests:
 *  1. HELLO → HELLO_OK
 *  2. CREATE_TABLE (trades) → OK
 *  3. WRITE_BATCH (100 rows) → WRITE_ACK
 *  4. QUERY "SELECT count(*) FROM trades" → HDR + ROWS (count=100) + FIN
 *  5. PING → PONG
 *  6. 10 concurrent clients, each writes 100 rows → total count = 1000
 *  7. Bad frame magic → server closes connection
 */

#include "../src/server/server.h"
#include "../src/server/proto.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>

/* ---- Test infrastructure ------------------------------------------------- */

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
        g_fail++; \
    } else { \
        printf("PASS: %s\n", msg); \
        g_pass++; \
    } \
} while(0)

/* ---- Client helpers ------------------------------------------------------ */

static int client_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    /* Retry loop to handle server not yet ready. */
    for (int i = 0; i < 50; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) return fd;
        if (errno == ECONNREFUSED) { usleep(20000); continue; }
        break;
    }
    close(fd);
    return -1;
}

static int read_all_n(int fd, uint8_t *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

/* Receive one frame; returns payload in *out (malloc'd, caller frees).
 * Returns 0 on success. */
static int recv_frame(int fd, tsdb_frame_hdr_t *hdr, uint8_t **out) {
    return tsdb_proto_recv(fd, hdr, out);
}

/* ---- Payload builders ---------------------------------------------------- */

/*
 * Build CREATE_TABLE payload:
 *   [table_name_len u8] [table_name]
 *   [ts_col_len u8]     [ts_col]
 *   [ncols u8]
 *   for each col: [name_len u8] [name] [type u8]
 */
static size_t build_create_table(uint8_t *buf, size_t cap,
                                  const char *tname, const char *ts_col,
                                  int ncols,
                                  const char **col_names, const int *col_types) {
    uint8_t *p = buf;
    (void)cap;
    uint8_t tnl = (uint8_t)strlen(tname);
    *p++ = tnl; memcpy(p, tname, tnl); p += tnl;
    uint8_t tsl = (uint8_t)strlen(ts_col);
    *p++ = tsl; memcpy(p, ts_col, tsl); p += tsl;
    *p++ = (uint8_t)ncols;
    for (int i = 0; i < ncols; i++) {
        uint8_t nl = (uint8_t)strlen(col_names[i]);
        *p++ = nl; memcpy(p, col_names[i], nl); p += nl;
        *p++ = (uint8_t)col_types[i];
    }
    return (size_t)(p - buf);
}

/*
 * Build WRITE_BATCH payload for a simple trades table:
 *   columns: ts(TIMESTAMP), price(FLOAT64), qty(INT64)
 *   nrows rows of test data
 *
 * Wire columnar payload:
 *   [table_name_len u8][table_name]
 *   [ncols u16][nrows u32]
 *   for each col:
 *     [col_name_len u8][col_name][type u8][codec u8][size u32][data]
 */
static size_t build_write_batch(uint8_t *buf, const char *tname,
                                 int nrows, int64_t ts_offset) {
    uint8_t *p = buf;

    /* table name */
    uint8_t tnl = (uint8_t)strlen(tname);
    *p++ = tnl; memcpy(p, tname, tnl); p += tnl;

    /* ncols=3, nrows */
    uint16_t nc = 3;
    memcpy(p, &nc, 2); p += 2;
    uint32_t nr = (uint32_t)nrows;
    memcpy(p, &nr, 4); p += 4;

    /* Col 0: ts (TIMESTAMP, RAW) */
    {
        const char *cn = "ts";
        uint8_t cnl = (uint8_t)strlen(cn);
        *p++ = cnl; memcpy(p, cn, cnl); p += cnl;
        *p++ = TSDB_TYPE_TIMESTAMP;  /* type */
        *p++ = 0;                     /* codec = RAW */
        uint32_t csz = (uint32_t)(nrows * 8);
        memcpy(p, &csz, 4); p += 4;
        for (int i = 0; i < nrows; i++) {
            int64_t ts = ts_offset + (int64_t)i * 1000000LL; /* 1ms apart */
            memcpy(p, &ts, 8); p += 8;
        }
    }

    /* Col 1: price (FLOAT64, RAW) */
    {
        const char *cn = "price";
        uint8_t cnl = (uint8_t)strlen(cn);
        *p++ = cnl; memcpy(p, cn, cnl); p += cnl;
        *p++ = TSDB_TYPE_FLOAT64;
        *p++ = 0;
        uint32_t csz = (uint32_t)(nrows * 8);
        memcpy(p, &csz, 4); p += 4;
        for (int i = 0; i < nrows; i++) {
            double v = 100.0 + (double)i * 0.01;
            memcpy(p, &v, 8); p += 8;
        }
    }

    /* Col 2: qty (INT64, RAW) */
    {
        const char *cn = "qty";
        uint8_t cnl = (uint8_t)strlen(cn);
        *p++ = cnl; memcpy(p, cn, cnl); p += cnl;
        *p++ = TSDB_TYPE_INT64;
        *p++ = 0;
        uint32_t csz = (uint32_t)(nrows * 8);
        memcpy(p, &csz, 4); p += 4;
        for (int i = 0; i < nrows; i++) {
            int64_t v = (int64_t)(i + 1);
            memcpy(p, &v, 8); p += 8;
        }
    }

    return (size_t)(p - buf);
}

/* ---- In-process server (used by tests) ----------------------------------- */
/* We start the server in-process (no fork) using a temp directory.
 * This avoids complications with shared memory / sockets across fork. */

static tsdb_server_t *g_test_srv = NULL;
static tsdb_db_t     *g_test_db  = NULL;
static int            g_test_port = 0;
static char           g_test_dir[256];

static int setup_server(void) {
    snprintf(g_test_dir, sizeof(g_test_dir), "/tmp/tsdb_test_server_%d", (int)getpid());

    int rc = tsdb_open(g_test_dir, &g_test_db);
    if (rc != TSDB_OK) { fprintf(stderr, "tsdb_open failed: %s\n", tsdb_errstr(rc)); return -1; }

    tsdb_server_opts_t opts = {
        .bind_addr    = "127.0.0.1:0",  /* port 0 = OS assigns */
        .max_conns    = 64,
        .write_workers = 0,
        .db           = g_test_db,
    };
    rc = tsdb_server_start(&opts, &g_test_srv);
    if (rc != TSDB_OK) { fprintf(stderr, "server_start failed: %s\n", tsdb_errstr(rc)); return -1; }

    g_test_port = tsdb_server_port(g_test_srv);
    printf("Test server on port %d\n", g_test_port);
    usleep(10000); /* give accept thread time to start */
    return 0;
}

static void teardown_server(void) {
    if (g_test_srv) { tsdb_server_stop(g_test_srv); g_test_srv = NULL; }
    if (g_test_db)  { tsdb_close(g_test_db); g_test_db = NULL; }
    /* Clean up tmp dir. */
    char cmd[300];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_test_dir);
    (void)system(cmd);
}

/* ---- Test 1: HELLO ------------------------------------------------------- */
static void test_hello(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect");
    if (fd < 0) return;

    int rc = tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    CHECK(rc == TSDB_OK, "send HELLO");

    tsdb_frame_hdr_t hdr;
    uint8_t *payload = NULL;
    rc = recv_frame(fd, &hdr, &payload);
    CHECK(rc == TSDB_OK, "recv HELLO_OK");
    CHECK(hdr.type == TSDB_MT_HELLO_OK, "type == HELLO_OK");
    CHECK(hdr.req_id == 1, "req_id echoed");

    free(payload);
    close(fd);
}

/* ---- Test 2: CREATE_TABLE ------------------------------------------------ */
static void test_create_table(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for create_table");
    if (fd < 0) return;

    /* First HELLO. */
    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    /* Build CREATE_TABLE payload. */
    const char *col_names[] = { "ts", "price", "qty" };
    int col_types[] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_FLOAT64, TSDB_TYPE_INT64 };
    uint8_t buf[512];
    size_t sz = build_create_table(buf, sizeof(buf), "trades", "ts",
                                    3, col_names, col_types);

    int rc = tsdb_proto_send(fd, TSDB_MT_CREATE_TABLE, 0, 2, buf, sz);
    CHECK(rc == TSDB_OK, "send CREATE_TABLE");

    rc = recv_frame(fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv CREATE_TABLE response");
    CHECK(hdr.type == TSDB_MT_HELLO_OK || hdr.type == TSDB_MT_ERROR,
          "CREATE_TABLE got OK or ERROR");
    CHECK(hdr.type == TSDB_MT_HELLO_OK, "CREATE_TABLE → OK");
    free(pl); pl = NULL;

    close(fd);
}

/* ---- Test 3: WRITE_BATCH ------------------------------------------------- */
static void test_write_batch(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for write_batch");
    if (fd < 0) return;

    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    uint8_t *wbuf = (uint8_t *)malloc(1024 * 1024);
    if (!wbuf) { close(fd); return; }
    size_t wsz = build_write_batch(wbuf, "trades", 100, 1000000000000000000LL);

    int rc = tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, 3, wbuf, wsz);
    CHECK(rc == TSDB_OK, "send WRITE_BATCH");
    free(wbuf);

    rc = recv_frame(fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv WRITE_ACK");
    CHECK(hdr.type == TSDB_MT_WRITE_ACK, "type == WRITE_ACK");

    if (pl && hdr.payload_len >= 4) {
        uint32_t acked;
        memcpy(&acked, pl, 4);
        CHECK(acked == 100, "WRITE_ACK acked 100 rows");
    }
    free(pl);

    close(fd);
}

/* ---- Test 4: QUERY ------------------------------------------------------- */
static void test_query(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for query");
    if (fd < 0) return;

    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    const char *qtl = "SELECT count(*) FROM trades";
    int rc = tsdb_proto_send(fd, TSDB_MT_QUERY, 0, 4,
                              (const uint8_t *)qtl, strlen(qtl));
    CHECK(rc == TSDB_OK, "send QUERY");

    /* Expect: QUERY_RESULT_HDR, then QUERY_RESULT_ROWS with FIN. */
    rc = recv_frame(fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv QUERY_RESULT_HDR");
    CHECK(hdr.type == TSDB_MT_QUERY_RESULT_HDR || hdr.type == TSDB_MT_ERROR,
          "got HDR or ERROR");

    if (hdr.type == TSDB_MT_QUERY_RESULT_HDR) {
        CHECK(1, "QUERY_RESULT_HDR received");
        free(pl); pl = NULL;

        /* Drain ROWS frames until FIN. */
        int64_t count_val = -1;
        int got_fin = 0;
        while (!got_fin) {
            rc = recv_frame(fd, &hdr, &pl);
            if (rc != TSDB_OK) break;
            CHECK(hdr.type == TSDB_MT_QUERY_RESULT_ROWS, "got ROWS frame");

            /* Decode: [nrows u32][ncols u16][data...] */
            if (pl && hdr.payload_len >= 6) {
                uint32_t nr; memcpy(&nr, pl, 4);
                /* uint16_t nc; memcpy(&nc, pl+4, 2); */
                if (nr == 1 && hdr.payload_len >= 6 + 8) {
                    /* First col first row. */
                    memcpy(&count_val, pl + 6, 8);
                }
            }

            got_fin = (hdr.flags & TSDB_FLAG_FIN) != 0;
            free(pl); pl = NULL;
        }
        CHECK(got_fin, "ROWS stream ended with FIN");
        printf("  count(*) = %lld\n", (long long)count_val);
        /* count(*) should be 100 (from test 3). */
        CHECK(count_val == 100, "count(*) == 100");
    } else {
        free(pl); pl = NULL;
    }

    close(fd);
}

/* ---- Test 5: PING → PONG ------------------------------------------------- */
static void test_ping(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for ping");
    if (fd < 0) return;

    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    int rc = tsdb_proto_send(fd, TSDB_MT_PING, 0, 5, NULL, 0);
    CHECK(rc == TSDB_OK, "send PING");

    rc = recv_frame(fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv PING response");
    CHECK(hdr.type == TSDB_MT_PING, "type == PING (echo)");
    CHECK((hdr.flags & TSDB_FLAG_PONG) != 0, "PONG flag set");
    CHECK(hdr.req_id == 5, "req_id echoed in PONG");
    free(pl);

    close(fd);
}

/* ---- Test 6: 10 concurrent clients --------------------------------------- */

typedef struct {
    int     port;
    int     client_id;
    int     result;  /* 0=ok, nonzero=fail */
} concurrent_arg_t;

static void *concurrent_writer(void *arg) {
    concurrent_arg_t *a = (concurrent_arg_t *)arg;

    int fd = client_connect(a->port);
    if (fd < 0) { a->result = 1; return NULL; }

    /* HELLO */
    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    /* WRITE_BATCH 100 rows each, with unique ts offsets per client */
    uint8_t *wbuf = (uint8_t *)malloc(512 * 1024);
    if (!wbuf) { close(fd); a->result = 2; return NULL; }
    int64_t ts_off = (int64_t)a->client_id * 100LL * 1000000LL
                   + 2000000000000000000LL;
    size_t wsz = build_write_batch(wbuf, "trades_concurrent", 100, ts_off);

    int rc = tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, (uint64_t)(2 + a->client_id),
                              wbuf, wsz);
    free(wbuf);
    if (rc != TSDB_OK) { close(fd); a->result = 3; return NULL; }

    rc = recv_frame(fd, &hdr, &pl);
    if (rc != TSDB_OK || hdr.type != TSDB_MT_WRITE_ACK) {
        free(pl); close(fd); a->result = 4; return NULL;
    }
    free(pl);
    close(fd);
    a->result = 0;
    return NULL;
}

static void test_concurrent(int port) {
    /* First, create the table. */
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for concurrent setup");
    if (fd < 0) return;

    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    const char *col_names[] = { "ts", "price", "qty" };
    int col_types[] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_FLOAT64, TSDB_TYPE_INT64 };
    uint8_t cbuf[512];
    size_t csz = build_create_table(cbuf, sizeof(cbuf), "trades_concurrent", "ts",
                                     3, col_names, col_types);
    tsdb_proto_send(fd, TSDB_MT_CREATE_TABLE, 0, 2, cbuf, csz);
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;
    close(fd);

    #define NCLIENTS 10
    pthread_t tids[NCLIENTS];
    concurrent_arg_t args[NCLIENTS];

    for (int i = 0; i < NCLIENTS; i++) {
        args[i].port      = port;
        args[i].client_id = i;
        args[i].result    = -1;
        pthread_create(&tids[i], NULL, concurrent_writer, &args[i]);
    }

    int all_ok = 1;
    for (int i = 0; i < NCLIENTS; i++) {
        pthread_join(tids[i], NULL);
        if (args[i].result != 0) {
            fprintf(stderr, "  Client %d failed with code %d\n", i, args[i].result);
            all_ok = 0;
        }
    }
    CHECK(all_ok, "all 10 concurrent clients succeeded");

    /* Now query count(*) — should be 1000. */
    fd = client_connect(port);
    CHECK(fd >= 0, "connect for concurrent count query");
    if (fd < 0) return;

    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    const char *qtl = "SELECT count(*) FROM trades_concurrent";
    tsdb_proto_send(fd, TSDB_MT_QUERY, 0, 3, (const uint8_t *)qtl, strlen(qtl));

    recv_frame(fd, &hdr, &pl);
    int64_t total_count = -1;
    int got_fin = 0;

    if (hdr.type == TSDB_MT_QUERY_RESULT_HDR) {
        free(pl); pl = NULL;
        while (!got_fin) {
            int rc2 = recv_frame(fd, &hdr, &pl);
            if (rc2 != TSDB_OK) break;
            if (pl && hdr.payload_len >= 6 + 8) {
                uint32_t nr; memcpy(&nr, pl, 4);
                if (nr == 1) memcpy(&total_count, pl + 6, 8);
            }
            got_fin = (hdr.flags & TSDB_FLAG_FIN) != 0;
            free(pl); pl = NULL;
        }
    } else {
        free(pl); pl = NULL;
    }

    printf("  concurrent count(*) = %lld\n", (long long)total_count);
    CHECK(total_count == 1000, "concurrent count(*) == 1000");

    close(fd);
    #undef NCLIENTS
}

/* ---- Test 7: Bad magic → server closes ---------------------------------- */
static void test_bad_magic(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for bad_magic");
    if (fd < 0) return;

    /* Send a frame with wrong magic. */
    uint8_t bad_frame[28] = {0};
    bad_frame[0] = 0xDE; bad_frame[1] = 0xAD;  /* wrong magic */
    bad_frame[2] = 0xBE; bad_frame[3] = 0xEF;
    bad_frame[4] = 1;   /* ver */
    bad_frame[5] = 1;   /* type = HELLO */
    /* rest zero */

    ssize_t w = write(fd, bad_frame, sizeof(bad_frame));
    (void)w;

    /* Server should close the connection. */
    uint8_t dummy[64];
    ssize_t r = read(fd, dummy, sizeof(dummy));
    CHECK(r <= 0, "server closed connection after bad magic");

    close(fd);
}

/* ---- Test 8: group/device DDL → UNSUPPORTED ------------------------------ */
/* These handlers used to be no-op stubs that ACKed success; they must now
 * answer an honest ERROR/TSDB_ERR_UNSUPPORTED. */
static void test_group_ddl_unsupported(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for group DDL");
    if (fd < 0) return;

    /* CREATE_GROUP payload: [name_len u8]["g1"][nprops u8 = 0] */
    uint8_t pl_out[4] = { 2, 'g', '1', 0 };
    int rc = tsdb_proto_send(fd, TSDB_MT_CREATE_GROUP, 0, 7, pl_out, 4);
    CHECK(rc == TSDB_OK, "send CREATE_GROUP");

    tsdb_frame_hdr_t hdr;
    uint8_t *pl = NULL;
    rc = recv_frame(fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv CREATE_GROUP response");
    CHECK(hdr.type == TSDB_MT_ERROR, "group DDL answers ERROR, not fake OK");
    if (pl && hdr.payload_len >= 4) {
        int32_t ec;
        memcpy(&ec, pl, 4);
        CHECK(ec == TSDB_ERR_UNSUPPORTED, "err code is TSDB_ERR_UNSUPPORTED");
    }
    free(pl);
    close(fd);
}

/* ---- Test 9: CLUSTER_STATS → kv counters --------------------------------- */
/* Response: HELLO_OK with [kv_count u16] then [klen u8][key][vlen u8][val],
 * values as decimal strings.  By this point the suite has run writes and
 * queries, so the counters must be present (queries_total, rows_written). */
static void test_cluster_stats(int port) {
    int fd = client_connect(port);
    CHECK(fd >= 0, "connect for CLUSTER_STATS");
    if (fd < 0) return;

    int rc = tsdb_proto_send(fd, TSDB_MT_CLUSTER_STATS, 0, 8, NULL, 0);
    CHECK(rc == TSDB_OK, "send CLUSTER_STATS");

    tsdb_frame_hdr_t hdr;
    uint8_t *pl = NULL;
    rc = recv_frame(fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv CLUSTER_STATS response");
    CHECK(hdr.type == TSDB_MT_HELLO_OK, "stats response type is HELLO_OK");
    CHECK(hdr.payload_len >= 2, "stats payload non-empty");

    int found_queries = 0;
    long long rows_written = -1;
    if (pl && hdr.payload_len >= 2) {
        const uint8_t *p   = pl;
        const uint8_t *end = pl + hdr.payload_len;
        uint16_t nkv = (uint16_t)(p[0] | (p[1] << 8));
        p += 2;
        CHECK(nkv >= 5, "stats has at least 5 kv pairs");
        for (uint16_t i = 0; i < nkv && p < end; i++) {
            uint8_t kl = *p++;
            if (p + kl > end) break;
            char key[64] = {0};
            memcpy(key, p, kl < 63 ? kl : 63);
            p += kl;
            if (p >= end) break;
            uint8_t vl = *p++;
            if (p + vl > end) break;
            char val[32] = {0};
            memcpy(val, p, vl < 31 ? vl : 31);
            p += vl;
            if (strcmp(key, "queries_total") == 0)      found_queries = 1;
            if (strcmp(key, "rows_written_total") == 0) rows_written = atoll(val);
        }
    }
    CHECK(found_queries, "stats contains queries_total");
    CHECK(rows_written >= 1100, "rows_written_total counts prior writes");

    free(pl);
    close(fd);
}

/* ---- CRC32C self-test ---------------------------------------------------- */
static void test_crc32c(void) {
    /* Known vector: crc32c("123456789") == 0xE3069283 */
    const uint8_t msg[] = {'1','2','3','4','5','6','7','8','9'};
    uint32_t got = tsdb_crc32c(msg, 9);
    CHECK(got == 0xE3069283u, "crc32c(\"123456789\") == 0xE3069283");

    /* Round-trip: encode then decode a frame, verify CRC not triggered. */
    uint8_t *pl2 = NULL;
    tsdb_frame_hdr_t h2;
    /* We need a pipe or socketpair for round-trip test. */
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0) {
        const char *data = "hello world";
        tsdb_proto_send(sv[0], TSDB_MT_PING, 0, 42,
                        (const uint8_t *)data, strlen(data));
        close(sv[0]);
        int rc = tsdb_proto_recv(sv[1], &h2, &pl2);
        CHECK(rc == TSDB_OK, "round-trip recv ok");
        CHECK(h2.type == TSDB_MT_PING, "round-trip type");
        CHECK(h2.req_id == 42, "round-trip req_id");
        CHECK(h2.payload_len == strlen(data), "round-trip payload_len");
        if (pl2) CHECK(memcmp(pl2, data, strlen(data)) == 0, "round-trip payload data");
        free(pl2);
        close(sv[1]);
    }
}

/* ---- Perf / throughput measurement (informational) ----------------------- */
static void bench_write(int port) {
    int fd = client_connect(port);
    if (fd < 0) { printf("bench_write: connect failed\n"); return; }

    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    /* Create bench table. */
    const char *col_names[] = { "ts", "val" };
    int col_types[] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_FLOAT64 };
    uint8_t cbuf[256];
    size_t csz = build_create_table(cbuf, sizeof(cbuf), "bench_tbl", "ts",
                                     2, col_names, col_types);
    tsdb_proto_send(fd, TSDB_MT_CREATE_TABLE, 0, 2, cbuf, csz);
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    /* Send 10 batches of 10000 rows. */
    const int BATCHES = 10;
    const int ROWS    = 10000;

    uint8_t *wbuf = (uint8_t *)malloc(2 * 1024 * 1024);
    if (!wbuf) { close(fd); return; }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int b = 0; b < BATCHES; b++) {
        /* Build simple 2-col batch inline. */
        uint8_t *p = wbuf;
        /* table name */
        const char *tn = "bench_tbl";
        uint8_t tnl = (uint8_t)strlen(tn);
        *p++ = tnl; memcpy(p, tn, tnl); p += tnl;
        /* ncols=2, nrows */
        uint16_t nc = 2; memcpy(p, &nc, 2); p += 2;
        uint32_t nr = (uint32_t)ROWS; memcpy(p, &nr, 4); p += 4;
        /* ts col */
        const char *cn0 = "ts"; *p++ = 2; memcpy(p, cn0, 2); p += 2;
        *p++ = TSDB_TYPE_TIMESTAMP; *p++ = 0;
        uint32_t csz0 = (uint32_t)(ROWS * 8); memcpy(p, &csz0, 4); p += 4;
        for (int i = 0; i < ROWS; i++) {
            int64_t ts = (int64_t)b * ROWS * 1000LL + i * 1000LL + 3000000000000000000LL;
            memcpy(p, &ts, 8); p += 8;
        }
        /* val col */
        const char *cn1 = "val"; *p++ = 3; memcpy(p, cn1, 3); p += 3;
        *p++ = TSDB_TYPE_FLOAT64; *p++ = 0;
        uint32_t csz1 = (uint32_t)(ROWS * 8); memcpy(p, &csz1, 4); p += 4;
        for (int i = 0; i < ROWS; i++) {
            double v = (double)(b * ROWS + i);
            memcpy(p, &v, 8); p += 8;
        }

        size_t wsz = (size_t)(p - wbuf);
        tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, (uint64_t)(100 + b), wbuf, wsz);
        recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    free(wbuf);

    double elapsed = (t1.tv_sec - t0.tv_sec)
                   + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
    long long total_rows = (long long)BATCHES * ROWS;
    printf("bench_write: %lld rows in %.3f s = %.0f rows/sec\n",
           total_rows, elapsed, (double)total_rows / elapsed);

    /* Query latency: run 10 queries, measure p50. */
    const char *qtl_bench = "SELECT count(*) FROM bench_tbl";
    double latencies[20];
    int nlat = 0;
    for (int q = 0; q < 10 && nlat < 20; q++) {
        struct timespec qa, qb;
        clock_gettime(CLOCK_MONOTONIC, &qa);
        tsdb_proto_send(fd, TSDB_MT_QUERY, 0, (uint64_t)(200+q),
                        (const uint8_t *)qtl_bench, strlen(qtl_bench));
        int fin = 0;
        while (!fin) {
            recv_frame(fd, &hdr, &pl);
            fin = (hdr.type == TSDB_MT_QUERY_RESULT_ROWS &&
                   (hdr.flags & TSDB_FLAG_FIN)) ||
                  (hdr.type == TSDB_MT_ERROR);
            free(pl); pl = NULL;
        }
        clock_gettime(CLOCK_MONOTONIC, &qb);
        latencies[nlat++] = ((qb.tv_sec - qa.tv_sec)
                            + (qb.tv_nsec - qa.tv_nsec) * 1e-9) * 1000.0;
    }
    /* Sort for p50. */
    for (int i = 0; i < nlat; i++)
        for (int j = i+1; j < nlat; j++)
            if (latencies[j] < latencies[i]) {
                double tmp = latencies[i]; latencies[i] = latencies[j]; latencies[j] = tmp;
            }
    printf("bench_query p50 latency: %.3f ms\n", latencies[nlat/2]);

    close(fd);
}

/* ---- Concurrent aggregate throughput benchmark --------------------------- */

typedef struct {
    int     port;
    int     client_id;
    int     nbatches;
    int     rows_per_batch;
    double  elapsed_sec;  /* out */
    int     ok;
} bench_concurrent_arg_t;

static void *bench_concurrent_worker(void *arg) {
    bench_concurrent_arg_t *a = (bench_concurrent_arg_t *)arg;
    a->ok = 0;

    int fd = client_connect(a->port);
    if (fd < 0) { a->ok = -1; return NULL; }

    /* HELLO */
    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    uint8_t *wbuf = (uint8_t *)malloc(2 * 1024 * 1024);
    if (!wbuf) { close(fd); a->ok = -2; return NULL; }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int b = 0; b < a->nbatches; b++) {
        /* Build 2-column batch (ts + val) inline. */
        uint8_t *p = wbuf;
        const char *tn = "bench_concurrent_tbl";
        uint8_t tnl = (uint8_t)strlen(tn);
        *p++ = tnl; memcpy(p, tn, tnl); p += tnl;

        int ROWS = a->rows_per_batch;
        uint16_t nc = 2; memcpy(p, &nc, 2); p += 2;
        uint32_t nr = (uint32_t)ROWS; memcpy(p, &nr, 4); p += 4;

        /* ts col */
        const char *cn0 = "ts"; *p++ = 2; memcpy(p, cn0, 2); p += 2;
        *p++ = TSDB_TYPE_TIMESTAMP; *p++ = 0;
        uint32_t csz0 = (uint32_t)(ROWS * 8); memcpy(p, &csz0, 4); p += 4;
        /* unique base: client_id * (nbatches*ROWS) + b*ROWS, scaled to ns */
        int64_t ts_base = (int64_t)a->client_id * (int64_t)(a->nbatches) * ROWS * 1000LL
                        + (int64_t)b * ROWS * 1000LL
                        + 4000000000000000000LL;
        for (int i = 0; i < ROWS; i++) {
            int64_t ts = ts_base + i;
            memcpy(p, &ts, 8); p += 8;
        }

        /* val col */
        const char *cn1 = "val"; *p++ = 3; memcpy(p, cn1, 3); p += 3;
        *p++ = TSDB_TYPE_FLOAT64; *p++ = 0;
        uint32_t csz1 = (uint32_t)(ROWS * 8); memcpy(p, &csz1, 4); p += 4;
        for (int i = 0; i < ROWS; i++) {
            double v = (double)(a->client_id * a->nbatches * ROWS + b * ROWS + i);
            memcpy(p, &v, 8); p += 8;
        }

        size_t wsz = (size_t)(p - wbuf);
        int rc = tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0,
                                 (uint64_t)(a->client_id * 1000 + b + 100),
                                 wbuf, wsz);
        if (rc != TSDB_OK) { free(wbuf); close(fd); a->ok = -3; return NULL; }

        uint8_t *resp = NULL;
        recv_frame(fd, &hdr, &resp);
        free(resp);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    free(wbuf);
    close(fd);

    a->elapsed_sec = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
    a->ok = 1;
    return NULL;
}

static void bench_concurrent(int port) {
    /* Create the bench table first. */
    int fd = client_connect(port);
    if (fd < 0) { printf("bench_concurrent: connect failed\n"); return; }

    tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;

    const char *col_names[] = { "ts", "val" };
    int col_types[] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_FLOAT64 };
    uint8_t cbuf[256];
    size_t csz = build_create_table(cbuf, sizeof(cbuf), "bench_concurrent_tbl", "ts",
                                     2, col_names, col_types);
    tsdb_proto_send(fd, TSDB_MT_CREATE_TABLE, 0, 2, cbuf, csz);
    recv_frame(fd, &hdr, &pl); free(pl); pl = NULL;
    close(fd);

    #define BENCH_NCLIENTS  10
    #define BENCH_NBATCHES  10
    #define BENCH_ROWS_PER_BATCH 10000

    pthread_t tids[BENCH_NCLIENTS];
    bench_concurrent_arg_t args[BENCH_NCLIENTS];

    /* Record wall-clock for aggregate measure. */
    struct timespec wall0, wall1;
    clock_gettime(CLOCK_MONOTONIC, &wall0);

    for (int i = 0; i < BENCH_NCLIENTS; i++) {
        args[i].port          = port;
        args[i].client_id     = i;
        args[i].nbatches      = BENCH_NBATCHES;
        args[i].rows_per_batch = BENCH_ROWS_PER_BATCH;
        args[i].elapsed_sec   = 0.0;
        args[i].ok            = -1;
        pthread_create(&tids[i], NULL, bench_concurrent_worker, &args[i]);
    }

    int all_ok = 1;
    for (int i = 0; i < BENCH_NCLIENTS; i++) {
        pthread_join(tids[i], NULL);
        if (args[i].ok != 1) { all_ok = 0; }
    }

    clock_gettime(CLOCK_MONOTONIC, &wall1);
    double wall_elapsed = (wall1.tv_sec - wall0.tv_sec)
                        + (wall1.tv_nsec - wall0.tv_nsec) * 1e-9;

    long long total_rows = (long long)BENCH_NCLIENTS * BENCH_NBATCHES * BENCH_ROWS_PER_BATCH;
    double aggregate_rps = (all_ok && wall_elapsed > 0.0)
                         ? (double)total_rows / wall_elapsed
                         : 0.0;

    printf("bench_concurrent: %d clients × %d batches × %d rows = %lld rows total\n",
           BENCH_NCLIENTS, BENCH_NBATCHES, BENCH_ROWS_PER_BATCH, total_rows);
    printf("bench_concurrent: wall time %.3f s  →  aggregate %.0f rows/sec\n",
           wall_elapsed, aggregate_rps);

    if (!all_ok)
        printf("bench_concurrent: WARNING — some workers failed\n");

    #undef BENCH_NCLIENTS
    #undef BENCH_NBATCHES
    #undef BENCH_ROWS_PER_BATCH
}

/* ---- main ---------------------------------------------------------------- */

int main(void) {
    printf("=== test_server ===\n\n");

    /* CRC32C self-test first (no server needed). */
    printf("--- CRC32C ---\n");
    test_crc32c();

    /* Start in-process server. */
    if (setup_server() != 0) {
        fprintf(stderr, "Failed to setup server\n");
        return 1;
    }

    int port = g_test_port;

    printf("\n--- Test 1: HELLO ---\n");
    test_hello(port);

    printf("\n--- Test 2: CREATE_TABLE ---\n");
    test_create_table(port);

    printf("\n--- Test 3: WRITE_BATCH ---\n");
    test_write_batch(port);

    printf("\n--- Test 4: QUERY ---\n");
    test_query(port);

    printf("\n--- Test 5: PING/PONG ---\n");
    test_ping(port);

    printf("\n--- Test 6: Concurrent (10 clients × 100 rows) ---\n");
    test_concurrent(port);

    printf("\n--- Test 7: Bad magic → connection close ---\n");
    test_bad_magic(port);

    printf("\n--- Test 8: group/device DDL unsupported ---\n");
    test_group_ddl_unsupported(port);

    printf("\n--- Test 9: CLUSTER_STATS ---\n");
    test_cluster_stats(port);

    printf("\n--- Benchmarks (single connection) ---\n");
    bench_write(port);

    printf("\n--- Benchmarks (10-client concurrent aggregate) ---\n");
    bench_concurrent(port);

    teardown_server();

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
