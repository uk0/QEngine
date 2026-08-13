/* test_pubsub.c — end-to-end tests for pub/sub subscription.
 *
 * Tests:
 *  1. SUBSCRIBE → HELLO_OK with sub_id
 *  2. Write batch (conn B) → subscriber (conn A) receives SUB_EVENT in the
 *     WRITE_BATCH columnar shape whose decoded rows match the written batch
 *  3. UNSUBSCRIBE (payload = SUBSCRIBE req_id) → no further SUB_EVENTs
 *  4. Column filter: subscriber only receives rows matching filter
 *  5. Deadlock safety: slow subscriber does not block writer
 *  6. Multiple subscribers to same table all receive events
 */

#include "../src/server/server.h"
#include "../src/server/proto.h"
#include "../include/tsdb.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>

/* ---- Assertions ---------------------------------------------------------- */

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
} while (0)

/* ---- Client helpers ------------------------------------------------------ */

static int client_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    for (int i = 0; i < 80; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) return fd;
        if (errno == ECONNREFUSED || errno == ETIMEDOUT) { usleep(25000); continue; }
        break;
    }
    close(fd);
    return -1;
}

static int hello(int fd, uint64_t req_id) {
    int rc = tsdb_proto_send(fd, TSDB_MT_HELLO, 0, req_id, NULL, 0);
    if (rc != TSDB_OK) return rc;
    tsdb_frame_hdr_t hdr;
    uint8_t *pl = NULL;
    rc = tsdb_proto_recv(fd, &hdr, &pl);
    free(pl);
    return (rc == TSDB_OK && hdr.type == TSDB_MT_HELLO_OK) ? 0 : -1;
}

/* Set a socket receive timeout. */
static void set_recv_timeout(int fd, int ms) {
    struct timeval tv;
    tv.tv_sec  = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

/* ---- Payload builders ---------------------------------------------------- */

/* Build CREATE_TABLE for "sensor" table: ts(TIMESTAMP), val(INT64), device(INT64). */
static size_t build_create_sensor(uint8_t *buf) {
    uint8_t *p = buf;
    const char *tname = "sensor";
    uint8_t tnl = (uint8_t)strlen(tname);
    *p++ = tnl; memcpy(p, tname, tnl); p += tnl;
    const char *ts_col = "ts";
    uint8_t tsl = (uint8_t)strlen(ts_col);
    *p++ = tsl; memcpy(p, ts_col, tsl); p += tsl;
    *p++ = 3; /* ncols */
    struct { const char *name; int type; } cols[] = {
        { "ts",     TSDB_TYPE_TIMESTAMP },
        { "val",    TSDB_TYPE_INT64     },
        { "device", TSDB_TYPE_INT64     },
    };
    for (int i = 0; i < 3; i++) {
        uint8_t nl = (uint8_t)strlen(cols[i].name);
        *p++ = nl; memcpy(p, cols[i].name, nl); p += nl;
        *p++ = (uint8_t)cols[i].type;
    }
    return (size_t)(p - buf);
}

/*
 * Build WRITE_BATCH for "sensor" (nrows rows).
 * device_id is stored as an INT64 column; all rows get the same device_id.
 */
static size_t build_write_sensor(uint8_t *buf, int nrows, int64_t ts_start,
                                  int64_t device_id) {
    uint8_t *p = buf;
    const char *tname = "sensor";
    uint8_t tnl = (uint8_t)strlen(tname);
    *p++ = tnl; memcpy(p, tname, tnl); p += tnl;

    uint16_t nc = 3;
    memcpy(p, &nc, 2); p += 2;
    uint32_t nr = (uint32_t)nrows;
    memcpy(p, &nr, 4); p += 4;

    /* Col 0: ts */
    { const char *cn = "ts"; uint8_t cnl=(uint8_t)strlen(cn);
      *p++=cnl; memcpy(p,cn,cnl); p+=cnl;
      *p++=TSDB_TYPE_TIMESTAMP; *p++=0;
      uint32_t csz=(uint32_t)(nrows*8); memcpy(p,&csz,4); p+=4;
      for(int i=0;i<nrows;i++){int64_t ts=ts_start+(int64_t)i*1000; memcpy(p,&ts,8); p+=8;}
    }
    /* Col 1: val */
    { const char *cn = "val"; uint8_t cnl=(uint8_t)strlen(cn);
      *p++=cnl; memcpy(p,cn,cnl); p+=cnl;
      *p++=TSDB_TYPE_INT64; *p++=0;
      uint32_t csz=(uint32_t)(nrows*8); memcpy(p,&csz,4); p+=4;
      for(int i=0;i<nrows;i++){int64_t v=(int64_t)i; memcpy(p,&v,8); p+=8;}
    }
    /* Col 2: device */
    { const char *cn = "device"; uint8_t cnl=(uint8_t)strlen(cn);
      *p++=cnl; memcpy(p,cn,cnl); p+=cnl;
      *p++=TSDB_TYPE_INT64; *p++=0;
      uint32_t csz=(uint32_t)(nrows*8); memcpy(p,&csz,4); p+=4;
      for(int i=0;i<nrows;i++) { memcpy(p,&device_id,8); p+=8; }
    }
    return (size_t)(p - buf);
}

/*
 * Build SUBSCRIBE payload (v1 format):
 *   [table_len u8][table utf8][filter_len u16][filter utf8]
 * filter may be NULL or "" for no filter.
 */
static size_t build_subscribe(uint8_t *buf, const char *table, const char *filter) {
    uint8_t *p = buf;
    uint8_t tl = (uint8_t)strlen(table);
    *p++ = tl; memcpy(p, table, tl); p += tl;
    uint16_t fl = filter ? (uint16_t)strlen(filter) : 0;
    memcpy(p, &fl, 2); p += 2;
    if (fl > 0) { memcpy(p, filter, fl); p += fl; }
    return (size_t)(p - buf);
}

/* ---- Server setup -------------------------------------------------------- */

static tsdb_server_t *g_srv  = NULL;
static tsdb_db_t     *g_db   = NULL;
static int            g_port = 0;
static char           g_dir[256];

static int setup(void) {
    snprintf(g_dir, sizeof(g_dir), "/tmp/tsdb_pubsub_%d", (int)getpid());
    if (tsdb_open(g_dir, &g_db) != TSDB_OK) return -1;

    tsdb_server_opts_t opts = {
        .bind_addr    = "127.0.0.1:0",
        .max_conns    = 128,
        .write_workers = 0,
        .db           = g_db,
    };
    if (tsdb_server_start(&opts, &g_srv) != TSDB_OK) return -1;
    g_port = tsdb_server_port(g_srv);
    printf("[pubsub test] server on port %d\n", g_port);
    usleep(10000);
    return 0;
}

static void teardown(void) {
    if (g_srv) { tsdb_server_stop(g_srv); g_srv = NULL; }
    if (g_db)  { tsdb_close(g_db);  g_db  = NULL; }
    char cmd[300];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_dir);
    (void)system(cmd);
}

/* ---- Helpers: create table + single write -------------------------------- */

static void ensure_sensor_table(void) {
    int fd = client_connect(g_port);
    if (fd < 0) return;
    hello(fd, 1);
    uint8_t buf[512];
    size_t sz = build_create_sensor(buf);
    tsdb_proto_send(fd, TSDB_MT_CREATE_TABLE, 0, 2, buf, sz);
    tsdb_frame_hdr_t hdr; uint8_t *pl=NULL;
    tsdb_proto_recv(fd, &hdr, &pl); free(pl);
    close(fd);
}

static void do_write(int nrows, int64_t ts_start, int64_t device_id) {
    int fd = client_connect(g_port);
    if (fd < 0) return;
    hello(fd, 10);
    uint8_t *buf = (uint8_t *)malloc(1 << 20);
    size_t sz = build_write_sensor(buf, nrows, ts_start, device_id);
    tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, 11, buf, sz);
    free(buf);
    tsdb_frame_hdr_t hdr; uint8_t *pl=NULL;
    tsdb_proto_recv(fd, &hdr, &pl); free(pl);
    close(fd);
}

/* ---- Test 1: Subscribe → sub_id ack -------------------------------------- */
static void test_subscribe_ack(void) {
    printf("\n[test 1] Subscribe ack\n");
    int fd = client_connect(g_port);
    CHECK(fd >= 0, "connect for subscribe");
    if (fd < 0) return;
    hello(fd, 1);

    uint8_t sbuf[128];
    size_t ssz = build_subscribe(sbuf, "sensor", NULL);
    int rc = tsdb_proto_send(fd, TSDB_MT_SUBSCRIBE, 0, 20, sbuf, ssz);
    CHECK(rc == TSDB_OK, "send SUBSCRIBE");

    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    rc = tsdb_proto_recv(fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv SUBSCRIBE ack");
    CHECK(hdr.type == TSDB_MT_HELLO_OK, "SUBSCRIBE → HELLO_OK");
    uint64_t sub_id = 0;
    if (pl && hdr.payload_len >= 8) {
        memcpy(&sub_id, pl, 8);
    }
    CHECK(sub_id > 0, "sub_id > 0");
    free(pl);

    close(fd);
}

/* ---- Test 2: Write → subscriber receives SUB_EVENT ----------------------- */
static void test_write_triggers_event(void) {
    printf("\n[test 2] Write triggers SUB_EVENT\n");

    /* Subscriber connection. */
    int sub_fd = client_connect(g_port);
    CHECK(sub_fd >= 0, "sub connect");
    if (sub_fd < 0) return;
    hello(sub_fd, 1);

    uint8_t sbuf[128];
    size_t ssz = build_subscribe(sbuf, "sensor", NULL);
    tsdb_proto_send(sub_fd, TSDB_MT_SUBSCRIBE, 0, 21, sbuf, ssz);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    tsdb_proto_recv(sub_fd, &hdr, &pl); free(pl); pl = NULL;

    /* Give subscription time to register. */
    usleep(5000);

    /* Write 5 rows. */
    do_write(5, 2000000000000LL, 42);

    /* Expect SUB_EVENT within 1 second. */
    set_recv_timeout(sub_fd, 1000);
    int rc = tsdb_proto_recv(sub_fd, &hdr, &pl);
    CHECK(rc == TSDB_OK, "recv SUB_EVENT");
    CHECK(hdr.type == TSDB_MT_SUB_EVENT, "type == SUB_EVENT");
    CHECK((hdr.flags & TSDB_FLAG_STREAM) != 0, "STREAM flag set");

    /* Parse SUB_EVENT payload — the WRITE_BATCH columnar shape (tsdb_wire.h):
     *   [table_len u8][table][ncols u16][nrows u32]
     *   per col: [name_len u8][name][type u8][codec u8][csz u32][data] */
    if (rc == TSDB_OK && pl && hdr.payload_len >= 1) {
        const uint8_t *pp = pl;
        const uint8_t *pe = pl + hdr.payload_len;
        uint8_t tlen = *pp++;
        char ev_table[64] = {0};
        if (tlen < 64) memcpy(ev_table, pp, tlen);
        pp += tlen;
        uint16_t ev_ncols = 0;
        memcpy(&ev_ncols, pp, 2); pp += 2;
        uint32_t ev_nrows = 0;
        memcpy(&ev_nrows, pp, 4); pp += 4;

        CHECK(strcmp(ev_table, "sensor") == 0, "SUB_EVENT table == sensor");
        CHECK(ev_nrows == 5, "SUB_EVENT nrows == 5");
        CHECK(ev_ncols == 3, "SUB_EVENT ncols == 3");

        /* Decode all three columns; row values must match what conn B wrote
         * (build_write_sensor: ts_start + i*1000, val = i, device = 42). */
        int ts_ok = 1, val_ok = 1, dev_ok = 1, shape_ok = 1;
        for (int c = 0; c < ev_ncols; c++) {
            if (pp + 1 > pe) { shape_ok = 0; break; }
            uint8_t nl = *pp++;
            char cname[64] = {0};
            if (pp + nl + 2 + 4 > pe) { shape_ok = 0; break; }
            memcpy(cname, pp, nl < 63 ? nl : 63); pp += nl;
            pp++;                                  /* type */
            pp++;                                  /* codec */
            uint32_t csz = 0;
            memcpy(&csz, pp, 4); pp += 4;
            if (pp + csz > pe || csz < (size_t)ev_nrows * 8) { shape_ok = 0; break; }
            const uint8_t *data = pp;
            pp += csz;
            for (uint32_t r = 0; r < ev_nrows; r++) {
                int64_t v;
                memcpy(&v, data + (size_t)r * 8, 8);
                if (strcmp(cname, "ts") == 0 &&
                    v != 2000000000000LL + (int64_t)r * 1000) ts_ok = 0;
                if (strcmp(cname, "val") == 0 && v != (int64_t)r) val_ok = 0;
                if (strcmp(cname, "device") == 0 && v != 42)      dev_ok = 0;
            }
        }
        CHECK(shape_ok, "SUB_EVENT per-column headers well-formed");
        CHECK(pp == pe, "SUB_EVENT payload fully consumed");
        CHECK(ts_ok,  "SUB_EVENT ts column matches written batch");
        CHECK(val_ok, "SUB_EVENT val column matches written batch");
        CHECK(dev_ok, "SUB_EVENT device column matches written batch");
    }
    free(pl);

    close(sub_fd);
}

/* ---- Test 3: Unsubscribe stops events ------------------------------------ */
static void test_unsubscribe(void) {
    printf("\n[test 3] Unsubscribe stops events\n");

    int sub_fd = client_connect(g_port);
    CHECK(sub_fd >= 0, "sub connect");
    if (sub_fd < 0) return;
    hello(sub_fd, 1);

    uint8_t sbuf[128];
    size_t ssz = build_subscribe(sbuf, "sensor", NULL);
    tsdb_proto_send(sub_fd, TSDB_MT_SUBSCRIBE, 0, 30, sbuf, ssz);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    tsdb_proto_recv(sub_fd, &hdr, &pl);
    uint64_t sub_id = 0;
    if (pl && hdr.payload_len >= 8) memcpy(&sub_id, pl, 8);
    free(pl); pl = NULL;
    CHECK(sub_id > 0, "got sub_id");

    usleep(5000);

    /* Unsubscribe.  Payload is the req_id of the original SUBSCRIBE (30),
     * per the wire spec — NOT the server-internal sub_id from the ack. */
    uint8_t uid_buf[8];
    uint64_t sub_req = 30;
    for (int i = 0; i < 8; i++) uid_buf[i] = (uint8_t)(sub_req >> (i * 8));
    tsdb_proto_send(sub_fd, TSDB_MT_UNSUBSCRIBE, 0, 31, uid_buf, 8);
    tsdb_proto_recv(sub_fd, &hdr, &pl); free(pl); pl = NULL;

    usleep(5000);

    /* Write rows — should NOT arrive on sub_fd. */
    do_write(3, 3000000000000LL, 77);

    set_recv_timeout(sub_fd, 300); /* 300ms timeout */
    int rc = tsdb_proto_recv(sub_fd, &hdr, &pl);
    CHECK(rc != TSDB_OK, "no SUB_EVENT after unsubscribe");
    free(pl);

    close(sub_fd);
}

/* ---- Test 4: Column filter ------------------------------------------------ */
static void test_filter(void) {
    printf("\n[test 4] Column filter\n");

    /* Subscribe with filter: device=42 */
    int sub_fd = client_connect(g_port);
    CHECK(sub_fd >= 0, "sub connect");
    if (sub_fd < 0) return;
    hello(sub_fd, 1);

    uint8_t sbuf[256];
    size_t ssz = build_subscribe(sbuf, "sensor", "device=42");
    tsdb_proto_send(sub_fd, TSDB_MT_SUBSCRIBE, 0, 40, sbuf, ssz);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    tsdb_proto_recv(sub_fd, &hdr, &pl); free(pl); pl = NULL;
    usleep(5000);

    /* Write rows with device=99 — should NOT trigger event. */
    do_write(4, 4000000000000LL, 99);
    set_recv_timeout(sub_fd, 300);
    int rc = tsdb_proto_recv(sub_fd, &hdr, &pl);
    CHECK(rc != TSDB_OK, "no SUB_EVENT for device=99 when filter=device=42");
    free(pl); pl = NULL;

    /* Write rows with device=42 — SHOULD trigger event. */
    do_write(6, 4001000000000LL, 42);
    set_recv_timeout(sub_fd, 1000);
    rc = tsdb_proto_recv(sub_fd, &hdr, &pl);
    CHECK(rc == TSDB_OK && hdr.type == TSDB_MT_SUB_EVENT,
          "SUB_EVENT received for device=42 match");
    free(pl);

    close(sub_fd);
}

/* ---- Test 5: Slow subscriber does not block write ------------------------ */
typedef struct {
    int  port;
    /* Atomic: the worker signals readiness here while main polls it, so a
     * plain int is a data race — ThreadSanitizer flags it inside a suite whose
     * purpose is reading TSan reports about the engine.  relaxed suffices; the
     * flag carries no payload and the pthread_join orders the worker's work. */
    _Atomic int  done;
    int  received_event;
} slow_sub_ctx_t;

static void *slow_subscriber_thread(void *arg) {
    slow_sub_ctx_t *ctx = (slow_sub_ctx_t *)arg;

    int fd = client_connect(ctx->port);
    if (fd < 0) { atomic_store_explicit(&ctx->done, 1, memory_order_relaxed); return NULL; }
    hello(fd, 1);

    uint8_t sbuf[128];
    size_t ssz = build_subscribe(sbuf, "sensor", NULL);
    tsdb_proto_send(fd, TSDB_MT_SUBSCRIBE, 0, 50, sbuf, ssz);
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    tsdb_proto_recv(fd, &hdr, &pl); free(pl); pl = NULL;

    /* Signal ready, then do NOT read — simulate slow subscriber. */
    atomic_store_explicit(&ctx->done, 1, memory_order_relaxed);
    /* Sleep 2 seconds (much longer than the write should take). */
    usleep(2000000);

    /* Now read (if anything arrived). */
    set_recv_timeout(fd, 500);
    pl = NULL;
    if (tsdb_proto_recv(fd, &hdr, &pl) == TSDB_OK &&
        hdr.type == TSDB_MT_SUB_EVENT) {
        ctx->received_event = 1;
    }
    free(pl);
    close(fd);
    return NULL;
}

static void test_slow_subscriber(void) {
    printf("\n[test 5] Slow subscriber does not block write\n");

    slow_sub_ctx_t ctx = {0};
    ctx.port = g_port;

    pthread_t tid;
    pthread_create(&tid, NULL, slow_subscriber_thread, &ctx);

    /* Wait for subscriber to register. */
    int waited = 0;
    while (!atomic_load_explicit(&ctx.done, memory_order_relaxed) && waited < 2000) { usleep(10000); waited += 10; }
    usleep(10000);

    /* Write must complete quickly even though subscriber is slow. */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    do_write(10, 5000000000000LL, 1);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    long ms = (long)((t1.tv_sec - t0.tv_sec) * 1000L
                   + (t1.tv_nsec - t0.tv_nsec) / 1000000L);
    /* Write should complete in under 500ms regardless of slow subscriber. */
    CHECK(ms < 500, "write completes < 500ms with slow subscriber");

    pthread_join(tid, NULL);
}

/* ---- Test 6: Multiple subscribers --------------------------------------- */
static void test_multi_subscriber(void) {
    printf("\n[test 6] Multiple subscribers\n");

    int fds[3];
    for (int i = 0; i < 3; i++) {
        fds[i] = client_connect(g_port);
        CHECK(fds[i] >= 0, "multi sub connect");
        if (fds[i] < 0) return;
        hello(fds[i], (uint64_t)(100 + i));
        uint8_t sbuf[128];
        size_t ssz = build_subscribe(sbuf, "sensor", NULL);
        tsdb_proto_send(fds[i], TSDB_MT_SUBSCRIBE, 0, (uint64_t)(200 + i), sbuf, ssz);
        tsdb_frame_hdr_t hdr; uint8_t *pl=NULL;
        tsdb_proto_recv(fds[i], &hdr, &pl); free(pl);
    }
    usleep(10000);

    do_write(2, 6000000000000LL, 55);

    int ok_count = 0;
    for (int i = 0; i < 3; i++) {
        tsdb_frame_hdr_t hdr; uint8_t *pl=NULL;
        set_recv_timeout(fds[i], 1000);
        int rc = tsdb_proto_recv(fds[i], &hdr, &pl);
        if (rc == TSDB_OK && hdr.type == TSDB_MT_SUB_EVENT) ok_count++;
        free(pl);
        close(fds[i]);
    }
    CHECK(ok_count == 3, "all 3 subscribers received SUB_EVENT");
}

/* ---- main ---------------------------------------------------------------- */

int main(void) {
    printf("=== test_pubsub ===\n");

    if (setup() != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    ensure_sensor_table();

    test_subscribe_ack();
    test_write_triggers_event();
    test_unsubscribe();
    test_filter();
    test_slow_subscriber();
    test_multi_subscriber();

    teardown();

    printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
    return (g_fail > 0) ? 1 : 0;
}
