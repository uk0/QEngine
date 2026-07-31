/* test_rpc_ack_honesty.c — an ACK must mean the peer did the thing.
 *
 * Three responders on this transport answered TSDB_RPC_ACK for work that had
 * not happened.  All three are single-process reproducible: a real
 * tsdb_rpc_server_new bound to 127.0.0.1:0 over a real tsdb_db_t, driven by a
 * real tsdb_rpc_connect client.  No cluster, no forks, no sleeps.
 *
 * [1] UNKNOWN OPCODE.  The dispatch `default:` ACKed with an empty body, so a
 *     peer on a newer build that sends an opcode this binary has never heard
 *     of is told it succeeded.  rpc.c parses the frame's `ver` and immediately
 *     (void)s it — this arm is the entire forward-compatibility story on this
 *     transport, and "yes" is the wrong default.  It must be an error, and it
 *     must be a DISTINGUISHABLE error: TSDB_RPC_LOCAL_TABLE_STATS's old-peer
 *     fallback (db_cluster.c) has to keep telling "too old for this opcode"
 *     apart from "knows it, could not answer", or the anti-entropy probe stops
 *     degrading to the legacy query and starts skipping the peer.
 *
 * [2] SCHEMA_SYNC.  The handler discarded tsdb_rpc_decode_schema's rc, the
 *     `ncols > 0 && ncols <= TSDB_MAX_COLS` range check, and
 *     tsdb_create_table_local_ex's return, then ACKed anything with
 *     payload_len > 0.  A follower that failed to create the table reported
 *     it as synced, and the leader's fanout counted the peer as carrying the
 *     schema; the next WRITE_BATCH for that table is what discovers otherwise.
 *     The idempotent repeat (TSDB_ERR_EXISTS) must STILL ack — re-broadcast
 *     and startup catch-up both replay schemas the peer already has.
 *
 * [3] DDL_FORWARD.  The handler put the failure TEXT in an ACK body and let
 *     the caller string-match it.  tsdb_query returning TSDB_ERR_PARSE came
 *     back as a successful RPC whose payload happened to read "parse error".
 *     The failure has to be on the transport, with the text still in the body
 *     so the operator sees the leader's own message.
 *
 * Every check below is written against the OBSERVABLE rc of a real call, so
 * reverting any one of the three fixes turns exactly its own block red.
 */
#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/cluster/rpc.h"
#include "../src/storage/db.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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

#define TDIR "/tmp/tsdb_test_rpc_ack_honesty"

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

static tsdb_rpc_conn_t *dial(int port) {
    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    return tsdb_rpc_connect(addr, 2000);
}

/* ---- SCHEMA_SYNC payload builder ----------------------------------------
 * Hand-rolled rather than via tsdb_rpc_encode_schema so the test can emit the
 * shapes a well-behaved encoder never would (ncols = 0, a ts column that is
 * not a TIMESTAMP) — those are precisely the inputs whose rejection the
 * handler used to throw away. */
static int build_schema_payload(uint8_t *buf, size_t cap,
                                const char *table,
                                int ncols, const char *const *names,
                                const int *types, int ts_idx)
{
    uint8_t *p = buf, *end = buf + cap;
    size_t tl = strlen(table);
    if (p + 1 + tl + 2 > end) return -1;
    *p++ = (uint8_t)tl;
    memcpy(p, table, tl); p += tl;
    *p++ = (uint8_t)ncols;
    *p++ = (uint8_t)ts_idx;
    for (int i = 0; i < ncols; i++) {
        size_t nl = strlen(names[i]);
        if (p + 1 + nl + 1 > end) return -1;
        *p++ = (uint8_t)nl;
        memcpy(p, names[i], nl); p += nl;
        *p++ = (uint8_t)types[i];
    }
    return (int)(p - buf);
}

int main(void) {
    printf("=== test_rpc_ack_honesty ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));

    tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", db, NULL);
    if (!srv) { fprintf(stderr, "FATAL: rpc server\n"); return 1; }
    int port = tsdb_rpc_server_port(srv);
    tsdb_rpc_conn_t *conn = dial(port);
    if (!conn) { fprintf(stderr, "FATAL: dial\n"); return 1; }

    /* ── [1] an opcode this build has never heard of ─────────────────────── */
    printf("\n[1] unknown opcode is refused, not ACKed\n");
    {
        uint8_t body[8]; memset(body, 0x5A, sizeof(body));
        /* 200 is past every value in tsdb_rpc_type_t and always will be:
         * the enum is a u8 on the wire and grows from 1 upward. */
        int rc = tsdb_rpc_call(conn, (tsdb_rpc_type_t)200, body, sizeof(body));
        printf("  opcode 200 -> rc=%d (%s)\n", rc, tsdb_errstr(rc));
        CHECK(rc != TSDB_OK,
              "an opcode the server does not implement is NOT reported as done");
        CHECK(rc == TSDB_ERR_UNSUPPORTED,
              "and it is UNSUPPORTED, distinguishable from a handler that ran "
              "and failed (TSDB_ERR_INTERNAL)");

        /* The refusal is a normal reply, not a broken stream: the very next
         * call on the SAME pooled connection must still work.  (Answering an
         * unknown opcode by hanging up would have healed [1] and broken every
         * multiplexed caller behind it.) */
        uint8_t hb[16]; memset(hb, 0, sizeof(hb));
        CHECK(tsdb_rpc_call(conn, TSDB_RPC_HEARTBEAT, hb, sizeof(hb)) == TSDB_OK,
              "the connection survives the refusal — next call still ACKs");
    }

    /* ── [2] SCHEMA_SYNC ─────────────────────────────────────────────────── */
    printf("\n[2] SCHEMA_SYNC acks only a schema that exists afterwards\n");
    {
        uint8_t pl[512];
        uint32_t rlen = 0;

        /* (a) undecodable: name_len says 255, payload is 1 byte. */
        uint8_t junk[1] = { 0xFF };
        int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_SCHEMA_SYNC,
                                    junk, sizeof(junk), NULL, 0, &rlen);
        printf("  undecodable payload -> rc=%d\n", rc);
        CHECK(rc != TSDB_OK, "a payload that does not decode is refused");

        /* (b) decodes, but ncols == 0 — the range check the handler had and
         * then ignored. */
        const char *no_names[1] = { "x" };
        const int   no_types[1] = { TSDB_TYPE_INT64 };
        int n = build_schema_payload(pl, sizeof(pl), "zerocols",
                                     0, no_names, no_types, 0);
        if (n <= 0) { fprintf(stderr, "FATAL: build\n"); return 1; }
        rc = tsdb_rpc_call_recv(conn, TSDB_RPC_SCHEMA_SYNC,
                                pl, (uint32_t)n, NULL, 0, &rlen);
        printf("  ncols=0 -> rc=%d\n", rc);
        CHECK(rc != TSDB_OK, "ncols=0 is refused (the discarded range check)");
        CHECK(tsdb_db_find_table(db, "zerocols") == NULL,
              "and no table called 'zerocols' exists");

        /* (c) decodes and passes the range check, but the create FAILS: the
         * declared ts column is an INT64, so tsdb_schema_create_ex returns
         * TSDB_ERR_SCHEMA.  This is the return value the handler threw away. */
        const char *bad_names[2] = { "ts", "v" };
        const int   bad_types[2] = { TSDB_TYPE_INT64, TSDB_TYPE_INT64 };
        n = build_schema_payload(pl, sizeof(pl), "badts",
                                 2, bad_names, bad_types, 0);
        if (n <= 0) { fprintf(stderr, "FATAL: build\n"); return 1; }
        rc = tsdb_rpc_call_recv(conn, TSDB_RPC_SCHEMA_SYNC,
                                pl, (uint32_t)n, NULL, 0, &rlen);
        printf("  ts column typed INT64 -> rc=%d\n", rc);
        CHECK(rc != TSDB_OK,
              "a schema the peer could not create is refused "
              "(tsdb_create_table_local_ex's rc)");
        CHECK(tsdb_db_find_table(db, "badts") == NULL,
              "and no table called 'badts' exists");

        /* (d) a real schema still ACKs and really lands. */
        const char *good_names[2] = { "ts", "v" };
        const int   good_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
        n = build_schema_payload(pl, sizeof(pl), "goodtbl",
                                 2, good_names, good_types, 0);
        if (n <= 0) { fprintf(stderr, "FATAL: build\n"); return 1; }
        rc = tsdb_rpc_call_recv(conn, TSDB_RPC_SCHEMA_SYNC,
                                pl, (uint32_t)n, NULL, 0, &rlen);
        printf("  valid schema -> rc=%d\n", rc);
        CHECK(rc == TSDB_OK, "a valid schema ACKs");
        CHECK(tsdb_db_find_table(db, "goodtbl") != NULL,
              "and the table exists on the receiver");

        /* (e) THE IDEMPOTENT REPEAT.  Re-broadcast and startup catch-up both
         * replay a schema the peer already has; TSDB_ERR_EXISTS is success. */
        rc = tsdb_rpc_call_recv(conn, TSDB_RPC_SCHEMA_SYNC,
                                pl, (uint32_t)n, NULL, 0, &rlen);
        printf("  same schema again -> rc=%d\n", rc);
        CHECK(rc == TSDB_OK,
              "replaying a schema the peer already has still ACKs "
              "(EXISTS is not a failure)");
    }

    /* ── [3] DDL_FORWARD ─────────────────────────────────────────────────── */
    printf("\n[3] DDL_FORWARD reports failure on the transport\n");
    {
        uint8_t pl[4608];
        uint8_t resp[512];
        uint32_t rlen = 0;

        /* A statement the query engine rejects outright. */
        int n = tsdb_rpc_encode_catalog_qtl(pl, sizeof(pl),
                                            "CREATE TABLE");   /* truncated */
        if (n <= 0) { fprintf(stderr, "FATAL: encode qtl\n"); return 1; }
        int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_DDL_FORWARD,
                                    pl, (uint32_t)n,
                                    resp, sizeof(resp) - 1, &rlen);
        resp[rlen < sizeof(resp) ? rlen : sizeof(resp) - 1] = '\0';
        printf("  bad DDL -> rc=%d body=\"%s\"\n", rc, (const char *)resp);
        CHECK(rc != TSDB_OK,
              "a statement the leader could not run is a FAILED call, not an "
              "ACK carrying the error text");
        CHECK(rlen > 0,
              "the leader's own message still rides in the body so the "
              "operator sees it");

        /* A statement that works still ACKs, body = its status row. */
        n = tsdb_rpc_encode_catalog_qtl(
                pl, sizeof(pl),
                "CREATE TABLE fwd_ok (ts TIMESTAMP, v INT64) TIMESTAMP(ts)");
        if (n <= 0) { fprintf(stderr, "FATAL: encode qtl\n"); return 1; }
        rlen = 0;
        rc = tsdb_rpc_call_recv(conn, TSDB_RPC_DDL_FORWARD,
                                pl, (uint32_t)n,
                                resp, sizeof(resp) - 1, &rlen);
        resp[rlen < sizeof(resp) ? rlen : sizeof(resp) - 1] = '\0';
        printf("  good DDL -> rc=%d body=\"%s\"\n", rc, (const char *)resp);
        CHECK(rc == TSDB_OK, "a statement that ran ACKs");
        CHECK(tsdb_db_find_table(db, "fwd_ok") != NULL,
              "and the table it asked for exists");
    }

    tsdb_rpc_conn_close(conn);
    tsdb_rpc_server_stop(srv);
    tsdb_close(db);
    rm_rf(TDIR);

    printf("\n=== test_rpc_ack_honesty %s ===\n", g_fail ? "FAILED" : "PASSED");
    return g_fail ? 1 : 0;
}
