/* test_table_incarnation.c — a durable per-table incarnation so a DROP+recreate
 * of the same name cannot silently reuse the old table's identity on the
 * replication path.
 *
 * The bug: table identity on the WRITE_BATCH path was the NAME alone.  A batch
 * for the OLD `t` that was in flight, retried, or sitting in a sender queue when
 * `DROP TABLE t; CREATE TABLE t (...)` happened would apply to the NEW `t` —
 * splicing a dead table's rows (maybe not even the same schema) into the live
 * one, with no error.
 *
 * The fix stamps a durable, non-zero incarnation at every originating CREATE
 * (different on every recreate), persists it in schema.bin, carries it on the
 * WRITE_BATCH wire as an additive trailer, and REJECTS a batch whose incarnation
 * does not match the receiver's current one.  Zero on either side = UNKNOWN =
 * "cannot verify" = apply-as-before (the mixed-version compatibility path).
 *
 * Everything here is in-process and deterministic — no cluster, no crash.  The
 * receiver seam is tsdb_rpc_apply_write_batch_for_test (the exact decode → open
 * → incarnation-gate → begin → append → commit chain a WRITE_BATCH RPC drives).
 *
 *   [lifecycle]  a fresh CREATE has a non-zero incarnation; a plain reopen
 *                (close+open) KEEPS it (a restart must not look like a recreate,
 *                or replication would reject in-flight writes); a DROP+recreate
 *                of the same name CHANGES it.
 *   [reject]     the core reproduce: capture a batch for incarnation A, recreate
 *                the table (incarnation B), and the captured batch is REJECTED —
 *                the new table stays empty.
 *   [match]      a batch whose incarnation matches applies (the common case).
 *   [legacy]     an incarnation-less batch (a pre-incarnation sender) applies —
 *                the compatibility path a rolling upgrade needs.
 *   [wire]       encode_ex(inc=0) is byte-for-byte the legacy encoder, and a
 *                legacy decoder reads a V5 payload's columns and ignores the
 *                trailer (both mixed-version directions).
 *   [schema]     schema.bin incarnation round-trips, and a legacy schema (no
 *                trailer) loads back as 0 = UNKNOWN.
 */

#include "tsdb.h"
#include "../src/storage/db.h"          /* tsdb_table_get_schema, tsdb_schema_t */
#include "../src/storage/schema.h"
#include "../src/cluster/rpc.h"         /* encode/_ex, decode, the apply seam */

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

#define DAY1 (1735689600LL * 1000000000LL)   /* 2025-01-01 UTC */

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        chmod(q, 0700);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static void make_table(tsdb_db_t *db) {
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) OK(rc);
}

static int count_all(tsdb_db_t *db) {
    tsdb_result_t *r = NULL;
    int n = 0;
    if (tsdb_query(db, "SELECT v FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

static uint64_t table_incarnation(tsdb_db_t *db, const char *name) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, name, &t));
    tsdb_schema_t *s = tsdb_table_get_schema(t);
    ASSERT(s != NULL);
    return s->incarnation;
}

/* Encode a 5-row (ts, v) WRITE_BATCH for "t" carrying `inc` (0 → legacy, no
 * trailer).  Returns the encoded length; bytes live in `buf`. */
static int encode_batch(uint8_t *buf, size_t cap, uint64_t inc, int nrows) {
    int64_t ts[16], v[16];
    ASSERT(nrows <= 16);
    for (int i = 0; i < nrows; i++) { ts[i] = DAY1 + i * 1000000LL; v[i] = 100 + i; }
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
    const void *col_data[2] = { ts, v };
    return tsdb_rpc_encode_write_batch_ex(buf, (uint32_t)cap, "t",
                                          2, col_types, nrows, col_data, inc);
}

/* ---- [lifecycle] fresh != 0, reopen keeps, recreate changes -------------- */
static void test_lifecycle(void) {
    const char *dir = "/tmp/tsdb_test_incarnation_lifecycle";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    make_table(db);
    uint64_t A = table_incarnation(db, "t");
    ASSERT(A != 0);                                  /* a fresh CREATE stamps one */
    tsdb_close(db);

    /* Plain reopen (a restart) MUST keep the incarnation — otherwise every
     * restart would look like a recreate and reject all in-flight replication. */
    db = NULL;
    OK(tsdb_open(dir, &db));
    uint64_t A2 = table_incarnation(db, "t");
    ASSERT(A2 == A);                                 /* reopen keeps it */

    /* DROP + recreate of the SAME name MUST change it. */
    OK(tsdb_drop_table(db, "t"));
    make_table(db);
    uint64_t B = table_incarnation(db, "t");
    ASSERT(B != 0);
    ASSERT(B != A);                                  /* recreate changes it */

    /* And the recreated value is itself durable across a reopen. */
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    ASSERT(table_incarnation(db, "t") == B);
    tsdb_close(db);

    rm_rf(dir);
    printf("  [lifecycle] fresh!=0, reopen keeps, recreate changes, durable: OK\n");
}

/* ---- [reject] a batch for a since-dropped incarnation must NOT apply ------ */
static void test_reject_stale_incarnation(void) {
    const char *dir = "/tmp/tsdb_test_incarnation_reject";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    make_table(db);
    uint64_t A = table_incarnation(db, "t");
    ASSERT(A != 0);

    /* Capture a WRITE_BATCH stamped for incarnation A (as a sender would). */
    uint8_t payload[4096];
    int plen = encode_batch(payload, sizeof(payload), A, 5);
    ASSERT(plen > 0);

    /* DROP + recreate: the name now maps to a DIFFERENT incarnation B. */
    OK(tsdb_drop_table(db, "t"));
    make_table(db);
    uint64_t B = table_incarnation(db, "t");
    ASSERT(B != A);
    ASSERT(count_all(db) == 0);                      /* new t starts empty */

    /* Delivering the stale batch must be REJECTED (0 = ERR to the sender) and
     * must leave the new table untouched — no cross-incarnation contamination.
     * Pre-fix this applied and count_all became 5. */
    int applied = tsdb_rpc_apply_write_batch_for_test(db, payload, (uint32_t)plen);
    ASSERT(applied == 0);                            /* rejected */
    ASSERT(count_all(db) == 0);                      /* NOT contaminated */

    tsdb_close(db);
    rm_rf(dir);
    printf("  [reject] stale-incarnation WRITE_BATCH rejected, table clean: OK\n");
}

/* ---- [broadcast] replaying a peer's catalog DDL must NOT mint -------------
 *
 * The catalog fanout ships a CREATE as QTL TEXT and every peer RE-EXECUTES it.
 * That replay arrives on the ordinary (hook-unsuppressed) create path, so it
 * used to mint a FRESH incarnation on each node — the same table then carried a
 * DIFFERENT non-zero incarnation on every node, and the WRITE_BATCH gate (two
 * known values that differ → reject) refused replication in BOTH directions.
 * Nodes silently stopped replicating to each other and each kept only its own
 * writes.  Measured live before this fix: one table held four distinct
 * incarnations across four nodes and replicate_incarnation_reject_total climbed
 * while the row counts split complementarily (each node = exactly its own
 * writes).  A replay must stamp UNKNOWN(0) — "cannot verify, never reject" —
 * exactly like every other non-SCHEMA_SYNC replica create. */
extern __thread int tsdb_g_suppress_catalog_broadcast;

static void test_broadcast_replay_does_not_mint(void) {
    /* The ORIGIN of the CREATE mints, as before. */
    const char *ldir = "/tmp/tsdb_test_incarnation_bcast_leader";
    rm_rf(ldir);
    tsdb_db_t *leader = NULL;
    OK(tsdb_open(ldir, &leader));
    make_table(leader);
    uint64_t L = table_incarnation(leader, "t");
    ASSERT(L != 0);                         /* origin still stamps an identity */

    /* A PEER replaying the broadcast DDL must stamp UNKNOWN(0), not mint. */
    const char *fdir = "/tmp/tsdb_test_incarnation_bcast_follower";
    rm_rf(fdir);
    tsdb_db_t *follower = NULL;
    OK(tsdb_open(fdir, &follower));
    tsdb_g_suppress_catalog_broadcast = 1;   /* what the DDL-apply handler sets */
    make_table(follower);
    tsdb_g_suppress_catalog_broadcast = 0;
    uint64_t F = table_incarnation(follower, "t");
    ASSERT(F == 0);                          /* THE FIX: replay does not mint */

    /* Therefore replication flows BOTH ways instead of being mutually refused.
     * leader → follower: batch carries L, follower is 0 → cannot verify → apply. */
    uint8_t payload[4096];
    int plen = encode_batch(payload, sizeof(payload), L, 5);
    ASSERT(plen > 0);
    ASSERT(tsdb_rpc_apply_write_batch_for_test(follower, payload, (uint32_t)plen) == 1);
    ASSERT(count_all(follower) == 5);

    /* follower → leader: batch carries 0 (its schema is UNKNOWN) → apply. */
    plen = encode_batch(payload, sizeof(payload), F, 5);
    ASSERT(plen > 0);
    ASSERT(tsdb_rpc_apply_write_batch_for_test(leader, payload, (uint32_t)plen) == 1);
    ASSERT(count_all(leader) == 5);

    tsdb_close(leader);
    tsdb_close(follower);

    /* The bug shape, pinned: two INDEPENDENT origin creates of the same name DO
     * mint different values, which is exactly why a replay must not be treated
     * as an origination.  (This is the state the live cluster was found in.) */
    const char *adir = "/tmp/tsdb_test_incarnation_bcast_a";
    const char *bdir = "/tmp/tsdb_test_incarnation_bcast_b";
    rm_rf(adir); rm_rf(bdir);
    tsdb_db_t *a = NULL, *b = NULL;
    OK(tsdb_open(adir, &a));
    OK(tsdb_open(bdir, &b));
    make_table(a);
    make_table(b);
    uint64_t IA = table_incarnation(a, "t"), IB = table_incarnation(b, "t");
    ASSERT(IA != 0 && IB != 0 && IA != IB);  /* independent mints DIVERGE */

    /* And that divergence is precisely what the gate refuses — so had the replay
     * minted, this is the rejection every peer would have hit. */
    plen = encode_batch(payload, sizeof(payload), IA, 5);
    ASSERT(plen > 0);
    ASSERT(tsdb_rpc_apply_write_batch_for_test(b, payload, (uint32_t)plen) == 0);
    ASSERT(count_all(b) == 0);

    tsdb_close(a); tsdb_close(b);
    rm_rf(ldir); rm_rf(fdir); rm_rf(adir); rm_rf(bdir);
    printf("  [broadcast] DDL replay stamps UNKNOWN(0); replication flows both ways: OK\n");
}

/* ---- [match] a same-incarnation batch applies (the common case) ---------- */
static void test_same_incarnation_applies(void) {
    const char *dir = "/tmp/tsdb_test_incarnation_match";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    make_table(db);
    uint64_t A = table_incarnation(db, "t");

    uint8_t payload[4096];
    int plen = encode_batch(payload, sizeof(payload), A, 5);
    ASSERT(plen > 0);

    int applied = tsdb_rpc_apply_write_batch_for_test(db, payload, (uint32_t)plen);
    ASSERT(applied == 1);                            /* matches → applies */
    ASSERT(count_all(db) == 5);

    tsdb_close(db);
    rm_rf(dir);
    printf("  [match] same-incarnation WRITE_BATCH applies: OK\n");
}

/* ---- [legacy] an incarnation-less batch applies (rolling-upgrade path) ---- */
static void test_legacy_sender_applies(void) {
    const char *dir = "/tmp/tsdb_test_incarnation_legacy";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    make_table(db);
    ASSERT(table_incarnation(db, "t") != 0);         /* receiver DOES know one */

    /* An old sender emits no trailer.  The receiver cannot verify → must apply
     * exactly as before, or a V5 node would refuse every V4 peer's writes. */
    uint8_t payload[4096];
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
    int64_t ts[5], v[5];
    for (int i = 0; i < 5; i++) { ts[i] = DAY1 + i * 1000000LL; v[i] = 100 + i; }
    const void *col_data[2] = { ts, v };
    int plen = tsdb_rpc_encode_write_batch(payload, sizeof(payload), "t",
                                           2, col_types, 5, col_data);
    ASSERT(plen > 0);

    int applied = tsdb_rpc_apply_write_batch_for_test(db, payload, (uint32_t)plen);
    ASSERT(applied == 1);                            /* cannot verify → applies */
    ASSERT(count_all(db) == 5);

    tsdb_close(db);
    rm_rf(dir);
    printf("  [legacy] incarnation-less WRITE_BATCH applies (compat path): OK\n");
}

/* ---- [wire] additive-trailer compatibility, both directions -------------- */
static void test_wire_compat(void) {
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
    int64_t ts[5], v[5];
    for (int i = 0; i < 5; i++) { ts[i] = DAY1 + i * 1000000LL; v[i] = 100 + i; }
    const void *col_data[2] = { ts, v };

    uint8_t p_plain[4096], p_zero[4096], p_inc[4096];
    int n_plain = tsdb_rpc_encode_write_batch(p_plain, sizeof(p_plain), "t",
                                              2, col_types, 5, col_data);
    int n_zero  = tsdb_rpc_encode_write_batch_ex(p_zero, sizeof(p_zero), "t",
                                                 2, col_types, 5, col_data, 0);
    int n_inc   = tsdb_rpc_encode_write_batch_ex(p_inc, sizeof(p_inc), "t",
                                                 2, col_types, 5, col_data,
                                                 0x0123456789ABCDEFULL);
    ASSERT(n_plain > 0 && n_zero > 0 && n_inc > 0);

    /* A V5 node with NO incarnation emits byte-for-byte the legacy encoder. */
    ASSERT(n_zero == n_plain);
    ASSERT(memcmp(p_zero, p_plain, (size_t)n_plain) == 0);

    /* A V5 node WITH an incarnation appends exactly an 8-byte trailer. */
    ASSERT(n_inc == n_plain + 8);
    ASSERT(memcmp(p_inc, p_plain, (size_t)n_plain) == 0);   /* body unchanged */

    /* A V4 receiver (the legacy decoder) reads the V5 payload's columns and
     * ignores the trailer — header fields identical to the plain payload. */
    char tbl[64]; int dn = 0, dr = 0, dt[TSDB_MAX_COLS]; uint8_t *dd = NULL;
    ASSERT(tsdb_rpc_decode_write_batch(p_inc, (uint32_t)n_inc, tbl, sizeof(tbl),
                                       &dn, dt, &dr, &dd) == 0);
    ASSERT(strcmp(tbl, "t") == 0);
    ASSERT(dn == 2 && dr == 5);
    ASSERT(dt[0] == TSDB_TYPE_TIMESTAMP && dt[1] == TSDB_TYPE_INT64);

    printf("  [wire] encode_ex(0)==legacy, trailer additive, V4 decodes V5: OK\n");
}

/* ---- [schema] schema.bin incarnation round-trip + legacy loads as 0 ------- */
static void test_schema_roundtrip(void) {
    const char *dir = "/tmp/tsdb_test_incarnation_schema";
    rm_rf(dir);
    if (mkdir(dir, 0755) != 0) FAIL("mkdir");

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };

    /* Explicit incarnation persists and reloads. */
    char d1[4096]; snprintf(d1, sizeof(d1), "%s/withinc", dir);
    tsdb_schema_t *s = NULL;
    OK(tsdb_schema_create_ex2(d1, "t", cols, 2, "ts",
                              TSDB_PARTITION_DAY, 0,
                              0xFEEDBEEFCAFEF00DULL, &s));
    ASSERT(s->incarnation == 0xFEEDBEEFCAFEF00DULL);
    tsdb_schema_free(s);
    s = NULL;
    OK(tsdb_schema_open(d1, &s));
    ASSERT(s->incarnation == 0xFEEDBEEFCAFEF00DULL);   /* durable round-trip */
    tsdb_schema_free(s);

    /* The legacy create path (no incarnation) writes NO trailer, so it loads
     * back as 0 = UNKNOWN — exactly how a pre-incarnation schema.bin reads. */
    char d2[4096]; snprintf(d2, sizeof(d2), "%s/legacy", dir);
    s = NULL;
    OK(tsdb_schema_create_ex(d2, "t", cols, 2, "ts",
                             TSDB_PARTITION_DAY, 0, &s));
    ASSERT(s->incarnation == 0);
    tsdb_schema_free(s);
    s = NULL;
    OK(tsdb_schema_open(d2, &s));
    ASSERT(s->incarnation == 0);                       /* UNKNOWN, never rejects */
    tsdb_schema_free(s);

    /* A freshly minted incarnation is never 0 and never repeats. */
    uint64_t x = tsdb_schema_new_incarnation("t", d1);
    uint64_t y = tsdb_schema_new_incarnation("t", d1);
    ASSERT(x != 0 && y != 0 && x != y);

    rm_rf(dir);
    printf("  [schema] incarnation round-trips; legacy schema loads as 0: OK\n");
}

/* ---- [schema-sync] SCHEMA_SYNC wire carries the incarnation additively ---- */
static void test_schema_sync_wire(void) {
    const char *names[2] = { "ts", "v" };
    int types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
    uint8_t buf[512];
    int n = tsdb_rpc_encode_schema(buf, sizeof(buf), "t", 2, names, types, 0,
                                   0 /*DAY*/, 0 /*bp*/, -1 /*sort*/,
                                   0xA5A5A5A5A5A5A5A5ULL);
    ASSERT(n > 0);

    char otbl[64]; char onames[TSDB_MAX_COLS][64]; int otypes[TSDB_MAX_COLS];
    int onc = 0, ots = 0, opu = 0, obp = 0, osort = 0;
    uint64_t oinc = 0;
    ASSERT(tsdb_rpc_decode_schema(buf, (uint32_t)n, otbl, sizeof(otbl),
                                  &onc, onames, otypes, &ots,
                                  &opu, &obp, &osort, &oinc) == 0);
    ASSERT(oinc == 0xA5A5A5A5A5A5A5A5ULL);             /* incarnation round-trips */
    ASSERT(onc == 2 && strcmp(otbl, "t") == 0);

    /* A legacy sender's payload (no incarnation trailer) — simulated by dropping
     * the trailing 8 bytes — decodes with incarnation 0, and the follower then
     * stamps 0 (UNKNOWN) instead of a bogus value that would reject the leader. */
    oinc = 12345;
    ASSERT(tsdb_rpc_decode_schema(buf, (uint32_t)(n - 8), otbl, sizeof(otbl),
                                  &onc, onames, otypes, &ots,
                                  &opu, &obp, &osort, &oinc) == 0);
    ASSERT(oinc == 0);                                 /* absent → UNKNOWN */

    printf("  [schema-sync] SCHEMA_SYNC carries incarnation, legacy → 0: OK\n");
}

int main(void) {
    printf("=== durable table incarnation (DROP+recreate identity) ===\n");
    test_lifecycle();
    test_reject_stale_incarnation();
    test_broadcast_replay_does_not_mint();
    test_same_incarnation_applies();
    test_legacy_sender_applies();
    test_wire_compat();
    test_schema_roundtrip();
    test_schema_sync_wire();
    printf("=== TABLE INCARNATION TESTS PASSED ===\n");
    return 0;
}
