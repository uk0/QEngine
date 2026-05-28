/* test_catalog.c — tests for Group/Device catalog + QTL DDL integration */

#include "../include/tsdb.h"
#include "../src/catalog/group.h"
#include "../src/catalog/device.h"
#include "../src/catalog/database.h"
#include "../src/catalog/stable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

/* ---- Tiny test harness -------------------------------------------------- */

static int g_pass = 0, g_fail = 0;

#define CHECK(cond, msg) do { \
    if (cond) { g_pass++; } \
    else { fprintf(stderr, "FAIL [line %d]: %s\n", __LINE__, msg); g_fail++; } \
} while(0)

#define CHECK_OK(rc, msg) CHECK((rc) == TSDB_OK, msg)
#define CHECK_EXISTS(rc, msg) CHECK((rc) == TSDB_ERR_EXISTS, msg)
#define CHECK_NOTFOUND(rc, msg) CHECK((rc) == TSDB_ERR_NOTFOUND, msg)

/* ---- Test dir setup ----------------------------------------------------- */

static const char *TEST_DIR = "/tmp/tsdb_test_catalog";

static void rmrf(const char *dir) {
    /* Simple recursive delete for test cleanup. */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", dir);
    (void)system(cmd);
}

static void setup(void) {
    rmrf(TEST_DIR);
    mkdir(TEST_DIR, 0755);
}

/* ---- Catalog API tests --------------------------------------------------- */

static void test_group_create_list(void) {
    printf("test_group_create_list\n");
    setup();

    tsdb_catalog_t *c = NULL;
    CHECK_OK(tsdb_catalog_open(TEST_DIR, &c), "catalog_open");

    tsdb_group_t g;
    memset(&g, 0, sizeof(g));
    snprintf(g.name, sizeof(g.name), "factory_a");
    snprintf(g.region, sizeof(g.region), "us-east-1");
    g.retention_ns = 30LL * 86400LL * 1000000000LL;
    snprintf(g.codec_profile, sizeof(g.codec_profile), "chimp128+lz");
    g.replica_factor = 2;
    snprintf(g.tags, sizeof(g.tags), "env=prod,owner=team_iot");

    CHECK_OK(tsdb_group_create(c, &g), "group_create");

    /* Duplicate should fail. */
    CHECK_EXISTS(tsdb_group_create(c, &g), "group_create_dup");

    /* Get */
    tsdb_group_t got;
    CHECK_OK(tsdb_group_get(c, "factory_a", &got), "group_get");
    CHECK(strcmp(got.name, "factory_a") == 0, "group name match");
    CHECK(strcmp(got.region, "us-east-1") == 0, "group region match");
    CHECK(got.retention_ns == 30LL * 86400LL * 1000000000LL, "retention match");
    CHECK(strcmp(got.codec_profile, "chimp128+lz") == 0, "codec_profile match");
    CHECK(got.replica_factor == 2, "replica_factor match");

    /* List */
    tsdb_group_t *arr = NULL;
    size_t n = 0;
    CHECK_OK(tsdb_group_list(c, &arr, &n), "group_list");
    CHECK(n == 1, "group_list count == 1");
    int found = 0;
    for (size_t i = 0; i < n; i++) if (strcmp(arr[i].name, "factory_a") == 0) found = 1;
    CHECK(found, "factory_a in group list");
    tsdb_group_list_free(arr);

    tsdb_catalog_close(c);
    rmrf(TEST_DIR);
}

static void test_device_create_list(void) {
    printf("test_device_create_list\n");
    setup();

    tsdb_catalog_t *c = NULL;
    CHECK_OK(tsdb_catalog_open(TEST_DIR, &c), "catalog_open");

    /* Create group first. */
    tsdb_group_t g;
    memset(&g, 0, sizeof(g));
    snprintf(g.name, sizeof(g.name), "plant_b");
    snprintf(g.region, sizeof(g.region), "cn-beijing");
    CHECK_OK(tsdb_group_create(c, &g), "group_create");

    /* Create device in group. */
    tsdb_device_t d;
    memset(&d, 0, sizeof(d));
    snprintf(d.group, sizeof(d.group), "plant_b");
    snprintf(d.id, sizeof(d.id), "sensor_001");
    snprintf(d.type, sizeof(d.type), "temperature");
    snprintf(d.location, sizeof(d.location), "line_3");
    snprintf(d.tags, sizeof(d.tags), "floor=2,rack=A1");

    CHECK_OK(tsdb_device_create(c, &d), "device_create");

    /* Duplicate device. */
    CHECK_EXISTS(tsdb_device_create(c, &d), "device_create_dup");

    /* Device in non-existent group. */
    tsdb_device_t d2 = d;
    snprintf(d2.group, sizeof(d2.group), "nonexistent");
    CHECK_NOTFOUND(tsdb_device_create(c, &d2), "device_create_bad_group");

    /* Get device. */
    tsdb_device_t got;
    CHECK_OK(tsdb_device_get(c, "plant_b", "sensor_001", &got), "device_get");
    CHECK(strcmp(got.id, "sensor_001") == 0, "device id match");
    CHECK(strcmp(got.type, "temperature") == 0, "device type match");
    CHECK(strcmp(got.location, "line_3") == 0, "device location match");

    /* List devices in group. */
    tsdb_device_t *arr = NULL;
    size_t n = 0;
    CHECK_OK(tsdb_device_list(c, "plant_b", &arr, &n), "device_list");
    CHECK(n == 1, "device_list count == 1");
    int found = 0;
    for (size_t i = 0; i < n; i++) if (strcmp(arr[i].id, "sensor_001") == 0) found = 1;
    CHECK(found, "sensor_001 in device list");
    tsdb_device_list_free(arr);

    tsdb_catalog_close(c);
    rmrf(TEST_DIR);
}

static void test_device_drop(void) {
    printf("test_device_drop\n");
    setup();

    tsdb_catalog_t *c = NULL;
    CHECK_OK(tsdb_catalog_open(TEST_DIR, &c), "catalog_open");

    tsdb_group_t g;
    memset(&g, 0, sizeof(g));
    snprintf(g.name, sizeof(g.name), "grp_drop");
    CHECK_OK(tsdb_group_create(c, &g), "group_create");

    tsdb_device_t d;
    memset(&d, 0, sizeof(d));
    snprintf(d.group, sizeof(d.group), "grp_drop");
    snprintf(d.id, sizeof(d.id), "dev_x");
    snprintf(d.type, sizeof(d.type), "pressure");
    CHECK_OK(tsdb_device_create(c, &d), "device_create");

    /* Drop device. */
    CHECK_OK(tsdb_device_drop(c, "grp_drop", "dev_x"), "device_drop");
    CHECK_NOTFOUND(tsdb_device_drop(c, "grp_drop", "dev_x"), "device_drop_again");

    /* List should be empty. */
    tsdb_device_t *arr = NULL;
    size_t n = 0;
    CHECK_OK(tsdb_device_list(c, "grp_drop", &arr, &n), "device_list_empty");
    CHECK(n == 0, "device list empty after drop");
    tsdb_device_list_free(arr);

    tsdb_catalog_close(c);
    rmrf(TEST_DIR);
}

static void test_group_drop_cascade(void) {
    printf("test_group_drop_cascade\n");
    setup();

    tsdb_catalog_t *c = NULL;
    CHECK_OK(tsdb_catalog_open(TEST_DIR, &c), "catalog_open");

    tsdb_group_t g;
    memset(&g, 0, sizeof(g));
    snprintf(g.name, sizeof(g.name), "cascade_grp");
    CHECK_OK(tsdb_group_create(c, &g), "group_create");

    /* Add two devices. */
    tsdb_device_t d;
    memset(&d, 0, sizeof(d));
    snprintf(d.group, sizeof(d.group), "cascade_grp");
    snprintf(d.id, sizeof(d.id), "d1");
    CHECK_OK(tsdb_device_create(c, &d), "device_create_d1");
    snprintf(d.id, sizeof(d.id), "d2");
    CHECK_OK(tsdb_device_create(c, &d), "device_create_d2");

    /* Drop group — should cascade-delete devices. */
    CHECK_OK(tsdb_group_drop(c, "cascade_grp"), "group_drop");

    /* Group should not exist. */
    tsdb_group_t got;
    CHECK_NOTFOUND(tsdb_group_get(c, "cascade_grp", &got), "group_gone");

    /* Devices list empty. */
    tsdb_device_t *arr = NULL;
    size_t n = 0;
    CHECK_OK(tsdb_device_list(c, "cascade_grp", &arr, &n), "device_list_after_cascade");
    CHECK(n == 0, "no devices after cascade drop");
    tsdb_device_list_free(arr);

    tsdb_catalog_close(c);
    rmrf(TEST_DIR);
}

static void test_persistence(void) {
    printf("test_persistence\n");
    setup();

    /* Open, create, close. */
    {
        tsdb_catalog_t *c = NULL;
        CHECK_OK(tsdb_catalog_open(TEST_DIR, &c), "catalog_open_1");

        tsdb_group_t g;
        memset(&g, 0, sizeof(g));
        snprintf(g.name, sizeof(g.name), "persist_grp");
        snprintf(g.region, sizeof(g.region), "eu-west-1");
        g.retention_ns = 7LL * 86400LL * 1000000000LL;
        snprintf(g.codec_profile, sizeof(g.codec_profile), "raw");
        g.replica_factor = 1;
        /* Tags with spaces and special chars to test encoding. */
        snprintf(g.tags, sizeof(g.tags), "env=staging,note=has space");
        CHECK_OK(tsdb_group_create(c, &g), "group_create");

        tsdb_device_t d;
        memset(&d, 0, sizeof(d));
        snprintf(d.group, sizeof(d.group), "persist_grp");
        snprintf(d.id, sizeof(d.id), "persist_dev_1");
        snprintf(d.type, sizeof(d.type), "humidity");
        snprintf(d.location, sizeof(d.location), "warehouse A");
        CHECK_OK(tsdb_device_create(c, &d), "device_create");

        tsdb_catalog_close(c);
    }

    /* Reopen — replay. */
    {
        tsdb_catalog_t *c = NULL;
        CHECK_OK(tsdb_catalog_open(TEST_DIR, &c), "catalog_open_2");

        tsdb_group_t got;
        CHECK_OK(tsdb_group_get(c, "persist_grp", &got), "group_get_after_reopen");
        CHECK(strcmp(got.region, "eu-west-1") == 0, "region after reopen");
        CHECK(got.retention_ns == 7LL * 86400LL * 1000000000LL, "retention after reopen");
        /* Check special-char tag encoding survived roundtrip. */
        CHECK(strstr(got.tags, "has space") != NULL, "space tag after reopen");

        tsdb_device_t dg;
        CHECK_OK(tsdb_device_get(c, "persist_grp", "persist_dev_1", &dg), "device_get_after_reopen");
        CHECK(strcmp(dg.type, "humidity") == 0, "device type after reopen");
        CHECK(strcmp(dg.location, "warehouse A") == 0, "device location after reopen");

        tsdb_catalog_close(c);
    }

    rmrf(TEST_DIR);
}

static void test_update_last_seen(void) {
    printf("test_update_last_seen\n");
    setup();

    tsdb_catalog_t *c = NULL;
    CHECK_OK(tsdb_catalog_open(TEST_DIR, &c), "catalog_open");

    tsdb_group_t g;
    memset(&g, 0, sizeof(g));
    snprintf(g.name, sizeof(g.name), "ls_grp");
    CHECK_OK(tsdb_group_create(c, &g), "group_create");

    tsdb_device_t d;
    memset(&d, 0, sizeof(d));
    snprintf(d.group, sizeof(d.group), "ls_grp");
    snprintf(d.id, sizeof(d.id), "ls_dev");
    CHECK_OK(tsdb_device_create(c, &d), "device_create");

    tsdb_ts_t new_ts = 1700000000000000000LL;
    CHECK_OK(tsdb_device_update_last_seen(c, "ls_grp", "ls_dev", new_ts), "update_last_seen");

    tsdb_device_t got;
    CHECK_OK(tsdb_device_get(c, "ls_grp", "ls_dev", &got), "device_get");
    CHECK(got.last_seen == new_ts, "last_seen updated");

    /* Persist and reload. */
    tsdb_catalog_close(c);

    tsdb_catalog_t *c2 = NULL;
    CHECK_OK(tsdb_catalog_open(TEST_DIR, &c2), "catalog_reopen");
    CHECK_OK(tsdb_device_get(c2, "ls_grp", "ls_dev", &got), "device_get_reopen");
    CHECK(got.last_seen == new_ts, "last_seen after reopen");
    tsdb_catalog_close(c2);

    rmrf(TEST_DIR);
}

/* ---- QTL DDL integration tests ------------------------------------------ */

static void test_qtl_ddl(void) {
    printf("test_qtl_ddl\n");
    /* Use tsdb_query() which uses the catalog embedded in tsdb_db_t. */
    setup();

    tsdb_db_t *db = NULL;
    CHECK_OK(tsdb_open(TEST_DIR, &db), "db_open");

    tsdb_result_t *r = NULL;
    int rc;

    /* CREATE GROUP */
    rc = tsdb_query(db, "CREATE GROUP factory_a (region='us-east-1', retention='30d', profile='chimp128+lz')", &r);
    CHECK_OK(rc, "qtl create group");
    if (r) { CHECK(tsdb_result_next(r) == 1, "create group result row"); tsdb_result_free(r); r = NULL; }

    /* LIST GROUPS */
    rc = tsdb_query(db, "LIST GROUPS", &r);
    CHECK_OK(rc, "qtl list groups");
    if (r) {
        int found = 0;
        while (tsdb_result_next(r) == 1) {
            const char *name = tsdb_result_sym(r, 0);
            if (name && strcmp(name, "factory_a") == 0) found = 1;
        }
        CHECK(found, "factory_a in LIST GROUPS result");
        tsdb_result_free(r); r = NULL;
    }

    /* CREATE DEVICE */
    rc = tsdb_query(db,
        "CREATE DEVICE sensor_001 IN GROUP factory_a (type='temperature', location='line_3')", &r);
    CHECK_OK(rc, "qtl create device");
    if (r) { CHECK(tsdb_result_next(r) == 1, "create device result row"); tsdb_result_free(r); r = NULL; }

    /* LIST DEVICES IN GROUP */
    rc = tsdb_query(db, "LIST DEVICES IN GROUP factory_a", &r);
    CHECK_OK(rc, "qtl list devices");
    if (r) {
        int found = 0;
        while (tsdb_result_next(r) == 1) {
            const char *id = tsdb_result_sym(r, 1);
            if (id && strcmp(id, "sensor_001") == 0) found = 1;
        }
        CHECK(found, "sensor_001 in LIST DEVICES result");
        tsdb_result_free(r); r = NULL;
    }

    /* DROP DEVICE */
    rc = tsdb_query(db, "DROP DEVICE sensor_001 IN GROUP factory_a", &r);
    CHECK_OK(rc, "qtl drop device");
    if (r) { tsdb_result_free(r); r = NULL; }

    /* LIST DEVICES should be empty now */
    rc = tsdb_query(db, "LIST DEVICES IN GROUP factory_a", &r);
    CHECK_OK(rc, "qtl list devices after drop");
    if (r) {
        int count = 0;
        while (tsdb_result_next(r) == 1) count++;
        CHECK(count == 0, "no devices after drop");
        tsdb_result_free(r); r = NULL;
    }

    /* DROP GROUP */
    rc = tsdb_query(db, "DROP GROUP factory_a", &r);
    CHECK_OK(rc, "qtl drop group");
    if (r) { tsdb_result_free(r); r = NULL; }

    /* LIST GROUPS should be empty */
    rc = tsdb_query(db, "LIST GROUPS", &r);
    CHECK_OK(rc, "qtl list groups after drop");
    if (r) {
        int count = 0;
        while (tsdb_result_next(r) == 1) count++;
        CHECK(count == 0, "no groups after drop");
        tsdb_result_free(r); r = NULL;
    }

    /* Existing SELECT queries still work. */
    tsdb_col_t cols[] = {{"ts", TSDB_TYPE_TIMESTAMP}, {"val", TSDB_TYPE_FLOAT64}};
    CHECK_OK(tsdb_create_table(db, "sensor_data", cols, 2, "ts"), "create table");
    rc = tsdb_query(db, "SELECT * FROM sensor_data", &r);
    CHECK_OK(rc, "select still works after ddl");
    if (r) { tsdb_result_free(r); r = NULL; }

    tsdb_close(db);
    rmrf(TEST_DIR);
}

static void test_cascade_drop_via_qtl(void) {
    printf("test_cascade_drop_via_qtl\n");
    setup();

    tsdb_db_t *db = NULL;
    CHECK_OK(tsdb_open(TEST_DIR, &db), "db_open");

    tsdb_result_t *r = NULL;

    tsdb_query(db, "CREATE GROUP grp_cascade", &r);
    if (r) { tsdb_result_free(r); r = NULL; }
    tsdb_query(db, "CREATE DEVICE dev1 IN GROUP grp_cascade", &r);
    if (r) { tsdb_result_free(r); r = NULL; }
    tsdb_query(db, "CREATE DEVICE dev2 IN GROUP grp_cascade", &r);
    if (r) { tsdb_result_free(r); r = NULL; }

    /* Verify 2 devices. */
    tsdb_query(db, "LIST DEVICES IN GROUP grp_cascade", &r);
    if (r) {
        int cnt = 0;
        while (tsdb_result_next(r) == 1) cnt++;
        CHECK(cnt == 2, "2 devices before cascade drop");
        tsdb_result_free(r); r = NULL;
    }

    /* Drop group (cascade). */
    int rc = tsdb_query(db, "DROP GROUP grp_cascade", &r);
    CHECK_OK(rc, "drop group cascade");
    if (r) { tsdb_result_free(r); r = NULL; }

    /* Devices now gone in a fresh catalog (reopen). */
    tsdb_close(db);
    CHECK_OK(tsdb_open(TEST_DIR, &db), "db_reopen");

    rc = tsdb_query(db, "LIST DEVICES IN GROUP grp_cascade", &r);
    CHECK_OK(rc, "list devices after cascade reopen");
    if (r) {
        int cnt = 0;
        while (tsdb_result_next(r) == 1) cnt++;
        CHECK(cnt == 0, "0 devices after cascade drop + reopen");
        tsdb_result_free(r); r = NULL;
    }

    tsdb_close(db);
    rmrf(TEST_DIR);
}

/* Forward decl for the db accessor; db.h isn't on the public surface. */
tsdb_catalog_t *tsdb_db_catalog(tsdb_db_t *db);

/* ---- Hot-reopen of in-memory catalog (tsdb_db_reload_catalog) ---------- */
/*
 * Simulates what the Raft InstallSnapshot receive path does:
 *   - a remote agent atomically rewrites the on-disk catalog .log files
 *   - the live db handle must re-read them without a restart
 *
 * The concurrency wrinkle: another thread may be repeatedly hitting the
 * catalog via the stable tsdb_db_catalog() accessor while the reload is
 * in flight.  The reload must not blow the process up — the overlap is
 * a microsecond window where the racer may still be reading the old
 * catalog right after we've swapped, but before we close it.  We use
 * db->lock to serialise the swap itself and rely on the racer having
 * dropped its pointer by the time close runs (racer only touches the
 * pointer inside tsdb_db_catalog() itself, which is a plain atomic read).
 *
 * What we actually verify post-reload is the visible-state contract:
 *   before reload: catalog reports state from dir's first log snapshot
 *   after  reload: catalog reports the overwritten log contents
 */

static volatile int g_reload_reader_stop = 0;

static void *reload_reader_thread(void *ud) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    /* Pound on tsdb_db_catalog() while the main thread swaps in a fresh
     * catalog.  Purely a "doesn't crash" check — we don't assert on the
     * pointer contents because they legitimately change mid-loop. */
    int loops = 0;
    while (!g_reload_reader_stop) {
        tsdb_catalog_t *c = tsdb_db_catalog(db);
        (void)c;
        loops++;
        /* Brief pause so we actually race vs. spin the CPU into a wedge. */
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 10000 };
        nanosleep(&ts, NULL);
    }
    (void)loops;
    return NULL;
}

/* Count DB rows in the on-disk catalog via LIST DATABASES — deliberately
 * exercises the same code path that Raft apply + scenarios hit, rather
 * than peeking at the internal hashmap. */
static int count_databases(tsdb_db_t *db) {
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, "LIST DATABASES;", &r) != TSDB_OK || !r) return -1;
    int n = 0;
    while (tsdb_result_next(r) == 1) n++;
    tsdb_result_free(r);
    return n;
}

static void test_reload_catalog(void) {
    printf("test_reload_catalog\n");
    setup();

    tsdb_db_t *db = NULL;
    CHECK_OK(tsdb_open(TEST_DIR, &db), "db_open");

    /* Seed the live catalog with one user DB.  sysdb is always there
     * (auto-bootstrapped), so baseline = sysdb + our new one = 2. */
    tsdb_result_t *r = NULL;
    CHECK_OK(tsdb_query(db, "CREATE DATABASE pre_snap", &r), "create pre_snap");
    if (r) { tsdb_result_free(r); r = NULL; }
    CHECK(count_databases(db) == 2, "pre-reload shows sysdb + pre_snap");

    /* Prepare a "snapshot body" in a staging dir — a fresh DB set — and
     * spawn a reader thread that pounds tsdb_db_catalog() concurrently. */
    g_reload_reader_stop = 0;
    pthread_t racer;
    pthread_create(&racer, NULL, reload_reader_thread, db);

    /* Fabricate the on-disk state a fresh catalog would produce by
     * closing db, opening a second db on a side dir, running the DDL we
     * want, closing it, then copy the side catalog log files over the
     * live TEST_DIR catalog ones.  This mirrors the raft_snapshot
     * restore_cb atomic-rename path.  Finally hand control back to the
     * main db and call tsdb_db_reload_catalog. */
    char side_dir[256];
    snprintf(side_dir, sizeof(side_dir), "%s_side", TEST_DIR);
    rmrf(side_dir);
    mkdir(side_dir, 0755);

    {
        tsdb_db_t *side_db = NULL;
        CHECK_OK(tsdb_open(side_dir, &side_db), "side_open");
        CHECK_OK(tsdb_query(side_db, "CREATE DATABASE post_snap_a", &r),
                 "side create post_snap_a");
        if (r) { tsdb_result_free(r); r = NULL; }
        CHECK_OK(tsdb_query(side_db, "CREATE DATABASE post_snap_b", &r),
                 "side create post_snap_b");
        if (r) { tsdb_result_free(r); r = NULL; }
        CHECK_OK(tsdb_query(side_db, "CREATE DATABASE post_snap_c", &r),
                 "side create post_snap_c");
        if (r) { tsdb_result_free(r); r = NULL; }
        tsdb_close(side_db);
    }

    /* Overlay the side catalog files onto the live test dir.  Simulates
     * the atomic-rename loop in raft_snapshot_restore_cb. */
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "cp %s/catalog/*.log %s/catalog/", side_dir, TEST_DIR);
    (void)system(cmd);

    /* Reload — this is the whole point of the test. */
    CHECK_OK(tsdb_db_reload_catalog(db), "reload_catalog");

    g_reload_reader_stop = 1;
    pthread_join(racer, NULL);

    /* Post-reload, the databases must match what's on disk under
     * side_dir: sysdb + post_snap_a + _b + _c = 4 rows.  Previous
     * pre_snap entry is still present because side_db saw sysdb from
     * auto-bootstrap and we appended a/b/c — the original pre_snap
     * record lives in the .log that was overwritten.  So final count
     * depends on whether side_db's log overlay totally replaced the
     * original: it does (cp -f).  Expected set: sysdb + a + b + c. */
    r = NULL;
    CHECK_OK(tsdb_query(db, "LIST DATABASES;", &r), "LIST after reload");
    int saw_sysdb = 0, saw_a = 0, saw_b = 0, saw_c = 0, saw_pre = 0;
    if (r) {
        int ncols = tsdb_result_ncols(r);
        while (tsdb_result_next(r) == 1) {
            const char *nm = NULL;
            for (int c = 0; c < ncols; c++) {
                const char *cn = tsdb_result_col_name(r, c);
                if (cn && strcmp(cn, "name") == 0) nm = tsdb_result_sym(r, c);
            }
            if (!nm) continue;
            if (!strcmp(nm, "sysdb"))        saw_sysdb = 1;
            else if (!strcmp(nm, "post_snap_a")) saw_a = 1;
            else if (!strcmp(nm, "post_snap_b")) saw_b = 1;
            else if (!strcmp(nm, "post_snap_c")) saw_c = 1;
            else if (!strcmp(nm, "pre_snap"))    saw_pre = 1;
        }
        tsdb_result_free(r);
    }
    CHECK(saw_sysdb, "sysdb survives reload");
    CHECK(saw_a, "post_snap_a visible after reload");
    CHECK(saw_b, "post_snap_b visible after reload");
    CHECK(saw_c, "post_snap_c visible after reload");
    CHECK(!saw_pre, "pre_snap no longer present (overwritten)");

    /* Reload is idempotent: a second call should leave state intact. */
    CHECK_OK(tsdb_db_reload_catalog(db), "reload_catalog (idempotent)");
    CHECK(count_databases(db) == 4, "post-reload count stable on repeat");

    tsdb_close(db);
    rmrf(side_dir);
    rmrf(TEST_DIR);
}

/* ---- Main --------------------------------------------------------------- */

/* DROP DATABASE must cascade ALL stables of the database — including a
 * stable whose group record is already missing (the orphan-prone state a
 * chaos / partial-failure window can produce).  Pre-fix the cascade walked
 * group-by-group and only also swept group=="" stables, so a stable under
 * a non-existent group survived as an orphan_stable.  This injects exactly
 * that state via the catalog API and asserts DROP DATABASE leaves nothing. */
static void test_drop_database_cascade_orphan_group(void) {
    printf("test_drop_database_cascade_orphan_group\n");
    setup();

    tsdb_db_t *db = NULL;
    CHECK_OK(tsdb_open(TEST_DIR, &db), "db_open");
    tsdb_catalog_t *c = tsdb_db_catalog(db);
    CHECK(c != NULL, "db_catalog");

    tsdb_database_t d = {0};
    snprintf(d.name, sizeof(d.name), "casc_db");
    CHECK_OK(tsdb_database_create(c, &d), "database_create");

    /* Stable A: group record exists. */
    tsdb_group_t g = {0};
    snprintf(g.name, sizeof(g.name), "g_real");
    snprintf(g.database, sizeof(g.database), "casc_db");
    CHECK_OK(tsdb_group_create(c, &g), "group_create");

    tsdb_stable_t sa = {0};
    snprintf(sa.name, sizeof(sa.name), "st_real");
    snprintf(sa.database, sizeof(sa.database), "casc_db");
    snprintf(sa.group, sizeof(sa.group), "g_real");
    sa.ncols = 1; sa.ts_col_idx = 0;
    snprintf(sa.cols[0].name, sizeof(sa.cols[0].name), "ts");
    sa.cols[0].type = TSDB_TYPE_TIMESTAMP;
    CHECK_OK(tsdb_stable_create(c, &sa), "stable_real_create");

    /* Stable B: group "g_ghost" is NEVER created as a record — exactly the
     * orphan-prone case the old group-gated cascade missed. */
    tsdb_stable_t sb = {0};
    snprintf(sb.name, sizeof(sb.name), "st_ghost");
    snprintf(sb.database, sizeof(sb.database), "casc_db");
    snprintf(sb.group, sizeof(sb.group), "g_ghost");
    sb.ncols = 1; sb.ts_col_idx = 0;
    snprintf(sb.cols[0].name, sizeof(sb.cols[0].name), "ts");
    sb.cols[0].type = TSDB_TYPE_TIMESTAMP;
    CHECK_OK(tsdb_stable_create(c, &sb), "stable_ghost_create");

    CHECK(tsdb_stable_exists(c, "st_real") == 1, "st_real exists pre-drop");
    CHECK(tsdb_stable_exists(c, "st_ghost") == 1, "st_ghost exists pre-drop");

    /* DROP DATABASE via the QTL path (exercises the exec.c cascade). */
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "DROP DATABASE casc_db", &r);
    CHECK_OK(rc, "drop database casc_db");
    if (r) { tsdb_result_free(r); r = NULL; }

    /* Both stables must be gone — no orphan survives. */
    CHECK(tsdb_stable_exists(c, "st_real") == 0, "st_real cascaded");
    CHECK(tsdb_stable_exists(c, "st_ghost") == 0, "st_ghost cascaded (orphan-group)");
    CHECK(tsdb_database_exists(c, "casc_db") == 0, "casc_db dropped");

    tsdb_close(db);
    rmrf(TEST_DIR);
}

int main(void) {
    printf("=== test_catalog ===\n");

    test_group_create_list();
    test_device_create_list();
    test_device_drop();
    test_group_drop_cascade();
    test_persistence();
    test_update_last_seen();
    test_qtl_ddl();
    test_cascade_drop_via_qtl();
    test_drop_database_cascade_orphan_group();
    test_reload_catalog();

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
