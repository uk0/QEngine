/* test_orphan_reap.c — the tombstone predicate that gates orphan quarantine.
 *
 * A DROP removes the catalog row cluster-wide and the storage on every node that
 * RUNS it.  A node that was DOWN for the DROP never runs it: the catalog
 * tombstone correctly keeps the table out of its catalog when it rejoins, but
 * the local directory stays forever and DROP cannot reach it (the leader refuses
 * to propose a DROP for a name it does not know).  The quarantine sweep reaps
 * those directories — but ONLY on positive evidence.
 *
 * THE SAFETY PROPERTY under test.  "Not in the catalog" is NOT evidence of a
 * drop: a table CREATED while this node was down is also absent, and that is
 * exactly the case the resurrection-safe merge exists to ADD rather than
 * destroy.  Reaping on absence would delete live data.  Only a DROP TOMBSTONE
 * — a `-stable` / `-child` record in this node's own catalog log — proves the
 * name was dropped here.
 *
 *   [1] never existed        -> NOT tombstoned  (the delete-live-data trap)
 *   [2] created, still live  -> NOT tombstoned
 *   [3] created then dropped -> tombstoned      (the reapable case)
 *   [4] child table dropped  -> tombstoned      (the other log)
 *   [5] dropped, recreated   -> tombstoned, but LIVE, so the caller's
 *                               "not live" precondition is what protects it
 *   [6] missing/unreadable log -> NOT tombstoned (fails closed)
 */

#include "../include/tsdb.h"
#include "../src/catalog/stable.h"
#include "../src/catalog/group.h"   /* tsdb_catalog_open / _close */

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int g_pass = 0, g_fail = 0;

#define CHECK(c, ...) do { \
    if (!(c)) { fprintf(stderr, "FAIL [%s:%d]: ", __FILE__, __LINE__); \
                fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); g_fail++; } \
    else { printf("  ok: "); printf(__VA_ARGS__); printf("\n"); g_pass++; } \
} while (0)

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

static void mk_stable(tsdb_catalog_t *c, const char *name) {
    tsdb_stable_t s;
    memset(&s, 0, sizeof(s));
    snprintf(s.name, sizeof(s.name), "%s", name);
    s.ncols = 2;
    snprintf(s.cols[0].name, sizeof(s.cols[0].name), "ts");
    s.cols[0].type = TSDB_TYPE_TIMESTAMP;
    snprintf(s.cols[1].name, sizeof(s.cols[1].name), "v");
    s.cols[1].type = TSDB_TYPE_INT64;
    s.ntag_cols = 1;
    snprintf(s.tag_cols[0].name, sizeof(s.tag_cols[0].name), "host");
    s.tag_cols[0].type = TSDB_TYPE_SYMBOL;
    int rc = tsdb_stable_create(c, &s);
    if (rc != TSDB_OK) { fprintf(stderr, "FATAL: stable_create %s rc=%d\n", name, rc); exit(1); }
}

static void mk_child(tsdb_catalog_t *c, const char *name, const char *stbl) {
    tsdb_child_table_t ct;
    memset(&ct, 0, sizeof(ct));
    snprintf(ct.name, sizeof(ct.name), "%s", name);
    snprintf(ct.stable_name, sizeof(ct.stable_name), "%s", stbl);
    ct.ntags = 1;
    ct.tags[0].type = TSDB_TYPE_SYMBOL;
    snprintf(ct.tags[0].v.s, sizeof(ct.tags[0].v.s), "h1");
    int rc = tsdb_child_table_create(c, &ct);
    if (rc != TSDB_OK) { fprintf(stderr, "FATAL: child_create %s rc=%d\n", name, rc); exit(1); }
}

int main(void) {
    printf("=== test_orphan_reap ===\n");
    const char *dir = "/tmp/tsdb_test_orphan_reap";
    rm_rf(dir);

    tsdb_catalog_t *cat = NULL;
    if (tsdb_catalog_open(dir, &cat) != TSDB_OK || !cat) {
        fprintf(stderr, "FATAL: catalog_open\n");
        return 1;
    }

    /* [1] THE TRAP.  A name this node never saw must NOT look dropped —
     * that is indistinguishable, by absence alone, from a table created
     * while this node was down, and reaping it would destroy live data. */
    CHECK(tsdb_catalog_name_tombstoned(cat, "never_existed") == 0,
          "a name that never existed is NOT tombstoned (absence != drop)");

    /* [2] live table: present, no tombstone. */
    mk_stable(cat, "live_t");
    CHECK(tsdb_catalog_name_tombstoned(cat, "live_t") == 0,
          "a live stable is NOT tombstoned");

    /* [3] the reapable case: created here, then dropped here. */
    mk_stable(cat, "gone_t");
    CHECK(tsdb_stable_drop(cat, "gone_t") == TSDB_OK, "drop gone_t");
    CHECK(tsdb_catalog_name_tombstoned(cat, "gone_t") == 1,
          "a dropped stable IS tombstoned (the reapable case)");

    /* [4] the other log: child tables carry their own `-child` tombstone. */
    mk_stable(cat, "parent_t");
    mk_child(cat, "kid_t", "parent_t");
    CHECK(tsdb_child_table_drop(cat, "kid_t") == TSDB_OK, "drop kid_t");
    CHECK(tsdb_catalog_name_tombstoned(cat, "kid_t") == 1,
          "a dropped CHILD is tombstoned too (child_tables.log)");

    /* [5] dropped then RECREATED: still tombstoned (the record stays), so the
     * predicate alone is not enough — the caller must also require the name to
     * be absent from the live catalog.  Pin both halves. */
    mk_stable(cat, "again_t");
    CHECK(tsdb_stable_drop(cat, "again_t") == TSDB_OK, "drop again_t");
    mk_stable(cat, "again_t");
    tsdb_stable_t out;
    CHECK(tsdb_catalog_name_tombstoned(cat, "again_t") == 1,
          "a recreated name still carries its old tombstone");
    CHECK(tsdb_stable_get(cat, "again_t", &out) == TSDB_OK,
          "...but it IS live, and the caller's not-live precondition spares it");

    /* And the reap condition the sweep actually evaluates — not live AND
     * tombstoned — is false for every name that must be spared, true only for
     * the dropped one. */
    struct { const char *n; int want; } cases[] = {
        { "never_existed", 0 },   /* created-while-down look-alike */
        { "live_t",        0 },
        { "again_t",       0 },   /* tombstoned but live */
        { "gone_t",        1 },
        { "kid_t",         1 },
    };
    for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        tsdb_stable_t s; tsdb_child_table_t c2;
        int live = (tsdb_stable_get(cat, cases[i].n, &s) == TSDB_OK) ||
                   (tsdb_child_table_get(cat, cases[i].n, &c2) == TSDB_OK);
        int reap = !live && tsdb_catalog_name_tombstoned(cat, cases[i].n);
        CHECK(reap == cases[i].want,
              "reap(%s) == %d as required", cases[i].n, cases[i].want);
    }

    tsdb_catalog_close(cat);

    /* [6] fail closed: no catalog logs at all must answer "no evidence",
     * never "dropped" — an unreadable log must not authorise a reap. */
    const char *empty = "/tmp/tsdb_test_orphan_reap_empty";
    rm_rf(empty);
    tsdb_catalog_t *c3 = NULL;
    if (tsdb_catalog_open(empty, &c3) == TSDB_OK && c3) {
        CHECK(tsdb_catalog_name_tombstoned(c3, "anything") == 0,
              "an empty/unreadable catalog log yields NO evidence (fails closed)");
        tsdb_catalog_close(c3);
    }
    rm_rf(empty);
    rm_rf(dir);

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
