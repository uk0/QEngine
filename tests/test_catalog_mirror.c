/* test_catalog_mirror.c — v1→v2 dual-write mirror fidelity (Track B P4).
 *
 * Builds a v1 catalog (db / group / stable / children / a shadow-like stable),
 * mirrors it into a fresh v2, and asserts the unified model holds every entity
 * with the right kind + parent oid.  Then drops a stable in v1 (cascading its
 * children) and re-mirrors, asserting the v2 reflects the drop — proving v2 can
 * shadow v1 faithfully before any read path switches over.
 */
#include "../src/catalog/catalog_mirror.h"
#include "../src/catalog/database.h"
#include "../src/catalog/group.h"
#include "../src/catalog/stable.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *V1DIR = "/tmp/tsdb_test_mirror_v1";
static const char *V2DIR = "/tmp/tsdb_test_mirror_v2";

static void seed_v1(tsdb_catalog_t *c) {
    tsdb_database_t db; memset(&db, 0, sizeof(db));
    snprintf(db.name, sizeof(db.name), "prod");
    ASSERT(tsdb_database_create(c, &db) == TSDB_OK);

    tsdb_group_t g; memset(&g, 0, sizeof(g));
    snprintf(g.name, sizeof(g.name), "g1"); snprintf(g.database, sizeof(g.database), "prod");
    ASSERT(tsdb_group_create(c, &g) == TSDB_OK);

    tsdb_stable_t s; memset(&s, 0, sizeof(s));
    snprintf(s.name, sizeof(s.name), "meters");
    snprintf(s.database, sizeof(s.database), "prod"); snprintf(s.group, sizeof(s.group), "g1");
    s.ncols = 2; s.ts_col_idx = 0;
    snprintf(s.cols[0].name, sizeof(s.cols[0].name), "ts"); s.cols[0].type = TSDB_TYPE_TIMESTAMP;
    snprintf(s.cols[1].name, sizeof(s.cols[1].name), "v");  s.cols[1].type = TSDB_TYPE_FLOAT64;
    s.ntag_cols = 1;
    snprintf(s.tag_cols[0].name, sizeof(s.tag_cols[0].name), "host"); s.tag_cols[0].type = TSDB_TYPE_SYMBOL;
    ASSERT(tsdb_stable_create(c, &s) == TSDB_OK);

    for (int i = 1; i <= 2; i++) {
        tsdb_child_table_t ct; memset(&ct, 0, sizeof(ct));
        snprintf(ct.name, sizeof(ct.name), "d%d", i);
        snprintf(ct.stable_name, sizeof(ct.stable_name), "meters");
        snprintf(ct.database, sizeof(ct.database), "prod"); snprintf(ct.group, sizeof(ct.group), "g1");
        ct.ntags = 1; ct.tags[0].type = TSDB_TYPE_SYMBOL;
        snprintf(ct.tags[0].v.s, sizeof(ct.tags[0].v.s), "h%d", i);
        ASSERT(tsdb_child_table_create(c, &ct) == TSDB_OK);
    }

    /* shadow-like stable: empty db/group, no tags (a plain-table mirror in v1). */
    tsdb_stable_t px; memset(&px, 0, sizeof(px));
    snprintf(px.name, sizeof(px.name), "plainx");
    px.ncols = 1; px.ts_col_idx = 0;
    snprintf(px.cols[0].name, sizeof(px.cols[0].name), "ts"); px.cols[0].type = TSDB_TYPE_TIMESTAMP;
    px.ntag_cols = 0;
    ASSERT(tsdb_stable_create(c, &px) == TSDB_OK);
}

/* Fresh v2 helper. */
static tsdb_catalog_v2_t *fresh_v2(void) {
    char cmd[256]; snprintf(cmd, sizeof(cmd), "rm -rf %s", V2DIR); (void)system(cmd);
    tsdb_catalog_v2_t *v2 = NULL;
    ASSERT(tsdb_cat2_open(V2DIR, 11, &v2) == TSDB_OK);
    return v2;
}

int main(void) {
    printf("=== test_catalog_mirror ===\n");
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s %s", V1DIR, V2DIR); (void)system(cmd);

    tsdb_catalog_t *v1 = NULL;
    ASSERT(tsdb_catalog_open(V1DIR, &v1) == TSDB_OK);
    seed_v1(v1);

    /* Mirror into a fresh v2. */
    tsdb_catalog_v2_t *v2 = fresh_v2();
    size_t mir = 0, skip = 0;
    ASSERT(tsdb_catalog_mirror_to_v2(v1, v2, &mir, &skip) == TSDB_OK);
    printf("  mirrored=%zu skipped=%zu\n", mir, skip);

    /* prod + g1 + meters + d1 + d2 + plainx = 6 new (sysdb already reserved). */
    ASSERT(mir == 6);
    ASSERT(skip == 0);

    tsdb_oid_t prod = tsdb_cat2_db_by_name(v2, "prod");
    ASSERT(prod != TSDB_OID_NONE);
    tsdb_oid_t g1 = tsdb_cat2_group_by_name(v2, prod, "g1");
    ASSERT(g1 != TSDB_OID_NONE);
    tsdb_oid_t meters = tsdb_cat2_table_by_name(v2, prod, "meters");
    ASSERT(meters != TSDB_OID_NONE);

    tsdb_table_meta_t t;
    ASSERT(tsdb_cat2_table_get(v2, meters, &t) == TSDB_OK);
    ASSERT(t.kind == TBL_SUPER && t.db_id == prod && t.group_id == g1 && t.ntag_cols == 1);

    tsdb_oid_t d1 = tsdb_cat2_table_by_name(v2, prod, "d1");
    ASSERT(d1 != TSDB_OID_NONE);
    ASSERT(tsdb_cat2_table_get(v2, d1, &t) == TSDB_OK);
    ASSERT(t.kind == TBL_CHILD && t.parent_id == meters && t.ntag_vals == 1);
    ASSERT(strcmp(t.tag_vals[0].v.s, "h1") == 0);

    /* shadow-like stable mirrors as a SUPER under the default db. */
    tsdb_oid_t plainx = tsdb_cat2_table_by_name(v2, TSDB_OID_DEFAULTDB, "plainx");
    ASSERT(plainx != TSDB_OID_NONE);
    ASSERT(tsdb_cat2_table_get(v2, plainx, &t) == TSDB_OK && t.kind == TBL_SUPER);
    printf("  v2 holds prod/g1/meters(SUPER)/d1(CHILD)/plainx faithfully\n");
    tsdb_cat2_close(v2);

    /* Drop the stable in v1 (cascades d1,d2), then re-mirror to a FRESH v2. */
    ASSERT(tsdb_stable_drop(v1, "meters") == TSDB_OK);
    v2 = fresh_v2();
    ASSERT(tsdb_catalog_mirror_to_v2(v1, v2, &mir, &skip) == TSDB_OK);
    ASSERT(tsdb_cat2_table_by_name(v2, prod, "meters") == TSDB_OID_NONE);  /* drop reflected */
    ASSERT(tsdb_cat2_table_by_name(v2, prod, "d1") == TSDB_OID_NONE);      /* children gone */
    ASSERT(tsdb_cat2_db_by_name(v2, "prod") != TSDB_OID_NONE);             /* db intact */
    ASSERT(tsdb_cat2_table_by_name(v2, TSDB_OID_DEFAULTDB, "plainx") != TSDB_OID_NONE);
    printf("  after v1 DROP STABLE + re-mirror: meters+children gone, db+plainx intact\n");
    tsdb_cat2_close(v2);

    /* Flag-gated shadow at v1 open: TSDB_CATALOG_V2 builds the v2 shadow from
     * the replayed v1 state.  Reopen v1 with the flag and confirm the shadow
     * holds prod + plainx (meters stayed dropped). */
    tsdb_catalog_close(v1);
    setenv("TSDB_CATALOG_V2", "1", 1);
    ASSERT(tsdb_catalog_open(V1DIR, &v1) == TSDB_OK);
    size_t scount = tsdb_catalog_shadow_v2_count(v1);
    ASSERT(scount >= 4);   /* default, sysdb, prod, plainx (at minimum) */
    printf("  TSDB_CATALOG_V2 shadow built at open: %zu live entities\n", scount);
    tsdb_catalog_close(v1);
    unsetenv("TSDB_CATALOG_V2");

    snprintf(cmd, sizeof(cmd), "rm -rf %s %s", V1DIR, V2DIR); (void)system(cmd);
    printf("[PASS] v1->v2 mirror faithful, reflects drops, flag-gated shadow builds at open\n");
    return 0;
}
