/* test_catalog_v2.c — unified ID-keyed catalog (Track B P2).
 *
 * Round-trips db / super / child / plain through the binary catalog.log:
 * create → lookup by oid + by name → drop → reopen (replay) → compact → reopen,
 * asserting oids are stable, a dropped entity never resurrects, and the unified
 * `kind` model carries schema (PLAIN/SUPER) vs tag values (CHILD).
 */
#include "../src/catalog/catalog_v2.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_cat2";

static void col(tsdb_col_def_t *c, const char *n, tsdb_type_t t) {
    snprintf(c->name, sizeof(c->name), "%s", n); c->type = t;
}

int main(void) {
    printf("=== test_catalog_v2 ===\n");
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TMP); (void)system(cmd);

    tsdb_catalog_v2_t *c = NULL;
    ASSERT(tsdb_cat2_open(TMP, 5, &c) == TSDB_OK);

    /* Reserved databases materialized at oid 1 / 2. */
    ASSERT(tsdb_cat2_db_by_name(c, "default") == TSDB_OID_DEFAULTDB);
    ASSERT(tsdb_cat2_db_by_name(c, "sysdb")   == TSDB_OID_SYSDB);

    /* Database "prod" — minted oid carries node prefix 5. */
    tsdb_db_meta_t db; memset(&db, 0, sizeof(db));
    snprintf(db.name, sizeof(db.name), "prod");
    ASSERT(tsdb_cat2_db_create(c, &db) == TSDB_OK);
    tsdb_oid_t prod = db.oid;
    ASSERT(prod >= TSDB_OID_FIRST_USR);
    ASSERT(tsdb_oid_node(prod) == 5);
    ASSERT(tsdb_cat2_db_by_name(c, "prod") == prod);

    /* Super-table meters(ts,v) TAGS(host) in prod. */
    tsdb_table_meta_t st; memset(&st, 0, sizeof(st));
    st.db_id = prod; st.kind = TBL_SUPER; snprintf(st.name, sizeof(st.name), "meters");
    st.ncols = 2; st.ts_col_idx = 0; col(&st.cols[0], "ts", TSDB_TYPE_TIMESTAMP); col(&st.cols[1], "v", TSDB_TYPE_FLOAT64);
    st.ntag_cols = 1; col(&st.tag_cols[0], "host", TSDB_TYPE_SYMBOL);
    ASSERT(tsdb_cat2_table_create(c, &st) == TSDB_OK);
    tsdb_oid_t meters = st.oid;

    /* Child d1001 USING meters TAGS('hostA'). */
    tsdb_table_meta_t ch; memset(&ch, 0, sizeof(ch));
    ch.db_id = prod; ch.kind = TBL_CHILD; ch.parent_id = meters; snprintf(ch.name, sizeof(ch.name), "d1001");
    ch.ntag_vals = 1; ch.tag_vals[0].type = TSDB_TYPE_SYMBOL; snprintf(ch.tag_vals[0].v.s, sizeof(ch.tag_vals[0].v.s), "hostA");
    ASSERT(tsdb_cat2_table_create(c, &ch) == TSDB_OK);
    tsdb_oid_t d1001 = ch.oid;

    /* Plain table logs in the default db — a first-class TBL_PLAIN, no shadow. */
    tsdb_table_meta_t pl; memset(&pl, 0, sizeof(pl));
    pl.db_id = TSDB_OID_DEFAULTDB; pl.kind = TBL_PLAIN; snprintf(pl.name, sizeof(pl.name), "logs");
    pl.ncols = 2; pl.ts_col_idx = 0; col(&pl.cols[0], "ts", TSDB_TYPE_TIMESTAMP); col(&pl.cols[1], "msg", TSDB_TYPE_SYMBOL);
    ASSERT(tsdb_cat2_table_create(c, &pl) == TSDB_OK);
    tsdb_oid_t logs = pl.oid;

    /* All four oids distinct + name lookups scoped by db. */
    ASSERT(meters != d1001 && d1001 != logs && meters != logs);
    ASSERT(tsdb_cat2_table_by_name(c, prod, "meters") == meters);
    ASSERT(tsdb_cat2_table_by_name(c, prod, "d1001")  == d1001);
    ASSERT(tsdb_cat2_table_by_name(c, TSDB_OID_DEFAULTDB, "logs") == logs);
    ASSERT(tsdb_cat2_table_by_name(c, prod, "logs") == TSDB_OID_NONE);  /* wrong db scope */

    /* Field round-trip via get-by-oid. */
    tsdb_table_meta_t g;
    ASSERT(tsdb_cat2_table_get(c, meters, &g) == TSDB_OK);
    ASSERT(g.kind == TBL_SUPER && g.ncols == 2 && g.ntag_cols == 1);
    ASSERT(strcmp(g.tag_cols[0].name, "host") == 0 && g.tag_cols[0].type == TSDB_TYPE_SYMBOL);
    ASSERT(tsdb_cat2_table_get(c, d1001, &g) == TSDB_OK);
    ASSERT(g.kind == TBL_CHILD && g.parent_id == meters && g.ntag_vals == 1);
    ASSERT(strcmp(g.tag_vals[0].v.s, "hostA") == 0);

    size_t live_before = tsdb_cat2_count(c);   /* default, sysdb, prod, meters, d1001, logs = 6 */
    ASSERT(live_before == 6);

    /* Drop the child. */
    ASSERT(tsdb_cat2_drop(c, d1001) == TSDB_OK);
    ASSERT(tsdb_cat2_table_by_name(c, prod, "d1001") == TSDB_OID_NONE);
    ASSERT(tsdb_cat2_table_get(c, d1001, &g) == TSDB_ERR_NOTFOUND);
    ASSERT(tsdb_cat2_count(c) == 5);
    printf("  created 4 entities, dropped child; live=%zu\n", tsdb_cat2_count(c));
    tsdb_cat2_close(c);

    /* Reopen → replay: oids stable, dropped child stays dropped. */
    ASSERT(tsdb_cat2_open(TMP, 5, &c) == TSDB_OK);
    ASSERT(tsdb_cat2_db_by_name(c, "prod") == prod);
    ASSERT(tsdb_cat2_table_by_name(c, prod, "meters") == meters);
    ASSERT(tsdb_cat2_table_by_name(c, TSDB_OID_DEFAULTDB, "logs") == logs);
    ASSERT(tsdb_cat2_table_get(c, d1001, &g) == TSDB_ERR_NOTFOUND);   /* not resurrected */
    ASSERT(tsdb_cat2_count(c) == 5);
    printf("  after reopen: oids stable, child still dropped, live=5\n");

    /* Compact (retains tombstones) → reopen → still no resurrection. */
    ASSERT(tsdb_cat2_compact(c) == TSDB_OK);
    tsdb_cat2_close(c);
    ASSERT(tsdb_cat2_open(TMP, 5, &c) == TSDB_OK);
    ASSERT(tsdb_cat2_table_get(c, d1001, &g) == TSDB_ERR_NOTFOUND);   /* tombstone survived compaction */
    ASSERT(tsdb_cat2_count(c) == 5);
    ASSERT(tsdb_cat2_table_by_name(c, prod, "meters") == meters);
    printf("  after compaction+reopen: child still dropped, live=5\n");

    /* ── P3: referential integrity — no entity under a missing parent ─────── */
    tsdb_oid_t bogus = tsdb_oid_make(5, 9999999);
    {
        tsdb_table_meta_t orphan; memset(&orphan, 0, sizeof(orphan));
        orphan.db_id = prod; orphan.kind = TBL_CHILD; orphan.parent_id = bogus;
        snprintf(orphan.name, sizeof(orphan.name), "ghostkid");
        ASSERT(tsdb_cat2_table_create(c, &orphan) == TSDB_ERR_NOTFOUND);  /* parent gone */

        tsdb_table_meta_t nodb; memset(&nodb, 0, sizeof(nodb));
        nodb.db_id = bogus; nodb.kind = TBL_PLAIN; snprintf(nodb.name, sizeof(nodb.name), "nodbtbl");
        nodb.ncols = 1; col(&nodb.cols[0], "ts", TSDB_TYPE_TIMESTAMP);
        ASSERT(tsdb_cat2_table_create(c, &nodb) == TSDB_ERR_NOTFOUND);    /* db gone */

        tsdb_group_meta_t nog; memset(&nog, 0, sizeof(nog));
        nog.db_id = bogus; snprintf(nog.name, sizeof(nog.name), "noG");
        ASSERT(tsdb_cat2_group_create(c, &nog) == TSDB_ERR_NOTFOUND);     /* db gone */
    }
    printf("  RI: child/table/group under a missing parent all rejected\n");

    /* ── P3: structural cascade — DROP DATABASE wipes the whole subtree ────── */
    {
        tsdb_table_meta_t ch2; memset(&ch2, 0, sizeof(ch2));
        ch2.db_id = prod; ch2.kind = TBL_CHILD; ch2.parent_id = meters;
        snprintf(ch2.name, sizeof(ch2.name), "d1002");
        ch2.ntag_vals = 1; ch2.tag_vals[0].type = TSDB_TYPE_SYMBOL;
        snprintf(ch2.tag_vals[0].v.s, sizeof(ch2.tag_vals[0].v.s), "hostB");
        ASSERT(tsdb_cat2_table_create(c, &ch2) == TSDB_OK);
        tsdb_oid_t d1002 = ch2.oid;

        ASSERT(tsdb_cat2_drop(c, prod) == TSDB_OK);                  /* cascade */
        ASSERT(tsdb_cat2_db_by_name(c, "prod") == TSDB_OID_NONE);
        ASSERT(tsdb_cat2_table_by_name(c, prod, "meters") == TSDB_OID_NONE);
        tsdb_table_meta_t tmp;
        ASSERT(tsdb_cat2_table_get(c, d1002, &tmp) == TSDB_ERR_NOTFOUND);  /* child cascaded */
        ASSERT(tsdb_cat2_table_by_name(c, TSDB_OID_DEFAULTDB, "logs") == logs); /* other db intact */
        ASSERT(tsdb_cat2_count(c) == 3);   /* default, sysdb, logs */
    }
    printf("  cascade: DROP DATABASE prod wiped meters+d1002, logs intact, live=3\n");

    /* Cascade survives reopen — no descendant resurrects. */
    tsdb_cat2_close(c);
    ASSERT(tsdb_cat2_open(TMP, 5, &c) == TSDB_OK);
    ASSERT(tsdb_cat2_db_by_name(c, "prod") == TSDB_OID_NONE);
    ASSERT(tsdb_cat2_table_by_name(c, prod, "meters") == TSDB_OID_NONE);
    ASSERT(tsdb_cat2_count(c) == 3);
    printf("  cascade durable across reopen, live=3\n");
    tsdb_cat2_close(c);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", TMP); (void)system(cmd);
    printf("[PASS] v2 catalog: unified kind model, oid identity, RI + structural cascade, no resurrection\n");
    return 0;
}
