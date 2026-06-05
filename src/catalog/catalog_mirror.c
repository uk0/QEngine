/* catalog_mirror.c — translate the v1 catalog into a v2 catalog.  See header. */
#include "catalog_mirror.h"
#include "database.h"
#include "group.h"
#include "stable.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* v1 empty database/group means "the implicit default"; map it to the
 * materialized default db (oid 1) so v2 never carries a blank parent. */
static tsdb_oid_t db_oid_for(tsdb_catalog_v2_t *v2, const char *v1_db) {
    if (!v1_db || !v1_db[0]) return TSDB_OID_DEFAULTDB;
    tsdb_oid_t o = tsdb_cat2_db_by_name(v2, v1_db);
    return o ? o : TSDB_OID_DEFAULTDB;   /* unknown db → default (defensive) */
}

int tsdb_catalog_mirror_to_v2(struct tsdb_catalog *v1, tsdb_catalog_v2_t *v2,
                              size_t *out_mirrored, size_t *out_skipped) {
    if (!v1 || !v2) return TSDB_ERR_INVAL;
    size_t mirrored = 0, skipped = 0;
    int rc;

    /* 1. databases.  v2 already holds reserved default(1)+sysdb(2); a v1 db of
     * the same name returns EXISTS, which we treat as already-present. */
    tsdb_database_t *dbs = NULL; size_t ndb = 0;
    if ((rc = tsdb_database_list(v1, &dbs, &ndb)) != TSDB_OK) return rc;
    for (size_t i = 0; i < ndb; i++) {
        tsdb_db_meta_t d; memset(&d, 0, sizeof(d));
        snprintf(d.name, sizeof(d.name), "%s", dbs[i].name);
        snprintf(d.description, sizeof(d.description), "%s", dbs[i].description);
        d.retention_ns = dbs[i].retention_ns;
        d.created_at = dbs[i].created_at;
        d.protected_flag = dbs[i].protected_flag;
        rc = tsdb_cat2_db_create(v2, &d);
        if (rc == TSDB_OK) mirrored++;
        else if (rc != TSDB_ERR_EXISTS) { free(dbs); return rc; }
    }
    free(dbs);

    /* 2. groups → under their (resolved) database. */
    tsdb_group_t *grps = NULL; size_t ng = 0;
    if ((rc = tsdb_group_list(v1, &grps, &ng)) != TSDB_OK) return rc;
    for (size_t i = 0; i < ng; i++) {
        tsdb_group_meta_t g; memset(&g, 0, sizeof(g));
        g.db_id = db_oid_for(v2, grps[i].database);
        snprintf(g.name, sizeof(g.name), "%s", grps[i].name);
        snprintf(g.region, sizeof(g.region), "%s", grps[i].region);
        snprintf(g.codec_profile, sizeof(g.codec_profile), "%s", grps[i].codec_profile);
        g.retention_ns = grps[i].retention_ns;
        g.replica_factor = grps[i].replica_factor;
        g.created_at = grps[i].created_at;
        rc = tsdb_cat2_group_create(v2, &g);
        if (rc == TSDB_OK) mirrored++;
        else if (rc == TSDB_ERR_EXISTS) { /* already present */ }
        else skipped++;   /* e.g. db_id resolution failed — count, don't abort */
    }
    free(grps);

    /* 3. stables → TBL_SUPER. */
    tsdb_stable_t *sts = NULL; size_t ns = 0;
    if ((rc = tsdb_stable_list(v1, &sts, &ns)) != TSDB_OK) return rc;
    for (size_t i = 0; i < ns; i++) {
        tsdb_table_meta_t t; memset(&t, 0, sizeof(t));
        t.kind = TBL_SUPER;
        t.db_id = db_oid_for(v2, sts[i].database);
        t.group_id = sts[i].group[0] ? tsdb_cat2_group_by_name(v2, t.db_id, sts[i].group) : TSDB_OID_NONE;
        snprintf(t.name, sizeof(t.name), "%s", sts[i].name);
        t.ncols = sts[i].ncols; t.ts_col_idx = sts[i].ts_col_idx;
        for (int k = 0; k < sts[i].ncols && k < TSDB_CAT2_MAX_COLS; k++) {
            snprintf(t.cols[k].name, sizeof(t.cols[k].name), "%s", sts[i].cols[k].name);
            t.cols[k].type = sts[i].cols[k].type;
        }
        t.ntag_cols = sts[i].ntag_cols;
        for (int k = 0; k < sts[i].ntag_cols && k < TSDB_CAT2_MAX_TAGS; k++) {
            snprintf(t.tag_cols[k].name, sizeof(t.tag_cols[k].name), "%s", sts[i].tag_cols[k].name);
            t.tag_cols[k].type = sts[i].tag_cols[k].type;
        }
        t.created_at = sts[i].created_at;
        rc = tsdb_cat2_table_create(v2, &t);
        if (rc == TSDB_OK) mirrored++;
        else if (rc == TSDB_ERR_EXISTS) { /* present */ }
        else skipped++;
    }
    free(sts);

    /* 4. children → TBL_CHILD under their parent super (resolved by name). */
    tsdb_child_table_t *chs = NULL; size_t nc = 0;
    if ((rc = tsdb_child_table_list(v1, NULL, &chs, &nc)) != TSDB_OK) return rc;
    for (size_t i = 0; i < nc; i++) {
        tsdb_oid_t pdb = db_oid_for(v2, chs[i].database);
        tsdb_oid_t parent = tsdb_cat2_table_by_name(v2, pdb, chs[i].stable_name);
        if (parent == TSDB_OID_NONE) { skipped++; continue; }   /* orphan — don't mirror */
        tsdb_table_meta_t t; memset(&t, 0, sizeof(t));
        t.kind = TBL_CHILD; t.parent_id = parent; t.db_id = pdb;
        snprintf(t.name, sizeof(t.name), "%s", chs[i].name);
        t.ntag_vals = chs[i].ntags;
        for (int k = 0; k < chs[i].ntags && k < TSDB_CAT2_MAX_TAGS; k++) {
            t.tag_vals[k].type = chs[i].tags[k].type;
            memcpy(&t.tag_vals[k].v, &chs[i].tags[k].v, sizeof(t.tag_vals[k].v));
        }
        rc = tsdb_cat2_table_create(v2, &t);
        if (rc == TSDB_OK) mirrored++;
        else if (rc == TSDB_ERR_EXISTS) { /* present */ }
        else skipped++;
    }
    free(chs);

    if (out_mirrored) *out_mirrored = mirrored;
    if (out_skipped)  *out_skipped  = skipped;
    return TSDB_OK;
}
