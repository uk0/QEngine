/* catalog_check.c — diagnose-only consistency scan over the 4-level
 * catalog (Database → Group → VTable/STable → PTable/Child Table).
 *
 * Walks every catalog row and reports:
 *   - groups whose database doesn't exist
 *   - stables whose database or group doesn't exist
 *   - child tables whose stable doesn't exist
 *   - on-disk table directories with no catalog row (orphan dirs)
 *
 * Read-only: never repairs, never deletes.  A noisy report is the
 * input to a follow-up DROP or to a real repair tool.  Output is
 * a single JSON object the dashboard / CI can consume.
 */
#include "../catalog/database.h"
#include "../catalog/group.h"
#include "../catalog/stable.h"
#include "db.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

/* Helper: does `name` look like a user table directory (not catalog,
 * wal, raft, etc)?  Mirrors resync_is_table_dir in db_cluster.c. */
static int is_table_dir_name(const char *name) {
    if (!name || name[0] == '.' || name[0] == '_') return 0;
    if (strcmp(name, "catalog") == 0 ||
        strcmp(name, "wal")     == 0 ||
        strcmp(name, "raft")    == 0) return 0;
    return 1;
}

/* Helper: returns 1 if a table or child-table row exists with this name.
 * Either kind matches — the on-disk dir is created identically for
 * both regular tables (CREATE TABLE foo (...)) and child tables
 * (CREATE TABLE foo USING super TAGS (...)). */
static int has_catalog_row(tsdb_catalog_t *cat,
                           tsdb_db_t *db,
                           const char *table_name)
{
    tsdb_child_table_t ct;
    if (tsdb_child_table_get(cat, table_name, &ct) == TSDB_OK) return 1;
    if (tsdb_db_find_table(db, table_name) != NULL)            return 1;
    return 0;
}

int tsdb_catalog_check(tsdb_db_t *db, char *buf, size_t cap) {
    if (!db || !buf || cap < 64) return -1;

    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    if (!cat) {
        return snprintf(buf, cap,
                        "{\"error\":\"catalog not initialised\","
                        " \"orphan_groups\":[], \"orphan_stables\":[],"
                        " \"orphan_children\":[], \"orphan_dirs\":[],"
                        " \"summary\":{\"databases\":0,\"groups\":0,"
                        "\"stables\":0,\"children\":0}}");
    }

    /* ---- Pull every catalog list once. -------------------------------- */
    tsdb_database_t *dbs = NULL;  size_t ndbs = 0;
    tsdb_group_t    *grs = NULL;  size_t ngrs = 0;
    tsdb_stable_t   *sts = NULL;  size_t nsts = 0;
    tsdb_child_table_t *cts = NULL; size_t ncts = 0;

    (void)tsdb_database_list(cat, &dbs, &ndbs);
    (void)tsdb_group_list   (cat, &grs, &ngrs);
    (void)tsdb_stable_list  (cat, &sts, &nsts);
    (void)tsdb_child_table_list(cat, NULL, &cts, &ncts);

    int written = 0;
    written += snprintf(buf + written, cap - written, "{\"orphan_groups\":[");

    /* ---- Orphan groups (group references missing database). ----------- */
    int first = 1;
    for (size_t i = 0; i < ngrs; i++) {
        const char *gdb = grs[i].database;
        if (!gdb[0]) continue; /* group attached to "default" — no parent ref */
        if (tsdb_database_exists(cat, gdb)) continue;
        int add = snprintf(buf + written, cap - written,
                           "%s{\"name\":\"%s\",\"missing_db\":\"%s\"}",
                           first ? "" : ",", grs[i].name, gdb);
        if (add < 0 || (size_t)written + (size_t)add + 256 > cap) break;
        written += add; first = 0;
    }

    written += snprintf(buf + written, cap - written, "],\"orphan_stables\":[");

    /* ---- Orphan stables (missing database OR missing group). ---------- */
    first = 1;
    for (size_t i = 0; i < nsts; i++) {
        const char *sdb = sts[i].database;
        const char *sgr = sts[i].group;
        const char *miss_db = (sdb[0] && !tsdb_database_exists(cat, sdb)) ? sdb : NULL;
        tsdb_group_t gtmp;
        const char *miss_gr = (sgr[0] && tsdb_group_get(cat, sgr, &gtmp) != TSDB_OK)
                              ? sgr : NULL;
        if (!miss_db && !miss_gr) continue;
        int add = snprintf(buf + written, cap - written,
                           "%s{\"name\":\"%s\""
                           "%s%s%s"
                           "%s%s%s"
                           "}",
                           first ? "" : ",", sts[i].name,
                           miss_db ? ",\"missing_db\":\""  : "",
                           miss_db ? miss_db                : "",
                           miss_db ? "\""                   : "",
                           miss_gr ? ",\"missing_group\":\"" : "",
                           miss_gr ? miss_gr                 : "",
                           miss_gr ? "\""                    : "");
        if (add < 0 || (size_t)written + (size_t)add + 256 > cap) break;
        written += add; first = 0;
    }

    written += snprintf(buf + written, cap - written, "],\"orphan_children\":[");

    /* ---- Orphan child tables (missing stable). ------------------------ */
    first = 1;
    for (size_t i = 0; i < ncts; i++) {
        if (tsdb_stable_exists(cat, cts[i].stable_name)) continue;
        int add = snprintf(buf + written, cap - written,
                           "%s{\"name\":\"%s\",\"missing_stable\":\"%s\"}",
                           first ? "" : ",", cts[i].name, cts[i].stable_name);
        if (add < 0 || (size_t)written + (size_t)add + 256 > cap) break;
        written += add; first = 0;
    }

    written += snprintf(buf + written, cap - written, "],\"orphan_dirs\":[");

    /* ---- Physical dirs with no catalog row. --------------------------- *
     * Walk every striped data dir and surface entries that aren't
     * referenced by any catalog row. */
    first = 1;
    int ndirs = tsdb_db_data_dir_count(db);
    if (ndirs <= 0) ndirs = 1;
    for (int di = 0; di < ndirs; di++) {
        const char *dd = tsdb_db_data_dir_at(db, di);
        if (!dd) {
            if (di == 0) dd = tsdb_db_data_dir(db);
            if (!dd) continue;
        }
        DIR *d = opendir(dd);
        if (!d) continue;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (!is_table_dir_name(de->d_name)) continue;
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s", dd, de->d_name);
            struct stat st;
            if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
            if (has_catalog_row(cat, db, de->d_name)) continue;
            int add = snprintf(buf + written, cap - written,
                               "%s{\"name\":\"%s\",\"path\":\"%s\"}",
                               first ? "" : ",", de->d_name, path);
            if (add < 0 || (size_t)written + (size_t)add + 256 > cap) {
                closedir(d);
                goto out_summary;
            }
            written += add; first = 0;
        }
        closedir(d);
    }

out_summary:
    written += snprintf(buf + written, cap - written,
                        "],\"summary\":{\"databases\":%zu,\"groups\":%zu,"
                        "\"stables\":%zu,\"children\":%zu}}",
                        ndbs, ngrs, nsts, ncts);

    if (dbs) tsdb_database_list_free(dbs);
    if (grs) tsdb_group_list_free(grs);
    if (sts) free(sts);
    if (cts) free(cts);
    return written;
}
