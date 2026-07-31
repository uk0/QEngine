/* backup.c — backup / restore manifest support.
 *
 * /backup ships a tar.gz of data_dir as a physical snapshot, but
 * pre-fix there was no way to verify the restored copy actually
 * contained the same rows as the source.  An operator following
 * "stop → extract → restart" had no signal if the tar was
 * truncated, the extract dropped a partition, or anti-entropy
 * silently masked a missing table.
 *
 * This module produces a JSON manifest written into data_dir
 * just before tar runs.  The manifest captures:
 *   - format_version (forward-compat)
 *   - created_at_ns
 *   - per-table: row count + max_ts
 *
 * Restore tooling can parse the manifest and re-query the live
 * cluster for the same numbers; mismatch = bad restore.
 */
#include "db.h"
#include "../../include/tsdb.h"
#include "../server/metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

/* Maximum tables we list in the manifest.  Above this we emit a
 * "tables_truncated" hint so operators don't trust an incomplete
 * count.  Practical IoT clusters have thousands of PTables; the
 * limit here is a buffer-size safeguard, not a hard schema rule. */
#define MANIFEST_MAX_TABLES 4096

/* Per-table stats fetched from the live db.  Pulled via tsdb_query so
 * the same code path that serves SELECT to the dashboard sees the
 * same numbers as the manifest. */
typedef struct {
    char     name[64];
    uint64_t count;
    int64_t  max_ts;
    int      ok;       /* 0 = SELECT failed; manifest still emits the entry */
} mf_row_t;

/* Same heuristic catalog_check.c uses: filter system dirs out of the
 * data_dir scan so partition listings don't leak as "tables". */
static int mf_is_table_dir(const char *name) {
    if (!name || name[0] == '.' || name[0] == '_') return 0;
    if (strcmp(name, "catalog") == 0 ||
        strcmp(name, "wal")     == 0 ||
        strcmp(name, "raft")    == 0) return 0;
    return 1;
}

/* Append `name` to `names` if not already present.  Returns the new
 * count or unchanged if the name was a duplicate.  Linear scan is
 * fine — typical clusters have ≤ thousands of tables, and this
 * runs once per /backup call. */
static int mf_add_unique(char (*names)[64], int n, int max, const char *name) {
    for (int i = 0; i < n; i++) {
        if (strcmp(names[i], name) == 0) return n;
    }
    if (n >= max) return n;
    snprintf(names[n], 64, "%s", name);
    return n + 1;
}

static int mf_compare(const void *pa, const void *pb) {
    const mf_row_t *a = (const mf_row_t *)pa;
    const mf_row_t *b = (const mf_row_t *)pb;
    return strcmp(a->name, b->name);
}

/* Compose the manifest JSON into buf.  Returns bytes written or
 * negative on error.  Caller is responsible for sizing buf — at
 * minimum ~150 bytes per table plus the document framing. */
int tsdb_backup_write_manifest_json(tsdb_db_t *db, char *buf, size_t cap) {
    if (!db || !buf || cap < 256) return -1;

    char (*names)[64] = (char (*)[64])calloc(MANIFEST_MAX_TABLES, sizeof(*names));
    if (!names) return -1;

    /* Source 1: open table list.  Catches every table the in-process
     * db handle knows about — what tsdb_query / SELECT would resolve. */
    int n = tsdb_db_list_table_names(db, names, MANIFEST_MAX_TABLES);
    if (n < 0) n = 0;

    /* Source 2: data_dir scan across every striped dir.  Catches tables
     * whose physical dir exists on disk but is somehow not in db->tables
     * (e.g. cluster mode where a recently-CREATEd table hasn't been
     * locally written-to yet).  The tarball will include these dirs, so
     * the manifest must too — otherwise the verifier reports "missing
     * from manifest" while the bytes are right there in the snapshot. */
    int ndirs = tsdb_db_data_dir_count(db);
    if (ndirs <= 0) ndirs = 1;
    for (int di = 0; di < ndirs; di++) {
        const char *dd = tsdb_db_data_dir_at(db, di);
        if (!dd || !dd[0]) {
            if (di == 0) dd = tsdb_db_data_dir(db);
            if (!dd) continue;
        }
        DIR *d = opendir(dd);
        if (!d) continue;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (!mf_is_table_dir(de->d_name)) continue;
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s", dd, de->d_name);
            struct stat st;
            if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
            n = mf_add_unique(names, n, MANIFEST_MAX_TABLES, de->d_name);
        }
        closedir(d);
    }

    mf_row_t *rows = (mf_row_t *)calloc((size_t)(n > 0 ? n : 1), sizeof(*rows));
    if (!rows) { free(names); return -1; }

    /* Pull (count, max_ts) per table.  Ignore SELECT errors — we still
     * emit the entry with ok=false so a restore-time verifier can flag
     * the table as "couldn't compute baseline at backup time". */
    for (int i = 0; i < n; i++) {
        snprintf(rows[i].name, sizeof(rows[i].name), "%s", names[i]);
        char qtl[256];
        snprintf(qtl, sizeof(qtl),
                 "SELECT count(*), max(ts) FROM %s", names[i]);
        tsdb_result_t *r = NULL;
        int qrc = tsdb_query(db, qtl, &r);
        if (qrc == TSDB_OK && r) {
            if (tsdb_result_next(r) && tsdb_result_ncols(r) >= 2) {
                rows[i].count  = (uint64_t)tsdb_result_i64(r, 0);
                rows[i].max_ts = (int64_t) tsdb_result_ts (r, 1);
                rows[i].ok     = 1;
            }
        }
        if (r) tsdb_result_free(r);
    }
    free(names);

    /* Sort so the manifest is reproducible across calls — operators
     * comparing two backups see deterministic ordering. */
    qsort(rows, (size_t)n, sizeof(*rows), mf_compare);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    int64_t now_ns = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;

    int written = snprintf(buf, cap,
        "{\"format_version\":1,\"created_at_ns\":%lld,"
        "\"table_count\":%d,\"tables\":[",
        (long long)now_ns, n);

    for (int i = 0; i < n; i++) {
        int add = snprintf(buf + written, cap - written,
            "%s{\"name\":\"%s\",\"count\":%llu,\"max_ts_ns\":%lld,\"ok\":%s}",
            i == 0 ? "" : ",",
            rows[i].name,
            (unsigned long long)rows[i].count,
            (long long)rows[i].max_ts,
            rows[i].ok ? "true" : "false");
        if (add < 0 || (size_t)written + (size_t)add + 32 > cap) {
            written += snprintf(buf + written, cap - written,
                                "],\"truncated\":true");
            free(rows);
            written += snprintf(buf + written, cap - written, "}");
            return written;
        }
        written += add;
    }
    written += snprintf(buf + written, cap - written, "],\"truncated\":false}");

    free(rows);
    return written;
}

/* Write the manifest to <data_dir>/_backup_manifest.json so it gets
 * picked up by the subsequent tar -cz pass.
 *
 * Returns 0, TSDB_BACKUP_MANIFEST_FAILED (the aid is missing; a backup can
 * still proceed) or TSDB_BACKUP_NOT_ON_DISK (the flush below failed, so a tar
 * of this data dir is NOT a copy of this database).  See db.h. */
int tsdb_backup_emit_manifest_file(tsdb_db_t *db, const char *data_dir) {
    if (!db || !data_dir || !data_dir[0]) return TSDB_BACKUP_MANIFEST_FAILED;

    /* Flush every memtable to disk first.  Otherwise the subsequent
     * tar misses any rows still buffered in RAM — a /backup taken
     * during light ingest would silently drop the tail.  Caught by
     * tests/e2e/backup_restore.sh phase 7: 100-row seed showed 0 on
     * a freshly-restored sidecar before this flush call.
     *
     * The rc was discarded here with a (void).  db.h has promised since
     * 90c4bc73 that the first failure is RETURNED precisely so a caller that
     * asked for its rows on disk can find out they are not, and restore.c's
     * export path names THIS call as the one that must not imitate it.  A
     * failed flush under TSDB_WAL_ONLY_COMMIT (the cluster's mode) means the
     * rows are in the redo log and in RAM and in no partition file — while the
     * manifest written below is built from tsdb_query, which reads the
     * memtable, so it would record counts the tarball cannot contain and the
     * restore verify would blame the restore. */
    int frc = tsdb_db_flush_all(db);
    if (frc != TSDB_OK) {
        tsdb_metric_inc("qengine_backup_flush_fail_total");
        fprintf(stderr,
                "[backup] refusing: tsdb_db_flush_all rc=%d (%s) — rows are "
                "not in the partition files, a tar of %s would be incomplete\n",
                frc, tsdb_errstr(frc), data_dir);
        return TSDB_BACKUP_NOT_ON_DISK;
    }

    char path[4096];
    snprintf(path, sizeof(path), "%s/_backup_manifest.json", data_dir);

    /* Generous buffer: 150 B per table × 4096 = ~600 KB headroom. */
    size_t cap = 1024 * 1024;
    char *buf = (char *)malloc(cap);
    if (!buf) return -1;
    int n = tsdb_backup_write_manifest_json(db, buf, cap);
    if (n <= 0) { free(buf); return -1; }

    FILE *fp = fopen(path, "w");
    if (!fp) { free(buf); return -1; }
    size_t wr = fwrite(buf, 1, (size_t)n, fp);
    fclose(fp);
    free(buf);
    return (wr == (size_t)n) ? 0 : -1;
}
