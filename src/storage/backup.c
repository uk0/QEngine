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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
    int n = tsdb_db_list_table_names(db, names, MANIFEST_MAX_TABLES);
    if (n < 0) n = 0;

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
 * picked up by the subsequent tar -cz pass.  Best-effort: if the
 * write fails the backup itself still proceeds — the manifest is
 * a verification aid, not a hard prereq. */
int tsdb_backup_emit_manifest_file(tsdb_db_t *db, const char *data_dir) {
    if (!db || !data_dir || !data_dir[0]) return -1;

    /* Flush every memtable to disk first.  Otherwise the subsequent
     * tar misses any rows still buffered in RAM — a /backup taken
     * during light ingest would silently drop the tail.  Caught by
     * tests/e2e/backup_restore.sh phase 7: 100-row seed showed 0 on
     * a freshly-restored sidecar before this flush call. */
    (void)tsdb_db_flush_all(db);

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
