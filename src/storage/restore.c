/* restore.c — backup sets and restore-into-a-live-database.
 *
 * See include/tsdb_restore.h for the contract and for why each discipline is
 * here.  This file is the mechanism.
 *
 * A backup set reuses the migration SDK's stream format verbatim (migrate.c),
 * one stream per table, because that format already carries the two things a
 * physical tar does not have to think about and a logical restore does: the
 * schema, and the SYMBOL dictionaries whose codes the blocks are meaningless
 * without.  What restore adds on top of tsdb_migrate_import is the part a
 * BACKUP needs and a migration does not:
 *
 *   - a completion marker, so an interrupted restore is detectable;
 *   - ts landed last across the WHOLE stream, not just within a partition,
 *     so an interruption is always a "behind", never a "torn";
 *   - dedup keyed on what the reader pairs on, occurrence for occurrence;
 *   - per-partition max_seq carried across, so the restored partition keeps
 *     its WAL redo checkpoint;
 *   - a report instead of an abort on the first bad table.
 */
#define _POSIX_C_SOURCE 200809L

#include "../../include/tsdb_restore.h"
#include "../../include/tsdb_migrate.h"

#include "db.h"
#include "part.h"
#include "schema.h"
#include "migrate_internal.h"
#include "../cluster/rawblock.h"
#include "../core/types.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

extern int tsdb_mkdir_p(const char *path);

#define RST_MAX_TABLES 4096

/* ---- small io helpers ---------------------------------------------------- */

static int rst_read_all(int fd, void *buf, size_t n, int *got_eof) {
    uint8_t *p = (uint8_t *)buf;
    size_t want = n;
    if (got_eof) *got_eof = 0;
    while (n) {
        ssize_t r = read(fd, p, n);
        if (r < 0) { if (errno == EINTR) continue; return TSDB_ERR_IO; }
        if (r == 0) {
            if (got_eof && n == want) { *got_eof = 1; return TSDB_OK; }
            return TSDB_ERR_CORRUPT;
        }
        p += (size_t)r; n -= (size_t)r;
    }
    return TSDB_OK;
}

static uint32_t rst_get_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void rst_fsync_dir(const char *dir) {
    int fd = open(dir, O_RDONLY);
    if (fd >= 0) { (void)fsync(fd); close(fd); }
}

/* Write `len` bytes to `path` via tmp + fsync + rename, then fsync the parent
 * directory — the same publish discipline every other writer in this tree
 * uses, so a crash sees either the old file or the new one and never a
 * half-written manifest that parses. */
static int rst_write_file_atomic(const char *dir, const char *leaf,
                                 const void *buf, size_t len)
{
    char path[4096], tmp[4200];
    snprintf(path, sizeof(path), "%s/%s", dir, leaf);
    snprintf(tmp,  sizeof(tmp),  "%s/%s.tmp", dir, leaf);

    int fd = open(tmp, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) return TSDB_ERR_IO;
    const uint8_t *p = (const uint8_t *)buf;
    size_t n = len;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) { if (errno == EINTR) continue; close(fd); unlink(tmp); return TSDB_ERR_IO; }
        p += (size_t)w; n -= (size_t)w;
    }
    if (fsync(fd) != 0) { close(fd); unlink(tmp); return TSDB_ERR_IO; }
    close(fd);
    if (rename(tmp, path) != 0) { unlink(tmp); return TSDB_ERR_IO; }
    rst_fsync_dir(dir);
    return TSDB_OK;
}

static int rst_read_file(const char *path, char **out, size_t *out_len) {
    *out = NULL; *out_len = 0;
    FILE *f = fopen(path, "rb");
    if (!f) return TSDB_ERR_NOTFOUND;
    if (fseeko(f, 0, SEEK_END) != 0) { fclose(f); return TSDB_ERR_IO; }
    off_t sz = ftello(f);
    if (sz < 0 || fseeko(f, 0, SEEK_SET) != 0) { fclose(f); return TSDB_ERR_IO; }
    char *b = (char *)malloc((size_t)sz + 1);
    if (!b) { fclose(f); return TSDB_ERR_NOMEM; }
    if (sz > 0 && fread(b, 1, (size_t)sz, f) != (size_t)sz) {
        free(b); fclose(f); return TSDB_ERR_IO;
    }
    b[sz] = '\0';
    fclose(f);
    *out = b; *out_len = (size_t)sz;
    return TSDB_OK;
}

/* ---- manifest ------------------------------------------------------------
 *
 * JSON so an operator (and the dashboard) can read it, but parsed by the tiny
 * scanner below rather than a JSON library: we are the only writer, the keys
 * are emitted in a fixed order, and table names are identifiers, so there is
 * nothing to escape and nothing to guess. */

typedef struct {
    char     name[64];
    uint64_t rows;
    uint64_t blocks;
    uint64_t digest;
    int      hour_partitioned;
} rst_manifest_row_t;

/* Advance *p past one table object, filling `row`.  Returns 1 on a hit. */
static int rst_manifest_next(const char **p, rst_manifest_row_t *row) {
    static const char KN[] = "\"name\":\"";
    const char *at = strstr(*p, KN);
    if (!at) return 0;
    at += sizeof(KN) - 1;

    size_t k = 0;
    while (at[k] && at[k] != '"' && k < sizeof(row->name) - 1) { row->name[k] = at[k]; k++; }
    row->name[k] = '\0';
    if (!at[k]) return 0;

    /* Bound every numeric lookup by the start of the NEXT object so a missing
     * key reads as absent rather than borrowing its neighbour's value. */
    const char *end = strstr(at + k, KN);
    row->rows = row->blocks = row->digest = 0;
    row->hour_partitioned = 0;

    static const struct { const char *key; int which; } KEYS[] = {
        { "\"rows\":",   0 }, { "\"blocks\":", 1 },
        { "\"digest\":", 2 }, { "\"hour_partitioned\":", 3 },
    };
    for (size_t i = 0; i < sizeof(KEYS) / sizeof(KEYS[0]); i++) {
        const char *h = strstr(at + k, KEYS[i].key);
        if (!h || (end && h >= end)) continue;
        h += strlen(KEYS[i].key);
        uint64_t v = strtoull(h, NULL, 10);
        switch (KEYS[i].which) {
            case 0: row->rows   = v; break;
            case 1: row->blocks = v; break;
            case 2: row->digest = v; break;
            default: row->hour_partitioned = (v != 0); break;
        }
    }
    *p = end ? end : (at + k);
    return 1;
}

static int rst_manifest_load(const char *backup_dir,
                             rst_manifest_row_t **out, int *out_n)
{
    *out = NULL; *out_n = 0;
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", backup_dir, TSDB_BACKUP_MANIFEST);
    char *buf = NULL; size_t len = 0;
    int rc = rst_read_file(path, &buf, &len);
    if (rc != TSDB_OK) return rc;

    rst_manifest_row_t *v = NULL; int n = 0, cap = 0;
    const char *p = buf;
    rst_manifest_row_t row;
    while (rst_manifest_next(&p, &row)) {
        if (n == cap) {
            int nc = cap ? cap * 2 : 16;
            rst_manifest_row_t *nv = (rst_manifest_row_t *)realloc(v, (size_t)nc * sizeof(*nv));
            if (!nv) { free(v); free(buf); return TSDB_ERR_NOMEM; }
            v = nv; cap = nc;
        }
        v[n++] = row;
    }
    free(buf);
    *out = v; *out_n = n;
    return TSDB_OK;
}

/* ---- table enumeration --------------------------------------------------- */

static int rst_is_table_dir(const char *dd, const char *name) {
    if (!name || name[0] == '.' || name[0] == '_') return 0;
    char p[4200];
    snprintf(p, sizeof(p), "%s/%s/schema.bin", dd, name);
    struct stat st;
    return (stat(p, &st) == 0 && S_ISREG(st.st_mode));
}

/* Every table that exists ON DISK, across every striped data dir.  "Open in
 * this process" is a different and much smaller set — a table nobody touched
 * since boot is still data that has to be backed up. */
static int rst_list_tables(tsdb_db_t *db, char (*names)[64], int max) {
    int n = 0;
    int ndirs = tsdb_db_data_dir_count(db);
    if (ndirs <= 0) ndirs = 1;
    for (int di = 0; di < ndirs; di++) {
        const char *dd = tsdb_db_data_dir_at(db, di);
        if (!dd || !dd[0]) dd = tsdb_db_data_dir(db);
        if (!dd) continue;
        DIR *d = opendir(dd);
        if (!d) continue;
        struct dirent *e;
        while ((e = readdir(d)) && n < max) {
            if (!rst_is_table_dir(dd, e->d_name)) continue;
            int dup = 0;
            for (int i = 0; i < n; i++) if (strcmp(names[i], e->d_name) == 0) { dup = 1; break; }
            if (dup) continue;
            snprintf(names[n], 64, "%s", e->d_name);
            n++;
        }
        closedir(d);
    }
    /* Deterministic order so two backups of the same db produce comparable
     * manifests. */
    for (int i = 1; i < n; i++) {
        char k[64]; snprintf(k, sizeof(k), "%s", names[i]);
        int j = i - 1;
        while (j >= 0 && strcmp(names[j], k) > 0) {
            snprintf(names[j + 1], 64, "%s", names[j]); j--;
        }
        snprintf(names[j + 1], 64, "%s", k);
    }
    return n;
}

static int rst_is_part_name(const char *s) {
    size_t k = strlen(s);
    if (k != 8 && k != 10) return 0;
    for (size_t i = 0; i < k; i++) if (s[i] < '0' || s[i] > '9') return 0;
    return 1;
}

/* ---- striped-deployment refusal ------------------------------------------
 *
 * With TSDB_DATA_DIRS set, tsdb_create_table places a table on
 * data_dirs[hash(name) % N] (db.c) — but every reader and writer in this file
 * addresses the database through tsdb_db_data_dir(), which is dir 0.  Both
 * directions of that mismatch are a SILENT wrong answer, so both are refused:
 *
 *   BACKUP.  rst_list_tables enumerates every stripe, so the manifest NAMES
 *   the tables living off dir 0 while tsdb_migrate_export read nothing for
 *   them: rc=0, complete=1, rows=0.  In a manifest, rows=0 is
 *   indistinguishable from an empty table, and tsdb_restore_verify agrees at
 *   BOTH levels because tsdb_migrate_digest is blind the same way (0 == 0).
 *   Measured on a 3-dir/12-table database: 8000 rows absent from a set that
 *   reported itself complete.
 *
 *   RESTORE.  tsdb_create_table puts the new table on its hash stripe while
 *   tsdb_rawblock_apply writes every block into dir 0 — the blocks land
 *   beside a table that is not there, count(*) answers 0 after an rc=0
 *   restore, and the deep verify reads dir 0 too, so it certifies it.
 *
 * Refusing costs a striped operator the logical backup API (the physical
 * /backup tarball still covers them) and costs single-dir deployments
 * nothing: tsdb_db_data_dir_count() is 1 when TSDB_DATA_DIRS is unset.
 * Resolving each table's real dir for export/digest/land is the right long-
 * term fix; until then the failure has to be loud.
 */
static int rst_reject_striped(tsdb_db_t *db, const char *what) {
    if (tsdb_db_data_dir_count(db) <= 1) return TSDB_OK;
    fprintf(stderr, "[restore] %s REFUSED: this database stripes tables across "
                    "%d data dirs (TSDB_DATA_DIRS), and the backup/restore path "
                    "reads and writes data dir 0 only — a set that silently "
                    "omits every table on another stripe is worse than no set\n",
            what, tsdb_db_data_dir_count(db));
    return TSDB_ERR_UNSUPPORTED;
}

/* ---- backup -------------------------------------------------------------- */

/* Per-partition idx-header facts the stream has no field for, one
 * "<partname> <max_seq> <ncols>" per line.  Bumping the stream version would
 * break every reader that already speaks v2, so they travel beside it.
 *
 *   max_seq  the WAL redo checkpoint.  Dropping it silently downgrades a
 *            restored partition's idx from v4 to v3 and re-arms WAL replay for
 *            records the partition already contains.
 *   ncols    the SOURCE's column-count stamp (idx header bytes [10..11]), read
 *            from the TS column because that is the one the reader consults
 *            (part.c: tsdb_part_open takes ts_ncols_stamp from ts alone).  It
 *            is rule 2 of part_col_absence_is_late_add — the only evidence that
 *            separates a column whose blocks are GONE from one ALTER-added
 *            later — and rule 1 is vacuous for the LAST column, so without it a
 *            restored trailing column with no blocks reads back as fabricated
 *            zeros with rc=0 where the source answers TSDB_ERR_CORRUPT.
 *            TSDB_IDX_NCOLS_UNKNOWN (0) is a real answer here: it means the
 *            source never asserted a count either.
 *
 * A set written before the ncols field existed has two fields per line and is
 * read back as UNKNOWN — see rst_apply_seq_sidecar. */
static int rst_write_seq_sidecar(tsdb_db_t *db, const char *table,
                                 tsdb_schema_t *s, const char *backup_dir,
                                 int *out_hour_partitioned)
{
    *out_hour_partitioned = 0;
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", tsdb_db_data_dir(db), table);

    size_t cap = 8192, len = 0;
    char *buf = (char *)malloc(cap);
    if (!buf) return TSDB_ERR_NOMEM;
    buf[0] = '\0';

    DIR *d = opendir(tbl_dir);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (!rst_is_part_name(e->d_name)) continue;
            if (strlen(e->d_name) == 10) *out_hour_partitioned = 1;
            char part_dir[4300];
            snprintf(part_dir, sizeof(part_dir), "%s/%s", tbl_dir, e->d_name);
            uint64_t seq = tsdb_part_max_seq(s, part_dir);
            uint16_t ncols = TSDB_IDX_NCOLS_UNKNOWN;
            if (s->ts_col_idx >= 0 && s->ts_col_idx < s->ncols) {
                char ts_idx[4500];
                snprintf(ts_idx, sizeof(ts_idx), "%s/%s.idx",
                         part_dir, s->cols[s->ts_col_idx].name);
                ncols = tsdb_part_idx_ncols(ts_idx);
            }
            char line[160];
            int m = snprintf(line, sizeof(line), "%s %llu %u\n",
                             e->d_name, (unsigned long long)seq,
                             (unsigned)ncols);
            if (m < 0) continue;
            if (len + (size_t)m + 1 > cap) {
                size_t nc = cap * 2 + (size_t)m + 1;
                char *nb = (char *)realloc(buf, nc);
                if (!nb) { free(buf); closedir(d); return TSDB_ERR_NOMEM; }
                buf = nb; cap = nc;
            }
            memcpy(buf + len, line, (size_t)m);
            len += (size_t)m;
            buf[len] = '\0';
        }
        closedir(d);
    }

    char leaf[128];
    snprintf(leaf, sizeof(leaf), "%s.seq", table);
    int rc = rst_write_file_atomic(backup_dir, leaf, buf, len);
    free(buf);
    return rc;
}

int tsdb_backup_create(tsdb_db_t *db, const char *backup_dir,
                       tsdb_restore_report_t *out)
{
    if (out) memset(out, 0, sizeof(*out));
    if (!db || !backup_dir || !backup_dir[0]) return TSDB_ERR_INVAL;
    /* Before the directory is created, so a refusal leaves nothing behind
     * that could be mistaken for a partial set. */
    int srej = rst_reject_striped(db, "backup");
    if (srej != TSDB_OK) return srej;
    if (tsdb_mkdir_p(backup_dir) < 0) return TSDB_ERR_IO;

    char (*names)[64] = (char (*)[64])calloc(RST_MAX_TABLES, sizeof(*names));
    if (!names) return TSDB_ERR_NOMEM;
    int n = rst_list_tables(db, names, RST_MAX_TABLES);

    /* Rows still in a memtable are not on disk, and the streams are read from
     * disk.  Under deferred flush (the cluster's mode) that is most of the
     * recent data.  backup.c's manifest emitter ignores this call's rc; a
     * BACKUP must not — "we could not put your rows on disk" is exactly the
     * thing an operator has to hear before they trust the set.
     *
     * THE SET TO FLUSH IS THE SET TO BACK UP — every table ON DISK.
     * tsdb_db_flush_all walks db->tables, i.e. only the tables THIS PROCESS has
     * opened, while rst_list_tables enumerates the directory.  The difference
     * used to be exported unflushed: under TSDB_WAL_ONLY_COMMIT a commit is a
     * WAL fsync plus a memtable insert, so a table written before an unclean
     * shutdown and untouched since reopen has every one of its acked rows in
     * the redo log and none of them in a partition.  Backing that node up
     * before touching anything — the first move of every DR runbook there is —
     * wrote rows=0 into a manifest that said complete=1, and restoring it gave
     * back an empty table with rc=0 at every step.  Measured: 1000 acked rows,
     * complete=1, restored count(*) = 0; a SECOND backup on the same handle
     * moments later reported 1000, because the first one's own export had
     * opened the table.
     *
     * So open them first.  tsdb_open_table replays the redo log into a fresh
     * memtable (that is what made backup #2 correct), and the flush below then
     * reaches it.  (void) rather than a hard return keeps this file's "report
     * instead of abort on the first bad table" discipline: a table that will
     * not open is reported by its own export a few lines down. */
    for (int i = 0; i < n; i++) {
        tsdb_table_t *th = NULL;
        (void)tsdb_open_table(db, names[i], &th);
    }
    int frc = tsdb_db_flush_all(db);
    if (frc != TSDB_OK) { free(names); return frc; }

    tsdb_restore_table_t *rows = (tsdb_restore_table_t *)
        calloc((size_t)(n > 0 ? n : 1), sizeof(*rows));
    int *hourp = (int *)calloc((size_t)(n > 0 ? n : 1), sizeof(*hourp));
    uint64_t *digest = (uint64_t *)calloc((size_t)(n > 0 ? n : 1), sizeof(*digest));
    if (!rows || !hourp || !digest) {
        free(names); free(rows); free(hourp); free(digest);
        return TSDB_ERR_NOMEM;
    }

    int first_err = TSDB_OK, nfailed = 0;

    for (int i = 0; i < n; i++) {
        snprintf(rows[i].table, sizeof(rows[i].table), "%s", names[i]);

        char leaf[128], tmp_path[4300], final_path[4300];
        snprintf(leaf, sizeof(leaf), "%s.tsm", names[i]);
        snprintf(tmp_path,   sizeof(tmp_path),   "%s/%s.tmp", backup_dir, leaf);
        snprintf(final_path, sizeof(final_path), "%s/%s",     backup_dir, leaf);

        int fd = open(tmp_path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) { rows[i].rc = TSDB_ERR_IO; goto tbl_done; }

        tsdb_mig_stats_t st;
        memset(&st, 0, sizeof(st));
        rows[i].rc = tsdb_migrate_export(db, names[i], fd, NULL, &st);
        if (rows[i].rc == TSDB_OK && fsync(fd) != 0) rows[i].rc = TSDB_ERR_IO;
        close(fd);
        if (rows[i].rc != TSDB_OK) { unlink(tmp_path); goto tbl_done; }
        if (rename(tmp_path, final_path) != 0) {
            unlink(tmp_path); rows[i].rc = TSDB_ERR_IO; goto tbl_done;
        }
        rows[i].blocks_seen = st.blocks;
        rows[i].rows        = st.rows;
        digest[i]           = st.digest;

        /* The checkpoint sidecar needs the live schema. */
        {
            tsdb_table_t *th = NULL;
            int orc = tsdb_open_table(db, names[i], &th);
            if (orc != TSDB_OK) { rows[i].rc = orc; goto tbl_done; }
            tsdb_table_internal_t *ti = tsdb_db_find_table(db, names[i]);
            tsdb_schema_t *s = ti ? tsdb_tbl_schema(ti) : NULL;
            if (!s) { rows[i].rc = TSDB_ERR_INTERNAL; goto tbl_done; }
            rows[i].rc = rst_write_seq_sidecar(db, names[i], s, backup_dir, &hourp[i]);
        }

        /* HOUR granularity is refused HERE, not only in tsdb_restore_run.
         *
         * rst_write_seq_sidecar has just walked the partition directories, so
         * this is the first moment the answer is known — and it is the last
         * moment it is still useful.  Learning at RESTORE time that the set
         * was never restorable means learning it during the incident, from a
         * set the operator has been trusting since it was taken.  The manifest
         * is written only when nfailed == 0, so this refusal makes the whole
         * set unrestorable-by-construction rather than shipping one that
         * reports complete=1 and refuses table by table on the way back in. */
        if (rows[i].rc == TSDB_OK && hourp[i]) {
            fprintf(stderr, "[restore] backup of '%s' REFUSED: it is "
                            "HOUR-partitioned, and the raw-block applier "
                            "rebuilds a partition directory from the DAY form "
                            "of the key — all 24 hourly partitions would land "
                            "in one directory on the way back\n", names[i]);
            rows[i].rc = TSDB_ERR_UNSUPPORTED;
        }
tbl_done:
        if (rows[i].rc != TSDB_OK) {
            nfailed++;
            if (first_err == TSDB_OK) first_err = rows[i].rc;
        }
    }
    free(names);

    /* The manifest goes last and only when everything else is on disk: its
     * absence is what makes an incomplete set unrestorable by accident. */
    int rc = first_err;
    if (nfailed == 0) {
        struct timespec tv;
        clock_gettime(CLOCK_REALTIME, &tv);
        long long now_ns = (long long)tv.tv_sec * 1000000000LL + tv.tv_nsec;

        size_t cap = 512 + (size_t)n * 256;
        char *mf = (char *)malloc(cap);
        if (!mf) { free(rows); free(hourp); free(digest); return TSDB_ERR_NOMEM; }
        size_t w = (size_t)snprintf(mf, cap,
            "{\"format_version\":1,\"created_at_ns\":%lld,\"table_count\":%d,"
            "\"tables\":[", now_ns, n);
        for (int i = 0; i < n; i++) {
            w += (size_t)snprintf(mf + w, cap - w,
                "%s{\"name\":\"%s\",\"rows\":%llu,\"blocks\":%llu,"
                "\"digest\":%llu,\"hour_partitioned\":%d}",
                i ? "," : "", rows[i].table,
                (unsigned long long)rows[i].rows,
                (unsigned long long)rows[i].blocks_seen,
                (unsigned long long)digest[i], hourp[i]);
        }
        w += (size_t)snprintf(mf + w, cap - w, "]}");
        rc = rst_write_file_atomic(backup_dir, TSDB_BACKUP_MANIFEST, mf, w);
        free(mf);
    }

    if (out) {
        out->ntables  = n;
        out->nfailed  = nfailed;
        out->complete = (rc == TSDB_OK);
        out->tables   = rows;
    } else {
        free(rows);
    }
    free(hourp); free(digest);
    return rc;
}

/* ---- landing dedup -------------------------------------------------------
 *
 * THE SILENT-DUPLICATION PATH THIS CLOSES.
 *
 * tsdb_rawblock_apply skips a block only when it matches the LAST entry of the
 * target's index, and matches it on (size, count, ts_min, ts_max).  For
 * replication that is enough — blocks arrive in order and only the newest can
 * be a repeat.  For a restore it is not, twice over:
 *
 *   1. Tail only.  Replaying a stream re-offers block 0 while the tail is
 *      block N-1, so nothing matches and every block appends a second time.  A
 *      partition with k blocks per column comes back with 2k, and because the
 *      duplicates are in <ts>.idx the rows are COUNTED twice — SELECT count(*)
 *      doubles and every aggregate is wrong, with no error anywhere.  This is
 *      reachable from the two things a restore is for: resuming an interrupted
 *      run, and re-running one that already finished.
 *
 *   2. `size` is in the key.  The compressed length is not what the reader
 *      pairs on, and it is not stable: the outer-LZ gate is measured per block
 *      and per build (see the lzlite work), so the same rows re-encoded by a
 *      different binary land on a different size.  A block that IS the block
 *      the target already has then fails the equality test and appends.
 *
 * The reader pairs a non-ts column to its ts block by FIRST-MATCH on
 * (ts_min, count) — exec.c:1978 and :6124 — so that pair, and nothing else, is
 * what "the target already has this block" has to mean.  A second block with a
 * key the column already carries is unreachable for pairing by construction;
 * in <ts>.idx it is double-counted rows, in a value column it is dead bytes.
 *
 * Occurrence counting rather than a plain set: a source that legitimately
 * holds two blocks with the same (ts_min, count) must come back with two.
 * Priming records HOW MANY the target has and each landing consumes one, so a
 * half-finished run resumes to exactly the source's multiplicity — no
 * duplicate, and no dropped block either.
 */
typedef struct {
    uint32_t day;
    uint16_t col;
    int64_t  ts_min;
    uint32_t count;
    uint32_t avail;      /* occurrences on the target not yet accounted for */
} rst_sig_t;

/* Scoped to ONE (partition, column): the stream is partition-major then
 * column-major, so a group is contiguous and the table never grows past one
 * column's block count.  Re-entering a group already processed is harmless —
 * re-priming reads back exactly what was landed. */
typedef struct {
    uint32_t   day;
    uint16_t   col;
    int        have;
    rst_sig_t *v; size_t n, cap;
} rst_seen_t;

static void rst_seen_free(rst_seen_t *sn) {
    free(sn->v); memset(sn, 0, sizeof(*sn));
}

static void rst_seen_bump(rst_seen_t *sn, int64_t ts_min, uint32_t count)
{
    for (size_t i = 0; i < sn->n; i++) {
        if (sn->v[i].ts_min == ts_min && sn->v[i].count == count) {
            sn->v[i].avail++;
            return;
        }
    }
    if (sn->n == sn->cap) {
        size_t nc = sn->cap ? sn->cap * 2 : 256;
        rst_sig_t *nv = (rst_sig_t *)realloc(sn->v, nc * sizeof(*nv));
        if (!nv) return;
        sn->v = nv; sn->cap = nc;
    }
    sn->v[sn->n].ts_min = ts_min;
    sn->v[sn->n].count  = count;
    sn->v[sn->n].avail  = 1;
    sn->n++;
}

/* Consume one occurrence of (ts_min,count).  1 = the target already has this
 * block and landing it again would duplicate; 0 = it must be landed. */
static int rst_seen_take(rst_seen_t *sn, int64_t ts_min, uint32_t count)
{
    for (size_t i = 0; i < sn->n; i++) {
        if (sn->v[i].ts_min == ts_min && sn->v[i].count == count &&
            sn->v[i].avail > 0) {
            sn->v[i].avail--;
            return 1;
        }
    }
    return 0;
}

/* Switch to (day, col), loading that column's existing (ts_min,count) keys.
 *
 * The idx is read DIRECTLY rather than through tsdb_part_open: that path
 * reconciles a partition whose columns are uneven and synthesises entries for
 * a column whose .idx does not exist yet — which is precisely the state an
 * interrupted restore leaves behind, and it would make every column look like
 * a duplicate of the first. */
static void rst_seen_switch(rst_seen_t *sn, const char *data_dir,
                            const char *table, tsdb_schema_t *s,
                            uint32_t day, uint16_t col)
{
    if (sn->have && sn->day == day && sn->col == col) return;
    sn->n = 0; sn->day = day; sn->col = col; sn->have = 1;

    if (col >= (uint16_t)s->ncols) return;
    char idx_path[4400];
    snprintf(idx_path, sizeof(idx_path), "%s/%s/%08u/%s.idx",
             data_dir, table, day, s->cols[col].name);

    uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(idx_path, NULL, &cnt, &esz, NULL, NULL, NULL, NULL);
    if (hsz <= 0 || cnt == 0 || esz < 24) return;

    FILE *f = fopen(idx_path, "rb");
    if (!f) return;
    uint8_t *e = (uint8_t *)malloc(esz);
    if (e) {
        for (uint32_t i = 0; i < cnt; i++) {
            if (fseeko(f, (off_t)hsz + (off_t)i * esz, SEEK_SET) != 0) break;
            if (fread(e, 1, esz, f) != esz) break;
            uint32_t bcount = rst_get_u32(e + 12);
            uint64_t tmin = 0;
            for (int k = 7; k >= 0; k--) tmin = (tmin << 8) | e[16 + k];
            rst_seen_bump(sn, (int64_t)tmin, bcount);
        }
        free(e);
    }
    fclose(f);
}

/* ---- idx header carry: max_seq and the ncols stamp ------------------------
 *
 * The column count a restore is ENTITLED to claim for a partition it built,
 * when the source never claimed one itself.
 *
 * part.h states the rule: the schema's column count may be stamped ONLY by "a
 * writer that writes every column of the partition".  tsdb_rawblock_apply is
 * not that writer — it publishes one (column, block) per call, so it correctly
 * passes the value it read back from the file — but the RESTORE is: it is the
 * only step in the chain that sees the whole stream.  So it may assert the
 * claim, and only when it can check it: every column of the schema has to have
 * landed at least one block HERE.  A partition whose trailing column the stream
 * carried no block for is exactly the shape of a legitimate ALTER-added column,
 * and stamping it would turn a shipped feature into a permanent read error on
 * the restored copy while the source still answers zeros.
 *
 * Returns TSDB_IDX_NCOLS_UNKNOWN when the claim is not earned — the same value
 * the flush leaves on a partition nobody stamped, i.e. no claim at all. */
static uint16_t rst_earned_ncols(tsdb_schema_t *s, const char *part_dir)
{
    if (!s || s->ncols <= 0 || s->ncols > TSDB_MAX_COLS)
        return TSDB_IDX_NCOLS_UNKNOWN;
    for (int ci = 0; ci < s->ncols; ci++) {
        char p[4500];
        snprintf(p, sizeof(p), "%s/%s.idx", part_dir, s->cols[ci].name);
        uint32_t cnt = 0;
        int hsz = tsdb_part_idx_probe(p, NULL, &cnt, NULL, NULL, NULL, NULL, NULL);
        if (hsz <= 0 || cnt == 0) return TSDB_IDX_NCOLS_UNKNOWN;
    }
    return (uint16_t)s->ncols;
}

/*
 * Re-head one restored partition's <ts>.idx with the two facts the raw-block
 * applier could not carry: the source's durable checkpoint, and the source's
 * column-count stamp.
 *
 * max_seq is raised to the source's, never lowered, and the header version is
 * never downgraded.  tsdb_part_max_seq reads the TS column's stamp only (ts is
 * published last, so it is the one column whose checkpoint means "every column
 * of this partition is durable up to here"), and tsdb_part_open reads the ncols
 * stamp from ts for the same reason — so ts is the file to stamp, and only
 * after the whole table landed, or either value would be a claim the partition
 * cannot back.
 *
 * `src_ncols` is the SOURCE's stamp for this partition (UNKNOWN when the source
 * had none).  It wins outright: it is the claim the copy is reproducing, and
 * reproducing it is the whole job — a source that errors on a lost trailing
 * column must not turn into a copy that answers zeros.  Only when the source
 * asserted nothing does rst_earned_ncols get a say.
 *
 * Runs for EVERY partition of the set, not only the ones carrying a checkpoint:
 * gating it on max_seq != 0 left every partition of a default-mode (flush-on-
 * commit) restore unstamped, which is most of them. */
static int rst_stamp_max_seq(tsdb_schema_t *s, const char *part_dir,
                             uint64_t want_seq, uint16_t src_ncols)
{
    if (!s || s->ts_col_idx < 0 || s->ts_col_idx >= s->ncols) return TSDB_ERR_INVAL;

    char idx_path[4400];
    snprintf(idx_path, sizeof(idx_path), "%s/%s.idx",
             part_dir, s->cols[s->ts_col_idx].name);
    char tmp[4500];
    snprintf(tmp, sizeof(tmp), "%s.rsttmp", idx_path);

    tsdb_part_idx_lock(part_dir);
    int rc = TSDB_OK;
    char *raw = NULL; size_t raw_len = 0;
    uint8_t *outbuf = NULL;

    uint16_t ver = 0; uint32_t cnt = 0, esz = 0;
    uint64_t tot = 0, mseq = 0;
    int64_t fmn = 0, fmx = 0;
    uint16_t ncols_stamp = TSDB_IDX_NCOLS_UNKNOWN, cur_ncols = TSDB_IDX_NCOLS_UNKNOWN;
    uint64_t new_seq = 0;
    int hsz = tsdb_part_idx_probe(idx_path, &ver, &cnt, &esz, &tot, &fmn, &fmx, &mseq);
    if (hsz <= 0) { rc = (hsz == 0) ? TSDB_ERR_NOTFOUND : TSDB_ERR_CORRUPT; goto done; }

    /* The header written below always declares TSDB_IDX_ENTRY_SIZE (88) —
     * tsdb_part_write_idx_header has no other mode.  A legacy V1/V2 idx carries
     * 40-byte entries, so re-heading one re-interprets the whole entry array at
     * the wrong stride: measured, a partition holding 1000 rows read back as
     * count(*) == 0 with tsdb_restore_run returning TSDB_OK.  It is reachable
     * on exactly the mixed-version path the "old blocks stay readable"
     * invariant protects — a legacy partition dedup-skips every block, so
     * tsdb_rawblock_apply (which WOULD widen the entries to 88) never runs and
     * the stamp is the only writer to touch the file.
     *
     * Declining costs only the checkpoint and the ncols stamp, and in both
     * cases "unasserted" is the safe direction: too-low a checkpoint only
     * re-applies records the landing dedup already handles (see the comment
     * above rst_apply_seq_sidecar), and no ncols stamp leaves the reader at its
     * pre-stamp behaviour rather than making it stricter than the source. */
    if (cnt > 0 && esz != TSDB_IDX_ENTRY_SIZE) {
        fprintf(stderr, "[restore] %s: not re-heading this idx — it is v%u with "
                        "%u-byte entries, and the header this would write "
                        "declares %u-byte ones, so the WAL checkpoint and the "
                        "column-count stamp are both left alone\n",
                idx_path, ver, esz, (unsigned)TSDB_IDX_ENTRY_SIZE);
        goto done;
    }

    /* WHICH ncols STAMP GOES BACK.
     *
     * Bytes [10..11] of a V3/V4 header record how many columns the writer of
     * this partition knew about, and that number is not decoration: it is
     * rule 2 of part_col_absence_is_late_add, the ONLY evidence separating a
     * column whose blocks are GONE (read as an error) from one that was
     * ALTER-added later (zero-filled by design).  Rule 1 — "some later column
     * has blocks" — is vacuous for the last column, so for that column the
     * stamp is the only thing standing between a hole and fabricated zeros.
     *
     * Reading it back off the file being republished — which is what
     * rawblock.c does, correctly, and what this function used to do — yields
     * TSDB_IDX_NCOLS_UNKNOWN for a partition the restore itself just created,
     * because nothing in the chain had ever written one.  Every restored
     * partition therefore came out unstamped and a trailing column whose blocks
     * the source had already lost read back as zeros with rc=0, where the
     * source answered TSDB_ERR_CORRUPT.  So the SOURCE's stamp is carried
     * beside max_seq in the .seq sidecar and written back here; when the source
     * asserted nothing, rst_earned_ncols may assert what this restore can
     * actually check it landed. */
    cur_ncols   = tsdb_part_idx_ncols(idx_path);
    ncols_stamp = (src_ncols != TSDB_IDX_NCOLS_UNKNOWN)
                      ? src_ncols : rst_earned_ncols(s, part_dir);
    if (ncols_stamp == TSDB_IDX_NCOLS_UNKNOWN) ncols_stamp = cur_ncols;
    new_seq = (want_seq > mseq) ? want_seq : mseq;     /* raised, never lowered */

    /* Nothing to change: do not rewrite the file.  The rewrite moves the
     * partition dir's mtime, which the compaction memo and the anti-entropy
     * COLD gate both read, so a no-op re-run must not touch it. */
    if (new_seq == mseq && ncols_stamp == cur_ncols) goto done;

    if (rst_read_file(idx_path, &raw, &raw_len) != TSDB_OK) { rc = TSDB_ERR_IO; goto done; }
    if (raw_len < (size_t)hsz) { rc = TSDB_ERR_CORRUPT; goto done; }

    size_t entries_len = raw_len - (size_t)hsz;
    outbuf = (uint8_t *)malloc(TSDB_IDX_HEADER_SIZE + entries_len);
    if (!outbuf) { rc = TSDB_ERR_NOMEM; goto done; }
    size_t nh = tsdb_part_write_idx_header(outbuf, cnt, tot, fmn, fmx, new_seq,
                                           ncols_stamp);
    memcpy(outbuf + nh, raw + hsz, entries_len);

    {
        int fd = open(tmp, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) { rc = TSDB_ERR_IO; goto done; }
        size_t total = nh + entries_len;
        const uint8_t *p = outbuf;
        while (total) {
            ssize_t w = write(fd, p, total);
            if (w < 0) { if (errno == EINTR) continue; close(fd); unlink(tmp); rc = TSDB_ERR_IO; goto done; }
            p += (size_t)w; total -= (size_t)w;
        }
        if (fsync(fd) != 0) { close(fd); unlink(tmp); rc = TSDB_ERR_IO; goto done; }
        close(fd);
        if (rename(tmp, idx_path) != 0) { unlink(tmp); rc = TSDB_ERR_IO; goto done; }
        rst_fsync_dir(part_dir);
    }
done:
    /* A crash between the write and the rename above leaves the tmp behind
     * and nothing else ever removes it — one per crashed restore, forever.
     * After a successful rename this is a no-op (ENOENT); on every early
     * return it collects the residue of the run that died here. */
    unlink(tmp);
    free(raw); free(outbuf);
    tsdb_part_idx_unlock(part_dir);
    return rc;
}

/* ---- landing one table's stream ------------------------------------------ */

typedef struct {
    int      ncols;
    int      ts_idx;
    char     name[64];
    off_t    records_off;      /* file offset of the first block record */
} rst_stream_hdr_t;

/* Parse header + column list + dictionary section and resolve the target
 * table.  for_write != 0 also CREATES a missing table and adopts the stream's
 * SYMBOL dictionaries; verify passes 0 so a check never mutates the database
 * it is checking. */
static int rst_open_stream(tsdb_db_t *db, int fd, const char *rename_to,
                           int for_write,
                           rst_stream_hdr_t *out, tsdb_schema_t **out_schema)
{
    tsdb_schema_t *tgt = NULL;
    uint8_t hdr[4 + 4 + TSDB_MIG_NAME_BYTES + 4 + 4];
    int rc = rst_read_all(fd, hdr, sizeof(hdr), NULL);
    if (rc != TSDB_OK) return rc;
    if (rst_get_u32(hdr) != TSDB_MIG_MAGIC)       return TSDB_ERR_CORRUPT;
    if (rst_get_u32(hdr + 4) != TSDB_MIG_VERSION) return TSDB_ERR_CORRUPT;

    char src_name[TSDB_MIG_NAME_BYTES + 1];
    memcpy(src_name, hdr + 8, TSDB_MIG_NAME_BYTES);
    src_name[TSDB_MIG_NAME_BYTES] = '\0';
    int ncols  = (int)rst_get_u32(hdr + 8 + TSDB_MIG_NAME_BYTES);
    int ts_idx = (int)rst_get_u32(hdr + 12 + TSDB_MIG_NAME_BYTES);
    if (ncols <= 0 || ncols > TSDB_MAX_COLS || ts_idx < 0 || ts_idx >= ncols)
        return TSDB_ERR_CORRUPT;

    const char *tname = (rename_to && rename_to[0]) ? rename_to : src_name;

    tsdb_col_t *cols = (tsdb_col_t *)calloc((size_t)ncols, sizeof(*cols));
    char (*names)[TSDB_MIG_NAME_BYTES] =
        (char (*)[TSDB_MIG_NAME_BYTES])calloc((size_t)ncols, TSDB_MIG_NAME_BYTES);
    if (!cols || !names) { free(cols); free(names); return TSDB_ERR_NOMEM; }
    for (int i = 0; i < ncols; i++) {
        uint8_t c[TSDB_MIG_NAME_BYTES + 4];
        rc = rst_read_all(fd, c, sizeof(c), NULL);
        if (rc != TSDB_OK) { free(cols); free(names); return rc; }
        c[TSDB_MIG_NAME_BYTES - 1] = '\0';
        snprintf(names[i], TSDB_MIG_NAME_BYTES, "%s", (const char *)c);
        cols[i].name = names[i];
        cols[i].type = (tsdb_type_t)rst_get_u32(c + TSDB_MIG_NAME_BYTES);
    }

    uint32_t nsym = 0;
    {
        uint8_t nb[4];
        rc = rst_read_all(fd, nb, 4, NULL);
        if (rc != TSDB_OK) { free(cols); free(names); return rc; }
        nsym = rst_get_u32(nb);
        if (nsym > (uint32_t)ncols) { free(cols); free(names); return TSDB_ERR_CORRUPT; }
    }
    uint32_t *sym_col = nsym ? (uint32_t *)calloc(nsym, sizeof(*sym_col)) : NULL;
    uint8_t **sym_buf = nsym ? (uint8_t **)calloc(nsym, sizeof(*sym_buf)) : NULL;
    size_t   *sym_len = nsym ? (size_t *)calloc(nsym, sizeof(*sym_len)) : NULL;
    if (nsym && (!sym_col || !sym_buf || !sym_len)) {
        free(cols); free(names); free(sym_col); free(sym_buf); free(sym_len);
        return TSDB_ERR_NOMEM;
    }
    for (uint32_t k = 0; k < nsym && rc == TSDB_OK; k++) {
        uint8_t hb[8];
        rc = rst_read_all(fd, hb, 8, NULL);
        if (rc != TSDB_OK) break;
        sym_col[k] = rst_get_u32(hb);
        uint32_t len = rst_get_u32(hb + 4);
        if (sym_col[k] >= (uint32_t)ncols || len > (64u << 20)) { rc = TSDB_ERR_CORRUPT; break; }
        if (len) {
            sym_buf[k] = (uint8_t *)malloc(len);
            if (!sym_buf[k]) { rc = TSDB_ERR_NOMEM; break; }
            rc = rst_read_all(fd, sym_buf[k], len, NULL);
            if (rc != TSDB_OK) break;
            sym_len[k] = len;
        }
    }
    if (rc != TSDB_OK) goto fail;

    /* Existence is an ON-DISK question, not "has this process opened it". */
    {
        tsdb_table_t *th = NULL;
        int orc = tsdb_open_table(db, tname, &th);
        if (orc != TSDB_OK && orc != TSDB_ERR_NOTFOUND) { rc = orc; goto fail; }
        if (orc == TSDB_ERR_NOTFOUND) {
            if (!for_write) { rc = TSDB_ERR_NOTFOUND; goto fail; }
            rc = tsdb_create_table(db, tname, cols, (size_t)ncols, cols[ts_idx].name);
            if (rc != TSDB_OK) goto fail;
        } else {
            /* Landing addresses columns by INDEX, so a disagreeing schema would
             * write one column's bytes into another's file. */
            tsdb_table_internal_t *ti = tsdb_db_find_table(db, tname);
            tsdb_schema_t *es = ti ? tsdb_tbl_schema(ti) : NULL;
            if (!es || es->ncols != ncols || es->ts_col_idx != ts_idx) {
                rc = TSDB_ERR_SCHEMA; goto fail;
            }
            for (int i = 0; i < ncols; i++) {
                if (strcmp(es->cols[i].name, cols[i].name) != 0 ||
                    es->cols[i].type != cols[i].type) { rc = TSDB_ERR_SCHEMA; goto fail; }
            }
        }
    }

    {
        tsdb_table_internal_t *ti = tsdb_db_find_table(db, tname);
        tgt = ti ? tsdb_tbl_schema(ti) : NULL;
    }
    if (!tgt) { rc = TSDB_ERR_INTERNAL; goto fail; }

    /* Dictionaries before any block: the blocks carry CODES and are meaningless
     * against a different dictionary.  The LIVE symtab has to agree, not just
     * the .sym file, because schema_save rewrites that file from memory. */
    for (uint32_t k = 0; k < nsym && rc == TSDB_OK && for_write; k++) {
        int ci = (int)sym_col[k];
        if (ci >= tgt->ncols || tgt->cols[ci].type != TSDB_TYPE_SYMBOL) continue;
        if (!sym_len[k]) continue;
        rc = tsdb_migrate_symtab_adopt(tgt, ci, sym_buf[k], sym_len[k]);
    }
    if (rc != TSDB_OK) goto fail;

    out->ncols  = ncols;
    out->ts_idx = ts_idx;
    snprintf(out->name, sizeof(out->name), "%s", tname);
    out->records_off = lseek(fd, 0, SEEK_CUR);
    *out_schema = tgt;

fail:
    for (uint32_t k = 0; k < nsym; k++) free(sym_buf[k]);
    free(sym_col); free(sym_buf); free(sym_len);
    free(cols); free(names);
    return rc;
}

/* ---- re-run gate --------------------------------------------------------
 *
 * THE SILENT-DUPLICATION PATH THIS ONE CLOSES, and why it is a REFUSAL.
 *
 * The dedup above keys on (ts_min, count) because that is what exec.c pairs
 * on.  That makes it the right key for RECOGNISING a block the reader already
 * sees — and the wrong key for a durable IDENTITY, because it is a BLOCK
 * BOUNDARY and this database rewrites block boundaries on purpose:
 *
 *   - compact_partition re-encodes every column of a cold partition into
 *     32768-row blocks.  It runs ts first and derives every other column's
 *     boundaries from ts, so the whole partition moves to one new layout;
 *   - tsdb_part_flush picks its boundaries off block_points.
 *
 * Neither is stable across a restore.  A second tsdb_restore_run over a
 * partition the DEFAULT compactor has touched therefore primes from 5 keys of
 * 32768 rows, is offered the stream's 150 keys of 1000 rows, recognises none
 * of them, and lands all 150 a second time: measured 150000 rows -> 300000,
 * sum(n) doubled with it, rc=0 and nothing reported.  tsdb_node_main starts
 * the compactor on a 5 s timer, so "the compactor has touched it" is the
 * steady state of every partition nobody is actively writing — not a corner.
 * Both durability modes.
 *
 * WHY NOT MATCH ON ts COVERAGE INSTEAD.  Asking "does the target's <ts>.idx
 * already cover [ts_min, ts_max] for `count` rows" does survive the boundary
 * rewrite.  It is unsafe twice over:
 *
 *   1. A raw block is an opaque compressed byte string; tsdb_rawblock_apply
 *      appends it verbatim and has no notion of a fraction of one.  There is
 *      no "land only the uncovered remainder" to fall back on, so under
 *      PARTIAL coverage landing duplicates the covered rows and skipping drops
 *      the uncovered ones.  Neither is right and there is no third option.
 *
 *   2. Coverage is not identity.  A timestamp is not a key in a time-series
 *      table — one row per (series, ts) means many rows share a ts, and a
 *      backfill flush writes a block whose range sits entirely inside an
 *      existing one.  Two ts blocks with the SAME [ts_min, ts_max] and the same
 *      count are an ordinary two-flush partition; coverage skips the second and
 *      the restore silently LOSES half the partition.  Turning a silent
 *      over-count into a silent under-count is not a fix.
 *
 * So this refuses, the way the striped and HOUR-granularity cases are refused.
 * Before ANY block lands, every partition the stream carries is checked: the
 * target's ts keys must be a SUB-MULTISET of the stream's.  That admits exactly
 * the two shapes a restore is for —
 *
 *     empty target     nothing to compare against; land everything.
 *     resume / re-run  the target holds a prefix (or all) of this stream's own
 *                      blocks, nobody rewrote them, and the skip path below is
 *                      exact, occurrence for occurrence.
 *
 * — and rejects the two it cannot do safely: a rewritten layout, and a
 * partition already holding rows this stream does not carry.  ts alone is
 * checked because a partition has ONE block layout shared by every column
 * (compact_partition's phase 2 swaps all columns or none, and the flush stamps
 * identical (ts_min,count) pairs across them), so ts diverging is ts diverging
 * for the partition.  A false alarm costs the operator one manual step —
 * restore into an empty data dir, then merge.  The behaviour it replaces cost
 * them a doubled count(*) they had no way to notice.
 */
typedef struct {
    int64_t  ts_min;
    uint32_t count;
    uint32_t avail;       /* occurrences the stream carries, not yet accounted */
} rst_tskey_t;

typedef struct {
    uint32_t     day;
    rst_tskey_t *v; size_t n, cap;
    uint64_t     blocks;     /* ts blocks the stream carries for this day */
    uint64_t     rows;       /* their total row count                      */
} rst_daykeys_t;

static void rst_dk_free(rst_daykeys_t *d, size_t n) {
    for (size_t i = 0; i < n; i++) free(d[i].v);
    free(d);
}

/* Record one ts block of the stream under its partition day. */
static int rst_dk_note(rst_daykeys_t **pv, size_t *pn, size_t *pcap,
                       uint32_t day, int64_t ts_min, uint32_t count)
{
    rst_daykeys_t *d = NULL;
    for (size_t i = 0; i < *pn; i++)
        if ((*pv)[i].day == day) { d = &(*pv)[i]; break; }
    if (!d) {
        if (*pn == *pcap) {
            size_t nc = *pcap ? *pcap * 2 : 8;
            rst_daykeys_t *nv = (rst_daykeys_t *)realloc(*pv, nc * sizeof(*nv));
            if (!nv) return TSDB_ERR_NOMEM;
            *pv = nv; *pcap = nc;
        }
        d = &(*pv)[(*pn)++];
        memset(d, 0, sizeof(*d));
        d->day = day;
    }
    d->blocks++;
    d->rows += count;
    for (size_t i = 0; i < d->n; i++)
        if (d->v[i].ts_min == ts_min && d->v[i].count == count) {
            d->v[i].avail++;
            return TSDB_OK;
        }
    if (d->n == d->cap) {
        size_t nc = d->cap ? d->cap * 2 : 64;
        rst_tskey_t *nv = (rst_tskey_t *)realloc(d->v, nc * sizeof(*nv));
        if (!nv) return TSDB_ERR_NOMEM;
        d->v = nv; d->cap = nc;
    }
    d->v[d->n].ts_min = ts_min;
    d->v[d->n].count  = count;
    d->v[d->n].avail  = 1;
    d->n++;
    return TSDB_OK;
}

/* 1 when the stream still carries an unaccounted (ts_min,count). */
static int rst_dk_consume(rst_daykeys_t *d, int64_t ts_min, uint32_t count) {
    for (size_t i = 0; i < d->n; i++)
        if (d->v[i].ts_min == ts_min && d->v[i].count == count &&
            d->v[i].avail > 0) {
            d->v[i].avail--;
            return 1;
        }
    return 0;
}

static void rst_refuse_rerun(const char *table, uint32_t day,
                             uint32_t tgt_blocks, uint64_t tgt_rows,
                             const rst_daykeys_t *d, const char *why)
{
    fprintf(stderr, "[restore] table '%s' partition %08u REFUSED: %s.  The "
                    "target holds %u ts block(s) / %llu rows; the stream "
                    "carries %llu block(s) / %llu rows for this partition.  A "
                    "restore recognises what it already landed by the "
                    "(ts_min,count) pair the reader pairs on, and this database "
                    "rewrites that pair on purpose — the compactor re-encodes a "
                    "cold partition into 32768-row blocks — so landing here "
                    "would append every block a SECOND time and double "
                    "count(*) with rc=0.  Restore into an empty data dir "
                    "instead.  (If tsdb_restore_verify ran against this "
                    "database and reported this table as drained, the flush it "
                    "took to answer is one of the writers that put these "
                    "boundaries on disk.)\n",
            table, day, why, tgt_blocks, (unsigned long long)tgt_rows,
            (unsigned long long)d->blocks, (unsigned long long)d->rows);
}

/* Read the target's ts blocks for one partition and check they are a
 * sub-multiset of what the stream carries.  Consumes `d`; the landing pass
 * re-primes from the target independently. */
static int rst_precheck_day(const char *data_dir, const char *table,
                            const char *ts_col, rst_daykeys_t *d)
{
    char idx_path[4400];
    snprintf(idx_path, sizeof(idx_path), "%s/%s/%08u/%s.idx",
             data_dir, table, d->day, ts_col);

    uint32_t cnt = 0, esz = 0;
    uint64_t tot = 0;
    int hsz = tsdb_part_idx_probe(idx_path, NULL, &cnt, &esz, &tot, NULL, NULL, NULL);
    if (hsz <= 0 || cnt == 0) return TSDB_OK;      /* nothing here — land it */

    /* Entries this narrow carry no (ts_min,count) to read, so the dedup below
     * would prime NOTHING and land every block on top of whatever is there. */
    if (esz < 24) {
        rst_refuse_rerun(table, d->day, cnt, tot, d,
                         "its <ts>.idx has entries too narrow to carry a "
                         "(ts_min,count) the landing dedup could match");
        return TSDB_ERR_UNSUPPORTED;
    }

    FILE *f = fopen(idx_path, "rb");
    if (!f) return TSDB_ERR_IO;
    uint8_t *e = (uint8_t *)malloc(esz);
    if (!e) { fclose(f); return TSDB_ERR_NOMEM; }

    int rc = TSDB_OK;
    const char *why = NULL;
    for (uint32_t i = 0; i < cnt; i++) {
        if (fseeko(f, (off_t)hsz + (off_t)i * esz, SEEK_SET) != 0 ||
            fread(e, 1, esz, f) != esz) {
            rc = TSDB_ERR_IO;
            break;
        }
        uint32_t bcount = rst_get_u32(e + 12);
        uint64_t tmin = 0;
        for (int k = 7; k >= 0; k--) tmin = (tmin << 8) | e[16 + k];
        if (!rst_dk_consume(d, (int64_t)tmin, bcount)) {
            why = "it already holds ts blocks whose (ts_min,count) keys this "
                  "stream does not carry — a rewritten block layout (the "
                  "compactor) or data that did not come from this backup set";
            break;
        }
    }
    free(e);
    fclose(f);

    if (why) {
        rst_refuse_rerun(table, d->day, cnt, tot, d, why);
        return TSDB_ERR_UNSUPPORTED;
    }
    return rc;
}

/* Walk the record area once, ts records only, and gate every partition it
 * names.  Called BEFORE pass 0 so a refusal has landed nothing. */
static int rst_precheck_rerun(tsdb_db_t *db, int fd, const rst_stream_hdr_t *h,
                              tsdb_schema_t *tgt)
{
    if (!tgt || tgt->ts_col_idx < 0 || tgt->ts_col_idx >= tgt->ncols)
        return TSDB_ERR_INTERNAL;

    rst_daykeys_t *dk = NULL; size_t ndk = 0, capdk = 0;
    uint8_t *buf = NULL; size_t cap = 0;
    int rc = TSDB_OK;

    for (;;) {
        uint8_t lb[4]; int eof = 0;
        rc = rst_read_all(fd, lb, 4, &eof);
        if (rc != TSDB_OK) break;
        if (eof) { rc = TSDB_ERR_CORRUPT; break; }
        uint32_t len = rst_get_u32(lb);
        if (len == 0) { rc = TSDB_OK; break; }
        if (len > (64u << 20)) { rc = TSDB_ERR_CORRUPT; break; }
        if (len > cap) {
            uint8_t *nb = (uint8_t *)realloc(buf, len);
            if (!nb) { rc = TSDB_ERR_NOMEM; break; }
            buf = nb; cap = len;
        }
        rc = rst_read_all(fd, buf, len, NULL);
        if (rc != TSDB_OK) break;

        tsdb_rawblock_push_t r;
        if (tsdb_rawblock_parse(buf, len, &r) != TSDB_OK) { rc = TSDB_ERR_CORRUPT; break; }
        if ((int)r.col_idx != h->ts_idx) continue;
        rc = rst_dk_note(&dk, &ndk, &capdk, r.part_day, r.ts_min, r.count);
        if (rc != TSDB_OK) break;
    }
    free(buf);

    const char *data_dir = tsdb_db_data_dir(db);
    const char *ts_col   = tgt->cols[tgt->ts_col_idx].name;
    for (size_t i = 0; i < ndk && rc == TSDB_OK; i++)
        rc = rst_precheck_day(data_dir, h->name, ts_col, &dk[i]);

    rst_dk_free(dk, ndk);
    return rc;
}

/* TEST-ONLY fault injection.  Both are getenv-gated and unset in production
 * (getenv returns NULL -> no-op), matching part.c's TSDB_TEST_CRASH_*.  They
 * _exit() rather than return an error on purpose: a crash test that unwinds
 * cleanly proves nothing about what a power cut leaves on disk. */
static void rst_crash_hook_blocks(uint64_t landed) {
    const char *e = getenv("TSDB_TEST_CRASH_RESTORE_AFTER_BLOCKS");
    if (e && e[0] && landed >= strtoull(e, NULL, 10)) _exit(99);
}
static void rst_crash_hook_before_ts(void) {
    const char *e = getenv("TSDB_TEST_CRASH_RESTORE_BEFORE_TS");
    if (e && e[0] && e[0] != '0') _exit(98);
}

/* One pass over the record area.  want_ts selects which half lands. */
static int rst_land_pass(tsdb_db_t *db, int fd, const rst_stream_hdr_t *h,
                         tsdb_schema_t *tgt, rst_seen_t *seen, int want_ts,
                         tsdb_restore_table_t *rep)
{
    const char *data_dir = tsdb_db_data_dir(db);
    uint8_t *buf = NULL; size_t cap = 0;
    int rc = TSDB_OK;

    for (;;) {
        uint8_t lb[4]; int eof = 0;
        rc = rst_read_all(fd, lb, 4, &eof);
        if (rc != TSDB_OK) break;
        if (eof) { rc = TSDB_ERR_CORRUPT; break; }    /* missing terminator */
        uint32_t len = rst_get_u32(lb);
        if (len == 0) { rc = TSDB_OK; break; }        /* clean end */
        if (len > (64u << 20)) { rc = TSDB_ERR_CORRUPT; break; }

        if (len > cap) {
            uint8_t *nb = (uint8_t *)realloc(buf, len);
            if (!nb) { rc = TSDB_ERR_NOMEM; break; }
            buf = nb; cap = len;
        }
        rc = rst_read_all(fd, buf, len, NULL);
        if (rc != TSDB_OK) break;

        tsdb_rawblock_push_t r;
        if (tsdb_rawblock_parse(buf, len, &r) != TSDB_OK) { rc = TSDB_ERR_CORRUPT; break; }
        snprintf(r.table, sizeof(r.table), "%s", h->name);

        int is_ts = ((int)r.col_idx == h->ts_idx);
        if (want_ts >= 0 && is_ts != want_ts) continue;
        rep->blocks_seen++;

        rst_seen_switch(seen, data_dir, h->name, tgt, r.part_day, r.col_idx);
        if (rst_seen_take(seen, r.ts_min, r.count)) {
            rep->blocks_skipped++;
            continue;
        }

        /* Deliberately the plain applier, not TSDB_RB_VERIFY_TS.  The two-pass
         * ordering above already guarantees every sibling has landed before
         * any ts block does, so the commit test would be redundant here — and
         * it is not free: tsdb_part_ts_publish_ready refuses a ts block whose
         * group is incomplete in a column that has SOME blocks in this
         * partition, which is exactly what an ALTER-added column looks like
         * once it starts receiving data.  A source in that (legal, readable)
         * shape would become unrestorable.  Restore reproduces the source; it
         * does not repair it. */
        int arc = tsdb_rawblock_apply(db, &r);
        if (arc != TSDB_OK) { rc = arc; break; }
        rep->blocks_landed++;
        if (is_ts) rep->rows += r.count;
        rst_crash_hook_blocks(rep->blocks_landed);
    }
    free(buf);
    return rc;
}

/* Apply the per-partition sidecar for one table, after every block landed:
 * the source's WAL checkpoint and the source's ncols stamp.
 *
 * THE CHECKPOINT IS REFUSED when the target's own WAL for this table is
 * non-empty.  It is the seq at or below which recovery SKIPS replay for that
 * partition (db.c redo_recover_table), and the source's sequence numbers come
 * from a different table's counter.  Stamping a foreign — possibly higher —
 * value over a target that still has un-flushed redo records would make the
 * next open skip records the partition does not contain: acked rows gone,
 * silently.  A checkpoint that is too LOW only re-applies records the landing
 * dedup already handles, so declining is the safe direction.  An empty WAL
 * cannot hide anything, and commit_seq continues above the stamped value
 * (redo_recover_table's `hi`), so future records are never masked either.
 *
 * THE ncols STAMP IS NOT refused there, and must not be: it says nothing about
 * sequence numbers and cannot mask a redo record.  Skipping the whole sidecar
 * for a target with its own WAL would leave exactly those partitions
 * unstamped — i.e. reading a lost trailing column back as fabricated zeros —
 * which is the defect this carries the stamp to close.  So a non-empty WAL
 * zeroes the checkpoint for every line and nothing else.
 *
 * A restorable set always has this file: tsdb_backup_create writes it per table
 * and a table whose sidecar failed makes nfailed != 0, which suppresses
 * BACKUP.manifest, which makes tsdb_restore_run refuse the set outright. */
static int rst_apply_seq_sidecar(tsdb_db_t *db, const char *backup_dir,
                                 const char *table, tsdb_schema_t *s,
                                 uint64_t *out_max)
{
    int allow_seq = 1;
    {
        char wal_path[4300];
        snprintf(wal_path, sizeof(wal_path), "%s/wal/%s.log",
                 tsdb_db_data_dir(db), table);
        struct stat wst;
        if (stat(wal_path, &wst) == 0 && wst.st_size > 0) {
            fprintf(stderr, "[restore] %s: not stamping the backup's WAL "
                            "checkpoints — this table has %lld bytes of its own "
                            "redo log, and a foreign checkpoint could mask it "
                            "(the column-count stamp still lands)\n",
                    table, (long long)wst.st_size);
            allow_seq = 0;
        }
    }

    char path[4300];
    snprintf(path, sizeof(path), "%s/%s.seq", backup_dir, table);
    char *buf = NULL; size_t len = 0;
    if (rst_read_file(path, &buf, &len) != TSDB_OK) return TSDB_OK;  /* optional */

    int rc = TSDB_OK;
    uint64_t stamped_hi = 0;
    char *save = NULL;
    for (char *line = strtok_r(buf, "\n", &save); line;
         line = strtok_r(NULL, "\n", &save)) {
        char pname[32]; unsigned long long seq = 0; unsigned nc = 0;
        int nf = sscanf(line, "%31s %llu %u", pname, &seq, &nc);
        /* Two fields is a set written before the ncols stamp travelled beside
         * max_seq: read it back as "the source asserted nothing", which is
         * exactly what such a set can tell us. */
        if (nf < 2) continue;
        /* Bounded by THIS TABLE's column count, not by TSDB_MAX_COLS.  The
         * stamp is read back as "the writer of this partition knew about N
         * columns", and rule 2 of part_col_absence_is_late_add refuses to
         * zero-fill any column whose index is below it — so a stamp above
         * s->ncols marks EVERY column of the partition as one whose blocks are
         * gone, turning the whole partition into a read error.  The stream's
         * schema is required to match the target's (rst_open_stream), so a line
         * claiming more than that is a foreign or damaged sidecar and the
         * unasserted value is the safe reading of it. */
        if (nf < 3 || nc > (unsigned)s->ncols) nc = TSDB_IDX_NCOLS_UNKNOWN;
        if (!rst_is_part_name(pname)) continue;

        /* The applier rebuilds the partition dir from the DAY form, so stamp
         * the directory the blocks actually landed in. */
        uint32_t day = (uint32_t)strtoul(pname, NULL, 10);
        if (strlen(pname) == 10) day = (uint32_t)(strtoull(pname, NULL, 10) / 100);

        char part_dir[4400];
        snprintf(part_dir, sizeof(part_dir), "%s/%s/%08u",
                 tsdb_db_data_dir(db), table, day);
        struct stat st;
        if (stat(part_dir, &st) != 0 || !S_ISDIR(st.st_mode)) continue;

        int src = rst_stamp_max_seq(s, part_dir,
                                    allow_seq ? (uint64_t)seq : 0, (uint16_t)nc);
        if (src == TSDB_ERR_NOTFOUND) continue;       /* nothing landed here */
        if (src != TSDB_OK) { rc = src; break; }
        if (!allow_seq) continue;
        if ((uint64_t)seq > stamped_hi) stamped_hi = (uint64_t)seq;
        if (out_max && seq > *out_max) *out_max = (uint64_t)seq;
    }
    free(buf);

    /* The stamp is a claim about seqs from the SOURCE's counter.  The target's
     * counter is still wherever its own recovery left it — 0 for a table this
     * restore just created — so its next commit would be handed a seq at or
     * below the stamp, and the following open would skip that record for this
     * partition.  A restore into an empty directory followed by ordinary
     * writes is the ENTIRE disaster-recovery workflow, so this is the common
     * path, not a corner. */
    if (rc == TSDB_OK && stamped_hi)
        (void)tsdb_db_raise_commit_seq(db, table, stamped_hi);
    return rc;
}

static int rst_land_table(tsdb_db_t *db, const char *backup_dir,
                          const char *table, tsdb_restore_table_t *rep)
{
    char path[4300];
    snprintf(path, sizeof(path), "%s/%s.tsm", backup_dir, table);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return TSDB_ERR_NOTFOUND;

    rst_stream_hdr_t h;
    memset(&h, 0, sizeof(h));
    tsdb_schema_t *tgt = NULL;
    int rc = rst_open_stream(db, fd, table, 1, &h, &tgt);
    if (rc != TSDB_OK) { close(fd); return rc; }

    rst_seen_t seen;
    memset(&seen, 0, sizeof(seen));

    /* ts LAST across the WHOLE stream.
     *
     * The exporter already emits ts last WITHIN a partition, which is what
     * makes a truncated stream safe for that partition.  Two passes make it
     * unconditional: after pass 1 the target holds every value column and no
     * ts at all, so any interruption anywhere leaves zero rows visible for the
     * partitions still in flight rather than rows whose value columns are
     * missing.  A missing value column under a published ts reads back as
     * fabricated zeros with rc=0 — the silent wrong answer.
     *
     * Needs a seekable stream.  A backup set is a regular file; if the seek
     * fails (a pipe) fall back to one pass and rely on the exporter's
     * per-partition ordering, which is weaker but never wrong.  The same seek
     * is what the re-run gate needs, so an unseekable stream gets neither. */
    if (lseek(fd, h.records_off, SEEK_SET) == (off_t)-1) {
        rc = rst_land_pass(db, fd, &h, tgt, &seen, -1, rep);   /* -1: land all */
    } else {
        /* Before pass 0 — a refusal must have landed nothing. */
        rc = rst_precheck_rerun(db, fd, &h, tgt);
        if (rc == TSDB_OK && lseek(fd, h.records_off, SEEK_SET) == (off_t)-1)
            rc = TSDB_ERR_IO;
        if (rc == TSDB_OK) rc = rst_land_pass(db, fd, &h, tgt, &seen, 0, rep);
        if (rc == TSDB_OK) {
            rst_crash_hook_before_ts();
            if (lseek(fd, h.records_off, SEEK_SET) == (off_t)-1) rc = TSDB_ERR_IO;
            else rc = rst_land_pass(db, fd, &h, tgt, &seen, 1, rep);
        }
    }
    rst_seen_free(&seen);
    close(fd);

    if (rc == TSDB_OK)
        rc = rst_apply_seq_sidecar(db, backup_dir, table, tgt, &rep->max_seq);
    return rc;
}

/* ---- marker -------------------------------------------------------------- */

int tsdb_restore_in_progress(const char *data_dir) {
    if (!data_dir || !data_dir[0]) return 0;
    char p[4200];
    snprintf(p, sizeof(p), "%s/%s", data_dir, TSDB_RESTORE_MARKER);
    struct stat st;
    return (stat(p, &st) == 0) ? 1 : 0;
}

static int rst_marker_write(const char *data_dir, const char *backup_dir) {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    char body[4600];
    int n = snprintf(body, sizeof(body),
                     "started_at_ns=%lld\npid=%d\nsource=%s\n",
                     (long long)tv.tv_sec * 1000000000LL + tv.tv_nsec,
                     (int)getpid(), backup_dir);
    if (n < 0) return TSDB_ERR_IO;
    return rst_write_file_atomic(data_dir, TSDB_RESTORE_MARKER, body, (size_t)n);
}

static void rst_marker_clear(const char *data_dir) {
    char p[4200];
    snprintf(p, sizeof(p), "%s/%s", data_dir, TSDB_RESTORE_MARKER);
    unlink(p);
    rst_fsync_dir(data_dir);
}

/* ---- restore ------------------------------------------------------------- */

int tsdb_restore_run(tsdb_db_t *db, const char *backup_dir,
                     tsdb_restore_report_t *out)
{
    if (out) memset(out, 0, sizeof(*out));
    if (!db || !backup_dir || !backup_dir[0]) return TSDB_ERR_INVAL;

    /* Before the marker is written: a restore that is refused never started,
     * so it must not leave a .tsdb_restore behind for the next open to find. */
    int srej = rst_reject_striped(db, "restore");
    if (srej != TSDB_OK) return srej;

    rst_manifest_row_t *mf = NULL; int n = 0;
    int rc = rst_manifest_load(backup_dir, &mf, &n);
    if (rc != TSDB_OK) {
        /* No manifest means the set was never completed — the whole point of
         * writing it last.  Refuse rather than restore an unknown fraction. */
        return (rc == TSDB_ERR_NOTFOUND) ? TSDB_ERR_NOTFOUND : rc;
    }

    const char *data_dir = tsdb_db_data_dir(db);
    if (!data_dir) { free(mf); return TSDB_ERR_INTERNAL; }
    rc = rst_marker_write(data_dir, backup_dir);
    if (rc != TSDB_OK) { free(mf); return rc; }

    tsdb_restore_table_t *rows = (tsdb_restore_table_t *)
        calloc((size_t)(n > 0 ? n : 1), sizeof(*rows));
    if (!rows) { free(mf); return TSDB_ERR_NOMEM; }

    int nfailed = 0, first_err = TSDB_OK;
    for (int i = 0; i < n; i++) {
        snprintf(rows[i].table, sizeof(rows[i].table), "%s", mf[i].name);
        if (mf[i].hour_partitioned) {
            /* The raw-block applier rebuilds a partition directory from the
             * DAY form of the key, so an HOUR-granularity table's 24 hourly
             * partitions would all land in one YYYYMMDD directory and read
             * back as one partition with interleaved blocks.  Refuse loudly
             * rather than restore something subtly wrong. */
            rows[i].rc = TSDB_ERR_UNSUPPORTED;
        } else {
            rows[i].rc = rst_land_table(db, backup_dir, mf[i].name, &rows[i]);
        }
        if (rows[i].rc != TSDB_OK) {
            nfailed++;
            if (first_err == TSDB_OK) first_err = rows[i].rc;
            fprintf(stderr, "[restore] table '%s' FAILED rc=%d (%s) — "
                            "continuing with the remaining tables\n",
                    mf[i].name, rows[i].rc, tsdb_errstr(rows[i].rc));
        }
    }
    free(mf);

    /* The marker is removed only when every table landed.  A partial restore
     * leaves it behind so the next open, the next operator and the next
     * verifier all see an unfinished restore instead of a healthy database. */
    if (nfailed == 0) rst_marker_clear(data_dir);

    if (out) {
        out->ntables  = n;
        out->nfailed  = nfailed;
        out->complete = (nfailed == 0);
        out->tables   = rows;
    } else {
        free(rows);
    }
    return (nfailed == 0) ? TSDB_OK : first_err;
}

void tsdb_restore_report_free(tsdb_restore_report_t *r) {
    if (!r) return;
    free(r->tables);
    r->tables = NULL; r->ntables = 0; r->nfailed = 0; r->complete = 0;
}

void tsdb_restore_verify_free(tsdb_restore_verify_t *v) {
    if (!v) return;
    free(v->tables);
    v->tables = NULL; v->ntables = 0; v->nfailed = 0;
}

/* ---- verify -------------------------------------------------------------- */

const char *tsdb_restore_target_rel_name(tsdb_restore_target_rel_t rel) {
    switch (rel) {
        case TSDB_RESTORE_TARGET_EQUAL:    return "equal";
        case TSDB_RESTORE_TARGET_AHEAD:    return "ahead";
        case TSDB_RESTORE_TARGET_BEHIND:   return "behind";
        case TSDB_RESTORE_TARGET_DIVERGED: return "diverged";
        case TSDB_RESTORE_TARGET_REENCODED: return "reencoded";
    }
    return "?";
}

/* How many real blocks of `col` carry this (ts_min, count)?
 *
 * THE ONE QUESTION THAT SEPARATES A MERGE FROM A LOSS.  Every column of a
 * partition shares ONE block layout: compact_partition rewrites ts first and
 * derives every other column's boundaries from it, swapping all columns or
 * none, and a flush stamps identical (ts_min, count) pairs across them.  So
 * comparing how many blocks the ts column carries under a key against how many
 * the value column carries under the same key says which of the two happened:
 * ts has MORE, and the value column lost one; ts has the same or fewer, and the
 * layout itself moved and there is nothing left to compare against.
 *
 * IT IS A COUNT AND NOT A BOOLEAN BECAUSE (ts_min, count) IS NOT UNIQUE.  Equal
 * timestamps are accepted and kept in insertion order and a flush splits on
 * block_points, so one partition can hold several blocks under one key.  Once
 * the deep check claims each target block at most once, a record can miss for
 * two different reasons — no block under the key at all, or every block under it
 * already claimed — and a boolean "is the key present" cannot tell them apart:
 * it answers "present" for the exhausted case and the loss is then filed as
 * unresolved.  The multiplicities give the right answer in all three shapes:
 *
 *     ts=2 val=1   ->  2 > 1  ->  LOSS        (one of a duplicate pair is gone)
 *     ts=1 val=1   ->  not >  ->  UNRESOLVED  (a merge collapsed the key)
 *     ts=1 val=0   ->  1 > 0  ->  LOSS        (the whole column's block is gone)
 */
static size_t rst_key_multiplicity(tsdb_part_t *p, int col,
                                   int64_t ts_min, uint32_t count)
{
    tsdb_block_meta_t *metas = NULL; size_t nb = 0;
    if (tsdb_part_col_blocks(p, col, &metas, &nb) != TSDB_OK) return 0;
    size_t n = 0;
    for (size_t i = 0; i < nb; i++) {
        if (metas[i].ts_min == ts_min && metas[i].count == count &&
            metas[i].offset != UINT64_MAX && metas[i].size != 0) n++;
    }
    free(metas);
    return n;
}

/* Compare ONE candidate target block against a stream record.
 *
 * Returns 0 = same block, 2 = present but DIFFERENT bytes that still decode,
 * 3 = present and UNREADABLE (its .col cannot be read at this offset, or the
 * codec / CRC trailer / header cross-check rejects it).
 *
 * 2 AND 3 ARE SPLIT BECAUSE ONLY ONE OF THEM IS AMBIGUOUS.  A legitimate
 * re-encode is free to produce different bytes under a key it happened to
 * preserve — the outer-LZ gate is measured per block and per build — so "the
 * bytes differ" alone cannot be reported on a compacted target without raising
 * the [Y] false alarm.  It is NOT free to produce bytes the reader refuses:
 * the compactor writes blocks it has just encoded and CRC-stamped.  So an
 * unreadable block is rot no matter what the rest of the table looks like, and
 * the caller reports it unconditionally.
 *
 * Which is why the decode runs even when the bytes DIFFER, and not only when
 * they match: that is the whole discriminator between the two causes of a byte
 * difference.  (It still runs on an identical block too — a restore can be
 * byte-perfect against a source that was already rotten.) */
static int rst_deep_compare_one(tsdb_schema_t *s, const char *part_dir,
                                tsdb_part_t *p, const tsdb_rawblock_push_t *r,
                                const tsdb_block_meta_t *hit)
{
    int differs = (hit->size != r->block_bytes_len);

    if (!differs) {
        char col_path[4500];
        snprintf(col_path, sizeof(col_path), "%s/%s.col",
                 part_dir, s->cols[r->col_idx].name);
        FILE *f = fopen(col_path, "rb");
        if (!f) return 3;
        uint8_t *have = (uint8_t *)malloc(hit->size ? hit->size : 1);
        if (!have) { fclose(f); return 2; }   /* OOM is not evidence either way */
        int io = (fseeko(f, (off_t)hit->offset + TSDB_BLOCK_HEADER_SIZE,
                         SEEK_SET) == 0 &&
                  fread(have, 1, hit->size, f) == hit->size);
        if (io) differs = (memcmp(have, r->block_bytes, hit->size) != 0);
        free(have);
        fclose(f);
        if (!io) return 3;         /* the .col is shorter than its own index */
    }

    size_t width = tsdb_type_width(s->cols[r->col_idx].type);
    if (width == 0) width = 8;
    void *outb = malloc((size_t)hit->count * width + 8);
    if (!outb) return differs ? 2 : 0;        /* OOM is not evidence either way */
    int drc = tsdb_part_read_block(p, (int)r->col_idx, hit, outb);
    free(outb);
    if (drc != TSDB_OK) return 3;
    return differs ? 2 : 0;
}

/* Which of ONE (partition, column)'s target blocks a stream record has already
 * claimed.
 *
 * (ts_min, count) IS NOT A UNIQUE BLOCK IDENTITY, and the containment answer
 * fell apart on exactly that.  Equal timestamps are accepted and kept in
 * insertion order and a flush splits on block_points, so one partition can hold
 * two blocks with identical (ts_min, ts_max, count) — and when the target has
 * lost one of them, an UNCONSUMED first-match let the single survivor satisfy
 * BOTH stream records.  Identical payload bytes, so both compared equal, and
 * DEEP reported blocks_checked=4 blocks_missing=0 blocks_unresolved=0: a
 * positively clean, fully-resolved containment answer for a database whose
 * value column reads back half fabricated zeros with rc=0.  (It also disarmed
 * the blocks_checked == 0 => INDETERMINATE guard, which cannot fire against a
 * count of 4.)
 *
 * The landing path already counts occurrences for the same reason (rst_seen_t:
 * "a source that legitimately holds two blocks with the same (ts_min, count)
 * must come back with two").  The check has to count them too. */
typedef struct {
    uint8_t *bits;      /* one flag per target block of this column          */
    size_t   n;
} rst_claim_t;

/* Locate the target block matching a stream record, claim it, and compare it.
 * Returns 0 = match, 1 = missing, 2 = present with different but decodable
 * bytes, 3 = present and unreadable. */
static int rst_deep_check_block(tsdb_schema_t *s, const char *part_dir,
                                tsdb_part_t *p, const tsdb_rawblock_push_t *r,
                                rst_claim_t *cl)
{
    tsdb_block_meta_t *metas = NULL; size_t nb = 0;
    if (tsdb_part_col_blocks(p, (int)r->col_idx, &metas, &nb) != TSDB_OK)
        return 1;

    if (cl && !cl->bits && nb) {
        cl->bits = (uint8_t *)calloc(nb, 1);
        cl->n    = cl->bits ? nb : 0;
    }

    /* Pair the way the reader pairs — FIRST match on (ts_min, count) — among the
     * blocks NO EARLIER RECORD HAS TAKEN.  Take the first free candidate and
     * report what it is; do not look for a better one.
     *
     * An earlier revision preferred whichever free candidate compared equal, to
     * make the answer independent of the order two interchangeable blocks sit
     * in.  It bought that with a silent wrong answer: verdict 3 (present and
     * UNREADABLE) sorts WORSE than verdict 0, so when one block under a key had
     * rotted and its sibling had not, the scan walked past the rot to the clean
     * one and returned 0.  The rot was never counted and DEEP answered TSDB_OK
     * — blocks_checked=2 missing=0 mismatched=0 unresolved=0, the positively
     * clean fully-resolved answer that is the worst shape this check can emit,
     * for a database whose SELECT returns TSDB_ERR_CORRUPT.  It also defeated
     * both backstops: blocks_checked == 0 cannot fire against a count of 2, and
     * a rot is not a HOLE so the index level's holes test saw nothing either.
     *
     * First-free-match needs no preference to be order-independent in the only
     * sense that matters — the TOTALS.  Whichever of the two sits first, one
     * record takes the rotted block and reports it and the other takes the clean
     * one; the pair comes out 1 mismatched + 1 match either way.  It is also
     * O(records) rather than O(records x duplicates), which for a long
     * duplicate-key run is the difference between one decode per record and one
     * per pair. */
    int v = 1; size_t take = 0; int have = 0;
    for (size_t i = 0; i < nb; i++) {
        if (metas[i].ts_min != r->ts_min || metas[i].count != r->count) continue;
        if (metas[i].offset == UINT64_MAX || metas[i].size == 0) continue;
        if (cl && i < cl->n && cl->bits[i]) continue;
        v = rst_deep_compare_one(s, part_dir, p, r, &metas[i]);
        take = i; have = 1;
        break;
    }
    if (have && cl && take < cl->n) cl->bits[take] = 1;

    free(metas);
    return have ? v : 1;
}

/* `reenc_shape` != 0 when the index level saw this table hold at least the
 * set's rows in FEWER blocks — the shape a re-encode leaves, and equally the
 * shape a lost value column leaves.  It does NOT switch the check off (that
 * decline is what let a lost column verify clean); it switches ON the per-block
 * question above, so every miss is attributed either to the layout having moved
 * or to the block being gone. */
static int rst_verify_deep_table(tsdb_db_t *db, const char *backup_dir,
                                 const char *table, int reenc_shape,
                                 tsdb_restore_verify_table_t *vt)
{
    char path[4300];
    snprintf(path, sizeof(path), "%s/%s.tsm", backup_dir, table);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return TSDB_ERR_NOTFOUND;

    rst_stream_hdr_t h;
    memset(&h, 0, sizeof(h));
    tsdb_schema_t *tgt = NULL;
    /* for_write == 0: a verify never creates a table and never adopts a
     * dictionary — a check that repairs what it is checking cannot fail. */
    int rc = rst_open_stream(db, fd, table, 0, &h, &tgt);
    if (rc != TSDB_OK) { close(fd); return rc; }

    uint8_t *buf = NULL; size_t cap = 0;
    uint32_t open_day = 0; tsdb_part_t *p = NULL; char part_dir[4300] = {0};

    /* One claim table per column, reset with the partition — the block indexes
     * they refer to are positions in the open tsdb_part_t. */
    rst_claim_t *claim = (rst_claim_t *)calloc((size_t)tgt->ncols, sizeof(*claim));
    if (!claim) { close(fd); return TSDB_ERR_NOMEM; }

    for (;;) {
        uint8_t lb[4]; int eof = 0;
        rc = rst_read_all(fd, lb, 4, &eof);
        if (rc != TSDB_OK) break;
        if (eof) { rc = TSDB_ERR_CORRUPT; break; }
        uint32_t len = rst_get_u32(lb);
        if (len == 0) { rc = TSDB_OK; break; }
        if (len > (64u << 20)) { rc = TSDB_ERR_CORRUPT; break; }
        if (len > cap) {
            uint8_t *nb = (uint8_t *)realloc(buf, len);
            if (!nb) { rc = TSDB_ERR_NOMEM; break; }
            buf = nb; cap = len;
        }
        rc = rst_read_all(fd, buf, len, NULL);
        if (rc != TSDB_OK) break;

        tsdb_rawblock_push_t r;
        if (tsdb_rawblock_parse(buf, len, &r) != TSDB_OK) { rc = TSDB_ERR_CORRUPT; break; }
        if ((int)r.col_idx >= tgt->ncols) { rc = TSDB_ERR_SCHEMA; break; }

        if (!p || r.part_day != open_day) {
            if (p) { tsdb_part_close(p); p = NULL; }
            for (int c = 0; c < tgt->ncols; c++) {
                free(claim[c].bits); claim[c].bits = NULL; claim[c].n = 0;
            }
            open_day = r.part_day;
            snprintf(part_dir, sizeof(part_dir), "%s/%s/%08u",
                     tsdb_db_data_dir(db), table, open_day);
            if (tsdb_part_open(tgt, part_dir, &p) != TSDB_OK) p = NULL;
        }

        vt->blocks_checked++;
        if ((int)r.col_idx == h.ts_idx) vt->rows_backup += r.count;

        if (!p) { vt->blocks_missing++; continue; }
        int v = rst_deep_check_block(tgt, part_dir, p, &r, &claim[r.col_idx]);
        if (v == 0) continue;

        /* AN UNREADABLE BLOCK IS NOT AMBIGUOUS, so it is never swallowed.
         * The re-encode hypothesis below explains different BYTES; it does not
         * explain bytes the reader refuses, because whatever wrote them would
         * have CRC-stamped what it encoded.  This is the one verdict the
         * table-wide `reenc_shape` used to swallow on partitions the compactor
         * had never touched. */
        if (v == 3) { vt->blocks_mismatched++; continue; }

        /* On a table with no block deficit — every relation but the two a
         * deficit produces — the verdict stands exactly as it always has.
         *
         * On one that HAS the deficit, a verdict has two possible causes and
         * they are opposite answers, so ask the ts column which one it is.
         * TS CARRIES MORE BLOCKS UNDER THIS KEY THAN THE COLUMN DOES: this
         * partition's layout is intact at this position and the column is one
         * block short — GONE (the lost-column case), reported as the loss it is.
         * TS CARRIES THE SAME OR FEWER: this partition was re-encoded, the set's
         * block does not exist as a block any more, and neither "missing" nor
         * "present" is a finding.  The question is per BLOCK, so one merged
         * partition and one torn partition in the same table come out as
         * separate numbers.  It is a COUNT rather than "is the key there at all"
         * because the key is not unique and a record can now miss through
         * EXHAUSTION as well as absence — see rst_key_multiplicity.
         *
         * A DECODABLE BYTE DIFFERENCE (v == 2) IS UNRESOLVABLE OUTRIGHT HERE,
         * and the ts key is deliberately NOT consulted for it — it would always
         * answer "present", since the block was found in this very column under
         * that key.  A merge that happens to preserve a boundary (32768 rows
         * merged, then a trailing group of exactly one source block) re-encodes
         * the same rows, and the outer-LZ gate is measured per block and per
         * build, so different-but-decodable bytes under an identical key are
         * exactly what a legitimate re-encode produces.  Reporting it would be
         * the [Y] false alarm on a healthy compacted node; it stays a number the
         * operator can see (blocks_unresolved) rather than one they would act
         * on.  v == 3 never reaches here — see above. */
        if (reenc_shape &&
            (v == 2 ||
             rst_key_multiplicity(p, h.ts_idx, r.ts_min, r.count) <=
             rst_key_multiplicity(p, (int)r.col_idx, r.ts_min, r.count))) {
            vt->blocks_unresolved++;
            continue;
        }
        if (v == 1) vt->blocks_missing++;
        else        vt->blocks_mismatched++;
    }
    if (p) tsdb_part_close(p);
    for (int c = 0; c < tgt->ncols; c++) free(claim[c].bits);
    free(claim);
    free(buf);
    close(fd);
    return rc;
}

int tsdb_restore_verify(tsdb_db_t *db, const char *backup_dir,
                        tsdb_restore_level_t level,
                        tsdb_restore_verify_t *out)
{
    if (out) memset(out, 0, sizeof(*out));
    if (!db || !backup_dir || !backup_dir[0]) return TSDB_ERR_INVAL;

    /* The third door into the same blindness.  tsdb_migrate_digest and
     * rst_verify_deep_table both read tsdb_db_data_dir() — dir 0 — so on a
     * striped db a verify would answer about a table that is not where it
     * looked.  Refusing backup and restore but letting VERIFY through would
     * leave the one call an operator makes to ask "did my data survive?" as
     * the only one still willing to answer wrongly. */
    int srej = rst_reject_striped(db, "verify");
    if (srej != TSDB_OK) return srej;

    rst_manifest_row_t *mf = NULL; int n = 0;
    int rc = rst_manifest_load(backup_dir, &mf, &n);
    if (rc != TSDB_OK) return rc;

    tsdb_restore_verify_table_t *vt = (tsdb_restore_verify_table_t *)
        calloc((size_t)(n > 0 ? n : 1), sizeof(*vt));
    if (!vt) { free(mf); return TSDB_ERR_NOMEM; }

    int deep = (level == TSDB_RESTORE_VERIFY_DEEP);
    int nfailed = 0, first_err = TSDB_OK;

    for (int i = 0; i < n; i++) {
        snprintf(vt[i].table, sizeof(vt[i].table), "%s", mf[i].name);
        vt[i].rows_backup = mf[i].rows;

        /* Index level: fold the target's block metadata and compare against
         * what the backup recorded.  Reads idx headers only — no .col byte is
         * touched and nothing is decompressed.  tsdb_migrate_digest resolves
         * the table on DISK, so it opens one this process has not touched (and
         * therefore replays its redo log into a memtable) without flushing —
         * opening is not writing. */
        tsdb_mig_stats_t dg;
        memset(&dg, 0, sizeof(dg));
        int drc = tsdb_migrate_digest(db, mf[i].name, &dg);

        /* DRAIN ONLY A TARGET THAT HOLDS FEWER ROWS THAN THE SET.
         *
         * tsdb_migrate_digest reads the PARTITION FILES.  Under deferred flush
         * (the cluster's mode) a table's acked rows can be entirely in a
         * memtable, or — before anything opened it — only in its redo log, so
         * the fold comes back SHORT and the set is being compared against a
         * FRACTION of the database.  That is not a corner: it is what the node
         * an operator is verifying looks like right after the crash they are
         * verifying because of.  Partitions empty, 1000 rows in the WAL, digest
         * 0 against a manifest of 1000 — a scary CORRUPT on exactly the DR path
         * this feature exists for.  So when the fold is short, put the rows
         * where the comparison can see them and fold again.
         *
         * ROWS, AND NOT BLOCKS.  A flush moves rows out of a memtable; the
         * number of BLOCKS the partition ends up with is the target's own
         * business and it goes DOWN, not up, when the compactor merges them.
         * `dg.blocks < mf.blocks` was therefore true of every partition the
         * compactor had touched — fewer blocks, same rows — and the compactor
         * runs on a 5 s timer, so this fired on healthy nodes: a drain that
         * wrote to the database, shipped rows to peers, and published the very
         * block boundaries tsdb_restore_run's re-run gate refuses on.  There is
         * nothing a flush could have added: the rows were already there.
         *
         * The bound is the fix for two defects, not an optimisation:
         *
         *   1. A diagnostic must not replicate — and must not silence
         *      replication either.  A drain is a real flush and a memtable
         *      flushes exactly ONCE: flush_and_clear_locked fires the cluster
         *      hook and then clears, so skip_replicate=1 here would not "stop
         *      the check replicating", it would DELETE those rows from the
         *      replication stream for good (db.c tsdb_db_flush_all: "suppressing
         *      the hook silently dropped them from replication for good").  The
         *      way out is not to suppress the hook on a drain, it is not to
         *      drain a database that is not behind — and a healthy cluster node
         *      never is, because its partitions already hold the set.
         *
         *   2. A drain publishes blocks whose (ts_min,count) boundaries came
         *      from the target's memtable, not from this stream, and that is
         *      the state tsdb_restore_run's re-run gate REFUSES on.  Verifying
         *      a live source used to arm that refusal every time; now only a
         *      target that was already behind can, and it says so below.
         *
         * Opening is not creating and draining is not repairing: this touches
         * only the target's own already-acked rows, never the backup set's
         * content — rst_open_stream is still called with for_write == 0 below,
         * so no table is created and no dictionary is adopted from the stream.
         * A table the manifest names and the target does not have still reports
         * NOTFOUND. */
        if (drc == TSDB_OK && dg.rows < mf[i].rows) {
            uint64_t was_rows = dg.rows, was_blocks = dg.blocks;
            /* skip_replicate=0 deliberately — see (1) above. */
            if (tsdb_table_flush(db, mf[i].name) == TSDB_OK) {
                tsdb_mig_stats_t dg2;
                memset(&dg2, 0, sizeof(dg2));
                if (tsdb_migrate_digest(db, mf[i].name, &dg2) == TSDB_OK &&
                    (dg2.rows != was_rows || dg2.blocks != was_blocks)) {
                    dg = dg2;
                    vt[i].drained = 1;
                    fprintf(stderr, "[restore] verify drained '%s': its "
                                    "partitions held %llu row(s) / %llu block(s) "
                                    "against a manifest of %llu / %llu, so its "
                                    "memtable was flushed to answer — now %llu / "
                                    "%llu.  Those blocks carry the TARGET's block "
                                    "boundaries, so a later tsdb_restore_run on "
                                    "this table can refuse with UNSUPPORTED; on a "
                                    "cluster the flush also replicated them, the "
                                    "same as the size/idle flush it front-ran\n",
                            mf[i].name,
                            (unsigned long long)was_rows, (unsigned long long)was_blocks,
                            (unsigned long long)mf[i].rows, (unsigned long long)mf[i].blocks,
                            (unsigned long long)dg.rows, (unsigned long long)dg.blocks);
                }
            }
        }

        if (drc != TSDB_OK) {
            vt[i].rc = drc;
            vt[i].blocks_missing = mf[i].blocks;
            /* A table the target does not have holds NONE of what the set
             * carries.  Leaving the enum at its zero value would report it as
             * "equal", which is the one word it must not be. */
            vt[i].target_rel = TSDB_RESTORE_TARGET_BEHIND;
            nfailed++;
            if (first_err == TSDB_OK || first_err == TSDB_ERR_INDETERMINATE)
                first_err = TSDB_ERR_CORRUPT;
            continue;
        }
        vt[i].rows_target    = dg.rows;
        vt[i].blocks_checked = mf[i].blocks;

        /* THE RE-ENCODE HYPOTHESIS, as a predicate rather than as a verdict.
         *
         * "Fewer blocks than the set, and no rows short to explain them" is the
         * evidence that SOME partition of this table may have had its block
         * layout rewritten.  It is not proof — a lost value column produces the
         * same two numbers, which is the whole defect — so it decides nothing
         * on its own; it only tells the DEEP level that a lookup miss needs
         * attributing (see rst_verify_deep_table).
         *
         * Kept separate from target_rel deliberately: holes push the relation
         * to BEHIND while leaving the re-encode hypothesis true for the OTHER
         * partitions, which is exactly the mixed shape — one merged partition
         * beside one that lost a column.  Reading the hint off the relation
         * instead reported all of the merged partition's blocks as missing. */
        int reenc_shape = (dg.rows >= mf[i].rows && dg.blocks < mf[i].blocks);

        /* Set when the target has UNREADABLE block slots that this level has no
         * way to attribute — see the AHEAD arm below. */
        int holes_unproven = 0;

        /* Classify the DATABASE-vs-SET relation, and fail only on the
         * directions that mean something the set carries is gone.  Under AHEAD
         * and REENCODED nothing is KNOWN to be missing or mismatched, so
         * neither counter is touched: the old code set blocks_mismatched = 1
         * under AHEAD, which reported a mismatch it had not found and could not
         * have found — the fold is an XOR over the whole target and extra
         * blocks make it differ on their own.
         *
         * ROWS decide loss, and they decide it first.  Fewer BLOCKS holding at
         * least the set's rows is the compactor — UNLESS the target's own holes
         * account for the deficit, which is the case below.
         *
         * WHY THE HOLE COUNT IS A THIRD AXIS AND NOT A REFINEMENT OF `blocks`.
         * tsdb_migrate_digest sums `rows` over the TS COLUMN ONLY and counts
         * `blocks` over EVERY column, so those two do not measure the same
         * population — and a target that loses one value column's blocks moves
         * only the second: same rows, fewer blocks, which is bit for bit the
         * signature of a compactor merge.  That was certified TSDB_OK at both
         * levels for a database whose SELECT of that column returns
         * TSDB_ERR_CORRUPT.  A hole is the READER's own verdict on a block slot
         * (tsdb_part_open sets TSDB_BLOCK_FLAG_HOLE only after ruling out the
         * ALTER-added explanation), so it separates the two exactly.
         *
         * ONLY A DEFICIT THEY EXPLAIN FAILS.  Holes on their own are not loss:
         * a set taken from an already-torn source carries that tear, the
         * restored copy reproduces it, and `blocks` matches on both sides — the
         * copy contains everything the set carries, which is the question this
         * call answers.  It is the combination "the set carries blocks the
         * target does not hold, and the target has unreadable slots" that means
         * they were lost after the set was taken. */
        if (dg.rows < mf[i].rows) {
            vt[i].target_rel = TSDB_RESTORE_TARGET_BEHIND;
            vt[i].rc = TSDB_ERR_CORRUPT;
            if (dg.blocks < mf[i].blocks) vt[i].blocks_missing = mf[i].blocks - dg.blocks;
        } else if (dg.blocks < mf[i].blocks && dg.holes) {
            vt[i].target_rel = TSDB_RESTORE_TARGET_BEHIND;
            vt[i].rc = TSDB_ERR_CORRUPT;
            /* A measurement, not a difference of totals: these are the block
             * slots tsdb_part_read_block answers TSDB_ERR_CORRUPT for. */
            vt[i].blocks_missing = dg.holes;
            fprintf(stderr, "[restore] verify '%s': the target holds the set's "
                            "rows (%llu against %llu) in FEWER blocks (%llu "
                            "against %llu) and %llu of its block slot(s) are "
                            "UNREADABLE — a column's blocks are gone, not "
                            "re-encoded.  The row count cannot see this: it is "
                            "summed over the ts column alone, and the ts column "
                            "is intact\n",
                    mf[i].name,
                    (unsigned long long)dg.rows, (unsigned long long)mf[i].rows,
                    (unsigned long long)dg.blocks, (unsigned long long)mf[i].blocks,
                    (unsigned long long)dg.holes);
        } else if (dg.blocks < mf[i].blocks) {
            vt[i].target_rel = TSDB_RESTORE_TARGET_REENCODED;
            fprintf(stderr, "[restore] verify '%s': the target holds the set's "
                            "rows (%llu against %llu) in FEWER blocks (%llu "
                            "against %llu) with nothing unreadable — its block "
                            "boundaries were rewritten under it, which is what "
                            "the compactor does to every cold partition.  The "
                            "INDEX level cannot answer containment here; the "
                            "DEEP level looks the set's blocks up one by one "
                            "and reports what it could not resolve\n",
                    mf[i].name,
                    (unsigned long long)dg.rows, (unsigned long long)mf[i].rows,
                    (unsigned long long)dg.blocks, (unsigned long long)mf[i].blocks);
        } else if (dg.rows > mf[i].rows || dg.blocks > mf[i].blocks) {
            vt[i].target_rel = TSDB_RESTORE_TARGET_AHEAD;
            /* THE HOLES ARE EVIDENCE HERE TOO, AND THIS LEVEL CANNOT SPEND IT.
             *
             * A target that kept ingesting is the live cluster node the drain
             * logic exists for, and it is the one relation where a deficit can
             * never form: extra blocks put `blocks` ABOVE the set's, so the
             * branch above cannot fire and the same whole-column loss [H1]
             * catches on a quiesced node was measured and thrown away.  The
             * index level answered TSDB_OK for a database whose SELECT of that
             * column returns TSDB_ERR_CORRUPT — [H1]'s "must NOT verify clean at
             * EITHER level" held only because its target was not AHEAD.
             *
             * BUT IT IS NOT PROOF OF LOSS EITHER, and CORRUPT would be the false
             * alarm in the other direction: a set taken from an already-torn
             * SOURCE carries the tear, a copy reproducing it faithfully holds
             * unreadable slots the set never carried a block for, and that copy
             * ingesting one more row lands exactly here.  Deciding needs the
             * per-block question — is the set's own block the one that is gone?
             * — and a fold of totals cannot ask it.  So this level reports what
             * it has: unreadable slots, and no way to tell.  DEEP can ask, and
             * clears this the moment it measures anything (below). */
            if (dg.holes) holes_unproven = 1;
            if (!deep)
                fprintf(stderr, "[restore] verify '%s': the target holds MORE "
                                "than the set (%llu rows / %llu blocks against "
                                "%llu / %llu) — not set corruption, it kept "
                                "ingesting.  The index level cannot prove the "
                                "set's own blocks are still among them; run the "
                                "DEEP level for a containment answer\n",
                        mf[i].name,
                        (unsigned long long)dg.rows, (unsigned long long)dg.blocks,
                        (unsigned long long)mf[i].rows, (unsigned long long)mf[i].blocks);
        } else if (dg.digest != mf[i].digest) {
            vt[i].target_rel = TSDB_RESTORE_TARGET_DIVERGED;
            vt[i].rc = TSDB_ERR_CORRUPT;
            vt[i].blocks_mismatched = 1;
        } else {
            vt[i].target_rel = TSDB_RESTORE_TARGET_EQUAL;
        }

        /* DEEP DOES NOT DECLINE ON A RE-ENCODED TARGET ANY MORE.
         *
         * It used to: rst_deep_check_block finds the target's copy of a stream
         * block by the (ts_min, count) pair the reader pairs on and then
         * compares the bytes, and a merge destroys both — 150 blocks of 1000
         * rows become 5 of 32768, so every lookup misses.  Running it blind
         * there reports blocks_missing == the whole set against a database that
         * holds every row of it, the [Y] false alarm on its most common trigger.
         *
         * But declining was worse, because REENCODED is not only reached by a
         * re-encode.  `rows` is summed over the ts column alone and `blocks` is
         * counted over every column, so a target that loses one value column's
         * blocks lands in exactly this branch — and the decline answered "your
         * data survived" for a database the engine refuses to read.  The index
         * level now separates the two by the target's holes (above); DEEP
         * separates them per BLOCK, which is finer: it asks, for each miss,
         * whether the target's ts column still carries that block's key.  One
         * merged partition therefore no longer switches the containment check
         * off for the intact partitions beside it — the merged one's blocks
         * come back unresolved and the intact one's are checked normally.
         *
         * What cannot be resolved is REPORTED as unresolved rather than folded
         * into "missing" or into silence, so a compacted target still passes
         * (nothing is known to be missing) while carrying an explicit count of
         * what no level could answer for. */
        if (deep) {
            tsdb_restore_verify_table_t d;
            memset(&d, 0, sizeof(d));
            int vrc = rst_verify_deep_table(db, backup_dir, mf[i].name,
                                            reenc_shape, &d);
            /* Assigned, not added.  DEEP looks every block of the set up
             * individually, so it IS the containment answer; the index level's
             * two counters are a difference of totals — an estimate under
             * BEHIND and, under AHEAD, not derivable at all.  Adding them on
             * top reported a number neither pass had measured.
             *
             * The one exception is a hole count DEEP could not improve on: when
             * every block it looked at came back unresolved it measured nothing
             * about containment, so overwriting the index level's measurement
             * with its own 0 would erase the only number anybody took. */
            vt[i].blocks_checked     = d.blocks_checked;
            vt[i].blocks_unresolved  = d.blocks_unresolved;
            vt[i].blocks_mismatched  = d.blocks_mismatched;
            if (d.blocks_missing || d.blocks_checked > d.blocks_unresolved)
                vt[i].blocks_missing = d.blocks_missing;
            if (vrc != TSDB_OK && vt[i].rc == TSDB_OK) vt[i].rc = vrc;
            if ((d.blocks_missing || d.blocks_mismatched) && vt[i].rc == TSDB_OK)
                vt[i].rc = TSDB_ERR_CORRUPT;
        }

        /* UNREADABLE SLOTS THIS CALL COULD NOT ATTRIBUTE.
         *
         * Only DEEP can attribute them, and only by looking the set's blocks up
         * one at a time: a hole at a coordinate the set carries a block for is
         * loss (DEEP counts it in blocks_missing, and under AHEAD it does so
         * with no swallowing at all — reenc_shape is false whenever the target
         * holds MORE blocks); a hole at a coordinate the set never carried is
         * the source's own tear, faithfully reproduced, and DEEP correctly has
         * nothing to say about it.  So DEEP clears this the moment it resolved
         * anything, and the INDEX level — which cannot ask — reports that it
         * could not tell.  INDETERMINATE, not CORRUPT: "run the deep level" is
         * the finding, not "your data is gone". */
        if (holes_unproven && vt[i].rc == TSDB_OK &&
            !(deep && vt[i].blocks_checked > vt[i].blocks_unresolved)) {
            vt[i].rc = TSDB_ERR_INDETERMINATE;
            fprintf(stderr, "[restore] verify '%s': the target holds MORE than "
                            "the set AND %llu of its block slot(s) are "
                            "UNREADABLE.  Extra blocks mean no deficit can form, "
                            "so this level cannot tell whether the set's own "
                            "blocks are among the unreadable ones — run the DEEP "
                            "level, which looks them up one by one\n",
                    mf[i].name, (unsigned long long)dg.holes);
        }

        /* "NOTHING WAS MEASURED" IS NOT "NOTHING IS MISSING".
         *
         * DEEP's whole claim is that it looked every block of the set up
         * individually.  A pass that looked at NONE of them while the manifest
         * says the set carries some has not cleared it — it FAILED TO CHECK it —
         * and TSDB_OK is the one answer that must not come back, because an
         * operator making this call once, during an incident, reads TSDB_OK as
         * "your data survived".  That is exactly what the decline above used to
         * produce for a target that had lost a whole column.
         *
         * DELIBERATELY OUTSIDE the deep branch, and phrased over the RESULT
         * rather than over any particular cause: the defect this closes was a
         * decline that returned before the check ran, so an invariant nested
         * inside the path that runs the check cannot be the thing that catches
         * the next one.  The count is not a finding either way, so this is
         * INDETERMINATE, not CORRUPT. */
        if (deep && vt[i].rc == TSDB_OK && mf[i].blocks > 0 &&
            vt[i].blocks_checked == 0) {
            vt[i].rc = TSDB_ERR_INDETERMINATE;
            fprintf(stderr, "[restore] verify '%s': DEEP looked at NO block at "
                            "all, against a manifest that says this table "
                            "carries %llu.  Nothing was measured, which is not "
                            "the same as nothing being missing — the set has "
                            "not been cleared\n",
                    mf[i].name, (unsigned long long)mf[i].blocks);
        }

        if (vt[i].rc != TSDB_OK) {
            nfailed++;
            /* A table nobody could measure must not out-rank one found broken:
             * CORRUPT is the louder answer and it wins the call's rc. */
            if (first_err == TSDB_OK || first_err == TSDB_ERR_INDETERMINATE)
                first_err = (vt[i].rc == TSDB_ERR_INDETERMINATE)
                                ? TSDB_ERR_INDETERMINATE : TSDB_ERR_CORRUPT;
        }
    }
    free(mf);

    if (out) {
        out->level_ran  = deep ? TSDB_RESTORE_VERIFY_DEEP : TSDB_RESTORE_VERIFY_INDEX;
        out->level_name = deep ? "deep" : "index";
        out->ntables    = n;
        out->nfailed    = nfailed;
        out->tables     = vt;
    } else {
        free(vt);
    }
    /* TSDB_OK only when every table was CHECKED and found intact.  A table that
     * could not be measured returns TSDB_ERR_INDETERMINATE and one found broken
     * returns TSDB_ERR_CORRUPT, which wins when both are present. */
    return (nfailed == 0) ? TSDB_OK : first_err;
}
