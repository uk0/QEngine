/* test_restore_multidir.c — the two silent-wrong-answer paths the restore
 * reviewer reproduced, pinned so they cannot come back.
 *
 * Both are "rc=0 and the data is gone", which is the exact failure class the
 * restore API exists to eliminate, so both are asserted on the LOUD outcome
 * (a named error / an untouched target), never on "it happened to work".
 *
 *   [A] STRIPED BACKUP.  With TSDB_DATA_DIRS set, tsdb_create_table places a
 *       table on data_dirs[hash(name)%N] while every reader in restore.c uses
 *       tsdb_db_data_dir() == dir 0.  rst_list_tables deliberately enumerates
 *       ALL stripes, so tsdb_backup_create used to emit a manifest NAMING
 *       tables it never read: rc=0, complete=1, rows=0 for every table that
 *       lives off dir 0, and tsdb_restore_verify(DEEP) certifies it because
 *       tsdb_migrate_digest is blind the same way (0 == 0).  A set that
 *       silently omits half the database is worse than no set, so a striped
 *       db is REFUSED up front.
 *
 *   [B] STRIPED RESTORE TARGET.  Reverse direction: a good single-dir set
 *       restored into a striped db creates each table on its hash stripe
 *       while tsdb_rawblock_apply writes every block into dir 0.  The blocks
 *       land beside a table that is not there — count(*) == 0 after an rc=0
 *       restore, and the deep verify reads dir 0 too, so it confirms the
 *       restore is clean.  Refused, and refused BEFORE the marker is written
 *       so a refusal leaves no trace of a restore that never started.
 *
 *   [C] LEGACY V1/V2 idx.  rst_stamp_max_seq probes the entry stride and then
 *       re-heads the file with tsdb_part_write_idx_header, which always
 *       declares TSDB_IDX_ENTRY_SIZE (88).  A V1/V2 idx carries 40-byte
 *       entries, so the stamp re-interpreted every entry at the wrong stride:
 *       a partition holding rows read back as 0 with rc=0.  Reachable exactly
 *       on the "old blocks stay readable" path — the legacy partition
 *       dedup-skips every block, so tsdb_rawblock_apply (which WOULD widen
 *       the entries to 88) never runs and only the stamp touches the file.
 *       Declining the checkpoint costs nothing a resume cannot redo; a
 *       too-low checkpoint only re-applies records the landing dedup already
 *       handles.
 *
 *   [D] THE ncols STAMP.  rst_stamp_max_seq rewrites <ts>.idx's header to
 *       carry the WAL checkpoint.  Bytes [10..11] of that header record how
 *       many columns the writer of the partition knew about, and that number
 *       is rule 2 of part_col_absence_is_late_add — the ONLY evidence
 *       separating a column whose blocks are GONE (read as an error) from one
 *       ALTER-added later (zero-filled by design).  Rule 1 ("a later column
 *       still has blocks") is vacuous for the LAST column, so for that column
 *       the stamp is all there is.  Re-heading with 0 therefore turns every
 *       checkpointed partition into one where a missing trailing column reads
 *       back as zeros with rc=0 — the fabrication the read path was fixed to
 *       stop, re-introduced through the restore checkpoint.
 *
 *   [E] HOUR AT BACKUP TIME.  An HOUR-granularity table cannot be restored
 *       (the applier rebuilds a partition dir from the DAY form of the key).
 *       That was enforced at RESTORE time only, so a set was written, reported
 *       complete=1, and trusted until the incident it was taken for.  The
 *       refusal belongs where the operator can still act on it.
 *
 * [C] also pins the residue fix: the stamp's tmp file never survives.
 *
 * Each phase runs standalone: `test_restore_multidir A` runs only [A].  No
 * argument runs all of them.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define MD_SRC  "/tmp/tsdb_test_rmd_src"
#define MD_D1   "/tmp/tsdb_test_rmd_d1"
#define MD_D2   "/tmp/tsdb_test_rmd_d2"
#define MD_BK   "/tmp/tsdb_test_rmd_bk"
#define PLAIN   "/tmp/tsdb_test_rmd_plain"
#define PLAINBK "/tmp/tsdb_test_rmd_plainbk"
#define MD_DST  "/tmp/tsdb_test_rmd_dst"
#define LEG_SRC "/tmp/tsdb_test_rmd_legsrc"
#define LEG_BK  "/tmp/tsdb_test_rmd_legbk"
#define LEG_DST "/tmp/tsdb_test_rmd_legdst"
#define NC_SRC  "/tmp/tsdb_test_rmd_ncsrc"
#define NC_BK   "/tmp/tsdb_test_rmd_ncbk"
#define NC_DST  "/tmp/tsdb_test_rmd_ncdst"
#define HR_SRC  "/tmp/tsdb_test_rmd_hrsrc"
#define HR_BK   "/tmp/tsdb_test_rmd_hrbk"

#define NTABLES 12
#define NROWS   1000
#define TS0     1700000000000000000LL

static const char *TABLES[NTABLES] = {
    "t_a", "t_b", "t_c", "t_d", "t_e", "t_f",
    "t_g", "t_h", "t_i", "t_j", "t_k", "t_l"
};

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static int64_t val_n(int64_t i) { return (i * 7919) % 100003; }

/* SELECT count(*), or -1 when the query itself failed. */
static int64_t count_rows(tsdb_db_t *db, const char *table) {
    char q[160];
    snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
    tsdb_result_t *r = NULL;
    int64_t n = -1;
    if (tsdb_query(db, q, &r) == TSDB_OK && r && tsdb_result_next(r) > 0)
        n = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);
    return n;
}

/* Fold every cell so a wrong-but-same-shaped answer still moves the number. */
static uint64_t digest_rows(tsdb_db_t *db, const char *table, int64_t *out_rows) {
    char q[160];
    snprintf(q, sizeof(q), "SELECT ts, n FROM %s", table);
    tsdb_result_t *r = NULL;
    uint64_t h = 1469598103934665603ULL;
    int64_t rows = 0;
    if (tsdb_query(db, q, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            h = (h ^ (uint64_t)tsdb_result_ts(r, 0))  * 1099511628211ULL;
            h = (h ^ (uint64_t)tsdb_result_i64(r, 1)) * 1099511628211ULL;
            rows++;
        }
        tsdb_result_free(r);
    }
    if (out_rows) *out_rows = rows;
    return h;
}

static void make_tables(tsdb_db_t *db, int ntables) {
    for (int i = 0; i < ntables; i++) {
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "n",  TSDB_TYPE_INT64     },
        };
        OK(tsdb_create_table(db, TABLES[i], cols, 2, "ts"));
        tsdb_table_t *t = NULL;
        OK(tsdb_open_table(db, TABLES[i], &t));
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int k = 0; k < NROWS; k++) {
            OK(tsdb_batch_row_ts(b, TS0 + (int64_t)k * 1000000LL));
            OK(tsdb_batch_row_i64(b, 1, val_n((int64_t)i * NROWS + k)));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }
    OK(tsdb_db_flush_all(db));
}

static int dir_has_table(const char *dir, const char *table) {
    char p[4200];
    snprintf(p, sizeof(p), "%s/%s/schema.bin", dir, table);
    struct stat st;
    return (stat(p, &st) == 0);
}

static int file_exists(const char *p) {
    struct stat st;
    return (stat(p, &st) == 0);
}

/* ---- [C] helpers: forge a legacy V2 idx ---------------------------------- */

static void wr_u16(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static void wr_u32(uint8_t *p, uint32_t v) {
    for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8*i));
}
static void wr_u64(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8*i));
}

/* Rewrite a V3/V4 (88-byte-entry) idx as the V1/V2-era format: a 36-byte
 * header and 40-byte entries.  Bytes 0..39 of an entry are stable across
 * every version, so this is exactly the file an older build would have
 * written — the "old blocks stay readable" case. */
static void downgrade_idx_to_v2(const char *idx_path) {
    uint16_t ver = 0; uint32_t cnt = 0, esz = 0;
    uint64_t tot = 0; int64_t fmn = 0, fmx = 0;
    int hsz = tsdb_part_idx_probe(idx_path, &ver, &cnt, &esz, &tot, &fmn, &fmx, NULL);
    if (hsz <= 0) FAIL("probe %s failed (hsz=%d)", idx_path, hsz);
    if (esz != TSDB_IDX_ENTRY_SIZE)
        FAIL("expected an 88-byte-entry idx to downgrade, got esz=%u", esz);

    FILE *f = fopen(idx_path, "rb");
    if (!f) FAIL("open %s", idx_path);
    uint8_t *out = (uint8_t *)calloc(TSDB_IDX_HEADER_SIZE_V2 + (size_t)cnt * 40, 1);
    if (!out) FAIL("oom");
    uint8_t hdr[TSDB_IDX_HEADER_SIZE];
    if (fread(hdr, 1, (size_t)hsz, f) != (size_t)hsz) FAIL("read header");
    memcpy(out, hdr, 4);                     /* magic, verbatim */
    wr_u32(out + 4,  cnt);
    wr_u16(out + 8,  2);                     /* version = 2 */
    wr_u16(out + 10, 0);
    wr_u64(out + 12, tot);
    wr_u64(out + 20, (uint64_t)fmn);
    wr_u64(out + 28, (uint64_t)fmx);
    for (uint32_t i = 0; i < cnt; i++) {
        uint8_t e[TSDB_IDX_ENTRY_SIZE];
        if (fseeko(f, (off_t)hsz + (off_t)i * esz, SEEK_SET) != 0) FAIL("seek");
        if (fread(e, 1, esz, f) != esz) FAIL("read entry %u", i);
        memcpy(out + TSDB_IDX_HEADER_SIZE_V2 + (size_t)i * 40, e, 40);
    }
    fclose(f);

    FILE *w = fopen(idx_path, "wb");
    if (!w) FAIL("rewrite %s", idx_path);
    size_t total = TSDB_IDX_HEADER_SIZE_V2 + (size_t)cnt * 40;
    if (fwrite(out, 1, total, w) != total) FAIL("write %s", idx_path);
    fclose(w);
    free(out);
}

/* First partition dir name of `table` under `data_dir`. */
static void first_part(const char *data_dir, const char *table,
                       char *out, size_t cap) {
    out[0] = '\0';
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t k = strlen(e->d_name);
        if (k != 8 && k != 10) continue;
        int num = 1;
        for (size_t i = 0; i < k; i++)
            if (e->d_name[i] < '0' || e->d_name[i] > '9') { num = 0; break; }
        if (!num) continue;
        if (!out[0] || strcmp(e->d_name, out) < 0) snprintf(out, cap, "%s", e->d_name);
    }
    closedir(d);
}

/* Overwrite the backup's checkpoint sidecar so the stamp is always attempted,
 * independent of the durability mode (default mode leaves max_seq == 0). */
static void force_seq_sidecar(const char *backup_dir, const char *table,
                              const char *part, uint64_t seq)
{
    char p[4300];
    snprintf(p, sizeof(p), "%s/%s.seq", backup_dir, table);
    FILE *f = fopen(p, "wb");
    if (!f) FAIL("open %s", p);
    fprintf(f, "%s %llu\n", part, (unsigned long long)seq);
    fclose(f);
}

/* ---- [A] ----------------------------------------------------------------- */

static void phase_striped_backup(void) {
    rm_rf(MD_SRC); rm_rf(MD_D1); rm_rf(MD_D2); rm_rf(MD_BK);

    char dirs[8192];
    snprintf(dirs, sizeof(dirs), "%s,%s", MD_D1, MD_D2);
    setenv("TSDB_DATA_DIRS", dirs, 1);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(MD_SRC, &db));
    if (tsdb_db_data_dir_count(db) != 3)
        FAIL("expected 3 striped dirs, got %d", tsdb_db_data_dir_count(db));
    make_tables(db, NTABLES);

    /* The test is only meaningful if striping actually moved something off
     * dir 0 — otherwise every read in restore.c would happen to be right. */
    int off_dir0 = 0;
    for (int i = 0; i < NTABLES; i++)
        if (!dir_has_table(MD_SRC, TABLES[i])) off_dir0++;
    printf("[A] striped: %d of %d tables live off data dir 0\n", off_dir0, NTABLES);
    if (off_dir0 == 0)
        FAIL("striping placed every table on dir 0 — the case under test is "
             "unreachable, so this test would pass vacuously");

    /* Every table is readable through the live db regardless of its stripe. */
    for (int i = 0; i < NTABLES; i++) {
        int64_t c = count_rows(db, TABLES[i]);
        if (c != NROWS) FAIL("[A] live %s count=%lld want %d",
                             TABLES[i], (long long)c, NROWS);
    }

    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_backup_create(db, MD_BK, &rep);
    printf("[A] tsdb_backup_create rc=%d (%s) ntables=%d nfailed=%d complete=%d\n",
           rc, tsdb_errstr(rc), rep.ntables, rep.nfailed, rep.complete);
    if (rc == TSDB_OK) {
        uint64_t lost = 0;
        for (int i = 0; i < rep.ntables; i++)
            if (rep.tables[i].rows == 0) lost += NROWS;
        printf("[A] SILENT: the set reports complete=%d while holding %llu "
               "fewer rows than the database\n", rep.complete,
               (unsigned long long)lost);
    }
    tsdb_restore_report_free(&rep);

    if (rc != TSDB_ERR_UNSUPPORTED)
        FAIL("[A] a striped database must REFUSE to be backed up through data "
             "dir 0 (rc=%d, want TSDB_ERR_UNSUPPORTED=%d)", rc,
             TSDB_ERR_UNSUPPORTED);

    char mf[4300];
    snprintf(mf, sizeof(mf), "%s/%s", MD_BK, TSDB_BACKUP_MANIFEST);
    if (file_exists(mf))
        FAIL("[A] a refused backup wrote %s — an unrestorable set that "
             "looks restorable", mf);

    /* A refusal must not have cost availability either. */
    for (int i = 0; i < NTABLES; i++) {
        int64_t c = count_rows(db, TABLES[i]);
        if (c != NROWS) FAIL("[A] after the refusal %s count=%lld want %d",
                             TABLES[i], (long long)c, NROWS);
    }
    printf("[A] refused, no manifest, all %d tables still readable\n", NTABLES);

    /* VERIFY is the third door into the same blindness, and it is the call an
     * operator makes to ask "did my data survive?".  tsdb_migrate_digest and
     * the deep pass both read data dir 0, so on a striped db it must refuse
     * rather than answer about a table that is not where it looked. */
    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int vrc = tsdb_restore_verify(db, MD_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    printf("[A] tsdb_restore_verify(DEEP) on a striped db rc=%d (%s) nfailed=%d\n",
           vrc, tsdb_errstr(vrc), v.nfailed);
    tsdb_restore_verify_free(&v);
    if (vrc != TSDB_ERR_UNSUPPORTED)
        FAIL("[A] verifying a striped database must be REFUSED (rc=%d, want "
             "TSDB_ERR_UNSUPPORTED=%d) — it reads data dir 0 like every other "
             "path here", vrc, TSDB_ERR_UNSUPPORTED);

    tsdb_close(db);
    unsetenv("TSDB_DATA_DIRS");
}

/* ---- [B] ----------------------------------------------------------------- */

static void phase_striped_target(void) {
    rm_rf(PLAIN); rm_rf(PLAINBK); rm_rf(MD_DST); rm_rf(MD_D1); rm_rf(MD_D2);

    /* A GOOD single-dir set. */
    unsetenv("TSDB_DATA_DIRS");
    tsdb_db_t *src = NULL;
    OK(tsdb_open(PLAIN, &src));
    make_tables(src, NTABLES);
    tsdb_restore_report_t brep;
    memset(&brep, 0, sizeof(brep));
    int brc = tsdb_backup_create(src, PLAINBK, &brep);
    printf("[B] single-dir backup rc=%d ntables=%d complete=%d\n",
           brc, brep.ntables, brep.complete);
    if (brc != TSDB_OK || !brep.complete)
        FAIL("[B] the single-dir backup must succeed (rc=%d)", brc);
    for (int i = 0; i < brep.ntables; i++)
        if (brep.tables[i].rows != NROWS)
            FAIL("[B] backup of %s captured %llu rows, want %d",
                 brep.tables[i].table,
                 (unsigned long long)brep.tables[i].rows, NROWS);
    tsdb_restore_report_free(&brep);
    tsdb_close(src);

    /* ... restored into a STRIPED target. */
    char dirs[8192];
    snprintf(dirs, sizeof(dirs), "%s,%s", MD_D1, MD_D2);
    setenv("TSDB_DATA_DIRS", dirs, 1);
    tsdb_db_t *dst = NULL;
    OK(tsdb_open(MD_DST, &dst));
    if (tsdb_db_data_dir_count(dst) != 3)
        FAIL("[B] expected 3 striped dirs, got %d", tsdb_db_data_dir_count(dst));

    tsdb_restore_report_t rrep;
    memset(&rrep, 0, sizeof(rrep));
    int rc = tsdb_restore_run(dst, PLAINBK, &rrep);
    printf("[B] tsdb_restore_run into a striped target rc=%d (%s) "
           "ntables=%d nfailed=%d complete=%d\n",
           rc, tsdb_errstr(rc), rrep.ntables, rrep.nfailed, rrep.complete);
    if (rc == TSDB_OK) {
        int64_t missing = 0;
        for (int i = 0; i < NTABLES; i++) {
            int64_t c = count_rows(dst, TABLES[i]);
            if (c < NROWS) missing += (NROWS - (c < 0 ? 0 : c));
        }
        printf("[B] SILENT: rc=0 restore, %lld rows missing\n",
               (long long)missing);
    }
    tsdb_restore_report_free(&rrep);

    if (rc != TSDB_ERR_UNSUPPORTED)
        FAIL("[B] restoring into a striped database must be REFUSED (rc=%d, "
             "want TSDB_ERR_UNSUPPORTED=%d)", rc, TSDB_ERR_UNSUPPORTED);
    if (tsdb_restore_in_progress(MD_DST))
        FAIL("[B] a refused restore left %s behind — the marker must only "
             "exist for a restore that actually started", TSDB_RESTORE_MARKER);

    tsdb_close(dst);
    unsetenv("TSDB_DATA_DIRS");
    printf("[B] refused before the marker was written\n");
}

/* ---- [C] ----------------------------------------------------------------- */

static void phase_legacy_idx(void) {
    rm_rf(LEG_SRC); rm_rf(LEG_BK); rm_rf(LEG_DST);
    unsetenv("TSDB_DATA_DIRS");

    tsdb_db_t *src = NULL;
    OK(tsdb_open(LEG_SRC, &src));
    make_tables(src, 1);
    int64_t src_rows = 0;
    uint64_t src_dig = digest_rows(src, TABLES[0], &src_rows);
    tsdb_restore_report_t brep;
    memset(&brep, 0, sizeof(brep));
    OK(tsdb_backup_create(src, LEG_BK, &brep));
    tsdb_restore_report_free(&brep);
    tsdb_close(src);

    /* First restore: the target is empty, so every block lands normally. */
    tsdb_db_t *dst = NULL;
    OK(tsdb_open(LEG_DST, &dst));
    tsdb_restore_report_t r1;
    memset(&r1, 0, sizeof(r1));
    OK(tsdb_restore_run(dst, LEG_BK, &r1));
    tsdb_restore_report_free(&r1);
    int64_t got_rows = 0;
    uint64_t got_dig = digest_rows(dst, TABLES[0], &got_rows);
    if (got_rows != src_rows || got_dig != src_dig)
        FAIL("[C] first restore already wrong: rows=%lld/%lld digest=%llu/%llu",
             (long long)got_rows, (long long)src_rows,
             (unsigned long long)got_dig, (unsigned long long)src_dig);
    tsdb_close(dst);

    /* Downgrade the landed partition to the legacy on-disk format — the
     * mixed-version state the "old blocks stay readable" invariant covers. */
    char part[64];
    first_part(LEG_DST, TABLES[0], part, sizeof(part));
    if (!part[0]) FAIL("[C] no partition landed");
    char ts_idx[4400];
    snprintf(ts_idx, sizeof(ts_idx), "%s/%s/%s/ts.idx", LEG_DST, TABLES[0], part);
    downgrade_idx_to_v2(ts_idx);

    uint16_t ver = 0; uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(ts_idx, &ver, &cnt, &esz, NULL, NULL, NULL, NULL);
    printf("[C] forged legacy idx: hdr=%d v%u entries=%u esz=%u\n",
           hsz, ver, cnt, esz);
    if (ver != 2 || esz != TSDB_IDX_ENTRY_SIZE_V2)
        FAIL("[C] downgrade did not produce a V2/40-byte idx (v%u esz=%u)", ver, esz);

    OK(tsdb_open(LEG_DST, &dst));
    int64_t pre_rows = 0;
    uint64_t pre_dig = digest_rows(dst, TABLES[0], &pre_rows);
    printf("[C] legacy partition reads: rows=%lld count(*)=%lld\n",
           (long long)pre_rows, (long long)count_rows(dst, TABLES[0]));
    if (pre_rows != src_rows || pre_dig != src_dig)
        FAIL("[C] the legacy idx is not readable before the restore "
             "(rows=%lld want %lld) — the premise of this phase is broken",
             (long long)pre_rows, (long long)src_rows);

    /* Force a checkpoint into the sidecar so the stamp is attempted in BOTH
     * durability modes; default mode leaves max_seq == 0 and would skip it. */
    force_seq_sidecar(LEG_BK, TABLES[0], part, 7);

    /* Exactly what a restore killed between the stamp's tmp write and its
     * rename leaves behind.  Nothing used to remove it — one per crashed
     * restore, forever — so the next stamp has to collect it, including on
     * the decline path taken immediately below. */
    char tmp[4500];
    snprintf(tmp, sizeof(tmp), "%s.rsttmp", ts_idx);
    {
        FILE *junk = fopen(tmp, "wb");
        if (!junk) FAIL("[C] could not plant %s", tmp);
        fwrite("stale", 1, 5, junk);
        fclose(junk);
    }

    /* Replay.  Every block dedup-skips, so tsdb_rawblock_apply — which would
     * widen the entries to 88 bytes — never runs, and the stamp is the only
     * writer that touches ts.idx. */
    tsdb_restore_report_t r2;
    memset(&r2, 0, sizeof(r2));
    int rc = tsdb_restore_run(dst, LEG_BK, &r2);
    printf("[C] replay rc=%d (%s) seen=%llu landed=%llu skipped=%llu\n",
           rc, tsdb_errstr(rc),
           (unsigned long long)r2.tables[0].blocks_seen,
           (unsigned long long)r2.tables[0].blocks_landed,
           (unsigned long long)r2.tables[0].blocks_skipped);
    if (r2.tables[0].blocks_landed != 0)
        FAIL("[C] the replay landed %llu blocks — the stamp is no longer the "
             "only writer, so this phase is not testing what it claims",
             (unsigned long long)r2.tables[0].blocks_landed);
    tsdb_restore_report_free(&r2);
    OK(rc);

    int64_t post_rows = 0;
    uint64_t post_dig = digest_rows(dst, TABLES[0], &post_rows);
    int64_t post_cnt = count_rows(dst, TABLES[0]);
    printf("[C] after the replay: rows=%lld count(*)=%lld\n",
           (long long)post_rows, (long long)post_cnt);
    tsdb_close(dst);

    /* And from a FRESH process view of the same directory. */
    OK(tsdb_open(LEG_DST, &dst));
    int64_t re_rows = 0;
    uint64_t re_dig = digest_rows(dst, TABLES[0], &re_rows);
    int64_t re_cnt = count_rows(dst, TABLES[0]);
    tsdb_close(dst);

    hsz = tsdb_part_idx_probe(ts_idx, &ver, &cnt, &esz, NULL, NULL, NULL, NULL);
    printf("[C] ts.idx after the replay: hdr=%d v%u entries=%u esz=%u\n",
           hsz, ver, cnt, esz);

    if (post_rows != src_rows || post_dig != src_dig || post_cnt != src_rows)
        FAIL("[C] the checkpoint stamp re-headed a legacy idx: rows=%lld "
             "count(*)=%lld want %lld (digest %llu vs %llu) — restore returned "
             "TSDB_OK", (long long)post_rows, (long long)post_cnt,
             (long long)src_rows, (unsigned long long)post_dig,
             (unsigned long long)src_dig);
    if (re_rows != src_rows || re_dig != src_dig || re_cnt != src_rows)
        FAIL("[C] reopen sees %lld rows / count(*)=%lld, want %lld",
             (long long)re_rows, (long long)re_cnt, (long long)src_rows);
    if (esz != TSDB_IDX_ENTRY_SIZE_V2)
        FAIL("[C] the legacy idx entry stride changed to %u under a header "
             "that does not carry 88-byte entries", esz);

    if (file_exists(tmp))
        FAIL("[C] the stamp left the stale %s behind", tmp);

    printf("[C] legacy idx untouched, %lld rows still readable\n",
           (long long)re_rows);
}

/* ---- [D] ----------------------------------------------------------------- */

#define NC_TBL "t_nc"

/* ts, a, b — `b` is the LAST column, so rule 1 of part_col_absence_is_late_add
 * ("some later column still has blocks") is vacuous for it and the ts.idx
 * ncols stamp is the only thing that can classify its absence. */
static void nc_make(tsdb_db_t *db) {
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "a",  TSDB_TYPE_INT64     },
        { "b",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, NC_TBL, cols, 3, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, NC_TBL, &t));
    tsdb_batch_t *bt = NULL;
    OK(tsdb_batch_begin(t, &bt));
    for (int k = 0; k < NROWS; k++) {
        OK(tsdb_batch_row_ts(bt, TS0 + (int64_t)k * 1000000LL));
        OK(tsdb_batch_row_i64(bt, 1, val_n(k)));
        OK(tsdb_batch_row_i64(bt, 2, val_n(k) + 5));
        OK(tsdb_batch_row_end(bt));
    }
    OK(tsdb_batch_commit(bt));
    OK(tsdb_db_flush_all(db));
}

/* SELECT b FROM t_nc — rows returned, and whether the query failed. */
static int64_t nc_read_b(tsdb_db_t *db, int *out_rc, int64_t *out_zeros) {
    tsdb_result_t *r = NULL;
    int64_t rows = 0, zeros = 0;
    int rc = tsdb_query(db, "SELECT b FROM " NC_TBL, &r);
    if (rc == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            if (tsdb_result_i64(r, 0) == 0) zeros++;
            rows++;
        }
    }
    if (r) tsdb_result_free(r);
    if (out_rc)    *out_rc = rc;
    if (out_zeros) *out_zeros = zeros;
    return rows;
}

static void phase_ncols_stamp(void) {
    rm_rf(NC_SRC); rm_rf(NC_BK); rm_rf(NC_DST);
    unsetenv("TSDB_DATA_DIRS");

    tsdb_db_t *src = NULL;
    OK(tsdb_open(NC_SRC, &src));
    nc_make(src);
    tsdb_restore_report_t brep;
    memset(&brep, 0, sizeof(brep));
    OK(tsdb_backup_create(src, NC_BK, &brep));
    tsdb_restore_report_free(&brep);
    tsdb_close(src);

    /* The target is a LIVE database holding the same rows, written by its own
     * FLUSH — not an empty directory.  That matters: a partition built purely
     * by tsdb_rawblock_apply (replication, or a restore into nothing) carries
     * no ncols stamp to begin with and part.c documents that as expected,
     * whereas a flushed partition carries the real one.  So this is the shape
     * in which the checkpoint stamp has something to DESTROY, and it is the
     * API's headline case: restore into a live database, and re-running a
     * restore that already completed. */
    tsdb_db_t *dst = NULL;
    OK(tsdb_open(NC_DST, &dst));
    nc_make(dst);
    tsdb_close(dst);

    char part[64];
    first_part(NC_DST, NC_TBL, part, sizeof(part));
    if (!part[0]) FAIL("[D] the target flushed no partition");

    char ts_idx[4400];
    snprintf(ts_idx, sizeof(ts_idx), "%s/%s/%s/ts.idx", NC_DST, NC_TBL, part);
    uint16_t pre_stamp = tsdb_part_idx_ncols(ts_idx);
    printf("[D] target partition %s written by a flush: ts.idx ncols=%u\n",
           part, pre_stamp);
    if (pre_stamp != 3)
        FAIL("[D] the flush did not stamp ncols (got %u, want 3) — the premise "
             "of this phase is broken and it would pass vacuously", pre_stamp);

    /* rst_apply_seq_sidecar declines to stamp over a table that still has its
     * own redo log, and the stamp is the code under test.  Everything is
     * already durable (flushed and closed), so the log is pure redo for data
     * on disk — dropping it is what a checkpoint does anyway. */
    {
        char wal[4300];
        snprintf(wal, sizeof(wal), "%s/wal/%s.log", NC_DST, NC_TBL);
        struct stat ws;
        if (stat(wal, &ws) == 0) {
            printf("[D] clearing the target's own redo log (%lld bytes)\n",
                   (long long)ws.st_size);
            if (truncate(wal, 0) != 0) FAIL("[D] could not truncate %s", wal);
        }
    }

    /* Force a checkpoint so the stamp runs in BOTH durability modes — in the
     * default mode max_seq is 0 and rst_stamp_max_seq returns early, which
     * would make this phase pass without exercising the writer at all. */
    force_seq_sidecar(NC_BK, NC_TBL, part, 11);

    OK(tsdb_open(NC_DST, &dst));
    tsdb_restore_report_t rrep;
    memset(&rrep, 0, sizeof(rrep));
    OK(tsdb_restore_run(dst, NC_BK, &rrep));
    uint64_t landed = rrep.ntables > 0 ? rrep.tables[0].blocks_landed : 0;
    uint64_t skipped = rrep.ntables > 0 ? rrep.tables[0].blocks_skipped : 0;
    tsdb_restore_report_free(&rrep);
    printf("[D] replay into the live target: landed=%llu skipped=%llu\n",
           (unsigned long long)landed, (unsigned long long)skipped);

    int64_t rc_rows = count_rows(dst, NC_TBL);
    if (rc_rows != NROWS)
        FAIL("[D] after the replay count(*)=%lld, want %d",
             (long long)rc_rows, NROWS);
    tsdb_close(dst);

    /* Every block dedup-skips, so tsdb_rawblock_apply — which re-stamps ncols
     * from the file it is appending to — never runs, and the checkpoint stamp
     * is the ONLY writer that touches ts.idx. */
    if (landed != 0)
        FAIL("[D] the replay landed %llu blocks, so the checkpoint stamp is "
             "not the only writer here and this phase is not testing what it "
             "claims", (unsigned long long)landed);

    uint64_t mseq = 0; uint16_t ver = 0;
    int hsz = tsdb_part_idx_probe(ts_idx, &ver, NULL, NULL, NULL, NULL, NULL, &mseq);
    uint16_t stamp = tsdb_part_idx_ncols(ts_idx);
    printf("[D] after the restore: ts.idx hdr=%d v%u max_seq=%llu ncols=%u\n",
           hsz, ver, (unsigned long long)mseq, stamp);

    /* Without this the phase is vacuous: if the stamp never ran, ncols could
     * not have been clobbered and the assertion below would prove nothing. */
    if (mseq != 11)
        FAIL("[D] the checkpoint stamp did not run (max_seq=%llu, want 11) — "
             "this phase would pass without exercising the writer",
             (unsigned long long)mseq);

    /* The behavioural half FIRST, so a failure reports the wrong ANSWER the
     * clobbered stamp produces and not merely the clobbered field.  Take
     * column `b`'s blocks away — exactly the state the stamp's classification
     * is asked about — and read it back. */
    char bidx[4500], bcol[4500];
    snprintf(bidx, sizeof(bidx), "%s/%s/%s/b.idx", NC_DST, NC_TBL, part);
    snprintf(bcol, sizeof(bcol), "%s/%s/%s/b.col", NC_DST, NC_TBL, part);
    if (unlink(bidx) != 0) FAIL("[D] could not remove %s", bidx);
    unlink(bcol);

    OK(tsdb_open(NC_DST, &dst));
    int brc = 0; int64_t zeros = 0;
    int64_t rows = nc_read_b(dst, &brc, &zeros);
    int64_t still = count_rows(dst, NC_TBL);
    printf("[D] SELECT b with b's blocks gone: rc=%d (%s) rows=%lld zeros=%lld "
           "| count(*)=%lld\n", brc, tsdb_errstr(brc), (long long)rows,
           (long long)zeros, (long long)still);
    tsdb_close(dst);

    if (brc == TSDB_OK)
        FAIL("[D] SELECT b answered rc=0 with %lld rows (%lld of them zero) "
             "for a column whose %d values are GONE — a silent wrong answer "
             "produced by the restore checkpoint, not by the write path "
             "(ts.idx ncols stamp is %u, want 3)",
             (long long)rows, (long long)zeros, NROWS, stamp);
    if (still != NROWS)
        FAIL("[D] count(*) became %lld — a hole in one column must not cost "
             "the queries that do not need it", (long long)still);
    if (stamp != 3)
        FAIL("[D] the checkpoint stamp rewrote ts.idx's ncols to %u (want 3). "
             "That field is rule 2 of part_col_absence_is_late_add: with it "
             "gone, a trailing column whose blocks are missing is classified "
             "as an ALTER-added column and ZERO-FILLED instead of reported as "
             "a hole", stamp);

    printf("[D] ncols stamp carried through; a gone column reads as an error\n");
}

/* ---- [E] ----------------------------------------------------------------- */

#define HR_TBL "t_hr"

static void phase_hour_backup(void) {
    rm_rf(HR_SRC); rm_rf(HR_BK);
    unsetenv("TSDB_DATA_DIRS");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(HR_SRC, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "n",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table_ex(db, HR_TBL, cols, 2, "ts", TSDB_CREATE_PART_HOUR));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, HR_TBL, &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    /* Two hours, so the partition directories really are YYYYMMDDHH. */
    for (int k = 0; k < NROWS; k++) {
        OK(tsdb_batch_row_ts(b, TS0 + (int64_t)k * 10000000000LL));
        OK(tsdb_batch_row_i64(b, 1, val_n(k)));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));

    char part[64];
    first_part(HR_SRC, HR_TBL, part, sizeof(part));
    if (strlen(part) != 10)
        FAIL("[E] expected an HOUR partition dir (YYYYMMDDHH), got '%s' — the "
             "case under test is unreachable", part);

    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_backup_create(db, HR_BK, &rep);
    printf("[E] tsdb_backup_create of an HOUR table rc=%d (%s) ntables=%d "
           "nfailed=%d complete=%d\n", rc, tsdb_errstr(rc), rep.ntables,
           rep.nfailed, rep.complete);
    int trc = rep.ntables > 0 ? rep.tables[0].rc : TSDB_OK;
    tsdb_restore_report_free(&rep);

    if (rc != TSDB_ERR_UNSUPPORTED || trc != TSDB_ERR_UNSUPPORTED)
        FAIL("[E] backing up an HOUR-granularity table must be REFUSED at "
             "BACKUP time (rc=%d table_rc=%d, want TSDB_ERR_UNSUPPORTED=%d) — "
             "refusing only at restore means the operator finds out during the "
             "incident, from a set that reported complete=1",
             rc, trc, TSDB_ERR_UNSUPPORTED);

    char mf[4300];
    snprintf(mf, sizeof(mf), "%s/%s", HR_BK, TSDB_BACKUP_MANIFEST);
    if (file_exists(mf))
        FAIL("[E] a refused backup wrote %s — a set that cannot be restored "
             "must not look restorable", mf);

    if (count_rows(db, HR_TBL) != NROWS)
        FAIL("[E] the refusal cost availability");
    tsdb_close(db);
    printf("[E] refused at backup time, no manifest, table still readable\n");
}

int main(int argc, char **argv) {
    const char *only = (argc > 1) ? argv[1] : NULL;
    printf("=== test_restore_multidir ===\n");
    if (!only || !strcmp(only, "A")) phase_striped_backup();
    if (!only || !strcmp(only, "B")) phase_striped_target();
    if (!only || !strcmp(only, "C")) phase_legacy_idx();
    if (!only || !strcmp(only, "D")) phase_ncols_stamp();
    if (!only || !strcmp(only, "E")) phase_hour_backup();

    rm_rf(MD_SRC); rm_rf(MD_D1); rm_rf(MD_D2); rm_rf(MD_BK);
    rm_rf(PLAIN); rm_rf(PLAINBK); rm_rf(MD_DST);
    rm_rf(LEG_SRC); rm_rf(LEG_BK); rm_rf(LEG_DST);
    rm_rf(NC_SRC); rm_rf(NC_BK); rm_rf(NC_DST);
    rm_rf(HR_SRC); rm_rf(HR_BK);

    printf("\n=== test_restore_multidir PASSED ===\n");
    return 0;
}
