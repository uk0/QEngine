/* migrate.c — cross-cluster bulk data movement.  See include/tsdb_migrate.h.
 *
 * The stream is a header, a run of block records, and a terminator:
 *
 *   header  : "TSM1" u32 | version u32 | table u8[64] | ncols u32
 *             | ts_col_idx u32 | { name u8[64], type u32 } * ncols
 *   symbols : nsym u32 | { col_idx u32, len u32, .sym bytes } * nsym   (v2)
 *   record  : len u32 (>0) | tsdb_rawblock_serialize() payload
 *   end     : len u32 == 0
 *
 * The symbol section is not optional: a SYMBOL column's blocks hold dictionary
 * CODES, so shipping them without the dictionary produced a target where every
 * tag value queried back as zero rows.
 *
 * Block payloads are produced by the SAME serializer replication uses, and
 * landed by the SAME applier, so migration inherits its wire encoding and its
 * idempotence instead of growing a second, subtly-different copy of both.
 *
 * A block's payload is the COMPRESSED bytes only — the 32-byte block header
 * and the CRC trailer are reconstructed on the target from the metadata
 * travelling alongside, exactly as part.c's raw-block hook hands them to a
 * replica (part.c ~:787-800: comp_buf/comp_bytes plus a tsdb_block_meta_t).
 */
#define _POSIX_C_SOURCE 200809L

#include "../../include/tsdb_migrate.h"

#include "db.h"
#include "part.h"
#include "schema.h"
#include "migrate_internal.h"
#include "../cluster/rawblock.h"
#include "../core/symbol.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define MIG_NAME_BYTES 64

/* ---- byte helpers -------------------------------------------------------- */

static void put_u32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}
static uint32_t get_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* write_all / read_all: a short write is an error, a short read at record
 * boundary zero is EOF (handled by the caller), anything else is corruption. */
static int write_all(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) { if (errno == EINTR) continue; return TSDB_ERR_IO; }
        if (w == 0) return TSDB_ERR_IO;
        p += (size_t)w; n -= (size_t)w;
    }
    return TSDB_OK;
}
static int read_all(int fd, void *buf, size_t n, int *got_eof) {
    uint8_t *p = (uint8_t *)buf;
    size_t want = n;
    if (got_eof) *got_eof = 0;
    while (n) {
        ssize_t r = read(fd, p, n);
        if (r < 0) { if (errno == EINTR) continue; return TSDB_ERR_IO; }
        if (r == 0) {
            if (got_eof && n == want) { *got_eof = 1; return TSDB_OK; }
            return TSDB_ERR_CORRUPT;    /* truncated mid-item */
        }
        p += (size_t)r; n -= (size_t)r;
    }
    return TSDB_OK;
}

/* Order-independent fold, so source and target agree regardless of the order
 * partitions/columns happen to be enumerated in. */
static uint64_t digest_block(uint32_t day, uint16_t col, int64_t ts_min,
                             int64_t ts_max, uint32_t count)
{
    uint64_t h = 1469598103934665603ULL;
    uint64_t f[5] = { day, col, (uint64_t)ts_min, (uint64_t)ts_max, count };
    for (int i = 0; i < 5; i++) { h ^= f[i]; h *= 1099511628211ULL; }
    return h;
}

/* ---- partition enumeration ---------------------------------------------- */

static int is_part_name(const char *n) {
    size_t k = strlen(n);
    if (k != 8 && k != 10) return 0;
    for (size_t i = 0; i < k; i++) if (n[i] < '0' || n[i] > '9') return 0;
    return 1;
}

/* Collect partition dir names of <data_dir>/<table>, sorted for reproducible
 * streams.  Returns count, or -1. */
static int list_parts(const char *tbl_dir, char ***out) {
    DIR *d = opendir(tbl_dir);
    if (!d) return -1;
    char **v = NULL; int n = 0, cap = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!is_part_name(e->d_name)) continue;
        if (n == cap) {
            int nc = cap ? cap * 2 : 16;
            char **nv = realloc(v, (size_t)nc * sizeof(*nv));
            if (!nv) { closedir(d); goto oom; }
            v = nv; cap = nc;
        }
        v[n] = strdup(e->d_name);
        if (!v[n]) { closedir(d); goto oom; }
        n++;
    }
    closedir(d);
    for (int i = 1; i < n; i++) {           /* insertion sort: n is small */
        char *k = v[i]; int j = i - 1;
        while (j >= 0 && strcmp(v[j], k) > 0) { v[j + 1] = v[j]; j--; }
        v[j + 1] = k;
    }
    *out = v;
    return n;
oom:
    for (int i = 0; i < n; i++) free(v[i]);
    free(v);
    return -1;
}

/* ---- export -------------------------------------------------------------- */

static int write_header(int fd, const char *table, tsdb_schema_t *s) {
    uint8_t hdr[4 + 4 + MIG_NAME_BYTES + 4 + 4];
    memset(hdr, 0, sizeof(hdr));
    put_u32(hdr + 0, TSDB_MIG_MAGIC);
    put_u32(hdr + 4, TSDB_MIG_VERSION);
    snprintf((char *)hdr + 8, MIG_NAME_BYTES, "%s", table);
    put_u32(hdr + 8 + MIG_NAME_BYTES, (uint32_t)s->ncols);
    put_u32(hdr + 12 + MIG_NAME_BYTES, (uint32_t)s->ts_col_idx);
    int rc = write_all(fd, hdr, sizeof(hdr));
    if (rc != TSDB_OK) return rc;

    for (int i = 0; i < s->ncols; i++) {
        uint8_t c[MIG_NAME_BYTES + 4];
        memset(c, 0, sizeof(c));
        snprintf((char *)c, MIG_NAME_BYTES, "%s", s->cols[i].name);
        put_u32(c + MIG_NAME_BYTES, (uint32_t)s->cols[i].type);
        rc = write_all(fd, c, sizeof(c));
        if (rc != TSDB_OK) return rc;
    }

    /* SYMBOL dictionary section: u32 count, then { col_idx u32, len u32, bytes }.
     * A SYMBOL column's blocks hold dictionary CODES, so without the dictionary
     * the target decodes them against its own empty table and every tag reads
     * back as nothing. */
    uint32_t nsym = 0;
    for (int i = 0; i < s->ncols; i++)
        if (s->cols[i].type == TSDB_TYPE_SYMBOL) nsym++;
    uint8_t nb[4]; put_u32(nb, nsym);
    rc = write_all(fd, nb, 4);
    if (rc != TSDB_OK) return rc;

    for (int i = 0; i < s->ncols && rc == TSDB_OK; i++) {
        if (s->cols[i].type != TSDB_TYPE_SYMBOL) continue;

        /* Persist the live table's dictionary first: the on-disk .sym is only
         * rewritten at close/flush, so reading the file alone can miss codes
         * this process has interned. */
        char sym_path[4400];
        snprintf(sym_path, sizeof(sym_path), "%s/%s.sym", s->dir, s->cols[i].name);
        if (s->cols[i].symtab) tsdb_symtab_save(s->cols[i].symtab, sym_path);

        uint8_t *bytes = NULL; size_t len = 0;
        FILE *sf = fopen(sym_path, "rb");
        if (sf) {
            if (fseeko(sf, 0, SEEK_END) == 0) {
                off_t sz = ftello(sf);
                if (sz > 0 && fseeko(sf, 0, SEEK_SET) == 0) {
                    bytes = malloc((size_t)sz);
                    if (bytes && fread(bytes, 1, (size_t)sz, sf) == (size_t)sz)
                        len = (size_t)sz;
                    else { free(bytes); bytes = NULL; }
                }
            }
            fclose(sf);
        }

        uint8_t hb[8];
        put_u32(hb, (uint32_t)i);
        put_u32(hb + 4, (uint32_t)len);
        rc = write_all(fd, hb, 8);
        if (rc == TSDB_OK && len) rc = write_all(fd, bytes, len);
        free(bytes);
    }
    return rc;
}

int tsdb_migrate_export(tsdb_db_t *db, const char *table, int fd,
                        const tsdb_mig_opts_t *opts, tsdb_mig_stats_t *out)
{
    if (!db || !table || fd < 0) return TSDB_ERR_INVAL;

    /* Resolve through tsdb_open_table, not tsdb_db_find_table: the latter only
     * walks the tables this process has already opened, so a table sitting on
     * disk untouched reported NOTFOUND even though a SELECT on the same handle
     * reads it fine.  Its rc IS the answer — NOTFOUND when the table really is
     * absent, anything else a real error. */
    tsdb_table_t *th = NULL;
    int orc = tsdb_open_table(db, table, &th);
    if (orc != TSDB_OK) return orc;
    tsdb_table_internal_t *ti = tsdb_db_find_table(db, table);
    if (!ti) return TSDB_ERR_INTERNAL;
    tsdb_schema_t *s = tsdb_tbl_schema(ti);
    if (!s) return TSDB_ERR_INTERNAL;

    /* COMPLETENESS.  The scan below reads on-disk partitions only; rows in the
     * memtable are not there, and under deferred flush that is most of the
     * recent data.  Default: flush the source so the stream is complete.  With
     * no_flush: refuse (TSDB_ERR_BUSY) if the memtable holds rows rather than
     * ship a silently-short stream — an export must not report success while
     * carrying less than the table.
     *
     * The default flush uses the normal replicating path, so on a cluster the
     * drained rows fan out to peers as a side effect of the export.  That is
     * deliberate over the alternative (a skip-replicate flush): the rows are
     * already committed and DO belong on the peers, so replicating them early
     * is harmless, whereas a skip-replicate flush would clear the memtable
     * without ever handing those rows to the cluster hook — and a memtable
     * flushes once, so any row not yet replicated would be lost to replication
     * for good (the same trap a restore-verify skip-replicate flush was
     * rejected for).  A caller that must not mutate or replicate the source
     * uses no_flush and accepts the BUSY refusal on un-flushed data. */
    if (!opts || !opts->no_flush) {
        int frc = tsdb_table_flush(db, table);
        if (frc != TSDB_OK) return frc;
    } else {
        tsdb_memtable_t *mt = tsdb_tbl_memtable(ti);
        if (mt && tsdb_memtable_rows(mt) > 0) return TSDB_ERR_BUSY;
    }

    tsdb_mig_stats_t st;
    memset(&st, 0, sizeof(st));

    int rc = write_header(fd, table, s);
    if (rc != TSDB_OK) return rc;

    const char *data_dir = tsdb_db_data_dir(db);
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", data_dir, table);

    char **parts = NULL;
    int nparts = list_parts(tbl_dir, &parts);
    if (nparts < 0) nparts = 0;          /* no partitions yet — empty stream */

    for (int pi = 0; pi < nparts && rc == TSDB_OK; pi++) {
        char part_dir[4200];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", tbl_dir, parts[pi]);

        tsdb_part_t *p = NULL;
        if (tsdb_part_open(s, part_dir, &p) != TSDB_OK || !p) continue;
        st.partitions++;

        /* The partition dir name is YYYYMMDD (DAY) or YYYYMMDDHH (HOUR); the
         * raw-block record carries the day form the applier rebuilds from. */
        uint32_t day = (uint32_t)strtoul(parts[pi], NULL, 10);
        if (strlen(parts[pi]) == 10) day = (uint32_t)(strtoull(parts[pi], NULL, 10) / 100);

        /* Emit the TS column LAST, exactly like the flush path (part.c) and
         * for the same reason: ts.idx is the partition's visibility marker, so
         * a stream truncated mid-partition must leave the target BEHIND, never
         * TORN.  Iterating schema order put ts FIRST (ts_col_idx is
         * conventionally 0), which made a truncated or interrupted import a
         * DETERMINISTIC multi-column hole rather than a race. */
        int ci_iter[TSDB_MAX_COLS];
        int ci_count = 0;
        for (int ci = 0; ci < s->ncols; ci++)
            if (ci != s->ts_col_idx) ci_iter[ci_count++] = ci;
        if (s->ts_col_idx >= 0 && s->ts_col_idx < s->ncols)
            ci_iter[ci_count++] = s->ts_col_idx;

        char col_col_path[4400];
        for (int ix = 0; ix < ci_count && rc == TSDB_OK; ix++) {
            int ci = ci_iter[ix];
            tsdb_block_meta_t *metas = NULL; size_t nb = 0;
            if (tsdb_part_col_blocks(p, ci, &metas, &nb) != TSDB_OK) continue;

            snprintf(col_col_path, sizeof(col_col_path), "%s/%s.col",
                     part_dir, s->cols[ci].name);
            FILE *cf = fopen(col_col_path, "rb");
            if (!cf) { free(metas); continue; }

            for (size_t bi = 0; bi < nb && rc == TSDB_OK; bi++) {
                const tsdb_block_meta_t *m = &metas[bi];
                if (m->size == 0) continue;

                uint8_t *payload = malloc(m->size);
                if (!payload) { rc = TSDB_ERR_NOMEM; break; }
                /* Skip the 32-byte block header: the applier rebuilds it from
                 * the metadata, exactly like the replication hook. */
                if (fseeko(cf, (off_t)m->offset + TSDB_BLOCK_HEADER_SIZE, SEEK_SET) != 0 ||
                    fread(payload, 1, m->size, cf) != m->size) {
                    free(payload); rc = TSDB_ERR_IO; break;
                }

                tsdb_rawblock_push_t r;
                memset(&r, 0, sizeof(r));
                snprintf(r.table, sizeof(r.table), "%s", table);
                r.part_day        = day;
                r.col_idx         = (uint16_t)ci;
                r.codec           = m->codec;
                r.flags           = m->flags;
                r.count           = m->count;
                r.ts_min          = m->ts_min;
                r.ts_max          = m->ts_max;
                r.stats_min       = m->stats_min;
                r.stats_max       = m->stats_max;
                r.stats_sum       = m->stats_sum;
                r.stats_first     = m->stats_first;
                r.stats_last      = m->stats_last;
                r.stats_flags     = m->stats_flags;
                r.ord             = m->ord;
                r.block_bytes_len = m->size;
                r.block_bytes     = payload;

                uint8_t *buf = NULL; size_t len = 0;
                if (tsdb_rawblock_serialize(&r, &buf, &len) != TSDB_OK) {
                    free(payload); rc = TSDB_ERR_INTERNAL; break;
                }
                uint8_t lb[4]; put_u32(lb, (uint32_t)len);
                rc = write_all(fd, lb, 4);
                if (rc == TSDB_OK) rc = write_all(fd, buf, len);
                free(buf);
                free(payload);
                if (rc != TSDB_OK) break;

                st.blocks++;
                /* Count rows once per row, not once per column: every column
                 * of a partition carries the same row count. */
                if (ci == s->ts_col_idx) st.rows += m->count;
                st.payload_bytes += m->size;
                st.digest        ^= digest_block(day, (uint16_t)ci,
                                                 m->ts_min, m->ts_max, m->count);
            }
            fclose(cf);
            free(metas);
        }
        tsdb_part_close(p);
    }

    for (int i = 0; i < nparts; i++) free(parts[i]);
    free(parts);

    if (rc == TSDB_OK) {                    /* terminator */
        uint8_t z[4]; put_u32(z, 0);
        rc = write_all(fd, z, 4);
    }
    if (rc == TSDB_OK && out) *out = st;
    return rc;
}


/* ---- import-side dedup ---------------------------------------------------
 *
 * tsdb_rawblock_apply only skips a block that matches the TAIL of the target
 * index — right for replication, where blocks arrive in order and only the
 * last one can be a repeat, but not enough to replay a whole stream: every
 * record would append again.  So check the target's existing blocks properly.
 *
 * The stream is emitted partition-major then column-major, so one cached
 * signature list per (day, column) covers the whole run.
 */
/* The resume key, and why it is CONTENT and not the block's durable ordinal.
 *
 * The ordinal is partition-local to the node that ISSUED it.  This list is
 * primed from the TARGET's index while the incoming block carries the SOURCE's
 * numbering, so ordinal N of the stream and ordinal N of the target name
 * unrelated rows — matching on it dropped live blocks at a brand-new site, and
 * because this is a PRE-FILTER it did so without the applier ever seeing them.
 * (tsdb_rawblock_apply translates the stream's ordinal into the target's space;
 * that translation is the only place a remote ordinal means anything.)
 *
 * So: content, widened from (ts_min, count) to include ts_max and the block's
 * byte size — the same four fields the applier's own idempotency test compares.
 *
 * NO TEST COVERS THE WIDENING, and it is kept anyway: no fixture builds two
 * blocks of one (day, col) that agree on (ts_min, count) and differ in ts_max
 * or size, so nothing goes red without it.  What it buys is that this
 * PRE-FILTER cannot be coarser than the applier it filters for.  A coarser key
 * skips a block the applier would have kept, and a skip here is silent — the
 * applier never sees the block at all.
 *
 * And it is CONSUMED, not just matched.  One token per block the target already
 * holds: a duplicate-timestamp run of three blocks against a target holding one
 * of them skips one and lands two, where a plain "is this key present" test
 * skipped all three.  A repeat inside one run needs no token — the applier's
 * translated-ordinal test absorbs it exactly. */
typedef struct {
    uint32_t day; uint16_t col; int64_t ts_min; int64_t ts_max;
    uint32_t count; uint32_t size;
    int      taken;
} mig_sig_t;

typedef struct {
    mig_sig_t *v; size_t n, cap;        /* blocks the TARGET already holds   */
    uint64_t  *loaded; size_t ln, lcap; /* (day,col) pairs already scanned  */
} mig_seen_t;

static void mig_seen_free(mig_seen_t *sn) { free(sn->v); free(sn->loaded); memset(sn, 0, sizeof(*sn)); }

static int mig_seen_add(mig_seen_t *sn, uint32_t day, uint16_t col,
                        int64_t ts_min, int64_t ts_max,
                        uint32_t count, uint32_t size) {
    if (sn->n == sn->cap) {
        size_t nc = sn->cap ? sn->cap * 2 : 256;
        mig_sig_t *nv = realloc(sn->v, nc * sizeof(*nv));
        if (!nv) return TSDB_ERR_NOMEM;
        sn->v = nv; sn->cap = nc;
    }
    sn->v[sn->n++] = (mig_sig_t){ day, col, ts_min, ts_max, count, size, 0 };
    return TSDB_OK;
}

/* Consume one token for this block if the target has an unconsumed copy of it.
 * 1 == "already present, skip"; 0 == "hand it to the applier". */
static int mig_seen_take(mig_seen_t *sn, uint32_t day, uint16_t col,
                         int64_t ts_min, int64_t ts_max,
                         uint32_t count, uint32_t size) {
    for (size_t i = 0; i < sn->n; i++) {
        if (sn->v[i].taken) continue;
        if (sn->v[i].day != day || sn->v[i].col != col) continue;
        if (sn->v[i].ts_min != ts_min || sn->v[i].ts_max != ts_max) continue;
        if (sn->v[i].count != count || sn->v[i].size != size) continue;
        sn->v[i].taken = 1;
        return 1;
    }
    return 0;
}

/* Scan the target's existing blocks for one (day, column) exactly once, so a
 * resumed migration sees what a previous run already landed.  Every column of
 * a partition shares the same (ts_min, count) layout, so the column MUST be
 * part of the key — otherwise column 1 looks like a duplicate of column 0.
 *
 * Returns TSDB_OK, or TSDB_ERR_CORRUPT when the target's idx EXISTS but
 * cannot be parsed — that state must never prime as "no blocks seen" (see
 * the probe-result split at the bottom). */
static int mig_seen_prime(mig_seen_t *sn, tsdb_db_t *db, const char *table,
                          tsdb_schema_t *s, uint32_t day, uint16_t col)
{
    uint64_t key = ((uint64_t)day << 16) | col;
    for (size_t i = 0; i < sn->ln; i++) if (sn->loaded[i] == key) return TSDB_OK;
    if (sn->ln == sn->lcap) {
        size_t nc = sn->lcap ? sn->lcap * 2 : 32;
        uint64_t *nl = realloc(sn->loaded, nc * sizeof(*nl));
        if (!nl) return TSDB_OK;
        sn->loaded = nl; sn->lcap = nc;
    }
    sn->loaded[sn->ln++] = key;

    /* Read the column's .idx DIRECTLY rather than going through
     * tsdb_part_open: that path reconciles a partition whose columns are
     * uneven (the ALTER-added-column sentinel pass) and will happily
     * synthesise entries for a column whose .idx does not exist yet — which
     * is precisely the state a half-finished migration leaves behind, and it
     * made every column look like a duplicate of column 0. */
    char idx_path[4400];
    snprintf(idx_path, sizeof(idx_path), "%s/%s/%08u/%s.idx",
             tsdb_db_data_dir(db), table, day,
             (col < (uint16_t)s->ncols) ? s->cols[col].name : "");

    uint16_t ver = 0; uint32_t cnt = 0, esz = 0;
    uint64_t tot = 0, mseq = 0; int64_t fmn = 0, fmx = 0;
    int hsz = tsdb_part_idx_probe(idx_path, &ver, &cnt, &esz, &tot, &fmn, &fmx, &mseq);
    /* The probe's 0 (absent — genuinely nothing landed yet) and -1 (exists
     * but unparseable: corrupt magic or unknown version) must not collapse:
     * priming a corrupt idx as "no blocks seen" hands every streamed block to
     * the applier as if the target were empty, over an index that may well
     * hold them.  Surface it here and fail the import before any block moves;
     * the applier (rawblock.c) would only refuse the same idx later anyway.
     * Un-mark the key so this is detected again rather than remembered as a
     * successful (empty) prime. */
    if (hsz < 0) { sn->ln--; return TSDB_ERR_CORRUPT; }
    if (hsz == 0 || cnt == 0 || esz < 32) return TSDB_OK; /* need ts_max at [24..31] */

    FILE *f = fopen(idx_path, "rb");
    if (!f) return TSDB_OK;
    uint8_t *e = malloc(esz);
    if (e) {
        for (uint32_t i = 0; i < cnt; i++) {
            if (fseeko(f, (off_t)hsz + (off_t)i * esz, SEEK_SET) != 0) break;
            if (fread(e, 1, esz, f) != esz) break;
            uint32_t bsize  = (uint32_t)e[8]  | ((uint32_t)e[9]  << 8)
                            | ((uint32_t)e[10] << 16) | ((uint32_t)e[11] << 24);
            uint32_t bcount = (uint32_t)e[12] | ((uint32_t)e[13] << 8)
                            | ((uint32_t)e[14] << 16) | ((uint32_t)e[15] << 24);
            uint64_t tmin = 0, tmax = 0;
            for (int k = 7; k >= 0; k--) tmin = (tmin << 8) | e[16 + k];
            for (int k = 7; k >= 0; k--) tmax = (tmax << 8) | e[24 + k];
            mig_seen_add(sn, day, col, (int64_t)tmin, (int64_t)tmax,
                         bcount, bsize);
        }
        free(e);
    }
    fclose(f);
    return TSDB_OK;
}

/* ---- import-side dictionary reconciliation --------------------------------
 *
 * Merge a streamed dictionary into the target column's LIVE symtab.
 *
 * v2 refused any target whose dictionary was non-empty, on the theory that its
 * codes must have come from somewhere else.  That is wrong for the one case
 * the SDK promises to handle: a RESUMED migration.  The second run finds the
 * dictionary the FIRST run installed — identical, by construction — and the
 * count-based guard rejected it, so every interrupted migration of a table
 * with a tag column became permanently unresumable.  (It only escaped notice
 * because resolving the target with tsdb_db_find_table hid the table from the
 * second run entirely.)
 *
 * So compare instead of count.  Codes are assigned densely from 0 in intern
 * order, so two dictionaries agree exactly when one is a prefix of the other.
 * While the target's codes match, the tail can simply be interned — intern
 * appends, so string N lands at code N and the prefix property is preserved.
 * One disagreeing code means the dictionaries really were built independently:
 * refuse, because landing the blocks would silently retag every row.
 *
 * Interning in place also avoids swapping schema->cols[ci].symtab out from
 * under a reader: exec.c snapshots that pointer for the life of a query, and
 * the old code freed it.
 */
int tsdb_migrate_symtab_adopt(tsdb_schema_t *s, int ci, const uint8_t *b, size_t len)
{
    /* Wire bytes are a .sym file — see core/symbol.h for the layout. */
    uint32_t magic = 0, cnt = 0, hsz = 0;
    if (len < 12) return TSDB_ERR_CORRUPT;
    memcpy(&magic, b + 0, 4);
    memcpy(&cnt,   b + 4, 4);
    memcpy(&hsz,   b + 8, 4);
    if (magic != TSDB_SYMTAB_MAGIC) return TSDB_ERR_CORRUPT;
    if ((uint64_t)len < 12ull + (uint64_t)cnt * 4u + (uint64_t)hsz)
        return TSDB_ERR_CORRUPT;

    const uint8_t *offs = b + 12;
    const char    *heap = (const char *)(b + 12 + (size_t)cnt * 4u);
    /* Every interned string is NUL-terminated, so a heap that does not end in
     * one is truncated — bail before strcmp/intern walk off the end. */
    if (cnt && (hsz == 0 || heap[hsz - 1] != '\0')) return TSDB_ERR_CORRUPT;

    tsdb_symtab_t *cur = s->cols[ci].symtab;
    if (!cur) {
        if (tsdb_symtab_new(&cur) != TSDB_OK || !cur) return TSDB_ERR_NOMEM;
        s->cols[ci].symtab = cur;
    }
    size_t have = tsdb_symtab_size(cur);

    for (uint32_t c = 0; c < cnt; c++) {
        uint32_t off = 0;
        memcpy(&off, offs + (size_t)c * 4u, 4);
        if (off >= hsz) return TSDB_ERR_CORRUPT;
        const char *str = heap + off;

        if (c < have) {                     /* shared prefix: must be identical */
            const char *mine = tsdb_symtab_str(cur, c);
            if (!mine || strcmp(mine, str) != 0) return TSDB_ERR_SCHEMA;
            continue;
        }
        /* Fresh code.  intern assigns count++, which is exactly c here; if it
         * does not, the dictionaries disagree and landing the blocks would
         * mislabel rows. */
        uint32_t got = tsdb_symtab_intern(cur, str);
        if (got == TSDB_SYMBOL_INVALID) return TSDB_ERR_NOMEM;
        if (got != c) return TSDB_ERR_SCHEMA;
    }
    /* Codes the target holds beyond the stream's are its own and stay: the
     * arriving blocks can only name codes < cnt. */

    char sp[4400];
    snprintf(sp, sizeof(sp), "%s/%s.sym", s->dir, s->cols[ci].name);
    return tsdb_symtab_save(cur, sp);
}

/* ---- import -------------------------------------------------------------- */

int tsdb_migrate_import(tsdb_db_t *db, int fd,
                        const tsdb_mig_opts_t *opts, tsdb_mig_stats_t *out)
{
    if (!db || fd < 0) return TSDB_ERR_INVAL;
    tsdb_mig_land_t land = opts ? opts->land : TSDB_MIG_CREATE_OR_RESUME;
    const char *rename_to = (opts && opts->rename_to && opts->rename_to[0])
                          ? opts->rename_to : NULL;

    uint8_t hdr[4 + 4 + MIG_NAME_BYTES + 4 + 4];
    int rc = read_all(fd, hdr, sizeof(hdr), NULL);
    if (rc != TSDB_OK) return rc;
    if (get_u32(hdr) != TSDB_MIG_MAGIC) return TSDB_ERR_CORRUPT;
    if (get_u32(hdr + 4) != TSDB_MIG_VERSION) return TSDB_ERR_CORRUPT;

    char src_name[MIG_NAME_BYTES + 1];
    memcpy(src_name, hdr + 8, MIG_NAME_BYTES);
    src_name[MIG_NAME_BYTES] = '\0';
    int ncols   = (int)get_u32(hdr + 8 + MIG_NAME_BYTES);
    int ts_idx  = (int)get_u32(hdr + 12 + MIG_NAME_BYTES);
    if (ncols <= 0 || ncols > 4096 || ts_idx < 0 || ts_idx >= ncols)
        return TSDB_ERR_CORRUPT;

    const char *tname = rename_to ? rename_to : src_name;

    /* tsdb_col_t.name is a const char*, so keep the names in a buffer we own
     * for as long as the create call needs them. */
    tsdb_col_t *cols  = calloc((size_t)ncols, sizeof(*cols));
    char (*names)[MIG_NAME_BYTES] = calloc((size_t)ncols, MIG_NAME_BYTES);
    if (!cols || !names) { free(cols); free(names); return TSDB_ERR_NOMEM; }
    for (int i = 0; i < ncols; i++) {
        uint8_t c[MIG_NAME_BYTES + 4];
        rc = read_all(fd, c, sizeof(c), NULL);
        if (rc != TSDB_OK) { free(cols); free(names); return rc; }
        c[MIG_NAME_BYTES - 1] = '\0';
        snprintf(names[i], MIG_NAME_BYTES, "%s", (const char *)c);
        cols[i].name = names[i];
        cols[i].type = (tsdb_type_t)get_u32(c + MIG_NAME_BYTES);
    }

    /* SYMBOL dictionary section (v2). */
    uint32_t nsym = 0;
    {
        uint8_t nb[4];
        rc = read_all(fd, nb, 4, NULL);
        if (rc != TSDB_OK) { free(cols); free(names); return rc; }
        nsym = get_u32(nb);
        if (nsym > (uint32_t)ncols) { free(cols); free(names); return TSDB_ERR_CORRUPT; }
    }
    uint32_t *sym_col  = nsym ? calloc(nsym, sizeof(*sym_col)) : NULL;
    uint8_t **sym_buf  = nsym ? calloc(nsym, sizeof(*sym_buf)) : NULL;
    size_t   *sym_len  = nsym ? calloc(nsym, sizeof(*sym_len)) : NULL;
    if (nsym && (!sym_col || !sym_buf || !sym_len)) {
        free(cols); free(names); free(sym_col); free(sym_buf); free(sym_len);
        return TSDB_ERR_NOMEM;
    }
    for (uint32_t k = 0; k < nsym; k++) {
        uint8_t hb[8];
        rc = read_all(fd, hb, 8, NULL);
        if (rc != TSDB_OK) goto sym_fail;
        sym_col[k] = get_u32(hb);
        uint32_t len = get_u32(hb + 4);
        if (sym_col[k] >= (uint32_t)ncols || len > (64u << 20)) { rc = TSDB_ERR_CORRUPT; goto sym_fail; }
        if (len) {
            sym_buf[k] = malloc(len);
            if (!sym_buf[k]) { rc = TSDB_ERR_NOMEM; goto sym_fail; }
            rc = read_all(fd, sym_buf[k], len, NULL);
            if (rc != TSDB_OK) goto sym_fail;
            sym_len[k] = len;
        }
        continue;
sym_fail:
        for (uint32_t j = 0; j < nsym; j++) free(sym_buf[j]);
        free(cols); free(names); free(sym_col); free(sym_buf); free(sym_len);
        return rc;
    }

    /* Does the target exist?  tsdb_db_find_table answers "is it OPEN", which is
     * not the same question: a table this process never touched looked ABSENT,
     * so CREATE_ONLY landed instead of refusing and tsdb_create_table rewrote
     * the target's schema.bin out from under its own rows.  tsdb_open_table
     * answers the real question and its rc is authoritative. */
    tsdb_table_t *th = NULL;
    int orc = tsdb_open_table(db, tname, &th);
    if (orc != TSDB_OK && orc != TSDB_ERR_NOTFOUND) { rc = orc; goto resolve_fail; }

    tsdb_table_internal_t *existing = NULL;
    if (orc == TSDB_OK) {
        existing = tsdb_db_find_table(db, tname);
        if (!existing) { rc = TSDB_ERR_INTERNAL; goto resolve_fail; }
    }
    if (existing && land == TSDB_MIG_CREATE_ONLY) { rc = TSDB_ERR_EXISTS; goto resolve_fail; }
    if (!existing) {
        rc = tsdb_create_table(db, tname, cols, (size_t)ncols, cols[ts_idx].name);
        if (rc != TSDB_OK) goto resolve_fail;
    } else {
        /* Landing writes by column INDEX, so a disagreeing schema would write
         * one column's bytes into another's file.  Refuse instead. */
        tsdb_schema_t *es = tsdb_tbl_schema(existing);
        if (!es || es->ncols != ncols) { rc = TSDB_ERR_SCHEMA; goto resolve_fail; }
        for (int i = 0; i < ncols; i++) {
            if (strcmp(es->cols[i].name, cols[i].name) != 0 ||
                es->cols[i].type != cols[i].type) { rc = TSDB_ERR_SCHEMA; goto resolve_fail; }
        }
    }
    free(cols);  cols  = NULL;
    free(names); names = NULL;

    tsdb_mig_stats_t st;
    memset(&st, 0, sizeof(st));

    tsdb_schema_t *tgt_schema = NULL;
    {
        tsdb_table_internal_t *tt = tsdb_db_find_table(db, tname);
        if (tt) tgt_schema = tsdb_tbl_schema(tt);
    }

    /* Reconcile the source dictionaries before any block lands.  The blocks
     * carry CODES, so they are only meaningful against the dictionary that
     * assigned them; the live symtab has to agree, not just the .sym file,
     * because schema_save rewrites that file from memory at close. */
    for (uint32_t k = 0; k < nsym && tgt_schema && rc == TSDB_OK; k++) {
        int ci = (int)sym_col[k];
        if (ci >= tgt_schema->ncols ||
            tgt_schema->cols[ci].type != TSDB_TYPE_SYMBOL) continue;
        if (!sym_len[k]) continue;

        rc = tsdb_migrate_symtab_adopt(tgt_schema, ci, sym_buf[k], sym_len[k]);
    }
    for (uint32_t k = 0; k < nsym; k++) free(sym_buf[k]);
    free(sym_col); free(sym_buf); free(sym_len);
    if (rc != TSDB_OK) return rc;
    mig_seen_t seen;
    memset(&seen, 0, sizeof(seen));

    uint8_t *buf = NULL; size_t cap = 0;
    for (;;) {
        uint8_t lb[4]; int eof = 0;
        rc = read_all(fd, lb, 4, &eof);
        if (rc != TSDB_OK) break;
        if (eof) { rc = TSDB_ERR_CORRUPT; break; }   /* missing terminator */
        uint32_t len = get_u32(lb);
        if (len == 0) { rc = TSDB_OK; break; }       /* clean end */
        if (len > (64u << 20)) { rc = TSDB_ERR_CORRUPT; break; }

        if (len > cap) {
            uint8_t *nb = realloc(buf, len);
            if (!nb) { rc = TSDB_ERR_NOMEM; break; }
            buf = nb; cap = len;
        }
        rc = read_all(fd, buf, len, NULL);
        if (rc != TSDB_OK) break;

        tsdb_rawblock_push_t r;
        if (tsdb_rawblock_parse(buf, len, &r) != TSDB_OK) { rc = TSDB_ERR_CORRUPT; break; }
        /* Land under the target's name, which may differ from the source's. */
        snprintf(r.table, sizeof(r.table), "%s", tname);

        /* Already present from an earlier (possibly interrupted) run?  A
         * prime that cannot READ the target's manifest aborts the import —
         * continuing would treat the partition as empty and re-land blocks
         * over an index nobody can parse. */
        if (tgt_schema) {
            rc = mig_seen_prime(&seen, db, tname, tgt_schema,
                                r.part_day, r.col_idx);
            if (rc != TSDB_OK) break;
        }

        if (!mig_seen_take(&seen, r.part_day, r.col_idx, r.ts_min, r.ts_max,
                           r.count, r.block_bytes_len)) {
            /* No token: the target does not already hold this block.  The
             * applier is the authority from here — it translates the stream's
             * ordinal into the target's space and absorbs a genuine repeat
             * there, which is where a repeat can be told apart from a second
             * block of a duplicate-timestamp run. */
            int arc = tsdb_rawblock_apply(db, &r);
            if (arc != TSDB_OK) { rc = arc; break; }
            st.blocks_landed++;
        }

        st.blocks++;
        if ((int)r.col_idx == ts_idx) st.rows += r.count;
        st.payload_bytes += r.block_bytes_len;
        st.digest        ^= digest_block(r.part_day, r.col_idx,
                                         r.ts_min, r.ts_max, r.count);
    }
    free(buf);
    mig_seen_free(&seen);

    if (rc == TSDB_OK && out) *out = st;
    return rc;

    /* Every exit taken while resolving the target has to drop BOTH the header
     * allocations and the symbol section — four of them used to return straight
     * out, leaking the dictionaries (up to 64 MB each), and the schema-mismatch
     * path runs on every test_migrate run. */
resolve_fail:
    for (uint32_t k = 0; k < nsym; k++) free(sym_buf[k]);
    free(cols); free(names);
    free(sym_col); free(sym_buf); free(sym_len);
    return rc;
}

/* ---- digest -------------------------------------------------------------- */

int tsdb_migrate_digest(tsdb_db_t *db, const char *table, tsdb_mig_stats_t *out)
{
    if (!db || !table || !out) return TSDB_ERR_INVAL;
    /* Same resolution as export: on-disk existence, not "already open". */
    tsdb_table_t *th = NULL;
    int orc = tsdb_open_table(db, table, &th);
    if (orc != TSDB_OK) return orc;
    tsdb_table_internal_t *ti = tsdb_db_find_table(db, table);
    if (!ti) return TSDB_ERR_INTERNAL;
    tsdb_schema_t *s = tsdb_tbl_schema(ti);
    if (!s) return TSDB_ERR_INTERNAL;

    tsdb_mig_stats_t st;
    memset(&st, 0, sizeof(st));

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", tsdb_db_data_dir(db), table);
    char **parts = NULL;
    int nparts = list_parts(tbl_dir, &parts);
    if (nparts < 0) { *out = st; return TSDB_OK; }

    for (int pi = 0; pi < nparts; pi++) {
        char part_dir[4200];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", tbl_dir, parts[pi]);
        tsdb_part_t *p = NULL;
        if (tsdb_part_open(s, part_dir, &p) != TSDB_OK || !p) continue;
        st.partitions++;
        uint32_t day = (uint32_t)strtoul(parts[pi], NULL, 10);
        if (strlen(parts[pi]) == 10) day = (uint32_t)(strtoull(parts[pi], NULL, 10) / 100);

        for (int ci = 0; ci < s->ncols; ci++) {
            tsdb_block_meta_t *metas = NULL; size_t nb = 0;
            if (tsdb_part_col_blocks(p, ci, &metas, &nb) != TSDB_OK) continue;
            for (size_t bi = 0; bi < nb; bi++) {
                /* A HOLE is a ts block this column has no value for, and
                 * tsdb_part_open has already ruled out the ALTER-added
                 * explanation for it — tsdb_part_read_block answers
                 * TSDB_ERR_CORRUPT here.  It has size 0, so `blocks` skips it
                 * below and a lost column reads out of these counters as
                 * "fewer blocks, same rows" — indistinguishable from a
                 * compactor merge.  Count it on its own axis so the caller can
                 * tell those two apart. */
                if (metas[bi].flags & TSDB_BLOCK_FLAG_HOLE) st.holes++;
                if (metas[bi].size == 0) continue;
                st.blocks++;
                if (ci == s->ts_col_idx) st.rows += metas[bi].count;
                st.payload_bytes += metas[bi].size;
                st.digest        ^= digest_block(day, (uint16_t)ci,
                                                 metas[bi].ts_min,
                                                 metas[bi].ts_max,
                                                 metas[bi].count);
            }
            free(metas);
        }
        tsdb_part_close(p);
    }
    for (int i = 0; i < nparts; i++) free(parts[i]);
    free(parts);

    *out = st;
    return TSDB_OK;
}
