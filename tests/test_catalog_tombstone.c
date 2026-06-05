/* test_catalog_tombstone.c — catalog tombstones survive compaction.
 *
 * Compaction rewrites each catalog log from the live in-memory hmap, which
 * holds only surviving entities.  A naive rewrite drops every `-` tombstone —
 * but the cluster reconcile guard (tsdb_catalog_dump_apply_filtered) refuses to
 * resurrect a dropped object by scanning the log for that prior `-`.  Strip the
 * tombstone and a peer that was down during the drop re-teaches it on the next
 * anti-entropy pass (the catalog analogue of the data-layer DELETE watermark).
 *
 * This pins: after create A + create B + drop B + COMPACT,
 *   - the `-stable\tB` tombstone is still on disk (survived compaction),
 *   - the live `+stable\tA` is still on disk,
 *   - a peer dump advertising B as live does NOT resurrect B.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/catalog/stable.h"
#include "../src/catalog/group.h"   /* tsdb_catalog_open / _close */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/* Compaction entry points are internal (extern, no public header). */
extern int tsdb_catalog_compact_stables(tsdb_catalog_t *c, const char *path);

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_cat_tomb";

static int file_count(const char *path, const char *needle) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    char buf[16384]; size_t n = fread(buf, 1, sizeof(buf) - 1, f); buf[n] = 0; fclose(f);
    int c = 0; const char *p = buf;
    while ((p = strstr(p, needle))) { c++; p += strlen(needle); }
    return c;
}

/* Minimal one-column stable named `nm`. */
static void mk_stable(tsdb_catalog_t *c, const char *nm) {
    tsdb_stable_t s; memset(&s, 0, sizeof(s));
    snprintf(s.name, sizeof(s.name), "%s", nm);
    snprintf(s.cols[0].name, sizeof(s.cols[0].name), "ts");
    s.cols[0].type = TSDB_TYPE_TIMESTAMP;
    snprintf(s.cols[1].name, sizeof(s.cols[1].name), "v");
    s.cols[1].type = TSDB_TYPE_FLOAT64;
    s.ncols = 2; s.ts_col_idx = 0; s.ntag_cols = 0;
    ASSERT(tsdb_stable_create(c, &s) == TSDB_OK);
}

/* Build a CATALOG_DUMP wire frame for one log file. */
static size_t frame_one(uint8_t *out, const char *fname, const char *body) {
    size_t nl = strlen(fname), bl = strlen(body), o = 0;
    out[o++] = (uint8_t)nl; out[o++] = (uint8_t)(nl>>8); out[o++] = (uint8_t)(nl>>16); out[o++] = (uint8_t)(nl>>24);
    memcpy(out+o, fname, nl); o += nl;
    out[o++] = (uint8_t)bl; out[o++] = (uint8_t)(bl>>8); out[o++] = (uint8_t)(bl>>16); out[o++] = (uint8_t)(bl>>24);
    memcpy(out+o, body, bl); o += bl;
    return o;
}

int main(void) {
    printf("=== test_catalog_tombstone ===\n");
    char cmd[512], stb_path[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TMP); (void)system(cmd);
    snprintf(stb_path, sizeof(stb_path), "%s/catalog/stables.log", TMP);

    tsdb_catalog_t *c = NULL;
    ASSERT(tsdb_catalog_open(TMP, &c) == TSDB_OK);
    ASSERT(c);

    mk_stable(c, "A");          /* live */
    mk_stable(c, "B");          /* will be dropped */
    ASSERT(tsdb_stable_drop(c, "B") == TSDB_OK);

    /* Pre-compaction sanity: log holds +A, +B and -B. */
    ASSERT(file_count(stb_path, "+stable\tA\t") == 1);
    ASSERT(file_count(stb_path, "-stable\tB")   == 1);

    /* Compact.  Pre-fix this rewrites from the live hmap (A only) and the
     * B tombstone vanishes; post-fix it is re-emitted. */
    ASSERT(tsdb_catalog_compact_stables(c, stb_path) == TSDB_OK);

    ASSERT(file_count(stb_path, "+stable\tA\t") == 1);   /* live survived */
    ASSERT(file_count(stb_path, "+stable\tB\t") == 0);   /* not resurrected by compaction */
    ASSERT(file_count(stb_path, "-stable\tB")   == 1);   /* TOMBSTONE SURVIVED — the fix */
    printf("  after compaction: A live(1), B tombstone kept(1), no +B\n");

    tsdb_catalog_close(c);

    /* Reconcile guard: a peer dump where B is live must NOT resurrect B,
     * because the surviving tombstone tells us we dropped it. */
    const char *peer_body =
        "+stable\tA\t1\t0\t0\t200\tts:1\n"
        "+stable\tB\t1\t0\t0\t200\tts:1\n";
    uint8_t dump[4096];
    size_t len = frame_one(dump, "stables.log", peer_body);
    int added = tsdb_catalog_dump_apply_filtered(TMP, dump, len);
    printf("  apply_filtered after compaction added %d (want 0)\n", added);
    ASSERT(added == 0);
    ASSERT(file_count(stb_path, "+stable\tB\t") == 0);   /* still not resurrected */

    /* ── replay drops orphan children whose parent stable is gone ─────────── */
    /* Create stable P + child childP via the API, then hand-append an orphan
     * +child under a parent that never existed, reopen, and assert replay keeps
     * childP (live parent) but drops childGhost (orphan) — so a cold restart no
     * longer rebuilds on-disk orphans that DROP STABLE could never cascade. */
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TMP); (void)system(cmd);
    ASSERT(tsdb_catalog_open(TMP, &c) == TSDB_OK);
    mk_stable(c, "P");
    {
        tsdb_child_table_t ct; memset(&ct, 0, sizeof(ct));
        snprintf(ct.name, sizeof(ct.name), "childP");
        snprintf(ct.stable_name, sizeof(ct.stable_name), "P");
        ct.ntags = 0;
        ASSERT(tsdb_child_table_create(c, &ct) == TSDB_OK);
    }
    tsdb_catalog_close(c);
    {
        char chl[512]; snprintf(chl, sizeof(chl), "%s/catalog/child_tables.log", TMP);
        FILE *af = fopen(chl, "a"); ASSERT(af);
        fputs("+child\tchildGhost\tGHOSTPARENT\t0\t300\n", af);
        fclose(af);
    }
    ASSERT(tsdb_catalog_open(TMP, &c) == TSDB_OK);
    {
        tsdb_child_table_t out;
        ASSERT(tsdb_child_table_get(c, "childP", &out) == TSDB_OK);      /* parent live → kept */
        ASSERT(tsdb_child_table_get(c, "childGhost", &out) != TSDB_OK);  /* orphan → dropped */
    }
    tsdb_catalog_close(c);
    printf("  replay kept childP (parent live), dropped childGhost (orphan)\n");

    snprintf(cmd, sizeof(cmd), "rm -rf %s", TMP); (void)system(cmd);
    printf("[PASS] catalog tombstones survive compaction; replay drops orphan children\n");
    return 0;
}
