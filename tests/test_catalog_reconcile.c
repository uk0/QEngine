/* test_catalog_reconcile.c — resurrection-safe catalog merge.
 *
 * tsdb_catalog_dump_apply_filtered merges a peer's catalog dump into the local
 * logs so a recovered node learns tables created during its downtime, WITHOUT
 * ever resurrecting one it created-then-dropped.  This pins the four cases on a
 * stables.log:
 *   A  — present locally (live)            -> not re-appended (no dup)
 *   B  — created+dropped locally (tombstone)-> peer's live +B is IGNORED (no resurrection)
 *   C  — new, live on peer                  -> appended (learned)
 *   D  — created+dropped on the peer        -> not appended (peer's last op is '-')
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_catrec";

/* Build a CATALOG_DUMP wire frame for a single log file. */
static size_t frame_one(uint8_t *out, const char *fname, const char *body) {
    size_t nl = strlen(fname), bl = strlen(body), o = 0;
    out[o++] = (uint8_t)nl; out[o++] = (uint8_t)(nl>>8); out[o++] = (uint8_t)(nl>>16); out[o++] = (uint8_t)(nl>>24);
    memcpy(out+o, fname, nl); o += nl;
    out[o++] = (uint8_t)bl; out[o++] = (uint8_t)(bl>>8); out[o++] = (uint8_t)(bl>>16); out[o++] = (uint8_t)(bl>>24);
    memcpy(out+o, body, bl); o += bl;
    return o;
}

/* Count occurrences of `needle` in the file at `path`. */
static int file_count(const char *path, const char *needle) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    char buf[8192]; size_t n = fread(buf, 1, sizeof(buf)-1, f); buf[n] = 0; fclose(f);
    int c = 0; const char *p = buf;
    while ((p = strstr(p, needle))) { c++; p += strlen(needle); }
    return c;
}

int main(void) {
    printf("=== test_catalog_reconcile ===\n");
    char dir[256], path[512];
    snprintf(dir, sizeof(dir), "%s/catalog", TMP);
    /* clean + create dirs */
    snprintf(path, sizeof(path), "rm -rf %s", TMP); (void)system(path);
    mkdir(TMP, 0755); mkdir(dir, 0755);

    /* Local stables.log: A live, B created-then-dropped (tombstone). */
    snprintf(path, sizeof(path), "%s/stables.log", dir);
    FILE *lf = fopen(path, "wb");
    ASSERT(lf);
    fputs("+stable\tA\t1\t0\t0\t100\tts:1\n", lf);
    fputs("+stable\tB\t1\t0\t0\t100\tts:1\n", lf);
    fputs("-stable\tB\n", lf);
    fclose(lf);

    /* Peer dump stables.log body: A live, B live (would resurrect!), C new live,
     * D created+dropped on peer. */
    const char *peer_body =
        "+stable\tA\t1\t0\t0\t200\tts:1\n"
        "+stable\tB\t1\t0\t0\t200\tts:1\n"
        "+stable\tC\t1\t0\t0\t200\tts:1\n"
        "+stable\tD\t1\t0\t0\t200\tts:1\n"
        "-stable\tD\n";
    uint8_t dump[4096];
    size_t len = frame_one(dump, "stables.log", peer_body);

    int added = tsdb_catalog_dump_apply_filtered(TMP, dump, len);
    printf("  apply_filtered added %d record(s)\n", added);

    /* Only C should have been learned. */
    ASSERT(added == 1);
    ASSERT(file_count(path, "\tA\t") == 1);   /* not re-appended */
    ASSERT(file_count(path, "+stable\tB\t") == 1); /* original only — NOT resurrected */
    ASSERT(file_count(path, "-stable\tB")  == 1);  /* tombstone intact */
    ASSERT(file_count(path, "+stable\tC\t") == 1); /* learned */
    ASSERT(file_count(path, "\tD\t") == 0);   /* peer-dropped, never added */
    printf("  A kept(1) B not-resurrected(1+tomb) C learned(1) D skipped(0)\n");

    /* Idempotent: a second apply adds nothing (C now locally mentioned). */
    int added2 = tsdb_catalog_dump_apply_filtered(TMP, dump, len);
    ASSERT(added2 == 0);
    ASSERT(file_count(path, "+stable\tC\t") == 1);
    printf("  second apply is a no-op (idempotent)\n");

    /* Child-orphan rejection: a +child under a LIVE local stable (A) is learned;
     * a +child under a non-existent parent stable is refused, so reconcile never
     * resurrects an orphan child whose super-table is gone. */
    const char *child_body =
        "+child\tchildA\tA\t0\t300\n"             /* parent A is live locally */
        "+child\tchildGhost\tGHOSTST\t0\t300\n";  /* parent GHOSTST never existed */
    uint8_t cdump[2048];
    size_t clen = frame_one(cdump, "child_tables.log", child_body);
    int cadded = tsdb_catalog_dump_apply_filtered(TMP, cdump, clen);
    char cpath[600]; snprintf(cpath, sizeof(cpath), "%s/child_tables.log", dir);
    ASSERT(cadded == 1);
    ASSERT(file_count(cpath, "+child\tchildA\t") == 1);   /* parent live → learned */
    ASSERT(file_count(cpath, "childGhost") == 0);         /* parent gone → skipped */
    printf("  childA learned (parent live), childGhost skipped (orphan refused)\n");

    snprintf(path, sizeof(path), "rm -rf %s", TMP); (void)system(path);
    printf("[PASS] reconcile merges new tables, never resurrects dropped ones\n");
    return 0;
}
