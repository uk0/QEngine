/* test_drop_reserved.c — DROP TABLE of a reserved plumbing name is refused.
 *
 * tsdb_name_is_one_path_component (the 33f0555 gate) only blocks names that
 * ESCAPE the data dir (`..`, `/`, leading `.`).  A bare `wal` / `catalog` /
 * `raft` stays inside the data dir and passes — yet table_dir_db resolves it to
 * the SAME path as the node's WAL dir / catalog / raft state.  So DROP TABLE
 * wal ran trash_or_rm(<data_dir>/wal) and destroyed the node's plumbing while
 * returning TSDB_OK, unauthenticated-reachable via the RPC DDL_FORWARD path.
 *
 * These names can never name a REAL table (create would collide with the live
 * plumbing dir), so refusing to drop them costs nothing legitimate.  This pins:
 * a DROP of each reserved name is refused with TSDB_ERR_INVAL and the plumbing
 * dir + its contents survive untouched, while an ordinary table still drops.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d\n",_r);FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_drop_reserved";

static int dir_exists(const char *p) {
    struct stat st;
    return stat(p, &st) == 0 && S_ISDIR(st.st_mode);
}
static int file_exists(const char *p) {
    struct stat st;
    return stat(p, &st) == 0 && S_ISREG(st.st_mode);
}
static void rm_tree(const char *p) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c); }

static void plant_sentinel(const char *dir, const char *fname) {
    mkdir(dir, 0755);
    char p[600]; snprintf(p, sizeof(p), "%s/%s", dir, fname);
    FILE *f = fopen(p, "w");
    ASSERT(f != NULL);
    fputs("plumbing — do not delete\n", f);
    fclose(f);
}

static void make_table(tsdb_db_t *db, const char *name) {
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    OK(tsdb_create_table(db, name, cols, 2, "ts"));
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, name, &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 200; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));   /* real on-disk partition + live WAL */
}

/* One reserved name: plant a sentinel in <TMP>/<name>, DROP it, prove refusal
 * and survival.  On the unfixed engine the dir is renamed into .trash by the
 * DROP call itself (synchronous), so the sentinel vanishes from its location. */
static void check_reserved(tsdb_db_t *db, const char *name) {
    char dir[512], sent[600];
    snprintf(dir,  sizeof(dir),  "%s/%s", TMP, name);
    snprintf(sent, sizeof(sent), "%s/%s/SENTINEL", TMP, name);
    plant_sentinel(dir, "SENTINEL");
    ASSERT(dir_exists(dir));
    ASSERT(file_exists(sent));

    int rc = tsdb_drop_table(db, name);

    /* Damage first, errno second: without the gate the dir is already gone. */
    if (!dir_exists(dir) || !file_exists(sent)) {
        fprintf(stderr, "DROP TABLE %s destroyed <data_dir>/%s (rc=%d)\n",
                name, name, rc);
        FAIL("reserved plumbing dir destroyed by DROP");
    }
    if (rc != TSDB_ERR_INVAL) {
        fprintf(stderr, "DROP TABLE %s rc=%d (want INVAL=%d)\n",
                name, rc, TSDB_ERR_INVAL);
        FAIL("DROP of reserved name not refused with INVAL");
    }
    printf("  reserved: DROP '%s' refused, <data_dir>/%s intact\n", name, name);
}

int main(void) {
    printf("=== test_drop_reserved ===\n");
    rm_tree(TMP);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TMP, &db));

    /* A live table so wal/ and catalog/ are genuine, populated plumbing. */
    make_table(db, "t1");
    char waldir[512], catdir[512];
    snprintf(waldir, sizeof(waldir), "%s/wal", TMP);
    snprintf(catdir, sizeof(catdir), "%s/catalog", TMP);
    ASSERT(dir_exists(waldir));   /* <data_dir>/wal is live (t1.log lives here) */
    ASSERT(dir_exists(catdir));   /* <data_dir>/catalog is live */

    /* raft/ is only minted in cluster mode; plant it so the standalone test
     * still exercises the reserved-name refusal for it. */
    char raftdir[512]; snprintf(raftdir, sizeof(raftdir), "%s/raft", TMP);
    plant_sentinel(raftdir, "config.bin");

    check_reserved(db, "wal");
    check_reserved(db, "catalog");
    check_reserved(db, "raft");

    /* The node is still whole: the live table survives and still reads. */
    {
        tsdb_result_t *qr = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM t1", &qr));
        ASSERT(qr != NULL);
        tsdb_result_free(qr);
        printf("  node intact: t1 still queryable after refused DROPs\n");
    }

    /* Guard against over-blocking: an ordinary table still drops, and a
     * non-reserved absent name is still an idempotent no-op (TSDB_OK). */
    {
        char t1dir[512]; snprintf(t1dir, sizeof(t1dir), "%s/t1", TMP);
        ASSERT(dir_exists(t1dir));
        OK(tsdb_drop_table(db, "t1"));
        ASSERT(!dir_exists(t1dir));
        OK(tsdb_drop_table(db, "no_such_table"));   /* idempotent, not reserved */
        printf("  ordinary DROP still works; absent-name DROP still OK\n");
    }

    tsdb_close(db);
    rm_tree(TMP);
    printf("[PASS] DROP refuses reserved plumbing names; node plumbing intact\n");
    return 0;
}
