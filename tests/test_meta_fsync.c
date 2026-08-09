/* test_meta_fsync.c — metadata creates/renames must fsync the PARENT DIRECTORY,
 * and create-names must reject the JSON-manifest injection bytes.
 *
 * THE BUG (pre-fix), two halves:
 *
 * 1. DUR-2 — schema_save fsync'd schema.bin.tmp and renamed it over schema.bin
 *    but never fsync'd the directory holding the entry; the table dir itself
 *    (mkdir in tsdb_schema_create_ex2), the SYMBOL dict publish
 *    (tsdb_symtab_save) and the WAL file creation (tsdb_wal_open) had the same
 *    gap.  POSIX makes rename/mkdir/creat ATOMIC, not DURABLE: the new entry
 *    lives in the parent directory, and only an fsync of the DIRECTORY makes
 *    it durable.  Concretely: CREATE TABLE acked, rows acked (their WAL
 *    records fsync'd), power cut — the WAL file's directory entry is gone, so
 *    the fsync'd records are unreachable at replay: acked-write loss.  The
 *    data path has had the discipline for a while (part.c part_fsync_dir);
 *    the metadata paths did not.
 *
 * 2. 5a2e6bb open sibling — tsdb_name_is_one_path_component() accepted '"',
 *    ',' and ':'.  backup.c emits table names verbatim into its JSON manifest
 *    ("name":"<name>", NO escaping), so an unauthenticated Influx measurement
 *    like  x",host=a v=1  creates a table whose name closes the JSON string
 *    and injects manifest fields.
 *
 * Test technique for (1): this binary defines its own fsync().  Tests link
 * the library .o files directly into the executable, so every library call
 * to fsync binds here at link time.  It records the realpath of every
 * DIRECTORY fd fsync'd; the test then asserts "the parent dir of that
 * create/rename was fsync'd".  Pre-fix no directory is ever fsync'd on these
 * paths, so every dir_was_fsynced() assert below fails.
 */
#include "tsdb.h"
#include "../src/storage/schema.h"
#include "../src/storage/wal.h"
#include "../src/core/symbol.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)

#define BASE "/tmp/tsdb_test_meta_fsync"

/* ---- fsync interposer ---------------------------------------------------- */

#define MAX_DIRS 256
static char            g_dirs[MAX_DIRS][1024];
static int             g_ndirs;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;

/* Overrides libc fsync for every call made from the linked library objects.
 * Records directory fds only; returns success without syncing — the test
 * needs to SEE the calls, not pay for the durability. */
int fsync(int fd) {
    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
        char path[1024] = {0};
#if defined(__APPLE__)
        if (fcntl(fd, F_GETPATH, path) != 0) path[0] = '\0';
#else
        char lnk[64];
        snprintf(lnk, sizeof(lnk), "/proc/self/fd/%d", fd);
        ssize_t n = readlink(lnk, path, sizeof(path) - 1);
        if (n > 0) path[n] = '\0'; else path[0] = '\0';
#endif
        if (path[0]) {
            pthread_mutex_lock(&g_mu);
            if (g_ndirs < MAX_DIRS)
                snprintf(g_dirs[g_ndirs++], sizeof(g_dirs[0]), "%s", path);
            pthread_mutex_unlock(&g_mu);
        }
    }
    return 0;
}

static void recorder_reset(void) {
    pthread_mutex_lock(&g_mu);
    g_ndirs = 0;
    pthread_mutex_unlock(&g_mu);
}

static int dir_was_fsynced(const char *dir) {
    char want[1024];
    if (!realpath(dir, want)) return 0;
    int hit = 0;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < g_ndirs && !hit; i++)
        if (strcmp(g_dirs[i], want) == 0) hit = 1;
    pthread_mutex_unlock(&g_mu);
    return hit;
}

/* ---- helpers ------------------------------------------------------------- */

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[2048]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        chmod(q, 0700); rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static int path_exists(const char *p) {
    struct stat st;
    return stat(p, &st) == 0;
}

/* ---- part 1: create-name predicate rejects injection bytes -------------- */

static void test_name_predicate(void) {
    /* The three manifest-injection bytes must be rejected. */
    if (tsdb_name_is_one_path_component("x\""))
        FAIL("name with '\"' accepted — JSON manifest injection byte");
    if (tsdb_name_is_one_path_component("a,b"))
        FAIL("name with ',' accepted — JSON manifest injection byte");
    if (tsdb_name_is_one_path_component("a:b"))
        FAIL("name with ':' accepted — JSON manifest injection byte");

    /* Previously-rejected classes stay rejected. */
    if (tsdb_name_is_one_path_component("a/b"))   FAIL("'/' accepted");
    if (tsdb_name_is_one_path_component("a\\b"))  FAIL("'\\\\' accepted");
    if (tsdb_name_is_one_path_component(".hid"))  FAIL("leading '.' accepted");
    if (tsdb_name_is_one_path_component("a\nb"))  FAIL("control byte accepted");
    if (tsdb_name_is_one_path_component(""))      FAIL("empty name accepted");

    /* Ordinary names stay legal, including the deliberately-wide cases the
     * predicate documents (spaces, interior dots, UTF-8). */
    if (!tsdb_name_is_one_path_component("cpu_usage"))   FAIL("plain name rejected");
    if (!tsdb_name_is_one_path_component("web servers")) FAIL("space rejected");
    if (!tsdb_name_is_one_path_component("cpu.usage"))   FAIL("interior dot rejected");
    if (!tsdb_name_is_one_path_component("\xe6\x9c\xba")) FAIL("UTF-8 rejected");

    /* End to end: CREATE with an injection-byte name must refuse and create
     * NOTHING on disk. */
    const char *bad_dir = BASE "/data/x\"";
    tsdb_col_t cols[2] = { { "ts", TSDB_TYPE_TIMESTAMP },
                           { "v",  TSDB_TYPE_FLOAT64 } };
    tsdb_schema_t *s = NULL;
    int rc = tsdb_schema_create(bad_dir, "x\"", cols, 2, "ts", &s);
    if (rc != TSDB_ERR_INVAL)
        FAIL("CREATE of name `x\"` returned %d, want TSDB_ERR_INVAL (%d)",
             rc, TSDB_ERR_INVAL);
    if (path_exists(bad_dir))
        FAIL("CREATE of name `x\"` refused but still created %s", bad_dir);
    printf("  name predicate: injection bytes rejected, legal names kept\n");
}

/* ---- part 2: schema create fsyncs the parent dirs ------------------------ */

static void test_schema_create_fsync(void) {
    const char *data_dir = BASE "/data";
    const char *tbl_dir  = BASE "/data/t1";
    tsdb_col_t cols[3] = { { "ts",   TSDB_TYPE_TIMESTAMP },
                           { "host", TSDB_TYPE_SYMBOL },
                           { "v",    TSDB_TYPE_FLOAT64 } };
    tsdb_schema_t *s = NULL;

    recorder_reset();
    int rc = tsdb_schema_create(tbl_dir, "t1", cols, 3, "ts", &s);
    if (rc != TSDB_OK) FAIL("schema_create rc=%d", rc);

    /* The new table dir's ENTRY lives in data_dir: without fsync(data_dir) a
     * power cut can drop the whole acked table. */
    if (!dir_was_fsynced(data_dir))
        FAIL("CREATE TABLE did not fsync the data dir (parent of the new "
             "table dir) — the table dir entry is not durable");
    /* schema.bin was renamed INTO tbl_dir: without fsync(tbl_dir) the rename
     * can roll back (or to ABSENT on first create). */
    if (!dir_was_fsynced(tbl_dir))
        FAIL("schema_save did not fsync the table dir after the schema.bin "
             "rename — the rename is not durable");
    tsdb_schema_free(s);
    printf("  schema create: parent-dir fsyncs present\n");
}

/* ---- part 3: symtab save fsyncs the parent dir --------------------------- */

static void test_symtab_save_fsync(void) {
    const char *sym_dir = BASE "/symd";
    if (mkdir(sym_dir, 0755) != 0 && errno != EEXIST) FAIL("mkdir %s", sym_dir);

    tsdb_symtab_t *st = NULL;
    if (tsdb_symtab_new(&st) != TSDB_OK) FAIL("symtab_new");
    tsdb_symtab_intern(st, "web-1");
    tsdb_symtab_intern(st, "web-2");

    char sym_path[2048];
    snprintf(sym_path, sizeof(sym_path), "%s/host.sym", sym_dir);

    recorder_reset();
    if (tsdb_symtab_save(st, sym_path) != TSDB_OK) FAIL("symtab_save");
    if (!dir_was_fsynced(sym_dir))
        FAIL("tsdb_symtab_save did not fsync the dict's parent dir — the "
             "dict rename is not durable and a crash resurrects the OLD dict "
             "under NEW durable codes");
    tsdb_symtab_free(st);
    printf("  symtab save: parent-dir fsync present\n");
}

/* ---- part 4: WAL open fsyncs the wal dir and the db dir ------------------ */

static void test_wal_open_fsync(void) {
    const char *db_dir  = BASE "/db1";
    const char *wal_dir = BASE "/db1/wal";
    if (mkdir(db_dir, 0755) != 0 && errno != EEXIST) FAIL("mkdir %s", db_dir);

    tsdb_wal_t *w = NULL;
    recorder_reset();
    if (tsdb_wal_open(db_dir, "tbl", &w) != TSDB_OK) FAIL("wal_open");
    /* The .log entry lives in wal/, and wal/ itself lives in the db dir: if
     * either entry is lost with the power, every fsync'd WAL record becomes
     * unreachable at replay — acked-write loss. */
    if (!dir_was_fsynced(wal_dir))
        FAIL("tsdb_wal_open did not fsync the wal dir — the new .log entry "
             "is not durable, fsync'd records would be unreachable");
    if (!dir_was_fsynced(db_dir))
        FAIL("tsdb_wal_open did not fsync the db dir — the wal/ dir entry "
             "is not durable, fsync'd records would be unreachable");
    tsdb_wal_close(w);
    printf("  wal open: wal-dir and db-dir fsyncs present\n");
}

int main(void) {
    rm_rf(BASE);
    if (mkdir(BASE, 0755) != 0) FAIL("mkdir %s: %s", BASE, strerror(errno));
    if (mkdir(BASE "/data", 0755) != 0) FAIL("mkdir data");

    test_name_predicate();
    test_schema_create_fsync();
    test_symtab_save_fsync();
    test_wal_open_fsync();

    rm_rf(BASE);
    printf("test_meta_fsync: ALL PASS\n");
    return 0;
}
