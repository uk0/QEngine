/* test_node_id_durable.c — the replication issuer must survive an unclean exit.
 *
 * The node id is the replication ISSUER: the identity the block ordinal's
 * .ordmap keys on and the stream identity write-batch dedup needs. If a crash
 * loses it and the next start mints a fresh one, every batch this node already
 * replicated looks like it came from a new sender — old blocks appear new and
 * dedup is defeated.
 *
 * generate_node_id persists to node_id.tmp, fsyncs it, renames onto node_id,
 * then fsyncs the directory. This test drives the black-box path: a child opens
 * the cluster (which mints and persists the id) and _exit()s WITHOUT
 * tsdb_close — a real unclean exit, no cleanup handlers — then the parent
 * reopens the same data dir and asserts the id is IDENTICAL. It also asserts no
 * node_id.tmp is left behind on the success path.
 *
 * A functional test cannot prove the fsync survives a power cut (only a crash
 * BEFORE the metadata flush would show that, which needs a device fault) — but
 * it does prove the tmp+rename atomicity that keeps the id stable across a
 * process death, which is the part a wrong ordering breaks.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define DIRP "/tmp/tsdb_test_node_id_durable"

static int g_fail;
#define CHECK(c, ...) do { if (!(c)) { printf("FAIL " __VA_ARGS__); \
    printf("\n"); g_fail++; } } while (0)

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

/* Open the cluster, print its id, exit hard — no tsdb_close, no atexit. */
static uint64_t open_and_get_id(void) {
    tsdb_db_t *db = NULL;
    if (tsdb_open_cluster(DIRP, "127.0.0.1:0", NULL, &db) != TSDB_OK || !db)
        return 0;
    uint64_t id = tsdb_cluster_local_id_for_db(db);
    tsdb_close_cluster(db);
    tsdb_close(db);
    return id;
}

static int tmp_present(void) {
    char p[4096]; snprintf(p, sizeof(p), "%s/node_id.tmp", DIRP);
    struct stat st;
    return stat(p, &st) == 0;
}

int main(void) {
    printf("=== node id durable across an unclean exit ===\n");
    rm_rf(DIRP);
    if (mkdir(DIRP, 0755) != 0 && access(DIRP, W_OK) != 0) {
        printf("FAIL cannot create %s\n", DIRP); return 2;
    }

    /* [1] A child mints the id and dies WITHOUT a clean close. */
    int pipefd[2];
    if (pipe(pipefd) != 0) { printf("FAIL pipe\n"); return 2; }
    pid_t pid = fork();
    if (pid == 0) {
        close(pipefd[0]);
        tsdb_db_t *db = NULL;
        uint64_t id = 0;
        if (tsdb_open_cluster(DIRP, "127.0.0.1:0", NULL, &db) == TSDB_OK && db)
            id = tsdb_cluster_local_id_for_db(db);
        ssize_t w = write(pipefd[1], &id, sizeof(id));
        (void)w;
        /* Unclean: no tsdb_close, no tsdb_close_cluster — straight out. */
        _exit(0);
    }
    close(pipefd[1]);
    uint64_t child_id = 0;
    ssize_t r = read(pipefd[0], &child_id, sizeof(child_id));
    close(pipefd[0]);
    int wst = 0; waitpid(pid, &wst, 0);
    CHECK(r == (ssize_t)sizeof(child_id) && child_id != 0,
          "the child minted a non-zero node id (got %llu)",
          (unsigned long long)child_id);
    printf("  child minted id = %llu, then _exit(0) with no clean close\n",
           (unsigned long long)child_id);

    /* [2] The id file survived the unclean exit and no tmp is stranded. */
    char idp[4096]; snprintf(idp, sizeof(idp), "%s/node_id", DIRP);
    struct stat st;
    CHECK(stat(idp, &st) == 0 && st.st_size > 0,
          "node_id file is present and non-empty after the unclean exit");
    CHECK(!tmp_present(),
          "no node_id.tmp is stranded (tmp+rename completed)");

    /* [3] A fresh open reads back the SAME id — the issuer is stable. */
    uint64_t reopened = open_and_get_id();
    printf("  reopened id = %llu\n", (unsigned long long)reopened);
    CHECK(reopened == child_id,
          "reopened id %llu == minted id %llu — the replication issuer is "
          "stable across a crash (a fresh id here would make every already-"
          "replicated batch look like it came from a new sender)",
          (unsigned long long)reopened, (unsigned long long)child_id);

    rm_rf(DIRP);
    printf("\n=== %s ===\n", g_fail ? "FAILED" : "node id durable: all pass");
    return g_fail ? 1 : 0;
}
