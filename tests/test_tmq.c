/* test_tmq.c — TMQ consumer-group lifecycle tests.
 *
 * Phases:
 *   1. CREATE CONSUMER GROUP + single JOIN → the only member owns
 *      every partition.
 *   2. Second JOIN → partitions rebalanced round-robin.
 *   3. COMMIT OFFSET, close + reopen the DB, verify offset persisted
 *      from consumer_groups.log replay.
 *   4. LEAVE GROUP → the remaining consumer owns every partition again.
 *
 * In addition, smoke-tests that each statement is reachable via the QTL
 * dispatcher (tsdb_query) — proves parser + lexer wire-up.
 */

#include "../include/tsdb.h"
#include "../src/catalog/tmq.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>

/* ---- Assertions --------------------------------------------------------- */

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
        g_fail++; \
    } else { \
        printf("PASS: %s\n", msg); \
        g_pass++; \
    } \
} while (0)

/* ---- Helpers ------------------------------------------------------------ */

static void ensure_topic_table(tsdb_db_t *db, const char *name) {
    tsdb_col_t cols[2];
    cols[0].name = "ts";  cols[0].type = TSDB_TYPE_TIMESTAMP;
    cols[1].name = "val"; cols[1].type = TSDB_TYPE_INT64;
    int rc = tsdb_create_table(db, name, cols, 2, "ts");
    /* Table may already exist across reopens; both are fine. */
    (void)rc;
}

/* Count how many partitions a given member owns. */
static int owned_count(const tsdb_tmq_group_t *g, const char *consumer_id) {
    int mi = -1;
    for (int i = 0; i < g->nmembers; i++) {
        if (strcmp(g->members[i].consumer_id, consumer_id) == 0) { mi = i; break; }
    }
    if (mi < 0) return 0;
    int n = 0;
    for (int p = 0; p < g->npart; p++) if (g->partition_owner[p] == mi) n++;
    return n;
}

static int result_first_symbol(tsdb_result_t *r, char *out, size_t cap) {
    if (!r) return -1;
    if (tsdb_result_next(r) <= 0) return -1;
    const char *s = tsdb_result_sym(r, 0);
    if (!s) return -1;
    snprintf(out, cap, "%s", s);
    return 0;
}

/* ---- Phase 1: create + single join ------------------------------------- */

static void phase1_single_join(tsdb_db_t *db) {
    printf("\n[phase 1] create + single join\n");
    tsdb_tmq_t *tmq = tsdb_db_tmq(db);
    CHECK(tmq != NULL, "tsdb_db_tmq returns handle");
    if (!tmq) return;

    int rc = tsdb_tmq_group_create(tmq, "g1", "topic_a");
    CHECK(rc == TSDB_OK, "group_create g1 ok");

    /* Duplicate create is rejected. */
    rc = tsdb_tmq_group_create(tmq, "g1", "topic_a");
    CHECK(rc == TSDB_ERR_EXISTS, "duplicate group_create rejected");

    /* Joining a non-existent group must fail. */
    rc = tsdb_tmq_join(tmq, "doesnotexist", "c1");
    CHECK(rc == TSDB_ERR_NOTFOUND, "join unknown group -> NOTFOUND");

    rc = tsdb_tmq_join(tmq, "g1", "c1");
    CHECK(rc == TSDB_OK, "join c1 ok");

    tsdb_tmq_group_t g;
    rc = tsdb_tmq_group_get(tmq, "g1", &g);
    CHECK(rc == TSDB_OK, "group_get after join");
    CHECK(g.nmembers == 1, "exactly 1 member");
    CHECK(g.npart == TSDB_TMQ_NPARTITIONS,
          "npart == TSDB_TMQ_NPARTITIONS");
    CHECK(owned_count(&g, "c1") == TSDB_TMQ_NPARTITIONS,
          "sole member owns every partition");

    /* Joining same consumer twice is rejected. */
    rc = tsdb_tmq_join(tmq, "g1", "c1");
    CHECK(rc == TSDB_ERR_EXISTS, "duplicate join rejected");
}

/* ---- Phase 2: second consumer joins → rebalance ------------------------ */

static void phase2_rebalance(tsdb_db_t *db) {
    printf("\n[phase 2] second consumer joins -> rebalance\n");
    tsdb_tmq_t *tmq = tsdb_db_tmq(db);

    int rc = tsdb_tmq_join(tmq, "g1", "c2");
    CHECK(rc == TSDB_OK, "join c2 ok");

    tsdb_tmq_group_t g;
    tsdb_tmq_group_get(tmq, "g1", &g);
    CHECK(g.nmembers == 2, "2 members after 2nd join");

    /* With 4 partitions and 2 members, each owns exactly 2. */
    int c1_own = owned_count(&g, "c1");
    int c2_own = owned_count(&g, "c2");
    CHECK(c1_own + c2_own == TSDB_TMQ_NPARTITIONS,
          "partitions fully covered");
    CHECK(c1_own == 2 && c2_own == 2, "round-robin 2:2 split");

    /* Partition 0 goes to index 0, partition 1 to index 1, etc. */
    CHECK(g.partition_owner[0] != g.partition_owner[1],
          "adjacent partitions map to distinct owners");
}

/* ---- Phase 3: commit + reopen persists --------------------------------- */

static void phase3_commit_and_reopen(const char *data_dir) {
    printf("\n[phase 3] commit + reopen -> persisted offset\n");
    tsdb_db_t *db = NULL;
    int rc = tsdb_open(data_dir, &db);
    CHECK(rc == TSDB_OK, "reopen db");
    if (rc != TSDB_OK) return;

    tsdb_tmq_t *tmq = tsdb_db_tmq(db);
    CHECK(tmq != NULL, "tmq reopened");

    /* State from prior phases must have replayed. */
    tsdb_tmq_group_t g;
    rc = tsdb_tmq_group_get(tmq, "g1", &g);
    CHECK(rc == TSDB_OK, "g1 present after reopen");
    CHECK(g.nmembers == 2, "2 members replayed");

    /* Commit a seq from c1. It applies to c1's owned partitions. */
    rc = tsdb_tmq_commit(tmq, "g1", "c1", 12345);
    CHECK(rc == TSDB_OK, "commit c1 seq=12345");

    /* Monotone rejection: seq lower than current should fail. */
    rc = tsdb_tmq_commit(tmq, "g1", "c1", 999);
    CHECK(rc == TSDB_ERR_INVAL, "monotone: backwards commit rejected");

    /* Equal seq is permitted (>= current). */
    rc = tsdb_tmq_commit(tmq, "g1", "c1", 12345);
    CHECK(rc == TSDB_OK, "monotone: equal seq accepted");

    /* Close and reopen to verify offsets persist via the log. */
    tsdb_close(db);
    db = NULL;
    rc = tsdb_open(data_dir, &db);
    CHECK(rc == TSDB_OK, "reopen #2");

    tmq = tsdb_db_tmq(db);
    rc = tsdb_tmq_group_get(tmq, "g1", &g);
    CHECK(rc == TSDB_OK, "group survived close+reopen");

    /* Find c1's partitions and verify seq. */
    int mi = -1;
    for (int i = 0; i < g.nmembers; i++) {
        if (strcmp(g.members[i].consumer_id, "c1") == 0) { mi = i; break; }
    }
    CHECK(mi >= 0, "c1 replayed");

    int seq_matches = 0, partitions_checked = 0;
    for (int p = 0; p < g.npart; p++) {
        if (g.partition_owner[p] == mi) {
            int64_t seq = 0;
            int qrc = tsdb_tmq_get_offset(tmq, "g1", "c1", p, &seq);
            if (qrc == TSDB_OK && seq == 12345) seq_matches++;
            partitions_checked++;
        }
    }
    CHECK(partitions_checked > 0, "c1 owned at least one partition");
    CHECK(seq_matches == partitions_checked,
          "every c1-owned partition has seq=12345 after replay");

    tsdb_close(db);
}

/* ---- Phase 4: leave -> remaining member owns everything --------------- */

static void phase4_leave(const char *data_dir) {
    printf("\n[phase 4] leave -> remaining gets all partitions\n");
    tsdb_db_t *db = NULL;
    int rc = tsdb_open(data_dir, &db);
    CHECK(rc == TSDB_OK, "reopen for phase 4");
    tsdb_tmq_t *tmq = tsdb_db_tmq(db);

    rc = tsdb_tmq_leave(tmq, "g1", "c2");
    CHECK(rc == TSDB_OK, "leave c2 ok");

    tsdb_tmq_group_t g;
    tsdb_tmq_group_get(tmq, "g1", &g);
    CHECK(g.nmembers == 1, "1 member after leave");
    CHECK(owned_count(&g, "c1") == TSDB_TMQ_NPARTITIONS,
          "c1 regains every partition after c2 leaves");

    /* Duplicate leave fails. */
    rc = tsdb_tmq_leave(tmq, "g1", "c2");
    CHECK(rc == TSDB_ERR_NOTFOUND, "second leave -> NOTFOUND");

    tsdb_close(db);
}

/* ---- QTL smoke tests ---------------------------------------------------- */

static void qtl_smoke(const char *data_dir) {
    printf("\n[qtl] statement dispatch through tsdb_query()\n");

    /* Use a fresh sub-directory so we don't collide with the phases above. */
    char sub[300];
    snprintf(sub, sizeof(sub), "%s_qtl", data_dir);
    char cmd[400];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", sub);
    (void)system(cmd);

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(sub, &db);
    CHECK(rc == TSDB_OK, "qtl db open");
    ensure_topic_table(db, "topic_b");

    tsdb_result_t *r = NULL;
    char status[128];

    rc = tsdb_query(db, "CREATE CONSUMER GROUP gq ON topic_b", &r);
    CHECK(rc == TSDB_OK, "QTL: CREATE CONSUMER GROUP dispatched");
    if (r) {
        result_first_symbol(r, status, sizeof(status));
        CHECK(strstr(status, "OK:") == status, "QTL create returns OK status");
        tsdb_result_free(r); r = NULL;
    }

    rc = tsdb_query(db, "JOIN GROUP gq AS ca", &r);
    CHECK(rc == TSDB_OK, "QTL: JOIN GROUP dispatched");
    if (r) { result_first_symbol(r, status, sizeof(status));
             CHECK(strstr(status, "OK:") == status, "QTL join OK"); tsdb_result_free(r); r = NULL; }

    rc = tsdb_query(db, "JOIN GROUP gq AS cb", &r);
    CHECK(rc == TSDB_OK, "QTL: JOIN second consumer");
    if (r) { tsdb_result_free(r); r = NULL; }

    rc = tsdb_query(db, "COMMIT OFFSET gq AS ca AT 42", &r);
    CHECK(rc == TSDB_OK, "QTL: COMMIT OFFSET dispatched");
    if (r) { result_first_symbol(r, status, sizeof(status));
             CHECK(strstr(status, "OK:") == status, "QTL commit OK"); tsdb_result_free(r); r = NULL; }

    /* Monotone: backwards commit returns non-OK. */
    rc = tsdb_query(db, "COMMIT OFFSET gq AS ca AT 1", &r);
    CHECK(rc == TSDB_ERR_INVAL, "QTL: backwards commit rejected");
    if (r) { tsdb_result_free(r); r = NULL; }

    rc = tsdb_query(db, "LEAVE GROUP gq AS cb", &r);
    CHECK(rc == TSDB_OK, "QTL: LEAVE GROUP dispatched");
    if (r) { result_first_symbol(r, status, sizeof(status));
             CHECK(strstr(status, "OK:") == status, "QTL leave OK"); tsdb_result_free(r); r = NULL; }

    /* Verify underlying store got the right state. */
    tsdb_tmq_t *tmq = tsdb_db_tmq(db);
    tsdb_tmq_group_t g;
    rc = tsdb_tmq_group_get(tmq, "gq", &g);
    CHECK(rc == TSDB_OK && g.nmembers == 1,
          "QTL path: 1 member after LEAVE");

    /* Invalid topic is rejected at CREATE dispatch. */
    rc = tsdb_query(db, "CREATE CONSUMER GROUP zz ON not_a_table", &r);
    CHECK(rc == TSDB_ERR_NOTFOUND, "QTL: unknown topic rejected");
    if (r) { tsdb_result_free(r); r = NULL; }

    tsdb_close(db);
    (void)system(cmd);
}

/* ---- main --------------------------------------------------------------- */

int main(void) {
    printf("=== test_tmq ===\n");

    char data_dir[256];
    snprintf(data_dir, sizeof(data_dir), "/tmp/tsdb_tmq_%d", (int)getpid());
    char cleanup[300];
    snprintf(cleanup, sizeof(cleanup), "rm -rf %s %s_qtl", data_dir, data_dir);
    (void)system(cleanup);

    /* Phase 1 + 2 use a single open db; phase 3 + 4 reopen. */
    tsdb_db_t *db = NULL;
    int rc = tsdb_open(data_dir, &db);
    if (rc != TSDB_OK) { fprintf(stderr, "initial open failed\n"); return 1; }
    ensure_topic_table(db, "topic_a");

    phase1_single_join(db);
    phase2_rebalance(db);
    tsdb_close(db); /* forces fflush on the log */

    phase3_commit_and_reopen(data_dir);
    phase4_leave(data_dir);

    qtl_smoke(data_dir);

    (void)system(cleanup);

    printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
    return (g_fail > 0) ? 1 : 0;
}
