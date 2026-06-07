/* test_skiplist_monotonic.c — skip-list monotonic-append fast-path tests.
 *
 * memtable.c's sl_insert has a fast path: when keys arrive monotonically
 * non-decreasing (key >= monotonic_key) it splices at per-level tail
 * pointers in O(1); out-of-order keys fall back to the full multi-level
 * walk.  Regardless of insert order, tsdb_memtable_sorted_indices must
 * return a ts-ascending, stable (ties keep insertion order) permutation
 * of the row positions.
 *
 * Phases:
 *   (a) strictly ascending ts inserts (hits fast path) → is_sorted == 1
 *       and sorted_indices == identity 0..n-1.
 *   (b) strictly descending ts inserts (forces fallback walk) → is_sorted
 *       == 0 and sorted_indices yields ascending ts.
 *   (c) mixed: ascending run, one out-of-order insert, then more ascending
 *       (re-enters fast path) → sorted_indices fully sorted.
 *   (d) duplicate/equal ts (ties) → level-0 traversal keeps insertion
 *       order among equal-ts rows.
 *
 * For each phase the emitted ts sequence (read back through the memtable
 * column at the permuted indices) is verified to be both monotonically
 * non-decreasing AND a permutation of the inputs.
 */

#include "../src/storage/memtable.h"
#include "../src/storage/schema.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int g_pass = 0, g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *p) {
    char cmd[512]; snprintf(cmd, sizeof(cmd), "rm -rf %s", p); (void)system(cmd);
}

static tsdb_schema_t *make_schema(const char *dir) {
    rmrf(dir); mkdir(dir, 0755);
    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_INT64},
    };
    tsdb_schema_t *s = NULL;
    if (tsdb_schema_create_ex(dir, "t", cols, 2, "ts",
                               TSDB_PARTITION_DAY, 0, &s) != TSDB_OK) {
        fprintf(stderr, "schema_create failed\n");
        exit(1);
    }
    return s;
}

static void push_row(tsdb_memtable_t *m, int64_t ts, int64_t v) {
    tsdb_memtable_row_begin(m);
    tsdb_memtable_row_ts(m, (tsdb_ts_t)ts);
    tsdb_memtable_row_i64(m, 1, v);
    tsdb_memtable_row_end(m);
}

/* Verify, for a memtable holding `n` rows, that the permutation returned by
 * tsdb_memtable_sorted_indices over the ts column `in[]` is:
 *   1. a valid index permutation of 0..n-1,
 *   2. monotonically non-decreasing in ts, and
 *   3. a multiset permutation of the inputs (same ts values, same counts).
 * Returns 1 on full success, 0 otherwise. */
static int check_sorted_perm(tsdb_memtable_t *m, const int64_t *in, int n) {
    size_t *idx = malloc((size_t)n * sizeof(size_t));
    if (!idx) return 0;
    if (tsdb_memtable_sorted_indices(m, idx) != TSDB_OK) { free(idx); return 0; }

    const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, 0);
    int ok = 1;

    /* (1) index permutation: each position 0..n-1 used exactly once. */
    char *seen = calloc((size_t)n, 1);
    if (!seen) { free(idx); return 0; }
    for (int i = 0; i < n; i++) {
        if (idx[i] >= (size_t)n || seen[idx[i]]) { ok = 0; break; }
        seen[idx[i]] = 1;
    }
    free(seen);

    /* (2) ts non-decreasing through the permutation. */
    for (int i = 1; ok && i < n; i++)
        if (ts_buf[idx[i-1]] > ts_buf[idx[i]]) ok = 0;

    /* (3) multiset of permuted ts == multiset of inputs.  Both lists hold
     * the same values iff their sorted copies are equal element-by-element;
     * the permuted side is already sorted by (2), so sort a copy of in[]. */
    if (ok) {
        int64_t *a = malloc((size_t)n * sizeof(int64_t));   /* sorted inputs */
        if (!a) { free(idx); return 0; }
        memcpy(a, in, (size_t)n * sizeof(int64_t));
        for (int i = 1; i < n; i++) {                       /* insertion sort */
            int64_t k = a[i]; int j = i - 1;
            while (j >= 0 && a[j] > k) { a[j+1] = a[j]; j--; }
            a[j+1] = k;
        }
        for (int i = 0; i < n; i++)
            if (a[i] != ts_buf[idx[i]]) { ok = 0; break; }
        free(a);
    }

    free(idx);
    return ok;
}

int main(void) {
    printf("=== test_skiplist_monotonic ===\n");

    const char *dir = "/tmp/tsdb_test_skiplist_monotonic";
    rmrf(dir);
    mkdir(dir, 0755);

    enum { N = 2000 };

    /* ---- (a) strictly ascending → fast path → identity ---------------- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/a", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        int64_t in[N];
        for (int i = 0; i < N; i++) {
            in[i] = (int64_t)(i + 1) * 1000000LL;   /* strictly increasing */
            push_row(m, in[i], (int64_t)i);
        }

        CHECK(tsdb_memtable_is_sorted(m), "ascending: is_sorted == 1 (fast path)");

        size_t idx[N];
        tsdb_memtable_sorted_indices(m, idx);
        int identity = 1;
        for (int i = 0; i < N; i++) if (idx[i] != (size_t)i) { identity = 0; break; }
        CHECK(identity, "ascending: sorted_indices == identity 0..n-1");
        CHECK(check_sorted_perm(m, in, N),
              "ascending: permutation is sorted ts + multiset of inputs");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- (b) strictly descending → fallback walk → ascending ts ------- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/b", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        int64_t in[N];
        for (int i = 0; i < N; i++) {
            /* Insert order N..1; in[] mirrors that insertion sequence. */
            in[i] = (int64_t)(N - i) * 1000000LL;   /* strictly decreasing */
            push_row(m, in[i], (int64_t)i);
        }

        CHECK(!tsdb_memtable_is_sorted(m), "descending: is_sorted == 0 (fallback walk)");
        CHECK(check_sorted_perm(m, in, N),
              "descending: permutation is sorted ts + multiset of inputs");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- (c) mixed: ascending run, one out-of-order, then ascending --- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/c", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        int64_t in[N];
        int k = 0;
        /* First half ascending: 1000..(N/2 * 1000) — hits fast path. */
        for (int i = 1; i <= N / 2; i++) in[k++] = (int64_t)i * 1000LL;
        /* One out-of-order insert that lands inside the existing range,
         * forcing the legacy multi-level walk for this row. */
        in[k++] = 1500LL;                               /* between 1000 and 2000 */
        /* Then a strictly ascending tail far above the prior max, which
         * re-arms and re-enters the monotonic fast path. */
        for (int i = 1; k < N; i++) in[k++] = (int64_t)(N + i) * 1000LL;

        for (int i = 0; i < N; i++) push_row(m, in[i], (int64_t)i);

        CHECK(!tsdb_memtable_is_sorted(m), "mixed: is_sorted == 0 (out-of-order seen)");
        CHECK(check_sorted_perm(m, in, N),
              "mixed: permutation is sorted ts + multiset of inputs");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    /* ---- (d) duplicate/equal ts → stable insertion order among ties --- */
    {
        char td[256]; snprintf(td, sizeof(td), "%s/d", dir);
        tsdb_schema_t *s = make_schema(td);
        tsdb_memtable_t *m = NULL;
        tsdb_memtable_new(s, &m);

        /* Three ts groups (10/20/30 buckets), each with several equal-ts
         * rows.  v encodes the global insertion order so ties are checkable.
         * Groups are inserted ascending so the fast path is exercised while
         * still producing equal-key runs. */
        const int64_t group_ts[3] = {10000000LL, 20000000LL, 30000000LL};
        const int per_group = 5;
        const int NT = 3 * per_group;   /* 15 rows */
        int64_t in[15];
        int v = 0;
        for (int g = 0; g < 3; g++)
            for (int j = 0; j < per_group; j++) {
                in[v] = group_ts[g];
                push_row(m, group_ts[g], (int64_t)v);
                v++;
            }

        /* Fast path stays armed: equal keys satisfy key >= monotonic_key,
         * and groups are non-decreasing. */
        CHECK(tsdb_memtable_is_sorted(m), "ties: is_sorted == 1 (equal keys are non-decreasing)");
        CHECK(check_sorted_perm(m, in, NT),
              "ties: permutation is sorted ts + multiset of inputs");

        /* Stability: within each equal-ts run the v values (= insertion
         * order) must appear strictly increasing in the level-0 traversal. */
        size_t idx[15];
        tsdb_memtable_sorted_indices(m, idx);
        const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, 0);
        const int64_t *v_buf  = (const int64_t *)tsdb_memtable_col(m, 1);
        int stable = 1;
        for (int i = 1; i < NT; i++) {
            if (ts_buf[idx[i-1]] == ts_buf[idx[i]] &&
                v_buf[idx[i-1]] >= v_buf[idx[i]]) { stable = 0; break; }
        }
        CHECK(stable, "ties: equal-ts rows keep insertion order (stable)");

        tsdb_memtable_free(m);
        tsdb_schema_free(s);
    }

    rmrf(dir);
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
