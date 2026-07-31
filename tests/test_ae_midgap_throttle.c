/* test_ae_midgap_throttle.c — the middle-gap stderr line must be rate-limited,
 * the counter must not.
 *
 * THE BUG (4b70d4b made anti-entropy periodic).  ae_attempt_cb's SKIP_UNSAFE
 * branch logged an fprintf(stderr) AND bumped qengine_antientropy_middle_gap
 * _total ONCE PER divergent candidate PER table PER sweep, forever.  A table
 * permanently middle-gapped against three peers = 3 lines every 30 s = ~8,600
 * lines/day/table with no rate limit.
 *
 * THE FIX under test: tsdb_ae_midgap_should_log throttles the HUMAN line — it
 * fires on the TRANSITION into middle-gap and then at most once per N sweeps,
 * and at most once per sweep no matter how many peers diverge.  The counter
 * (bumped unconditionally in ae_attempt_cb) is untouched and remains the
 * scrapeable signal.  The first occurrence is never lost.
 *
 * Pure policy, driven deterministically over synthetic sweep numbers.
 */
#include "../src/storage/antientropy.h"

#include <stdio.h>

static int g_fail = 0;
#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

int main(void) {
    printf("=== test_ae_midgap_throttle ===\n");

    /* [1] first sight logs — the TRANSITION into middle-gap is never lost. */
    printf("\n[1] first occurrence logs; same-sweep repeats do not\n");
    CHECK(tsdb_ae_midgap_should_log("t_a", 1, 60) == 1,
          "first time 't_a' is middle-gapped, log the line");
    CHECK(tsdb_ae_midgap_should_log("t_a", 1, 60) == 0,
          "a 2nd divergent peer in the SAME sweep does not log again");
    CHECK(tsdb_ae_midgap_should_log("t_a", 1, 60) == 0,
          "a 3rd divergent peer in the same sweep is silent too");

    /* [2] a persistently-gapped table (contacted EVERY sweep, as production
     *     does for a real middle gap) is silent for the whole window. */
    printf("\n[2] consecutive sweeps within the window are all suppressed\n");
    int silent = 1;
    for (long s = 2; s <= 60; s++)
        if (tsdb_ae_midgap_should_log("t_a", s, 60) != 0) silent = 0;
    CHECK(silent, "sweeps 2..60 (last logged at sweep 1) are all suppressed");

    /* [3] the throttle re-fires after exactly `every` sweeps, then goes quiet. */
    printf("\n[3] the line reappears once per N sweeps\n");
    CHECK(tsdb_ae_midgap_should_log("t_a", 61, 60) == 1,
          "sweep 61: 61-1 = 60 >= window -> re-log");
    CHECK(tsdb_ae_midgap_should_log("t_a", 62, 60) == 0, "sweep 62: suppressed again");

    /* [4] a gap that CLEARS and returns is a fresh transition (re-logs), so an
     *     operator sees a genuine re-divergence even inside a throttle window. */
    printf("\n[4] recovered-then-regapped is a fresh transition\n");
    CHECK(tsdb_ae_midgap_should_log("t_b", 5, 60) == 1, "t_b first gap at sweep 5");
    /* sweep 6: gap ABSENT -> should_log simply not called for t_b */
    CHECK(tsdb_ae_midgap_should_log("t_b", 7, 60) == 1,
          "t_b gapped again at sweep 7 after a clean sweep 6 -> re-log");

    /* [5] exact throttle-boundary math with a small window. */
    printf("\n[5] boundary math: every=3\n");
    CHECK(tsdb_ae_midgap_should_log("t_c", 1, 3) == 1, "s1 transition");
    CHECK(tsdb_ae_midgap_should_log("t_c", 2, 3) == 0, "s2 < window");
    CHECK(tsdb_ae_midgap_should_log("t_c", 3, 3) == 0, "s3 < window");
    CHECK(tsdb_ae_midgap_should_log("t_c", 4, 3) == 1, "s4: 4-1=3 >= 3 -> re-log");
    CHECK(tsdb_ae_midgap_should_log("t_c", 5, 3) == 0, "s5 suppressed");
    CHECK(tsdb_ae_midgap_should_log("t_c", 6, 3) == 0, "s6 suppressed");
    CHECK(tsdb_ae_midgap_should_log("t_c", 7, 3) == 1, "s7: 7-4=3 >= 3 -> re-log");

    /* [6] distinct tables throttle INDEPENDENTLY. */
    printf("\n[6] two tables do not share a throttle\n");
    CHECK(tsdb_ae_midgap_should_log("t_d", 100, 60) == 1, "t_d transition at s100");
    CHECK(tsdb_ae_midgap_should_log("t_e", 100, 60) == 1,
          "t_e transition at s100 logs independently of t_d");

    printf("\n=== test_ae_midgap_throttle: %s ===\n", g_fail ? "FAILED" : "ALL PASS");
    return g_fail ? 1 : 0;
}
