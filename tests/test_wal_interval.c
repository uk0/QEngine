/* test_wal_interval.c — TSDB_WAL_SYNC_MS interval-fsync WAL mode.
 *
 * In interval mode tsdb_wal_sync only marks the WAL dirty and acks; a
 * global flusher thread fdatasyncs every dirty WAL each N ms.  Verifies:
 *   [1] sync acks immediately (no error) and appends are replayable
 *   [2] the flusher actually fsyncs within the interval (metric advances)
 *   [3] re-dirty after a flush round is flushed again (steady-state)
 *   [4] close-with-dirty drains the debt (no crash) and replay stays intact
 *   [5] CRC replay: every appended record comes back byte-identical, in order
 *
 * The env is latched once per process, so this binary runs ONLY in interval
 * mode; the rest of the suite covers the default per-commit path.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/storage/wal.h"
#include "../src/server/metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>

#define ASSERT(c) do { if (!(c)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #c, __FILE__, __LINE__); abort(); } } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) { \
    fprintf(stderr, "rc=%d at %s:%d\n", _r, __FILE__, __LINE__); abort(); } } while (0)

static const char *DIR = "/tmp/tsdb_test_wal_interval";
static void rm_tree(const char *p) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c); }

static uint64_t metric_counter(const char *name) {
    size_t len = 0;
    char *txt = tsdb_metrics_render(&len);
    if (!txt) return 0;
    uint64_t val = 0;
    char needle[128]; snprintf(needle, sizeof(needle), "\n%s ", name);
    const char *p = strstr(txt, needle);
    if (p) { p += strlen(needle); val = (uint64_t)strtoull(p, NULL, 10); }
    free(txt);
    return val;
}

static void msleep(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* Replay callback: verify record i is 64 bytes of (uint8_t)(seq + j). */
typedef struct { int count; int bad; } replay_ctx_t;
static int replay_cb(const void *rec, size_t n, void *ctx) {
    replay_ctx_t *rc = (replay_ctx_t *)ctx;
    const uint8_t *p = (const uint8_t *)rec;
    if (n != 64) { rc->bad++; return 0; }
    for (int j = 0; j < 64; j++) {
        if (p[j] != (uint8_t)(rc->count + j)) { rc->bad++; break; }
    }
    rc->count++;
    return 0;
}

static void append_n(tsdb_wal_t *w, int start, int n) {
    uint8_t rec[64];
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < 64; j++) rec[j] = (uint8_t)(start + i + j);
        OK(tsdb_wal_append(w, rec, sizeof(rec)));
        OK(tsdb_wal_sync(w));          /* interval mode: marks dirty, acks */
    }
}

int main(void) {
    printf("=== test_wal_interval ===\n");

    /* Latch interval mode BEFORE any WAL use (read once per process). */
    setenv("TSDB_WAL_SYNC_MS", "25", 1);
    tsdb_metrics_init();
    rm_tree(DIR);
    char cmd[512]; snprintf(cmd, sizeof(cmd), "mkdir -p %s", DIR); (void)system(cmd);

    tsdb_wal_t *w = NULL;
    OK(tsdb_wal_open(DIR, "ivt", &w));

    /* [1] sync acks immediately. */
    uint64_t fsyncs0 = metric_counter("qengine_wal_interval_fsync_total");
    append_n(w, 0, 100);
    printf("  [1] 100 append+sync acked (no per-commit fsync errors)\n");

    /* [2] flusher fires within the interval. */
    msleep(90);                                  /* > 3 intervals of 25ms */
    uint64_t fsyncs1 = metric_counter("qengine_wal_interval_fsync_total");
    ASSERT(fsyncs1 > fsyncs0);                   /* it fsynced at least once */
    printf("  [2] flusher fsynced within interval (counter %llu -> %llu)\n",
           (unsigned long long)fsyncs0, (unsigned long long)fsyncs1);

    /* [3] re-dirty -> flushed again. */
    append_n(w, 100, 50);
    msleep(90);
    uint64_t fsyncs2 = metric_counter("qengine_wal_interval_fsync_total");
    ASSERT(fsyncs2 > fsyncs1);
    printf("  [3] steady-state re-flush works (%llu -> %llu)\n",
           (unsigned long long)fsyncs1, (unsigned long long)fsyncs2);

    /* [4] close with dirty debt: append, close immediately. */
    append_n(w, 150, 25);
    tsdb_wal_close(w);                            /* drains dirty via fsync */
    printf("  [4] close-with-dirty drained cleanly\n");

    /* [5] full replay: 175 records, in order, byte-identical (CRC pass). */
    replay_ctx_t rc = { 0, 0 };
    OK(tsdb_wal_replay(DIR, "ivt", replay_cb, &rc));
    ASSERT(rc.count == 175);
    ASSERT(rc.bad == 0);
    printf("  [5] replay: %d records intact, 0 corrupt\n", rc.count);

    rm_tree(DIR);
    printf("[PASS] WAL interval-fsync mode: ack-fast, flusher-durable, replay-intact\n");
    return 0;
}
