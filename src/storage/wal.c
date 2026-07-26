/* wal.c — Write-Ahead Log implementation. */

#include "wal.h"
#include "io_async.h"
#include "../server/metrics.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <pthread.h>
#include <time.h>

/* Thread-local io_async ring used by tsdb_wal_sync — mirrors part.c's
 * pattern.  Iter 5: 2000-table workloads were spawning 2000 per-table
 * group_commit threads, almost all sleeping on futex.  Disabling
 * group_commit (TSDB_GROUP_COMMIT_US=0) collapses those threads, but
 * exposes plain fsync() in the writer's path; routing it through
 * io_uring restores the kernel-side pipelining we'd otherwise lose.
 * Default ON when liburing is linked + ring create succeeds; falls
 * back to plain fsync() on any failure.  Opt-out: TSDB_WAL_IO_URING=0. */
static __thread tsdb_io_async_t *tl_wal_io_async   = NULL;
static __thread int              tl_wal_io_async_inited = 0;

static int wal_async_fsync(int fd) {
    if (!tl_wal_io_async_inited) {
        tl_wal_io_async_inited = 1;
        const char *e = getenv("TSDB_WAL_IO_URING");
        int enabled = !(e && e[0] == '0');
        if (enabled && tsdb_io_async_available()) {
            if (tsdb_io_async_create(32, &tl_wal_io_async) != 0)
                tl_wal_io_async = NULL;
        }
    }
    if (tl_wal_io_async &&
        tsdb_io_async_fsync_sync(tl_wal_io_async, fd) == 0) return 0;
    return fsync(fd);
}

/* Forward declaration of mkdir_p from schema.c. */
extern int tsdb_mkdir_p(const char *path);

/* ---- CRC-32 (IEEE 802.3) ----------------------------------------------- */

/*
 * Slice-by-8 over the same reflected IEEE polynomial (0xEDB88320) the
 * byte-at-a-time loop used, so every CRC this file has ever written still
 * verifies — the values are identical, only the schedule changes.  Worth doing:
 * CRC was 178 ms of the 198 ms spent inside tsdb_wal_append (~56% of commit in
 * wal-only mode); measured 411 MB/s byte-at-a-time vs 1894 MB/s here.
 * crc32_table[0] is still the plain byte table, used for the tail and by the
 * callers that walk a few header bytes.
 */
static uint32_t crc32_table[8][256];
static int      crc32_table_ready = 0;

static void crc32_init(void) {
    if (crc32_table_ready) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = crc32_table[0][i];
        for (int s = 1; s < 8; s++) {
            c = crc32_table[0][c & 0xFF] ^ (c >> 8);
            crc32_table[s][i] = c;
        }
    }
    crc32_table_ready = 1;
}

/* Fold `n` bytes into the running CRC `c`. */
static uint32_t crc32_upd(uint32_t c, const uint8_t *p, size_t n) {
    while (n >= 8) {
        c ^= (uint32_t)p[0] | ((uint32_t)p[1] << 8)
           | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
        uint32_t d = (uint32_t)p[4] | ((uint32_t)p[5] << 8)
                   | ((uint32_t)p[6] << 16) | ((uint32_t)p[7] << 24);
        c = crc32_table[7][ c        & 0xFF] ^ crc32_table[6][(c >>  8) & 0xFF]
          ^ crc32_table[5][(c >> 16) & 0xFF] ^ crc32_table[4][(c >> 24) & 0xFF]
          ^ crc32_table[3][ d        & 0xFF] ^ crc32_table[2][(d >>  8) & 0xFF]
          ^ crc32_table[1][(d >> 16) & 0xFF] ^ crc32_table[0][(d >> 24) & 0xFF];
        p += 8; n -= 8;
    }
    while (n--) c = crc32_table[0][(c ^ *p++) & 0xFF] ^ (c >> 8);
    return c;
}

/* ---- WAL struct --------------------------------------------------------- */

struct tsdb_wal {
    int  fd;           /* file descriptor, opened O_WRONLY|O_APPEND|O_CREAT */
    char path[4096];   /* for truncate / replay */
    int  dirty;        /* interval mode: 1 = appended since last fsync
                          (plain int, accessed via __atomic builtins) */
};

/* ---- Interval fsync mode (TSDB_WAL_SYNC_MS) -----------------------------
 *
 * Default (env unset / 0): tsdb_wal_sync fdatasyncs on every commit — the
 * commit ack means "on stable storage".  On this HDD cluster that is a
 * ~30 ms jbd2 commit cycle per ack, and it is THE ingest ceiling (batch-size
 * A/B: commit rate pinned at ~500-548/s regardless of flush work).
 *
 * TSDB_WAL_SYNC_MS=N flips the WAL to Kafka-style acks: tsdb_wal_sync only
 * marks the WAL dirty and returns; one global flusher thread fdatasyncs
 * every dirty WAL each N ms.  A process crash loses NOTHING (the appends
 * are in the page cache, which survives the process); only a POWER CUT can
 * lose up to the last N ms of acked rows.  The commit ack therefore means
 * "on stable storage within N ms".  Opt-in per deployment.
 *
 * The mode is latched once per process on first WAL use. */
static int             g_wal_sync_ms      = -1;   /* -1 = not read yet */
static pthread_mutex_t g_wal_reg_mu       = PTHREAD_MUTEX_INITIALIZER;
static tsdb_wal_t    **g_wal_reg          = NULL;
static int             g_wal_reg_n        = 0;
static int             g_wal_reg_cap      = 0;
static pthread_once_t  g_wal_flusher_once = PTHREAD_ONCE_INIT;

static int wal_interval_ms(void) {
    if (g_wal_sync_ms < 0) {
        const char *e = getenv("TSDB_WAL_SYNC_MS");
        g_wal_sync_ms = (e && *e) ? atoi(e) : 0;
        if (g_wal_sync_ms < 0) g_wal_sync_ms = 0;
    }
    return g_wal_sync_ms;
}

static void *wal_flusher_main(void *arg) {
    (void)arg;
    const int ms = wal_interval_ms();
    for (;;) {
        struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
        nanosleep(&ts, NULL);
        /* fsync under the registry mutex: open/close are cold paths, and
         * holding it makes the fd guaranteed-valid for the whole sync (no
         * use-after-close / fd-reuse race with tsdb_wal_close). */
        pthread_mutex_lock(&g_wal_reg_mu);
        for (int i = 0; i < g_wal_reg_n; i++) {
            tsdb_wal_t *w = g_wal_reg[i];
            if (__atomic_exchange_n(&w->dirty, 0, __ATOMIC_ACQ_REL)) {
                if (wal_async_fsync(w->fd) == 0)
                    tsdb_metric_inc("qengine_wal_interval_fsync_total");
                else
                    __atomic_store_n(&w->dirty, 1, __ATOMIC_RELEASE);
            }
        }
        pthread_mutex_unlock(&g_wal_reg_mu);
    }
    return NULL;
}

static void wal_flusher_start(void) {
    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&tid, &attr, wal_flusher_main, NULL);
    pthread_attr_destroy(&attr);
}

static void wal_reg_add(tsdb_wal_t *w) {
    pthread_mutex_lock(&g_wal_reg_mu);
    if (g_wal_reg_n >= g_wal_reg_cap) {
        int ncap = g_wal_reg_cap ? g_wal_reg_cap * 2 : 256;
        tsdb_wal_t **nr = realloc(g_wal_reg, (size_t)ncap * sizeof(*nr));
        if (!nr) { pthread_mutex_unlock(&g_wal_reg_mu); return; }
        g_wal_reg = nr; g_wal_reg_cap = ncap;
    }
    g_wal_reg[g_wal_reg_n++] = w;
    pthread_mutex_unlock(&g_wal_reg_mu);
}

static void wal_reg_remove(tsdb_wal_t *w) {
    pthread_mutex_lock(&g_wal_reg_mu);
    for (int i = 0; i < g_wal_reg_n; i++) {
        if (g_wal_reg[i] == w) {
            g_wal_reg[i] = g_wal_reg[--g_wal_reg_n];
            break;
        }
    }
    pthread_mutex_unlock(&g_wal_reg_mu);
}

/* ---- Record format helpers ---------------------------------------------- */

/*
 * Record header: [crc32 u32][len u32] = 8 bytes.
 * crc32 covers the len field and payload.
 */
#define WAL_RECORD_HEADER 8

static void put_u32le_buf(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static uint32_t get_u32le_buf(const uint8_t *p) {
    return (uint32_t)p[0]
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

/* ---- Torn-tail repair ----------------------------------------------------
 *
 * A power cut mid-append leaves a PARTIAL record at the end of the log.  The
 * writer fd is O_APPEND, so every record appended after the reopen lands AFTER
 * those garbage bytes, while tsdb_wal_replay still stops AT them: on the next
 * recovery every one of those acked records is silently dropped.  Repair the
 * log to the end of its last CRC-valid record BEFORE the fd can append to it.
 *
 * The scan is deliberately independent of tsdb_wal_replay: it allocates
 * nothing (a garbage length field can therefore never OOM it) and it
 * classifies its own stop reason, so a genuinely torn tail (short read / bad
 * CRC) is distinguished from a read error, which must NEVER truncate.  It is
 * also independent of the replay CALLBACK, whose own TSDB_ERR_CORRUPT (a
 * CRC-valid record the decoder cannot parse) must not shorten the log either.
 */

/* Scan `path` and report the end offset of the longest prefix of CRC-valid
 * records (*out_end) plus the file size (*out_size).  Returns TSDB_ERR_IO on a
 * read error — the caller must NOT truncate in that case. */
static int wal_valid_prefix(const char *path, off_t *out_end, off_t *out_size) {
    FILE *f = fopen(path, "rb");
    if (!f) return TSDB_ERR_IO;

    struct stat st;
    if (fstat(fileno(f), &st) != 0) { fclose(f); return TSDB_ERR_IO; }
    off_t size = st.st_size;
    off_t off  = 0;

    crc32_init();

    while (off + WAL_RECORD_HEADER <= size) {
        uint8_t hdr[WAL_RECORD_HEADER];
        if (fread(hdr, 1, WAL_RECORD_HEADER, f) != WAL_RECORD_HEADER) break;

        uint32_t stored_crc = get_u32le_buf(hdr + 0);
        uint32_t len        = get_u32le_buf(hdr + 4);

        /* A length that runs past EOF can only be a torn tail (or a garbage
         * header).  Stop without reading — this also bounds the work an
         * arbitrary 4 GiB length field can cause. */
        if ((uint64_t)off + WAL_RECORD_HEADER + (uint64_t)len > (uint64_t)size)
            break;

        uint32_t c = 0xFFFFFFFFu;
        c = crc32_upd(c, hdr + 4, 4);

        int torn = 0;
        uint32_t left = len;
        while (left > 0) {
            uint8_t chunk[4096];
            size_t want = (left < sizeof(chunk)) ? (size_t)left : sizeof(chunk);
            size_t got  = fread(chunk, 1, want, f);
            if (got != want) { torn = 1; break; }
            c = crc32_upd(c, chunk, got);
            left -= (uint32_t)got;
        }
        if (torn) break;
        if ((c ^ 0xFFFFFFFFu) != stored_crc) break;

        off += WAL_RECORD_HEADER + (off_t)len;   /* record fully verified */
    }

    int io_err = ferror(f);
    fclose(f);
    if (io_err) return TSDB_ERR_IO;

    *out_end  = off;
    *out_size = size;
    return TSDB_OK;
}

int tsdb_wal_repair(tsdb_wal_t *w, uint64_t *discarded_out) {
    if (discarded_out) *discarded_out = 0;
    if (!w || w->fd < 0) return TSDB_ERR_INVAL;

    off_t end = 0, size = 0;
    int rc = wal_valid_prefix(w->path, &end, &size);
    if (rc != TSDB_OK) return rc;      /* read error: leave the log untouched */
    if (end == size) return TSDB_OK;   /* clean log (empty file included) */

    /* Truncate through the WRITER's fd — the same handle every append uses, so
     * the shortened size is what O_APPEND resolves against from here on.  No
     * lock is needed: the caller has not published this handle yet. */
    if (ftruncate(w->fd, end) < 0) return TSDB_ERR_IO;
    /* Make the shorter length durable before any new record is appended past
     * it.  fsync on the fd covers the inode; no parent-dir fsync is required
     * because the directory entry itself is unchanged. */
    if (fsync(w->fd) < 0) return TSDB_ERR_IO;

    if (discarded_out) *discarded_out = (uint64_t)(size - end);
    tsdb_metric_inc("qengine_wal_torn_tail_repaired_total");
    fprintf(stderr,
            "[wal] %s: repaired torn tail - dropped %lld byte(s) after the "
            "last valid record ending at offset %lld\n",
            w->path, (long long)(size - end), (long long)end);
    return TSDB_OK;
}

/* ---- Public API --------------------------------------------------------- */

int tsdb_wal_open(const char *db_dir, const char *table_name, tsdb_wal_t **out) {
    if (!db_dir || !table_name || !out) return TSDB_ERR_INVAL;

    /* Ensure wal directory exists. */
    char wal_dir[4096];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", db_dir);
    if (tsdb_mkdir_p(wal_dir) < 0) return TSDB_ERR_IO;

    tsdb_wal_t *w = calloc(1, sizeof(*w));
    if (!w) return TSDB_ERR_NOMEM;

    snprintf(w->path, sizeof(w->path), "%s/%s.log", wal_dir, table_name);

    w->fd = open(w->path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (w->fd < 0) { free(w); return TSDB_ERR_IO; }

    /* Repair a power-loss torn tail BEFORE this fd can append past it.  This is
     * the only choke point that hands out an appending handle, so no writer can
     * ever run ahead of the repair — and replay then sees a log that ends
     * cleanly.  Best-effort: a repair that cannot run (read error) leaves the
     * log exactly as it was. */
    (void)tsdb_wal_repair(w, NULL);

    if (wal_interval_ms() > 0) {
        pthread_once(&g_wal_flusher_once, wal_flusher_start);
        wal_reg_add(w);
    }

    *out = w;
    return TSDB_OK;
}

void tsdb_wal_close(tsdb_wal_t *w) {
    if (!w) return;
    if (wal_interval_ms() > 0) {
        wal_reg_remove(w);
        /* Drain the durability debt on clean close so a graceful shutdown
         * never loses acked rows even across a subsequent power cut. */
        if (w->fd >= 0 && __atomic_exchange_n(&w->dirty, 0, __ATOMIC_ACQ_REL))
            (void)fsync(w->fd);
    }
    if (w->fd >= 0) close(w->fd);
    free(w);
}

int tsdb_wal_append(tsdb_wal_t *w, const void *rec, size_t n) {
    if (!w || (!rec && n > 0)) return TSDB_ERR_INVAL;
    if (n > 0xFFFFFFFFu) return TSDB_ERR_OVERFLOW;

    /* Build header: [crc32][len] where crc covers len+payload. */
    uint8_t len_buf[4];
    put_u32le_buf(len_buf, (uint32_t)n);

    /* Compute CRC over the len field + payload. */
    uint32_t c = 0xFFFFFFFFu;
    crc32_init();
    c = crc32_upd(c, len_buf, 4);
    if (rec && n > 0) {
        const uint8_t *p = (const uint8_t *)rec;
        c = crc32_upd(c, p, n);
    }
    c ^= 0xFFFFFFFFu;

    uint8_t hdr[8];
    put_u32le_buf(hdr + 0, c);
    put_u32le_buf(hdr + 4, (uint32_t)n);

    /* One writev instead of two write()s: halves the syscalls per record and
     * closes the header-written/payload-not window a crash between the two
     * writes used to leave (replay already tolerates a torn tail, but a
     * single vectored append makes the common case atomic on O_APPEND). */
    struct iovec iov[2] = {
        { .iov_base = hdr,          .iov_len = 8 },
        { .iov_base = (void *)rec,  .iov_len = n },
    };
    ssize_t want = (ssize_t)(8 + n);
    /* Capture the pre-append size so a short/failed write (ENOSPC/quota) can
     * roll its partial bytes back.  Left in place, that torn record would — on
     * the O_APPEND fd — shadow every record appended after it, because replay
     * stops at the first CRC mismatch: the tail (all acked) is then silently
     * lost, and the next successful flush truncates it away for good.  Callers
     * serialise appends (compact_mtx / group-commit), so no concurrent writer
     * can race this fstat/ftruncate. */
    struct stat wst;
    off_t pre = (fstat(w->fd, &wst) == 0) ? wst.st_size : (off_t)-1;
    ssize_t wr = writev(w->fd, iov, n > 0 ? 2 : 1);
    if (wr != want) {
        if (pre >= 0) (void)ftruncate(w->fd, pre);
        return TSDB_ERR_IO;
    }
    return TSDB_OK;
}

int tsdb_wal_sync(tsdb_wal_t *w) {
    if (!w || w->fd < 0) return TSDB_ERR_INVAL;
    if (wal_interval_ms() > 0) {
        /* Interval mode: ack now, the global flusher makes it stable within
         * TSDB_WAL_SYNC_MS.  Appends are already in the page cache, so a
         * process crash loses nothing; only power loss can cost the window. */
        __atomic_store_n(&w->dirty, 1, __ATOMIC_RELEASE);
        return TSDB_OK;
    }
    return wal_async_fsync(w->fd) == 0 ? TSDB_OK : TSDB_ERR_IO;
}

int tsdb_wal_append_durable(tsdb_wal_t *w, const void *rec, size_t n) {
    if (!w || w->fd < 0) return TSDB_ERR_INVAL;

    /* Pre-append length, so a fsync that fails after a COMPLETE writev can put
     * the log back.  tsdb_wal_append covers the short-write case itself. */
    struct stat st;
    off_t pre = (fstat(w->fd, &st) == 0) ? st.st_size : (off_t)-1;

    int rc = tsdb_wal_append(w, rec, n);
    if (rc != TSDB_OK) return rc;          /* already rolled back */

    rc = tsdb_wal_sync(w);
    if (rc != TSDB_OK && pre >= 0) {
        /* The record is in the file but not on stable storage, and the caller
         * is about to be told the commit failed — so it will not advance the
         * seq this record carries and the next commit re-logs the same rows
         * under it.  Two live records with one seq replay as two, duplicating
         * the overlap.  Drop the record instead: its rows stay in the memtable
         * and are unacked, which is exactly what the returned error means. */
        (void)ftruncate(w->fd, pre);
    }
    return rc;
}

/* Legacy direct-fsync path kept available for code that explicitly wants
 * to bypass the async ring (e.g. test harnesses that don't want a
 * thread-local instance leaked).  Not currently called. */
static int tsdb_wal_sync_direct(tsdb_wal_t *w) {
    if (!w || w->fd < 0) return TSDB_ERR_INVAL;
    if (fsync(w->fd) < 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

int tsdb_wal_truncate(tsdb_wal_t *w) {
    if (!w || w->fd < 0) return TSDB_ERR_INVAL;

    /* Truncate to zero and seek to start. */
    if (ftruncate(w->fd, 0) < 0) return TSDB_ERR_IO;
    if (lseek(w->fd, 0, SEEK_SET) < 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

int tsdb_wal_replay(const char *db_dir, const char *table_name,
                    int (*cb)(const void *rec, size_t n, void *ctx), void *ctx)
{
    if (!db_dir || !table_name || !cb) return TSDB_ERR_INVAL;

    char path[4096];
    snprintf(path, sizeof(path), "%s/wal/%s.log", db_dir, table_name);

    FILE *f = fopen(path, "rb");
    if (!f) {
        /* File doesn't exist — nothing to replay. */
        return TSDB_OK;
    }

    crc32_init();

    int rc = TSDB_OK;
    void *buf = NULL;
    size_t buf_cap = 0;

    for (;;) {
        uint8_t hdr[8];
        size_t n = fread(hdr, 1, 8, f);
        if (n == 0) break; /* EOF */
        if (n < 8) { rc = TSDB_ERR_CORRUPT; break; }

        uint32_t stored_crc = get_u32le_buf(hdr + 0);
        uint32_t len        = get_u32le_buf(hdr + 4);

        /* Ensure buffer capacity. */
        if (len > buf_cap) {
            void *nb = realloc(buf, len ? len : 1);
            if (!nb) { rc = TSDB_ERR_NOMEM; break; }
            buf = nb;
            buf_cap = len;
        }

        if (len > 0 && fread(buf, 1, len, f) != len) {
            rc = TSDB_ERR_CORRUPT; break;
        }

        /* Verify CRC over [len_bytes][payload]. */
        uint32_t c = 0xFFFFFFFFu;
        c = crc32_upd(c, hdr + 4, 4);
        if (len > 0) {
            const uint8_t *p = (const uint8_t *)buf;
            c = crc32_upd(c, p, len);
        }
        c ^= 0xFFFFFFFFu;

        if (c != stored_crc) { rc = TSDB_ERR_CORRUPT; break; }

        rc = cb(buf, len, ctx);
        if (rc != TSDB_OK) break;
    }

    free(buf);
    fclose(f);
    return rc;
}

/* ---- Group-commit implementation ---------------------------------------- */

/*
 * State machine:
 *
 *   Writer thread                       BG thread
 *   ─────────────────────────           ───────────────────────────────────
 *   lock(gc->mu)
 *   tsdb_wal_append(...)                cond_timedwait(gc->append_cv, window)
 *   my_seq = ++gc->write_seq            OR nwaiters >= TSDB_GC_EAGER_WAITERS
 *   gc->nwaiters++
 *   if nwaiters >= EAGER: signal(bg_cv) wakes early
 *   while fsynced_seq < my_seq:         lock, capture target = write_seq
 *     cond_wait(gc->done_cv, gc->mu)    unlock, fsync(fd)
 *   gc->nwaiters--                      lock, fsynced_seq = target
 *   unlock(gc->mu)                      broadcast(done_cv)
 *   return TSDB_OK                      unlock
 *
 * Key invariants:
 * - gc->mu serializes all appends so no records interleave on the fd.
 * - A writer captures my_seq AFTER its append; the bg thread only
 *   marks fsynced_seq = write_seq captured BEFORE the fsync call,
 *   so it never over-reports durability.
 * - On stop: bg does a final fsync then sets fsynced_seq = write_seq
 *   and broadcasts, unblocking any remaining waiters.
 */

struct tsdb_group_commit {
    tsdb_wal_t     *wal;
    int64_t         batch_window_ns;

    pthread_mutex_t mu;
    pthread_cond_t  append_cv;  /* bg thread waits here for new appenders */
    pthread_cond_t  done_cv;    /* writers wait here for fsync completion  */

    uint64_t        write_seq;     /* incremented by each append_sync caller */
    uint64_t        fsynced_seq;   /* latest seq known durable after fsync   */
    int             nwaiters;      /* writers currently blocked in done_cv   */
    int             stop;          /* 1 = stopping, 2 = fully stopped        */
    int             last_fsync_rc; /* last fsync errno (0 = success)         */

    pthread_t       bg_thread;
};

/* Background thread: batches fsyncs.
 *
 * State machine:
 *
 *  IDLE:  write_seq == fsynced_seq — nothing to fsync.
 *         Sleeps on append_cv until a writer increments write_seq.
 *
 *  BATCH: write_seq > fsynced_seq — collecting writers.
 *         Waits up to batch_window_ns for more writers, or until
 *         nwaiters reaches TSDB_GC_EAGER_WAITERS (whichever comes first).
 *
 *  FSYNC: captures target_seq = write_seq, releases mutex, calls fsync(),
 *         reacquires mutex, advances fsynced_seq, broadcasts done_cv.
 *         Then explicitly yields the mutex (unlock+relock) so that all
 *         woken waiters have a chance to reacquire the mutex and finish
 *         their wait before the bg thread loops back to IDLE/BATCH.
 *
 * Without the yield after broadcast, the bg thread may spin in BATCH with
 * nwaiters >= EAGER and write_seq == fsynced_seq (all sequences already
 * covered), starving workers of the mutex they need to decrement nwaiters
 * and proceed to their next append.
 */
static void *gc_bg_thread(void *arg) {
    tsdb_group_commit_t *gc = (tsdb_group_commit_t *)arg;

    pthread_mutex_lock(&gc->mu);

    while (!gc->stop) {
        /* IDLE: wait until there is at least one unfsynced write. */
        while (!gc->stop && gc->write_seq == gc->fsynced_seq) {
            pthread_cond_wait(&gc->append_cv, &gc->mu);
        }
        if (gc->stop) break;

        /* BATCH: wait up to batch_window for more writers or eager threshold. */
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec  += gc->batch_window_ns / 1000000000LL;
        deadline.tv_nsec += gc->batch_window_ns % 1000000000LL;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }

        while (!gc->stop && gc->nwaiters < TSDB_GC_EAGER_WAITERS) {
            int r = pthread_cond_timedwait(&gc->append_cv, &gc->mu, &deadline);
            if (r == ETIMEDOUT) break;
            (void)r;
        }

        if (gc->stop) break;

        /* FSYNC: capture target, release lock, fsync, reacquire, advance seq. */
        uint64_t target_seq = gc->write_seq;

        pthread_mutex_unlock(&gc->mu);
        int frc = fsync(gc->wal->fd);
        pthread_mutex_lock(&gc->mu);

        gc->last_fsync_rc = (frc == 0) ? 0 : errno;
        if (target_seq > gc->fsynced_seq) {
            gc->fsynced_seq = target_seq;
        }

        /* Wake all blocked writers — each will check its own seq. */
        pthread_cond_broadcast(&gc->done_cv);

        /*
         * YIELD: release the mutex so that newly-woken waiters can
         * reacquire it, verify fsynced_seq >= my_seq, decrement nwaiters,
         * and proceed.  Without this unlock/relock, the bg thread may
         * spin in the BATCH section while holding the mutex, starving all
         * workers when nwaiters >= TSDB_GC_EAGER_WAITERS and no new
         * sequences have been added yet.
         */
        pthread_mutex_unlock(&gc->mu);
        pthread_mutex_lock(&gc->mu);
    }

    /* Final fsync on shutdown to drain any last unfsynced records. */
    uint64_t final_seq = gc->write_seq;
    pthread_mutex_unlock(&gc->mu);
    if (final_seq > 0) {
        fsync(gc->wal->fd);
    }
    pthread_mutex_lock(&gc->mu);
    gc->fsynced_seq = gc->write_seq; /* mark all remaining writes durable */
    gc->stop = 2;
    pthread_cond_broadcast(&gc->done_cv);
    pthread_mutex_unlock(&gc->mu);

    return NULL;
}

int tsdb_group_commit_start(tsdb_wal_t *w, int64_t batch_window_ns,
                             tsdb_group_commit_t **out)
{
    if (!w || batch_window_ns <= 0 || !out) return TSDB_ERR_INVAL;

    tsdb_group_commit_t *gc = calloc(1, sizeof(*gc));
    if (!gc) return TSDB_ERR_NOMEM;

    gc->wal             = w;
    gc->batch_window_ns = batch_window_ns;
    gc->write_seq       = 0;
    gc->fsynced_seq     = 0;
    gc->nwaiters        = 0;
    gc->stop            = 0;
    gc->last_fsync_rc   = 0;

    pthread_mutex_init(&gc->mu, NULL);
    pthread_cond_init(&gc->append_cv, NULL);
    pthread_cond_init(&gc->done_cv, NULL);

    int rc = pthread_create(&gc->bg_thread, NULL, gc_bg_thread, gc);
    if (rc != 0) {
        pthread_mutex_destroy(&gc->mu);
        pthread_cond_destroy(&gc->append_cv);
        pthread_cond_destroy(&gc->done_cv);
        free(gc);
        return TSDB_ERR_IO;
    }

    *out = gc;
    return TSDB_OK;
}

void tsdb_group_commit_stop(tsdb_group_commit_t *g) {
    if (!g) return;

    pthread_mutex_lock(&g->mu);
    g->stop = 1;
    pthread_cond_broadcast(&g->append_cv); /* wake bg thread */
    pthread_mutex_unlock(&g->mu);

    pthread_join(g->bg_thread, NULL);

    pthread_cond_destroy(&g->done_cv);
    pthread_cond_destroy(&g->append_cv);
    pthread_mutex_destroy(&g->mu);
    free(g);
}

int tsdb_wal_append_sync(tsdb_wal_t *w, tsdb_group_commit_t *g,
                          const void *rec, size_t n)
{
    if (!w || !g) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&g->mu);

    if (g->stop) {
        pthread_mutex_unlock(&g->mu);
        return TSDB_ERR_IO;
    }

    /* Append under the group-commit lock so records don't interleave. */
    int arc = tsdb_wal_append(w, rec, n);
    if (arc != TSDB_OK) {
        pthread_mutex_unlock(&g->mu);
        return arc;
    }

    /* Claim a sequence number AFTER the successful append. */
    uint64_t my_seq = ++g->write_seq;
    g->nwaiters++;

    /* Signal bg thread so it knows there is work. */
    pthread_cond_signal(&g->append_cv);

    /* Trigger eager batch if threshold reached. */
    if (g->nwaiters >= TSDB_GC_EAGER_WAITERS) {
        pthread_cond_broadcast(&g->append_cv);
    }

    /* Wait until our seq is durable or the thread has fully stopped. */
    while (g->fsynced_seq < my_seq && g->stop < 2) {
        pthread_cond_wait(&g->done_cv, &g->mu);
    }

    g->nwaiters--;
    int last_rc = g->last_fsync_rc;
    pthread_mutex_unlock(&g->mu);

    return (last_rc == 0) ? TSDB_OK : TSDB_ERR_IO;
}
