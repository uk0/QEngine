/* dedup.c — exact receiver-side dedup ledger.  See dedup.h for the design and
 * for why a scalar high-water mark (or a Bloom filter) is unsafe here. */

#include "dedup.h"
#include "../../include/tsdb.h"

#include "../server/proto.h"   /* tsdb_crc32c */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

uint64_t tsdb_stream_id(uint64_t issuer_node_id, uint64_t table_incarnation) {
    /* 0 on either side means the identity is UNKNOWN — a pre-incarnation sender
     * or a table created down a path that stamps 0.  Refuse to mint an id from
     * it: a fabricated stream would either collide with a real one or make every
     * retry look new.  The caller applies without dedup instead. */
    if (issuer_node_id == 0 || table_incarnation == 0) return 0;

    /* 128 -> 64 mix.  Both inputs are already well-distributed (an FNV-1a node
     * id, a Raft-(term,index)-derived incarnation), so this only has to avoid
     * cancelling them against each other — hence the asymmetric constants and
     * the final avalanche rather than a plain XOR. */
    uint64_t h = issuer_node_id * 0x9E3779B97F4A7C15ULL;
    h ^= table_incarnation + 0x165667B19E3779F9ULL + (h << 6) + (h >> 2);
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h ? h : 1ULL;   /* 0 is reserved for "no stream" */
}

/* One stream's state: a contiguous frontier plus the exact out-of-order tail.
 * `gaps` is kept SORTED ASCENDING so absorbing a newly-contiguous run after the
 * frontier advances is a prefix walk, and membership is a binary search. */
typedef struct {
    uint64_t  stream;      /* 0 = free slot */
    uint64_t  frontier;    /* every seq in [1, frontier] is applied */
    uint64_t *gaps;        /* applied seqs > frontier, ascending */
    size_t    ngaps;
    size_t    cap;
} stream_t;

struct tsdb_dedup_ledger {
    stream_t *streams;
    size_t    nstreams;    /* slots in use */
    size_t    max_streams;
    size_t    max_gap;
};

int tsdb_dedup_open(size_t max_streams, size_t max_gap,
                    tsdb_dedup_ledger_t **out)
{
    if (!out || max_streams == 0 || max_gap == 0) return TSDB_ERR_INVAL;
    *out = NULL;

    tsdb_dedup_ledger_t *l = calloc(1, sizeof(*l));
    if (!l) return TSDB_ERR_NOMEM;
    l->streams = calloc(max_streams, sizeof(*l->streams));
    if (!l->streams) { free(l); return TSDB_ERR_NOMEM; }
    l->max_streams = max_streams;
    l->max_gap     = max_gap;
    *out = l;
    return TSDB_OK;
}

void tsdb_dedup_close(tsdb_dedup_ledger_t *l) {
    if (!l) return;
    for (size_t i = 0; i < l->max_streams; i++) free(l->streams[i].gaps);
    free(l->streams);
    free(l);
}

/* Linear scan: a receiver talks to a handful of peers, so the stream count is
 * single digits in practice and a hash map would cost more than it saves. */
static stream_t *find(const tsdb_dedup_ledger_t *l, uint64_t stream) {
    for (size_t i = 0; i < l->max_streams; i++)
        if (l->streams[i].stream == stream) return (stream_t *)&l->streams[i];
    return NULL;
}

static stream_t *find_or_add(tsdb_dedup_ledger_t *l, uint64_t stream) {
    stream_t *s = find(l, stream);
    if (s) return s;
    for (size_t i = 0; i < l->max_streams; i++) {
        if (l->streams[i].stream == 0) {
            l->streams[i].stream   = stream;
            l->streams[i].frontier = 0;
            l->streams[i].ngaps    = 0;
            l->nstreams++;
            return &l->streams[i];
        }
    }
    return NULL;   /* no slot — caller turns this into NOMEM, never "seen" */
}

/* Index of `seq` in the sorted gaps array, or -1.  Also yields the insertion
 * point via *lo when absent. */
static long gap_find(const stream_t *s, uint64_t seq, size_t *lo_out) {
    size_t lo = 0, hi = s->ngaps;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (s->gaps[mid] == seq) { if (lo_out) *lo_out = mid; return (long)mid; }
        if (s->gaps[mid] < seq) lo = mid + 1; else hi = mid;
    }
    if (lo_out) *lo_out = lo;
    return -1;
}

int tsdb_dedup_seen(const tsdb_dedup_ledger_t *l, uint64_t stream, uint64_t seq) {
    if (!l || seq == 0) return 0;
    const stream_t *s = find(l, stream);
    if (!s) return 0;
    if (seq <= s->frontier) return 1;          /* under a CONTIGUOUS frontier */
    return gap_find(s, seq, NULL) >= 0;
}

/* After the frontier moves, swallow the run of gaps that is now contiguous with
 * it.  This is what keeps `gaps` bounded by the out-of-order window rather than
 * by how long the process has been running. */
static void absorb(stream_t *s) {
    size_t i = 0;
    while (i < s->ngaps && s->gaps[i] == s->frontier + 1) {
        s->frontier++;
        i++;
    }
    if (i > 0) {
        memmove(s->gaps, s->gaps + i, (s->ngaps - i) * sizeof(uint64_t));
        s->ngaps -= i;
    }
}

int tsdb_dedup_record(tsdb_dedup_ledger_t *l, uint64_t stream, uint64_t seq) {
    if (!l || seq == 0) return TSDB_ERR_INVAL;

    stream_t *s = find_or_add(l, stream);
    if (!s) return TSDB_ERR_NOMEM;

    if (seq <= s->frontier) return TSDB_ERR_EXISTS;

    size_t at = 0;
    if (gap_find(s, seq, &at) >= 0) return TSDB_ERR_EXISTS;

    if (seq == s->frontier + 1) {
        s->frontier = seq;
        absorb(s);
        return TSDB_OK;
    }

    /* Out of order.  Refuse rather than forget: with the window full we could
     * not record this seq exactly, and pretending otherwise would either drop a
     * batch that was never applied or admit one that was. */
    if (s->ngaps >= l->max_gap) return TSDB_ERR_FULL;

    if (s->ngaps == s->cap) {
        size_t nc = s->cap ? s->cap * 2 : 16;
        if (nc > l->max_gap) nc = l->max_gap;
        uint64_t *ng = realloc(s->gaps, nc * sizeof(uint64_t));
        if (!ng) return TSDB_ERR_NOMEM;
        s->gaps = ng;
        s->cap  = nc;
    }
    memmove(s->gaps + at + 1, s->gaps + at,
            (s->ngaps - at) * sizeof(uint64_t));
    s->gaps[at] = seq;
    s->ngaps++;
    return TSDB_OK;
}

uint64_t tsdb_dedup_frontier(const tsdb_dedup_ledger_t *l, uint64_t stream) {
    if (!l) return 0;
    const stream_t *s = find(l, stream);
    return s ? s->frontier : 0;
}

size_t tsdb_dedup_gap_count(const tsdb_dedup_ledger_t *l, uint64_t stream) {
    if (!l) return 0;
    const stream_t *s = find(l, stream);
    return s ? s->ngaps : 0;
}

int tsdb_dedup_set_frontier(tsdb_dedup_ledger_t *l, uint64_t stream,
                            uint64_t frontier)
{
    if (!l) return TSDB_ERR_INVAL;
    stream_t *s = find_or_add(l, stream);
    if (!s) return TSDB_ERR_NOMEM;
    /* Forward only.  Moving a frontier BACK would re-admit seqs already applied
     * and duplicate their rows — the exact failure this ledger prevents. */
    if (frontier > s->frontier) {
        s->frontier = frontier;
        /* Drop gap entries the restored frontier now covers, then absorb. */
        size_t i = 0;
        while (i < s->ngaps && s->gaps[i] <= s->frontier) i++;
        if (i > 0) {
            memmove(s->gaps, s->gaps + i, (s->ngaps - i) * sizeof(uint64_t));
            s->ngaps -= i;
        }
        absorb(s);
    }
    return TSDB_OK;
}

/* ---- Durable checkpoint -------------------------------------------------
 *
 * Layout, all little-endian:
 *   magic u32 'DDP1' | version u32 | count u32 | reserved u32
 *   count x { stream u64, frontier u64 }
 *   crc32c u32 over everything above
 *
 * The CRC is what makes "fail closed" possible: without it a truncated file
 * would silently restore a PREFIX of the streams, and the streams left out
 * would then re-admit already-applied batches while the ones restored looked
 * fine — the worst kind of half-state.  With it, damage restores nothing.
 */

#define DDP_MAGIC   0x31504444u   /* 'DDP1' */
#define DDP_VERSION 1u
#define DDP_HDR     16u

static void put32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static void put64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }
static uint32_t get32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static uint64_t get64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }

int tsdb_dedup_checkpoint_save(const tsdb_dedup_ledger_t *l, const char *path) {
    if (!l || !path) return TSDB_ERR_INVAL;

    /* Only streams that have actually advanced are worth a record. */
    size_t n = 0;
    for (size_t i = 0; i < l->max_streams; i++)
        if (l->streams[i].stream != 0 && l->streams[i].frontier != 0) n++;

    size_t body = DDP_HDR + n * 16;
    uint8_t *buf = malloc(body + 4);
    if (!buf) return TSDB_ERR_NOMEM;

    put32(buf + 0,  DDP_MAGIC);
    put32(buf + 4,  DDP_VERSION);
    put32(buf + 8,  (uint32_t)n);
    put32(buf + 12, 0);
    size_t off = DDP_HDR;
    for (size_t i = 0; i < l->max_streams; i++) {
        if (l->streams[i].stream == 0 || l->streams[i].frontier == 0) continue;
        put64(buf + off,     l->streams[i].stream);
        put64(buf + off + 8, l->streams[i].frontier);
        off += 16;
    }
    put32(buf + body, tsdb_crc32c(buf, body));

    char tmp[4200];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    int rc = TSDB_ERR_IO;
    FILE *f = fopen(tmp, "wb");
    if (f) {
        if (fwrite(buf, 1, body + 4, f) == body + 4 &&
            fflush(f) == 0 && fsync(fileno(f)) == 0) {
            rc = TSDB_OK;
        }
        fclose(f);
    }
    free(buf);
    if (rc != TSDB_OK) { unlink(tmp); return rc; }

    if (rename(tmp, path) != 0) { unlink(tmp); return TSDB_ERR_IO; }

    /* The rename itself must survive a crash, or the checkpoint can vanish and
     * take the dedup state with it — the same reason the node id fsyncs its
     * directory. */
    char dir[4200];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        int dfd = open(dir[0] ? dir : "/", O_RDONLY);
        if (dfd >= 0) { (void)fsync(dfd); close(dfd); }
    }
    return TSDB_OK;
}

int tsdb_dedup_checkpoint_load(tsdb_dedup_ledger_t *l, const char *path) {
    if (!l || !path) return TSDB_ERR_INVAL;

    FILE *f = fopen(path, "rb");
    if (!f) return TSDB_OK;            /* fresh node — nothing to restore */

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return TSDB_ERR_IO; }
    long sz = ftell(f);
    if (sz < (long)(DDP_HDR + 4)) { fclose(f); return TSDB_ERR_CORRUPT; }
    rewind(f);

    uint8_t *buf = malloc((size_t)sz);
    if (!buf) { fclose(f); return TSDB_ERR_NOMEM; }
    if (fread(buf, 1, (size_t)sz, f) != (size_t)sz) {
        free(buf); fclose(f); return TSDB_ERR_IO;
    }
    fclose(f);

    size_t body = (size_t)sz - 4;
    if (get32(buf) != DDP_MAGIC || get32(buf + 4) != DDP_VERSION ||
        tsdb_crc32c(buf, body) != get32(buf + body)) {
        free(buf);
        return TSDB_ERR_CORRUPT;       /* restore NOTHING — see the header */
    }
    uint32_t n = get32(buf + 8);
    if (DDP_HDR + (size_t)n * 16 != body) { free(buf); return TSDB_ERR_CORRUPT; }

    /* Only now, with the whole file verified, is anything applied — so a
     * damaged tail can never leave a half-restored ledger. */
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *rec = buf + DDP_HDR + (size_t)i * 16;
        (void)tsdb_dedup_set_frontier(l, get64(rec), get64(rec + 8));
    }
    free(buf);
    return TSDB_OK;
}
