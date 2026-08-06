/* dedup.c — exact receiver-side dedup ledger.  See dedup.h for the design and
 * for why a scalar high-water mark (or a Bloom filter) is unsafe here. */

#include "dedup.h"
#include "../../include/tsdb.h"

#include <stdlib.h>
#include <string.h>

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
