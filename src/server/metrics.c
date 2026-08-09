/* metrics.c — Prometheus-format self-telemetry registry for QEngine.
 *
 * Design:
 *   - Fixed metric list compiled into a global array (no dynamic registration).
 *   - counter: _Atomic uint64_t  — fully lock-free
 *   - gauge:   _Atomic uint64_t  — store double bits as uint64 (IEEE 754 trick)
 *   - histogram: fixed buckets [0.5,1,5,10,50,100,500,1000,+Inf] ms
 *                per-metric pthread_mutex for atomic bucket+sum update;
 *                each bucket is _Atomic uint64_t
 *
 * Render: one big snprintf into a resizable buffer; returns heap copy.
 */

#define _POSIX_C_SOURCE 200809L

#include "metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <math.h>

/* ---- Histogram buckets --------------------------------------------------- */

#define HIST_NBUCKETS  9   /* 8 finite + 1 +Inf */

static const double HIST_BOUNDS[HIST_NBUCKETS] = {
    0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0,
    /* +Inf sentinel */ 1e300
};

/* ---- Metric kinds -------------------------------------------------------- */

typedef enum {
    MT_COUNTER = 0,
    MT_GAUGE,
    MT_HISTOGRAM
} metric_kind_t;

/* ---- Counter ------------------------------------------------------------- */

typedef struct {
    _Atomic uint64_t value;
} counter_t;

/* ---- Gauge --------------------------------------------------------------- */

typedef struct {
    /* Store double bits in uint64 so we can use atomic operations. */
    _Atomic uint64_t bits;
} gauge_t;

static inline double gauge_load(const gauge_t *g) {
    uint64_t bits = atomic_load_explicit(&g->bits, memory_order_relaxed);
    double v; memcpy(&v, &bits, 8); return v;
}
static inline void gauge_store(gauge_t *g, double v) {
    uint64_t bits; memcpy(&bits, &v, 8);
    atomic_store_explicit(&g->bits, bits, memory_order_relaxed);
}

/* ---- Histogram ----------------------------------------------------------- */

typedef struct {
    _Atomic uint64_t buckets[HIST_NBUCKETS]; /* cumulative counts, [i] = count(val <= bound[i]) */
    _Atomic uint64_t count;                  /* total observations */
    _Atomic uint64_t sum_bits;               /* sum as double bits */
    pthread_mutex_t  lock;                   /* serialises multi-field updates */
} histogram_t;

/* ---- Generic metric entry ------------------------------------------------ */

typedef struct {
    const char   *name;         /* e.g. "qengine_connections_total" */
    const char   *help;
    metric_kind_t kind;
    union {
        counter_t   c;
        gauge_t     g;
        histogram_t h;
    } data;
} metric_t;

/* ---- Global registry ----------------------------------------------------- */

/*
 * All metrics are declared statically so the compiler lays them out
 * in a fixed array — no dynamic allocation needed in the hot path.
 * Initialisation zeros atomics (C11: atomic_init is not needed for
 * statically-allocated objects whose initial value is 0 — C11 §6.7.9p10
 * combined with §7.17.2.1 — but we call tsdb_metrics_init() defensively).
 */
static metric_t g_metrics[] = {
    /* --- counters --- */
    { "qengine_connections_total",
      "Total client connections served", MT_COUNTER, { .c = { 0 } } },

    { "qengine_rows_written_total",
      "Total rows ingested across all tables", MT_COUNTER, { .c = { 0 } } },

    { "qengine_bytes_written_total",
      "Total payload bytes received for write batches", MT_COUNTER, { .c = { 0 } } },

    { "qengine_queries_total",
      "Total query requests processed", MT_COUNTER, { .c = { 0 } } },

    { "qengine_query_errors_total",
      "Total query requests that returned an error", MT_COUNTER, { .c = { 0 } } },

    { "qengine_flushes_total",
      "Total memtable flush operations to disk", MT_COUNTER, { .c = { 0 } } },

    { "qengine_compactions_total",
      "Total column-file compaction operations", MT_COUNTER, { .c = { 0 } } },

    { "qengine_compaction_paused_total",
      "Compactor cycles skipped because writer flush rate exceeded busy_threshold",
      MT_COUNTER, { .c = { 0 } } },

    { "qengine_compaction_memo_skipped_total",
      "Table dirs the compactor fast-skipped via mtime memo (no new flush since last cycle)",
      MT_COUNTER, { .c = { 0 } } },

    { "qengine_rawblock_pushes_total",
      "Total raw-block replication pushes sent", MT_COUNTER, { .c = { 0 } } },

    /* The registry is a FIXED table and tsdb_metric_inc() on an unlisted name is
     * a silent no-op, so a counter that is not here can never increment however
     * often it is bumped.  These three are the "damaged partition served" and
     * "colliding block refused" signals; the read path keeps the first two APART
     * on purpose (a missing block and an index that disagrees about the rows are
     * different facts), which is only observable if both are real counters. */
    { "qengine_short_column_read_total",
      "Reads that hit a ts block the column has no stored value for",
      MT_COUNTER, { .c = { 0 } } },

    { "qengine_block_ordinal_mismatch_total",
      "Reads where a column's block ordinal paired but described different rows",
      MT_COUNTER, { .c = { 0 } } },

    { "qengine_rawblock_ordinal_collision_total",
      "Raw-block pushes refused because the ordinal is held by a different block",
      MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_sent_total",
      "Total per-peer WRITE_BATCH RPCs fanned out from this node", MT_COUNTER, { .c = { 0 } } },

    { "qengine_fanout_wait_timeout_total",
      "Fanout quorum waits that hit the hard deadline (worker left behind)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_wal_interval_fsync_total",
      "WAL fdatasyncs issued by the interval flusher (TSDB_WAL_SYNC_MS mode)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_wal_torn_tail_repaired_total",
      "WAL logs whose power-loss torn tail was truncated at open", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_ack_total",
      "Total per-peer WRITE_BATCH RPCs that returned OK", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_fail_total",
      "Total per-peer WRITE_BATCH RPCs that failed (conn dead or peer error)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_recv_ok_total",
      "WRITE_BATCH RPCs received where the local batch_commit landed rows", MT_COUNTER, { .c = { 0 } } },

    { "qengine_wal_replay_dedup_skipped_total",
      "Redo records skipped during replay because their tagged (stream, seq) was already applied — the log held the same batch twice, so recovery applies it once instead of reproducing an over-count no count comparison could repair", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_dedup_dropped_total",
      "WRITE_BATCH RPCs dropped as already-applied by the (stream_id, seq) dedup gate and ACKed rather than re-applied (opt-in TSDB_DEDUP=1)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_dedup_window_full_total",
      "Dedup ledger records refused because a stream's out-of-order window was exhausted — the batch still applied, but its id could not be remembered, so a later retry may duplicate it", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_incarnation_reject_total",
      "WRITE_BATCH RPCs rejected because the batch's table incarnation did not match the receiver's — a stale batch for a DROP+recreated table, refused rather than applied to the wrong incarnation", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_bytes_raw_total",
      "Replication WRITE_BATCH payload bytes BEFORE lzlite compression", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_bytes_wire_total",
      "Replication WRITE_BATCH payload bytes actually sent (post-compression)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replicate_recv_err_total",
      "WRITE_BATCH RPCs received where the local apply failed (missing table, batch_begin fail, commit fail)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_antientropy_rows_pulled_total",
      "Rows pulled from a peer via anti-entropy resync (catch-up after downtime)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_antientropy_no_source_total",
      "Anti-entropy resyncs where every candidate peer reported rows but delivered none (empty peer echoing the cluster aggregate, or a peer that lost the table)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_antientropy_digest_mismatch_total",
      "Row-range digest buckets where a peer at EQUAL (count,max_ts) disagreed on content — a silent middle hole the (count,max_ts) probe cannot see; the divergent ts range is pull-merged (never truncated)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_antientropy_runaway_averted_total",
      "Row-digest merges stopped because they hit the per-sweep pull budget (one fullest-peer's worth) — a legitimate heal never reaches this, so a nonzero value means a merge was duplicating rows and was bounded instead of running away", MT_COUNTER, { .c = { 0 } } },

    { "qengine_shard_local_skipped_total",
      "Memtable flushes a non-owner skipped because owners ACKed the rows (Phase β.2)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_dr_sent_total",
      "Cross-DC WRITE_BATCH RPCs the forwarder pushed to the remote DC", MT_COUNTER, { .c = { 0 } } },

    { "qengine_dr_ack_total",
      "Cross-DC WRITE_BATCH RPCs the remote DC ACKed successfully", MT_COUNTER, { .c = { 0 } } },

    { "qengine_dr_fail_total",
      "Cross-DC WRITE_BATCH RPCs that failed (remote unreachable or apply error)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_dr_dropped_total",
      "Cross-DC batches dropped because the forwarder ring was full (backpressure)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_dr_recv_ok_total",
      "FED_INGEST RPCs received where local apply landed rows", MT_COUNTER, { .c = { 0 } } },

    { "qengine_dr_recv_err_total",
      "FED_INGEST RPCs received where local apply failed", MT_COUNTER, { .c = { 0 } } },

    { "qengine_retention_sweeps_total",
      "Retention GC sweeps completed (background or manual)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_retention_partitions_deleted_total",
      "Partitions deleted by the retention GC", MT_COUNTER, { .c = { 0 } } },

    { "qengine_retention_bytes_reclaimed_total",
      "Bytes freed on disk by retention GC", MT_COUNTER, { .c = { 0 } } },

    { "qengine_backup_bytes_streamed_total",
      "Total bytes streamed by the /backup endpoint (before HTTP framing)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_backup_requests_total",
      "Total /backup invocations", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replica_dial_total",
      "Total new TCP connections established to peers", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replica_dial_fail_total",
      "Total failed TCP dial attempts to peers", MT_COUNTER, { .c = { 0 } } },

    { "qengine_bloom_lookups_total",
      "Total bloom-filter lookup probes", MT_COUNTER, { .c = { 0 } } },

    { "qengine_bloom_skips_total",
      "Total partitions skipped due to bloom-filter negative", MT_COUNTER, { .c = { 0 } } },

    { "qengine_agg_stats_hit_total",
      "Times the aggregate fast-path consumed a block's precomputed stats",
      MT_COUNTER, { .c = { 0 } } },

    { "qengine_agg_stats_miss_total",
      "Times the aggregate fell back to full-block scan (stats absent or gate failed)",
      MT_COUNTER, { .c = { 0 } } },

    /* --- transport honesty ---
     *
     * tsdb_metric_inc() on a name that is not in THIS table is a silent no-op
     * (find_metric returns NULL).  Every counter below has a caller; adding
     * the caller without adding the row here ships a counter that can never
     * move, which reads on /metrics exactly like a bug that never fires. */
    { "qengine_rpc_conn_retired_total",
      "Client RPC connections retired because request/response alignment was lost (abandoned reply, short write, bad frame)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_rpc_reqid_mismatch_total",
      "Replies whose req_id did not match the request just sent — a wrong answer caught in the act", MT_COUNTER, { .c = { 0 } } },

    { "qengine_replica_conn_evicted_retired_total",
      "Retired connections dropped at the pool instead of being handed back to another caller", MT_COUNTER, { .c = { 0 } } },

    { "qengine_rpc_unknown_opcode_total",
      "Requests refused because this build does not implement the opcode (peer on a newer binary)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_schema_sync_recv_err_total",
      "SCHEMA_SYNC requests refused: payload did not decode, ncols out of range, or the local create failed", MT_COUNTER, { .c = { 0 } } },

    { "qengine_schema_sync_fail_total",
      "CREATE TABLE fanouts where too few peers took the schema (peers left without it until anti-entropy)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_ddl_forward_err_total",
      "DDL_FORWARD requests whose statement failed on this node (answered ERR, not an ACK carrying the error text)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_antientropy_sweeps_total",
      "Anti-entropy row-resync sweeps completed (startup pass plus every periodic tick)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_antientropy_middle_gap_total",
      "Anti-entropy refusals to close a middle gap (equal max_ts, higher peer count) — divergence that persists", MT_COUNTER, { .c = { 0 } } },

    { "qengine_fanout_dropped_total",
      "Replication fanout sends dropped because in-flight payload bytes hit TSDB_FANOUT_MAX_INFLIGHT_BYTES (slow peer backpressure)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_backup_flush_fail_total",
      "Backups refused because tsdb_db_flush_all could not put the memtable on disk", MT_COUNTER, { .c = { 0 } } },

    /* --- gauges --- */
    { "qengine_connections_active",
      "Current number of active client connections", MT_GAUGE, { .g = { 0 } } },

    { "qengine_memtable_rows",
      "Approximate total rows currently in all memtables", MT_GAUGE, { .g = { 0 } } },

    { "qengine_disk_bytes",
      "Approximate total bytes used by all column files on disk", MT_GAUGE, { .g = { 0 } } },

    { "qengine_batch_split_across_flush_total",
      "Bulk batches larger than one memtable, which therefore SPLIT across a flush: a prefix becomes durable and cannot be rolled back, so the batch is not atomic and a commit-failure rollback can only undo the tail", MT_COUNTER, { .c = { 0 } } },

    { "qengine_dedup_rows_removed_total",
      "Exact duplicate rows removed by an explicit operator-invoked partition de-duplication (never automatic — two identical rows are legal in a tick store, so only an operator can assert repeats are duplicates)", MT_COUNTER, { .c = { 0 } } },

    { "qengine_orphan_table_dirs_reaped_total",
      "Orphan table directories quarantined into .trash by the anti-entropy sweep (opt-in TSDB_AE_REAP_ORPHANS=1; only for names the catalog holds a DROP tombstone for, never on mere absence). Moved, not deleted", MT_COUNTER, { .c = { 0 } } },

    { "qengine_orphan_table_dirs",
      "Table directories on disk that the catalog knows nothing about — typically dropped while this node was down, so the tombstone kept them out of the catalog but nothing reaped the files (DROP cannot reach them either). Report only; nothing is deleted", MT_GAUGE, { .g = { 0 } } },

    { "qengine_cluster_nodes_alive",
      "Number of cluster nodes currently considered alive", MT_GAUGE, { .g = { 0 } } },

    { "qengine_subscriptions_active",
      "Number of active SUBSCRIBE sessions", MT_GAUGE, { .g = { 0 } } },

    /* --- counters that code increments but the registry had omitted, so
     *     tsdb_metric_inc was a silent no-op and they never appeared on
     *     /metrics.  Caught in bulk by test_metrics' registration scan. --- */
    { "qengine_antientropy_overcount_persistent_total",
      "Tables where this node held MORE rows than the authority for N consecutive sweeps at equal max_ts — a probable duplicate the count comparison cannot auto-repair", MT_COUNTER, { .c = { 0 } } },
    { "qengine_antientropy_partition_backfills_total",
      "Partitions rebuilt from a peer by anti-entropy", MT_COUNTER, { .c = { 0 } } },
    { "qengine_antientropy_truncate_averted_total",
      "Destructive full-pull truncates aborted because rows had committed since the empty measurement — locally-committed rows preserved", MT_COUNTER, { .c = { 0 } } },
    { "qengine_auth_denied_total",
      "Authentication attempts denied (bad credentials or missing grant)", MT_COUNTER, { .c = { 0 } } },
    { "qengine_auth_logins_total",
      "Successful authentications", MT_COUNTER, { .c = { 0 } } },
    { "qengine_catalog_dump_truncated_total",
      "CATALOG_DUMP replies that exceeded the receive buffer and were truncated", MT_COUNTER, { .c = { 0 } } },
    { "qengine_datadir_degraded_total",
      "Times a configured data directory was adopted DEGRADED (failed its health probe) rather than dropped", MT_COUNTER, { .c = { 0 } } },
    { "qengine_rawblock_ts_deferred_total",
      "Raw-block ts publishes deferred because a sibling non-ts column had not landed yet", MT_COUNTER, { .c = { 0 } } },
    { "qengine_wal_recovered_records_total",
      "WAL redo records replayed on open", MT_COUNTER, { .c = { 0 } } },
    { "qengine_wal_replay_aborted_benign_total",
      "WAL replays aborted for a benign reason (a partition checkpoint already covered the tail)", MT_COUNTER, { .c = { 0 } } },
    { "qengine_wal_replay_incomplete_total",
      "WAL replays that stopped at a torn or unreadable record, freezing the table until TRUNCATE", MT_COUNTER, { .c = { 0 } } },

    /* Influx line-protocol ingest accounting.  These count LINES seen and
     * LINES that failed (parse rejects + per-row commit/create failures),
     * NOT rows committed — a single data line is one row attempt, and the
     * ingest layer charges every row of a failed measurement group to
     * errors, so (lines_total - errors_total) is the net committed lines.
     * (A group larger than one memtable block whose commit fails mid-flush
     * charges the whole group, so the difference is a lower bound there —
     * it never credits a row that did not land.) */
    { "qengine_influx_lines_total",
      "InfluxDB line-protocol data lines received across all /write requests (one data line = one row attempt)", MT_COUNTER, { .c = { 0 } } },
    { "qengine_influx_line_errors_total",
      "InfluxDB line-protocol lines that failed to ingest: parse rejects plus rows dropped by an auto-create or per-row commit failure. Charged per row, so (lines_total - this) is the net committed lines (a lower bound if a multi-block group's commit fails mid-flush)", MT_COUNTER, { .c = { 0 } } },
    { "qengine_influx_write_requests_total",
      "InfluxDB /write HTTP requests handled (any status)", MT_COUNTER, { .c = { 0 } } },
    { "qengine_influx_write_partial_total",
      "InfluxDB /write requests that returned 204 while at least one line failed — the caller sees success but lost rows", MT_COUNTER, { .c = { 0 } } },

    /* --- histograms --- */
    { "qengine_query_duration_ms",
      "Query execution duration in milliseconds",
      MT_HISTOGRAM,
      { .h = { {0,0,0,0,0,0,0,0,0}, 0, 0, PTHREAD_MUTEX_INITIALIZER } } },

    { "qengine_ingest_batch_size",
      "Number of rows per ingested write batch",
      MT_HISTOGRAM,
      { .h = { {0,0,0,0,0,0,0,0,0}, 0, 0, PTHREAD_MUTEX_INITIALIZER } } },
};

#define N_METRICS  ((int)(sizeof(g_metrics)/sizeof(g_metrics[0])))

/* ---- Name -> index lookup table -----------------------------------------
 *
 * find_metric() runs on every metric mutation, multiple times per write
 * batch / RPC / query.  A linear strcmp scan over g_metrics[] is O(N).
 * Replace it with a static open-addressing hash table mapping
 * hash(name) -> index into g_metrics[].  Built once (pthread_once), so it
 * is safe under repeated tsdb_metrics_init() calls and against metric bumps
 * that race ahead of any explicit init.
 *
 * Size is the next power of two >= 4*N_METRICS, keeping the load factor
 * <= 0.25 so expected probe length is ~1.  Empty slots are marked -1.
 */
#define IDX_TABLE_SIZE  256   /* pow2 >= 4*N_METRICS (N_METRICS ~= 60) */

static int16_t g_idx_table[IDX_TABLE_SIZE];
static pthread_once_t g_build_once = PTHREAD_ONCE_INIT;

static uint64_t fnv1a(const char *s) {
    uint64_t h = 1469598103934665603ULL; /* FNV offset basis */
    for (; *s; s++) {
        h ^= (uint64_t)(unsigned char)*s;
        h *= 1099511628211ULL;           /* FNV prime */
    }
    return h;
}

/* Build the index table (and initialise atomics).  Runs exactly once. */
static void build_registry(void) {
    for (int i = 0; i < IDX_TABLE_SIZE; i++)
        g_idx_table[i] = -1;

    for (int i = 0; i < N_METRICS; i++) {
        metric_t *m = &g_metrics[i];
        switch (m->kind) {
        case MT_COUNTER:
            atomic_init(&m->data.c.value, (uint64_t)0);
            break;
        case MT_GAUGE:
            atomic_init(&m->data.g.bits, (uint64_t)0);
            break;
        case MT_HISTOGRAM:
            for (int b = 0; b < HIST_NBUCKETS; b++)
                atomic_init(&m->data.h.buckets[b], (uint64_t)0);
            atomic_init(&m->data.h.count, (uint64_t)0);
            atomic_init(&m->data.h.sum_bits, (uint64_t)0);
            /* mutex already initialised via PTHREAD_MUTEX_INITIALIZER */
            break;
        }

        /* Insert i into the open-addressing table keyed by name hash. */
        uint64_t slot = fnv1a(m->name) & (IDX_TABLE_SIZE - 1);
        while (g_idx_table[slot] != -1)
            slot = (slot + 1) & (IDX_TABLE_SIZE - 1);
        g_idx_table[slot] = (int16_t)i;
    }
}

void tsdb_metrics_init(void) {
    pthread_once(&g_build_once, build_registry);
}

/* ---- Registry lookup ----------------------------------------------------- */

static metric_t *find_metric(const char *name) {
    /* Fall back to building the table on first use (defensive init). */
    pthread_once(&g_build_once, build_registry);

    uint64_t slot = fnv1a(name) & (IDX_TABLE_SIZE - 1);
    for (;;) {
        int16_t idx = g_idx_table[slot];
        if (idx < 0)
            return NULL;                 /* empty slot — name not registered */
        if (strcmp(g_metrics[idx].name, name) == 0)
            return &g_metrics[idx];
        slot = (slot + 1) & (IDX_TABLE_SIZE - 1);
    }
}

/* ---- Counter ------------------------------------------------------------- */

void tsdb_metric_inc(const char *name) {
    metric_t *m = find_metric(name);
    if (!m || m->kind != MT_COUNTER) return;
    atomic_fetch_add_explicit(&m->data.c.value, 1, memory_order_relaxed);
}

void tsdb_metric_add(const char *name, uint64_t val) {
    metric_t *m = find_metric(name);
    if (!m || m->kind != MT_COUNTER) return;
    atomic_fetch_add_explicit(&m->data.c.value, val, memory_order_relaxed);
}

/* ---- Gauge --------------------------------------------------------------- */

void tsdb_metric_gauge_set(const char *name, double val) {
    metric_t *m = find_metric(name);
    if (!m || m->kind != MT_GAUGE) return;
    gauge_store(&m->data.g, val);
}

void tsdb_metric_gauge_inc(const char *name) {
    metric_t *m = find_metric(name);
    if (!m || m->kind != MT_GAUGE) return;
    uint64_t old_bits, new_bits;
    do {
        old_bits = atomic_load_explicit(&m->data.g.bits, memory_order_relaxed);
        double old_val; memcpy(&old_val, &old_bits, 8);
        double new_val = old_val + 1.0;
        memcpy(&new_bits, &new_val, 8);
    } while (!atomic_compare_exchange_weak_explicit(
        &m->data.g.bits, &old_bits, new_bits,
        memory_order_relaxed, memory_order_relaxed));
}

void tsdb_metric_gauge_dec(const char *name) {
    metric_t *m = find_metric(name);
    if (!m || m->kind != MT_GAUGE) return;
    uint64_t old_bits, new_bits;
    do {
        old_bits = atomic_load_explicit(&m->data.g.bits, memory_order_relaxed);
        double old_val; memcpy(&old_val, &old_bits, 8);
        double new_val = old_val - 1.0;
        memcpy(&new_bits, &new_val, 8);
    } while (!atomic_compare_exchange_weak_explicit(
        &m->data.g.bits, &old_bits, new_bits,
        memory_order_relaxed, memory_order_relaxed));
}

/* ---- Histogram ----------------------------------------------------------- */

void tsdb_metric_observe(const char *name, double val) {
    metric_t *m = find_metric(name);
    if (!m || m->kind != MT_HISTOGRAM) return;
    histogram_t *h = &m->data.h;

    pthread_mutex_lock(&h->lock);

    /* Increment all buckets whose upper bound >= val (cumulative). */
    for (int b = 0; b < HIST_NBUCKETS; b++) {
        if (val <= HIST_BOUNDS[b]) {
            atomic_fetch_add_explicit(&h->buckets[b], 1, memory_order_relaxed);
        }
    }
    atomic_fetch_add_explicit(&h->count, 1, memory_order_relaxed);

    /* Accumulate sum. */
    uint64_t old_sum_bits = atomic_load_explicit(&h->sum_bits, memory_order_relaxed);
    double old_sum; memcpy(&old_sum, &old_sum_bits, 8);
    double new_sum = old_sum + val;
    uint64_t new_sum_bits; memcpy(&new_sum_bits, &new_sum, 8);
    atomic_store_explicit(&h->sum_bits, new_sum_bits, memory_order_relaxed);

    pthread_mutex_unlock(&h->lock);
}

/* ---- Render -------------------------------------------------------------- */

/*
 * Simple growable buffer for render output.
 */
typedef struct {
    char  *buf;
    size_t cap;
    size_t len;
} rbuf_t;

static int rbuf_init(rbuf_t *b, size_t initial) {
    b->buf = (char *)malloc(initial);
    if (!b->buf) return -1;
    b->cap = initial;
    b->len = 0;
    b->buf[0] = '\0';
    return 0;
}

static int rbuf_ensure(rbuf_t *b, size_t need) {
    if (b->len + need + 1 > b->cap) {
        size_t new_cap = b->cap * 2 + need + 256;
        char *p = (char *)realloc(b->buf, new_cap);
        if (!p) return -1;
        b->buf = p;
        b->cap = new_cap;
    }
    return 0;
}

static int rbuf_appendf(rbuf_t *b, const char *fmt, ...) {
    /* Estimate needed size first with vsnprintf into a small probe. */
    char probe[512];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(probe, sizeof(probe), fmt, ap);
    va_end(ap);
    if (n < 0) return -1;

    if (rbuf_ensure(b, (size_t)n + 1) != 0) return -1;

    if ((size_t)n < sizeof(probe)) {
        memcpy(b->buf + b->len, probe, (size_t)n);
        b->len += (size_t)n;
        b->buf[b->len] = '\0';
    } else {
        /* Rare: probe was truncated; write directly. */
        va_list ap2;
        va_start(ap2, fmt);
        int n2 = vsnprintf(b->buf + b->len, b->cap - b->len, fmt, ap2);
        va_end(ap2);
        if (n2 > 0) b->len += (size_t)n2;
        b->buf[b->len] = '\0';
    }
    return 0;
}

/* Prometheus label string for a bucket bound. */
static void bound_label(double bound, char *out, size_t cap) {
    if (bound >= 1e200) {
        snprintf(out, cap, "+Inf");
    } else if (bound >= 1.0 && fmod(bound, 1.0) == 0.0) {
        snprintf(out, cap, "%.0f", bound);
    } else {
        snprintf(out, cap, "%g", bound);
    }
}

char *tsdb_metrics_render(size_t *out_len) {
    rbuf_t b;
    if (rbuf_init(&b, 8192) != 0) return NULL;

    for (int i = 0; i < N_METRICS; i++) {
        metric_t *m = &g_metrics[i];

        /* HELP line */
        rbuf_appendf(&b, "# HELP %s %s\n", m->name, m->help);

        switch (m->kind) {
        case MT_COUNTER: {
            rbuf_appendf(&b, "# TYPE %s counter\n", m->name);
            uint64_t v = atomic_load_explicit(&m->data.c.value, memory_order_relaxed);
            rbuf_appendf(&b, "%s %" PRIu64 "\n", m->name, v);
            break;
        }
        case MT_GAUGE: {
            rbuf_appendf(&b, "# TYPE %s gauge\n", m->name);
            double v = gauge_load(&m->data.g);
            rbuf_appendf(&b, "%s %g\n", m->name, v);
            break;
        }
        case MT_HISTOGRAM: {
            rbuf_appendf(&b, "# TYPE %s histogram\n", m->name);
            histogram_t *h = &m->data.h;

            /* Take a consistent snapshot under lock. */
            uint64_t snap_buckets[HIST_NBUCKETS];
            uint64_t snap_count;
            double   snap_sum;

            pthread_mutex_lock(&h->lock);
            for (int b2 = 0; b2 < HIST_NBUCKETS; b2++)
                snap_buckets[b2] = atomic_load_explicit(&h->buckets[b2], memory_order_relaxed);
            snap_count = atomic_load_explicit(&h->count, memory_order_relaxed);
            {
                uint64_t sb = atomic_load_explicit(&h->sum_bits, memory_order_relaxed);
                memcpy(&snap_sum, &sb, 8);
            }
            pthread_mutex_unlock(&h->lock);

            /* Emit bucket lines (all finite bounds). */
            for (int b2 = 0; b2 < HIST_NBUCKETS - 1; b2++) {
                char lbl[32];
                bound_label(HIST_BOUNDS[b2], lbl, sizeof(lbl));
                rbuf_appendf(&b, "%s_bucket{le=\"%s\"} %" PRIu64 "\n",
                             m->name, lbl, snap_buckets[b2]);
            }
            /* +Inf bucket = total count */
            rbuf_appendf(&b, "%s_bucket{le=\"+Inf\"} %" PRIu64 "\n",
                         m->name, snap_count);
            rbuf_appendf(&b, "%s_sum %g\n", m->name, snap_sum);
            rbuf_appendf(&b, "%s_count %" PRIu64 "\n", m->name, snap_count);
            break;
        }
        }
    }

    if (out_len) *out_len = b.len;
    return b.buf;
}
