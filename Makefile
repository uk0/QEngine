# tsdb Makefile
# Single-machine column-oriented time-series database in C11.

CC        := clang
STD       ?= -std=c11
OPT       ?= -O3
WARN      := -Wall -Wextra -Wpedantic -Wshadow -Wno-unused-parameter
INC       := -Iinclude
# -ldl only needed on Linux; macOS libc provides dlopen unconditionally.
ifeq ($(shell uname -s), Linux)
  LIBS    := -lpthread -lm -ldl
else
  LIBS    := -lpthread -lm
endif

# liburing — Phase 1 of the async I/O upgrade.  Detected via pkg-config on
# Linux only; absence is non-fatal (io_async.c falls back to sync syscalls).
ifeq ($(shell uname -s), Linux)
  ifneq ($(shell pkg-config --exists liburing 2>/dev/null && echo y),)
    LIBS    += $(shell pkg-config --libs liburing)
    INC     += $(shell pkg-config --cflags liburing) -DTSDB_USE_IOURING
  endif
endif

# Per-architecture SIMD flags.
# -march=native covers AVX2/NEON at the global level.
# AVX-512 is guarded by __attribute__((target("avx512f,..."))) per-function
# so the binary remains loadable on non-AVX512 CPUs; the global flag is
# harmless on CPUs that do support it (native auto-detects).
UNAME_M   := $(shell uname -m)
ifeq ($(UNAME_M), x86_64)
  ARCH    := -march=native -mavx -mavx2 -mbmi2
else ifeq ($(UNAME_M), aarch64)
  ARCH    := -march=armv8-a+simd
else
  ARCH    := -march=native
endif

# ── TLS backend detection ─────────────────────────────────────────────────
# Preference: OpenSSL (stable, well-tested) → mbedtls → no TLS.
# Note: mbedtls 4.0 renamed libmbedcrypto to libtfpsacrypto; pkg-config
#       for 'mbedtls' alone pulls in the crypto and x509 transitively.
OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl 2>/dev/null)
OPENSSL_LIBS   := $(shell pkg-config --libs   openssl 2>/dev/null)
ifneq ($(OPENSSL_LIBS),)
  TLS_DEF    := -DTSDB_TLS_OPENSSL
  TLS_FLAGS  := $(OPENSSL_CFLAGS)
  TLS_LIBS   := $(OPENSSL_LIBS)
else
  MBEDTLS_CFLAGS := $(shell pkg-config --cflags mbedtls 2>/dev/null)
  MBEDTLS_LIBS   := $(shell pkg-config --libs   mbedtls 2>/dev/null)
  ifneq ($(MBEDTLS_LIBS),)
    TLS_DEF    := -DTSDB_TLS_MBEDTLS
    TLS_FLAGS  := $(MBEDTLS_CFLAGS)
    TLS_LIBS   := $(MBEDTLS_LIBS)
  else
    TLS_DEF    :=
    TLS_FLAGS  :=
    TLS_LIBS   :=
  endif
endif

COMMON    := $(STD) $(OPT) $(WARN) $(ARCH) $(INC) $(TLS_DEF) $(TLS_FLAGS)
CFLAGS    ?= $(COMMON)
LDFLAGS   ?= $(LIBS) $(TLS_LIBS)

SRC_DIRS  := src/core src/compress src/storage src/exec src/query src/cluster src/federation src/server src/catalog src/raft
SRCS      := $(filter-out src/cluster/tsdb_node_main.c,$(foreach d,$(SRC_DIRS),$(wildcard $(d)/*.c)))
OBJS      := $(SRCS:.c=.o)

TEST_SRCS := tests/test_compress.c tests/test_storage.c tests/test_exec.c tests/test_query.c tests/test_lzlite.c tests/test_pfor.c tests/test_simd_dispatch.c tests/test_adaptive.c tests/test_parallel.c tests/test_server.c tests/test_catalog.c tests/test_rawblock.c tests/test_pubsub.c tests/test_autobalance.c tests/test_tdigest.c tests/test_compaction.c tests/test_group_commit.c tests/test_asof_join.c tests/test_retention.c tests/test_v06_e2e.c tests/test_sample_by_stream.c tests/test_bloom_filter.c tests/test_tls.c tests/test_group_by.c tests/test_group_by_golden.c tests/test_v07_e2e.c tests/test_window_fns.c tests/test_ts_aggregates.c tests/test_advanced_windows.c tests/test_stable.c tests/test_v08_e2e.c tests/test_promql.c tests/test_influx_line.c tests/test_metrics.c tests/test_stable_sql.c tests/test_parallel_groupby.c tests/test_v09_e2e.c tests/test_alter_table.c tests/test_tmq.c tests/test_udf.c tests/test_parquet.c tests/test_rbac.c tests/test_auth_enforce.c tests/test_auth_wire.c tests/test_crc32c.c tests/test_iopolicy.c tests/test_hwcompress.c tests/test_bp128.c tests/test_block_points.c tests/test_skiplist_memtable.c tests/test_concurrent_delete.c tests/test_concurrent_writers.c tests/test_continuous_rw.c tests/test_block_stats.c tests/test_layout_invariants.c tests/test_raft_log.c tests/test_raft_rpc.c tests/test_raft_cluster.c tests/test_mpmc_ring.c tests/test_pitr.c tests/test_multi_data_dirs.c tests/test_cluster_route.c tests/test_drop_race.c tests/test_float32.c tests/test_codec_earlyexit.c tests/test_hier_mirror.c tests/test_catalog_reconcile.c tests/test_catalog_tombstone.c tests/test_catalog_id.c tests/test_catalog_v2.c tests/test_catalog_mirror.c tests/test_drop_trash.c tests/test_memtable_budget.c tests/test_compactor_memo.c tests/test_io_async.c tests/test_replication_compress.c tests/test_skiplist_monotonic.c tests/test_replication_compress_edge.c tests/test_io_async_robust.c tests/test_reactor.c tests/test_reactor_write.c tests/test_wal_interval.c tests/test_flush_append_race.c tests/test_rpc_tls.c tests/test_enospc.c tests/test_wal_recovery.c tests/test_torn_partition_read.c tests/test_fedrpc_byname.c tests/test_antientropy_safety.c tests/test_stable_open_fail.c tests/test_idx_version_mongrel.c tests/test_wire_count_disk.c tests/test_metrics_http.c tests/test_torn_value_column.c tests/test_zerocopy_read.c tests/test_net_hardening.c tests/test_standalone_parity.c tests/test_tls_connect_timeout.c tests/test_replica_backoff.c tests/test_sum_overflow.c tests/test_sql_insert.c tests/test_sql_having.c tests/test_sql_distinct.c tests/test_stable_scatter.c tests/test_partition_backfill.c tests/test_compact_swap_recover.c tests/test_partial_flush_recovery.c tests/test_multipart_recovery.c tests/test_symtab_recovery.c tests/test_compaction_v4_idx.c tests/test_wal_torn_tail.c tests/test_ae_local_stats.c tests/test_migrate.c tests/test_migrate_symbol.c tests/test_compaction_misaligned_blocks.c tests/test_migrate_resolve.c tests/test_bits_refill.c tests/test_wal_replay_abort.c tests/test_result_bulk.c
TEST_SRCS += tests/test_rawblock_multicol_atomic.c
TEST_SRCS += tests/test_zerofill_absent_column.c
TEST_SRCS += tests/test_part_blocks_ref.c
# SHOW DATABASES/GROUPS/STABLES/TABLES: grammar, the on_nodes scope column,
# and the federation symbol dictionary its peer legs depend on.  Single
# process, no ports beyond one loopback RPC server on port 0.
TEST_SRCS += tests/test_show_introspection.c
# Wire WRITE_BATCH length-field bounds: a symbol column whose declared total is
# a lie must be refused, not walked.  scripts/test-both-modes.sh globs tests/
# and would have run it either way; `make test` goes through TEST_SRCS, so the
# regression guard has to be listed here to be part of the default gate.
TEST_SRCS += tests/test_write_batch_bounds.c
# ALTER TABLE racing the background compactor was a UAF read of schema->cols plus
# an OOB write to the per-column arrays; and an unknown idx version mapped to the
# V4 header size with no range check.  Both are in the default gate now.
TEST_SRCS += tests/test_compact_alter_race.c
TEST_SRCS += tests/test_compact_idx_version_guard.c
# DROP TABLE of a reserved plumbing name (wal/catalog/raft) destroyed the node and
# returned OK; flush discarded a failed SYMBOL-dict save and published coded blocks
# whose dictionary was never persisted.
TEST_SRCS += tests/test_drop_reserved.c
TEST_SRCS += tests/test_flush_symtab_rc.c
# /sql ran with no deadline in both mains; no result-size ceiling; ingest drops
# uncounted -- request/connection governance for the unauth-by-default HTTP plane.
TEST_SRCS += tests/test_server_resource_gov.c
# Inter-node RPC dispatched every opcode (DDL_FORWARD/CATALOG_DUMP/WRITE_BATCH) with
# no authentication; a shared-secret HMAC handshake now gates dispatch.
TEST_SRCS += tests/test_rpc_auth.c
TEST_BINS := $(patsubst tests/%.c,build/test/%,$(TEST_SRCS))

# Cluster integration test: built by default but run separately.
# test_cluster_show: SHOW's multi-replica union, the on_nodes divergence
# count, and the refuse-rather-than-under-report gate — needs two live nodes.
CLUSTER_TEST_SRCS := tests/test_cluster.c tests/test_cluster_count_repro.c tests/test_ae_phantom_peer.c tests/test_cluster_show.c
CLUSTER_TEST_BINS := $(patsubst tests/%.c,build/test/%,$(CLUSTER_TEST_SRCS))

# Anti-entropy peer probe (TSDB_RPC_LOCAL_TABLE_STATS) unit test: single
# process — an in-tree RPC server plus a stub modelling a peer too old for the
# opcode.  Appended here rather than folded into the TEST_SRCS line above so
# the diff stays readable.
TEST_SRCS += tests/test_ae_peer_stats.c

# Anti-entropy row-range digest (TSDB_RPC_LOCAL_TABLE_DIGEST): the middle-hole
# net for two replicas that tie on (count,max_ts) but hold different interior
# rows.  In-process digest/diff/merge plus a loopback RPC server for the wire
# exchange and the old-peer degrade.  No forks, no sleeps, no cluster.
TEST_SRCS += tests/test_ae_rowdigest.c

# The two pure guards that confine the anti-entropy row-digest to a table's
# replica set and hard-bound its per-sweep pull, so it cannot cross-shard-merge
# and run away (the 2026-08-02 257M-row incident) when re-enabled.
TEST_SRCS += tests/test_ae_shard_safety.c

# The tombstone predicate gating orphan-storage quarantine: absence must NEVER
# authorise a reap (a table created while this node was down is absent too);
# only a DROP tombstone in this node's own catalog log may.
TEST_SRCS += tests/test_orphan_reap.c

# A bulk batch must not be SPLIT across a flush: the prefix would go durable
# and no rollback could undo it (dedup prerequisite 5).
TEST_SRCS += tests/test_batch_atomic_flush.c

# The exact receiver-side dedup ledger (WRITE_BATCH dedup prerequisite 4): a
# CONTIGUOUS frontier plus an exact out-of-order tail, so an out-of-order seq is
# never reported applied. A scalar watermark would drop its rows silently.
TEST_SRCS += tests/test_dedup_ledger.c

# Anti-entropy pull-candidate ranking + bounded retry (single process, no
# sockets): the peer probe can no longer be trusted to identify ONE usable
# source, so the resync keeps every peer's answer and walks them best-first.
TEST_SRCS += tests/test_ae_candidates.c

# Scaled-decimal FLOAT64 codec (TSDB_CODEC_DEC, id 12): adversarial scale
# selection, the -0.0 trap, a flag-off/flag-on differential, unknown-codec-id
# rejection, and a partition-level round trip.
TEST_SRCS += tests/test_codec_decimal.c

# Graceful-degrade read path for a column that is SHORT for a partition (fewer
# durable blocks than ts).  Builds the on-disk state by hand, then pins BOTH
# halves of the contract: everything that does not need the missing cells still
# answers exactly, and everything that does fails naming the column and the ts
# range — from every read site, including the stats fast path.
TEST_SRCS += tests/test_short_column_read.c

# Append-only .idx publish: kills a process at each step of the in-place
# publish (TSDB_TEST_CRASH_IDX_APPEND) and reads the result back from a fresh
# one — the sub-entry torn tail is what a mongrel-repairing reader mistakes for
# a 40-vs-48 header and relocates the whole entry array over.
TEST_SRCS += tests/test_idx_append_crash.c

# The anti-entropy COLD gate's one input: a flush into an EXISTING partition
# has to move that partition dir's mtime.  temp+rename got it for free from the
# dirent churn; an in-place idx publish has to do it explicitly, and when it
# stopped, the only guard keeping a partition under live local write out of
# tsdb_cluster_backfill_partition_from_result() ("local-unique rows ... are
# replaced by the peer copy") went ELIGIBLE after 60 s of writing.
TEST_SRCS += tests/test_ae_cold_gate_mtime.c

# Backup set -> restore round trip, by VALUE, plus the two properties a
# count(*) check cannot see: no block is duplicated on a replay, and a restore
# killed mid-flight leaves the .tsdb_restore marker behind.  test_restore_crash
# forks and _exit()s inside the restore, so it is a real process death.
TEST_SRCS += tests/test_restore.c tests/test_restore_crash.c

# The two rc=0-and-the-data-is-gone paths the restore reviewer reproduced: a
# striped (TSDB_DATA_DIRS) database backing up / restoring through data dir 0
# only, and the WAL-checkpoint stamp re-heading a legacy 40-byte-entry idx as
# an 88-byte-entry one.  Both must be refused loudly, never answered wrongly.
TEST_SRCS += tests/test_restore_multidir.c

# The two rc=0-and-the-data-is-wrong paths found in the round-2 restore work.
# test_restore_adv: a table nobody in this process opened still holds its acked
# rows in the redo log under TSDB_WAL_ONLY_COMMIT, and tsdb_backup_create used
# to flush only the OPENED tables — so it wrote complete=1 over rows=0.  It
# forks and _exit()s without closing, so the crash is a real process death.
# test_restore_adv2: a re-run of a finished restore after the DEFAULT compactor
# rewrote the partition's block boundaries used to land every block a second
# time; it drives tsdb_compactor_run_once with stock options, no test tuning.
TEST_SRCS += tests/test_restore_adv.c tests/test_restore_adv2.c

# tsdb_restore_verify against a database that is NOT a frozen copy of the set.
# A source that kept ingesting used to be certified TSDB_ERR_CORRUPT (an intact
# set, rows_backup=2000 vs rows_target=2700); the force-drain that made that
# fire also published TARGET-side block boundaries — arming the re-run gate's
# refusal on a later restore — and fired the cluster replication hook, so the
# check shipped rows.  The third case pins the drain that must still happen:
# a node whose acked rows are all in its redo log digests 0 against a manifest
# of 2000, and the drain that answers it MUST replicate (a memtable flushes
# exactly once).
TEST_SRCS += tests/test_restore_verify_live.c

# The fcacea4 zero-fill protection, asserted against a RESTORED data dir.  Every
# partition tsdb_restore_run creates used to come out with ncols == 0 in its idx
# header, so rule 2 of part_col_absence_is_late_add was gone and a trailing
# column whose blocks the source had already lost read back as fabricated zeros
# with rc=0 — where the source it was copied from answered TSDB_ERR_CORRUPT.
# The two availability guards ride along: an ALTER-added column must still
# zero-fill on the restored copy, and a claim the stream carries no blocks for
# must not be stamped.
TEST_SRCS += tests/test_restore_ncols.c

# tsdb_restore_verify against a target the ENGINE refuses to read.
# tsdb_migrate_digest counts `rows` over the ts column ONLY and `blocks` over
# EVERY column, so a target that loses one value column's blocks keeps its rows
# and drops blocks — bit for bit the signature the classification hands to
# REENCODED ("nothing is missing").  The index level passed it and the deep
# level DECLINED on that classification, so a database whose SELECT ts, val
# answers TSDB_ERR_CORRUPT verified clean at BOTH levels.  The control cases
# ride along: a purely re-encoded target must still verify clean (the false
# alarm REENCODED exists to prevent), one compacted partition must not switch
# off the per-block check for the partitions beside it, a deep level that
# measured nothing must not answer clean, and a .seq sidecar claiming more
# columns than the schema has must not be stamped.
TEST_SRCS += tests/test_restore_verify_hole.c

# The two ways tsdb_restore_verify still certified a database that had lost a
# block.  (ts_min, count) is not a unique block identity — equal timestamps are
# kept in insertion order and a flush splits on block_points — so the DEEP
# level's UNCONSUMED first-match let one surviving block satisfy two stream
# records: checked=4 missing=0 unresolved=0, a fully-resolved clean answer for
# a column reading back half fabricated zeros.  And the hole axis was only
# consulted on a block DEFICIT, which a target that kept ingesting can never
# show, so the index level certified the same whole-column loss [H1] catches on
# a quiesced node.  Two controls ride along: a faithful copy of an already-torn
# SOURCE that out-grew its set is containment and must not be called corrupt,
# and a byte rot on a partition the compactor never touched must not be
# swallowed by a table-wide re-encode hypothesis.
TEST_SRCS += tests/test_restore_verify_dup.c
TEST_SRCS += tests/test_restore_verify_rot.c
TEST_SRCS += tests/test_restore_verify_mult.c

# Duplicate-timestamp block identity.  (ts_min, ts_max, count) is NOT a unique
# key: equal timestamps are kept in insertion order and a flush cuts a
# partition into independent block_points chunks, so two full chunks of one
# repeated timestamp are genuinely distinct blocks with an identical key.
# Every path that paired or deduplicated by that content key mispaired or
# silently dropped data; these four cover the scan/stats/bloom read paths, the
# replication applier, the migrate importer and the compactor.
TEST_SRCS += tests/test_dup_ts_block_pair.c
TEST_SRCS += tests/test_dup_ts_readpaths.c
TEST_SRCS += tests/test_dup_ts_rawblock.c
TEST_SRCS += tests/test_dup_ts_compaction.c

# Where the ordinal comes from and who may believe it — the failures that do
# NOT involve duplicate timestamps: compaction renumbering the space downward
# (which then stalls replication permanently), a replica trusting a remote
# ordinal as proof of re-delivery, a legacy entry being handed its physical
# position as its ordinal (which breaks the repair push it exists for), a legacy
# ts prefix disabling ordinal pairing forever — making an ALTER performed by
# THIS binary unreadable — plus [O8] an ordinal carried without its ISSUER (two
# shards of one synchronised ingest write the same timestamp grid, both number
# from 0, and their ts blocks are byte-identical, so the second sender's group
# was ACKed and destroyed) and [O9] a repeated content key on the COLUMN side
# being treated as ambiguity, which made the whole column unreadable forever.
TEST_SRCS += tests/test_block_ordinal_space.c

# The anti-entropy backfill is the one writer that renumbers a partition's
# ordinal space DOWNWARD: it rebuilds from a peer's row set in an empty scratch
# dir, so the new blocks are stamped 0..k-1, and only the .col/.idx pairs are
# swapped.  A surviving <part>/.ordmap then maps every sender's group onto the
# OLD numbering, where local ordinal N now names different rows, and the next
# delivery of that group overwrites a just-restored block with the sender's
# values at rc=0.
TEST_SRCS += tests/test_backfill_ordmap.c

# Compaction on ONE side of a replication pair — the direction neither [O1] nor
# [O2] covers, and the one that decided round 3.  Every node runs a compactor
# unconditionally and nothing marks a partition as replicated, so a REPLICA
# compacts a replicated partition while the primary keeps flushing into it.  Both
# cross-node numbering rules failed here in opposite directions; ingest
# translation is what makes the two ordinal spaces independent.  Production
# threshold (min_blocks_to_compact = 0 -> 16), no test tuning.
TEST_SRCS += tests/test_replica_compact.c

# tsdb_part_ts_retract_unpaired against a LEGACY unmarked index whose column is
# SHORT — which is what an ALTER-added column always looks like.  On an unmarked
# index the effective ordinal IS the position, so an ordinal-only pairing test
# collapses to "same position" and the repair truncated a perfectly whole
# partition: measured count(*) 5120 -> 2048 with repeating timestamps and
# 5120 -> 0 with unique ones, permanently, because no push is coming for a
# locally added column.
TEST_SRCS += tests/test_retract_short_column.c

# The documented repair path on a partition an OLDER binary wrote — i.e. every
# partition in an existing fleet.  One value column's files are lost and an
# upgraded sender re-syncs THAT COLUMN block by block, which is the applier's
# only repair granularity and literally what the engine's own error message
# tells the operator to do.  With no <part>/.ordmap the translation allocates
# FRESH local ordinals, whose legacy floor is ts.idx's ENTRY COUNT, so the
# repaired column is numbered disjointly from ts's invented positions: every
# push returns TSDB_OK, anti-entropy sees a healthy partition, and the column
# reads TSDB_ERR_CORRUPT forever.  Written to compile and behave natively on
# 9dab5a2 too (everything new is behind TSDB_IDX_ORD_MARK), so the two trees
# can be compared directly.
TEST_SRCS += tests/test_adv_repair_portable.c

# ts_max is only evidence on an entry that carries the durable ordinal.  A
# LEGACY value entry can be a tick wider than its ts partner — the range borrow
# in the compaction this same change fixes — and demanding ts_max on bytes
# written before the marker turns a column pristine answers CORRECTLY into a
# permanent TSDB_ERR_CORRUPT that nothing can re-stamp.  [B1] the borrow where
# the writer left it, [B2] the same after a reorder so the positional fast path
# cannot fire and the content rule has to place it, [B3] the guard that relaxing
# it does NOT resurrect first-match on an all-equal-timestamp run.  Portable to
# 9dab5a2, where B1/B2 pass and B3 is the original bug.
TEST_SRCS += tests/test_legacy_range_borrow.c

# The repair, run against a partition whose placement is NOT recoverable: legacy
# on both sides, repeating ts keys, short column.  Unavailable is the right
# answer and there is no healing path — the information does not exist.  What
# the re-sync must not do is make it worse: its pushes APPEND marked entries
# beside the unmarked survivors, and a purely positional pairing then served a
# re-delivered copy of block 0 as ts block 2's answer — rc=0 rows=3072
# sum=2622976 against an intact 4720128, a refusal turned into a silent wrong
# answer by the repair the error message asks for.
TEST_SRCS += tests/test_repair_into_ambiguous.c
TEST_SRCS += tests/test_ordmap_torn.c

# The catalog anti-entropy tick (every 30 s) used to open a fresh catalog, swap
# db->catalog and free the old object — while every read path caches the
# pointer (`cat = tsdb_db_catalog(db)`) with no refcount and no lock.  ASan on
# the unfixed tree: heap-use-after-free in sc_hmap_get, freed by
# tsdb_db_reload_catalog.  Pins both halves: the live object keeps its identity
# across a reload, and the reloaded state is visible through that same pointer.
TEST_SRCS += tests/test_catalog_reload_uaf.c

# A reply must answer the request whose id it carries.  rpc.h has always
# described req_id as being for "request/response matching" and every responder
# echoes it; nothing compared it.  The senders abandon replies by design (10 s
# deadline on the replication fanout, 1 s on raft's vote) and the request is
# already on the wire when they do, so the next call on that socket — with
# TSDB_REPLICA_CONNS_PER_PEER=1, the live cluster's setting, there is exactly
# one per peer — read the previous batch's ACK as its own answer and counted a
# batch the peer had rejected as replicated.
TEST_SRCS += tests/test_rpc_reqid_pairing.c

# An ACK must mean the peer did the thing.  Three responders said yes to work
# that had not happened: the dispatch `default:` ACKed any opcode this build
# has never heard of (the whole forward-compatibility story on a transport that
# parses `ver` and (void)s it), SCHEMA_SYNC ACKed unconditionally with the
# decode rc / ncols range check / create return all discarded, and DDL_FORWARD
# put the failure TEXT in an ACK body for the caller to string-match.
TEST_SRCS += tests/test_rpc_ack_honesty.c

# Anti-entropy has to RUN.  tsdb_resync_startup_thread fired once, 5 s after
# boot, and returned — while "anti-entropy will heal it" is the stated
# fallback for the best-effort replication path (TSDB_REPLICATION_QUORUM=0),
# for a failed SCHEMA_SYNC, and for every evicted fanout conn.  Pins that the
# sweep repeats on a period, that the period is opt-outable, and that one
# sweep is bounded rather than unbounded.
TEST_SRCS += tests/test_ae_periodic.c

# A slow peer must not become unbounded sender RSS.  Under QUORUM=0 the
# submitter returns at once and every fanout worker it left behind still owns
# its encoded payload; the pool queue that holds them had no depth or byte cap
# and tsdb_pool_submit cannot fail on depth.  Drops are counted and loud.
TEST_SRCS += tests/test_fanout_backpressure.c

# A backup must not report success while holding less than the database.
# tsdb_db_flush_all propagates its first error; backup.c's manifest emitter
# discarded it with a (void) cast and the /backup route streamed a tar of a
# data dir whose rows were still in RAM.
TEST_SRCS += tests/test_backup_flush_rc.c

# Periodic anti-entropy turned a one-shot truncate race into a recurring one: a
# WRITE_BATCH committing between the empty measurement that gates a FULL_PULL and
# the truncate is wiped and not restored by the pull.  Pins that the guard
# aborts the truncate when a row lands in the window (deterministic injection).
TEST_SRCS += tests/test_ae_truncate_race.c

# The middle-gap stderr line was logged once per divergent peer per table per
# sweep, forever (~8,600 lines/day/table).  Pins that the human line is
# throttled to the transition + once per N sweeps while the counter still fires
# every occurrence.
TEST_SRCS += tests/test_ae_midgap_throttle.c

# Anti-entropy could not see an over-counted replica (best_count <= local_count
# -> UP_TO_DATE).  A duplicate must not be truncated (an async-ahead replica is
# indistinguishable by count alone); pins the persistence tracker that makes a
# real over-count visible while a transient ahead-state resolves.
TEST_SRCS += tests/test_ae_overcount.c

# *resp_len reported the wire payload_len, not the bytes copied, so a caller
# with a smaller buffer over-read the uninitialised tail.  Pins that *resp_len
# is the copied length and that _ex raises a distinct truncation signal.
TEST_SRCS += tests/test_rpc_resp_len_copied.c
TEST_SRCS += tests/test_node_id_durable.c
# A batch whose WAL append (wal_only) or commit-flush (default) fails must not
# leave its rows in the memtable: on failure they are truncated back to the
# durable boundary so a receiver's retry re-appends once instead of doubling.
TEST_SRCS += tests/test_batch_rollback.c
TEST_SRCS += tests/test_migrate_unflushed.c

# A durable per-table incarnation so DROP+recreate cannot reuse identity: a
# WRITE_BATCH left over from a since-dropped incarnation must be REJECTED, not
# spliced into the recreated table.  Stamped at CREATE, changed on recreate,
# kept across a plain reopen, carried additively on the WRITE_BATCH + SCHEMA_SYNC
# wires (older peers ignore the trailer, 0 = UNKNOWN = apply-as-before).
TEST_SRCS += tests/test_table_incarnation.c
TEST_BINS := $(patsubst tests/%.c,build/test/%,$(TEST_SRCS))

# Federation integration test.
FED_TEST_SRCS := tests/test_federation.c
FED_TEST_BINS := $(patsubst tests/%.c,build/test/%,$(FED_TEST_SRCS))

BENCH_SRCS := $(wildcard bench/*.c)
BENCH_BINS := $(patsubst bench/%.c,build/bench/%,$(BENCH_SRCS))

CLI_SRC         := cli/tsdb_cli.c
CLI_BIN         := build/tsdb

# TCP client CLI — standalone, links only against cli/tsdb_wire.c (no libtsdb)
TCP_CLI_SRCS    := cli/tsdb_client.c cli/tsdb_wire.c
TCP_CLI_BIN     := build/tsdb-cli

# Detect readline; fallback to fgets if absent
READLINE_FLAGS  := $(shell pkg-config --cflags readline 2>/dev/null)
READLINE_LIBS   := $(shell pkg-config --libs   readline 2>/dev/null)
ifneq ($(READLINE_LIBS),)
  TCP_CLI_CFLAGS  := -DHAVE_READLINE $(READLINE_FLAGS)
  TCP_CLI_LDFLAGS := $(READLINE_LIBS)
else
  TCP_CLI_CFLAGS  :=
  TCP_CLI_LDFLAGS :=
endif

# Wire object (shared by tsdb-cli and test_client, no libtsdb needed)
WIRE_OBJ        := cli/tsdb_wire.o

SERVER_CLI_SRC  := cli/tsdb_server_main.c
SERVER_CLI_BIN  := build/tsdb-server

CLUSTER_NODE_SRC := src/cluster/tsdb_node_main.c
CLUSTER_NODE_BIN := build/cluster/tsdb_node

.PHONY: all clean test test-client test-cluster test-federation bench cli tcp_cli server_cli cluster_node debug sdk-go sdk-java
.DEFAULT_GOAL := all

all: lib cli tcp_cli server_cli test $(CLUSTER_TEST_BINS) $(FED_TEST_BINS)

lib: build/libtsdb.a

build/libtsdb.a: $(OBJS)
	@mkdir -p build
	@ar rcs $@ $(OBJS)
	@echo "AR  $@"

%.o: %.c
	@$(CC) $(CFLAGS) -c -o $@ $<
	@echo "CC  $<"

debug: CFLAGS = $(STD) -O0 -g -Wall -Wextra -Wno-unused-parameter -fsanitize=address,undefined $(ARCH) $(INC) $(TLS_DEF) $(TLS_FLAGS)
debug: LDFLAGS += -fsanitize=address,undefined
debug: all

test: $(TEST_BINS)
	@for t in $(TEST_BINS); do \
	  echo "--- $$t ---"; $$t || exit 1; \
	done

test-cluster: $(CLUSTER_TEST_BINS)
	@for t in $(CLUSTER_TEST_BINS); do \
	  echo "--- $$t ---"; $$t || exit 1; \
	done

test-federation: $(FED_TEST_BINS)
	@for t in $(FED_TEST_BINS); do \
	  echo "--- $$t ---"; $$t || exit 1; \
	done

# test_tls has a special rule (links wire obj + TLS libs) — defined elsewhere
build/test/test_tls: tests/test_tls.c $(OBJS) $(WIRE_OBJ)

# UDF sample shared library — built alongside tests, not linked into libtsdb.
build/test/udf_sample.so: tests/udf_sample.c include/tsdb_udf.h
	@mkdir -p build/test
	@$(CC) -fPIC -shared -Iinclude -o $@ tests/udf_sample.c
	@echo "SO  $@"

# test_udf depends on the sample .so being built first.
build/test/test_udf: tests/test_udf.c $(OBJS) build/test/udf_sample.so

# test_auth_enforce exercises CREATE FUNCTION via auth path; needs the .so.
build/test/test_auth_enforce: tests/test_auth_enforce.c $(OBJS) build/test/udf_sample.so

# test_standalone_parity drives the REAL tsdb_server_wire_metrics_providers
# from cli/tsdb_server_main.c.  That TU has its own main(), so compile it to a
# dedicated object with main renamed out of the way, then link it in.
build/test/tsdb_server_main_nomain.o: cli/tsdb_server_main.c
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Dmain=tsdb_server_cli_main_unused -c -o $@ cli/tsdb_server_main.c
	@echo "CC  cli/tsdb_server_main.c (nomain)"

build/test/test_standalone_parity: tests/test_standalone_parity.c $(OBJS) build/test/tsdb_server_main_nomain.o
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -o $@ tests/test_standalone_parity.c \
	    build/test/tsdb_server_main_nomain.o $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

# test_replica_backoff calls static helpers in src/cluster/replica.c.  That TU
# exposes them as non-static *_test shims under -DTSDB_TEST, so recompile just
# replica.c with that flag into a side object and link it in place of the
# normal replica.o (filtered out of $(OBJS) to avoid a duplicate symbol).
build/test/replica_test.o: src/cluster/replica.c
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -DTSDB_TEST -c -o $@ src/cluster/replica.c
	@echo "CC  src/cluster/replica.c (TSDB_TEST)"

build/test/test_replica_backoff: tests/test_replica_backoff.c \
        $(filter-out src/cluster/replica.o,$(OBJS)) build/test/replica_test.o
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -o $@ tests/test_replica_backoff.c \
	    $(filter-out src/cluster/replica.o,$(OBJS)) build/test/replica_test.o $(LDFLAGS)
	@echo "LD  $@"

build/test/%: tests/%.c $(OBJS)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -o $@ $< $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

bench: $(BENCH_BINS)
	@for b in $(BENCH_BINS); do \
	  echo "--- $$b ---"; $$b; \
	done

# Five benches drive a server over the wire protocol (bench_docker_cluster,
# lat_probe, load_cluster, verify_count, enospc_writer) and need frame_send /
# frame_recv from cli/tsdb_wire.c.  Linking it into every bench is harmless —
# unused objects contribute no symbols — and keeps one rule instead of five.
# Without it `make bench` died on the first wire bench and never built the rest.
build/bench/%: bench/%.c $(OBJS) $(WIRE_OBJ)
	@mkdir -p build/bench
	@$(CC) $(CFLAGS) -Icli -o $@ $< $(OBJS) $(WIRE_OBJ) $(LDFLAGS)
	@echo "LD  $@"

cli: $(CLI_BIN)

$(CLI_BIN): $(CLI_SRC) $(OBJS)
	@mkdir -p build
	@$(CC) $(CFLAGS) -o $@ $(CLI_SRC) $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

# TCP client REPL (tsdb-cli): standalone, no libtsdb dependency
tcp_cli: $(TCP_CLI_BIN)

$(WIRE_OBJ): cli/tsdb_wire.c cli/tsdb_wire.h
	@$(CC) $(CFLAGS) -Icli -c -o $@ cli/tsdb_wire.c
	@echo "CC  cli/tsdb_wire.c"

$(TCP_CLI_BIN): $(TCP_CLI_SRCS) cli/tsdb_wire.h
	@mkdir -p build
	@$(CC) $(CFLAGS) $(TCP_CLI_CFLAGS) -Icli -o $@ $(TCP_CLI_SRCS) \
	    -lpthread -lm $(TCP_CLI_LDFLAGS) $(TLS_LIBS)
	@echo "LD  $@"

# test_client: links against tsdb_wire.o only (no full libtsdb)
build/test/test_client: tests/test_client.c $(WIRE_OBJ)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Icli -o $@ tests/test_client.c $(WIRE_OBJ) -lpthread $(TLS_LIBS)
	@echo "LD  build/test/test_client"

# test_tls_connect_timeout: exercises the wire non-blocking-connect helper
# directly; links tsdb_wire.o only (no full libtsdb), like test_client.
build/test/test_tls_connect_timeout: tests/test_tls_connect_timeout.c $(WIRE_OBJ)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Icli -o $@ tests/test_tls_connect_timeout.c $(WIRE_OBJ) -lpthread $(TLS_LIBS)
	@echo "LD  build/test/test_tls_connect_timeout"

test-client: build/test/test_client
	@echo "--- build/test/test_client ---"
	@build/test/test_client

# TLS test: links libtsdb + tsdb_wire.o (needs both server-side and client-side)
build/test/test_tls: tests/test_tls.c $(OBJS) $(WIRE_OBJ)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Icli -o $@ tests/test_tls.c $(OBJS) $(WIRE_OBJ) \
	    $(LDFLAGS) $(TLS_LIBS)
	@echo "LD  build/test/test_tls"

test-tls: build/test/test_tls
	@echo "--- build/test/test_tls ---"
	@build/test/test_tls

server_cli: $(SERVER_CLI_BIN)

$(SERVER_CLI_BIN): $(SERVER_CLI_SRC) $(OBJS)
	@mkdir -p build
	@$(CC) $(CFLAGS) -o $@ $(SERVER_CLI_SRC) $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

cluster_node: $(CLUSTER_NODE_BIN)

$(CLUSTER_NODE_BIN): $(CLUSTER_NODE_SRC) $(OBJS)
	@mkdir -p build/cluster
	@$(CC) $(CFLAGS) -o $@ $(CLUSTER_NODE_SRC) $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

clean:
	@rm -rf build $(OBJS)
	@echo "CLEAN"

# Run test_simd_dispatch under each CPU level supported on this machine.
# On ARM64: SCALAR + NEON.  On x86-64: SCALAR + AVX2 (+ AVX512 if present).
check-levels: build/test/test_simd_dispatch
	@echo "--- SCALAR ---"
	@TSDB_CPU_LEVEL=SCALAR build/test/test_simd_dispatch
	@echo "--- NEON ---"
	@TSDB_CPU_LEVEL=NEON   build/test/test_simd_dispatch 2>/dev/null || true
	@echo "--- AVX2 ---"
	@TSDB_CPU_LEVEL=AVX2   build/test/test_simd_dispatch 2>/dev/null || true
	@echo "--- AVX512 ---"
	@TSDB_CPU_LEVEL=AVX512 build/test/test_simd_dispatch 2>/dev/null || true

# ── SDK builds (not part of the default target) ────────────────────────────
# sdk-go: build + test the pure-Go client (mock-listener tests need no server).
sdk-go:
	cd sdk/go && go build ./... && go test ./...

# sdk-java: plain-javac compile of the Java client + JDBC driver (main + test
# sources; JDK-only, no Maven needed), --release 11 matching the pom.
sdk-java:
	@mkdir -p build/java
	@find sdk/java/src -name '*.java' | xargs javac --release 11 -d build/java
	@echo "JAVAC sdk/java -> build/java"

print-%:
	@echo $* = $($*)
