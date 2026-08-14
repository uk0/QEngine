# tsdb v0.2.0 — release notes

**Version authority**: `include/tsdb.h` is the single source of truth
for the release identity — `TSDB_VERSION_MAJOR` / `_MINOR` / `_PATCH`.
This file follows it; when they disagree, the header is right and this
file is stale.  `tests/test_tls_defaults_and_versions.c` compares the
two and fails if they drift apart again.

**Status**: public beta / preview.  Single-cluster deployments are
considered production-ready for internal systems.  Operator-facing
APIs and SQL grammar are frozen for 0.2.x; bug-fix releases within
the series will not break them.

---

## Highlights

- **Storage engine**
  - Column-oriented on-disk format, partition-per-day/hour, WAL +
    group-commit, nanosecond timestamps.
  - DoD / Gorilla / FOR / Dict compression selected per column type.
    Real-world compression ratios of 20–90× on metrics & trades data.
  - Bloom filters + per-block min/max/sum stats (V3 index) enable
    fast partition pruning on WHERE + aggregate queries.

- **Cluster**
  - Gossip membership + full-replica row replication with truthful ACK.
  - Raft (PreVote §9.6, stepdown on self-removal, InstallSnapshot) for
    catalog consensus on the master set.
  - Merkle anti-entropy closes gossip gaps after a node returns.

- **Security & compliance**
  - RBAC enforcement on every `/sql` call + wire-protocol path.
  - Append-only audit log at `catalog/audit.log`, exposed via
    `GET /audit?n=N` and rendered in the dashboard.
  - TLS / mTLS between cluster nodes (OpenSSL or mbedTLS backend).
  - PITR partition trim via `POST /pitr?ts=<ns>`.

- **Operability**
  - Prometheus-text `/metrics` covering ingest, replication, DR, Raft,
    retention, backup, anti-entropy.
  - Interactive Vite + React dashboard (catalog tree, SQL console
    with 40+ QTL examples, audit viewer, cluster/Raft panels).
  - `/backup` chunked tarball stream; retention GC driven by
    `retention.conf`.

- **Clients**
  - Go SDK (existing) + **new JDBC 4.2 driver** (`sdk/jdbc/`).
  - Influx line-protocol ingest on 28092.
  - `tsdb-cli` native REPL with per-session USE context.

- **Cross-DC**
  - Async one-way replication via `TSDB_RPC_FED_INGEST=18` — `WRITE_BATCH`
    payloads are copied into a bounded ring buffer and drained by a
    background thread to the remote DC; enabled by `TSDB_DR_REMOTE`.
  - Best-effort delivery: backlog drops are surfaced as
    `qengine_dr_dropped_total`.

---

## Verified release gates

| Gate | Result |
|---|:---:|
| `make test` — 25 unit-test suites | ✅ 0 fail |
| `tests/e2e/dashboard_api.sh` | ✅ 42 / 42 |
| `tests/e2e/dr_replication.sh` | ✅ 6 / 6 |
| `tests/e2e/coldpath_smoke.sh` | ✅ 34 / 34 |
| JDBC integration (`sdk/jdbc`) | ✅ 7 / 7 |
| 4-node lvm1 cluster long-soak | stable, no row loss |

Full regression log is reproducible from `make test && bash
tests/e2e/dashboard_api.sh && bash tests/e2e/dr_replication.sh &&
bash tests/e2e/coldpath_smoke.sh` against a live cluster.

---

## Known limitations (will not block 0.2.x GA, scoped for 0.3.x)

1. **Cross-DC replication is in-memory only.**
   A process exit drops the unsent backlog.  RPO is 0 for happy-path,
   but unbounded on DC crash.  Follow-up: on-disk WAL tail + resumable
   send.  `docs/design/multi-dc.md` has the full roadmap.

2. **SDK coverage.**
   Only Go + JDBC today.  Python / Rust / .NET are future phases.
   JDBC works against REST (/sql), so DBeaver / JetBrains DB tools
   connect immediately.

3. **At-rest encryption.**
   Traffic is TLS; on-disk blocks are plaintext.  Tiered storage
   (S3 / cold layer) is not yet integrated.

4. **Single-DC sizing envelope verified.**
   Largest tested: 100 M rows / 100 k ptables on a 4-node cluster.
   No public number yet for 1 M-device / multi-TB scenarios.

5. **Federation is read-only.**
   `tsdb_federation_*` APIs aggregate reads across clusters; write
   federation is the DR forwarder (one-way, async).

---

## Upgrade / deploy

- One-shot: `bash deployment/deploy.sh` runs the full cross-build →
  rsync → docker cp → docker commit → rolling-restart → verify
  pipeline.  Previous image is preserved as
  `qengine/tsdb:pre-<git-sha>` for rollback.
- Manual rollback: `docker tag qengine/tsdb:pre-beta qengine/tsdb:dev`
  then `docker compose up -d --force-recreate`.

---

## Acknowledgements

Thanks to everyone who stress-tested this cluster, wrote SQL that
broke the parser, and filed bugs that made the audit log grow.
