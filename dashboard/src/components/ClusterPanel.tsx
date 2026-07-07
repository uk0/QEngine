import { useCallback, useRef, useState } from 'react';
import { api, ClusterInfo, ClusterNode, HealthInfo, RaftInfo } from '../api';
import { usePoll } from '../hooks';

interface Props {
  cluster: ClusterInfo | null;
  raft: RaftInfo | null;
  err: string;
  lastAt: number;
}

/* Overview page: KPI tiles from /metrics + /health, then the cluster
 * cards.  /cluster + /raft arrive via the App-level shared 4s poll. */
export function ClusterPanel({ cluster, raft, err, lastAt }: Props) {
  const [metrics, setMetrics] = useState<Record<string, number> | null>(null);
  const [health, setHealth] = useState<HealthInfo | null>(null);
  const [metricsErr, setMetricsErr] = useState('');
  // Previous snapshot → per-second deltas without server-side rates.
  const prev = useRef<{ at: number; m: Record<string, number> } | null>(null);
  const [rate, setRate] = useState<Record<string, number>>({});

  const pollMetrics = useCallback(async () => {
    const [m, h] = await Promise.allSettled([api.metrics(), api.health()]);
    if (h.status === 'fulfilled') setHealth(h.value ?? null);
    if (m.status === 'fulfilled') {
      const cur = m.value ?? {};
      setMetrics(cur);
      setMetricsErr('');
      if (prev.current) {
        const dt = (Date.now() - prev.current.at) / 1000;
        if (dt > 0.2) {
          const r: Record<string, number> = {};
          for (const k of Object.keys(cur)) {
            const d = (cur[k] ?? 0) - (prev.current.m[k] ?? 0);
            if (d > 0) r[k] = d / dt;
          }
          setRate(r);
        }
      }
      prev.current = { at: Date.now(), m: cur };
    } else {
      setMetricsErr(m.reason instanceof Error ? m.reason.message : 'metrics unavailable');
    }
  }, []);
  usePoll(pollMetrics, 4000);

  const cold = cluster === null && raft === null && metrics === null;
  if (cold && !err) {
    return (
      <div className="panel-skeleton">
        <div className="skeleton-card" />
        <div className="skeleton-card" />
        <div className="skeleton-card" />
      </div>
    );
  }

  const age = lastAt ? Math.max(0, Math.round((Date.now() - lastAt) / 1000)) : null;

  return (
    <>
      <div className="page-head">
        <h2>Overview</h2>
        <span className="sub">
          <span className="live-dot" /> polling /cluster + /raft + /metrics every 4s
          {age !== null && ` · updated ${age}s ago`}
        </span>
      </div>

      {err && (
        <div className="alert err" role="alert">
          <div className="alert-body"><b>/cluster unreachable.</b> {err}</div>
        </div>
      )}
      {metricsErr && !err && (
        <div className="alert warn">
          <div className="alert-body"><b>/metrics degraded.</b> {metricsErr}</div>
        </div>
      )}

      <KpiRow metrics={metrics} rate={rate} health={health} cluster={cluster} />

      <LocalNodeCard cluster={cluster} raft={raft} />
      <RaftCard raft={raft} cluster={cluster} />
      <NodesTable cluster={cluster} raft={raft} />
      <ShardingCard cluster={cluster} />
      <ReplicationCard metrics={metrics} rate={rate} />
      <CompactionCard metrics={metrics} rate={rate} />
      <AutobalanceCard cluster={cluster} />
    </>
  );
}

/* ── KPI tiles: the four headline numbers ────────────────────────── */
function KpiRow({ metrics, rate, health, cluster }: {
  metrics: Record<string, number> | null;
  rate: Record<string, number>;
  health: HealthInfo | null;
  cluster: ClusterInfo | null;
}) {
  const m = metrics ?? {};
  const uptime = health?.uptime_s ?? cluster?.local?.uptime_s;
  const conns = m.qengine_connections_active;
  const queries = m.qengine_queries_total;
  const rows = m.qengine_rows_written_total;
  const qErr = m.qengine_query_errors_total ?? 0;
  const qPs = rate.qengine_queries_total ?? 0;
  const rPs = rate.qengine_rows_written_total ?? 0;

  return (
    <div className="kpi-row">
      <div className="kpi">
        <div className="kpi-label">Uptime</div>
        <div className="kpi-val">{uptime != null ? fmtSec(uptime) : '—'}</div>
        <div className="kpi-sub">{health?.pid ? `pid ${health.pid}` : 'this node'}</div>
      </div>
      <div className="kpi">
        <div className="kpi-label">Connections</div>
        <div className="kpi-val">{conns != null ? fmtNum(conns) : '—'}</div>
        <div className="kpi-sub">
          active · {fmtNum(m.qengine_connections_total ?? 0)} lifetime
        </div>
      </div>
      <div className="kpi">
        <div className="kpi-label">Queries</div>
        <div className="kpi-val">{queries != null ? fmtNum(queries) : '—'}</div>
        <div className={`kpi-sub ${qPs > 0 ? 'up' : ''}`}>
          {qPs > 0 ? `${qPs.toFixed(1)}/s` : 'idle'}
          {qErr > 0 && ` · ${fmtNum(qErr)} errors`}
        </div>
      </div>
      <div className="kpi">
        <div className="kpi-label">Rows written</div>
        <div className="kpi-val">{rows != null ? fmtNum(rows) : '—'}</div>
        <div className={`kpi-sub ${rPs > 0 ? 'up' : ''}`}>
          {rPs > 0 ? `${fmtNum(Math.round(rPs))}/s` : 'idle'}
          {m.qengine_bytes_written_total != null &&
            ` · ${fmtBytes(m.qengine_bytes_written_total)}`}
        </div>
      </div>
    </div>
  );
}

/* ── Sharding: TSDB_SHARD_REPLICA_N status ───────────────────────── */
function ShardingCard({ cluster }: { cluster: ClusterInfo | null }) {
  const s = cluster?.sharding;
  if (!s) {
    return (
      <div className="card">
        <h3 className="card-title">Sharding</h3>
        <div className="empty tight">data unavailable</div>
      </div>
    );
  }
  const active = s.active === 1;
  const status = active
    ? `ACTIVE — N=${s.replica_n} owners per table out of ${s.alive_nodes} alive nodes`
    : s.replica_n > 0
      ? `IDLE — N=${s.replica_n} ≥ alive ${s.alive_nodes} (degenerates to full broadcast)`
      : 'OFF — full broadcast (TSDB_SHARD_REPLICA_N unset / 0)';
  const dot = active ? 'var(--ok)' : s.replica_n > 0 ? 'var(--warn)' : 'var(--mu)';
  const badge = active ? { cls: 'ok', txt: 'Active' }
    : s.replica_n > 0 ? { cls: 'warn', txt: 'Idle' }
    : { cls: 'mut', txt: 'Off' };
  return (
    <div className="card">
      <h3 className="card-title">
        Sharding <span className="mu">Phase β / γ</span>
        <span className="grow" />
        <span className={`badge ${badge.cls}`}>{badge.txt}</span>
      </h3>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <span className="status-dot" style={{ background: dot }} />
        <span style={{ fontWeight: 500 }}>{status}</span>
      </div>
      <div className="mu" style={{ marginTop: 8, fontSize: 12, lineHeight: 1.5 }}>
        Set <code>TSDB_SHARD_REPLICA_N</code> to <code>2..N-1</code> to enable.
        Writes fan out to N owners; non-owner nodes skip the local persist
        (Phase β.2) and forward SELECT queries to an owner (Phase γ).
        Default 0 keeps the legacy broadcast behaviour unchanged.
      </div>
    </div>
  );
}

/* ── Local node: disk bar + identity ─────────────────────────────── */
function LocalNodeCard({ cluster, raft }: { cluster: ClusterInfo | null; raft: RaftInfo | null }) {
  const L = cluster?.local;
  const D = L?.disk;
  const usedPct = D && D.total_bytes > 0
    ? ((D.total_bytes - D.free_bytes) / D.total_bytes) * 100
    : 0;
  const barClass = usedPct > 90 ? 'bad' : usedPct > 75 ? 'warn' : 'ok';
  return (
    <div className="card">
      <h3 className="card-title">
        <span className="live-dot" />
        This node
        <span className="mu">
          {L ? `${L.host} · pid ${L.pid} · up ${fmtSec(L.uptime_s)}` : ''}
        </span>
      </h3>
      {L && D ? (
        <>
          <div className="stat-row">
            <div className="stat">
              <div className="stat-label">Role</div>
              <div className="stat-val"><RoleChip role={raft?.role ?? '—'} /></div>
            </div>
            <Stat label="Cluster term" val={String(raft?.current_term ?? '—')} />
            <Stat label="Disk used" val={`${usedPct.toFixed(1)}%`}
                  tone={usedPct > 90 ? 'bad' : usedPct > 75 ? 'warn' : undefined} />
            <Stat label="Free" val={fmtBytes(D.free_bytes ?? 0)} />
            <Stat label="Total" val={fmtBytes(D.total_bytes ?? 0)} />
            <div className="stat">
              <div className="stat-label">Data dir</div>
              <div className="stat-val mono" style={{ fontSize: 12 }}>{D.data_dir ?? '—'}</div>
            </div>
          </div>
          <div className={`bar ${barClass}`} title={`${usedPct.toFixed(1)}% used`}>
            <span style={{ width: `${Math.min(100, usedPct)}%` }} />
          </div>
        </>
      ) : (
        <div className="empty tight">No local telemetry yet.</div>
      )}
    </div>
  );
}

/* ── Node list: all gossiped peers with HB + state colour ────────── */
function NodesTable({ cluster, raft }: { cluster: ClusterInfo | null; raft: RaftInfo | null }) {
  if (!cluster) return null;
  const sorted = [...(cluster.nodes ?? [])].sort((a, b) =>
    (a?.addr ?? '').localeCompare(b?.addr ?? ''));
  const alive = sorted.filter(n => n?.state === 'ALIVE').length;
  const degraded = sorted.length - alive;
  const leaderId = raft?.leader_id && raft.leader_id !== '0' ? raft.leader_id : '';
  if (!sorted.length) {
    return (
      <div className="card">
        <h3 className="card-title"><span className="live-dot" />Nodes</h3>
        <div className="empty tight">
          No peers gossiped yet — this node is standalone, or gossip is still converging.
        </div>
      </div>
    );
  }
  return (
    <div className="card">
      <h3 className="card-title">
        <span className="live-dot" />
        Nodes <span className="mu">{sorted.length} gossiped · refresh 4s</span>
        <span className="grow" />
        <span className={`badge ${degraded > 0 ? 'warn' : 'ok'}`}>
          {alive}/{sorted.length} alive
        </span>
      </h3>
      <div className="result">
        <table className="rows">
          <thead>
            <tr>
              <th>Node</th>
              <th>Addr</th>
              <th>Role</th>
              <th>State</th>
              <th>Storage</th>
              <th>HB age</th>
              <th>Susp.</th>
              <th>Known for</th>
              <th>Ver</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(n => (
              <NodeRow key={n.id} n={n}
                       local={n.id === cluster.local_id}
                       isLeader={!!leaderId && n.id === leaderId} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function NodeRow({ n, local, isLeader }: { n: ClusterNode; local: boolean; isLeader: boolean }) {
  const hb = n.hb_age_ms ?? 0;
  const hbClass = hb < 500 ? 'ok' : hb < 2000 ? 'warn' : 'bad';
  const st = n.state ?? '?';
  const stClass = st === 'ALIVE' ? 'ok' : st === 'SUSPECT' ? 'warn' : 'bad';
  return (
    <tr>
      <td>
        {local && <b title="this node">★ </b>}
        <span title={n.id} className="mono">{shortId(n.id ?? '')}</span>
      </td>
      <td className="mono">{n.addr ?? '—'}</td>
      <td><RoleChip role={isLeader ? 'leader' : (n.role ?? '—')} /></td>
      <td><span className={`pill ${stClass}`}>{st}</span></td>
      <td className="mono">{n.disk_bytes ? fmtBytes(n.disk_bytes) : '–'}</td>
      <td className={`mono hb-${hbClass}`}>{fmtNum(hb)} ms</td>
      <td>{n.suspect_count || '-'}</td>
      <td>{fmtSec(n.known_for_s)}</td>
      <td>{n.ver ?? '—'}</td>
    </tr>
  );
}

/* ── Replication health: counters + rates from /metrics ──────────── */
function ReplicationCard({ metrics, rate }: {
  metrics: Record<string, number> | null;
  rate: Record<string, number>;
}) {
  if (!metrics) return null;
  const sent = metrics.qengine_replicate_sent_total ?? 0;
  const ack = metrics.qengine_replicate_ack_total ?? 0;
  const fail = metrics.qengine_replicate_fail_total ?? 0;
  const rcvOk = metrics.qengine_replicate_recv_ok_total ?? 0;
  const rcvErr = metrics.qengine_replicate_recv_err_total ?? 0;
  const aePull = metrics.qengine_antientropy_rows_pulled_total ?? 0;
  const bytesRaw = metrics.qengine_replicate_bytes_raw_total ?? 0;
  const bytesWire = metrics.qengine_replicate_bytes_wire_total ?? 0;
  // Quorum wait that hit the hard deadline — a peer wedged; always a warning.
  const fanoutTimeout = metrics.qengine_fanout_wait_timeout_total ?? 0;
  const ackRate = sent > 0 ? ((ack / sent) * 100).toFixed(1) : '—';
  const failRate = sent > 0 ? ((fail / sent) * 100).toFixed(2) : '—';
  const ratio = bytesRaw > 0 && bytesWire > 0 ? bytesRaw / bytesWire : 0;
  const saved = ratio > 0 ? (1 - bytesWire / bytesRaw) * 100 : 0;

  const sentPs = rate.qengine_replicate_sent_total ?? 0;
  const ackPs = rate.qengine_replicate_ack_total ?? 0;
  const failPs = rate.qengine_replicate_fail_total ?? 0;

  return (
    <div className="card">
      <h3 className="card-title">
        <span className="live-dot" />
        Replication <span className="mu">WRITE_BATCH fanout · last 4s rates</span>
        {fanoutTimeout > 0 && (
          <>
            <span className="grow" />
            <span className="badge warn">{fmtNum(fanoutTimeout)} fanout timeouts</span>
          </>
        )}
      </h3>
      <div className="stat-row">
        <Stat label="Sent" val={fmtNum(sent)} sub={`${sentPs.toFixed(0)}/s`} />
        <Stat label="ACK" val={fmtNum(ack)} sub={`${ackPs.toFixed(0)}/s`} tone="ok" />
        <Stat label="Fail" val={fmtNum(fail)} sub={`${failPs.toFixed(0)}/s`}
              tone={fail > 0 ? 'warn' : undefined} />
        <Stat label="ACK rate" val={`${ackRate}%`}
              tone={sent > 0 && ack / sent < 0.95 ? 'warn' : 'ok'} />
        <Stat label="Fail rate" val={`${failRate}%`}
              tone={fail > 0 && sent > 0 && fail / sent > 0.02 ? 'warn' : undefined} />
      </div>
      <div className="stat-row">
        <Stat label="Recv OK" val={fmtNum(rcvOk)} />
        <Stat label="Recv Err" val={fmtNum(rcvErr)} tone={rcvErr > 0 ? 'bad' : undefined} />
        <Stat label="Anti-entropy rows" val={fmtNum(aePull)} sub="pulled after downtime" />
        <Stat label="Fanout timeouts" val={fmtNum(fanoutTimeout)}
              sub={fanoutTimeout > 0 ? 'a peer wedged' : 'none'}
              tone={fanoutTimeout > 0 ? 'warn' : undefined} />
      </div>
      <div className="stat-row">
        <Stat label="Wire bytes" val={fmtBytes(bytesWire)} sub="post-compression" />
        <Stat label="Raw bytes" val={fmtBytes(bytesRaw)} sub="pre-compression" />
        <Stat label="Compression"
              val={ratio > 0 ? `${ratio.toFixed(2)}×` : '—'}
              sub={ratio > 0 ? `${saved.toFixed(0)}% saved on the wire` : 'no traffic yet'}
              tone={ratio > 0 ? 'ok' : undefined} />
      </div>
    </div>
  );
}

/* ── Compaction activity: background storage maintenance ─────────── */
function CompactionCard({ metrics, rate }: {
  metrics: Record<string, number> | null;
  rate: Record<string, number>;
}) {
  if (!metrics) return null;
  const flushes = metrics.qengine_flushes_total ?? 0;
  const compactions = metrics.qengine_compactions_total ?? 0;
  const paused = metrics.qengine_compaction_paused_total ?? 0;
  const memoSkip = metrics.qengine_compaction_memo_skipped_total ?? 0;
  const pausedPs = rate.qengine_compaction_paused_total ?? 0;

  return (
    <div className="card">
      <h3 className="card-title">
        <span className="live-dot" />
        Compaction <span className="mu">background column-file maintenance</span>
        {pausedPs > 0 && (
          <>
            <span className="grow" />
            <span className="badge warn">pausing {pausedPs.toFixed(1)}/s</span>
          </>
        )}
      </h3>
      <div className="stat-row">
        <Stat label="Flushes" val={fmtNum(flushes)} sub="memtable → disk" />
        <Stat label="Compactions" val={fmtNum(compactions)} sub="column-file merges" />
        <Stat label="Paused" val={fmtNum(paused)}
              sub={pausedPs > 0 ? `${pausedPs.toFixed(1)}/s — writers hot` : 'writers idle'}
              tone={pausedPs > 0 ? 'warn' : undefined} />
        <Stat label="Memo skipped" val={fmtNum(memoSkip)} sub="fast-skip via mtime" />
      </div>
    </div>
  );
}

/* ── Raft: consensus health + apply lag ───────────────────────────
 * This card must NEVER vanish (a raft outage was once invisible):
 * unreachable / not-enabled / unhealthy / healthy all render. */
function RaftCard({ raft, cluster }: { raft: RaftInfo | null; cluster: ClusterInfo | null }) {
  if (raft === null) {
    return (
      <div className="card">
        <h3 className="card-title">
          Raft <span className="mu">consensus</span>
          <span className="grow" />
          <span className="badge bad"><span className="dot" />Unreachable</span>
        </h3>
        <div className="empty tight">
          /raft did not respond. Consensus state for this node is unknown.
        </div>
      </div>
    );
  }

  const enabled = !!raft.role;
  if (!enabled) {
    return (
      <div className="card">
        <h3 className="card-title">
          Raft <span className="mu">consensus</span>
          <span className="grow" />
          <span className="badge mut">Not enabled</span>
        </h3>
        <div className="mu" style={{ fontSize: 12, lineHeight: 1.55 }}>
          This node runs in <b>fanout mode</b> — writes broadcast to peers
          without a raft log. Set <code>TSDB_CONSENSUS</code> and assign a
          master role to elect a leader and replicate through the raft
          state machine.
        </div>
      </div>
    );
  }

  const role = raft.role ?? '—';
  const lastIdx = raft.last_index ?? 0;
  const applied = raft.last_applied ?? 0;
  const commit = raft.commit_index ?? 0;
  const applyLag = lastIdx - applied;
  const commitLag = lastIdx - commit;

  const leaderId = raft.leader_id && raft.leader_id !== '0' ? raft.leader_id : '';
  const hasLeader = leaderId !== '';
  const leader = cluster?.nodes?.find(n => n.id === leaderId);
  const isLeader = role === 'leader';
  const isSelfLeader = isLeader || (!!raft.self_id && leaderId === raft.self_id);

  const healthBad = !hasLeader;
  const health = healthBad
    ? { cls: 'bad' as const, txt: 'No leader', dot: true }
    : role === 'candidate'
      ? { cls: 'warn' as const, txt: 'Electing', dot: false }
      : isSelfLeader
        ? { cls: 'ok' as const, txt: 'Leader', dot: false }
        : { cls: 'ok' as const, txt: 'Healthy', dot: false };

  const leaderVal = hasLeader ? (leader ? leader.addr : leaderId) : 'NONE';

  return (
    <div className="card">
      <h3 className="card-title">
        Raft <span className="mu">log state machine</span>
        <span className="grow" />
        <span className={`badge ${health.cls}`}>
          {health.dot && <span className="dot" />}{health.txt}
        </span>
      </h3>
      {healthBad && (
        <div className="alert err" style={{ marginBottom: 10 }}>
          <div className="alert-body">
            No leader elected — proposes (DDL / replicated writes) will fail
            until a quorum forms.
          </div>
        </div>
      )}
      <div className="stat-row">
        <Stat label="Role" val={role}
              tone={isLeader ? 'ok' : healthBad ? 'bad' : undefined} />
        <Stat label="Term" val={String(raft.current_term ?? '—')} />
        <Stat label="Self" val={raft.self_id ? shortId(raft.self_id) : '—'} />
        <Stat label="Leader" val={leaderVal}
              sub={hasLeader ? (leader ? shortId(leader.id) : 'id only') : 'unelected'}
              tone={healthBad ? 'bad' : undefined} />
        <Stat label="Last index" val={fmtNum(lastIdx)} />
        <Stat label="Commit index" val={fmtNum(commit)}
              sub={commitLag > 0 ? `lag ${commitLag}` : 'in sync'}
              tone={commitLag > 10 ? 'warn' : undefined} />
        <Stat label="Applied" val={fmtNum(applied)}
              sub={applyLag > 0 ? `lag ${applyLag}` : 'caught up'}
              tone={applyLag > 10 ? 'warn' : undefined} />
      </div>
    </div>
  );
}

/* ── Autobalance: ring placements + storage per VN ────────────────── */
function AutobalanceCard({ cluster }: { cluster: ClusterInfo | null }) {
  const ab = cluster?.autobalance;
  if (!ab || !ab.nodes?.length) return null;
  return (
    <div className="card">
      <h3 className="card-title">
        <span className="live-dot" />
        Autobalance
        <span className="mu">
          ring placement · interval {ab.interval_ms}ms · α {ab.alpha} β {ab.beta}
        </span>
      </h3>
      <div className="stat-row">
        <Stat label="EMA writes/s" val={(ab.ema_writes_sec ?? 0).toFixed(2)} />
        <Stat label="Interval" val={`${ab.interval_ms ?? 0} ms`} />
      </div>
      <div className="result">
        <table className="rows">
          <thead>
            <tr>
              <th>Node</th>
              <th>Writes/s</th>
              <th>Storage</th>
              <th>CPU %</th>
              <th>Virtual nodes</th>
            </tr>
          </thead>
          <tbody>
            {ab.nodes.map(n => (
              <tr key={n.id}>
                <td className="mono">
                  {shortId(n.id)}{n.id === cluster?.local_id && <b> ★</b>}
                </td>
                <td>{n.writes_sec ?? 0}</td>
                <td>{(n.storage_mb ?? 0) < 1024
                  ? `${(n.storage_mb ?? 0).toFixed(1)} MB`
                  : fmtBytes((n.storage_mb ?? 0) * 1024 * 1024)}</td>
                <td>{(n.cpu_pct ?? 0).toFixed(1)}</td>
                <td>{n.vn ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ── Small reusable bits ──────────────────────────────────────────── */

function Stat({ label, val, sub, tone }: {
  label: string;
  val: string;
  sub?: string;
  tone?: 'ok' | 'warn' | 'bad';
}) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className={`stat-val ${tone ? `tone-${tone}` : ''}`}>{val}</div>
      {sub && <div className="stat-sub">{sub}</div>}
    </div>
  );
}

function RoleChip({ role }: { role: string }) {
  const lead = role === 'leader' || role === 'master';
  return (
    <span className={`role-chip ${lead ? 'lead' : ''}`}>
      {role}
    </span>
  );
}

function shortId(id: string): string {
  return id.length > 10 ? `${id.slice(0, 6)}…${id.slice(-4)}` : id;
}

function fmtNum(n: number): string {
  return Number.isFinite(n) ? n.toLocaleString() : '—';
}

function fmtBytes(b: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let u = 0;
  while (b >= 1024 && u < units.length - 1) { b /= 1024; u++; }
  return `${b.toFixed(b < 10 ? 2 : b < 100 ? 1 : 0)} ${units[u]}`;
}

function fmtSec(s?: number): string {
  if (s == null || !Number.isFinite(s)) return '—';
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}
