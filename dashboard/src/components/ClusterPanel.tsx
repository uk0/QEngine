import { useCallback, useEffect, useRef, useState } from 'react';
import { api, ClusterInfo, ClusterNode, RaftInfo } from '../api';

export function ClusterPanel() {
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [raft, setRaft] = useState<RaftInfo | null>(null);
  const [metrics, setMetrics] = useState<Record<string, number> | null>(null);
  const [err, setErr] = useState('');
  // Keep the previous metrics snapshot so we can render per-interval deltas
  // (e.g. replicate rate/s) without asking the server for a rate directly.
  const prev = useRef<{ at: number; m: Record<string, number> } | null>(null);
  const [rate, setRate] = useState<Record<string, number>>({});

  const load = useCallback(async () => {
    try {
      const [c, r, m] = await Promise.all([
        api.cluster().catch(() => null),
        api.raft().catch(() => null),
        api.metrics().catch(() => null),
      ]);
      setCluster(c);
      setRaft(r);
      setMetrics(m);

      // Compute per-second rates against the previous sample.
      if (m && prev.current) {
        const dt = (Date.now() - prev.current.at) / 1000;
        if (dt > 0.2) {
          const r: Record<string, number> = {};
          for (const k of Object.keys(m)) {
            const d = (m[k] ?? 0) - (prev.current.m[k] ?? 0);
            if (d > 0) r[k] = d / dt;
          }
          setRate(r);
        }
      }
      if (m) prev.current = { at: Date.now(), m };
      setErr('');
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'load failed');
    }
  }, []);

  useEffect(() => {
    load();
    const h = setInterval(load, 4000);
    return () => clearInterval(h);
  }, [load]);

  return (
    <>
      {err && <div className="card" style={{ color: 'var(--bad)' }}>{err}</div>}

      <LocalNodeCard cluster={cluster} raft={raft} />
      <NodesTable cluster={cluster} />
      <ShardingCard cluster={cluster} />
      <ReplicationCard metrics={metrics} rate={rate} />
      <RaftCard raft={raft} cluster={cluster} />
      <AutobalanceCard cluster={cluster} />
    </>
  );
}

/* ── Sharding: TSDB_SHARD_REPLICA_N status ──────────────────────────── */
function ShardingCard({ cluster }: { cluster: ClusterInfo | null }) {
  const s = cluster?.sharding;
  if (!s) {
    return (
      <div className="card">
        <h3 className="card-title">Sharding</h3>
        <div style={{ opacity: 0.6, fontSize: 13 }}>data unavailable</div>
      </div>
    );
  }
  const active = s.active === 1;
  const status = active
    ? `ACTIVE — N=${s.replica_n} owners per table out of ${s.alive_nodes} alive nodes`
    : s.replica_n > 0
      ? `IDLE — N=${s.replica_n} ≥ alive ${s.alive_nodes} (degenerates to full broadcast)`
      : 'OFF — full broadcast (TSDB_SHARD_REPLICA_N unset / 0)';
  const dot = active ? 'var(--ok)' : s.replica_n > 0 ? 'var(--warn)' : 'var(--muted)';
  return (
    <div className="card">
      <h3 className="card-title">Sharding (Phase β / γ)</h3>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{
          width: 10, height: 10, borderRadius: 5,
          background: dot, display: 'inline-block',
        }} />
        <span style={{ fontWeight: 500 }}>{status}</span>
      </div>
      <div style={{ marginTop: 8, fontSize: 12, opacity: 0.7, lineHeight: 1.5 }}>
        Set <code>TSDB_SHARD_REPLICA_N</code> in compose to <code>2..N-1</code>
        to enable.  Writes fan out to N owners; non-owner nodes skip the
        local persist (Phase β.2) and forward SELECT queries to an owner
        (Phase γ).  Default 0 keeps the legacy "broadcast to every alive
        peer" behaviour bit-for-bit unchanged.
      </div>
    </div>
  );
}

/* ── Local node: disk bar + identity ──────────────────────────────── */
function LocalNodeCard({ cluster, raft }: { cluster: ClusterInfo | null; raft: RaftInfo | null }) {
  const L = cluster?.local;
  const D = L?.disk;
  const usedPct = D && D.total_bytes > 0
    ? ((D.total_bytes - D.free_bytes) / D.total_bytes) * 100
    : 0;
  const barClass = usedPct > 90 ? 'bad' : usedPct > 75 ? 'warn' : 'ok';
  return (
    <div className="card">
      <h3>
        This node
        <span className="mu">
          {L ? ` — ${L.host} · pid ${L.pid} · up ${fmtSec(L.uptime_s)}` : ''}
        </span>
      </h3>
      {L && D ? (
        <>
          <div className="stat-row">
            <div className="stat">
              <div className="stat-label">Role</div>
              <div className="stat-val">
                <RoleBadge role={raft?.role ?? '-'} /> {raft?.role ?? '-'}
              </div>
            </div>
            <div className="stat">
              <div className="stat-label">Cluster term</div>
              <div className="stat-val">{raft?.current_term ?? '-'}</div>
            </div>
            <div className="stat">
              <div className="stat-label">Disk used</div>
              <div className="stat-val">{usedPct.toFixed(1)}%</div>
            </div>
            <div className="stat">
              <div className="stat-label">Free</div>
              <div className="stat-val">{fmtBytes(D.free_bytes)}</div>
            </div>
            <div className="stat">
              <div className="stat-label">Total</div>
              <div className="stat-val">{fmtBytes(D.total_bytes)}</div>
            </div>
            <div className="stat">
              <div className="stat-label">Data dir</div>
              <div className="stat-val mono">{D.data_dir}</div>
            </div>
          </div>
          <div className={`bar ${barClass}`} title={`${usedPct.toFixed(1)}% used`}>
            <span style={{ width: `${Math.min(100, usedPct)}%` }} />
          </div>
        </>
      ) : (
        <div className="mu" style={{ fontSize: 12 }}>No local telemetry yet.</div>
      )}
    </div>
  );
}

/* ── Node list: all gossiped peers with HB + state colour ────────── */
function NodesTable({ cluster }: { cluster: ClusterInfo | null }) {
  if (!cluster) return null;
  const sorted = [...(cluster.nodes ?? [])].sort((a, b) => a.addr.localeCompare(b.addr));
  return (
    <div className="card">
      <h3>
        Nodes <span className="mu">({sorted.length} gossiped · refresh 4s)</span>
      </h3>
      <div className="result">
        <table className="rows">
          <thead>
            <tr>
              <th>Node</th>
              <th>Addr</th>
              <th>Role</th>
              <th>State</th>
              <th>HB age</th>
              <th>Susp.</th>
              <th>Known for</th>
              <th>Ver</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(n => (
              <NodeRow key={n.id} n={n} local={n.id === cluster.local_id} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function NodeRow({ n, local }: { n: ClusterNode; local: boolean }) {
  const hbClass =
    n.hb_age_ms < 500  ? 'ok'   :
    n.hb_age_ms < 2000 ? 'warn' : 'bad';
  const stClass = n.state === 'ALIVE' ? 'ok' : n.state === 'SUSPECT' ? 'warn' : 'bad';
  return (
    <tr>
      <td>
        {local && <b>★ </b>}
        <span title={n.id} className="mono">{shortId(n.id)}</span>
      </td>
      <td className="mono">{n.addr}</td>
      <td><RoleBadge role={n.role} />{n.role}</td>
      <td><span className={`pill ${stClass}`}>{n.state}</span></td>
      <td className={`mono hb-${hbClass}`}>{n.hb_age_ms} ms</td>
      <td>{n.suspect_count || '-'}</td>
      <td>{fmtSec(n.known_for_s)}</td>
      <td>{n.ver}</td>
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
  const ack  = metrics.qengine_replicate_ack_total  ?? 0;
  const fail = metrics.qengine_replicate_fail_total ?? 0;
  const rcvOk  = metrics.qengine_replicate_recv_ok_total  ?? 0;
  const rcvErr = metrics.qengine_replicate_recv_err_total ?? 0;
  const aePull = metrics.qengine_antientropy_rows_pulled_total ?? 0;
  const ackRate = sent > 0 ? ((ack / sent) * 100).toFixed(1) : '—';
  const failRate = sent > 0 ? ((fail / sent) * 100).toFixed(2) : '—';

  const sentPs = rate.qengine_replicate_sent_total ?? 0;
  const ackPs  = rate.qengine_replicate_ack_total  ?? 0;
  const failPs = rate.qengine_replicate_fail_total ?? 0;

  return (
    <div className="card">
      <h3>Replication <span className="mu">— WRITE_BATCH fanout · last 4s rates</span></h3>
      <div className="stat-row">
        <Stat label="Sent"       val={sent.toLocaleString()}   sub={`${sentPs.toFixed(0)}/s`} />
        <Stat label="ACK"        val={ack.toLocaleString()}    sub={`${ackPs.toFixed(0)}/s`} tone="ok" />
        <Stat label="Fail"       val={fail.toLocaleString()}   sub={`${failPs.toFixed(0)}/s`} tone={fail > 0 ? 'warn' : undefined} />
        <Stat label="ACK rate"   val={`${ackRate}%`} tone={sent > 0 && ack / sent < 0.95 ? 'warn' : 'ok'} />
        <Stat label="Fail rate"  val={`${failRate}%`} tone={fail > 0 && sent > 0 && fail / sent > 0.02 ? 'warn' : undefined} />
      </div>
      <div className="stat-row">
        <Stat label="Recv OK"  val={rcvOk.toLocaleString()}  />
        <Stat label="Recv Err" val={rcvErr.toLocaleString()} tone={rcvErr > 0 ? 'bad' : undefined} />
        <Stat label="Anti-entropy rows" val={aePull.toLocaleString()}
              sub="pulled after downtime" />
      </div>
    </div>
  );
}

/* ── Raft: apply lag = last_index − last_applied ─────────────────── */
function RaftCard({ raft, cluster }: { raft: RaftInfo | null; cluster: ClusterInfo | null }) {
  if (!raft || !raft.role) return null;
  const lastIdx = raft.last_index ?? 0;
  const applied = raft.last_applied ?? 0;
  const commit  = raft.commit_index ?? 0;
  const applyLag = lastIdx - applied;
  const commitLag = lastIdx - commit;
  const leader = cluster?.nodes?.find(n => n.id === raft.leader_id);
  return (
    <div className="card">
      <h3>Raft <span className="mu">— log state machine</span></h3>
      <div className="stat-row">
        <Stat label="Role"          val={raft.role} tone={raft.role === 'leader' ? 'ok' : undefined} />
        <Stat label="Term"          val={String(raft.current_term ?? '-')} />
        <Stat label="Leader"        val={leader ? `${leader.addr}` : (raft.leader_id ?? '-')}
              sub={leader ? shortId(leader.id) : ''} />
        <Stat label="Last index"    val={lastIdx.toLocaleString()} />
        <Stat label="Commit index"  val={commit.toLocaleString()}
              sub={commitLag > 0 ? `lag ${commitLag}` : 'in sync'}
              tone={commitLag > 10 ? 'warn' : undefined} />
        <Stat label="Applied"       val={applied.toLocaleString()}
              sub={applyLag > 0 ? `lag ${applyLag}` : 'caught up'}
              tone={applyLag > 10 ? 'warn' : undefined} />
      </div>
    </div>
  );
}

/* ── Autobalance: ring placements + storage_mb per VN ────────────── */
function AutobalanceCard({ cluster }: { cluster: ClusterInfo | null }) {
  const ab = cluster?.autobalance;
  if (!ab || !ab.nodes?.length) return null;
  return (
    <div className="card">
      <h3>Autobalance <span className="mu">— ring placement · interval {ab.interval_ms}ms · α {ab.alpha} β {ab.beta}</span></h3>
      <div className="stat-row">
        <Stat label="EMA writes/s" val={ab.ema_writes_sec.toFixed(2)} />
        <Stat label="Interval"     val={`${ab.interval_ms} ms`} />
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
                <td className="mono">{shortId(n.id)}{n.id === cluster?.local_id && <b> ★</b>}</td>
                <td>{n.writes_sec}</td>
                <td>{n.storage_mb < 1024 ? `${n.storage_mb.toFixed(1)} MB` : fmtBytes(n.storage_mb * 1024 * 1024)}</td>
                <td>{n.cpu_pct.toFixed(1)}</td>
                <td>{n.vn}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ── Small reusable bits ──────────────────────────────────────────── */

function Stat({
  label,
  val,
  sub,
  tone,
}: {
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

function RoleBadge({ role }: { role: string }) {
  const cls = role === 'leader' || role === 'master' ? 'hi' :
              role === 'follower' || role === 'data' ? 'g' : '';
  return <span className={`ic ${cls}`} style={{ marginRight: 4 }}>{initials(role)}</span>;
}

function initials(s: string): string {
  const map: Record<string, string> = {
    master: 'M', data: 'D',
    leader: 'L', follower: 'F', candidate: 'C',
  };
  return map[s] ?? s.slice(0, 1).toUpperCase();
}

function shortId(id: string): string {
  return id.length > 10 ? `${id.slice(0, 6)}…${id.slice(-4)}` : id;
}

function fmtBytes(b: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let u = 0;
  while (b >= 1024 && u < units.length - 1) { b /= 1024; u++; }
  return `${b.toFixed(b < 10 ? 2 : b < 100 ? 1 : 0)} ${units[u]}`;
}

function fmtSec(s?: number): string {
  if (s == null) return '-';
  if (s < 60)     return `${s}s`;
  if (s < 3600)   return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400)  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}
