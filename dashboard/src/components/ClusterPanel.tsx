import { useCallback, useEffect, useState } from 'react';
import { api } from '../api';

interface Node {
  id?: string;
  addr?: string;
  state?: string;
  role?: string;
  ver?: number;
  hb_age_ms?: number;
}
interface ClusterShape {
  local_id?: string;
  nodes?: Node[];
}
interface RaftShape {
  self_id?: string;
  role?: string;
  current_term?: number;
  leader_id?: string;
  commit_index?: number;
  last_applied?: number;
  last_index?: number;
}

export function ClusterPanel() {
  const [cluster, setCluster] = useState<ClusterShape | null>(null);
  const [raft, setRaft] = useState<RaftShape | null>(null);
  const [err, setErr] = useState('');

  const load = useCallback(async () => {
    try {
      const [c, r] = await Promise.all([
        api.cluster().catch(() => null) as Promise<ClusterShape | null>,
        api.raft().catch(() => null) as Promise<RaftShape | null>,
      ]);
      setCluster(c);
      setRaft(r);
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
      <div className="card">
        <h3>Cluster <span className="mu">— refresh 4s</span></h3>
        {err && <div style={{ color: 'var(--bad)' }}>{err}</div>}
        {cluster && (
          <>
            <div className="result-meta">local_id: {cluster.local_id ?? '-'}</div>
            <div className="result">
              <table className="rows">
                <thead>
                  <tr>
                    <th>Node ID</th>
                    <th>Addr</th>
                    <th>Role</th>
                    <th>State</th>
                    <th>HB age (ms)</th>
                    <th>Ver</th>
                  </tr>
                </thead>
                <tbody>
                  {(cluster.nodes ?? []).map(n => (
                    <tr key={n.id}>
                      <td>{n.id === cluster.local_id ? <b>{n.id} ←</b> : n.id}</td>
                      <td>{n.addr}</td>
                      <td>{n.role}</td>
                      <td>{n.state}</td>
                      <td>{n.hb_age_ms}</td>
                      <td>{n.ver}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>

      {raft && raft.role && (
        <div className="card">
          <h3>Raft</h3>
          <div className="result-meta">
            role <b>{raft.role}</b> · term <b>{raft.current_term}</b> · leader <b>{raft.leader_id}</b>
            &nbsp;· commit {raft.commit_index} · applied {raft.last_applied} · last {raft.last_index}
          </div>
        </div>
      )}
    </>
  );
}
