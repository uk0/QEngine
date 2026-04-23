import { useCallback, useEffect, useState } from 'react';
import { api, AuditRecord } from '../api';

export function AuditPanel() {
  const [rows, setRows] = useState<AuditRecord[]>([]);
  const [err, setErr] = useState('');
  const [loading, setLoading] = useState(false);
  const [limit, setLimit] = useState(200);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await api.audit(limit);
      setRows(r.rows ?? []);
      setErr('');
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'load failed');
    } finally {
      setLoading(false);
    }
  }, [limit]);

  useEffect(() => { load(); }, [load]);

  return (
    <div className="card">
      <h3>
        Audit log <span className="mu">— JSONL tail from /audit?n={limit}</span>
        <span style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
          <select value={limit} onChange={e => setLimit(Number(e.target.value))}>
            {[50, 100, 200, 500, 1000, 2000].map(n => (
              <option key={n} value={n}>tail {n}</option>
            ))}
          </select>
          <button onClick={load} disabled={loading}>{loading ? '…' : 'Reload'}</button>
        </span>
      </h3>
      {err && <div style={{ color: 'var(--bad)' }}>{err}</div>}
      <div style={{ maxHeight: 600, overflow: 'auto', border: '1px solid var(--bd)', borderRadius: 6 }}>
        <div className="audit-row header">
          <div>Timestamp</div>
          <div>Event</div>
          <div>User</div>
          <div>Action</div>
          <div>Result</div>
          <div>Detail / Object</div>
        </div>
        {rows.map((r, i) => (
          <div key={i} className="audit-row">
            <div>{r.ts.replace(/Z$/, '').replace('T', ' ')}</div>
            <div><span className={`ev ${r.event}`}>{r.event}</span></div>
            <div>{r.user || '-'}</div>
            <div>{r.action}</div>
            <div className={`rc ${r.result === 0 ? 'ok' : 'bad'}`}>
              {r.result === 0 ? 'OK' : r.result}
            </div>
            <div title={`${r.object} ${r.detail}`}>
              {r.object ? `on "${r.object}"` : ''}
              {r.detail ? ` — ${r.detail}` : ''}
            </div>
          </div>
        ))}
        {!rows.length && !err && (
          <div className="empty">No audit records yet.</div>
        )}
      </div>
    </div>
  );
}
