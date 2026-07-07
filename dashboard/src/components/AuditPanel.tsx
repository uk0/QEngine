import { useCallback, useEffect, useState } from 'react';
import { api, AuditRecord, errMsg } from '../api';

export function AuditPanel() {
  const [rows, setRows] = useState<AuditRecord[]>([]);
  const [err, setErr] = useState('');
  const [loading, setLoading] = useState(false);
  const [limit, setLimit] = useState(200);
  const [loaded, setLoaded] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await api.audit(limit);
      setRows(Array.isArray(r?.rows) ? r.rows : []);
      setErr('');
    } catch (e) {
      setErr(errMsg(e, 'audit load failed'));
    } finally {
      setLoading(false);
      setLoaded(true);
    }
  }, [limit]);

  useEffect(() => { load(); }, [load]);

  return (
    <>
      <div className="page-head">
        <h2>Audit log</h2>
        <span className="sub">JSONL tail from /audit?n={limit}</span>
        <span className="grow" />
        <select value={limit} onChange={e => setLimit(Number(e.target.value))}
                aria-label="Tail size">
          {[50, 100, 200, 500, 1000, 2000].map(n => (
            <option key={n} value={n}>tail {n}</option>
          ))}
        </select>
        <button onClick={load} disabled={loading}>
          {loading ? <><span className="spinner" /> Loading…</> : 'Reload'}
        </button>
      </div>

      <div className="card">
        <h3 className="card-title">
          Records
          {rows.length > 0 && (
            <span className="badge mut num">{rows.length.toLocaleString()} records</span>
          )}
        </h3>
        {err && (
          <div className="alert err" role="alert" style={{ marginBottom: 10 }}>
            <div className="alert-body"><b>/audit failed.</b> {err}</div>
          </div>
        )}
        <div className="audit-scroll">
          <div className="audit-row header">
            <div>Timestamp</div>
            <div>Event</div>
            <div>User</div>
            <div>Action</div>
            <div>Result</div>
            <div>Detail / Object</div>
          </div>
          {rows.map((r, i) => <AuditRow key={i} r={r} />)}
          {!rows.length && !err && (
            <div className="empty">
              {loading || !loaded
                ? <><span className="spinner" /> Loading audit tail…</>
                : 'No audit records yet.'}
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function AuditRow({ r }: { r: AuditRecord }) {
  const ts = (r?.ts ?? '').replace(/Z$/, '').replace('T', ' ');
  const result = r?.result ?? 0;
  return (
    <div className="audit-row">
      <div title={r?.ts ?? ''}>{ts || '—'}</div>
      <div><span className={`ev ${r?.event ?? ''}`}>{r?.event ?? '—'}</span></div>
      <div title={r?.user ?? ''}>{r?.user || '-'}</div>
      <div title={r?.action ?? ''}>{r?.action ?? '—'}</div>
      <div className={`rc ${result === 0 ? 'ok' : 'bad'}`}>
        {result === 0 ? 'OK' : result}
      </div>
      <div title={`${r?.object ?? ''} ${r?.detail ?? ''}`}>
        {r?.object ? `on "${r.object}"` : ''}
        {r?.detail ? ` — ${r.detail}` : ''}
      </div>
    </div>
  );
}
