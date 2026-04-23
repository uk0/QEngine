import { useCallback, useState } from 'react';
import { api, SqlResult } from '../api';
import { useToastCtx } from './Toasts';

interface Props {
  sql: string;
  onSql: (s: string) => void;
  onRefreshTree: () => void;
}

export function SqlConsole({ sql, onSql, onRefreshTree }: Props) {
  const [result, setResult] = useState<SqlResult | null>(null);
  const [err, setErr] = useState<string>('');
  const [busy, setBusy] = useState(false);
  const toast = useToastCtx();

  const run = useCallback(async () => {
    if (!sql.trim()) return;
    setBusy(true);
    setErr('');
    try {
      const r = await api.sql(sql);
      setResult(r);
      // Refresh tree on DDL so the sidebar tracks the new catalog state.
      if (/^\s*(CREATE|DROP|ALTER|TRUNCATE|DELETE)\b/i.test(sql)) onRefreshTree();
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'query failed';
      setErr(msg);
      toast.err(msg);
      setResult(null);
    } finally {
      setBusy(false);
    }
  }, [sql, onRefreshTree, toast]);

  const onKey = (ev: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter') {
      ev.preventDefault();
      run();
    }
  };

  return (
    <>
      <div className="card">
        <h3>SQL <span className="mu">— ⌘/Ctrl+Enter to run</span></h3>
        <div className="sql-wrap">
          <textarea
            className="sql"
            value={sql}
            onChange={e => onSql(e.target.value)}
            onKeyDown={onKey}
            spellCheck={false}
          />
          <div className="sql-actions">
            <button className="primary" onClick={run} disabled={busy}>
              {busy ? 'Running…' : 'Run'}
            </button>
            <button onClick={() => onSql('')}>Clear</button>
            <span className="hint">
              Examples:
              <button onClick={() => onSql('LIST DATABASES')}>LIST DBs</button>
              <button onClick={() => onSql('LIST VTABLES')}>LIST VTables</button>
              <button onClick={() => onSql('LIST PTABLES')}>LIST PTables</button>
              <button onClick={() => onSql('LIST USERS')}>LIST USERS</button>
            </span>
          </div>
        </div>
      </div>
      <div className="card">
        <h3>Result</h3>
        {err && <div className="result-meta" style={{ color: 'var(--bad)' }}>{err}</div>}
        {result && <ResultTable r={result} />}
        {!err && !result && <div className="mu" style={{ fontSize: 12 }}>Run a query above.</div>}
      </div>
    </>
  );
}

function ResultTable({ r }: { r: SqlResult }) {
  return (
    <>
      <div className="result-meta">
        {r.nrows} rows · {r.ms}ms{r.truncated ? ' · truncated' : ''}
      </div>
      <div className="result">
        <table className="rows">
          <thead>
            <tr>
              {r.cols.map((c, i) => (
                <th key={i}>{c} <span style={{ color: '#94a3b8', fontWeight: 400 }}>{r.types[i]}</span></th>
              ))}
            </tr>
          </thead>
          <tbody>
            {r.rows.map((row, ri) => (
              <tr key={ri}>
                {row.map((cell, ci) => <td key={ci}>{fmtCell(cell)}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'number') return String(v);
  if (typeof v === 'bigint') return v.toString();
  return String(v);
}
