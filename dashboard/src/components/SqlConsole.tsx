import { useCallback, useRef, useState } from 'react';
import { api, errMsg, SqlResult } from '../api';
import { useToastCtx } from './Toasts';
import { QtlExamples, QUICK_EXAMPLES } from './QtlExamples';

interface Props {
  sql: string;
  onSql: (s: string) => void;
  onRefreshTree: () => void;
}

export function SqlConsole({ sql, onSql, onRefreshTree }: Props) {
  const [result, setResult] = useState<SqlResult | null>(null);
  const [err, setErr] = useState<string>('');
  const [busy, setBusy] = useState(false);
  const [showCatalog, setShowCatalog] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const toast = useToastCtx();
  const editorRef = useRef<HTMLTextAreaElement | null>(null);

  const run = useCallback(async () => {
    const q = sql.trim();
    if (!q || busy) return;
    setBusy(true);
    setErr('');
    const t0 = performance.now();
    try {
      const r = await api.sql(q);
      setResult(r);
      setElapsed(Math.round(performance.now() - t0));
      if (/^\s*(CREATE|DROP|ALTER|TRUNCATE|DELETE)\b/i.test(q)) onRefreshTree();
    } catch (e) {
      const msg = errMsg(e, 'query failed');
      setErr(msg);
      setResult(null);
    } finally {
      setBusy(false);
    }
  }, [sql, busy, onRefreshTree]);

  const onKey = (ev: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter') {
      ev.preventDefault();
      run();
    }
  };

  const pick = (q: string) => {
    onSql(q);
    editorRef.current?.focus();
  };

  const copyResult = useCallback(async () => {
    if (!result) return;
    const lines = [
      (result.cols ?? []).join('\t'),
      ...(result.rows ?? []).map(r => (r ?? []).map(c => fmtCell(c)).join('\t')),
    ];
    try {
      await navigator.clipboard.writeText(lines.join('\n'));
      toast.ok(`${result.rows?.length ?? 0} rows copied as TSV`);
    } catch {
      toast.err('clipboard unavailable');
    }
  }, [result, toast]);

  return (
    <>
      <section className="card">
        <h3>
          Editor
          <span className="mu">QTL / SQL statement</span>
          <span className="grow" />
          <button className="ghost" onClick={() => setShowCatalog(s => !s)}>
            {showCatalog ? 'Hide examples' : 'Browse all examples'}
          </button>
        </h3>
        <textarea
          ref={editorRef}
          className="sql"
          value={sql}
          onChange={e => onSql(e.target.value)}
          onKeyDown={onKey}
          spellCheck={false}
          placeholder="Type a QTL statement, pick an example below, or double-click a table in the schema tree…"
        />
        <div className="sql-actions">
          <button className="primary" onClick={run} disabled={busy || !sql.trim()}>
            {busy ? <><span className="spinner" /> Running…</> : 'Run'}
          </button>
          <button className="ghost" onClick={() => onSql('')} disabled={busy || !sql}>
            Clear
          </button>
          <span className="hint">
            <span className="kbd">{navigator.platform?.includes('Mac') ? '⌘' : 'Ctrl'}</span>
            {' + '}
            <span className="kbd">Enter</span> to run
            {sql.length > 0 && ` · ${sql.length} chars`}
          </span>
        </div>
        <div className="chip-row">
          {QUICK_EXAMPLES.map(x => (
            <button key={x.title} className="chip" title={x.sql} onClick={() => pick(x.sql)}>
              {x.title}
            </button>
          ))}
        </div>
        {showCatalog && <QtlExamples onPick={pick} />}
      </section>

      <section className="card">
        <h3>
          Result
          {result && !err && (
            <span className="mu">
              {result.nrows ?? 0} rows · {result.ms ?? 0} ms server · {elapsed} ms total
            </span>
          )}
          <span className="grow" />
          {result && !err && (result.rows?.length ?? 0) > 0 && (
            <button className="ghost" onClick={copyResult}>Copy TSV</button>
          )}
          {result?.truncated && <span className="badge warn">truncated</span>}
        </h3>
        {err && (
          <div className="alert err" role="alert">
            <div className="alert-body"><b>Query failed.</b> {err}</div>
          </div>
        )}
        {!err && busy && !result && (
          <div className="empty"><span className="spinner" /> Executing…</div>
        )}
        {!err && result && <ResultTable r={result} />}
        {!err && !busy && !result && (
          <div className="empty">No result yet — run a statement above.</div>
        )}
      </section>
    </>
  );
}

function ResultTable({ r }: { r: SqlResult }) {
  const cols = r.cols ?? [];
  const rows = r.rows ?? [];
  if (!cols.length && !rows.length) {
    return <div className="alert ok"><div className="alert-body">OK — statement executed, no result set.</div></div>;
  }
  return (
    <div className="result">
      <table className="rows">
        <thead>
          <tr>
            {cols.map((c, i) => (
              <th key={i}>
                {c}
                <span className="ty">{r.types?.[i] ?? ''}</span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri}>
              {(row ?? []).map((cell, ci) => (
                <td key={ci} title={fmtCell(cell)}>
                  {cell === null || cell === undefined
                    ? <span className="null">NULL</span>
                    : fmtCell(cell)}
                </td>
              ))}
            </tr>
          ))}
          {!rows.length && (
            <tr><td colSpan={Math.max(cols.length, 1)} className="null">0 rows</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'number' || typeof v === 'bigint') return String(v);
  if (typeof v === 'object') {
    try { return JSON.stringify(v); } catch { return String(v); }
  }
  return String(v);
}
