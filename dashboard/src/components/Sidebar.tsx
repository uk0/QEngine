import { useCallback, useEffect, useState } from 'react';
import { api, ddl, Tree } from '../api';
import { useToastCtx } from './Toasts';

interface Props {
  bump: number;
  onSql: (q: string) => void;
  onRefresh: () => void;
}

export function Sidebar({ bump, onSql, onRefresh }: Props) {
  const [tree, setTree] = useState<Tree | null>(null);
  const [err, setErr] = useState<string>('');
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const toast = useToastCtx();

  const load = useCallback(async () => {
    try {
      const t = await api.tree();
      setTree(t);
      setErr('');
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'tree load failed');
    }
  }, []);

  useEffect(() => { load(); }, [load, bump]);

  const toggle = (key: string) => setOpen(o => ({ ...o, [key]: !o[key] }));
  const isOpen = (key: string) => !!open[key];

  async function run(
    kind: string,
    name: string,
    fn: () => Promise<unknown>,
  ) {
    if (!confirm(`Drop ${kind} "${name}" ?`)) return;
    try {
      await fn();
      toast.ok(`${kind} "${name}" dropped`);
      onRefresh();
    } catch (e) {
      toast.err(e instanceof Error ? e.message : `drop ${kind} failed`);
    }
  }

  async function createDb() {
    const name = prompt('New database name:');
    if (!name) return;
    try {
      await ddl.createDatabase(name);
      toast.ok(`database "${name}" created`);
      onRefresh();
    } catch (e) {
      toast.err(e instanceof Error ? e.message : 'create failed');
    }
  }

  async function createGroup() {
    const db = prompt('Database (optional, blank for global):') || '';
    const name = prompt('New group name:');
    if (!name) return;
    try {
      await ddl.createGroup(name, db || undefined);
      toast.ok(`group "${name}" created`);
      onRefresh();
    } catch (e) {
      toast.err(e instanceof Error ? e.message : 'create failed');
    }
  }

  if (err) return <aside className="sidebar"><div className="empty">{err}</div></aside>;
  if (!tree) return <aside className="sidebar"><div className="empty">Loading…</div></aside>;

  // Build index structures the same way the old dashboard did:
  //   grpsByDb    — rendered under "Groups" of each DB
  //   vtByDbGrp   — rendered under each group
  //   ptByVt      — PTables attached to a given VTable
  const grpsByDb: Record<string, { name: string }[]> = {};
  (tree.groups ?? []).forEach(g => {
    const k = g.database ?? '';
    (grpsByDb[k] ??= []).push(g);
  });
  const vtByDbGrp: Record<string, { name: string; ncols: number }[]> = {};
  const vtByDb: Record<string, { name: string; group?: string }[]> = {};
  (tree.vtables ?? []).forEach(v => {
    const db = v.database ?? '';
    const key = `${db}${v.group ?? ''}`;
    (vtByDbGrp[key] ??= []).push(v);
    (vtByDb[db] ??= []).push(v);
  });
  const ptByVt: Record<string, { name: string }[]> = {};
  (tree.ptables ?? []).forEach(p => {
    const k = p.vtable ?? '';
    (ptByVt[k] ??= []).push(p);
  });
  const ptNames = new Set((tree.ptables ?? []).map(p => p.name));
  const orphanTables = (tree.tables ?? []).filter(t => !ptNames.has(t.name));

  const dbInfo = tree.db ?? {};

  const renderPtables = (vtName: string) => {
    const ps = ptByVt[vtName] ?? [];
    if (!ps.length) return <div className="empty">no ptables</div>;
    return ps.map(p => (
      <div key={p.name} className="node leaf"
           title={p.name}
           onDoubleClick={() => onSql(`SELECT * FROM ${p.name} LIMIT 1000`)}>
        <span className="caret"></span>
        <span className="ic p">P</span>
        <span className="nm">{p.name}</span>
        <span className="x" title="DROP PTABLE"
              onClick={ev => { ev.stopPropagation(); run('PTABLE', p.name, () => ddl.dropPTable(p.name)); }}>✕</span>
      </div>
    ));
  };

  const renderVtable = (v: { name: string; ncols: number }) => {
    const k = `vt:${v.name}`;
    const o = isOpen(k);
    const pc = (ptByVt[v.name] ?? []).length;
    return (
      <div key={v.name}>
        <div className="node" title={v.name}>
          <span className={`caret ${o ? 'open' : ''}`} onClick={() => toggle(k)}>▸</span>
          <span className="ic v" onClick={() => toggle(k)}>V</span>
          <span className="nm" onClick={() => toggle(k)}>{v.name}</span>
          <span className="count">{v.ncols}c · {pc}p</span>
          <span className="x" title="DROP VTABLE"
                onClick={ev => { ev.stopPropagation(); run('VTABLE', v.name, () => ddl.dropVTable(v.name)); }}>✕</span>
        </div>
        {o && <div className="kids">{renderPtables(v.name)}</div>}
      </div>
    );
  };

  const renderGroup = (dbName: string, gName: string) => {
    const key = `${dbName}${gName}`;
    const k = `gr:${key}`;
    const o = isOpen(k);
    const vs = vtByDbGrp[key] ?? [];
    const label = gName || '(no group)';
    return (
      <div key={gName || '∅'}>
        <div className="node" title={label}>
          <span className={`caret ${o ? 'open' : ''}`} onClick={() => toggle(k)}>▸</span>
          <span className="ic g" onClick={() => toggle(k)}>G</span>
          <span className="nm" onClick={() => toggle(k)}>{label}</span>
          <span className="count">{vs.length}v</span>
          {gName && (
            <span className="x" title="DROP GROUP"
                  onClick={ev => { ev.stopPropagation(); run('GROUP', gName, () => ddl.dropGroup(gName)); }}>✕</span>
          )}
        </div>
        {o && (
          <div className="kids">
            {!vs.length
              ? <div className="empty">no vtables</div>
              : vs.map(renderVtable)}
          </div>
        )}
      </div>
    );
  };

  const renderDb = (dbName: string, isOrphan: boolean, isProtected: boolean) => {
    const k = `db:${dbName}`;
    const o = isOpen(k);
    const gs = grpsByDb[dbName] ?? [];
    const vs = vtByDb[dbName] ?? [];
    const grpNames = new Set<string>();
    gs.forEach(g => grpNames.add(g.name));
    vs.forEach(v => grpNames.add(v.group ?? ''));
    const allGrps = [...grpNames].filter(Boolean).sort();
    const hasUngrouped = grpNames.has('') || grpNames.has(undefined as unknown as string);
    return (
      <div key={`db-${dbName || '∅'}`}>
        <div className="node" title={dbName || '(no db)'}>
          <span className={`caret ${o ? 'open' : ''}`} onClick={() => toggle(k)}>▸</span>
          <span className="ic" onClick={() => toggle(k)}>DB</span>
          <span className="nm" onClick={() => toggle(k)}>{dbName || '(no db)'}</span>
          <span className="count">
            {isProtected ? '🔒' : `${allGrps.length + (hasUngrouped ? 1 : 0)}g · ${vs.length}v`}
          </span>
          {!isOrphan && !isProtected && (
            <span className="x" title="DROP DATABASE"
                  onClick={ev => { ev.stopPropagation(); run('DATABASE', dbName, () => ddl.dropDatabase(dbName)); }}>✕</span>
          )}
        </div>
        {o && (
          <div className="kids">
            <div className="section-label">Groups ({allGrps.length + (hasUngrouped ? 1 : 0)})</div>
            {!allGrps.length && !hasUngrouped && <div className="empty">no groups</div>}
            {allGrps.map(g => renderGroup(dbName, g))}
            {hasUngrouped && renderGroup(dbName, '')}
          </div>
        )}
      </div>
    );
  };

  return (
    <aside className="sidebar">
      <div className="db-hdr">
        <div className="db-name">
          <span className="ic">NODE</span>
          <span>{dbInfo.name ?? '-'}</span>
        </div>
        <div className="db-meta">
          {dbInfo.host && <span>host: {dbInfo.host}</span>}
          {dbInfo.path && <span> · path: {dbInfo.path}</span>}
        </div>
        <div className="db-meta">
          {typeof dbInfo.uptime_s === 'number' && <span>up: {fmtSec(dbInfo.uptime_s)}</span>}
          {typeof dbInfo.disk_bytes === 'number' && <span> · disk: {fmtBytes(dbInfo.disk_bytes)}</span>}
          {tree.tables && <span> · tbls: {tree.tables.length}</span>}
        </div>
      </div>

      <div className="tool-row">
        <button onClick={createDb}>+ DB</button>
        <button onClick={createGroup}>+ Group</button>
        <button onClick={() => onSql('CREATE TABLE t (ts TIMESTAMP, v INT64) TIMESTAMP(ts)')}>+ Table</button>
        <button onClick={onRefresh} title="reload">↻</button>
      </div>

      <div className="section-label">Databases ({(tree.databases ?? []).length})</div>
      {(tree.databases ?? []).map(d => renderDb(d.name, false, !!d.protected))}

      {(grpsByDb[''] || vtByDb['']) && renderDb('', true, false)}

      {orphanTables.length > 0 && (
        <>
          <div className="section-label">Tables ({orphanTables.length})</div>
          {orphanTables.map(t => (
            <div key={t.name} className="node leaf"
                 title={t.name}
                 onDoubleClick={() => onSql(`SELECT * FROM ${t.name} LIMIT 1000`)}>
              <span className="caret"></span>
              <span className="ic t">T</span>
              <span className="nm">{t.name}</span>
              <span className="count">{t.ncols}c</span>
              <span className="x" title="DROP TABLE"
                    onClick={ev => { ev.stopPropagation(); run('TABLE', t.name, () => ddl.dropTable(t.name)); }}>✕</span>
            </div>
          ))}
        </>
      )}
    </aside>
  );
}

function fmtSec(s: number): string {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h`;
  return `${Math.floor(s / 86400)}d`;
}
function fmtBytes(b: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let u = 0;
  while (b >= 1024 && u < units.length - 1) { b /= 1024; u++; }
  return `${b.toFixed(b < 10 ? 2 : b < 100 ? 1 : 0)} ${units[u]}`;
}
