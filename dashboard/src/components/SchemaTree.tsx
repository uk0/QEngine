import { useCallback, useEffect, useState } from 'react';
import { api, ddl, errMsg, Tree, ClusterInfo } from '../api';
import { useToastCtx } from './Toasts';
import { useModalCtx } from './ModalCtx';
import { IconRefresh } from './icons';

interface Props {
  bump: number;
  onSql: (q: string) => void;
  onRefresh: () => void;
  cluster: ClusterInfo | null;
}

/* sysdb is the protected system database — it owns no user tables but
 * surfaces cluster / RBAC state.  Hang these system views under a
 * synthetic "default" group so sysdb is never an empty locked node. */
const SYSDB_VIEWS: { name: string; q: string }[] = [
  { name: 'users', q: 'LIST USERS;' },
  { name: 'masters', q: 'LIST MASTERS;' },
  { name: 'functions', q: 'LIST FUNCTIONS;' },
];

export function SchemaTree({ bump, onSql, onRefresh, cluster }: Props) {
  const [tree, setTree] = useState<Tree | null>(null);
  const [err, setErr] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const toast = useToastCtx();
  const modal = useModalCtx();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const t = await api.tree();
      setTree(t ?? {});
      setErr('');
    } catch (e) {
      setErr(errMsg(e, 'tree load failed'));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load, bump]);

  const toggle = (key: string) => setOpen(o => ({ ...o, [key]: !o[key] }));
  const isOpen = (key: string) => !!open[key];

  /** Confirm a drop via modal, then run the SQL and toast the outcome. */
  async function drop(kind: string, name: string, runFn: () => Promise<unknown>) {
    const ok = await modal.confirm(
      `Drop ${kind}`,
      <>Drop <b>{kind}</b> <code>{name}</code>?<br />
        <span className="mu">This cannot be undone.</span></>,
      { danger: true, confirmText: `Drop ${kind}` },
    );
    if (!ok) return;
    try {
      await runFn();
      toast.ok(`${kind} "${name}" dropped`);
      onRefresh();
    } catch (e) {
      toast.err(errMsg(e, `drop ${kind} failed`));
    }
  }

  async function createDbRoot() {
    const v = await modal.form('Create database', [
      { name: 'name', label: 'Database name', placeholder: 'e.g. prod' },
      { name: 'description', label: 'Description (optional)', placeholder: 'free-form text' },
    ]);
    if (!v || !v.name) return;
    try {
      await ddl.createDatabase(v.name, v.description);
      toast.ok(`database "${v.name}" created`);
      onRefresh();
    } catch (e) {
      toast.err(errMsg(e, 'create failed'));
    }
  }

  async function createGroupUnder(dbName: string) {
    const v = await modal.form('Create group', [
      { name: 'db', label: 'Parent database', value: dbName, readonly: true },
      { name: 'name', label: 'Group name', placeholder: 'e.g. ops' },
    ]);
    if (!v || !v.name) return;
    try {
      await ddl.createGroup(v.name, dbName || undefined);
      toast.ok(`group "${v.name}" created`);
      onRefresh();
    } catch (e) {
      toast.err(errMsg(e, 'create failed'));
    }
  }

  /* Columns + TAGS can't sensibly be collected in a modal — load a
   * fully-scoped CREATE STABLE template into the SQL editor instead. */
  async function createVTableIn(dbName: string, gName: string) {
    const v = await modal.form('Create VTable (STable)', [
      { name: 'db', label: 'Database', value: dbName, readonly: true },
      { name: 'group', label: 'Group', value: gName || '(none)', readonly: true },
      { name: 'name', label: 'VTable name', placeholder: 'e.g. cpu_vt' },
    ], {
      submitText: 'Load template',
      body: (
        <span className="mu">
          A CREATE STABLE template will be loaded into the SQL editor
          with columns + TAGS you can tweak; child PTables are created
          by clicking <b>+</b> on the resulting VTable row.
        </span>
      ),
    });
    if (!v || !v.name) return;
    const scope = [
      dbName ? `IN DATABASE ${dbName}` : '',
      gName ? `IN GROUP ${gName}` : '',
    ].filter(Boolean).join(' ');
    const tmpl = `CREATE STABLE ${v.name} (
  ts TIMESTAMP,
  usage FLOAT64
)
TAGS (host SYMBOL)
${scope};`;
    onSql(tmpl);
    toast.ok('VTable template loaded — edit columns/tags, then Run');
  }

  /* Child table bound to the parent via USING + TAGS(...). */
  async function createPTableIn(vtable: {
    name: string; ncols: number; tags?: { name: string; type?: string }[];
  }) {
    const tags = vtable.tags ?? [];
    if (tags.length === 1) {
      const t = tags[0];
      const v = await modal.form('Create PTable (child table)', [
        { name: 'vt', label: 'Parent VTable', value: vtable.name, readonly: true },
        { name: 'name', label: 'PTable name', placeholder: `e.g. ${vtable.name}_host01` },
        {
          name: 'tag', label: `Tag value for ${t.name} (${t.type ?? 'SYMBOL'})`,
          placeholder: t.type === 'INT64' ? '42' : "'host01'",
        },
      ]);
      if (!v || !v.name || !v.tag) return;
      try {
        await api.sql(`CREATE TABLE ${v.name} USING ${vtable.name} TAGS (${v.tag})`);
        toast.ok(`PTable "${v.name}" created under ${vtable.name}`);
        onRefresh();
      } catch (e) {
        toast.err(errMsg(e, 'create failed'));
      }
      return;
    }
    const v = await modal.form('Create PTable (child table)', [
      { name: 'vt', label: 'Parent VTable', value: vtable.name, readonly: true },
      { name: 'name', label: 'PTable name', placeholder: `e.g. ${vtable.name}_host01` },
    ], {
      submitText: 'Load template',
      body: (
        <span className="mu">
          TAGS are positional — order must match the VTable's TAGS
          declaration. Template includes placeholders for you to fill in.
        </span>
      ),
    });
    if (!v || !v.name) return;
    const placeholder = tags.length
      ? tags.map(t => (t.type === 'INT64' ? '0' : `'${t.name}_value'`)).join(', ')
      : "'tag_value'";
    onSql(`CREATE TABLE ${v.name} USING ${vtable.name} TAGS (${placeholder});`);
    toast.ok('PTable template loaded — fill TAG values, then Run');
  }

  /* ── Render states ─────────────────────────────────────────────── */

  if (err) {
    return (
      <aside className="tree-card">
        <div className="alert err"><div className="alert-body">{err}</div></div>
        <div style={{ marginTop: 10 }}>
          <button onClick={load}>Retry</button>
        </div>
      </aside>
    );
  }
  if (!tree) {
    return (
      <aside className="tree-card">
        <div className="empty"><span className="spinner" /> Loading schema…</div>
      </aside>
    );
  }

  /* ── Index structures ──────────────────────────────────────────── */

  const grpsByDb: Record<string, { name: string }[]> = {};
  (tree.groups ?? []).forEach(g => {
    const k = g?.database ?? '';
    (grpsByDb[k] ??= []).push(g);
  });
  const vtByDbGrp: Record<string, { name: string; ncols: number }[]> = {};
  const vtByDb: Record<string, { name: string; group?: string }[]> = {};
  (tree.vtables ?? []).forEach(v => {
    const db = v?.database ?? '';
    const key = `${db}${v?.group ?? ''}`;
    (vtByDbGrp[key] ??= []).push(v);
    (vtByDb[db] ??= []).push(v);
  });
  const ptByVt: Record<string, { name: string }[]> = {};
  (tree.ptables ?? []).forEach(p => {
    const k = p?.vtable ?? '';
    (ptByVt[k] ??= []).push(p);
  });
  const ptNames = new Set((tree.ptables ?? []).map(p => p.name));
  const orphanTables = (tree.tables ?? []).filter(t => !ptNames.has(t.name));

  const dbInfo = tree.db ?? {};

  /* Cluster-aware header: aggregate per-node disk into a cluster total
   * so the header reflects the whole cluster, not just this node. */
  const nodes = cluster?.nodes ?? [];
  const nodeCount = nodes.length;
  const isCluster = nodeCount > 1;
  const aliveCount = nodes.filter(n => n?.state === 'ALIVE').length;
  const totalDisk = nodes.reduce((s, n) => s + (n?.disk_bytes ?? 0), 0);

  const renderPtables = (vtName: string) => {
    const ps = ptByVt[vtName] ?? [];
    if (!ps.length) return <div className="empty tight">no ptables</div>;
    return ps.map(p => (
      <div key={p.name} className="node leaf" title={p.name}
           onDoubleClick={() => onSql(`SELECT * FROM ${p.name} LIMIT 1000`)}>
        <span className="caret"></span>
        <span className="ic p">P</span>
        <span className="nm">{p.name}</span>
        <span className="x" title="DROP PTABLE"
              onClick={ev => { ev.stopPropagation(); drop('PTABLE', p.name, () => ddl.dropPTable(p.name)); }}>✕</span>
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
          <span className="plus" title="Create PTable under this VTable"
                onClick={ev => { ev.stopPropagation(); createPTableIn(v); }}>+</span>
          <span className="x" title="DROP VTABLE"
                onClick={ev => { ev.stopPropagation(); drop('VTABLE', v.name, () => ddl.dropVTable(v.name)); }}>✕</span>
        </div>
        {o && <div className="kids">{renderPtables(v.name)}</div>}
      </div>
    );
  };

  const renderGroup = (dbName: string, gName: string) => {
    const key = `${dbName}${gName}`;
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
          <span className="plus" title="Create VTable (STable) here"
                onClick={ev => { ev.stopPropagation(); createVTableIn(dbName, gName); }}>+</span>
          {gName && (
            <span className="x" title="DROP GROUP"
                  onClick={ev => { ev.stopPropagation(); drop('GROUP', gName, () => ddl.dropGroup(gName)); }}>✕</span>
          )}
        </div>
        {o && (
          <div className="kids">
            {!vs.length
              ? <div className="empty tight">no vtables</div>
              : vs.map(renderVtable)}
          </div>
        )}
      </div>
    );
  };

  const renderSysGroup = () => (
    <div key="sysdb-default">
      <div className="node" title="default — system views (cluster / RBAC)">
        <span className="caret open">▸</span>
        <span className="ic g">G</span>
        <span className="nm">default</span>
        <span className="count">{SYSDB_VIEWS.length}v</span>
      </div>
      <div className="kids">
        {SYSDB_VIEWS.map(s => (
          <div key={s.name} className="node leaf" title={s.q}
               onDoubleClick={() => onSql(s.q)}>
            <span className="caret"></span>
            <span className="ic v">V</span>
            <span className="nm">{s.name}</span>
            <span className="count">sys</span>
          </div>
        ))}
      </div>
    </div>
  );

  const renderDb = (dbName: string, isOrphan: boolean, isProtected: boolean) => {
    const k = `db:${dbName}`;
    const o = isOpen(k);
    const gs = grpsByDb[dbName] ?? [];
    const vs = vtByDb[dbName] ?? [];
    const grpNames = new Set<string>();
    gs.forEach(g => grpNames.add(g.name));
    vs.forEach(v => grpNames.add(v.group ?? ''));
    const allGrps = [...grpNames].filter(Boolean).sort();
    const hasUngrouped = grpNames.has('');
    return (
      <div key={`db-${dbName || '∅'}`}>
        <div className="node" title={dbName || '(no db)'}>
          <span className={`caret ${o ? 'open' : ''}`} onClick={() => toggle(k)}>▸</span>
          <span className="ic" onClick={() => toggle(k)}>DB</span>
          <span className="nm" onClick={() => toggle(k)}>{dbName || '(no db)'}</span>
          <span className="count">
            {isProtected ? 'sys' : `${allGrps.length + (hasUngrouped ? 1 : 0)}g · ${vs.length}v`}
          </span>
          {!isOrphan && !isProtected && (
            <>
              <span className="plus" title="Create group here"
                    onClick={ev => { ev.stopPropagation(); createGroupUnder(dbName); }}>+</span>
              <span className="x" title="DROP DATABASE"
                    onClick={ev => { ev.stopPropagation(); drop('DATABASE', dbName, () => ddl.dropDatabase(dbName)); }}>✕</span>
            </>
          )}
        </div>
        {o && (
          <div className="kids">
            <div className="section-label">
              Groups ({allGrps.length + (hasUngrouped ? 1 : 0) + (dbName === 'sysdb' ? 1 : 0)})
            </div>
            {!allGrps.length && !hasUngrouped && dbName !== 'sysdb' &&
              <div className="empty tight">no groups</div>}
            {allGrps.map(g => renderGroup(dbName, g))}
            {hasUngrouped && renderGroup(dbName, '')}
            {dbName === 'sysdb' && renderSysGroup()}
          </div>
        )}
      </div>
    );
  };

  return (
    <aside className="tree-card">
      <div className="db-hdr">
        <div className="db-name">
          <span className="ic hi">{isCluster ? 'CLUSTER' : 'NODE'}</span>
          <span>{dbInfo.name ?? '—'}</span>
          {loading && <span className="spinner" />}
        </div>
        <div className="db-meta">
          {dbInfo.host && <span>{isCluster ? 'via' : 'host'}: {dbInfo.host}</span>}
          {isCluster && <span> · {aliveCount}/{nodeCount} nodes</span>}
          {dbInfo.path && <span> · path: {dbInfo.path}</span>}
        </div>
        <div className="db-meta">
          {typeof dbInfo.uptime_s === 'number' && <span>up: {fmtSec(dbInfo.uptime_s)}</span>}
          {isCluster && totalDisk > 0
            ? <span> · disk: {fmtBytes(totalDisk)} across {nodeCount} nodes</span>
            : typeof dbInfo.disk_bytes === 'number' && <span> · disk: {fmtBytes(dbInfo.disk_bytes)}</span>}
          {tree.tables && <span> · tbls: {tree.tables.length}</span>}
        </div>
      </div>

      <div className="tool-row">
        <button onClick={createDbRoot}>+ Database</button>
        <button className="ghost" onClick={onRefresh} title="Reload schema" aria-label="Reload schema">
          <IconRefresh size={13} />
        </button>
      </div>

      <div className="section-label">
        Databases ({(tree.databases ?? []).length + (orphanTables.length > 0 ? 1 : 0)})
      </div>
      {(tree.databases ?? []).map(d => renderDb(d.name, false, !!d.protected))}
      {!((tree.databases ?? []).length) && !orphanTables.length && (
        <div className="empty tight">no databases yet — create one above</div>
      )}

      {(grpsByDb[''] || vtByDb['']) && renderDb('', true, false)}

      {/* Normal tables (no database) hang under a synthetic "default" DB. */}
      {orphanTables.length > 0 && (() => {
        const k = 'db:__default__';
        const o = isOpen(k);
        return (
          <div key="db-__default__">
            <div className="node" title="default — normal tables (no database)">
              <span className={`caret ${o ? 'open' : ''}`} onClick={() => toggle(k)}>▸</span>
              <span className="ic" onClick={() => toggle(k)}>DB</span>
              <span className="nm" onClick={() => toggle(k)}>default</span>
              <span className="count">{orphanTables.length}t</span>
            </div>
            {o && (
              <div className="kids">
                <div className="section-label">Normal tables ({orphanTables.length})</div>
                {orphanTables.map(t => (
                  <div key={t.name} className="node leaf" title={t.name}
                       onDoubleClick={() => onSql(`SELECT * FROM ${t.name} LIMIT 1000`)}>
                    <span className="caret"></span>
                    <span className="ic t">T</span>
                    <span className="nm">{t.name}</span>
                    <span className="count">{t.ncols}c</span>
                    <span className="x" title="DROP TABLE"
                          onClick={ev => { ev.stopPropagation(); drop('TABLE', t.name, () => ddl.dropTable(t.name)); }}>✕</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })()}
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
