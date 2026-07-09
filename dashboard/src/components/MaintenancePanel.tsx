import { useState } from 'react';
import { api, CatalogOrphan, CatalogReport, errMsg } from '../api';
import { useToastCtx } from './Toasts';
import { useModalCtx } from './ModalCtx';
import { IconDownload, IconLock } from './icons';

/* Maintenance page: catalog consistency scan (read-only), retention
 * sweep + PITR (both MUTATING — POST only, guarded by confirm modals),
 * and the /backup tarball download.
 *
 * role="normal" locks the mutating actions client-side (lock hint +
 * disabled buttons); the server enforces RBAC regardless. */
export function MaintenancePanel({ role }: { role: 'admin' | 'normal' }) {
  const locked = role !== 'admin';
  return (
    <>
      <div className="page-head">
        <h2>Maintenance</h2>
        <span className="sub">consistency checks · retention · point-in-time recovery · backup</span>
      </div>
      <CatalogCheckCard />
      <div className="maint-grid">
        <RetentionCard locked={locked} />
        <PitrCard locked={locked} />
        <BackupCard />
      </div>
    </>
  );
}

/* ── Catalog consistency ──────────────────────────────────────────── */

function CatalogCheckCard() {
  const [report, setReport] = useState<CatalogReport | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');
  const [at, setAt] = useState('');

  async function run() {
    setBusy(true);
    setErr('');
    try {
      const r = await api.catalogCheck();
      setReport(r ?? {});
      setAt(new Date().toLocaleTimeString());
    } catch (e) {
      setErr(errMsg(e, 'catalog check failed'));
      setReport(null);
    } finally {
      setBusy(false);
    }
  }

  const cats: { key: keyof CatalogReport; label: string; why: (o: CatalogOrphan) => string }[] = [
    { key: 'orphan_groups', label: 'Orphan groups', why: o => `missing database "${o.missing_db ?? '?'}"` },
    {
      key: 'orphan_stables', label: 'Orphan stables',
      why: o => [
        o.missing_db ? `missing database "${o.missing_db}"` : '',
        o.missing_group ? `missing group "${o.missing_group}"` : '',
      ].filter(Boolean).join(' · ') || 'dangling reference',
    },
    { key: 'orphan_children', label: 'Orphan children', why: o => `missing stable "${o.missing_stable ?? '?'}"` },
    { key: 'orphan_dirs', label: 'Stranded directories', why: o => o.path ?? '' },
  ];

  const total = report
    ? cats.reduce((s, c) => s + (((report[c.key] as CatalogOrphan[] | undefined) ?? []).length), 0)
    : 0;
  const summary = report?.summary ?? {};

  return (
    <div className="card">
      <h3 className="card-title">
        Catalog consistency
        <span className="mu">read-only scan — catalog rows vs on-disk dirs</span>
        <span className="grow" />
        {report && !report.error && (
          <span className={`badge ${total > 0 ? 'warn' : 'ok'}`}>
            {total > 0 ? `${total} orphans` : 'Clean'}
          </span>
        )}
        <button className="primary" onClick={run} disabled={busy}>
          {busy ? <><span className="spinner" /> Scanning…</> : 'Run check'}
        </button>
      </h3>
      <p className="maint-desc">
        Walks every catalog row and data directory, reporting groups, stables
        and child tables whose parents are gone, plus directories the catalog
        no longer references. Nothing is modified.
      </p>

      {err && (
        <div className="alert err" role="alert">
          <div className="alert-body"><b>Scan failed.</b> {err}</div>
        </div>
      )}
      {report?.error && !err && (
        <div className="alert warn">
          <div className="alert-body">{report.error}</div>
        </div>
      )}

      {report && !err && !report.error && (
        <>
          <div className="stat-row">
            <MiniStat label="Databases" val={summary.databases} />
            <MiniStat label="Groups" val={summary.groups} />
            <MiniStat label="Stables" val={summary.stables} />
            <MiniStat label="Children" val={summary.children} />
          </div>
          {total === 0 ? (
            <div className="alert ok">
              <div className="alert-body">
                No referential orphans, no stranded directories.
                {at && ` Checked at ${at}.`}
              </div>
            </div>
          ) : (
            cats.map(c => {
              const items = (report[c.key] as CatalogOrphan[] | undefined) ?? [];
              if (!items.length) return null;
              return (
                <div key={String(c.key)} style={{ marginTop: 10 }}>
                  <div className="section-label">{c.label} ({items.length})</div>
                  <div className="orphan-list">
                    {items.map((o, i) => (
                      <div key={`${o.name}-${i}`} className="orphan-item">
                        <span>{o.name}</span>
                        <span className="why">{c.why(o)}</span>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })
          )}
        </>
      )}
      {!report && !err && !busy && (
        <div className="empty tight">Not scanned yet in this session.</div>
      )}
    </div>
  );
}

function MiniStat({ label, val }: { label: string; val?: number }) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className="stat-val">{val != null ? val.toLocaleString() : '—'}</div>
    </div>
  );
}

/* ── Retention sweep (MUTATING) ───────────────────────────────────── */

function RetentionCard({ locked }: { locked: boolean }) {
  const toast = useToastCtx();
  const modal = useModalCtx();
  const [busy, setBusy] = useState(false);
  const [last, setLast] = useState('');

  async function sweep() {
    const ok = await modal.confirm(
      'Run retention sweep',
      <>
        Runs the retention policy <b>now</b>: every partition wholly past its
        TTL is <b>deleted permanently</b> on this node.
        <br /><span className="mu">Expired data cannot be recovered afterwards.</span>
      </>,
      { danger: true, confirmText: 'Sweep now' },
    );
    if (!ok) return;
    setBusy(true);
    try {
      const r = await api.retentionSweep();
      if (r?.ok) {
        const n = r.partitions_deleted ?? 0;
        toast.ok(`retention sweep done — ${n} partition${n === 1 ? '' : 's'} deleted`);
        setLast(`${n} partitions deleted at ${new Date().toLocaleTimeString()}`);
      } else {
        toast.err('retention sweep reported failure');
      }
    } catch (e) {
      toast.err(errMsg(e, 'retention sweep failed'));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="card">
      <h3 className="card-title">
        Retention sweep
        <span className="grow" />
        {locked && (
          <span className="lock-hint" title="requires admin">
            <IconLock size={11} /> admin only
          </span>
        )}
        <span className="badge warn">Mutating</span>
      </h3>
      <p className="maint-desc">
        Applies table TTLs immediately instead of waiting for the background
        cycle. <code>POST /retention/sweep</code> — expired partitions are
        removed from disk.
      </p>
      <button className="danger" onClick={sweep} disabled={busy || locked}
              title={locked ? 'requires admin' : undefined}>
        {busy ? <><span className="spinner" /> Sweeping…</> : 'Run sweep…'}
      </button>
      {last && <div className="stat-sub" style={{ marginTop: 8 }}>Last run: {last}</div>}
    </div>
  );
}

/* ── PITR trim (MUTATING) ─────────────────────────────────────────── */

function parseTarget(input: string): { ns: string; human: string } | null {
  const s = input.trim();
  if (!s) return null;
  if (/^\d+$/.test(s)) {
    // Raw epoch nanoseconds — 19 digits for any modern date.
    if (s.length < 16 || s.length > 19) return null;
    try {
      const ms = Number(BigInt(s) / 1000000n);
      if (!Number.isFinite(ms) || ms <= 0) return null;
      return { ns: s, human: new Date(ms).toISOString() };
    } catch {
      return null;
    }
  }
  const ms = Date.parse(s);
  if (Number.isNaN(ms) || ms <= 0) return null;
  return { ns: `${ms}000000`, human: new Date(ms).toISOString() };
}

function PitrCard({ locked }: { locked: boolean }) {
  const toast = useToastCtx();
  const modal = useModalCtx();
  const [busy, setBusy] = useState(false);
  const [last, setLast] = useState('');

  async function trim() {
    const v = await modal.form('Point-in-time recovery', [
      {
        name: 'ts',
        label: 'Roll forward to (ISO datetime or epoch nanoseconds)',
        placeholder: '2026-07-01T12:00:00Z  ·  or  1782… (19-digit ns)',
      },
    ], {
      submitText: 'Continue',
      body: (
        <span className="mu">
          Intended immediately after extracting a <code>/backup</code> tarball:
          trims every partition that starts <b>after</b> the target instant so
          the restored node stops at an exact point in time.
        </span>
      ),
    });
    if (!v) return;
    const target = parseTarget(v.ts ?? '');
    if (!target) {
      toast.err('invalid target — use an ISO datetime or 19-digit epoch nanoseconds');
      return;
    }
    const ok = await modal.confirm(
      'Confirm PITR trim',
      <>
        Every partition starting after
        <br /><code>{target.human}</code> <span className="mu">(ts={target.ns} ns)</span>
        <br />will be <b>permanently removed</b> across all tables on this node.
      </>,
      { danger: true, confirmText: 'Trim partitions' },
    );
    if (!ok) return;
    setBusy(true);
    try {
      const r = await api.pitr(target.ns);
      if (r?.ok) {
        const n = r.partitions_removed ?? 0;
        toast.ok(`PITR done — ${n} partition${n === 1 ? '' : 's'} removed`);
        setLast(`${n} partitions removed (→ ${target.human})`);
      } else {
        toast.err('PITR reported failure');
      }
    } catch (e) {
      toast.err(errMsg(e, 'PITR failed'));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="card">
      <h3 className="card-title">
        Point-in-time recovery
        <span className="grow" />
        {locked && (
          <span className="lock-hint" title="requires admin">
            <IconLock size={11} /> admin only
          </span>
        )}
        <span className="badge bad">Destructive</span>
      </h3>
      <p className="maint-desc">
        <code>POST /pitr?ts=&lt;ns&gt;</code> — drops partitions newer than the
        target instant. Use only right after restoring from a backup tarball.
      </p>
      <button className="danger" onClick={trim} disabled={busy || locked}
              title={locked ? 'requires admin' : undefined}>
        {busy ? <><span className="spinner" /> Trimming…</> : 'Roll to instant…'}
      </button>
      {last && <div className="stat-sub" style={{ marginTop: 8 }}>Last run: {last}</div>}
    </div>
  );
}

/* ── Backup download ──────────────────────────────────────────────── */

function BackupCard() {
  return (
    <div className="card">
      <h3 className="card-title">
        Backup
        <span className="grow" />
        <span className="badge ok">Read-only</span>
      </h3>
      <p className="maint-desc">
        Streams a consistent <code>tar.gz</code> of the data directory —
        WAL, partitions and catalog — without pausing writers. Restore by
        extracting into a fresh data dir, then optionally PITR-trim.
      </p>
      <a className="chip" href="/backup" download="tsdb-backup.tar.gz"
         style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
        <IconDownload size={14} /> Download /backup
      </a>
    </div>
  );
}
