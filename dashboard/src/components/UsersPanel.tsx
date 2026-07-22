import { useCallback, useEffect, useState } from 'react';
import { api, errMsg, SqlResult } from '../api';
import { useToastCtx } from './Toasts';
import { useModalCtx } from './ModalCtx';
import { IconRefresh, IconUsers } from './icons';

/* Users & access page (admin-only entry in the sidebar).
 *
 * Everything goes through the existing /sql endpoint:
 *   LIST USERS                                    → account table
 *   CREATE USER <n> IDENTIFIED BY '<pw>' [ROLE …] → create
 *   ALTER USER <n> SET PASSWORD '<pw>'            → rotate password
 *   DROP USER <n>                                 → remove
 * The server enforces per-statement RBAC regardless of this UI. */

interface Props {
  /** Signed-in username — used to flag "you" and warn on self-drop. */
  selfUser: string;
}

/* tsdb identifiers: letters/digits/_/. — same shape the parser accepts.
 * Validating client-side keeps interpolated names injection-safe. */
const NAME_RE = /^[A-Za-z_][A-Za-z0-9_.]*$/;

/** SQL string literal — single quotes escaped by doubling. */
function q(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}

interface UserRow {
  name: string;
  role: string;
  created: unknown;
}

export function UsersPanel({ selfUser }: Props) {
  const [rows, setRows] = useState<UserRow[]>([]);
  const [err, setErr] = useState('');
  const [loading, setLoading] = useState(true);
  const toast = useToastCtx();
  const modal = useModalCtx();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await api.sql('LIST USERS');
      setRows(parseUsers(r));
      setErr('');
    } catch (e) {
      setErr(errMsg(e, 'LIST USERS failed'));
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  async function createUser() {
    const v = await modal.form('Create user', [
      { name: 'name', label: 'Username', placeholder: 'e.g. metrics_ro' },
      { name: 'pw', label: 'Password', type: 'password', placeholder: 'required' },
      { name: 'role', label: 'Role', type: 'select', options: ['normal', 'admin'] },
    ], {
      submitText: 'Create user',
      body: (
        <span className="mu">
          <code>normal</code> users can query but the server rejects
          admin-only statements; <code>admin</code> grants full control.
        </span>
      ),
    });
    if (!v) return;
    const name = (v.name ?? '').trim();
    if (!name) return;
    if (!NAME_RE.test(name)) {
      toast.err('invalid username — letters, digits, _ and . only (must not start with a digit)');
      return;
    }
    if (!v.pw) {
      toast.err('password required');
      return;
    }
    const admin = v.role === 'admin';
    try {
      await api.sql(`CREATE USER ${name} IDENTIFIED BY ${q(v.pw)}${admin ? ' ROLE admin' : ''}`);
      toast.ok(`user "${name}" created${admin ? ' (admin)' : ''}`);
      load();
    } catch (e) {
      toast.err(errMsg(e, 'create user failed'));
    }
  }

  async function changePassword(name: string) {
    if (!NAME_RE.test(name)) { toast.err('unmanageable username'); return; }
    const v = await modal.form(`Change password — ${name}`, [
      { name: 'pw', label: 'New password', type: 'password', placeholder: 'required' },
    ], { submitText: 'Change password' });
    if (!v) return;
    if (!v.pw) {
      toast.err('password required');
      return;
    }
    try {
      await api.sql(`ALTER USER ${name} SET PASSWORD ${q(v.pw)}`);
      toast.ok(`password changed for "${name}"`);
    } catch (e) {
      toast.err(errMsg(e, 'password change failed'));
    }
  }

  async function dropUser(name: string) {
    if (!NAME_RE.test(name)) { toast.err('unmanageable username'); return; }
    const ok = await modal.confirm(
      'Drop user',
      <>
        Drop user <code>{name}</code>?
        {name === selfUser && (
          <><br /><b>This is the account you are signed in with.</b></>
        )}
        <br /><span className="mu">Its sessions stop authenticating. This cannot be undone.</span>
      </>,
      { danger: true, confirmText: 'Drop user' },
    );
    if (!ok) return;
    try {
      await api.sql(`DROP USER ${name}`);
      toast.ok(`user "${name}" dropped`);
      load();
    } catch (e) {
      toast.err(errMsg(e, 'drop user failed'));
    }
  }

  return (
    <>
      <div className="page-head">
        <h2>Users &amp; access</h2>
        <span className="sub">RBAC accounts · server enforces permissions per statement</span>
      </div>

      <div className="card">
        <h3 className="card-title">
          <IconUsers size={15} />
          Users
          <span className="mu">via <code>LIST USERS</code></span>
          <span className="grow" />
          <span className="badge mut">{rows.length} account{rows.length === 1 ? '' : 's'}</span>
          <button className="ghost" onClick={load} disabled={loading}
                  title="Reload user list" aria-label="Reload user list">
            <IconRefresh size={13} />
          </button>
          <button className="primary" onClick={createUser}>Create user…</button>
        </h3>

        {err && (
          <div className="alert err" role="alert">
            <div className="alert-body"><b>Load failed.</b> {err}</div>
          </div>
        )}
        {!err && loading && !rows.length && (
          <div className="empty"><span className="spinner" /> Loading users…</div>
        )}
        {!err && !loading && !rows.length && (
          <div className="empty">No users reported — create one above.</div>
        )}

        {rows.length > 0 && (
          <div className="result">
            <table className="rows">
              <thead>
                <tr>
                  <th>User</th>
                  <th>Role</th>
                  <th>Created</th>
                  <th aria-label="Actions" />
                </tr>
              </thead>
              <tbody>
                {rows.map(u => {
                  const admin = u.role.toLowerCase() === 'admin';
                  return (
                    <tr key={u.name}>
                      <td>
                        <span className="user-cell">
                          <span className="avatar">{u.name.slice(0, 2)}</span>
                          <span>{u.name}</span>
                          {u.name === selfUser && <span className="self-chip">you</span>}
                        </span>
                      </td>
                      <td>
                        <span className={`pill ${admin ? 'acc' : 'mut'}`}>
                          {u.role.toLowerCase() || '—'}
                        </span>
                      </td>
                      <td className="mu">{fmtTs(u.created)}</td>
                      <td>
                        <span className="row-actions">
                          <button onClick={() => changePassword(u.name)}>Password…</button>
                          <button className="danger" onClick={() => dropUser(u.name)}>Drop…</button>
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </>
  );
}

/* Map LIST USERS rows by column name — never assume column order. */
function parseUsers(r: SqlResult): UserRow[] {
  const cols = (r.cols ?? []).map(c => String(c).toLowerCase());
  const iName = cols.indexOf('name');
  const iRole = cols.indexOf('role');
  const iCreated = cols.indexOf('created_at');
  return (r.rows ?? [])
    .map(row => ({
      name: String(row?.[iName >= 0 ? iName : 0] ?? ''),
      role: String(row?.[iRole >= 0 ? iRole : 1] ?? ''),
      created: row?.[iCreated >= 0 ? iCreated : 2],
    }))
    .filter(u => u.name);
}

/* created_at arrives as an epoch integer whose unit depends on server
 * build (ns on current servers).  Pick by magnitude; strings pass through. */
function fmtTs(v: unknown): string {
  if (v == null) return '—';
  if (typeof v === 'number' && Number.isFinite(v)) {
    if (v <= 0) return '—';
    let ms = v;
    if (v > 1e17) ms = v / 1e6;        // nanoseconds
    else if (v > 1e14) ms = v / 1e3;   // microseconds
    else if (v > 1e11) ms = v;         // milliseconds
    else ms = v * 1000;                // seconds
    const d = new Date(ms);
    if (!Number.isNaN(d.getTime())) return d.toLocaleString();
  }
  return String(v);
}
