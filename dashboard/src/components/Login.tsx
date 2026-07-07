import { FormEvent, useState } from 'react';
import { api, errMsg } from '../api';

interface Props {
  onOk: (user: string) => void;
  initialUser?: string;
  /** Non-auth boot problem (server unreachable) surfaced above the form. */
  bootErr?: string;
}

export function Login({ onOk, initialUser = 'root', bootErr = '' }: Props) {
  const [user, setUser] = useState(initialUser);
  const [pass, setPass] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);

  async function submit(ev: FormEvent) {
    ev.preventDefault();
    if (busy) return;
    setErr('');
    setBusy(true);
    try {
      await api.login(user.trim(), pass);
      // Trust the cookie — re-probe a gated endpoint before entering.
      await api.tree();
      onOk(user.trim());
    } catch (e) {
      setErr(errMsg(e, 'login failed'));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="login-wrap">
      <form className="login-card" onSubmit={submit}>
        <div className="login-brand">
          <span className="brand-mark">ts</span>
          <h1>tsdb</h1>
        </div>
        <p className="login-sub">operator console</p>

        {bootErr && (
          <div className="alert warn" style={{ marginBottom: 12 }}>
            <div className="alert-body">
              Server not reachable at boot: {bootErr}. Check the node, then sign in.
            </div>
          </div>
        )}

        <label htmlFor="login-user">User</label>
        <input
          id="login-user"
          value={user}
          onChange={e => setUser(e.target.value)}
          autoComplete="username"
          autoFocus
        />
        <label htmlFor="login-pass">Password</label>
        <input
          id="login-pass"
          type="password"
          value={pass}
          onChange={e => setPass(e.target.value)}
          autoComplete="current-password"
        />
        <button
          className="primary"
          style={{ width: '100%', marginTop: 18, padding: '8px 12px' }}
          disabled={busy || !user.trim()}
        >
          {busy ? <><span className="spinner" /> Signing in…</> : 'Sign in'}
        </button>
        <div className="err" role="alert">
          {err && <span className="alert err" style={{ padding: '6px 10px', display: 'flex' }}>
            <span className="alert-body">{err}</span>
          </span>}
        </div>
        <div className="login-foot">cookie session · TSDB_DASHBOARD_AUTH</div>
      </form>
    </div>
  );
}
