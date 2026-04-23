import { FormEvent, useState } from 'react';
import { api } from '../api';

export function Login({ onOk }: { onOk: () => void }) {
  const [user, setUser] = useState('root');
  const [pass, setPass] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);

  async function submit(ev: FormEvent) {
    ev.preventDefault();
    setErr('');
    setBusy(true);
    try {
      await api.login(user, pass);
      // Trust the cookie — re-probe via /tree; on success parent flips auth.
      await api.tree();
      onOk();
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'login failed');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="login-wrap">
      <form className="login-card" onSubmit={submit}>
        <h1>tsdb</h1>
        <p>console login</p>
        <label>User</label>
        <input value={user} onChange={e => setUser(e.target.value)} autoFocus />
        <label>Password</label>
        <input type="password" value={pass} onChange={e => setPass(e.target.value)} />
        <button className="primary" style={{ width: '100%', marginTop: 16 }} disabled={busy}>
          {busy ? 'Signing in…' : 'Sign in'}
        </button>
        <div className="err">{err}</div>
      </form>
    </div>
  );
}
