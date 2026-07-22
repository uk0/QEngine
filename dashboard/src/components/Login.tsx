import { FormEvent, useState } from 'react';
import { api, errMsg, HealthInfo } from '../api';
import { usePoll } from '../hooks';

interface Props {
  onOk: (user: string) => void;
  /** Last-used username (App restores it from localStorage). */
  initialUser?: string;
  /** Non-auth boot problem (server unreachable) surfaced above the form. */
  bootErr?: string;
}

export function Login({ onOk, initialUser = 'root', bootErr = '' }: Props) {
  const [user, setUser] = useState(initialUser);
  const [pass, setPass] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const [shake, setShake] = useState(false);

  /* Footer status via public /health — probes every 10s while parked. */
  const [health, setHealth] = useState<HealthInfo | null>(null);
  const [probed, setProbed] = useState(false);
  usePoll(async () => {
    try {
      const h = await api.health();
      setHealth(h ?? null);
    } catch {
      setHealth(null);
    } finally {
      setProbed(true);
    }
  }, 10_000);

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
      setShake(true);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="login-viewport">
      {/* CSS-only ambience: faint graph grid + drifting signal traces. */}
      <div className="login-bg" aria-hidden="true">
        <svg className="login-wave" viewBox="0 0 1440 360" preserveAspectRatio="none" focusable="false">
          <path className="w1" d="M0,240 C120,180 200,300 320,250 C440,200 500,120 640,170 C780,220 830,300 960,260 C1090,220 1150,140 1280,180 C1360,205 1400,230 1440,215" />
          <path className="w2" d="M0,300 C160,260 260,330 400,295 C540,260 620,200 760,235 C900,270 980,330 1120,300 C1260,270 1350,230 1440,255" />
          <path className="w3" d="M0,180 C140,140 240,220 380,190 C520,160 600,90 740,120 C880,150 970,230 1110,205 C1250,180 1350,120 1440,145" />
        </svg>
      </div>

      <div className="login-col">
        <div className="login-identity">
          <span className="login-mark"><img src="/logo.png" alt="QEngine" className="login-mark-img" /></span>
          <span>
            <span className="login-word">QEngine</span>
            <span className="login-tag">time-series console</span>
          </span>
        </div>

        <form
          className={`login-card ${shake ? 'shake' : ''}`}
          onSubmit={submit}
          onAnimationEnd={() => setShake(false)}
        >
          {bootErr && (
            <div className="alert warn" style={{ marginBottom: 14 }}>
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
            spellCheck={false}
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
            className="primary login-btn"
            disabled={busy || !user.trim()}
          >
            {busy ? <><span className="spinner" /> Signing in…</> : 'Sign in'}
          </button>
          <div className="login-err" role="alert" aria-live="polite">
            {err && (
              <div className="alert err">
                <div className="alert-body">{err}</div>
              </div>
            )}
          </div>
          <div className="login-foot">cookie session · TSDB_DASHBOARD_AUTH</div>
        </form>

        <div className="login-status">
          {health ? (
            <>
              <span className="status-dot on" />
              cluster reachable · up {fmtUp(health.uptime_s)}
            </>
          ) : probed ? (
            <>
              <span className="status-dot off" />
              cluster unreachable — /health not responding
            </>
          ) : (
            <>
              <span className="status-dot idle" />
              probing /health…
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function fmtUp(s?: number): string {
  if (s == null || !Number.isFinite(s) || s < 0) return '—';
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}
