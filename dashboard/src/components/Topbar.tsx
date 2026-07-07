import { ClusterInfo, RaftInfo } from '../api';
import { IconSun, IconMoon, IconLogout, IconMenu } from './icons';

interface Props {
  title: string;
  cluster: ClusterInfo | null;
  raft: RaftInfo | null;
  clusterErr: string;
  theme: 'light' | 'dark';
  onTheme: () => void;
  onNav: () => void;
  user: string;
  onLogout: () => void;
  onHealthClick: () => void;
}

export function Topbar({
  title, cluster, raft, clusterErr, theme, onTheme, onNav,
  user, onLogout, onHealthClick,
}: Props) {
  return (
    <header className="topbar">
      <button className="icon-btn" onClick={onNav} title="Toggle navigation" aria-label="Toggle navigation">
        <IconMenu />
      </button>
      <span className="page-title">{title}</span>
      <span className="spacer" />
      <HealthPill cluster={cluster} raft={raft} err={clusterErr} onClick={onHealthClick} />
      <button
        className="icon-btn"
        onClick={onTheme}
        title={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
        aria-label="Toggle theme"
      >
        {theme === 'dark' ? <IconSun /> : <IconMoon />}
      </button>
      <span className="user-chip">
        <span className="avatar">{(user || '?').slice(0, 2)}</span>
        {user && <span className="user-name">{user}</span>}
      </span>
      <button className="icon-btn" onClick={onLogout} title="Sign out" aria-label="Sign out">
        <IconLogout />
      </button>
    </header>
  );
}

/* Live cluster-health pill — reads the shared 4s /cluster + /raft poll. */
function HealthPill({ cluster, raft, err, onClick }: {
  cluster: ClusterInfo | null;
  raft: RaftInfo | null;
  err: string;
  onClick: () => void;
}) {
  let cls: 'ok' | 'warn' | 'bad' | 'mut' = 'mut';
  let label = 'connecting';
  let detail = '';

  if (err) {
    cls = 'bad';
    label = 'offline';
    detail = err;
  } else if (cluster) {
    const nodes = cluster.nodes ?? [];
    if (nodes.length === 0) {
      cls = 'ok';
      label = 'standalone';
    } else {
      const alive = nodes.filter(n => n?.state === 'ALIVE').length;
      const dead = nodes.filter(n => n?.state === 'DEAD').length;
      cls = dead > 0 ? 'bad' : alive < nodes.length ? 'warn' : 'ok';
      label = `${alive}/${nodes.length} alive`;
    }
    if (raft?.role) detail = raft.role;
  }

  return (
    <button
      type="button"
      className={`health-pill ${cls}`}
      onClick={onClick}
      title={err || 'Cluster state — open overview'}
    >
      <span className="dot" />
      <span>{label}</span>
      {detail && (
        <span className="hp-detail">
          <span className="sep">·</span> {detail}
        </span>
      )}
    </button>
  );
}
