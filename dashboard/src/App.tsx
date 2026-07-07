import { useCallback, useEffect, useState } from 'react';
import { Topbar } from './components/Topbar';
import { Sidebar, Page } from './components/Sidebar';
import { SqlConsole } from './components/SqlConsole';
import { SchemaTree } from './components/SchemaTree';
import { AuditPanel } from './components/AuditPanel';
import { ClusterPanel } from './components/ClusterPanel';
import { MaintenancePanel } from './components/MaintenancePanel';
import { Login } from './components/Login';
import { Toasts, ToastCtx, useToasts } from './components/Toasts';
import { ModalProvider } from './components/ModalCtx';
import {
  api,
  ApiError,
  ClusterInfo,
  RaftInfo,
  errMsg,
  setUnauthorizedHandler,
} from './api';
import { usePoll, useTheme, useLocalFlag } from './hooks';

const PAGE_TITLES: Record<Page, string> = {
  overview: 'Cluster overview',
  sql: 'SQL console',
  audit: 'Audit log',
  maintenance: 'Maintenance',
};

export function App() {
  const [authed, setAuthed] = useState<boolean | null>(null);
  const [bootErr, setBootErr] = useState('');
  const [page, setPage] = useState<Page>('overview');
  const [sql, setSql] = useState<string>('SELECT 1');
  const [treeBump, setTreeBump] = useState(0);
  const [user, setUser] = useState<string>(() => {
    try { return localStorage.getItem('tsdb.user') ?? ''; } catch { return ''; }
  });
  const [navMin, toggleNav] = useLocalFlag('tsdb.navmin', false);
  const { theme, toggle: toggleTheme } = useTheme();
  const toasts = useToasts();

  /* Any 401 from any call flips the SPA back to its login screen —
   * never a crash, never a hard redirect loop. */
  useEffect(() => {
    setUnauthorizedHandler(() => setAuthed(false));
    return () => setUnauthorizedHandler(null);
  }, []);

  /* Session probe: /tree is auth-gated, so it answers the question. */
  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        await api.tree();
        if (alive) setAuthed(true);
      } catch (e) {
        if (!alive) return;
        setAuthed(false);
        if (!(e instanceof ApiError && e.kind === 'auth')) {
          setBootErr(errMsg(e, 'server unreachable'));
        }
      }
    })();
    return () => { alive = false; };
  }, []);

  /* ── Shared cluster/raft poll (Topbar pill + Overview panel) ────── */
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [raft, setRaft] = useState<RaftInfo | null>(null);
  const [clusterErr, setClusterErr] = useState('');
  const [clusterAt, setClusterAt] = useState(0);

  const pollCluster = useCallback(async () => {
    const [c, r] = await Promise.allSettled([api.cluster(), api.raft()]);
    if (c.status === 'fulfilled') {
      setCluster(c.value ?? null);
      setClusterErr('');
    } else {
      setClusterErr(errMsg(c.reason, 'cluster unreachable'));
    }
    setRaft(r.status === 'fulfilled' ? (r.value ?? null) : null);
    setClusterAt(Date.now());
  }, []);
  usePoll(pollCluster, 4000, authed === true);

  const refreshTree = useCallback(() => setTreeBump(b => b + 1), []);
  const runSql = useCallback((q: string) => {
    setSql(q);
    setPage('sql');
  }, []);

  const onLoggedIn = useCallback((u: string) => {
    setUser(u);
    setBootErr('');
    try { localStorage.setItem('tsdb.user', u); } catch { /* ignore */ }
    setAuthed(true);
  }, []);

  const onLogout = useCallback(async () => {
    await api.logout();
    setCluster(null);
    setRaft(null);
    setAuthed(false);
  }, []);

  if (authed === null) {
    return (
      <div className="login-wrap">
        <div className="mu"><span className="spinner" /> Connecting…</div>
      </div>
    );
  }

  if (!authed) {
    return (
      <ToastCtx.Provider value={toasts}>
        <Login onOk={onLoggedIn} initialUser={user || 'root'} bootErr={bootErr} />
        <Toasts items={toasts.items} />
      </ToastCtx.Provider>
    );
  }

  return (
    <ToastCtx.Provider value={toasts}>
      <ModalProvider>
        <div className={`shell ${navMin ? 'nav-min' : ''}`}>
          <Sidebar page={page} onPage={setPage} cluster={cluster} />
          <Topbar
            title={PAGE_TITLES[page]}
            cluster={cluster}
            raft={raft}
            clusterErr={clusterErr}
            theme={theme}
            onTheme={toggleTheme}
            onNav={toggleNav}
            user={user}
            onLogout={onLogout}
            onHealthClick={() => setPage('overview')}
          />
          <main className="page">
            <div className="page-inner">
              {page === 'overview' && (
                <ClusterPanel
                  cluster={cluster}
                  raft={raft}
                  err={clusterErr}
                  lastAt={clusterAt}
                />
              )}
              {page === 'sql' && (
                <div className="sql-layout">
                  <SchemaTree
                    bump={treeBump}
                    onSql={runSql}
                    onRefresh={refreshTree}
                    cluster={cluster}
                  />
                  <div className="sql-main">
                    <SqlConsole sql={sql} onSql={setSql} onRefreshTree={refreshTree} />
                  </div>
                </div>
              )}
              {page === 'audit' && <AuditPanel />}
              {page === 'maintenance' && <MaintenancePanel />}
            </div>
          </main>
        </div>
        <Toasts items={toasts.items} />
      </ModalProvider>
    </ToastCtx.Provider>
  );
}
