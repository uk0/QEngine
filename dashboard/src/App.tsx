import { useCallback, useEffect, useState } from 'react';
import { Topbar } from './components/Topbar';
import { Sidebar } from './components/Sidebar';
import { SqlConsole } from './components/SqlConsole';
import { AuditPanel } from './components/AuditPanel';
import { ClusterPanel } from './components/ClusterPanel';
import { Login } from './components/Login';
import { Toasts, ToastCtx, useToasts } from './components/Toasts';
import { api } from './api';

type Tab = 'query' | 'audit' | 'cluster';

export function App() {
  const [authed, setAuthed] = useState<boolean | null>(null);
  const [tab, setTab] = useState<Tab>('query');
  const [sql, setSql] = useState<string>('SELECT 1');
  const [treeBump, setTreeBump] = useState(0);
  const toasts = useToasts();

  // Probe session cookie by hitting /tree; 401 → login page.
  useEffect(() => {
    (async () => {
      try {
        await api.tree();
        setAuthed(true);
      } catch {
        setAuthed(false);
      }
    })();
  }, []);

  const refreshTree = useCallback(() => setTreeBump(b => b + 1), []);

  const runSql = useCallback((q: string) => {
    setSql(q);
    setTab('query');
  }, []);

  if (authed === null) {
    return <div className="login-wrap"><div className="mu">Loading…</div></div>;
  }
  if (!authed) {
    return (
      <ToastCtx.Provider value={toasts}>
        <Login onOk={() => setAuthed(true)} />
        <Toasts items={toasts.items} />
      </ToastCtx.Provider>
    );
  }
  return (
    <ToastCtx.Provider value={toasts}>
      <div className="app">
        <Topbar tab={tab} onTab={setTab} onLogout={async () => {
          await api.logout();
          setAuthed(false);
        }} />
        <div className="main">
          <Sidebar
            bump={treeBump}
            onSql={runSql}
            onRefresh={refreshTree}
          />
          <div className="content">
            {tab === 'query'   && <SqlConsole sql={sql} onSql={setSql} onRefreshTree={refreshTree} />}
            {tab === 'audit'   && <AuditPanel />}
            {tab === 'cluster' && <ClusterPanel />}
          </div>
        </div>
      </div>
      <Toasts items={toasts.items} />
    </ToastCtx.Provider>
  );
}
