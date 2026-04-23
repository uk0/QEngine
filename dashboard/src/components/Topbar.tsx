type Tab = 'query' | 'audit' | 'cluster';

interface Props {
  tab: Tab;
  onTab: (t: Tab) => void;
  onLogout: () => void;
}

export function Topbar({ tab, onTab, onLogout }: Props) {
  return (
    <div className="topbar">
      <span className="brand">tsdb</span>
      <span className="mu">console</span>
      <div className="spacer" />
      <span className={`tab ${tab === 'query' ? 'active' : ''}`}   onClick={() => onTab('query')}>Query</span>
      <span className={`tab ${tab === 'audit' ? 'active' : ''}`}   onClick={() => onTab('audit')}>Audit</span>
      <span className={`tab ${tab === 'cluster' ? 'active' : ''}`} onClick={() => onTab('cluster')}>Cluster</span>
      <button onClick={onLogout}>Logout</button>
    </div>
  );
}
