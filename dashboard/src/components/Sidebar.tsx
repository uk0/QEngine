import { ClusterInfo } from '../api';
import { IconPulse, IconTerminal, IconList, IconWrench } from './icons';

export type Page = 'overview' | 'sql' | 'audit' | 'maintenance';

interface Props {
  page: Page;
  onPage: (p: Page) => void;
  cluster: ClusterInfo | null;
}

const NAV: { id: Page; label: string; icon: (p: { size?: number }) => JSX.Element }[] = [
  { id: 'overview', label: 'Overview', icon: IconPulse },
  { id: 'sql', label: 'SQL console', icon: IconTerminal },
  { id: 'audit', label: 'Audit log', icon: IconList },
  { id: 'maintenance', label: 'Maintenance', icon: IconWrench },
];

export function Sidebar({ page, onPage, cluster }: Props) {
  const host = cluster?.local?.host ?? '';
  const nodes = cluster?.nodes ?? [];

  return (
    <nav className="side-nav" aria-label="Primary">
      <div className="brand">
        <span className="brand-mark">ts</span>
        <span>
          <div className="brand-name">tsdb</div>
          <div className="brand-sub">console</div>
        </span>
      </div>
      <div className="nav-list">
        <div className="nav-label">Operate</div>
        {NAV.map(item => (
          <button
            key={item.id}
            className={`nav-item ${page === item.id ? 'active' : ''}`}
            onClick={() => onPage(item.id)}
            aria-current={page === item.id ? 'page' : undefined}
            title={item.label}
          >
            <item.icon />
            <span className="nav-text">{item.label}</span>
          </button>
        ))}
      </div>
      <div className="nav-foot" title={host || undefined}>
        {host ? `${host}` : 'single node'}
        {nodes.length > 1 ? ` · ${nodes.length} nodes` : ''}
      </div>
    </nav>
  );
}
