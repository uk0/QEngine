/* Minimal inline stroke icons — no icon font, no CDN, theme-aware via
 * currentColor.  16×16 viewBox 24, stroke 1.7. */

interface P { size?: number }
const S = (p: P) => ({
  width: p.size ?? 16,
  height: p.size ?? 16,
  viewBox: '0 0 24 24',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.7,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
  'aria-hidden': true,
});

export const IconPulse = (p: P) => (
  <svg {...S(p)}><path d="M2 12h4l3-8 5 16 3-8h5" /></svg>
);

export const IconTerminal = (p: P) => (
  <svg {...S(p)}><path d="M4 17l6-5-6-5" /><path d="M12 19h8" /></svg>
);

export const IconList = (p: P) => (
  <svg {...S(p)}>
    <path d="M9 6h11M9 12h11M9 18h11" />
    <path d="M4 6h.01M4 12h.01M4 18h.01" strokeWidth={2.4} />
  </svg>
);

export const IconWrench = (p: P) => (
  <svg {...S(p)}>
    <path d="M14.7 6.3a4.5 4.5 0 0 0-6 5.6L3 17.6V21h3.4l5.7-5.7a4.5 4.5 0 0 0 5.6-6L14 13l-3-3 3.7-3.7z" />
  </svg>
);

export const IconSun = (p: P) => (
  <svg {...S(p)}>
    <circle cx="12" cy="12" r="4" />
    <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
  </svg>
);

export const IconMoon = (p: P) => (
  <svg {...S(p)}><path d="M21 12.8A9 9 0 1 1 11.2 3 7 7 0 0 0 21 12.8z" /></svg>
);

export const IconLogout = (p: P) => (
  <svg {...S(p)}>
    <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
    <path d="M16 17l5-5-5-5M21 12H9" />
  </svg>
);

export const IconMenu = (p: P) => (
  <svg {...S(p)}><path d="M4 6h16M4 12h16M4 18h16" /></svg>
);

export const IconRefresh = (p: P) => (
  <svg {...S(p)}>
    <path d="M21 12a9 9 0 1 1-2.6-6.4" />
    <path d="M21 3v6h-6" />
  </svg>
);

export const IconDownload = (p: P) => (
  <svg {...S(p)}>
    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
    <path d="M7 10l5 5 5-5M12 15V3" />
  </svg>
);
