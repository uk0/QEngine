/* api.ts — hardened HTTP client for the tsdb metrics_server endpoints.
 *
 * Every call goes through request(): AbortController timeout, consistent
 * ApiError typing (network / timeout / auth / http), 401 routed to a
 * registered handler (the App flips back to the login screen — never a
 * crash or a raw redirect loop), non-2xx surfaced as readable messages.
 * Cookie-based auth: the browser carries the session cookie itself.
 */

export type ApiErrorKind = 'network' | 'timeout' | 'auth' | 'http';

export class ApiError extends Error {
  readonly status: number; // 0 when no HTTP response (network / timeout)
  readonly kind: ApiErrorKind;
  constructor(message: string, kind: ApiErrorKind, status = 0) {
    super(message);
    this.name = 'ApiError';
    this.kind = kind;
    this.status = status;
  }
}

/** Registered by App: called once per 401 so the SPA can return to its
 *  login screen instead of dying mid-render. */
let onUnauthorized: (() => void) | null = null;
export function setUnauthorizedHandler(fn: (() => void) | null): void {
  onUnauthorized = fn;
}

export function errMsg(e: unknown, fallback = 'request failed'): string {
  if (e instanceof Error && e.message) return e.message;
  return fallback;
}

interface ReqOpts {
  body?: BodyInit;
  headers?: Record<string, string>;
  timeoutMs?: number;
  /** Return the raw response text instead of parsed JSON (/metrics). */
  raw?: boolean;
  /** Don't fire the global 401 handler (login-flow probes). */
  skipAuthHook?: boolean;
}

const DEFAULT_TIMEOUT_MS = 12_000;

async function request<T = unknown>(
  method: string,
  path: string,
  opts: ReqOpts = {},
): Promise<T> {
  const ctl = new AbortController();
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const timer = window.setTimeout(() => ctl.abort(), timeoutMs);

  let resp: Response;
  try {
    resp = await fetch(path, {
      method,
      credentials: 'include',
      signal: ctl.signal,
      headers: {
        Accept: 'application/json',
        ...(opts.body && !(opts.body instanceof FormData)
          ? { 'Content-Type': 'application/json' }
          : {}),
        ...opts.headers,
      },
      body: opts.body,
    });
  } catch (e) {
    if (ctl.signal.aborted) {
      throw new ApiError(
        `${path} timed out after ${Math.round(timeoutMs / 1000)}s`,
        'timeout',
      );
    }
    throw new ApiError(
      `server unreachable (${e instanceof Error ? e.message : 'network error'})`,
      'network',
    );
  } finally {
    window.clearTimeout(timer);
  }

  if (resp.status === 401) {
    if (!opts.skipAuthHook && onUnauthorized) onUnauthorized();
    throw new ApiError('login required', 'auth', 401);
  }

  let text = '';
  try {
    text = await resp.text();
  } catch {
    /* body read failed — fall through with empty text */
  }
  let parsed: unknown = null;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = text; // non-JSON payload (/metrics, HTML error pages)
    }
  }

  if (!resp.ok) {
    let msg = `HTTP ${resp.status}${resp.statusText ? ` ${resp.statusText}` : ''}`;
    if (parsed && typeof parsed === 'object') {
      const rec = parsed as Record<string, unknown>;
      if (typeof rec.error === 'string' && rec.error) msg = rec.error;
      else if (resp.status === 503) msg = 'service unavailable — provider not registered on this node';
    }
    if (resp.status === 405) msg = `${msg} (method not allowed)`;
    throw new ApiError(msg, 'http', resp.status);
  }

  return (opts.raw ? (text as unknown) : parsed) as T;
}

/* ── Result / payload types ───────────────────────────────────────── */

export interface SqlResult {
  cols: string[];
  types: string[];
  rows: unknown[][];
  nrows: number;
  truncated: boolean;
  ms: number;
}

export interface ClusterNode {
  id: string;
  addr: string;
  state: 'ALIVE' | 'SUSPECT' | 'DEAD' | string;
  role: 'master' | 'data' | string;
  ver: number;
  hb_age_ms: number;
  known_for_s: number;
  suspect_count: number;
  /** Data-directory size in bytes via DISK_SYNC gossip; 0 until sampled. */
  disk_bytes?: number;
}

export interface AutobalanceNode {
  id: string;
  writes_sec: number;
  storage_mb: number;
  cpu_pct: number;
  vn: number;
}

export interface AutobalanceInfo {
  local_id: string;
  ema_writes_sec: number;
  interval_ms: number;
  alpha: number;
  beta: number;
  dampen: number;
  nodes: AutobalanceNode[];
}

export interface LocalDisk {
  total_bytes: number;
  free_bytes: number;
  used_x10: number;
  data_dir: string;
  vn_weight: number;
}

export interface ShardingInfo {
  replica_n: number;
  alive_nodes: number;
  active: number;
}

export interface ClusterInfo {
  local_id: string;
  nodes: ClusterNode[];
  autobalance?: AutobalanceInfo;
  sharding?: ShardingInfo;
  local?: {
    host: string;
    pid: number;
    uptime_s: number;
    disk: LocalDisk;
  };
}

export interface RaftInfo {
  self_id?: string;
  role?: 'leader' | 'follower' | 'candidate' | string;
  current_term?: number;
  leader_id?: string;
  commit_index?: number;
  last_applied?: number;
  last_index?: number;
  snapshot_index?: number;
}

export interface AuditRecord {
  ts: string;
  user: string;
  event: string;
  action: string;
  object: string;
  result: number;
  detail: string;
}

export interface TreeDatabase { name: string; description?: string; protected?: boolean; }
export interface TreeGroup { name: string; database?: string; }
export interface TreeVTable { name: string; database?: string; group?: string; ncols: number; }
export interface TreePTable { name: string; vtable?: string; database?: string; group?: string; }
export interface TreeTable {
  name: string;
  ncols: number;
  is_stable?: boolean;
  /** Cluster nodes report on-disk footprint per table dir. */
  kind?: string;
  bytes?: number;
}
export interface TreeDbInfo {
  name?: string;
  path?: string;
  host?: string;
  uptime_s?: number;
  disk_bytes?: number;
}
export interface Tree {
  db?: TreeDbInfo;
  databases?: TreeDatabase[];
  groups?: TreeGroup[];
  vtables?: TreeVTable[];
  ptables?: TreePTable[];
  tables?: TreeTable[];
}

export interface HealthInfo { status: string; uptime_s: number; pid: number; }

/** GET /whoami — session identity for RBAC-aware UI.
 *  {auth:true,user,role} on gated servers; {auth:false} when the auth
 *  gate is disabled; 404 on servers that predate the endpoint. */
export interface WhoAmI {
  auth: boolean;
  user?: string | null;
  role?: 'admin' | 'normal' | string | null;
}

export interface CatalogOrphan {
  name: string;
  missing_db?: string;
  missing_group?: string;
  missing_stable?: string;
  path?: string;
}
export interface CatalogReport {
  error?: string;
  orphan_groups?: CatalogOrphan[];
  orphan_stables?: CatalogOrphan[];
  orphan_children?: CatalogOrphan[];
  orphan_dirs?: CatalogOrphan[];
  summary?: {
    databases?: number;
    groups?: number;
    stables?: number;
    children?: number;
  };
}

/* ── Endpoint surface ─────────────────────────────────────────────── */

export const api = {
  /** Execute a SQL/QTL statement.  The server can answer {error:...}
   *  with HTTP 200 — unwrap so callers catch one error shape. */
  async sql(q: string): Promise<SqlResult> {
    const r = await request<Partial<SqlResult> & { error?: string }>(
      'POST',
      '/sql',
      { body: JSON.stringify({ q }), timeoutMs: 30_000 },
    );
    if (r && typeof r === 'object' && r.error) {
      throw new ApiError(String(r.error), 'http', 200);
    }
    // Defensive defaults — a partial payload must never crash the grid.
    return {
      cols: r?.cols ?? [],
      types: r?.types ?? [],
      rows: r?.rows ?? [],
      nrows: r?.nrows ?? (r?.rows?.length ?? 0),
      truncated: r?.truncated ?? false,
      ms: r?.ms ?? 0,
    };
  },

  tree(): Promise<Tree> {
    return request<Tree>('GET', '/tree');
  },

  cluster(): Promise<ClusterInfo> {
    return request<ClusterInfo>('GET', '/cluster');
  },

  raft(): Promise<RaftInfo> {
    return request<RaftInfo>('GET', '/raft');
  },

  /** Prometheus-text /metrics → {series → value}.  Labelled series keep
   *  their label block in the key (`x_bucket{le="5"}`). */
  async metrics(): Promise<Record<string, number>> {
    const txt = await request<string>('GET', '/metrics', {
      raw: true,
      headers: { Accept: 'text/plain' },
    });
    const out: Record<string, number> = {};
    for (const line of String(txt ?? '').split('\n')) {
      if (!line || line.startsWith('#')) continue;
      const sp = line.indexOf(' ');
      if (sp < 0) continue;
      const k = line.slice(0, sp).trim();
      const v = Number(line.slice(sp + 1).trim());
      if (k && Number.isFinite(v)) out[k] = v;
    }
    return out;
  },

  health(): Promise<HealthInfo> {
    return request<HealthInfo>('GET', '/health');
  },

  /** Callers must treat any failure (404 on old servers, network) as
   *  "admin" — UI-only gating; the server enforces per-statement RBAC. */
  whoami(): Promise<WhoAmI> {
    return request<WhoAmI>('GET', '/whoami');
  },

  audit(n = 200): Promise<{ rows: AuditRecord[]; nrows: number }> {
    return request<{ rows: AuditRecord[]; nrows: number }>(
      'GET',
      `/audit?n=${encodeURIComponent(n)}`,
    );
  },

  /** MUTATING — POST only (GET answers 405 server-side). */
  retentionSweep(): Promise<{ ok: boolean; partitions_deleted: number }> {
    return request<{ ok: boolean; partitions_deleted: number }>(
      'POST',
      '/retention/sweep',
      { timeoutMs: 60_000 },
    );
  },

  /** MUTATING — trims every partition starting after target_ns. */
  pitr(
    targetNs: number | string,
  ): Promise<{ ok: boolean; target_ns: number; partitions_removed: number }> {
    return request<{ ok: boolean; target_ns: number; partitions_removed: number }>(
      'POST',
      `/pitr?ts=${encodeURIComponent(String(targetNs))}`,
      { timeoutMs: 60_000 },
    );
  },

  catalogCheck(): Promise<CatalogReport> {
    return request<CatalogReport>('GET', '/catalog/check', { timeoutMs: 30_000 });
  },

  /** POST the HTML-form login.  Success = 302 (opaqueredirect / status 0
   *  under redirect:"manual"); bad credentials = 401 JSON. */
  async login(user: string, password: string): Promise<void> {
    const form = new URLSearchParams();
    form.append('user', user);
    form.append('pass', password);

    const ctl = new AbortController();
    const timer = window.setTimeout(() => ctl.abort(), DEFAULT_TIMEOUT_MS);
    let resp: Response;
    try {
      resp = await fetch('/login', {
        method: 'POST',
        credentials: 'include',
        signal: ctl.signal,
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: form.toString(),
        redirect: 'manual',
      });
    } catch (e) {
      if (ctl.signal.aborted) throw new ApiError('login timed out', 'timeout');
      throw new ApiError(
        `server unreachable (${e instanceof Error ? e.message : 'network error'})`,
        'network',
      );
    } finally {
      window.clearTimeout(timer);
    }

    if (resp.status === 401) {
      let msg = 'invalid credentials';
      try {
        const j = (await resp.json()) as { error?: string };
        if (j?.error) msg = j.error;
      } catch { /* keep default */ }
      throw new ApiError(msg, 'auth', 401);
    }
    if (resp.status >= 400) {
      throw new ApiError(`login failed (HTTP ${resp.status})`, 'http', resp.status);
    }
    // status 0 (opaqueredirect) or 2xx/3xx ⇒ cookie set.
  },

  /** Best-effort — never throws. */
  async logout(): Promise<void> {
    try {
      await fetch('/logout', { credentials: 'include', redirect: 'manual' });
    } catch { /* cookie clear is best-effort */ }
  },
};

/* ── DDL helpers — one place owns the SQL shape ───────────────────── */

export const ddl = {
  dropDatabase: (name: string) => api.sql(`DROP DATABASE ${ident(name)}`),
  dropGroup: (name: string) => api.sql(`DROP GROUP ${ident(name)}`),
  dropVTable: (name: string) => api.sql(`DROP VTABLE ${ident(name)}`),
  dropPTable: (name: string) => api.sql(`DROP TABLE ${ident(name)}`),
  dropTable: (name: string) => api.sql(`DROP TABLE ${ident(name)}`),
  createDatabase: (name: string, desc = '') =>
    api.sql(
      `CREATE DATABASE ${ident(name)}${desc ? ` DESCRIPTION ${quote(desc)}` : ''}`,
    ),
  createGroup: (name: string, db?: string) =>
    api.sql(
      `CREATE GROUP ${ident(name)}${db ? ` IN DATABASE ${ident(db)}` : ''}`,
    ),
};

// tsdb accepts [A-Za-z0-9_.] identifiers; anything else gets quoted.
function ident(s: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_.]*$/.test(s)) return s;
  return `"${s.replace(/"/g, '""')}"`;
}
function quote(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}
