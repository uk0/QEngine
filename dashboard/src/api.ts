/* api.ts — thin HTTP client for the tsdb metrics_server endpoints.
 *
 * Every call is a POST/GET with cookie-based auth; the browser manages
 * the session cookie automatically, so we never need to pass a token
 * explicitly.  Errors surface as thrown Error objects carrying both the
 * HTTP status and the parsed body so callers can show actionable text.
 */

export interface SqlResult {
  cols: string[];
  types: string[];
  rows: unknown[][];
  nrows: number;
  truncated: boolean;
  ms: number;
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

export interface TreeDatabase {
  name: string;
  description?: string;
  protected?: boolean;
}
export interface TreeGroup {
  name: string;
  database?: string;
}
export interface TreeVTable {
  name: string;
  database?: string;
  group?: string;
  ncols: number;
}
export interface TreePTable {
  name: string;
  vtable?: string;
}
export interface TreeTable {
  name: string;
  ncols: number;
  is_stable?: boolean;
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

async function req<T = unknown>(
  method: string,
  path: string,
  body?: BodyInit,
  extraHeaders: Record<string, string> = {},
): Promise<T> {
  const resp = await fetch(path, {
    method,
    credentials: 'include',
    headers: {
      Accept: 'application/json',
      ...(body && !(body instanceof FormData)
        ? { 'Content-Type': 'application/json' }
        : {}),
      ...extraHeaders,
    },
    body,
  });
  if (resp.status === 401) {
    // Force login — full reload so the server's /login redirect takes.
    window.location.href = '/login';
    throw new Error('login required');
  }
  const text = await resp.text();
  let parsed: unknown = text;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    /* fall through — raw text */
  }
  if (!resp.ok) {
    let msg = `HTTP ${resp.status}`;
    if (parsed && typeof parsed === 'object' && 'error' in (parsed as Record<string, unknown>)) {
      msg = String((parsed as Record<string, unknown>).error);
    }
    throw new Error(msg);
  }
  return parsed as T;
}

export const api = {
  /** Execute a SQL/QTL statement.  Errors come back as {error: ...} with
   *  HTTP 200 — unwrap them so callers can catch them normally. */
  async sql(q: string): Promise<SqlResult> {
    const r = await req<SqlResult & { error?: string }>(
      'POST',
      '/sql',
      JSON.stringify({ q }),
    );
    if (r && typeof r === 'object' && 'error' in r && r.error) {
      throw new Error(String(r.error));
    }
    return r as SqlResult;
  },

  tree(): Promise<Tree> {
    return req<Tree>('GET', '/tree');
  },

  cluster(): Promise<unknown> {
    return req<unknown>('GET', '/cluster');
  },

  raft(): Promise<unknown> {
    return req<unknown>('GET', '/raft');
  },

  health(): Promise<{ status: string; uptime_s: number; pid: number }> {
    return req<{ status: string; uptime_s: number; pid: number }>(
      'GET',
      '/health',
    );
  },

  audit(n = 200): Promise<{ rows: AuditRecord[]; nrows: number }> {
    return req<{ rows: AuditRecord[]; nrows: number }>(
      'GET',
      `/audit?n=${n}`,
    );
  },

  retentionSweep(): Promise<{ ok: boolean; partitions_deleted: number }> {
    return req<{ ok: boolean; partitions_deleted: number }>(
      'POST',
      '/retention/sweep',
    );
  },

  pitr(
    targetNs: number | string,
  ): Promise<{ ok: boolean; target_ns: number; partitions_removed: number }> {
    return req<{
      ok: boolean;
      target_ns: number;
      partitions_removed: number;
    }>('POST', `/pitr?ts=${targetNs}`);
  },

  /** POST the HTML-form login — same endpoint the embedded HTML uses.
   *  Returns after the server has set the cookie. */
  async login(user: string, password: string): Promise<void> {
    const form = new URLSearchParams();
    form.append('user', user);
    form.append('pass', password);
    const resp = await fetch('/login', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: form.toString(),
      redirect: 'manual',
    });
    // Server answers 302 on success; fetch surfaces that as type:'opaqueredirect'.
    if (resp.status !== 0 && resp.status >= 400) {
      throw new Error(`login failed (${resp.status})`);
    }
  },

  async logout(): Promise<void> {
    await fetch('/logout', { credentials: 'include' });
  },
};

// ──────────────────────────────────────────────────────────────────────
// Delete helpers — centralised so the component tree doesn't care about
// the underlying SQL shape.  If the grammar changes we flip one place.
// ──────────────────────────────────────────────────────────────────────

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

// Very small identifier / literal quoting — tsdb accepts [A-Za-z0-9_.]
// identifiers.  Anything else we quote as a string for safety.
function ident(s: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_.]*$/.test(s)) return s;
  return `"${s.replace(/"/g, '""')}"`;
}
function quote(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}
