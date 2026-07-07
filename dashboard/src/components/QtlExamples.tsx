/* Curated QTL example catalog — one-click load into the editor.
 *
 * Keeps the grammar visible to new operators and serves as living
 * documentation for the dialect.  Every entry is a real statement the
 * current parser accepts; `notes` is surfaced as a tooltip.
 */

import { useState } from 'react';

export interface Example {
  title: string;
  sql: string;
  notes?: string;
}
export interface ExampleGroup {
  label: string;
  items: Example[];
}

/* Always-visible quick picks rendered as chips under the editor. */
export const QUICK_EXAMPLES: Example[] = [
  { title: 'List databases', sql: 'LIST DATABASES;' },
  { title: 'List VTables', sql: 'LIST VTABLES;' },
  { title: 'Top 1000 rows', sql: 'SELECT * FROM metrics ORDER BY ts DESC LIMIT 1000;' },
  { title: '5m time buckets', sql: "SELECT time_bucket('5m', ts) AS ts5,\n       host,\n       avg(cpu) AS cpu\nFROM metrics\nGROUP BY ts5, host\nORDER BY ts5;" },
  { title: 'List users', sql: 'LIST USERS;' },
  { title: 'List masters', sql: 'LIST MASTERS;' },
];

export const EXAMPLES: ExampleGroup[] = [
  {
    label: 'Catalog — list',
    items: [
      { title: 'List databases', sql: 'LIST DATABASES;' },
      { title: 'List groups',    sql: 'LIST GROUPS;' },
      { title: 'List groups in DB',    sql: 'LIST GROUPS IN DATABASE prod;' },
      { title: 'List devices',   sql: 'LIST DEVICES;' },
      { title: 'List VTables',   sql: 'LIST VTABLES;' },
      { title: 'List VTables in DB', sql: 'LIST VTABLES IN DATABASE prod;' },
      { title: 'List PTables',   sql: 'LIST PTABLES;' },
      { title: 'PTables of VT',  sql: 'LIST PTABLES USING cpu_vt;' },
      { title: 'List functions', sql: 'LIST FUNCTIONS;' },
      { title: 'List users',     sql: 'LIST USERS;' },
      { title: 'List masters',   sql: 'LIST MASTERS;' },
    ],
  },
  {
    label: 'DDL — databases & groups',
    items: [
      { title: 'Create DB',  sql: "CREATE DATABASE prod DESCRIPTION 'production metrics';" },
      { title: 'Create DB (minimal)', sql: 'CREATE DATABASE dev;' },
      { title: 'Drop DB',    sql: 'DROP DATABASE dev;' },
      { title: 'Create group',           sql: 'CREATE GROUP ops IN DATABASE prod;' },
      { title: 'Create group w/ props',  sql: "CREATE GROUP shard_us (replica_factor=3);" },
      { title: 'Drop group',             sql: 'DROP GROUP ops;' },
      { title: 'Create device', sql: "CREATE DEVICE sensor_01 IN GROUP ops (location='rack-A');" },
      { title: 'Drop device',   sql: 'DROP DEVICE sensor_01 IN GROUP ops;' },
    ],
  },
  {
    label: 'DDL — tables',
    items: [
      {
        title: 'Create plain table',
        sql: `CREATE TABLE metrics (
  ts TIMESTAMP,
  host SYMBOL,
  cpu FLOAT64,
  mem INT64
)
TIMESTAMP(ts)
WITH (PARTITION='day', BLOCK_POINTS=8192);`,
      },
      {
        title: 'Create STable (VTable)',
        sql: `CREATE STABLE cpu_vt (
  ts TIMESTAMP,
  usage FLOAT64,
  core INT64
)
TAGS (host SYMBOL, region SYMBOL)
IN DATABASE prod IN GROUP ops;`,
      },
      {
        title: 'Create child table (PTable)',
        notes: "TAGS are positional — order matches the STABLE's TAGS declaration; string values use single quotes",
        sql: "CREATE TABLE cpu_host01 USING cpu_vt TAGS ('host01', 'us-east');",
      },
      {
        title: 'ALTER ADD COLUMN',
        sql: 'ALTER TABLE metrics ADD COLUMN disk INT64;',
      },
      { title: 'Drop table',   sql: 'DROP TABLE metrics;' },
      { title: 'Drop VTable',  sql: 'DROP VTABLE cpu_vt;' },
      { title: 'Drop STable',  sql: 'DROP STABLE cpu_vt;', notes: 'STABLE and VTABLE are synonyms' },
    ],
  },
  {
    label: 'Query — SELECT',
    items: [
      { title: 'Top 1000 rows',
        sql: 'SELECT * FROM metrics ORDER BY ts DESC LIMIT 1000;' },
      { title: 'Time range',
        sql: "SELECT ts, host, cpu FROM metrics\nWHERE ts BETWEEN '2026-04-01' AND '2026-04-30';" },
      { title: 'Filter + project',
        sql: "SELECT host, avg(cpu) AS avg_cpu\nFROM metrics\nWHERE host='host01'\nGROUP BY host;" },
      { title: 'GROUP BY time bucket',
        sql: "SELECT time_bucket('5m', ts) AS ts5,\n       host,\n       avg(cpu) AS cpu\nFROM metrics\nGROUP BY ts5, host\nORDER BY ts5;" },
      { title: 'Window: rolling sum',
        sql: 'SELECT ts, host, cpu,\n       sum(cpu) OVER (PARTITION BY host ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS cpu5\nFROM metrics;' },
      { title: 'Percentiles',
        sql: 'SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY cpu) AS p95 FROM metrics;' },
      { title: 'ASOF JOIN',
        sql: 'SELECT m.ts, m.cpu, l.level AS logline\nFROM metrics m\nASOF JOIN logs l ON m.host = l.host\nWHERE m.ts > now() - 1h;' },
      { title: 'Spread / stddev',
        sql: 'SELECT spread(cpu) AS spread_, stddev(cpu) AS std_ FROM metrics;' },
      { title: 'Sample by stream',
        sql: "SELECT * FROM metrics SAMPLE BY 1h FILL(LINEAR);" },
    ],
  },
  {
    label: 'Admin — mutating',
    items: [
      { title: 'TRUNCATE TABLE', sql: 'TRUNCATE TABLE metrics;' },
      { title: 'DELETE old (>)',  sql: "DELETE FROM metrics WHERE ts < '2026-01-01';",
        notes: 'Partition-level: only wholly expired partitions are removed' },
      { title: 'DELETE recent (<)', sql: "DELETE FROM metrics WHERE ts > '2030-01-01';" },
      { title: 'EXPORT TABLE → parquet',
        sql: "EXPORT TABLE metrics TO PARQUET '/var/lib/tsdb/exports/metrics';" },
    ],
  },
  {
    label: 'RBAC / Auth',
    items: [
      { title: 'Create user', sql: "CREATE USER alice IDENTIFIED BY 'secret';" },
      { title: 'Create admin', sql: "CREATE USER opsadmin IDENTIFIED BY 'pw' ROLE admin;" },
      { title: 'Grant SELECT', sql: 'GRANT SELECT ON metrics TO alice;' },
      { title: 'Grant SELECT on *', sql: 'GRANT SELECT ON * TO alice;' },
      { title: 'Grant INSERT',  sql: 'GRANT INSERT ON metrics TO alice;' },
      { title: 'Grant DDL',     sql: 'GRANT DDL ON * TO opsadmin;' },
      { title: 'Grant ALL',     sql: 'GRANT ALL ON * TO opsadmin;' },
      { title: 'Revoke',        sql: 'REVOKE SELECT ON metrics FROM alice;' },
      { title: 'Alter password', sql: "ALTER USER alice SET PASSWORD 'newpw';" },
      { title: 'Drop user',     sql: 'DROP USER alice;' },
    ],
  },
  {
    label: 'TMQ — consumer groups',
    items: [
      { title: 'Create group', sql: 'CREATE CONSUMER GROUP cg1 ON metrics;' },
      { title: 'Join group',   sql: 'JOIN GROUP cg1 AS worker_01;' },
      { title: 'Commit offset', sql: 'COMMIT OFFSET cg1 AT 12345 AS worker_01;' },
      { title: 'Leave group',  sql: 'LEAVE GROUP cg1 AS worker_01;' },
    ],
  },
  {
    label: 'Cluster / Raft',
    items: [
      { title: 'List masters',  sql: 'LIST MASTERS;' },
      { title: 'Add master',    sql: "ADD MASTER '10.0.0.5:28081';" },
      { title: 'Remove master', sql: "REMOVE MASTER '10.0.0.5:28081';" },
    ],
  },
  {
    label: 'UDF',
    items: [
      { title: 'Register UDF',
        sql: "CREATE FUNCTION my_double(FLOAT64) RETURNS FLOAT64\n  FROM '/udf/libmath.so' SYMBOL 'my_double';" },
      { title: 'Drop UDF', sql: 'DROP FUNCTION my_double;' },
      { title: 'List UDFs', sql: 'LIST FUNCTIONS;' },
    ],
  },
];

export function QtlExamples({ onPick }: { onPick: (sql: string) => void }) {
  const [expanded, setExpanded] = useState<string | null>('Catalog — list');
  const [filter, setFilter]     = useState('');
  const q = filter.trim().toLowerCase();
  return (
    <div className="qtl-catalog">
      <input
        type="search"
        className="qtl-search"
        placeholder="filter examples (e.g. group by, grant)…"
        value={filter}
        onChange={e => setFilter(e.target.value)}
      />
      {EXAMPLES.map(group => {
        const items = q
          ? group.items.filter(
              it =>
                it.title.toLowerCase().includes(q) ||
                it.sql.toLowerCase().includes(q),
            )
          : group.items;
        if (q && !items.length) return null;
        const open = !!q || expanded === group.label;
        return (
          <div key={group.label} className="qtl-group">
            <div
              className="qtl-group-hdr"
              onClick={() => !q && setExpanded(open ? null : group.label)}
            >
              <span className={`caret ${open ? 'open' : ''}`}>▸</span>
              <span>{group.label}</span>
              <span className="count">{items.length}</span>
            </div>
            {open && (
              <div className="qtl-group-items">
                {items.map(it => (
                  <div
                    key={it.title}
                    className="qtl-item"
                    title={it.notes ?? it.sql}
                    onClick={() => onPick(it.sql)}
                  >
                    {it.title}
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
