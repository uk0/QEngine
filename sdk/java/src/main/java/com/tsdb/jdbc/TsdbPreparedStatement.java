package com.tsdb.jdbc;

import com.tsdb.client.TsdbClient;
import java.io.InputStream;
import java.io.Reader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Minimal PreparedStatement — substitutes ? with Java-side toString() of
 * the bound value.  Not SQL-injection-safe; intended for one-shot queries.
 * A production driver would move parameter binding into the wire protocol.
 */
public class TsdbPreparedStatement extends TsdbStatement implements PreparedStatement {

    private final String sql;
    private Object[] params;

    public TsdbPreparedStatement(TsdbConnection c, String sql) {
        super(c);
        this.sql = sql;
        int n = 0;
        for (int i = 0; i < sql.length(); i++) if (sql.charAt(i) == '?') n++;
        this.params = new Object[n];
    }

    private String bindSQL() {
        StringBuilder out = new StringBuilder(sql.length() + 32);
        int idx = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '?' && idx < params.length) {
                Object v = params[idx++];
                if (v == null)                  out.append("NULL");
                else if (v instanceof Number)   out.append(v.toString());
                else                            out.append('\'')
                                                   .append(v.toString().replace("'", "''"))
                                                   .append('\'');
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    @Override public ResultSet executeQuery() throws SQLException { return executeQuery(bindSQL()); }
    @Override public int       executeUpdate() throws SQLException { return executeUpdate(bindSQL()); }
    @Override public boolean   execute() throws SQLException { return execute(bindSQL()); }

    @Override public void setNull(int p, int t) { params[p - 1] = null; }
    @Override public void setBoolean(int p, boolean v) { params[p - 1] = v ? 1 : 0; }
    @Override public void setByte(int p, byte v)    { params[p - 1] = v; }
    @Override public void setShort(int p, short v)  { params[p - 1] = v; }
    @Override public void setInt(int p, int v)      { params[p - 1] = v; }
    @Override public void setLong(int p, long v)    { params[p - 1] = v; }
    @Override public void setFloat(int p, float v)  { params[p - 1] = v; }
    @Override public void setDouble(int p, double v){ params[p - 1] = v; }
    @Override public void setBigDecimal(int p, BigDecimal v) { params[p - 1] = v; }
    @Override public void setString(int p, String v){ params[p - 1] = v; }
    @Override public void setBytes(int p, byte[] v) { params[p - 1] = v; }
    @Override public void setDate(int p, Date v)    { params[p - 1] = v; }
    @Override public void setTime(int p, Time v)    { params[p - 1] = v; }
    @Override public void setTimestamp(int p, Timestamp v) {
        // tsdb timestamps are nanoseconds since epoch
        params[p - 1] = v == null ? null :
                (v.getTime() * 1_000_000L + (v.getNanos() % 1_000_000L));
    }
    @Override public void setObject(int p, Object v){ params[p - 1] = v; }
    @Override public void setObject(int p, Object v, int t) { params[p - 1] = v; }
    @Override public void setObject(int p, Object v, int t, int s) { params[p - 1] = v; }
    @Override public void clearParameters() {
        for (int i = 0; i < params.length; i++) params[i] = null;
    }
    @Override public boolean execute(String s) throws SQLException { return super.execute(s); }

    /* ---- Columnar batch: addBatch / clearBatch / executeBatch ----
     *
     * Routes JDBC's standard "buffer N rows then flush" pattern to the
     * binary wire WRITE_BATCH on the underlying TsdbClient.  Recognised
     * SQL shape:
     *
     *   INSERT INTO <table> (col1, col2, ...) VALUES (?, ?, ...)
     *
     * Column types are inferred from the first row's bound values:
     *   Long  → INT64    (or TIMESTAMP for any column called "ts")
     *   Double → FLOAT64
     *   String → SYMBOL
     *
     * setTimestamp() also produces a Long (ns since epoch), which we
     * route to the TIMESTAMP slot.  Other column-name conventions can
     * be supported later by querying the catalog for the schema.
     */
    private String           batchTable;
    private String[]         batchCols;
    private byte[]           batchTypes;
    private int              batchTsCi = -1;
    private List<Object[]>   batchRows;

    private void parseInsertOnce() throws SQLException {
        if (batchTable != null) return;
        String s = sql.trim();
        String upper = s.toUpperCase(Locale.ROOT);
        if (!upper.startsWith("INSERT INTO ")) {
            throw new SQLException("addBatch only supports INSERT INTO ... VALUES (?, ...) SQL");
        }
        int paren = s.indexOf('(');
        int values = upper.indexOf("VALUES");
        if (paren < 0 || values < 0 || paren > values) {
            throw new SQLException("addBatch: column list (col1, col2, ...) required before VALUES");
        }
        batchTable = s.substring("INSERT INTO ".length(), paren).trim();
        int closeP = s.indexOf(')', paren);
        String colList = s.substring(paren + 1, closeP);
        String[] raw = colList.split(",");
        batchCols  = new String[raw.length];
        batchTypes = new byte[raw.length];
        for (int i = 0; i < raw.length; i++) {
            batchCols[i] = raw[i].trim();
            if (batchCols[i].equalsIgnoreCase("ts")) batchTsCi = i;
        }
        if (params.length != batchCols.length) {
            throw new SQLException("addBatch: ? count (" + params.length + ") != column count ("
                                   + batchCols.length + ")");
        }
        batchRows = new ArrayList<>();
    }

    @Override public void addBatch() throws SQLException {
        parseInsertOnce();
        Object[] copy = new Object[params.length];
        System.arraycopy(params, 0, copy, 0, params.length);
        batchRows.add(copy);
    }

    @Override public void clearBatch() {
        if (batchRows != null) batchRows.clear();
    }

    @Override public int[] executeBatch() throws SQLException {
        if (batchTable == null || batchRows == null || batchRows.isEmpty())
            return new int[0];

        /* Infer types from the first row.  Subsequent rows must match. */
        Object[] first = batchRows.get(0);
        for (int i = 0; i < batchCols.length; i++) {
            if (i == batchTsCi) { batchTypes[i] = TsdbClient.T_TIMESTAMP; continue; }
            Object v = first[i];
            if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte)
                batchTypes[i] = TsdbClient.T_INT64;
            else if (v instanceof Double || v instanceof Float)
                batchTypes[i] = TsdbClient.T_FLOAT64;
            else if (v instanceof String)
                batchTypes[i] = TsdbClient.T_SYMBOL;
            else
                throw new SQLException("addBatch: unsupported parameter type at column "
                                       + batchCols[i] + " (" + (v == null ? "null" : v.getClass().getName()) + ")");
        }

        List<TsdbClient.Column> cols = new ArrayList<>(batchCols.length);
        for (int i = 0; i < batchCols.length; i++)
            cols.add(new TsdbClient.Column(batchCols[i], batchTypes[i]));

        List<TsdbClient.Row> rows = new ArrayList<>(batchRows.size());
        for (Object[] r : batchRows) {
            TsdbClient.Row row = new TsdbClient.Row();
            Map<Integer,Long>   im = null;
            Map<Integer,Double> fm = null;
            Map<Integer,String> sm = null;
            for (int i = 0; i < r.length; i++) {
                Object v = r[i];
                switch (batchTypes[i]) {
                    case TsdbClient.T_TIMESTAMP:
                        row.ts = v == null ? 0L : ((Number) v).longValue(); break;
                    case TsdbClient.T_INT64:
                        if (im == null) im = new HashMap<>();
                        im.put(i, v == null ? 0L : ((Number) v).longValue()); break;
                    case TsdbClient.T_FLOAT64:
                        if (fm == null) fm = new HashMap<>();
                        fm.put(i, v == null ? 0.0 : ((Number) v).doubleValue()); break;
                    case TsdbClient.T_SYMBOL:
                        if (sm == null) sm = new HashMap<>();
                        sm.put(i, v == null ? "" : v.toString()); break;
                }
            }
            if (im != null) row.i64 = im;
            if (fm != null) row.f64 = fm;
            if (sm != null) row.sym = sm;
            rows.add(row);
        }

        try {
            int n = ((TsdbConnection) getConnection()).client().writeBatch(batchTable, cols, rows);
            int[] result = new int[batchRows.size()];
            Arrays.fill(result, n == batchRows.size() ? 1 : SUCCESS_NO_INFO);
            batchRows.clear();
            return result;
        } catch (IOException e) {
            throw new SQLException("writeBatch failed: " + e.getMessage(), e);
        }
    }

    /* ---- everything else: not supported ---- */
    @Override public void setAsciiStream(int p, InputStream x, int l) throws SQLException { throw nope(); }
    @Override public void setUnicodeStream(int p, InputStream x, int l) throws SQLException { throw nope(); }
    @Override public void setBinaryStream(int p, InputStream x, int l) throws SQLException { throw nope(); }
    @Override public void setCharacterStream(int p, Reader r, int l) throws SQLException { throw nope(); }
    @Override public void setRef(int p, Ref r) throws SQLException { throw nope(); }
    @Override public void setBlob(int p, Blob b) throws SQLException { throw nope(); }
    @Override public void setClob(int p, Clob c) throws SQLException { throw nope(); }
    @Override public void setArray(int p, Array a) throws SQLException { throw nope(); }
    @Override public ResultSetMetaData getMetaData() throws SQLException { throw nope(); }
    @Override public void setDate(int p, Date v, Calendar c) throws SQLException { throw nope(); }
    @Override public void setTime(int p, Time v, Calendar c) throws SQLException { throw nope(); }
    @Override public void setTimestamp(int p, Timestamp v, Calendar c) throws SQLException { throw nope(); }
    @Override public void setNull(int p, int t, String n) throws SQLException { throw nope(); }
    @Override public void setURL(int p, URL u) throws SQLException { throw nope(); }
    @Override public ParameterMetaData getParameterMetaData() throws SQLException { throw nope(); }
    @Override public void setRowId(int p, RowId r) throws SQLException { throw nope(); }
    @Override public void setNString(int p, String v) { params[p - 1] = v; }
    @Override public void setNCharacterStream(int p, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void setNClob(int p, NClob c) throws SQLException { throw nope(); }
    @Override public void setClob(int p, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void setBlob(int p, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void setNClob(int p, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void setSQLXML(int p, SQLXML x) throws SQLException { throw nope(); }
    @Override public void setAsciiStream(int p, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void setBinaryStream(int p, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void setCharacterStream(int p, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void setAsciiStream(int p, InputStream s) throws SQLException { throw nope(); }
    @Override public void setBinaryStream(int p, InputStream s) throws SQLException { throw nope(); }
    @Override public void setCharacterStream(int p, Reader r) throws SQLException { throw nope(); }
    @Override public void setNCharacterStream(int p, Reader r) throws SQLException { throw nope(); }
    @Override public void setClob(int p, Reader r) throws SQLException { throw nope(); }
    @Override public void setBlob(int p, InputStream s) throws SQLException { throw nope(); }
    @Override public void setNClob(int p, Reader r) throws SQLException { throw nope(); }

    private static SQLFeatureNotSupportedException nope() {
        return new SQLFeatureNotSupportedException();
    }
}
