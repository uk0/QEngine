package dev.tsdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;

/**
 * Minimal {@link PreparedStatement} — the server has no bind protocol
 * so we substitute client-side.  Placeholders ({@code ?}) are replaced
 * in declaration order with SQL literals appropriate to the Java type
 * (numbers unquoted, strings single-quoted + doubled-up on any inner
 * quote, timestamps rendered as nanosecond longs).
 *
 * <p>Escaping is best-effort; callers handling untrusted input should
 * still prefer explicit quoting.
 */
public final class TSDBPreparedStatement extends TSDBStatement implements PreparedStatement {

    private final String template;
    private final int    nParams;
    private final Object[] params;

    TSDBPreparedStatement(TSDBConnection conn, String sql) {
        super(conn);
        this.template = sql;
        this.nParams = countPlaceholders(sql);
        this.params = new Object[nParams];
    }

    private static int countPlaceholders(String sql) {
        int n = 0;
        boolean inStr = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && (i == 0 || sql.charAt(i - 1) != '\\')) inStr = !inStr;
            else if (!inStr && c == '?') n++;
        }
        return n;
    }

    /** Walk the template, replacing ? (outside string literals) with
     *  bound values rendered as SQL literals. */
    String render() throws SQLException {
        StringBuilder sb = new StringBuilder(template.length() + 32);
        int idx = 0;
        boolean inStr = false;
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (c == '\'' && (i == 0 || template.charAt(i - 1) != '\\')) {
                inStr = !inStr;
                sb.append(c);
            } else if (!inStr && c == '?') {
                if (idx >= nParams)
                    throw new SQLException("too many '?' placeholders");
                sb.append(renderLiteral(params[idx]));
                idx++;
            } else {
                sb.append(c);
            }
        }
        if (idx != nParams)
            throw new SQLException("parameter " + (idx + 1) + " unbound");
        return sb.toString();
    }

    private static String renderLiteral(Object v) {
        if (v == null) return "NULL";
        if (v instanceof Number)  return v.toString();
        if (v instanceof Boolean) return ((Boolean) v) ? "TRUE" : "FALSE";
        if (v instanceof Timestamp) {
            // Nanosecond epoch is what tsdb stores; JDBC Timestamp is ms + nanos.
            long nanos = ((Timestamp) v).getTime() / 1000L * 1_000_000_000L +
                         ((Timestamp) v).getNanos();
            return Long.toString(nanos);
        }
        if (v instanceof Date || v instanceof Time)
            return "'" + v.toString() + "'";
        // Default: quote + double-escape single quote.
        String s = v.toString().replace("'", "''");
        return "'" + s + "'";
    }

    /* ── executions ───────────────────────────────────────────────── */
    @Override public ResultSet executeQuery() throws SQLException { return super.executeQuery(render()); }
    @Override public int       executeUpdate() throws SQLException { return super.executeUpdate(render()); }
    @Override public boolean   execute() throws SQLException { return super.execute(render()); }
    @Override public void      addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("JDBC addBatch not supported; use an explicit INSERT/VALUES statement");
    }

    /* ── parameter setters ────────────────────────────────────────── */
    private void set(int idx, Object v) throws SQLException {
        if (idx < 1 || idx > nParams)
            throw new SQLException("parameter index " + idx + " out of range [1.." + nParams + "]");
        params[idx - 1] = v;
    }

    @Override public void setNull(int idx, int sqlType) throws SQLException { set(idx, null); }
    @Override public void setNull(int idx, int sqlType, String n) throws SQLException { set(idx, null); }

    @Override public void setBoolean(int idx, boolean v) throws SQLException { set(idx, v); }
    @Override public void setByte(int idx, byte v)       throws SQLException { set(idx, v); }
    @Override public void setShort(int idx, short v)     throws SQLException { set(idx, v); }
    @Override public void setInt(int idx, int v)         throws SQLException { set(idx, v); }
    @Override public void setLong(int idx, long v)       throws SQLException { set(idx, v); }
    @Override public void setFloat(int idx, float v)     throws SQLException { set(idx, v); }
    @Override public void setDouble(int idx, double v)   throws SQLException { set(idx, v); }
    @Override public void setBigDecimal(int idx, BigDecimal v) throws SQLException { set(idx, v); }
    @Override public void setString(int idx, String v)   throws SQLException { set(idx, v); }
    @Override public void setDate(int idx, Date v)       throws SQLException { set(idx, v); }
    @Override public void setTime(int idx, Time v)       throws SQLException { set(idx, v); }
    @Override public void setTimestamp(int idx, Timestamp v) throws SQLException { set(idx, v); }
    @Override public void setDate(int idx, Date v, Calendar cal)       throws SQLException { set(idx, v); }
    @Override public void setTime(int idx, Time v, Calendar cal)       throws SQLException { set(idx, v); }
    @Override public void setTimestamp(int idx, Timestamp v, Calendar cal) throws SQLException { set(idx, v); }

    @Override public void setObject(int idx, Object v)                 throws SQLException { set(idx, v); }
    @Override public void setObject(int idx, Object v, int sqlType)    throws SQLException { set(idx, v); }
    @Override public void setObject(int idx, Object v, int sqlType, int scale) throws SQLException { set(idx, v); }

    /* ── everything else: NotSupported ───────────────────────────── */
    @Override public void clearParameters() { Arrays.fill(params, null); }

    @Override public void setBytes(int i, byte[] v)           throws SQLException { nope(); }
    @Override public void setBinaryStream(int i, InputStream is) throws SQLException { nope(); }
    @Override public void setBinaryStream(int i, InputStream is, int n)  throws SQLException { nope(); }
    @Override public void setBinaryStream(int i, InputStream is, long n) throws SQLException { nope(); }
    @Override public void setAsciiStream(int i, InputStream is)  throws SQLException { nope(); }
    @Override public void setAsciiStream(int i, InputStream is, int n)  throws SQLException { nope(); }
    @Override public void setAsciiStream(int i, InputStream is, long n) throws SQLException { nope(); }
    @Override public void setCharacterStream(int i, Reader r)  throws SQLException { nope(); }
    @Override public void setCharacterStream(int i, Reader r, int n)  throws SQLException { nope(); }
    @Override public void setCharacterStream(int i, Reader r, long n) throws SQLException { nope(); }
    @Override public void setNCharacterStream(int i, Reader r) throws SQLException { nope(); }
    @Override public void setNCharacterStream(int i, Reader r, long n) throws SQLException { nope(); }
    @Override public void setUnicodeStream(int i, InputStream is, int n) throws SQLException { nope(); }
    @Override public void setRef(int i, Ref r)              throws SQLException { nope(); }
    @Override public void setBlob(int i, Blob b)            throws SQLException { nope(); }
    @Override public void setBlob(int i, InputStream is)    throws SQLException { nope(); }
    @Override public void setBlob(int i, InputStream is, long n) throws SQLException { nope(); }
    @Override public void setClob(int i, Clob c)            throws SQLException { nope(); }
    @Override public void setClob(int i, Reader r)          throws SQLException { nope(); }
    @Override public void setClob(int i, Reader r, long n)  throws SQLException { nope(); }
    @Override public void setNClob(int i, NClob c)          throws SQLException { nope(); }
    @Override public void setNClob(int i, Reader r)         throws SQLException { nope(); }
    @Override public void setNClob(int i, Reader r, long n) throws SQLException { nope(); }
    @Override public void setArray(int i, Array a)          throws SQLException { nope(); }
    @Override public void setURL(int i, URL u)              throws SQLException { nope(); }
    @Override public void setRowId(int i, RowId r)          throws SQLException { nope(); }
    @Override public void setSQLXML(int i, SQLXML x)        throws SQLException { nope(); }
    @Override public void setNString(int i, String v)       throws SQLException { set(i, v); }

    @Override public ResultSetMetaData getMetaData() throws SQLException { nope(); return null; }
    @Override public ParameterMetaData getParameterMetaData() throws SQLException { nope(); return null; }

    private static void nope() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("unsupported PreparedStatement operation");
    }
}
