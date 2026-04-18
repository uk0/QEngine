package com.tsdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

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

    /* ---- everything else: not supported ---- */
    @Override public void setAsciiStream(int p, InputStream x, int l) throws SQLException { throw nope(); }
    @Override public void setUnicodeStream(int p, InputStream x, int l) throws SQLException { throw nope(); }
    @Override public void setBinaryStream(int p, InputStream x, int l) throws SQLException { throw nope(); }
    @Override public void addBatch() throws SQLException { throw nope(); }
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
