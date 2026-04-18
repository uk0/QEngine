package com.qengine.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Trivial prepared statement: string-replaces '?' placeholders with
 * literal text per JDBC-set parameters, then dispatches as a normal
 * query. No binary parameter binding is supported.
 */
public class QEnginePreparedStatement extends QEngineStatement implements PreparedStatement {

    private final String              template;
    private final int                 placeholders;
    private final Map<Integer, String> params = new HashMap<>();

    QEnginePreparedStatement(QEngineConnection conn, String sql) {
        super(conn);
        this.template     = sql == null ? "" : sql;
        this.placeholders = countUnquotedQMarks(this.template);
    }

    private static int countUnquotedQMarks(String s) {
        int n = 0; boolean inS = false, inD = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!inD && c == '\'' && (i == 0 || s.charAt(i - 1) != '\\')) inS = !inS;
            else if (!inS && c == '"' && (i == 0 || s.charAt(i - 1) != '\\')) inD = !inD;
            else if (!inS && !inD && c == '?') n++;
        }
        return n;
    }

    private String render() throws SQLException {
        StringBuilder out = new StringBuilder(template.length() + 32);
        int idx = 0;
        boolean inS = false, inD = false;
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (!inD && c == '\'' && (i == 0 || template.charAt(i - 1) != '\\')) { inS = !inS; out.append(c); }
            else if (!inS && c == '"' && (i == 0 || template.charAt(i - 1) != '\\')) { inD = !inD; out.append(c); }
            else if (!inS && !inD && c == '?') {
                idx++;
                String v = params.get(idx);
                if (v == null) throw new QEngineWireException("parameter " + idx + " not set");
                out.append(v);
            } else out.append(c);
        }
        return out.toString();
    }

    private static String quoteString(String s) {
        if (s == null) return "NULL";
        StringBuilder b = new StringBuilder(s.length() + 2);
        b.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') b.append("''"); else b.append(c);
        }
        b.append('\'');
        return b.toString();
    }

    private void setParam(int i, String literal) throws SQLException {
        if (i < 1 || i > placeholders) {
            throw new QEngineWireException(
                    "parameter index " + i + " out of range [1.." + placeholders + "]");
        }
        params.put(i, literal);
    }

    // ── execute ──

    @Override
    public ResultSet executeQuery() throws SQLException { return executeQueryInternal(render()); }
    @Override
    public int executeUpdate() throws SQLException { executeQueryInternal(render()); return 0; }
    @Override
    public boolean execute() throws SQLException { executeQueryInternal(render()); return true; }

    @Override public void clearParameters() { params.clear(); }

    // ── setters (all reduce to a text literal) ──

    @Override public void setNull(int i, int sqlType)                       throws SQLException { setParam(i, "NULL"); }
    @Override public void setNull(int i, int sqlType, String name)          throws SQLException { setParam(i, "NULL"); }
    @Override public void setBoolean(int i, boolean v)                      throws SQLException { setParam(i, v ? "1" : "0"); }
    @Override public void setByte(int i, byte v)                            throws SQLException { setParam(i, Byte.toString(v)); }
    @Override public void setShort(int i, short v)                          throws SQLException { setParam(i, Short.toString(v)); }
    @Override public void setInt(int i, int v)                              throws SQLException { setParam(i, Integer.toString(v)); }
    @Override public void setLong(int i, long v)                            throws SQLException { setParam(i, Long.toString(v)); }
    @Override public void setFloat(int i, float v)                          throws SQLException { setParam(i, Float.toString(v)); }
    @Override public void setDouble(int i, double v)                        throws SQLException { setParam(i, Double.toString(v)); }
    @Override public void setBigDecimal(int i, BigDecimal v)                throws SQLException { setParam(i, v == null ? "NULL" : v.toPlainString()); }
    @Override public void setString(int i, String v)                        throws SQLException { setParam(i, quoteString(v)); }
    @Override public void setBytes(int i, byte[] v)                         throws SQLException {
        if (v == null) setParam(i, "NULL");
        else setParam(i, quoteString(new String(v, StandardCharsets.UTF_8)));
    }
    @Override public void setDate(int i, Date v)                            throws SQLException { setParam(i, v == null ? "NULL" : quoteString(v.toString())); }
    @Override public void setTime(int i, Time v)                            throws SQLException { setParam(i, v == null ? "NULL" : quoteString(v.toString())); }
    @Override public void setTimestamp(int i, Timestamp v)                  throws SQLException {
        if (v == null) { setParam(i, "NULL"); return; }
        // nanoseconds since epoch as integer literal
        long nanos = v.getTime() * 1_000_000L + (v.getNanos() % 1_000_000L);
        setParam(i, Long.toString(nanos));
    }
    @Override public void setObject(int i, Object v)                        throws SQLException {
        if (v == null) { setParam(i, "NULL"); return; }
        if (v instanceof Number)    { setParam(i, v.toString()); return; }
        if (v instanceof Boolean)   { setBoolean(i, (Boolean) v); return; }
        if (v instanceof Timestamp) { setTimestamp(i, (Timestamp) v); return; }
        setString(i, v.toString());
    }
    @Override public void setObject(int i, Object v, int sqlType)           throws SQLException { setObject(i, v); }
    @Override public void setObject(int i, Object v, int sqlType, int sc)   throws SQLException { setObject(i, v); }

    @Override public void setDate(int i, Date v, Calendar cal)              throws SQLException { setDate(i, v); }
    @Override public void setTime(int i, Time v, Calendar cal)              throws SQLException { setTime(i, v); }
    @Override public void setTimestamp(int i, Timestamp v, Calendar cal)    throws SQLException { setTimestamp(i, v); }

    // ── unsupported stream / LOB setters ──
    private SQLFeatureNotSupportedException un(String w) { return new SQLFeatureNotSupportedException(w); }

    @Override public void setAsciiStream(int i, InputStream v, int l)   throws SQLException { throw un("asciiStream"); }
    @Override public void setAsciiStream(int i, InputStream v, long l)  throws SQLException { throw un("asciiStream"); }
    @Override public void setAsciiStream(int i, InputStream v)          throws SQLException { throw un("asciiStream"); }
    @Override public void setUnicodeStream(int i, InputStream v, int l) throws SQLException { throw un("unicodeStream"); }
    @Override public void setBinaryStream(int i, InputStream v, int l)  throws SQLException { throw un("binaryStream"); }
    @Override public void setBinaryStream(int i, InputStream v, long l) throws SQLException { throw un("binaryStream"); }
    @Override public void setBinaryStream(int i, InputStream v)         throws SQLException { throw un("binaryStream"); }
    @Override public void setCharacterStream(int i, Reader v, int l)    throws SQLException { throw un("characterStream"); }
    @Override public void setCharacterStream(int i, Reader v, long l)   throws SQLException { throw un("characterStream"); }
    @Override public void setCharacterStream(int i, Reader v)           throws SQLException { throw un("characterStream"); }
    @Override public void setNCharacterStream(int i, Reader v, long l)  throws SQLException { throw un("nCharacterStream"); }
    @Override public void setNCharacterStream(int i, Reader v)          throws SQLException { throw un("nCharacterStream"); }
    @Override public void setRef(int i, Ref v)                          throws SQLException { throw un("ref"); }
    @Override public void setBlob(int i, Blob v)                        throws SQLException { throw un("blob"); }
    @Override public void setBlob(int i, InputStream v)                 throws SQLException { throw un("blob"); }
    @Override public void setBlob(int i, InputStream v, long l)         throws SQLException { throw un("blob"); }
    @Override public void setClob(int i, Clob v)                        throws SQLException { throw un("clob"); }
    @Override public void setClob(int i, Reader v)                      throws SQLException { throw un("clob"); }
    @Override public void setClob(int i, Reader v, long l)              throws SQLException { throw un("clob"); }
    @Override public void setNClob(int i, NClob v)                      throws SQLException { throw un("nclob"); }
    @Override public void setNClob(int i, Reader v)                     throws SQLException { throw un("nclob"); }
    @Override public void setNClob(int i, Reader v, long l)             throws SQLException { throw un("nclob"); }
    @Override public void setArray(int i, Array v)                      throws SQLException { throw un("array"); }
    @Override public void setURL(int i, URL v)                          throws SQLException { throw un("url"); }
    @Override public void setRowId(int i, RowId v)                      throws SQLException { throw un("rowId"); }
    @Override public void setNString(int i, String v)                   throws SQLException { setString(i, v); }
    @Override public void setSQLXML(int i, SQLXML v)                    throws SQLException { throw un("sqlxml"); }

    // ── metadata ──

    @Override public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("prepared metadata");
    }

    @Override public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("parameter metadata");
    }

    @Override public void addBatch() throws SQLException { throw new SQLFeatureNotSupportedException("batch"); }
}
