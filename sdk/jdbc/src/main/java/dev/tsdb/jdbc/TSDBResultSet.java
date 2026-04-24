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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Forward-only {@link ResultSet} backed by the JSON shape returned by
 * {@code POST /sql}:
 *
 * <pre>
 * { "cols": [...], "types": [...], "rows": [[...], ...],
 *   "nrows": N, "truncated": bool, "ms": N }
 * </pre>
 *
 * or an error object:
 *
 * <pre>{ "error": "…", "ms": N }</pre>
 *
 * which this class turns into {@link SQLException}.
 */
public final class TSDBResultSet implements ResultSet {

    private final Statement origin;
    private final String[]  cols;
    private final String[]  types;
    private final List<Object> rows;     // each element is List<Object>
    private int cursor = -1;              // -1 = before first
    private boolean closed;
    private boolean lastWasNull;

    /** Milliseconds the server reported for the query. */
    public final long serverMs;
    /** {@code true} if the server capped the row count. */
    public final boolean truncated;

    private TSDBResultSet(Statement origin, String[] cols, String[] types,
                           List<Object> rows, long ms, boolean trunc) {
        this.origin = origin;
        this.cols = cols;
        this.types = types;
        this.rows = rows;
        this.serverMs = ms;
        this.truncated = trunc;
    }

    static TSDBResultSet fromJson(Statement origin, String body) throws SQLException {
        Map<String, Object> obj;
        try { obj = Json.asObject(Json.parse(body)); }
        catch (RuntimeException e) { throw new SQLException("cannot parse /sql response: " + e.getMessage(), e); }

        if (obj.containsKey("error"))
            throw new SQLException(String.valueOf(obj.get("error")));

        List<Object> c  = Json.asArray(obj.getOrDefault("cols",  java.util.Collections.emptyList()));
        List<Object> tp = Json.asArray(obj.getOrDefault("types", java.util.Collections.emptyList()));
        List<Object> rs = Json.asArray(obj.getOrDefault("rows",  java.util.Collections.emptyList()));
        String[] ca = new String[c.size()];
        String[] ta = new String[tp.size()];
        for (int i = 0; i < c.size();  i++) ca[i] = Json.asString(c.get(i));
        for (int i = 0; i < tp.size(); i++) ta[i] = Json.asString(tp.get(i));

        long ms = 0;
        Object v = obj.get("ms");
        if (v instanceof Number) ms = ((Number) v).longValue();
        Object tr = obj.get("truncated");
        boolean truncated = tr instanceof Boolean && (Boolean) tr;

        return new TSDBResultSet(origin, ca, ta, rs, ms, truncated);
    }

    @Override public boolean next() throws SQLException {
        ensureOpen();
        cursor++;
        return cursor < rows.size();
    }

    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }
    @Override public Statement getStatement() { return origin; }
    @Override public ResultSetMetaData getMetaData() { return new TSDBResultSetMetaData(cols, types); }
    @Override public int  getFetchSize()           { return rows.size(); }
    @Override public void setFetchSize(int rows)   { /* no-op */ }
    @Override public int  getFetchDirection()      { return FETCH_FORWARD; }
    @Override public void setFetchDirection(int d) { /* only FORWARD */ }
    @Override public int  getType()                { return TYPE_FORWARD_ONLY; }
    @Override public int  getConcurrency()         { return CONCUR_READ_ONLY; }
    @Override public int  getHoldability()         { return CLOSE_CURSORS_AT_COMMIT; }
    @Override public boolean wasNull()             { return lastWasNull; }
    @Override public SQLWarning getWarnings()      { return null; }
    @Override public void       clearWarnings()    { /* no-op */ }
    @Override public int findColumn(String name) throws SQLException {
        for (int i = 0; i < cols.length; i++)
            if (cols[i].equalsIgnoreCase(name)) return i + 1;
        throw new SQLException("no such column: " + name);
    }
    @Override public String getCursorName() { return null; }

    /* ── row retrieval by index ─────────────────────────────────── */

    private Object cell(int idx) throws SQLException {
        if (cursor < 0) throw new SQLException("ResultSet: call next() first");
        if (cursor >= rows.size()) throw new SQLException("ResultSet: after last row");
        if (idx < 1 || idx > cols.length)
            throw new SQLException("column index " + idx + " out of range [1.." + cols.length + "]");
        Object row = rows.get(cursor);
        Object v = ((List<?>) row).get(idx - 1);
        lastWasNull = (v == null);
        return v;
    }

    @Override public String     getString(int i)     throws SQLException { Object v = cell(i); return v == null ? null : String.valueOf(v); }
    @Override public boolean    getBoolean(int i)    throws SQLException {
        Object v = cell(i);
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof Number)  return ((Number) v).longValue() != 0;
        String s = String.valueOf(v).trim().toLowerCase();
        return s.equals("t") || s.equals("true") || s.equals("1") || s.equals("y");
    }
    @Override public byte    getByte(int i)    throws SQLException { return (byte)  getLong(i); }
    @Override public short   getShort(int i)   throws SQLException { return (short) getLong(i); }
    @Override public int     getInt(int i)     throws SQLException { return (int)   getLong(i); }
    @Override public long    getLong(int i)    throws SQLException {
        Object v = cell(i);
        if (v == null) return 0L;
        if (v instanceof Number) return ((Number) v).longValue();
        return Long.parseLong(String.valueOf(v));
    }
    @Override public float   getFloat(int i)   throws SQLException { return (float) getDouble(i); }
    @Override public double  getDouble(int i)  throws SQLException {
        Object v = cell(i);
        if (v == null) return 0.0d;
        if (v instanceof Number) return ((Number) v).doubleValue();
        return Double.parseDouble(String.valueOf(v));
    }
    @Override public BigDecimal getBigDecimal(int i) throws SQLException {
        Object v = cell(i);
        return v == null ? null : new BigDecimal(String.valueOf(v));
    }
    @Override public BigDecimal getBigDecimal(int i, int scale) throws SQLException { return getBigDecimal(i); }
    @Override public byte[]  getBytes(int i) throws SQLException { String s = getString(i); return s == null ? null : s.getBytes(); }

    @Override public Timestamp getTimestamp(int i) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        long ns = v instanceof Number ? ((Number) v).longValue() : Long.parseLong(String.valueOf(v));
        Timestamp t = new Timestamp(ns / 1_000_000L);
        t.setNanos((int) (ns % 1_000_000_000L));
        return t;
    }
    @Override public Timestamp getTimestamp(int i, Calendar cal) throws SQLException { return getTimestamp(i); }
    @Override public Date getDate(int i) throws SQLException { Timestamp t = getTimestamp(i); return t == null ? null : new Date(t.getTime()); }
    @Override public Date getDate(int i, Calendar cal) throws SQLException { return getDate(i); }
    @Override public Time getTime(int i) throws SQLException { Timestamp t = getTimestamp(i); return t == null ? null : new Time(t.getTime()); }
    @Override public Time getTime(int i, Calendar cal) throws SQLException { return getTime(i); }

    @Override public Object getObject(int i) throws SQLException { return cell(i); }
    @Override public Object getObject(int i, Map<String, Class<?>> map) throws SQLException { return cell(i); }
    @Override public <T> T  getObject(int i, Class<T> type) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        if (type.isInstance(v)) return type.cast(v);
        // Small convenience conversions.
        if (type == String.class)    return type.cast(String.valueOf(v));
        if (type == Long.class)      return type.cast(getLong(i));
        if (type == Integer.class)   return type.cast(getInt(i));
        if (type == Double.class)    return type.cast(getDouble(i));
        if (type == Boolean.class)   return type.cast(getBoolean(i));
        if (type == Timestamp.class) return type.cast(getTimestamp(i));
        throw new SQLException("cannot convert " + v.getClass().getName() + " to " + type.getName());
    }

    /* ── by name delegates ─────────────────────────────────────── */
    @Override public String     getString    (String n) throws SQLException { return getString(findColumn(n)); }
    @Override public boolean    getBoolean   (String n) throws SQLException { return getBoolean(findColumn(n)); }
    @Override public byte       getByte      (String n) throws SQLException { return getByte(findColumn(n)); }
    @Override public short      getShort     (String n) throws SQLException { return getShort(findColumn(n)); }
    @Override public int        getInt       (String n) throws SQLException { return getInt(findColumn(n)); }
    @Override public long       getLong      (String n) throws SQLException { return getLong(findColumn(n)); }
    @Override public float      getFloat     (String n) throws SQLException { return getFloat(findColumn(n)); }
    @Override public double     getDouble    (String n) throws SQLException { return getDouble(findColumn(n)); }
    @Override public BigDecimal getBigDecimal(String n) throws SQLException { return getBigDecimal(findColumn(n)); }
    @Override public BigDecimal getBigDecimal(String n, int s) throws SQLException { return getBigDecimal(findColumn(n)); }
    @Override public byte[]     getBytes     (String n) throws SQLException { return getBytes(findColumn(n)); }
    @Override public Date       getDate      (String n) throws SQLException { return getDate(findColumn(n)); }
    @Override public Date       getDate      (String n, Calendar c) throws SQLException { return getDate(findColumn(n)); }
    @Override public Time       getTime      (String n) throws SQLException { return getTime(findColumn(n)); }
    @Override public Time       getTime      (String n, Calendar c) throws SQLException { return getTime(findColumn(n)); }
    @Override public Timestamp  getTimestamp (String n) throws SQLException { return getTimestamp(findColumn(n)); }
    @Override public Timestamp  getTimestamp (String n, Calendar c) throws SQLException { return getTimestamp(findColumn(n)); }
    @Override public Object     getObject    (String n) throws SQLException { return getObject(findColumn(n)); }
    @Override public Object     getObject    (String n, Map<String, Class<?>> m) throws SQLException { return getObject(findColumn(n)); }
    @Override public <T> T      getObject    (String n, Class<T> type) throws SQLException { return getObject(findColumn(n), type); }

    /* ── unsupported streaming / LOB / update corners ─────────── */

    @Override public InputStream getAsciiStream (int i) throws SQLException { nope(); return null; }
    @Override public InputStream getUnicodeStream(int i) throws SQLException { nope(); return null; }
    @Override public InputStream getBinaryStream(int i) throws SQLException { nope(); return null; }
    @Override public InputStream getAsciiStream (String n) throws SQLException { nope(); return null; }
    @Override public InputStream getUnicodeStream(String n) throws SQLException { nope(); return null; }
    @Override public InputStream getBinaryStream(String n) throws SQLException { nope(); return null; }
    @Override public Reader      getCharacterStream(int i) throws SQLException { nope(); return null; }
    @Override public Reader      getCharacterStream(String n) throws SQLException { nope(); return null; }
    @Override public Reader      getNCharacterStream(int i) throws SQLException { nope(); return null; }
    @Override public Reader      getNCharacterStream(String n) throws SQLException { nope(); return null; }
    @Override public String      getNString(int i)    throws SQLException { return getString(i); }
    @Override public String      getNString(String n) throws SQLException { return getString(n); }
    @Override public Ref    getRef(int i) throws SQLException { nope(); return null; }
    @Override public Ref    getRef(String n) throws SQLException { nope(); return null; }
    @Override public Blob   getBlob(int i) throws SQLException { nope(); return null; }
    @Override public Blob   getBlob(String n) throws SQLException { nope(); return null; }
    @Override public Clob   getClob(int i) throws SQLException { nope(); return null; }
    @Override public Clob   getClob(String n) throws SQLException { nope(); return null; }
    @Override public NClob  getNClob(int i) throws SQLException { nope(); return null; }
    @Override public NClob  getNClob(String n) throws SQLException { nope(); return null; }
    @Override public Array  getArray(int i) throws SQLException { nope(); return null; }
    @Override public Array  getArray(String n) throws SQLException { nope(); return null; }
    @Override public URL    getURL(int i) throws SQLException { nope(); return null; }
    @Override public URL    getURL(String n) throws SQLException { nope(); return null; }
    @Override public RowId  getRowId(int i) throws SQLException { nope(); return null; }
    @Override public RowId  getRowId(String n) throws SQLException { nope(); return null; }
    @Override public SQLXML getSQLXML(int i) throws SQLException { nope(); return null; }
    @Override public SQLXML getSQLXML(String n) throws SQLException { nope(); return null; }

    @Override public boolean isBeforeFirst() { return cursor < 0; }
    @Override public boolean isAfterLast()   { return cursor >= rows.size(); }
    @Override public boolean isFirst()       { return cursor == 0; }
    @Override public boolean isLast()        { return cursor == rows.size() - 1; }
    @Override public void beforeFirst() throws SQLException { nope(); }
    @Override public void afterLast()   throws SQLException { nope(); }
    @Override public boolean first()    throws SQLException { nope(); return false; }
    @Override public boolean last()     throws SQLException { nope(); return false; }
    @Override public int     getRow()   { return cursor + 1; }
    @Override public boolean absolute(int row)  throws SQLException { nope(); return false; }
    @Override public boolean relative(int rows) throws SQLException { nope(); return false; }
    @Override public boolean previous() throws SQLException { nope(); return false; }

    /* read-only — all update* methods throw */
    @Override public boolean rowUpdated()  { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted()  { return false; }
    @Override public void insertRow() throws SQLException { nope(); }
    @Override public void updateRow() throws SQLException { nope(); }
    @Override public void deleteRow() throws SQLException { nope(); }
    @Override public void refreshRow()    throws SQLException { nope(); }
    @Override public void cancelRowUpdates() throws SQLException { nope(); }
    @Override public void moveToInsertRow()  throws SQLException { nope(); }
    @Override public void moveToCurrentRow() throws SQLException { nope(); }
    @Override public void updateNull(int c) throws SQLException { nope(); }
    @Override public void updateBoolean(int c, boolean v) throws SQLException { nope(); }
    @Override public void updateByte(int c, byte v) throws SQLException { nope(); }
    @Override public void updateShort(int c, short v) throws SQLException { nope(); }
    @Override public void updateInt(int c, int v) throws SQLException { nope(); }
    @Override public void updateLong(int c, long v) throws SQLException { nope(); }
    @Override public void updateFloat(int c, float v) throws SQLException { nope(); }
    @Override public void updateDouble(int c, double v) throws SQLException { nope(); }
    @Override public void updateBigDecimal(int c, BigDecimal v) throws SQLException { nope(); }
    @Override public void updateString(int c, String v) throws SQLException { nope(); }
    @Override public void updateBytes(int c, byte[] v) throws SQLException { nope(); }
    @Override public void updateDate(int c, Date v) throws SQLException { nope(); }
    @Override public void updateTime(int c, Time v) throws SQLException { nope(); }
    @Override public void updateTimestamp(int c, Timestamp v) throws SQLException { nope(); }
    @Override public void updateAsciiStream(int c, InputStream v, int n) throws SQLException { nope(); }
    @Override public void updateBinaryStream(int c, InputStream v, int n) throws SQLException { nope(); }
    @Override public void updateCharacterStream(int c, Reader v, int n) throws SQLException { nope(); }
    @Override public void updateObject(int c, Object v, int s) throws SQLException { nope(); }
    @Override public void updateObject(int c, Object v) throws SQLException { nope(); }
    @Override public void updateNull(String c) throws SQLException { nope(); }
    @Override public void updateBoolean(String c, boolean v) throws SQLException { nope(); }
    @Override public void updateByte(String c, byte v) throws SQLException { nope(); }
    @Override public void updateShort(String c, short v) throws SQLException { nope(); }
    @Override public void updateInt(String c, int v) throws SQLException { nope(); }
    @Override public void updateLong(String c, long v) throws SQLException { nope(); }
    @Override public void updateFloat(String c, float v) throws SQLException { nope(); }
    @Override public void updateDouble(String c, double v) throws SQLException { nope(); }
    @Override public void updateBigDecimal(String c, BigDecimal v) throws SQLException { nope(); }
    @Override public void updateString(String c, String v) throws SQLException { nope(); }
    @Override public void updateBytes(String c, byte[] v) throws SQLException { nope(); }
    @Override public void updateDate(String c, Date v) throws SQLException { nope(); }
    @Override public void updateTime(String c, Time v) throws SQLException { nope(); }
    @Override public void updateTimestamp(String c, Timestamp v) throws SQLException { nope(); }
    @Override public void updateAsciiStream(String c, InputStream v, int n) throws SQLException { nope(); }
    @Override public void updateBinaryStream(String c, InputStream v, int n) throws SQLException { nope(); }
    @Override public void updateCharacterStream(String c, Reader v, int n) throws SQLException { nope(); }
    @Override public void updateObject(String c, Object v, int s) throws SQLException { nope(); }
    @Override public void updateObject(String c, Object v) throws SQLException { nope(); }
    @Override public void updateRef(int c, Ref v)    throws SQLException { nope(); }
    @Override public void updateRef(String c, Ref v) throws SQLException { nope(); }
    @Override public void updateBlob(int c, Blob v)  throws SQLException { nope(); }
    @Override public void updateBlob(String c, Blob v) throws SQLException { nope(); }
    @Override public void updateClob(int c, Clob v)  throws SQLException { nope(); }
    @Override public void updateClob(String c, Clob v) throws SQLException { nope(); }
    @Override public void updateArray(int c, Array v) throws SQLException { nope(); }
    @Override public void updateArray(String c, Array v) throws SQLException { nope(); }
    @Override public void updateRowId(int c, RowId v) throws SQLException { nope(); }
    @Override public void updateRowId(String c, RowId v) throws SQLException { nope(); }
    @Override public void updateNString(int c, String v) throws SQLException { nope(); }
    @Override public void updateNString(String c, String v) throws SQLException { nope(); }
    @Override public void updateNClob(int c, NClob v) throws SQLException { nope(); }
    @Override public void updateNClob(String c, NClob v) throws SQLException { nope(); }
    @Override public void updateSQLXML(int c, SQLXML v) throws SQLException { nope(); }
    @Override public void updateSQLXML(String c, SQLXML v) throws SQLException { nope(); }
    @Override public void updateNCharacterStream(int c, Reader r, long n) throws SQLException { nope(); }
    @Override public void updateNCharacterStream(String c, Reader r, long n) throws SQLException { nope(); }
    @Override public void updateAsciiStream(int c, InputStream v)            throws SQLException { nope(); }
    @Override public void updateAsciiStream(String c, InputStream v)         throws SQLException { nope(); }
    @Override public void updateAsciiStream(int c, InputStream v, long n)    throws SQLException { nope(); }
    @Override public void updateAsciiStream(String c, InputStream v, long n) throws SQLException { nope(); }
    @Override public void updateBinaryStream(int c, InputStream v)            throws SQLException { nope(); }
    @Override public void updateBinaryStream(String c, InputStream v)         throws SQLException { nope(); }
    @Override public void updateBinaryStream(int c, InputStream v, long n)    throws SQLException { nope(); }
    @Override public void updateBinaryStream(String c, InputStream v, long n) throws SQLException { nope(); }
    @Override public void updateCharacterStream(int c, Reader r)              throws SQLException { nope(); }
    @Override public void updateCharacterStream(String c, Reader r)           throws SQLException { nope(); }
    @Override public void updateCharacterStream(int c, Reader r, long n)      throws SQLException { nope(); }
    @Override public void updateCharacterStream(String c, Reader r, long n)   throws SQLException { nope(); }
    @Override public void updateBlob(int c, InputStream v)             throws SQLException { nope(); }
    @Override public void updateBlob(String c, InputStream v)          throws SQLException { nope(); }
    @Override public void updateBlob(int c, InputStream v, long n)     throws SQLException { nope(); }
    @Override public void updateBlob(String c, InputStream v, long n)  throws SQLException { nope(); }
    @Override public void updateClob(int c, Reader r)          throws SQLException { nope(); }
    @Override public void updateClob(String c, Reader r)       throws SQLException { nope(); }
    @Override public void updateClob(int c, Reader r, long n)  throws SQLException { nope(); }
    @Override public void updateClob(String c, Reader r, long n) throws SQLException { nope(); }
    @Override public void updateNClob(int c, Reader r)         throws SQLException { nope(); }
    @Override public void updateNClob(String c, Reader r)      throws SQLException { nope(); }
    @Override public void updateNClob(int c, Reader r, long n) throws SQLException { nope(); }
    @Override public void updateNClob(String c, Reader r, long n) throws SQLException { nope(); }
    @Override public void updateNCharacterStream(int c, Reader r) throws SQLException { nope(); }
    @Override public void updateNCharacterStream(String c, Reader r) throws SQLException { nope(); }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName());
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }
    private static void nope() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("unsupported ResultSet operation");
    }

    /** tsdb type name → java.sql.Types constant (used by metadata). */
    static int sqlTypeOf(String t) {
        if (t == null) return Types.OTHER;
        switch (t.toUpperCase()) {
            case "TIMESTAMP": return Types.TIMESTAMP;
            case "INT64":     return Types.BIGINT;
            case "FLOAT64":   return Types.DOUBLE;
            case "SYMBOL":    return Types.VARCHAR;
            case "BOOL":      return Types.BOOLEAN;
            default:          return Types.OTHER;
        }
    }
}
