package com.tsdb.jdbc;

import com.tsdb.client.TsdbClient;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * Minimal JDBC ResultSet wrapping a {@link TsdbClient.QueryResult}.
 * Cursor starts before the first row; {@code next()} advances it.
 * Only forward-only, read-only access is supported.
 */
public class TsdbResultSet implements ResultSet {

    private final Statement stmt;
    private final TsdbClient.QueryResult qr;
    private int cursor = -1;
    private boolean closed = false;
    private boolean lastWasNull = false;

    public TsdbResultSet(Statement s, TsdbClient.QueryResult qr) {
        this.stmt = s; this.qr = qr;
    }

    @Override public boolean next() {
        if (closed) return false;
        cursor++;
        return cursor < qr.rows.size();
    }

    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }

    @Override public boolean wasNull() { return lastWasNull; }

    private Object get(int col1) throws SQLException {
        if (cursor < 0 || cursor >= qr.rows.size())
            throw new SQLException("no current row");
        Object[] row = qr.rows.get(cursor);
        if (col1 < 1 || col1 > row.length)
            throw new SQLException("bad column: " + col1);
        Object v = row[col1 - 1];
        lastWasNull = (v == null);
        return v;
    }

    @Override public String getString(int c) throws SQLException {
        Object v = get(c);
        return v == null ? null : v.toString();
    }

    @Override public long getLong(int c) throws SQLException {
        Object v = get(c);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).longValue();
        return Long.parseLong(v.toString());
    }

    @Override public int getInt(int c) throws SQLException {
        return (int) getLong(c);
    }

    @Override public double getDouble(int c) throws SQLException {
        Object v = get(c);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).doubleValue();
        return Double.parseDouble(v.toString());
    }

    @Override public float getFloat(int c) throws SQLException { return (float) getDouble(c); }
    @Override public boolean getBoolean(int c) throws SQLException { return getInt(c) != 0; }
    @Override public byte getByte(int c) throws SQLException { return (byte) getInt(c); }
    @Override public short getShort(int c) throws SQLException { return (short) getInt(c); }

    @Override public Timestamp getTimestamp(int c) throws SQLException {
        long ns = getLong(c);
        long ms = ns / 1_000_000L;
        Timestamp ts = new Timestamp(ms);
        ts.setNanos((int) (ns % 1_000_000_000L));
        return ts;
    }

    @Override public Object getObject(int c) throws SQLException { return get(c); }

    @Override public ResultSetMetaData getMetaData() {
        return new ResultSetMetaData() {
            @Override public int getColumnCount() { return qr.colNames.length; }
            @Override public String getColumnName(int c) { return qr.colNames[c - 1]; }
            @Override public String getColumnLabel(int c) { return qr.colNames[c - 1]; }
            @Override public int getColumnType(int c) {
                switch (qr.colTypes[c - 1]) {
                    case TsdbClient.T_TIMESTAMP: return Types.TIMESTAMP;
                    case TsdbClient.T_INT64:     return Types.BIGINT;
                    case TsdbClient.T_FLOAT64:   return Types.DOUBLE;
                    case TsdbClient.T_SYMBOL:    return Types.INTEGER;
                }
                return Types.OTHER;
            }
            @Override public String getColumnTypeName(int c) {
                switch (qr.colTypes[c - 1]) {
                    case TsdbClient.T_TIMESTAMP: return "TIMESTAMP";
                    case TsdbClient.T_INT64:     return "INT64";
                    case TsdbClient.T_FLOAT64:   return "FLOAT64";
                    case TsdbClient.T_SYMBOL:    return "SYMBOL";
                }
                return "OTHER";
            }
            @Override public int getColumnDisplaySize(int c) { return 32; }
            @Override public String getSchemaName(int c) { return ""; }
            @Override public int getPrecision(int c) { return 0; }
            @Override public int getScale(int c) { return 0; }
            @Override public String getTableName(int c) { return ""; }
            @Override public String getCatalogName(int c) { return ""; }
            @Override public boolean isAutoIncrement(int c) { return false; }
            @Override public boolean isCaseSensitive(int c) { return false; }
            @Override public boolean isSearchable(int c) { return true; }
            @Override public boolean isCurrency(int c) { return false; }
            @Override public int isNullable(int c) { return columnNullableUnknown; }
            @Override public boolean isSigned(int c) { return true; }
            @Override public String getColumnClassName(int c) { return Object.class.getName(); }
            @Override public boolean isReadOnly(int c) { return true; }
            @Override public boolean isWritable(int c) { return false; }
            @Override public boolean isDefinitelyWritable(int c) { return false; }
            @Override public <T> T unwrap(Class<T> i) { return null; }
            @Override public boolean isWrapperFor(Class<?> i) { return false; }
        };
    }

    @Override public int findColumn(String name) throws SQLException {
        for (int i = 0; i < qr.colNames.length; i++)
            if (qr.colNames[i].equalsIgnoreCase(name)) return i + 1;
        throw new SQLException("no such column: " + name);
    }

    @Override public String getString(String n) throws SQLException { return getString(findColumn(n)); }
    @Override public long   getLong  (String n) throws SQLException { return getLong  (findColumn(n)); }
    @Override public int    getInt   (String n) throws SQLException { return getInt   (findColumn(n)); }
    @Override public double getDouble(String n) throws SQLException { return getDouble(findColumn(n)); }
    @Override public Timestamp getTimestamp(String n) throws SQLException { return getTimestamp(findColumn(n)); }
    @Override public Object getObject(String n) throws SQLException { return getObject(findColumn(n)); }

    @Override public Statement getStatement() { return stmt; }

    /* ---- Everything else: minimally compliant stubs ---- */

    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public String getCursorName() { return ""; }
    @Override public boolean isBeforeFirst() { return cursor < 0; }
    @Override public boolean isAfterLast() { return cursor >= qr.rows.size(); }
    @Override public boolean isFirst() { return cursor == 0; }
    @Override public boolean isLast() { return cursor == qr.rows.size() - 1; }
    @Override public void beforeFirst() { cursor = -1; }
    @Override public void afterLast() { cursor = qr.rows.size(); }
    @Override public boolean first() { cursor = 0; return !qr.rows.isEmpty(); }
    @Override public boolean last() { cursor = qr.rows.size() - 1; return !qr.rows.isEmpty(); }
    @Override public int getRow() { return cursor + 1; }
    @Override public boolean absolute(int row) { cursor = row - 1; return cursor >= 0 && cursor < qr.rows.size(); }
    @Override public boolean relative(int d) { cursor += d; return cursor >= 0 && cursor < qr.rows.size(); }
    @Override public boolean previous() { cursor--; return cursor >= 0; }
    @Override public void setFetchDirection(int d) {}
    @Override public int getFetchDirection() { return FETCH_FORWARD; }
    @Override public void setFetchSize(int r) {}
    @Override public int getFetchSize() { return 0; }
    @Override public int getType() { return TYPE_SCROLL_INSENSITIVE; }
    @Override public int getConcurrency() { return CONCUR_READ_ONLY; }
    @Override public int getHoldability() { return HOLD_CURSORS_OVER_COMMIT; }

    /* ---- Unsupported: throw SQLFeatureNotSupportedException ---- */

    private static SQLFeatureNotSupportedException nope() {
        return new SQLFeatureNotSupportedException();
    }

    @Override public BigDecimal getBigDecimal(int c) throws SQLException { throw nope(); }
    @Override public BigDecimal getBigDecimal(int c, int s) throws SQLException { throw nope(); }
    @Override public BigDecimal getBigDecimal(String n) throws SQLException { throw nope(); }
    @Override public BigDecimal getBigDecimal(String n, int s) throws SQLException { throw nope(); }
    @Override public byte[] getBytes(int c) throws SQLException { throw nope(); }
    @Override public byte[] getBytes(String n) throws SQLException { throw nope(); }
    @Override public Date getDate(int c) throws SQLException { throw nope(); }
    @Override public Date getDate(String n) throws SQLException { throw nope(); }
    @Override public Date getDate(int c, Calendar cal) throws SQLException { throw nope(); }
    @Override public Date getDate(String n, Calendar cal) throws SQLException { throw nope(); }
    @Override public Time getTime(int c) throws SQLException { throw nope(); }
    @Override public Time getTime(String n) throws SQLException { throw nope(); }
    @Override public Time getTime(int c, Calendar cal) throws SQLException { throw nope(); }
    @Override public Time getTime(String n, Calendar cal) throws SQLException { throw nope(); }
    @Override public Timestamp getTimestamp(int c, Calendar cal) throws SQLException { throw nope(); }
    @Override public Timestamp getTimestamp(String n, Calendar cal) throws SQLException { throw nope(); }
    @Override public InputStream getAsciiStream(int c) throws SQLException { throw nope(); }
    @Override public InputStream getAsciiStream(String n) throws SQLException { throw nope(); }
    @Override public InputStream getUnicodeStream(int c) throws SQLException { throw nope(); }
    @Override public InputStream getUnicodeStream(String n) throws SQLException { throw nope(); }
    @Override public InputStream getBinaryStream(int c) throws SQLException { throw nope(); }
    @Override public InputStream getBinaryStream(String n) throws SQLException { throw nope(); }
    @Override public Reader getCharacterStream(int c) throws SQLException { throw nope(); }
    @Override public Reader getCharacterStream(String n) throws SQLException { throw nope(); }
    @Override public byte   getByte(String n)    throws SQLException { return (byte) getInt(n); }
    @Override public short  getShort(String n)   throws SQLException { return (short) getInt(n); }
    @Override public boolean getBoolean(String n) throws SQLException { return getBoolean(findColumn(n)); }
    @Override public float  getFloat(String n)   throws SQLException { return (float) getDouble(n); }
    @Override public Object getObject(int c, Map<String, Class<?>> m) throws SQLException { throw nope(); }
    @Override public Object getObject(String n, Map<String, Class<?>> m) throws SQLException { throw nope(); }
    @Override public Ref getRef(int c) throws SQLException { throw nope(); }
    @Override public Ref getRef(String n) throws SQLException { throw nope(); }
    @Override public Blob getBlob(int c) throws SQLException { throw nope(); }
    @Override public Blob getBlob(String n) throws SQLException { throw nope(); }
    @Override public Clob getClob(int c) throws SQLException { throw nope(); }
    @Override public Clob getClob(String n) throws SQLException { throw nope(); }
    @Override public Array getArray(int c) throws SQLException { throw nope(); }
    @Override public Array getArray(String n) throws SQLException { throw nope(); }
    @Override public URL getURL(int c) throws SQLException { throw nope(); }
    @Override public URL getURL(String n) throws SQLException { throw nope(); }
    @Override public <T> T getObject(int c, Class<T> t) throws SQLException { throw nope(); }
    @Override public <T> T getObject(String n, Class<T> t) throws SQLException { throw nope(); }
    @Override public RowId getRowId(int c) throws SQLException { throw nope(); }
    @Override public RowId getRowId(String n) throws SQLException { throw nope(); }
    @Override public NClob getNClob(int c) throws SQLException { throw nope(); }
    @Override public NClob getNClob(String n) throws SQLException { throw nope(); }
    @Override public SQLXML getSQLXML(int c) throws SQLException { throw nope(); }
    @Override public SQLXML getSQLXML(String n) throws SQLException { throw nope(); }
    @Override public String getNString(int c) throws SQLException { return getString(c); }
    @Override public String getNString(String n) throws SQLException { return getString(n); }
    @Override public Reader getNCharacterStream(int c) throws SQLException { throw nope(); }
    @Override public Reader getNCharacterStream(String n) throws SQLException { throw nope(); }

    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted()  { return false; }
    @Override public void insertRow() throws SQLException { throw nope(); }
    @Override public void updateRow() throws SQLException { throw nope(); }
    @Override public void deleteRow() throws SQLException { throw nope(); }
    @Override public void refreshRow() throws SQLException { throw nope(); }
    @Override public void cancelRowUpdates() throws SQLException { throw nope(); }
    @Override public void moveToInsertRow() throws SQLException { throw nope(); }
    @Override public void moveToCurrentRow() throws SQLException { throw nope(); }

    /* A long tail of updateXxx stubs — all throw. */
    @Override public void updateNull(int c) throws SQLException { throw nope(); }
    @Override public void updateBoolean(int c, boolean v) throws SQLException { throw nope(); }
    @Override public void updateByte(int c, byte v) throws SQLException { throw nope(); }
    @Override public void updateShort(int c, short v) throws SQLException { throw nope(); }
    @Override public void updateInt(int c, int v) throws SQLException { throw nope(); }
    @Override public void updateLong(int c, long v) throws SQLException { throw nope(); }
    @Override public void updateFloat(int c, float v) throws SQLException { throw nope(); }
    @Override public void updateDouble(int c, double v) throws SQLException { throw nope(); }
    @Override public void updateBigDecimal(int c, BigDecimal v) throws SQLException { throw nope(); }
    @Override public void updateString(int c, String v) throws SQLException { throw nope(); }
    @Override public void updateBytes(int c, byte[] v) throws SQLException { throw nope(); }
    @Override public void updateDate(int c, Date v) throws SQLException { throw nope(); }
    @Override public void updateTime(int c, Time v) throws SQLException { throw nope(); }
    @Override public void updateTimestamp(int c, Timestamp v) throws SQLException { throw nope(); }
    @Override public void updateAsciiStream(int c, InputStream v, int l) throws SQLException { throw nope(); }
    @Override public void updateBinaryStream(int c, InputStream v, int l) throws SQLException { throw nope(); }
    @Override public void updateCharacterStream(int c, Reader v, int l) throws SQLException { throw nope(); }
    @Override public void updateObject(int c, Object v, int s) throws SQLException { throw nope(); }
    @Override public void updateObject(int c, Object v) throws SQLException { throw nope(); }
    @Override public void updateNull(String n) throws SQLException { throw nope(); }
    @Override public void updateBoolean(String n, boolean v) throws SQLException { throw nope(); }
    @Override public void updateByte(String n, byte v) throws SQLException { throw nope(); }
    @Override public void updateShort(String n, short v) throws SQLException { throw nope(); }
    @Override public void updateInt(String n, int v) throws SQLException { throw nope(); }
    @Override public void updateLong(String n, long v) throws SQLException { throw nope(); }
    @Override public void updateFloat(String n, float v) throws SQLException { throw nope(); }
    @Override public void updateDouble(String n, double v) throws SQLException { throw nope(); }
    @Override public void updateBigDecimal(String n, BigDecimal v) throws SQLException { throw nope(); }
    @Override public void updateString(String n, String v) throws SQLException { throw nope(); }
    @Override public void updateBytes(String n, byte[] v) throws SQLException { throw nope(); }
    @Override public void updateDate(String n, Date v) throws SQLException { throw nope(); }
    @Override public void updateTime(String n, Time v) throws SQLException { throw nope(); }
    @Override public void updateTimestamp(String n, Timestamp v) throws SQLException { throw nope(); }
    @Override public void updateAsciiStream(String n, InputStream v, int l) throws SQLException { throw nope(); }
    @Override public void updateBinaryStream(String n, InputStream v, int l) throws SQLException { throw nope(); }
    @Override public void updateCharacterStream(String n, Reader v, int l) throws SQLException { throw nope(); }
    @Override public void updateObject(String n, Object v, int s) throws SQLException { throw nope(); }
    @Override public void updateObject(String n, Object v) throws SQLException { throw nope(); }
    @Override public void updateRef(int c, Ref r) throws SQLException { throw nope(); }
    @Override public void updateRef(String n, Ref r) throws SQLException { throw nope(); }
    @Override public void updateBlob(int c, Blob b) throws SQLException { throw nope(); }
    @Override public void updateBlob(String n, Blob b) throws SQLException { throw nope(); }
    @Override public void updateClob(int c, Clob v) throws SQLException { throw nope(); }
    @Override public void updateClob(String n, Clob v) throws SQLException { throw nope(); }
    @Override public void updateArray(int c, Array a) throws SQLException { throw nope(); }
    @Override public void updateArray(String n, Array a) throws SQLException { throw nope(); }
    @Override public void updateRowId(int c, RowId r) throws SQLException { throw nope(); }
    @Override public void updateRowId(String n, RowId r) throws SQLException { throw nope(); }
    @Override public void updateNString(int c, String v) throws SQLException { throw nope(); }
    @Override public void updateNString(String n, String v) throws SQLException { throw nope(); }
    @Override public void updateNClob(int c, NClob v) throws SQLException { throw nope(); }
    @Override public void updateNClob(String n, NClob v) throws SQLException { throw nope(); }
    @Override public void updateSQLXML(int c, SQLXML v) throws SQLException { throw nope(); }
    @Override public void updateSQLXML(String n, SQLXML v) throws SQLException { throw nope(); }
    @Override public void updateNCharacterStream(int c, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateNCharacterStream(String n, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateAsciiStream(int c, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void updateBinaryStream(int c, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void updateCharacterStream(int c, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateAsciiStream(String n, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void updateBinaryStream(String n, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void updateCharacterStream(String n, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateBlob(int c, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void updateBlob(String n, InputStream s, long l) throws SQLException { throw nope(); }
    @Override public void updateClob(int c, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateClob(String n, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateNClob(int c, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateNClob(String n, Reader r, long l) throws SQLException { throw nope(); }
    @Override public void updateNCharacterStream(int c, Reader r) throws SQLException { throw nope(); }
    @Override public void updateNCharacterStream(String n, Reader r) throws SQLException { throw nope(); }
    @Override public void updateAsciiStream(int c, InputStream s) throws SQLException { throw nope(); }
    @Override public void updateBinaryStream(int c, InputStream s) throws SQLException { throw nope(); }
    @Override public void updateCharacterStream(int c, Reader r) throws SQLException { throw nope(); }
    @Override public void updateAsciiStream(String n, InputStream s) throws SQLException { throw nope(); }
    @Override public void updateBinaryStream(String n, InputStream s) throws SQLException { throw nope(); }
    @Override public void updateCharacterStream(String n, Reader r) throws SQLException { throw nope(); }
    @Override public void updateBlob(int c, InputStream s) throws SQLException { throw nope(); }
    @Override public void updateBlob(String n, InputStream s) throws SQLException { throw nope(); }
    @Override public void updateClob(int c, Reader r) throws SQLException { throw nope(); }
    @Override public void updateClob(String n, Reader r) throws SQLException { throw nope(); }
    @Override public void updateNClob(int c, Reader r) throws SQLException { throw nope(); }
    @Override public void updateNClob(String n, Reader r) throws SQLException { throw nope(); }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw nope();
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
