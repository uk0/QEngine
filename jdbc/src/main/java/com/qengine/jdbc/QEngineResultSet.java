package com.qengine.jdbc;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Forward-only, read-only cursor. Rows are materialised in memory
 * because the protocol streams them all before returning.
 *
 * Supported column types (wire → Java):
 *   TIMESTAMP (ns since epoch)  → java.sql.Timestamp
 *   INT64                       → long
 *   FLOAT64                     → double
 *   SYMBOL                      → String (but see note in README;
 *                                 server currently sends strlen as i64)
 */
public class QEngineResultSet implements ResultSet {

    private final QEngineStatement stmt;
    private final String[]         colNames;
    private final byte[]           colTypes;
    private final List<long[]>     columns;     // column-major, raw u64 per cell
    private final int              rowCount;
    private final Map<String, Integer> nameIdx = new HashMap<>();
    private final QEngineResultSetMetaData meta;

    private int     rowPos = 0;   // 1-based when inside the set; 0 = before first
    private boolean closed;
    private boolean wasNullFlag;

    QEngineResultSet(QEngineStatement stmt, String[] names, byte[] types,
                     List<long[]> columns, int rowCount) {
        this.stmt     = stmt;
        this.colNames = names;
        this.colTypes = types;
        this.columns  = columns;
        this.rowCount = rowCount;
        for (int i = 0; i < names.length; i++) {
            nameIdx.putIfAbsent(names[i].toLowerCase(), i + 1);
        }
        this.meta = new QEngineResultSetMetaData(names, types);
    }

    // ── cursor ──

    @Override public boolean next()       { if (closed) return false; if (rowPos >= rowCount) { rowPos = rowCount + 1; return false; } rowPos++; return true; }
    @Override public void    close()      { closed = true; }
    @Override public boolean isClosed()   { return closed; }
    @Override public boolean wasNull()    { return wasNullFlag; }
    @Override public boolean isBeforeFirst() { return rowPos == 0 && rowCount > 0; }
    @Override public boolean isAfterLast()   { return rowPos > rowCount; }
    @Override public boolean isFirst()        { return rowPos == 1; }
    @Override public boolean isLast()         { return rowPos == rowCount; }
    @Override public void    beforeFirst()    { rowPos = 0; }
    @Override public void    afterLast()      { rowPos = rowCount + 1; }
    @Override public boolean first()          { if (rowCount == 0) return false; rowPos = 1; return true; }
    @Override public boolean last()           { if (rowCount == 0) return false; rowPos = rowCount; return true; }
    @Override public int     getRow()         { return rowPos > rowCount ? 0 : rowPos; }
    @Override public boolean absolute(int r)  {
        if (r <= 0 || r > rowCount) { rowPos = r <= 0 ? 0 : rowCount + 1; return false; }
        rowPos = r; return true;
    }
    @Override public boolean relative(int d)  { return absolute(rowPos + d); }
    @Override public boolean previous()       { if (rowPos <= 1) { rowPos = 0; return false; } rowPos--; return true; }

    // ── column index resolution ──

    @Override public int findColumn(String label) throws SQLException {
        Integer i = nameIdx.get(label.toLowerCase());
        if (i == null) throw new QEngineWireException("no such column: " + label);
        return i;
    }

    private void checkRow() throws SQLException {
        if (closed) throw new QEngineWireException("result set closed");
        if (rowPos < 1 || rowPos > rowCount) throw new QEngineWireException("no current row");
    }

    private long rawAt(int col) throws SQLException {
        checkRow();
        if (col < 1 || col > colTypes.length) throw new QEngineWireException("bad col idx: " + col);
        return columns.get(col - 1)[rowPos - 1];
    }

    // ── getters ──

    @Override public String getString(int c) throws SQLException {
        long raw = rawAt(c);
        wasNullFlag = false;
        switch (colTypes[c - 1]) {
            case QEngineConnection.TYPE_TIMESTAMP: return getTimestamp(c).toString();
            case QEngineConnection.TYPE_INT64:     return Long.toString(raw);
            case QEngineConnection.TYPE_FLOAT64:   return Double.toString(Double.longBitsToDouble(raw));
            case QEngineConnection.TYPE_SYMBOL:    return Long.toString(raw); // server sends strlen — documented quirk
            default: return Long.toString(raw);
        }
    }
    @Override public String getString(String l) throws SQLException { return getString(findColumn(l)); }

    @Override public boolean getBoolean(int c) throws SQLException { return rawAt(c) != 0; }
    @Override public boolean getBoolean(String l) throws SQLException { return getBoolean(findColumn(l)); }

    @Override public byte getByte(int c)    throws SQLException { return (byte) getLong(c); }
    @Override public byte getByte(String l) throws SQLException { return getByte(findColumn(l)); }

    @Override public short getShort(int c)    throws SQLException { return (short) getLong(c); }
    @Override public short getShort(String l) throws SQLException { return getShort(findColumn(l)); }

    @Override public int getInt(int c)    throws SQLException { return (int) getLong(c); }
    @Override public int getInt(String l) throws SQLException { return getInt(findColumn(l)); }

    @Override public long getLong(int c) throws SQLException {
        long raw = rawAt(c);
        wasNullFlag = false;
        byte t = colTypes[c - 1];
        if (t == QEngineConnection.TYPE_FLOAT64) return (long) Double.longBitsToDouble(raw);
        return raw;
    }
    @Override public long getLong(String l) throws SQLException { return getLong(findColumn(l)); }

    @Override public float getFloat(int c)    throws SQLException { return (float) getDouble(c); }
    @Override public float getFloat(String l) throws SQLException { return getFloat(findColumn(l)); }

    @Override public double getDouble(int c) throws SQLException {
        long raw = rawAt(c);
        wasNullFlag = false;
        byte t = colTypes[c - 1];
        if (t == QEngineConnection.TYPE_FLOAT64) return Double.longBitsToDouble(raw);
        return (double) raw;
    }
    @Override public double getDouble(String l) throws SQLException { return getDouble(findColumn(l)); }

    @Override @Deprecated public BigDecimal getBigDecimal(int c, int s) throws SQLException { return BigDecimal.valueOf(getDouble(c)).setScale(s, BigDecimal.ROUND_HALF_UP); }
    @Override @Deprecated public BigDecimal getBigDecimal(String l, int s) throws SQLException { return getBigDecimal(findColumn(l), s); }
    @Override public BigDecimal getBigDecimal(int c)    throws SQLException { return BigDecimal.valueOf(getDouble(c)); }
    @Override public BigDecimal getBigDecimal(String l) throws SQLException { return getBigDecimal(findColumn(l)); }

    @Override public byte[] getBytes(int c)    throws SQLException { return getString(c).getBytes(); }
    @Override public byte[] getBytes(String l) throws SQLException { return getBytes(findColumn(l)); }

    @Override public Date getDate(int c) throws SQLException {
        Timestamp ts = getTimestamp(c);
        return ts == null ? null : new Date(ts.getTime());
    }
    @Override public Date getDate(String l)           throws SQLException { return getDate(findColumn(l)); }
    @Override public Date getDate(int c, Calendar cal)     throws SQLException { return getDate(c); }
    @Override public Date getDate(String l, Calendar cal)  throws SQLException { return getDate(findColumn(l)); }

    @Override public Time getTime(int c) throws SQLException {
        Timestamp ts = getTimestamp(c);
        return ts == null ? null : new Time(ts.getTime());
    }
    @Override public Time getTime(String l)          throws SQLException { return getTime(findColumn(l)); }
    @Override public Time getTime(int c, Calendar cal)    throws SQLException { return getTime(c); }
    @Override public Time getTime(String l, Calendar cal) throws SQLException { return getTime(findColumn(l)); }

    @Override public Timestamp getTimestamp(int c) throws SQLException {
        long ns = rawAt(c);
        wasNullFlag = false;
        // ns since epoch → Timestamp
        long ms = ns / 1_000_000L;
        int  nanos = (int) (((ns % 1_000_000_000L) + 1_000_000_000L) % 1_000_000_000L);
        Timestamp t = new Timestamp(ms);
        t.setNanos(nanos);
        return t;
    }
    @Override public Timestamp getTimestamp(String l)           throws SQLException { return getTimestamp(findColumn(l)); }
    @Override public Timestamp getTimestamp(int c, Calendar cal)    throws SQLException { return getTimestamp(c); }
    @Override public Timestamp getTimestamp(String l, Calendar cal) throws SQLException { return getTimestamp(findColumn(l)); }

    @Override public Object getObject(int c) throws SQLException {
        switch (colTypes[c - 1]) {
            case QEngineConnection.TYPE_TIMESTAMP: return getTimestamp(c);
            case QEngineConnection.TYPE_INT64:     return getLong(c);
            case QEngineConnection.TYPE_FLOAT64:   return getDouble(c);
            case QEngineConnection.TYPE_SYMBOL:    return getString(c);
            default: return rawAt(c);
        }
    }
    @Override public Object getObject(String l) throws SQLException { return getObject(findColumn(l)); }
    @Override public Object getObject(int c, Map<String, Class<?>> m) throws SQLException { return getObject(c); }
    @Override public Object getObject(String l, Map<String, Class<?>> m) throws SQLException { return getObject(findColumn(l)); }
    @Override public <T> T getObject(int c, Class<T> type) throws SQLException {
        Object v = getObject(c);
        return type.cast(v);
    }
    @Override public <T> T getObject(String l, Class<T> type) throws SQLException { return getObject(findColumn(l), type); }

    // ── unsupported / stubs ──

    @Override public InputStream getAsciiStream(int c)    throws SQLException { throw un("asciiStream"); }
    @Override public InputStream getAsciiStream(String l) throws SQLException { throw un("asciiStream"); }
    @Override @Deprecated public InputStream getUnicodeStream(int c)    throws SQLException { throw un("unicodeStream"); }
    @Override @Deprecated public InputStream getUnicodeStream(String l) throws SQLException { throw un("unicodeStream"); }
    @Override public InputStream getBinaryStream(int c)    throws SQLException { throw un("binaryStream"); }
    @Override public InputStream getBinaryStream(String l) throws SQLException { throw un("binaryStream"); }
    @Override public Reader      getCharacterStream(int c)    throws SQLException { throw un("characterStream"); }
    @Override public Reader      getCharacterStream(String l) throws SQLException { throw un("characterStream"); }
    @Override public Reader      getNCharacterStream(int c)    throws SQLException { throw un("nCharacterStream"); }
    @Override public Reader      getNCharacterStream(String l) throws SQLException { throw un("nCharacterStream"); }
    @Override public String      getNString(int c)    throws SQLException { return getString(c); }
    @Override public String      getNString(String l) throws SQLException { return getString(findColumn(l)); }
    @Override public SQLWarning  getWarnings()          { return null; }
    @Override public void        clearWarnings()        {}
    @Override public String      getCursorName()        { return ""; }
    @Override public ResultSetMetaData getMetaData()    { return meta; }

    @Override public Ref   getRef(int c)   throws SQLException { throw un("ref"); }
    @Override public Ref   getRef(String l)throws SQLException { throw un("ref"); }
    @Override public Blob  getBlob(int c)  throws SQLException { throw un("blob"); }
    @Override public Blob  getBlob(String l)throws SQLException{ throw un("blob"); }
    @Override public Clob  getClob(int c)  throws SQLException { throw un("clob"); }
    @Override public Clob  getClob(String l)throws SQLException{ throw un("clob"); }
    @Override public NClob getNClob(int c) throws SQLException { throw un("nclob"); }
    @Override public NClob getNClob(String l)throws SQLException{ throw un("nclob"); }
    @Override public Array getArray(int c) throws SQLException { throw un("array"); }
    @Override public Array getArray(String l)throws SQLException{ throw un("array"); }
    @Override public URL   getURL(int c)   throws SQLException { throw un("url"); }
    @Override public URL   getURL(String l)throws SQLException { throw un("url"); }
    @Override public RowId getRowId(int c) throws SQLException { throw un("rowId"); }
    @Override public RowId getRowId(String l) throws SQLException { throw un("rowId"); }
    @Override public SQLXML getSQLXML(int c)  throws SQLException { throw un("sqlxml"); }
    @Override public SQLXML getSQLXML(String l)throws SQLException{ throw un("sqlxml"); }

    @Override public int  getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchDirection(int d) {}
    @Override public int  getFetchSize() { return rowCount; }
    @Override public void setFetchSize(int r) {}
    @Override public int  getType()           { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public int  getConcurrency()    { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int  getHoldability()    { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public Statement getStatement() { return stmt; }

    // ── updateXxx (forbidden — read-only) ──
    private SQLFeatureNotSupportedException un(String w) { return new SQLFeatureNotSupportedException(w); }
    @Override public boolean rowUpdated()  { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted()  { return false; }
    @Override public void insertRow()  throws SQLException { throw un("insertRow"); }
    @Override public void updateRow()  throws SQLException { throw un("updateRow"); }
    @Override public void deleteRow()  throws SQLException { throw un("deleteRow"); }
    @Override public void refreshRow() throws SQLException { throw un("refreshRow"); }
    @Override public void cancelRowUpdates() throws SQLException { throw un("cancelRowUpdates"); }
    @Override public void moveToInsertRow()  throws SQLException { throw un("moveToInsertRow"); }
    @Override public void moveToCurrentRow() throws SQLException { throw un("moveToCurrentRow"); }

    // All updateXxx variants → unsupported
    @Override public void updateNull(int c) throws SQLException { throw un("update"); }
    @Override public void updateBoolean(int c, boolean v) throws SQLException { throw un("update"); }
    @Override public void updateByte(int c, byte v) throws SQLException { throw un("update"); }
    @Override public void updateShort(int c, short v) throws SQLException { throw un("update"); }
    @Override public void updateInt(int c, int v) throws SQLException { throw un("update"); }
    @Override public void updateLong(int c, long v) throws SQLException { throw un("update"); }
    @Override public void updateFloat(int c, float v) throws SQLException { throw un("update"); }
    @Override public void updateDouble(int c, double v) throws SQLException { throw un("update"); }
    @Override public void updateBigDecimal(int c, BigDecimal v) throws SQLException { throw un("update"); }
    @Override public void updateString(int c, String v) throws SQLException { throw un("update"); }
    @Override public void updateBytes(int c, byte[] v) throws SQLException { throw un("update"); }
    @Override public void updateDate(int c, Date v) throws SQLException { throw un("update"); }
    @Override public void updateTime(int c, Time v) throws SQLException { throw un("update"); }
    @Override public void updateTimestamp(int c, Timestamp v) throws SQLException { throw un("update"); }
    @Override public void updateAsciiStream(int c, InputStream v, int l) throws SQLException { throw un("update"); }
    @Override public void updateBinaryStream(int c, InputStream v, int l) throws SQLException { throw un("update"); }
    @Override public void updateCharacterStream(int c, Reader v, int l) throws SQLException { throw un("update"); }
    @Override public void updateObject(int c, Object v, int s) throws SQLException { throw un("update"); }
    @Override public void updateObject(int c, Object v) throws SQLException { throw un("update"); }
    @Override public void updateNull(String l) throws SQLException { throw un("update"); }
    @Override public void updateBoolean(String l, boolean v) throws SQLException { throw un("update"); }
    @Override public void updateByte(String l, byte v) throws SQLException { throw un("update"); }
    @Override public void updateShort(String l, short v) throws SQLException { throw un("update"); }
    @Override public void updateInt(String l, int v) throws SQLException { throw un("update"); }
    @Override public void updateLong(String l, long v) throws SQLException { throw un("update"); }
    @Override public void updateFloat(String l, float v) throws SQLException { throw un("update"); }
    @Override public void updateDouble(String l, double v) throws SQLException { throw un("update"); }
    @Override public void updateBigDecimal(String l, BigDecimal v) throws SQLException { throw un("update"); }
    @Override public void updateString(String l, String v) throws SQLException { throw un("update"); }
    @Override public void updateBytes(String l, byte[] v) throws SQLException { throw un("update"); }
    @Override public void updateDate(String l, Date v) throws SQLException { throw un("update"); }
    @Override public void updateTime(String l, Time v) throws SQLException { throw un("update"); }
    @Override public void updateTimestamp(String l, Timestamp v) throws SQLException { throw un("update"); }
    @Override public void updateAsciiStream(String l, InputStream v, int ln) throws SQLException { throw un("update"); }
    @Override public void updateBinaryStream(String l, InputStream v, int ln) throws SQLException { throw un("update"); }
    @Override public void updateCharacterStream(String l, Reader v, int ln) throws SQLException { throw un("update"); }
    @Override public void updateObject(String l, Object v, int s) throws SQLException { throw un("update"); }
    @Override public void updateObject(String l, Object v) throws SQLException { throw un("update"); }
    @Override public void updateRef(int c, Ref v) throws SQLException { throw un("update"); }
    @Override public void updateRef(String l, Ref v) throws SQLException { throw un("update"); }
    @Override public void updateBlob(int c, Blob v) throws SQLException { throw un("update"); }
    @Override public void updateBlob(String l, Blob v) throws SQLException { throw un("update"); }
    @Override public void updateClob(int c, Clob v) throws SQLException { throw un("update"); }
    @Override public void updateClob(String l, Clob v) throws SQLException { throw un("update"); }
    @Override public void updateArray(int c, Array v) throws SQLException { throw un("update"); }
    @Override public void updateArray(String l, Array v) throws SQLException { throw un("update"); }
    @Override public void updateRowId(int c, RowId v) throws SQLException { throw un("update"); }
    @Override public void updateRowId(String l, RowId v) throws SQLException { throw un("update"); }
    @Override public void updateNString(int c, String v) throws SQLException { throw un("update"); }
    @Override public void updateNString(String l, String v) throws SQLException { throw un("update"); }
    @Override public void updateNClob(int c, NClob v) throws SQLException { throw un("update"); }
    @Override public void updateNClob(String l, NClob v) throws SQLException { throw un("update"); }
    @Override public void updateSQLXML(int c, SQLXML v) throws SQLException { throw un("update"); }
    @Override public void updateSQLXML(String l, SQLXML v) throws SQLException { throw un("update"); }
    @Override public void updateAsciiStream(int c, InputStream v) throws SQLException { throw un("update"); }
    @Override public void updateAsciiStream(int c, InputStream v, long l) throws SQLException { throw un("update"); }
    @Override public void updateAsciiStream(String l, InputStream v) throws SQLException { throw un("update"); }
    @Override public void updateAsciiStream(String l, InputStream v, long ln) throws SQLException { throw un("update"); }
    @Override public void updateBinaryStream(int c, InputStream v) throws SQLException { throw un("update"); }
    @Override public void updateBinaryStream(int c, InputStream v, long l) throws SQLException { throw un("update"); }
    @Override public void updateBinaryStream(String l, InputStream v) throws SQLException { throw un("update"); }
    @Override public void updateBinaryStream(String l, InputStream v, long ln) throws SQLException { throw un("update"); }
    @Override public void updateCharacterStream(int c, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateCharacterStream(int c, Reader v, long l) throws SQLException { throw un("update"); }
    @Override public void updateCharacterStream(String l, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateCharacterStream(String l, Reader v, long ln) throws SQLException { throw un("update"); }
    @Override public void updateNCharacterStream(int c, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateNCharacterStream(int c, Reader v, long l) throws SQLException { throw un("update"); }
    @Override public void updateNCharacterStream(String l, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateNCharacterStream(String l, Reader v, long ln) throws SQLException { throw un("update"); }
    @Override public void updateBlob(int c, InputStream v) throws SQLException { throw un("update"); }
    @Override public void updateBlob(int c, InputStream v, long l) throws SQLException { throw un("update"); }
    @Override public void updateBlob(String l, InputStream v) throws SQLException { throw un("update"); }
    @Override public void updateBlob(String l, InputStream v, long ln) throws SQLException { throw un("update"); }
    @Override public void updateClob(int c, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateClob(int c, Reader v, long l) throws SQLException { throw un("update"); }
    @Override public void updateClob(String l, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateClob(String l, Reader v, long ln) throws SQLException { throw un("update"); }
    @Override public void updateNClob(int c, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateNClob(int c, Reader v, long l) throws SQLException { throw un("update"); }
    @Override public void updateNClob(String l, Reader v) throws SQLException { throw un("update"); }
    @Override public void updateNClob(String l, Reader v, long ln) throws SQLException { throw un("update"); }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isAssignableFrom(getClass()); }

    // Dummy reference to a constant to avoid "unused import" warnings in some toolchains.
    @SuppressWarnings("unused")
    private static final int TYPES_ANY = Types.OTHER;
}
