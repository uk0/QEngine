package com.qengine.jdbc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal JDBC Statement: sends MSG_QUERY and reassembles a QEngineResultSet.
 * No transactions, no batch updates, no fetch size tuning.
 */
public class QEngineStatement implements Statement {

    protected final QEngineConnection conn;
    protected QEngineResultSet lastResult;
    protected boolean closed;
    protected int maxRows;     // 0 = no cap
    protected int queryTimeout; // seconds; not enforced — socket timeout governs

    QEngineStatement(QEngineConnection conn) { this.conn = conn; }

    // ── core query flow ──

    QEngineResultSet executeQueryInternal(String qtl) throws SQLException {
        if (qtl == null) throw new QEngineWireException("query is null");
        byte[] q = qtl.getBytes(StandardCharsets.UTF_8);
        if (q.length > 65535) throw new QEngineWireException("query too long (>65535 bytes)");

        // Payload: [qtl_len u16 LE][qtl utf8]
        ByteBuffer body = ByteBuffer.allocate(2 + q.length).order(ByteOrder.LITTLE_ENDIAN);
        body.putShort((short) q.length);
        body.put(q);

        long reqId = conn.nextRequestId();
        try {
            conn.sendFrame(QEngineConnection.MSG_QUERY, (short) 0, reqId, body.array());
        } catch (IOException e) {
            throw new QEngineWireException("failed to send QUERY", e);
        }

        // Receive HDR first, then zero or more ROWS until FIN.
        String[] colNames = null;
        byte[]   colTypes = null;
        List<long[]> columns = null;       // one long[] per column (raw 8-byte values)
        int totalRows = 0;

        while (true) {
            QEngineConnection.Frame f;
            try {
                f = conn.recvFrame();
            } catch (IOException e) {
                throw new QEngineWireException("failed to recv frame", e);
            }
            if (f.type == QEngineConnection.MSG_ERROR) {
                throw QEngineConnection.decodeServerError(f);
            }
            if (f.type == QEngineConnection.MSG_QUERY_RESULT_HDR) {
                // Header payload: [ncols u16][(name_len u8)(name)(type u8)]*
                ByteBuffer bb = ByteBuffer.wrap(f.payload).order(ByteOrder.LITTLE_ENDIAN);
                int ncols = bb.getShort() & 0xFFFF;
                colNames = new String[ncols];
                colTypes = new byte[ncols];
                for (int i = 0; i < ncols; i++) {
                    int nl = bb.get() & 0xFF;
                    byte[] name = new byte[nl];
                    bb.get(name);
                    colNames[i] = new String(name, StandardCharsets.UTF_8);
                    colTypes[i] = bb.get();
                }
                columns = new ArrayList<>(ncols);
                for (int i = 0; i < ncols; i++) columns.add(null); // lazy init on first chunk
            } else if (f.type == QEngineConnection.MSG_QUERY_RESULT_ROWS) {
                if (colNames == null) throw new QEngineWireException("ROWS before HDR");
                // Server format: [nrows u32 LE][ncols u16 LE][col0: nrows*8][col1: nrows*8]...
                ByteBuffer bb = ByteBuffer.wrap(f.payload).order(ByteOrder.LITTLE_ENDIAN);
                int nrows = bb.getInt();
                int ncols = bb.getShort() & 0xFFFF;
                if (ncols != colNames.length) {
                    throw new QEngineWireException(
                            "ncols mismatch hdr=" + colNames.length + " rows=" + ncols);
                }
                if (nrows > 0 && (maxRows == 0 || totalRows < maxRows)) {
                    for (int c = 0; c < ncols; c++) {
                        long[] existing = columns.get(c);
                        int newLen = (existing == null ? 0 : existing.length) + nrows;
                        long[] merged = new long[newLen];
                        if (existing != null) System.arraycopy(existing, 0, merged, 0, existing.length);
                        int base = (existing == null ? 0 : existing.length);
                        for (int r = 0; r < nrows; r++) {
                            merged[base + r] = bb.getLong();
                        }
                        columns.set(c, merged);
                    }
                    totalRows += nrows;
                } else {
                    // skip payload bytes
                    for (int c = 0; c < ncols; c++) {
                        for (int r = 0; r < nrows; r++) bb.getLong();
                    }
                }
                if ((f.flags & QEngineConnection.FLAG_FIN) != 0) break;
            } else {
                throw new QEngineWireException("unexpected frame type=" + f.type);
            }
        }

        // Materialise missing column arrays as zero-length.
        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) == null) columns.set(i, new long[0]);
            }
        }
        if (colNames == null) {
            colNames = new String[0]; colTypes = new byte[0]; columns = new ArrayList<>();
        }
        int cap = maxRows > 0 ? Math.min(maxRows, totalRows) : totalRows;
        lastResult = new QEngineResultSet(this, colNames, colTypes, columns, cap);
        return lastResult;
    }

    // ── Statement API ──

    @Override public ResultSet executeQuery(String sql) throws SQLException { return executeQueryInternal(sql); }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        // DDL/DML flows through QUERY too; server-side row count is not returned.
        executeQueryInternal(sql);
        return 0;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQueryInternal(sql);
        return true;
    }

    @Override public ResultSet getResultSet() { return lastResult; }
    @Override public int getUpdateCount() { return -1; }
    @Override public boolean getMoreResults() { return false; }
    @Override public boolean getMoreResults(int cur) { return false; }

    @Override public void close() throws SQLException {
        closed = true;
        if (lastResult != null) lastResult.close();
    }

    @Override public boolean isClosed() { return closed; }
    @Override public int getMaxFieldSize() { return 0; }
    @Override public void setMaxFieldSize(int max) {}
    @Override public int getMaxRows() { return maxRows; }
    @Override public void setMaxRows(int max) { this.maxRows = Math.max(0, max); }
    @Override public void setEscapeProcessing(boolean enable) {}
    @Override public int getQueryTimeout() { return queryTimeout; }
    @Override public void setQueryTimeout(int seconds) { this.queryTimeout = seconds; }
    @Override public void cancel() throws SQLException { throw new SQLFeatureNotSupportedException("cancel"); }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public void setCursorName(String name) {}
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchDirection(int d) {}
    @Override public int getFetchSize() { return 0; }
    @Override public void setFetchSize(int rows) {}
    @Override public int getResultSetConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int getResultSetType() { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public int getResultSetHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }

    @Override public void addBatch(String sql)       throws SQLException { throw new SQLFeatureNotSupportedException("batch"); }
    @Override public void clearBatch()               throws SQLException { throw new SQLFeatureNotSupportedException("batch"); }
    @Override public int[] executeBatch()            throws SQLException { throw new SQLFeatureNotSupportedException("batch"); }
    @Override public Connection getConnection()       { return conn; }

    @Override public int executeUpdate(String sql, int autoKeys)    throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, int[] cols)      throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, String[] cols)   throws SQLException { return executeUpdate(sql); }
    @Override public boolean execute(String sql, int autoKeys)      throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, int[] cols)        throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, String[] cols)     throws SQLException { return execute(sql); }
    @Override public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("generated keys");
    }
    @Override public boolean isPoolable() { return false; }
    @Override public void setPoolable(boolean p) {}
    @Override public void closeOnCompletion() {}
    @Override public boolean isCloseOnCompletion() { return false; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isAssignableFrom(getClass()); }
}
