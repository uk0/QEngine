package com.tsdb.jdbc;

import com.tsdb.client.TsdbClient;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * Minimal JDBC Statement — executeQuery returns a {@link TsdbResultSet}
 * populated from the wire QueryResult.  executeUpdate returns 0 for DDL
 * (server status rows aren't row-counts anyway).
 */
public class TsdbStatement implements Statement {

    protected final TsdbConnection conn;
    protected TsdbResultSet lastResult;
    protected boolean closed = false;

    public TsdbStatement(TsdbConnection c) { this.conn = c; }

    @Override public ResultSet executeQuery(String sql) throws SQLException {
        try {
            TsdbClient.QueryResult qr = conn.client().query(sql);
            lastResult = new TsdbResultSet(this, qr);
            return lastResult;
        } catch (IOException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override public int executeUpdate(String sql) throws SQLException {
        try {
            conn.client().query(sql);   // DDL returns a status row
            return 0;
        } catch (IOException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override public ResultSet getResultSet() { return lastResult; }
    @Override public int getUpdateCount() { return -1; }
    @Override public boolean getMoreResults() { return false; }
    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }
    @Override public Connection getConnection() { return conn; }

    /* ---- unsupported but JDK-signature-compliant ---- */

    @Override public int getMaxFieldSize() { return 0; }
    @Override public void setMaxFieldSize(int m) {}
    @Override public int getMaxRows() { return 0; }
    @Override public void setMaxRows(int m) {}
    @Override public void setEscapeProcessing(boolean e) {}
    @Override public int getQueryTimeout() { return 0; }
    @Override public void setQueryTimeout(int t) {}
    @Override public void cancel() {}
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public void setCursorName(String n) {}
    @Override public void setFetchDirection(int d) {}
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchSize(int r) {}
    @Override public int getFetchSize() { return 0; }
    @Override public int getResultSetConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int getResultSetType() { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public void addBatch(String s) throws SQLException { throw nope(); }
    @Override public void clearBatch() {}
    @Override public int[] executeBatch() throws SQLException { return new int[0]; }
    @Override public boolean getMoreResults(int c) { return false; }
    @Override public ResultSet getGeneratedKeys() throws SQLException { throw nope(); }
    @Override public int executeUpdate(String s, int a) throws SQLException { return executeUpdate(s); }
    @Override public int executeUpdate(String s, int[] a) throws SQLException { return executeUpdate(s); }
    @Override public int executeUpdate(String s, String[] a) throws SQLException { return executeUpdate(s); }
    @Override public boolean execute(String s, int a) throws SQLException { return execute(s); }
    @Override public boolean execute(String s, int[] a) throws SQLException { return execute(s); }
    @Override public boolean execute(String s, String[] a) throws SQLException { return execute(s); }
    @Override public int getResultSetHoldability() { return 0; }
    @Override public void setPoolable(boolean p) {}
    @Override public boolean isPoolable() { return false; }
    @Override public void closeOnCompletion() {}
    @Override public boolean isCloseOnCompletion() { return false; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw nope();
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private static SQLFeatureNotSupportedException nope() {
        return new SQLFeatureNotSupportedException();
    }
}
