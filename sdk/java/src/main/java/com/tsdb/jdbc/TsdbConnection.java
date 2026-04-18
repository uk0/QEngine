package com.tsdb.jdbc;

import com.tsdb.client.TsdbClient;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Minimal JDBC Connection wrapping a {@link TsdbClient}.  Only the methods
 * needed for a basic executeQuery workflow are implemented; everything else
 * throws {@link SQLFeatureNotSupportedException}.
 */
public class TsdbConnection implements Connection {

    private final TsdbClient client;
    private boolean autoCommit = true;
    private boolean closed = false;

    public TsdbConnection(String host, int port, String user, String pass) throws SQLException {
        try {
            this.client = new TsdbClient(host, port);
            if (user != null && pass != null) this.client.login(user, pass);
        } catch (IOException e) {
            throw new SQLException("connect failed: " + e.getMessage(), e);
        }
    }

    public TsdbClient client() { return client; }

    @Override public Statement createStatement() throws SQLException {
        checkOpen();
        return new TsdbStatement(this);
    }

    @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return new TsdbPreparedStatement(this, sql);
    }

    @Override public void close() throws SQLException {
        if (closed) return;
        closed = true;
        try { client.close(); } catch (IOException e) { /* ignore */ }
    }

    @Override public boolean isClosed() { return closed; }

    @Override public boolean getAutoCommit() { return autoCommit; }
    @Override public void setAutoCommit(boolean v) { this.autoCommit = v; }
    @Override public void commit() {}
    @Override public void rollback() {}

    @Override public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    @Override public String getCatalog() { return ""; }
    @Override public void setCatalog(String c) {}
    @Override public void setTransactionIsolation(int level) {}
    @Override public int getTransactionIsolation() { return Connection.TRANSACTION_READ_COMMITTED; }

    /* ---- Everything else — SQLFeatureNotSupportedException --- */

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("connection closed");
    }

    @Override public CallableStatement prepareCall(String sql) throws SQLException { throw nope(); }
    @Override public String nativeSQL(String sql) { return sql; }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public Statement createStatement(int a, int b) throws SQLException { return createStatement(); }
    @Override public PreparedStatement prepareStatement(String s, int a, int b) throws SQLException { return prepareStatement(s); }
    @Override public CallableStatement prepareCall(String s, int a, int b) throws SQLException { throw nope(); }
    @Override public Map<String, Class<?>> getTypeMap() throws SQLException { throw nope(); }
    @Override public void setTypeMap(Map<String, Class<?>> m) throws SQLException { throw nope(); }
    @Override public void setHoldability(int h) {}
    @Override public int getHoldability() { return 0; }
    @Override public Savepoint setSavepoint() throws SQLException { throw nope(); }
    @Override public Savepoint setSavepoint(String n) throws SQLException { throw nope(); }
    @Override public void rollback(Savepoint s) throws SQLException { throw nope(); }
    @Override public void releaseSavepoint(Savepoint s) throws SQLException { throw nope(); }
    @Override public Statement createStatement(int a, int b, int c) throws SQLException { return createStatement(); }
    @Override public PreparedStatement prepareStatement(String s, int a, int b, int c) throws SQLException { return prepareStatement(s); }
    @Override public CallableStatement prepareCall(String s, int a, int b, int c) throws SQLException { throw nope(); }
    @Override public PreparedStatement prepareStatement(String s, int[] k) throws SQLException { return prepareStatement(s); }
    @Override public PreparedStatement prepareStatement(String s, String[] k) throws SQLException { return prepareStatement(s); }
    @Override public PreparedStatement prepareStatement(String s, int a) throws SQLException { return prepareStatement(s); }
    @Override public Clob createClob() throws SQLException { throw nope(); }
    @Override public Blob createBlob() throws SQLException { throw nope(); }
    @Override public NClob createNClob() throws SQLException { throw nope(); }
    @Override public SQLXML createSQLXML() throws SQLException { throw nope(); }
    @Override public boolean isValid(int t) { return !closed; }
    @Override public void setClientInfo(String n, String v) {}
    @Override public void setClientInfo(Properties p) {}
    @Override public String getClientInfo(String n) { return null; }
    @Override public Properties getClientInfo() { return new Properties(); }
    @Override public Array createArrayOf(String t, Object[] e) throws SQLException { throw nope(); }
    @Override public Struct createStruct(String t, Object[] a) throws SQLException { throw nope(); }
    @Override public void setSchema(String s) {}
    @Override public String getSchema() { return ""; }
    @Override public void abort(Executor e) { closed = true; }
    @Override public void setNetworkTimeout(Executor e, int t) {}
    @Override public int getNetworkTimeout() { return 0; }
    @Override public void setReadOnly(boolean b) {}
    @Override public boolean isReadOnly() { return false; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw nope();
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private static SQLFeatureNotSupportedException nope() {
        return new SQLFeatureNotSupportedException();
    }
}
