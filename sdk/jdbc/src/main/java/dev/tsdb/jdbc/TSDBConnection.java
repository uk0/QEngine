package dev.tsdb.jdbc;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * JDBC Connection backed by the tsdb {@code /sql} HTTP endpoint.
 *
 * <p>Auto-commit is always on — tsdb has no multi-statement
 * transactions, every {@code CREATE / DROP / ALTER / INSERT} lands at
 * commit time.  {@link #setAutoCommit(boolean)} therefore only accepts
 * {@code true}; passing {@code false} throws
 * {@link SQLFeatureNotSupportedException} so ORMs that blindly disable
 * auto-commit fail fast rather than silently losing a rollback.
 *
 * <p>{@link #isReadOnly()} is advisory — the driver doesn't enforce it,
 * but {@link TSDBStatement} refuses mutating statements when set.
 */
public final class TSDBConnection implements Connection {

    private final String url;
    private final HttpClient http;
    private final Map<String, String> props;

    private boolean closed;
    private String catalog;
    private boolean readOnly;

    TSDBConnection(String url, Map<String, String> props) throws SQLException {
        this.url = url;
        this.props = props;
        String host = req("host");
        int port = Integer.parseInt(req("port"));
        boolean tls = "true".equalsIgnoreCase(props.getOrDefault("tls", port == 443 ? "true" : "false"));
        int timeout = Integer.parseInt(props.getOrDefault("timeout_ms", "30000"));
        String scheme = tls ? "https" : "http";
        this.http = new HttpClient(scheme + "://" + host + ":" + port, timeout);
        this.catalog = props.getOrDefault("db", "");
        try {
            http.login(props.getOrDefault("user", "root"),
                       props.getOrDefault("pass", ""));
        } catch (IOException e) {
            throw new SQLException("connect " + url + " failed: " + e.getMessage(), e);
        }
    }

    private String req(String k) throws SQLException {
        String v = props.get(k);
        if (v == null) throw new SQLException("missing property " + k);
        return v;
    }

    HttpClient http() { return http; }
    boolean readOnlyFlag() { return readOnly; }

    /* ── core API ─────────────────────────────────────────────────── */

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();
        return new TSDBStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        ensureOpen();
        return new TSDBPreparedStatement(this, sql);
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        closed = true;
        http.logout();
    }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public DatabaseMetaData getMetaData() {
        return new TSDBDatabaseMetaData(this, url);
    }

    @Override
    public boolean getAutoCommit() { return true; }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (!autoCommit)
            throw new SQLFeatureNotSupportedException(
                "tsdb has no multi-statement transactions; auto-commit is always on");
    }

    @Override public void commit()   { /* no-op */ }
    @Override public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback not supported");
    }

    @Override public String  getCatalog() { return catalog; }
    @Override public void    setCatalog(String c) { this.catalog = c == null ? "" : c; }
    @Override public boolean isReadOnly() { return readOnly; }
    @Override public void    setReadOnly(boolean r) { this.readOnly = r; }

    @Override public String   getSchema() { return catalog; }
    @Override public void     setSchema(String s) { this.catalog = s == null ? "" : s; }

    @Override public int      getHoldability() { return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public int      getTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public void     setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE)
            throw new SQLFeatureNotSupportedException("tsdb has no transaction isolation levels");
    }

    @Override public void       clearWarnings() { /* no-op */ }
    @Override public SQLWarning getWarnings()   { return null; }
    @Override public boolean    isValid(int timeout) {
        return !closed;
    }
    @Override public Properties getClientInfo() { return new Properties(); }
    @Override public String     getClientInfo(String name) { return null; }
    @Override public void setClientInfo(String name, String value) throws SQLClientInfoException {
        /* no-op */
    }
    @Override public void setClientInfo(Properties p) throws SQLClientInfoException { /* no-op */ }

    private void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("connection is closed");
    }

    /* ── unsupported JDBC corners (throw consistently) ───────────── */

    @Override public CallableStatement prepareCall(String sql) throws SQLException { nope(); return null; }
    @Override public CallableStatement prepareCall(String sql, int a, int b) throws SQLException { nope(); return null; }
    @Override public CallableStatement prepareCall(String sql, int a, int b, int c) throws SQLException { nope(); return null; }
    @Override public String nativeSQL(String sql) { return sql; }

    @Override public PreparedStatement prepareStatement(String sql, int a, int b) throws SQLException { return prepareStatement(sql); }
    @Override public PreparedStatement prepareStatement(String sql, int a, int b, int c) throws SQLException { return prepareStatement(sql); }
    @Override public PreparedStatement prepareStatement(String sql, int autoGenKeys) throws SQLException { return prepareStatement(sql); }
    @Override public PreparedStatement prepareStatement(String sql, int[] cols)     throws SQLException { return prepareStatement(sql); }
    @Override public PreparedStatement prepareStatement(String sql, String[] cols)  throws SQLException { return prepareStatement(sql); }

    @Override public Statement createStatement(int a, int b)        throws SQLException { return createStatement(); }
    @Override public Statement createStatement(int a, int b, int c) throws SQLException { return createStatement(); }

    @Override public Map<String, Class<?>> getTypeMap() { return java.util.Collections.emptyMap(); }
    @Override public void setTypeMap(Map<String, Class<?>> m) { /* no-op */ }

    @Override public void setHoldability(int h) { /* no-op */ }

    @Override public Savepoint setSavepoint() throws SQLException { nope(); return null; }
    @Override public Savepoint setSavepoint(String n) throws SQLException { nope(); return null; }
    @Override public void rollback(Savepoint s) throws SQLException { nope(); }
    @Override public void releaseSavepoint(Savepoint s) throws SQLException { nope(); }

    @Override public Clob     createClob()     throws SQLException { nope(); return null; }
    @Override public Blob     createBlob()     throws SQLException { nope(); return null; }
    @Override public NClob    createNClob()    throws SQLException { nope(); return null; }
    @Override public SQLXML   createSQLXML()   throws SQLException { nope(); return null; }
    @Override public Array    createArrayOf(String t, Object[] els) throws SQLException { nope(); return null; }
    @Override public Struct   createStruct(String t, Object[] attrs) throws SQLException { nope(); return null; }

    @Override public void abort(Executor e) throws SQLException { close(); }
    @Override public void setNetworkTimeout(Executor e, int ms) { /* stored in http already */ }
    @Override public int  getNetworkTimeout() { return 0; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName());
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private static void nope() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("unsupported JDBC operation on tsdb");
    }
}
