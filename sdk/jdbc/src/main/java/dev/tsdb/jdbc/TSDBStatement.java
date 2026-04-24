package dev.tsdb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

/**
 * Simple non-prepared statement.  Every {@link #executeQuery(String)}
 * roundtrips once to {@code /sql}; results land in memory because the
 * current tsdb response is not streaming.
 */
public class TSDBStatement implements Statement {

    protected final TSDBConnection conn;
    protected TSDBResultSet lastResult;
    protected int lastUpdateCount = -1;
    protected boolean closed;
    protected int fetchSize;
    protected int maxRows;
    protected int queryTimeoutSec;

    TSDBStatement(TSDBConnection conn) { this.conn = conn; }

    /** Send a SQL/QTL statement and parse the {@code /sql} JSON body. */
    TSDBResultSet run(String sql) throws SQLException {
        if (conn.readOnlyFlag() && looksMutating(sql))
            throw new SQLException("connection is read-only; mutating statement refused: " + sql);
        String body = "{\"q\":" + Json.encodeString(sql) + "}";
        String raw;
        try { raw = conn.http().postSql(body); }
        catch (IOException e) { throw new SQLException("POST /sql: " + e.getMessage(), e); }
        return TSDBResultSet.fromJson(this, raw);
    }

    private static boolean looksMutating(String sql) {
        String t = sql.trim();
        if (t.length() < 4) return false;
        String head = t.substring(0, Math.min(t.length(), 8)).toUpperCase();
        return head.startsWith("CREATE") || head.startsWith("DROP") ||
               head.startsWith("ALTER")  || head.startsWith("INSERT") ||
               head.startsWith("DELETE") || head.startsWith("TRUNCATE") ||
               head.startsWith("GRANT")  || head.startsWith("REVOKE");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        lastResult = run(sql);
        lastUpdateCount = -1;
        return lastResult;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        ensureOpen();
        lastResult = run(sql);
        // tsdb returns {"cols":["status"], "rows":[["OK: ..."]]} for DDL;
        // treat as 0 affected rows — there's no actual count field.
        lastUpdateCount = lastResult != null ? 0 : -1;
        return 0;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        ensureOpen();
        lastResult = run(sql);
        lastUpdateCount = lastResult == null ? -1 : 0;
        return lastResult != null;
    }

    @Override public ResultSet getResultSet() { return lastResult; }
    @Override public int       getUpdateCount() { return lastUpdateCount; }
    @Override public boolean   getMoreResults() { return false; }
    @Override public boolean   getMoreResults(int current) { return false; }

    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }

    @Override public Connection getConnection() { return conn; }

    @Override public int  getMaxFieldSize()  { return 0; }
    @Override public void setMaxFieldSize(int m) { /* no-op */ }
    @Override public int  getMaxRows()       { return maxRows; }
    @Override public void setMaxRows(int m)  { this.maxRows = m; }
    @Override public int  getFetchSize()     { return fetchSize; }
    @Override public void setFetchSize(int s){ this.fetchSize = s; }
    @Override public int  getFetchDirection(){ return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchDirection(int d) { /* only FORWARD */ }
    @Override public int  getResultSetConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int  getResultSetType()        { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public int  getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

    @Override public int  getQueryTimeout() { return queryTimeoutSec; }
    @Override public void setQueryTimeout(int s) { this.queryTimeoutSec = s; }

    @Override public void cancel() { /* not supported: no async cancel on /sql */ }
    @Override public void setCursorName(String n) { /* no-op */ }
    @Override public void setEscapeProcessing(boolean e) { /* no-op */ }

    @Override public java.sql.SQLWarning getWarnings()  { return null; }
    @Override public void                clearWarnings(){ /* no-op */ }

    @Override public void addBatch(String sql) throws SQLException { nope(); }
    @Override public void clearBatch() throws SQLException          { nope(); }
    @Override public int[] executeBatch() throws SQLException       { nope(); return null; }

    @Override public boolean isPoolable() { return false; }
    @Override public void    setPoolable(boolean v) { /* no-op */ }
    @Override public boolean isCloseOnCompletion() { return false; }
    @Override public void    closeOnCompletion() { /* no-op */ }

    @Override public int     executeUpdate(String sql, int autoGenKeys) throws SQLException { return executeUpdate(sql); }
    @Override public int     executeUpdate(String sql, int[] cols)      throws SQLException { return executeUpdate(sql); }
    @Override public int     executeUpdate(String sql, String[] cols)   throws SQLException { return executeUpdate(sql); }
    @Override public boolean execute(String sql, int autoGenKeys)       throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, int[] cols)            throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, String[] cols)         throws SQLException { return execute(sql); }
    @Override public ResultSet getGeneratedKeys() throws SQLException   { nope(); return null; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName());
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    protected void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("statement is closed");
    }
    private static void nope() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("unsupported JDBC operation");
    }
}
