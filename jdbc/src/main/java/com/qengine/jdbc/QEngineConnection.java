package com.qengine.jdbc;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

/**
 * QEngine JDBC connection = a TCP socket running wire protocol v1.
 *
 * Frame layout (all little-endian):
 *   MAGIC      u32   = 0x42445354 ('TSDB')
 *   VER        u8    = 1
 *   TYPE       u8
 *   FLAGS      u16
 *   RESERVED   u32   = 0
 *   REQ_ID     u64   (monotonic)
 *   PAYLOAD_LEN u32
 *   PAYLOAD    [n]
 *   CRC32C     u32   (over hdr[4..23] + payload; MAGIC excluded)
 */
public class QEngineConnection implements Connection {

    // ── wire constants ──
    static final int  MAGIC          = 0x42445354;
    static final byte WIRE_VER       = 1;
    static final int  HDR_SIZE       = 24;

    static final short FLAG_FIN      = 1;
    static final short FLAG_STREAM   = 4;
    static final short FLAG_PONG     = 8;

    static final byte MSG_HELLO             = 1;
    static final byte MSG_HELLO_OK          = 2;
    static final byte MSG_ERROR             = 3;
    static final byte MSG_QUERY             = 40;
    static final byte MSG_QUERY_RESULT_HDR  = 41;
    static final byte MSG_QUERY_RESULT_ROWS = 42;

    static final byte TYPE_TIMESTAMP = 1;
    static final byte TYPE_INT64     = 2;
    static final byte TYPE_FLOAT64   = 3;
    static final byte TYPE_SYMBOL    = 4;

    // ── instance state ──
    private final Socket       socket;
    private final InputStream  in;
    private final OutputStream out;
    private final AtomicLong   nextReqId = new AtomicLong(1);
    private final String       host;
    private final int          port;
    private String             serverVersion = "unknown";
    private volatile boolean   closed = false;

    public QEngineConnection(String host, int port, String clientId, String token,
                             int timeoutMs) throws SQLException {
        this.host = host;
        this.port = port;
        try {
            this.socket = new Socket();
            this.socket.connect(new InetSocketAddress(host, port), timeoutMs);
            this.socket.setSoTimeout(timeoutMs);
            this.in  = this.socket.getInputStream();
            this.out = this.socket.getOutputStream();
            handshake(clientId, token);
        } catch (IOException ioe) {
            throw new QEngineWireException("failed to connect to " + host + ":" + port, ioe);
        }
    }

    // ── HELLO / HELLO_OK handshake ──
    private void handshake(String clientId, String token) throws IOException, SQLException {
        if (clientId == null) clientId = "qengine-jdbc";
        if (token == null) token = "";
        byte[] cid = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] tok = token.getBytes(StandardCharsets.UTF_8);
        if (cid.length > 255 || tok.length > 255) {
            throw new QEngineWireException("client_id/token must fit in 255 bytes each");
        }
        // [ver u8][cid_len u8][cid utf8][tok_len u8][tok utf8]
        ByteBuffer b = ByteBuffer.allocate(3 + cid.length + tok.length).order(ByteOrder.LITTLE_ENDIAN);
        b.put(WIRE_VER);
        b.put((byte) cid.length);
        b.put(cid);
        b.put((byte) tok.length);
        b.put(tok);

        long reqId = nextReqId.getAndIncrement();
        sendFrame(MSG_HELLO, (short) 0, reqId, b.array());
        Frame resp = recvFrame();
        if (resp.type == MSG_ERROR) throw decodeServerError(resp);
        if (resp.type != MSG_HELLO_OK) {
            throw new QEngineWireException("expected HELLO_OK, got type=" + resp.type);
        }
        if (resp.payload != null && resp.payload.length >= 1) {
            int svl = resp.payload[0] & 0xFF;
            if (svl > 0 && svl + 1 <= resp.payload.length) {
                serverVersion = new String(resp.payload, 1, svl, StandardCharsets.UTF_8);
            }
        }
    }

    // ── Wire I/O (package-private: used by Statement/ResultSet) ──

    static final class Frame {
        int    magic;
        byte   ver;
        byte   type;
        short  flags;
        int    reserved;
        long   reqId;
        byte[] payload; // may be null or empty
    }

    synchronized void sendFrame(byte type, short flags, long reqId, byte[] payload) throws IOException {
        int plen = (payload == null) ? 0 : payload.length;
        ByteBuffer hdr = ByteBuffer.allocate(HDR_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        hdr.putInt(MAGIC);           // 0..3   MAGIC
        hdr.put(WIRE_VER);           // 4      VER
        hdr.put(type);               // 5      TYPE
        hdr.putShort(flags);         // 6..7   FLAGS
        hdr.putInt(0);               // 8..11  RESERVED
        hdr.putLong(reqId);          // 12..19 REQ_ID
        hdr.putInt(plen);            // 20..23 PAYLOAD_LEN

        byte[] hdrBytes = hdr.array();
        CRC32C crc = new CRC32C();
        // CRC covers hdr[4..23] + payload — MAGIC excluded
        crc.update(hdrBytes, 4, HDR_SIZE - 4);
        if (plen > 0) crc.update(payload, 0, plen);
        int crcVal = (int) crc.getValue();

        ByteBuffer trail = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        trail.putInt(crcVal);

        out.write(hdrBytes);
        if (plen > 0) out.write(payload);
        out.write(trail.array());
        out.flush();
    }

    synchronized Frame recvFrame() throws IOException, QEngineWireException {
        byte[] hdrBytes = readExactly(HDR_SIZE);
        ByteBuffer hb = ByteBuffer.wrap(hdrBytes).order(ByteOrder.LITTLE_ENDIAN);
        Frame f = new Frame();
        f.magic    = hb.getInt();
        f.ver      = hb.get();
        f.type     = hb.get();
        f.flags    = hb.getShort();
        f.reserved = hb.getInt();
        f.reqId    = hb.getLong();
        int plen   = hb.getInt();

        if (f.magic != MAGIC) {
            throw new QEngineWireException(String.format(
                    "bad magic: expected 0x%08x got 0x%08x", MAGIC, f.magic));
        }
        if (plen < 0 || plen > 16 * 1024 * 1024) {
            throw new QEngineWireException("payload_len out of range: " + plen);
        }
        f.payload = (plen > 0) ? readExactly(plen) : new byte[0];

        byte[] trail = readExactly(4);
        int crcRecv = ByteBuffer.wrap(trail).order(ByteOrder.LITTLE_ENDIAN).getInt();
        CRC32C crc = new CRC32C();
        crc.update(hdrBytes, 4, HDR_SIZE - 4);
        if (plen > 0) crc.update(f.payload, 0, plen);
        int crcComp = (int) crc.getValue();
        if (crcComp != crcRecv) {
            throw new QEngineWireException(String.format(
                    "CRC32C mismatch: recv=0x%08x computed=0x%08x", crcRecv, crcComp));
        }
        return f;
    }

    private byte[] readExactly(int n) throws IOException {
        byte[] buf = new byte[n];
        DataInputStream dis = new DataInputStream(in);
        dis.readFully(buf, 0, n);
        return buf;
    }

    static QEngineWireException decodeServerError(Frame f) {
        byte[] p = f.payload;
        if (p == null || p.length < 6) {
            return new QEngineWireException("server error (empty payload)");
        }
        ByteBuffer bb = ByteBuffer.wrap(p).order(ByteOrder.LITTLE_ENDIAN);
        int code = bb.getInt();
        int mlen = bb.getShort() & 0xFFFF;
        String msg = "";
        if (mlen > 0 && 6 + mlen <= p.length) {
            msg = new String(p, 6, mlen, StandardCharsets.UTF_8);
        }
        return new QEngineWireException("server error " + code + ": " + msg, code);
    }

    long nextRequestId() { return nextReqId.getAndIncrement(); }

    // ── Connection API ──

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();
        return new QEngineStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        ensureOpen();
        return new QEnginePreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public String nativeSQL(String sql) { return sql; }

    @Override public void setAutoCommit(boolean autoCommit) {}
    @Override public boolean getAutoCommit() { return true; }
    @Override public void commit() throws SQLException { /* no-op */ }
    @Override public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback not supported");
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        closed = true;
        try {
            socket.close();
        } catch (IOException ioe) {
            throw new QEngineWireException("close failed", ioe);
        }
    }

    @Override public boolean isClosed() { return closed; }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData not implemented");
    }

    @Override public void setReadOnly(boolean readOnly) {}
    @Override public boolean isReadOnly() { return false; }
    @Override public void setCatalog(String catalog) {}
    @Override public String getCatalog() { return null; }
    @Override public void setTransactionIsolation(int level) {}
    @Override public int getTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}

    @Override
    public Statement createStatement(int rsType, int rsConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int rsType, int rsConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int rsType, int rsConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override public Map<String, Class<?>> getTypeMap() { return Map.of(); }
    @Override public void setTypeMap(Map<String, Class<?>> map) {}
    @Override public void setHoldability(int holdability) {}
    @Override public int getHoldability() { return 0; }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("savepoint not supported");
    }
    @Override public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("savepoint not supported");
    }
    @Override public void rollback(Savepoint sp) throws SQLException {
        throw new SQLFeatureNotSupportedException("savepoint not supported");
    }
    @Override public void releaseSavepoint(Savepoint sp) throws SQLException {
        throw new SQLFeatureNotSupportedException("savepoint not supported");
    }

    @Override
    public Statement createStatement(int a, int b, int c) throws SQLException { return createStatement(); }
    @Override
    public PreparedStatement prepareStatement(String sql, int a, int b, int c) throws SQLException {
        return prepareStatement(sql);
    }
    @Override
    public CallableStatement prepareCall(String sql, int a, int b, int c) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoKeys) throws SQLException {
        return prepareStatement(sql);
    }
    @Override
    public PreparedStatement prepareStatement(String sql, int[] cols) throws SQLException {
        return prepareStatement(sql);
    }
    @Override
    public PreparedStatement prepareStatement(String sql, String[] cols) throws SQLException {
        return prepareStatement(sql);
    }

    @Override public Clob   createClob()   throws SQLException { throw unsupp("createClob"); }
    @Override public Blob   createBlob()   throws SQLException { throw unsupp("createBlob"); }
    @Override public NClob  createNClob()  throws SQLException { throw unsupp("createNClob"); }
    @Override public SQLXML createSQLXML() throws SQLException { throw unsupp("createSQLXML"); }

    @Override public boolean isValid(int timeout) { return !closed && socket.isConnected() && !socket.isClosed(); }
    @Override public void setClientInfo(String name, String value) throws SQLClientInfoException {}
    @Override public void setClientInfo(Properties properties) throws SQLClientInfoException {}
    @Override public String getClientInfo(String name) { return null; }
    @Override public Properties getClientInfo() { return new Properties(); }

    @Override public Array  createArrayOf(String t, Object[] e) throws SQLException { throw unsupp("createArrayOf"); }
    @Override public Struct createStruct(String t, Object[] a)  throws SQLException { throw unsupp("createStruct"); }

    @Override public void setSchema(String schema) {}
    @Override public String getSchema() { return null; }
    @Override public void abort(Executor exec) throws SQLException {
        try { socket.close(); } catch (IOException ignored) {}
        closed = true;
    }
    @Override public void setNetworkTimeout(Executor exec, int ms) throws SQLException {
        try { socket.setSoTimeout(ms); } catch (IOException e) { throw new QEngineWireException("setSoTimeout", e); }
    }
    @Override public int getNetworkTimeout() throws SQLException {
        try { return socket.getSoTimeout(); } catch (IOException e) { throw new QEngineWireException("getSoTimeout", e); }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) { return iface.isAssignableFrom(getClass()); }

    // ── helpers ──
    private void ensureOpen() throws SQLException {
        if (closed) throw new QEngineWireException("connection is closed");
    }

    private static SQLFeatureNotSupportedException unsupp(String what) {
        return new SQLFeatureNotSupportedException(what + " not supported");
    }

    String getServerVersion() { return serverVersion; }
    String getHost()          { return host; }
    int    getPort()          { return port; }
}
