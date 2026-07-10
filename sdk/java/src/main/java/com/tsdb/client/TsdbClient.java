package com.tsdb.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;

/**
 * Pure-Java client for the QEngine (tsdb) TCP wire protocol v1.
 *
 * <p>Frame layout (all little-endian):
 *
 * <pre>
 *   MAGIC       u32 = 0x42445354 ('TSDB' LE)
 *   VER         u8  = 1
 *   TYPE        u8
 *   FLAGS       u16
 *   RESERVED    u32 (zero on send)
 *   REQ_ID      u64
 *   PAYLOAD_LEN u32
 *   PAYLOAD     [PAYLOAD_LEN]
 *   CRC32C      u32  (Castagnoli, over [ver..end-of-payload])
 * </pre>
 *
 * <p>Uses {@link CRC32C} from JDK 9+ which dispatches to hardware CRC when
 * available, matching the server's SSE4.2/ARMv8 path.
 */
public class TsdbClient implements AutoCloseable {

    public static final int MAGIC = 0x42445354;
    public static final byte VER  = 1;
    public static final int  HDR  = 24;

    /* Message types (mirror src/server/proto.h) */
    public static final byte MSG_HELLO          = 1;
    public static final byte MSG_HELLO_OK       = 2;
    public static final byte MSG_ERROR          = 3;
    public static final byte MSG_PING           = 4;
    public static final byte MSG_AUTH_LOGIN     = 5;
    public static final byte MSG_AUTH_OK        = 6;
    public static final byte MSG_CREATE_TABLE   = 20;
    public static final byte MSG_DROP_TABLE     = 21;
    public static final byte MSG_WRITE_BATCH    = 30;
    public static final byte MSG_WRITE_ACK      = 34;
    public static final byte MSG_QUERY          = 40;
    public static final byte MSG_QUERY_HDR      = 41;
    public static final byte MSG_QUERY_ROWS     = 42;
    public static final byte MSG_CLUSTER_STATS  = 60;

    public static final short FLAG_FIN  = 0x0001;

    /* Column types */
    public static final byte T_TIMESTAMP = 1;
    public static final byte T_INT64     = 2;
    public static final byte T_FLOAT64   = 3;
    public static final byte T_SYMBOL    = 4;
    /** FLOAT32 stores 4 bytes on disk (half of FLOAT64) but travels as a
     *  64-bit double on the wire (the server widens it) — write its values
     *  via {@link Row#f64}; query results decode to {@link Double}. */
    public static final byte T_FLOAT32   = 5;

    /* Live socket + streams.  Non-final: {@link #reconnect()} swaps them for a
     * fresh connection after a mid-session drop. */
    private Socket socket;
    private DataInputStream  in;
    private DataOutputStream out;
    private long nextReq = 1;
    private String token = "";

    /* ----- Retained reconnect state -----
     * Everything needed to transparently re-establish the session after a
     * transport drop (peer restart, network blip): the address, the connect
     * timeout, and — once login() succeeds — the credentials. */
    private final String host;
    private final int    port;
    private final int    timeoutMs;
    private String  loginUser;
    private String  loginPass;
    private boolean loggedIn = false;

    /**
     * Bounds automatic reconnect attempts on a dropped connection.  The
     * default (2) means auto-reconnect is ON.  Set to 0 (or any value &lt;= 0)
     * to opt out, in which case a transport drop surfaces the IOException
     * instead of re-connecting.  See {@link #setMaxReconnect(int)}.
     */
    private int maxReconnect = 2;

    /* Per-attempt backoff (ms) before re-opening the socket.  Index i is used
     * before attempt i+1.  Kept short: a peer restart usually recovers within a
     * few hundred ms. */
    private static final int[] RECONNECT_BACKOFF_MS = { 100, 300 };

    /* Non-null while a WritePipeline is open on this connection.  The pipeline
     * owns the socket's FIFO ack stream, so query/writeBatch/login refuse to
     * interleave (IllegalStateException) until WritePipeline.close() clears
     * this. */
    WritePipeline activePipeline;

    public TsdbClient(String host, int port) throws IOException {
        this(host, port, 10_000);
    }

    public TsdbClient(String host, int port, int timeoutMs) throws IOException {
        this.host      = host;
        this.port      = port;
        this.timeoutMs = timeoutMs;
        openSocket();
        hello();
    }

    /**
     * Opens (or re-opens) the socket to the retained host/port and rebuilds
     * the data streams, applying the same socket options the original connect
     * used.  Shared by the constructor and {@link #reconnect()}.
     */
    private void openSocket() throws IOException {
        Socket s = new Socket();
        s.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
        s.setTcpNoDelay(true);
        this.socket = s;
        this.in  = new DataInputStream(s.getInputStream());
        this.out = new DataOutputStream(s.getOutputStream());
    }

    @Override public void close() throws IOException {
        socket.close();
    }

    public String token() { return token; }

    /**
     * Maximum automatic reconnect attempts after a dropped connection.
     * Default 2 (auto-reconnect ON).  Pass 0 to opt out entirely — a
     * transport drop will then surface the IOException to the caller.
     */
    public void setMaxReconnect(int n) { this.maxReconnect = n; }

    public int getMaxReconnect() { return maxReconnect; }

    /* ----- Frame I/O ----- */

    public static class Frame {
        public byte   type;
        public short  flags;
        public long   reqId;
        public byte[] payload;
    }

    public long send(byte type, short flags, byte[] payload) throws IOException {
        long reqId = nextReq++;
        ByteBuffer hdr = ByteBuffer.allocate(HDR).order(ByteOrder.LITTLE_ENDIAN);
        hdr.putInt(MAGIC);
        hdr.put(VER);
        hdr.put(type);
        hdr.putShort(flags);
        hdr.putInt(0); // reserved
        hdr.putLong(reqId);
        hdr.putInt(payload == null ? 0 : payload.length);
        byte[] hdrBytes = hdr.array();

        CRC32C crc = new CRC32C();
        crc.update(hdrBytes, 4, HDR - 4);
        if (payload != null && payload.length > 0) crc.update(payload);
        int crcVal = (int) crc.getValue();

        out.write(hdrBytes);
        if (payload != null && payload.length > 0) out.write(payload);
        ByteBuffer tail = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        tail.putInt(crcVal);
        out.write(tail.array());
        out.flush();
        return reqId;
    }

    public Frame recv() throws IOException {
        byte[] hdrBytes = new byte[HDR];
        in.readFully(hdrBytes);
        ByteBuffer hdr = ByteBuffer.wrap(hdrBytes).order(ByteOrder.LITTLE_ENDIAN);
        int magic = hdr.getInt();
        if (magic != MAGIC) throw new IOException("bad magic: 0x" + Integer.toHexString(magic));
        byte ver = hdr.get();
        if (ver != VER) throw new IOException("unsupported ver " + ver);
        Frame f = new Frame();
        f.type  = hdr.get();
        f.flags = hdr.getShort();
        hdr.getInt(); // reserved
        f.reqId = hdr.getLong();
        int plen = hdr.getInt();
        if (plen < 0 || plen > 16 * 1024 * 1024) throw new IOException("bad plen " + plen);
        f.payload = new byte[plen];
        if (plen > 0) in.readFully(f.payload);
        byte[] tail = new byte[4];
        in.readFully(tail);
        int stored = ByteBuffer.wrap(tail).order(ByteOrder.LITTLE_ENDIAN).getInt();

        CRC32C crc = new CRC32C();
        crc.update(hdrBytes, 4, HDR - 4);
        if (plen > 0) crc.update(f.payload);
        int computed = (int) crc.getValue();
        if (computed != stored) throw new IOException("crc mismatch");
        return f;
    }

    /* ----- Session ----- */

    private void hello() throws IOException {
        send(MSG_HELLO, (short) 0, null);
        Frame f = recv();
        if (f.type != MSG_HELLO_OK) throw new IOException("unexpected HELLO response type=" + f.type);
    }

    /* ----- Reconnect ----- */

    /**
     * Reports whether {@code e} is a transport-level failure that warrants a
     * reconnect — the peer went away or the socket broke — as opposed to a
     * server application error (a decoded MSG_ERROR frame, surfaced by
     * {@link #decodeError}) or a frame-integrity error (e.g. "crc mismatch"),
     * neither of which must trigger a reconnect.
     *
     * <p>Reconnect-worthy: {@link EOFException} (readFully hit a closed
     * socket mid-frame), or a {@link SocketException} whose message names a
     * dropped/reset/closed/refused connection or a broken pipe.
     */
    static boolean isTransportError(IOException e) {
        if (e instanceof EOFException) return true;
        if (e instanceof SocketException) {
            String m = e.getMessage();
            if (m == null) return true; // a bare SocketException is transport-level
            m = m.toLowerCase();
            return m.contains("broken pipe")
                || m.contains("connection reset")
                || m.contains("connection closed")
                || m.contains("connection refused")
                || m.contains("socket closed")
                || m.contains("socket is closed")
                || m.contains("connection abort");
        }
        return false;
    }

    /**
     * Tears down the dead socket and re-establishes a fresh session on the
     * retained host/port: re-open the socket (same options), re-send HELLO
     * exactly as the constructor did, and — if the original session
     * authenticated — re-run login() with the retained credentials so the new
     * socket carries a valid server-side token.  Retries up to
     * {@code maxReconnect} times with a short backoff.
     *
     * @param trigger the transport error that prompted the reconnect; rethrown
     *                (with the last reconnect failure attached) if all attempts
     *                fail, so the caller still sees the original cause.
     */
    private void reconnect(IOException trigger) throws IOException {
        try { socket.close(); } catch (IOException ignore) { /* already dead */ }
        IOException last = null;
        for (int i = 0; i < maxReconnect; i++) {
            int d = i < RECONNECT_BACKOFF_MS.length ? i : RECONNECT_BACKOFF_MS.length - 1;
            try { Thread.sleep(RECONNECT_BACKOFF_MS[d]); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("reconnect interrupted", ie);
            }
            try {
                dialAndHandshake();
                return;
            } catch (IOException e) {
                last = e;
            }
        }
        // All attempts failed: surface the original transport error, with the
        // final reconnect failure (if any) as a suppressed cause.
        if (last != null) trigger.addSuppressed(last);
        throw trigger;
    }

    /** Reconnect on a best-effort basis, swallowing failure.  Used on the
     *  post-send WriteBatch path where we re-establish the socket for future
     *  calls but the original error is rethrown regardless. */
    private void reconnectQuietly() {
        try {
            try { socket.close(); } catch (IOException ignore) {}
            for (int i = 0; i < maxReconnect; i++) {
                int d = i < RECONNECT_BACKOFF_MS.length ? i : RECONNECT_BACKOFF_MS.length - 1;
                Thread.sleep(RECONNECT_BACKOFF_MS[d]);
                try { dialAndHandshake(); return; }
                catch (IOException ignore) { /* try again */ }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** One full open: re-open socket + HELLO + (optional) re-login.  On any
     *  failure the partial socket is closed and the error is propagated. */
    private void dialAndHandshake() throws IOException {
        openSocket();
        try {
            hello();
            // Re-establish auth on the fresh socket: the prior token died with
            // the old socket, so a new login() is mandatory, not optional.
            if (loggedIn) login(loginUser, loginPass);
        } catch (IOException e) {
            try { socket.close(); } catch (IOException ignore) {}
            throw e;
        }
    }

    public void login(String user, String pass) throws IOException {
        ensureNoPipeline();
        // Retain credentials so a reconnect can re-authenticate the fresh
        // socket: the server attaches the token to the connection, so the old
        // token dies with the old socket and a new login() is mandatory.
        this.loginUser = user;
        this.loginPass = pass;
        byte[] u = user.getBytes("UTF-8");
        byte[] p = pass.getBytes("UTF-8");
        ByteBuffer buf = ByteBuffer.allocate(2 + u.length + p.length);
        buf.put((byte) u.length).put(u);
        buf.put((byte) p.length).put(p);
        send(MSG_AUTH_LOGIN, (short) 0, buf.array());
        Frame f = recv();
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
        if (f.type != MSG_AUTH_OK) throw new IOException("unexpected AUTH type=" + f.type);
        if (f.payload.length != 32) throw new IOException("bad token len " + f.payload.length);
        token = new String(f.payload, "UTF-8");
        loggedIn = true;
    }

    /* ----- Ping / stats ----- */

    /**
     * Sends MSG_PING with a small payload and verifies the server echoes it
     * back (the server replies with the same type and payload, PONG flag
     * set).  A transport failure or a mismatched echo throws; a normal
     * return means the round-trip completed.
     */
    public void ping() throws IOException {
        ensureNoPipeline();
        byte[] payload = "ping".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        send(MSG_PING, (short) 0, payload);
        Frame f = recv();
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
        if (f.type != MSG_PING) throw new IOException("unexpected PING response type=" + f.type);
        if (!java.util.Arrays.equals(f.payload, payload))
            throw new IOException("PING echo mismatch");
    }

    /**
     * Requests the server's live counters (MSG_CLUSTER_STATS).  The server
     * replies with MSG_HELLO_OK carrying a kv payload — {@code [kv_count u16
     * LE]} then per pair {@code [klen u8][key][vlen u8][val]}, values being
     * decimal strings (server.c handle_cluster_stats) — rendered here as one
     * {@code key=value} line per pair, in server order.
     */
    public String clusterStats() throws IOException {
        ensureNoPipeline();
        send(MSG_CLUSTER_STATS, (short) 0, null);
        Frame f = recv();
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
        if (f.type != MSG_HELLO_OK)
            throw new IOException("unexpected CLUSTER_STATS response type=" + f.type);
        ByteBuffer b = ByteBuffer.wrap(f.payload).order(ByteOrder.LITTLE_ENDIAN);
        if (b.remaining() < 2) throw new IOException("short CLUSTER_STATS payload");
        int n = b.getShort() & 0xFFFF;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append('\n');
            sb.append(readU8Str(b, "key")).append('=').append(readU8Str(b, "value"));
        }
        return sb.toString();
    }

    /** Reads one [len u8][bytes] string out of a CLUSTER_STATS kv payload. */
    private static String readU8Str(ByteBuffer b, String what) throws IOException {
        if (b.remaining() < 1) throw new IOException("truncated CLUSTER_STATS " + what);
        int len = b.get() & 0xFF;
        if (b.remaining() < len) throw new IOException("truncated CLUSTER_STATS " + what);
        byte[] s = new byte[len];
        b.get(s);
        return new String(s, java.nio.charset.StandardCharsets.UTF_8);
    }

    /* ----- DDL ----- */

    public static final class Column {
        public final String name;
        public final byte   type;
        public Column(String name, byte type) { this.name = name; this.type = type; }
    }

    public void createTable(String table, String tsCol, List<Column> cols) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        putPascalStr(buf, table);
        putPascalStr(buf, tsCol);
        buf.put((byte) cols.size());
        for (Column c : cols) {
            putPascalStr(buf, c.name);
            buf.put(c.type);
        }
        byte[] payload = new byte[buf.position()];
        buf.flip(); buf.get(payload);
        send(MSG_CREATE_TABLE, (short) 0, payload);
        Frame f = recv();
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
    }

    /**
     * Runs DROP_TABLE.  The payload is the raw table name; the server replies
     * with a generic OK, or MSG_ERROR (e.g. when the table does not exist),
     * decoded like {@link #createTable}'s.
     */
    public void dropTable(String table) throws IOException {
        send(MSG_DROP_TABLE, (short) 0, table.getBytes("UTF-8"));
        Frame f = recv();
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
    }

    /* ----- UDF registration ----- */

    /** Maps a wire column type byte to its CREATE FUNCTION keyword. */
    private static String udfTypeKeyword(byte t) {
        switch (t) {
            case 1: return "TIMESTAMP";
            case 2: return "INT64";
            case 3: return "FLOAT64";
            default:
                throw new IllegalArgumentException(
                    "UDF v1 supports TIMESTAMP/INT64/FLOAT64, got type byte " + t);
        }
    }

    /**
     * Registers a scalar UDF by issuing
     * {@code CREATE FUNCTION name(argTypes...) RETURNS retType FROM 'soPath' SYMBOL 'symbol'}
     * over the standard query channel.
     *
     * <p>Types use the wire bytes (1 = TIMESTAMP, 2 = INT64, 3 = FLOAT64).
     * {@code symbol} may be null or empty — the server then defaults the
     * exported symbol to the function name.  Registration is metadata-only
     * on the server (dlopen happens lazily at first call), so the .so must
     * be present at {@code soPath} on every server node before the
     * function's first use.  Requires an ADMIN session when auth is enabled.
     */
    public void registerUdf(String name, byte[] argTypes, byte retType,
                            String soPath, String symbol) throws IOException {
        StringBuilder sb = new StringBuilder("CREATE FUNCTION ").append(name).append('(');
        for (int i = 0; i < argTypes.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(udfTypeKeyword(argTypes[i]));
        }
        sb.append(") RETURNS ").append(udfTypeKeyword(retType))
          .append(" FROM '").append(soPath).append('\'');
        if (symbol != null && !symbol.isEmpty())
            sb.append(" SYMBOL '").append(symbol).append('\'');
        query(sb.toString());
    }

    /** Unregisters a scalar UDF: {@code DROP FUNCTION name}. */
    public void dropUdf(String name) throws IOException {
        query("DROP FUNCTION " + name);
    }

    /* ----- Admin / user management (thin SQL wrappers) ----- */

    /** Escapes s for a single-quoted SQL string literal: every single quote
     *  is doubled, matching the server lexer's {@code ''} escape. */
    private static String sqlQuoteStr(String s) {
        return s.replace("'", "''");
    }

    /**
     * Issues {@code CREATE USER name IDENTIFIED BY 'pass' [ROLE ADMIN]} over
     * the standard query channel.  The server defaults the role to NORMAL
     * when the ROLE clause is omitted.  Requires an ADMIN session when auth
     * is enabled.
     */
    public void createUser(String name, String pass, boolean admin) throws IOException {
        StringBuilder sb = new StringBuilder("CREATE USER ").append(name)
            .append(" IDENTIFIED BY '").append(sqlQuoteStr(pass)).append('\'');
        if (admin) sb.append(" ROLE ADMIN");
        query(sb.toString());
    }

    /** Issues {@code DROP USER name}. */
    public void dropUser(String name) throws IOException {
        query("DROP USER " + name);
    }

    /**
     * Issues {@code GRANT priv ON table TO user}.  {@code priv} is one of
     * SELECT, INSERT, DDL, or ALL (server parse.c); {@code table} may be
     * {@code "*"} for all tables.
     */
    public void grant(String priv, String table, String user) throws IOException {
        query("GRANT " + priv + " ON " + table + " TO " + user);
    }

    /** Issues {@code REVOKE priv ON table FROM user}.  Same priv/table
     *  grammar as {@link #grant}. */
    public void revoke(String priv, String table, String user) throws IOException {
        query("REVOKE " + priv + " ON " + table + " FROM " + user);
    }

    /** Issues {@code CREATE DATABASE name}. */
    public void createDatabase(String name) throws IOException {
        query("CREATE DATABASE " + name);
    }

    /* ----- Columnar WRITE_BATCH ----- */

    /**
     * One row's column values, indexed by column position (matching the
     * cols list passed to {@link #writeBatch}).  Only the type-specific
     * map for each column needs to be populated; missing entries default
     * to zero / empty string.
     *
     * The TS field is the row's timestamp (ns since epoch); it must
     * agree with whichever column was declared TIMESTAMP at table create.
     */
    public static final class Row {
        public long             ts;
        public Map<Integer,Long>    i64 = Collections.emptyMap();
        public Map<Integer,Double>  f64 = Collections.emptyMap();
        public Map<Integer,String>  sym = Collections.emptyMap();
    }

    /**
     * Send a columnar WRITE_BATCH of rows.  All rows must share the
     * same set of columns.  cols describes ALL columns (including the
     * timestamp column) in schema order.
     *
     * Wire format mirrors the cluster RPC WRITE_BATCH receiver:
     * fixed-width columns carry n*8 raw bytes; SYMBOL columns carry
     * `[u32 total_bytes][u16 len][bytes]…`.
     *
     * <p><b>Reconnect idempotency.</b>  A WRITE_BATCH is NOT idempotent — the
     * server appends rows, so a blind resend could double-write.  Auto-retry
     * therefore covers ONLY the send/connect phase: if the failure happens
     * while writing the request frame (or on a dead socket before any byte
     * lands), the request provably never reached the server, so the client
     * reconnects and resends once.  If the failure happens AFTER the frame was
     * fully sent (a transport drop while awaiting the ack), the server may have
     * already applied the batch; the client reconnects so the next call lands
     * on a live socket but RETHROWS the original error so the caller decides
     * whether to resend.  Set {@link #setMaxReconnect(int)} to 0 to disable.
     *
     * @return number of rows the server confirmed it persisted.
     */
    public int writeBatch(String table, List<Column> cols, List<Row> rows) throws IOException {
        ensureNoPipeline();
        if (rows.isEmpty()) return 0;
        byte[] payload = encodeBatch(table, cols, rows);
        try {
            return writeBatchOnce(payload);
        } catch (PostSendException e) {
            // Frame was fully sent before the drop: the server may have applied
            // it.  Reconnect for future calls but do NOT auto-resend (would risk
            // duplicate rows); rethrow the underlying transport error.
            if (maxReconnect > 0) reconnectQuietly();
            throw e.cause;
        } catch (IOException e) {
            // Send/connect-phase failure (nothing landed) — safe to retry once.
            if (!isTransportError(e) || maxReconnect <= 0) throw e;
            reconnect(e);
            return writeBatchOnce(payload);
        }
    }

    /**
     * Builds the columnar WRITE_BATCH payload (see {@link #writeBatch} for the
     * wire format).  Shared by the synchronous {@link #writeBatch} path and
     * {@link WritePipeline#write}.
     */
    static byte[] encodeBatch(String table, List<Column> cols, List<Row> rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);

        byte[] tn = table.getBytes("UTF-8");
        dout.writeByte(tn.length);
        dout.write(tn);

        // ncols (u16 LE), nrows (u32 LE)
        dout.writeByte(cols.size() & 0xFF);
        dout.writeByte((cols.size() >> 8) & 0xFF);
        int n = rows.size();
        dout.writeByte( n        & 0xFF);
        dout.writeByte((n >>  8) & 0xFF);
        dout.writeByte((n >> 16) & 0xFF);
        dout.writeByte((n >> 24) & 0xFF);

        for (int ci = 0; ci < cols.size(); ci++) {
            Column c = cols.get(ci);
            byte[] cn = c.name.getBytes("UTF-8");
            dout.writeByte(cn.length);
            dout.write(cn);
            dout.writeByte(c.type);
            dout.writeByte(0); // codec = RAW

            if (c.type == T_SYMBOL) {
                // Pre-build [u16 len][bytes]… body to know total upfront.
                ByteArrayOutputStream sym = new ByteArrayOutputStream();
                for (Row r : rows) {
                    String s = r.sym.getOrDefault(ci, "");
                    byte[] sb = s.getBytes("UTF-8");
                    int slen = Math.min(sb.length, 65535);
                    sym.write(slen & 0xFF);
                    sym.write((slen >> 8) & 0xFF);
                    sym.write(sb, 0, slen);
                }
                int total = sym.size();
                int csz = 4 + total; // [u32 total] + body
                writeU32LE(dout, csz);
                writeU32LE(dout, total);
                dout.write(sym.toByteArray());
            } else {
                int csz = n * 8;
                writeU32LE(dout, csz);
                for (Row r : rows) {
                    long bits;
                    switch (c.type) {
                        case T_TIMESTAMP: bits = r.ts; break;
                        case T_INT64:     bits = r.i64.getOrDefault(ci, 0L); break;
                        // FLOAT32 travels as a double too — the server stores it
                        // 8-byte in memory and the codec narrows to 4 bytes only
                        // on disk — so its values also come from Row.f64 (a Float
                        // value is widened by the caller storing it as Double).
                        case T_FLOAT64:
                        case T_FLOAT32:   bits = Double.doubleToRawLongBits(r.f64.getOrDefault(ci, 0.0)); break;
                        default: throw new IOException("unsupported column type " + c.type);
                    }
                    writeU64LE(dout, bits);
                }
            }
        }

        return out.toByteArray();
    }

    /**
     * Opens a pipelined writer that keeps up to {@code depth} WRITE_BATCH
     * frames in flight on this connection (see {@link WritePipeline} for the
     * FIFO-ack contract and error semantics).  {@code depth} must be &gt;= 1
     * and is clamped to {@link WritePipeline#MAX_DEPTH}.  Only one pipeline
     * may be open per client at a time; while it is open, query/writeBatch/
     * login on this client throw {@link IllegalStateException} until
     * {@link WritePipeline#close()} releases the connection.
     */
    public WritePipeline newWritePipeline(int depth) {
        if (depth < 1)
            throw new IllegalArgumentException("write pipeline depth must be >= 1, got " + depth);
        if (depth > WritePipeline.MAX_DEPTH) depth = WritePipeline.MAX_DEPTH;
        ensureNoPipeline();
        WritePipeline p = new WritePipeline(this, depth);
        activePipeline = p;
        return p;
    }

    /** Rejects parent-client I/O while a {@link WritePipeline} owns the
     *  connection: interleaving would corrupt the FIFO ack accounting. */
    private void ensureNoPipeline() {
        if (activePipeline != null)
            throw new IllegalStateException("write pipeline in progress on this connection");
    }

    /**
     * One WRITE_BATCH round-trip.  A transport failure on the SEND phase
     * propagates as a plain IOException (nothing reached the server, safe to
     * retry).  A transport failure on the RECV phase — after the frame was
     * fully written — is wrapped in {@link PostSendException} so the caller can
     * distinguish it and suppress auto-resend.
     */
    private int writeBatchOnce(byte[] payload) throws IOException {
        // Send phase: a failure here (incl. a dead socket) propagates as a
        // plain IOException — the request never reached the server, safe to retry.
        send(MSG_WRITE_BATCH, (short) 0, payload);
        Frame f;
        try {
            f = recv();
        } catch (IOException e) {
            // Recv phase: the frame was fully written, so a transport drop here
            // is post-send — tag it so the caller suppresses auto-resend.
            if (isTransportError(e)) throw new PostSendException(e);
            throw e;
        }
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
        if (f.type != MSG_WRITE_ACK) throw new IOException("unexpected WRITE response type=" + f.type);
        if (f.payload.length < 4) throw new IOException("short WRITE_ACK");
        return ByteBuffer.wrap(f.payload, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    /** Tags a transport error that occurred after the request frame was fully
     *  sent (while awaiting the ack), so writeBatch suppresses auto-resend. */
    private static final class PostSendException extends IOException {
        private static final long serialVersionUID = 1L;
        final IOException cause;
        PostSendException(IOException cause) { super(cause); this.cause = cause; }
    }

    private static void writeU32LE(DataOutputStream o, int v) throws IOException {
        o.writeByte(v & 0xFF);
        o.writeByte((v >>>  8) & 0xFF);
        o.writeByte((v >>> 16) & 0xFF);
        o.writeByte((v >>> 24) & 0xFF);
    }
    private static void writeU64LE(DataOutputStream o, long v) throws IOException {
        for (int i = 0; i < 8; i++) o.writeByte((int)(v >>> (i * 8)) & 0xFF);
    }

    /* ----- Query ----- */

    public static final class QueryResult {
        public String[] colNames;
        public byte[]   colTypes;
        public List<Object[]> rows = new ArrayList<>();
    }

    /**
     * Runs a QTL/SQL query and returns the full result set.
     *
     * <p>Query is read-only and therefore safe to auto-retry: on a mid-flight
     * transport drop the client reconnects and re-runs the whole query once
     * (the result set is rebuilt from scratch, so a partially-drained prior
     * attempt cannot corrupt it).  Auto-retry is governed by
     * {@link #setMaxReconnect(int)}.  A server-side error frame is NOT a
     * transport failure and is rethrown without reconnecting.
     */
    public QueryResult query(String qtl) throws IOException {
        ensureNoPipeline();
        try {
            return queryOnce(qtl);
        } catch (IOException e) {
            if (!isTransportError(e) || maxReconnect <= 0) throw e;
            // read-only → safe to reconnect and re-run the whole query once.
            reconnect(e);
            return queryOnce(qtl);
        }
    }

    private QueryResult queryOnce(String qtl) throws IOException {
        byte[] q = qtl.getBytes("UTF-8");
        send(MSG_QUERY, (short) 0, q);
        Frame f = recv();
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
        if (f.type != MSG_QUERY_HDR) throw new IOException("unexpected HDR type=" + f.type);

        ByteBuffer h = ByteBuffer.wrap(f.payload).order(ByteOrder.LITTLE_ENDIAN);
        int ncols = h.getShort() & 0xFFFF;
        QueryResult qr = new QueryResult();
        qr.colNames = new String[ncols];
        qr.colTypes = new byte[ncols];
        for (int i = 0; i < ncols; i++) {
            int nl = h.get() & 0xFF;
            byte[] nb = new byte[nl];
            h.get(nb);
            qr.colNames[i] = new String(nb, "UTF-8");
            qr.colTypes[i] = h.get();
        }
        if ((f.flags & FLAG_FIN) != 0) return qr;

        for (;;) {
            Frame rf = recv();
            if (rf.type == MSG_ERROR) throw decodeError(rf.payload);
            if (rf.type != MSG_QUERY_ROWS) throw new IOException("unexpected ROWS type=" + rf.type);
            parseRowsChunk(qr, rf.payload);
            if ((rf.flags & FLAG_FIN) != 0) return qr;
        }
    }

    /**
     * Decodes one QUERY_RESULT_ROWS payload.  Columns are laid out
     * sequentially (see server emit_query_chunk): fixed-width columns
     * (TIMESTAMP/INT64/FLOAT64) occupy nrows*8 bytes; SYMBOL columns carry
     * {@code [u32 blocklen]} followed by nrows × {@code [u16 len][bytes]}
     * records — the server sends the resolved strings, not dict IDs — so
     * SYMBOL cells decode to {@link String}, matching the Go SDK and the
     * HTTP driver.
     */
    private static void parseRowsChunk(QueryResult qr, byte[] payload) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int nrows = b.getInt();
        int ncols = b.getShort() & 0xFFFF;
        if (ncols != qr.colNames.length) throw new IOException("col mismatch");
        Object[][] colVals = new Object[ncols][];
        for (int ci = 0; ci < ncols; ci++) {
            colVals[ci] = new Object[nrows];
            if (qr.colTypes[ci] == T_SYMBOL) {
                if (b.remaining() < 4) throw new IOException("short symbol block header");
                int blockLen = b.getInt();
                if (blockLen < 0 || b.remaining() < blockLen)
                    throw new IOException("short symbol block data");
                int end = b.position() + blockLen;
                for (int ri = 0; ri < nrows; ri++) {
                    if (end - b.position() < 2) throw new IOException("truncated symbol len");
                    int l = b.getShort() & 0xFFFF;
                    if (end - b.position() < l) throw new IOException("truncated symbol bytes");
                    byte[] sb = new byte[l];
                    b.get(sb);
                    colVals[ci][ri] = new String(sb, java.nio.charset.StandardCharsets.UTF_8);
                }
                b.position(end);
            } else {
                if (b.remaining() < (long) nrows * 8) throw new IOException("short column data");
                for (int ri = 0; ri < nrows; ri++) {
                    long raw = b.getLong();
                    switch (qr.colTypes[ci]) {
                        case T_TIMESTAMP: colVals[ci][ri] = raw; break;
                        case T_INT64:     colVals[ci][ri] = raw; break;
                        // FLOAT32 travels as an 8-byte double on the wire (the
                        // server widens it); decode it to Double exactly like
                        // FLOAT64 — matching the Go SDK — so FLOAT32 cells (and
                        // aggregates over them) don't come back null.
                        case T_FLOAT64:
                        case T_FLOAT32:   colVals[ci][ri] = Double.longBitsToDouble(raw); break;
                    }
                }
            }
        }
        for (int ri = 0; ri < nrows; ri++) {
            Object[] row = new Object[ncols];
            for (int ci = 0; ci < ncols; ci++) row[ci] = colVals[ci][ri];
            qr.rows.add(row);
        }
    }

    /* ----- helpers ----- */

    private static void putPascalStr(ByteBuffer buf, String s) {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        buf.put((byte) b.length).put(b);
    }

    /** Decodes a MSG_ERROR payload into a {@link TsdbServerException} (an
     *  IOException carrying the server status code).  Package-private so
     *  {@link WritePipeline} reuses the same decoding. */
    static IOException decodeError(byte[] payload) {
        if (payload.length < 4) return new IOException("server error");
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int rc = b.getInt();
        String msg = "";
        if (payload.length >= 6) {
            // Spec shape: [i32 code][u16 msg_len][msg]. Older servers sent
            // the message raw after the code; accept both via the length.
            int n = (payload[4] & 0xFF) | ((payload[5] & 0xFF) << 8);
            int off = (6 + n == payload.length) ? 6 : 4;
            msg = new String(payload, off, payload.length - off,
                    java.nio.charset.StandardCharsets.UTF_8);
        } else if (payload.length > 4) {
            msg = new String(payload, 4, payload.length - 4,
                    java.nio.charset.StandardCharsets.UTF_8);
        }
        return new TsdbServerException(rc, msg);
    }
}
