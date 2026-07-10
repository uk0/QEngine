package com.tsdb.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

/**
 * Self-contained reconnect tests for {@link TsdbClient}.  The project ships no
 * JUnit on the classpath (the SDK is JDK-only), so this is a runnable
 * {@code main} self-test, matching the existing {@code JdbcSmokeTest} style.
 * It exits non-zero on the first failed assertion.
 *
 * <p>The listener test stands up a local {@link ServerSocket} that speaks just
 * enough of the wire protocol (HELLO handshake + one query reply) to drive the
 * client's reconnect path without the real C server: connection #1 completes
 * the handshake then drops; the client must transparently re-dial (a SECOND
 * accept) and complete the retried query.
 */
public class ReconnectTest {

    /* ---- server-side wire codec (mirrors TsdbClient.send/recv) ---- */

    private static final int MAGIC = 0x42445354;
    private static final byte VER  = 1;
    private static final int  HDR  = 24;
    private static final byte MSG_HELLO        = 1;
    private static final byte MSG_HELLO_OK     = 2;
    private static final byte MSG_ERROR        = 3;
    private static final byte MSG_DROP_TABLE   = 21;
    private static final byte MSG_WRITE_BATCH  = 30;
    private static final byte MSG_WRITE_ACK    = 34;
    private static final byte MSG_QUERY        = 40;
    private static final byte MSG_QUERY_HDR    = 41;
    private static final byte MSG_QUERY_ROWS   = 42;
    private static final short FLAG_FIN        = 0x0001;

    private static final class SrvFrame {
        byte type; long reqId; byte[] payload;
    }

    private static SrvFrame srvRead(DataInputStream in) throws IOException {
        byte[] h = new byte[HDR];
        in.readFully(h);
        ByteBuffer b = ByteBuffer.wrap(h).order(ByteOrder.LITTLE_ENDIAN);
        if (b.getInt() != MAGIC) throw new IOException("srv: bad magic");
        b.get(); // ver
        SrvFrame f = new SrvFrame();
        f.type = b.get();
        b.getShort();       // flags
        b.getInt();         // reserved
        f.reqId = b.getLong();
        int plen = b.getInt();
        f.payload = new byte[plen];
        if (plen > 0) in.readFully(f.payload);
        byte[] tail = new byte[4];
        in.readFully(tail); // CRC — not validated server-side here
        return f;
    }

    private static void srvWrite(DataOutputStream out, byte type, short flags,
                                 long reqId, byte[] payload) throws IOException {
        ByteBuffer h = ByteBuffer.allocate(HDR).order(ByteOrder.LITTLE_ENDIAN);
        h.putInt(MAGIC);
        h.put(VER);
        h.put(type);
        h.putShort(flags);
        h.putInt(0);
        h.putLong(reqId);
        h.putInt(payload == null ? 0 : payload.length);
        byte[] hb = h.array();
        CRC32C crc = new CRC32C();
        crc.update(hb, 4, HDR - 4);
        if (payload != null && payload.length > 0) crc.update(payload);
        out.write(hb);
        if (payload != null && payload.length > 0) out.write(payload);
        ByteBuffer t = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        t.putInt((int) crc.getValue());
        out.write(t.array());
        out.flush();
    }

    /** Zero-column QUERY_HDR with FIN set: a complete empty result. */
    private static byte[] emptyQueryHdr() {
        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) 0).array();
    }

    private static byte[] writeAck(int n) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(n).array();
    }

    /** MSG_ERROR payload in the spec shape [i32 rc][u16 msg_len][msg]. */
    private static byte[] errPayload(int rc, String msg) {
        byte[] m = msg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(6 + m.length).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(rc);
        b.putShort((short) m.length);
        b.put(m);
        return b.array();
    }

    private static DataInputStream din(Socket s) throws IOException {
        return new DataInputStream(s.getInputStream());
    }
    private static DataOutputStream dout(Socket s) throws IOException {
        return new DataOutputStream(s.getOutputStream());
    }

    /* ---- assertions ---- */

    private static void check(boolean cond, String msg) {
        if (!cond) { System.err.println("FAIL: " + msg); System.exit(1); }
    }

    /* ===================== tests ===================== */

    /**
     * Listener test: handshake on conn #1, drop, assert the client re-dials
     * (server sees a 2nd accept) and the retried query succeeds.
     */
    private static void testReconnectOnDrop() throws Exception {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));
        AtomicInteger accepts = new AtomicInteger(0);
        final boolean[] secondQueryServed = { false };

        Thread server = new Thread(() -> {
            try {
                // conn #1: HELLO -> HELLO_OK, then DROP (client's next recv = EOF).
                Socket c1 = ln.accept();
                accepts.incrementAndGet();
                SrvFrame f1 = srvRead(din(c1));
                check(f1.type == MSG_HELLO, "conn1: expected HELLO");
                srvWrite(dout(c1), MSG_HELLO_OK, (short) 0, 0, null);
                c1.close(); // drop mid-session

                // conn #2: the reconnect — handshake, then serve the retried query.
                Socket c2 = ln.accept();
                accepts.incrementAndGet();
                DataInputStream in2 = din(c2);
                DataOutputStream out2 = dout(c2);
                SrvFrame h2 = srvRead(in2);
                check(h2.type == MSG_HELLO, "conn2: expected HELLO");
                srvWrite(out2, MSG_HELLO_OK, (short) 0, 0, null);
                SrvFrame q = srvRead(in2);
                check(q.type == MSG_QUERY, "conn2: expected QUERY");
                srvWrite(out2, MSG_QUERY_HDR, FLAG_FIN, q.reqId, emptyQueryHdr());
                secondQueryServed[0] = true;
                c2.close();
            } catch (IOException e) {
                // accept may unblock with an error after the test closes ln.
            }
        });
        server.setDaemon(true);
        server.start();

        int port = ln.getLocalPort();
        try (TsdbClient cl = new TsdbClient("127.0.0.1", port, 1000)) {
            // Issued on the dropped conn #1: first recv hits EOF, client
            // reconnects (accept #2), re-runs the query, which succeeds.
            TsdbClient.QueryResult r = cl.query("SELECT 1");
            check(r != null, "query across reconnect returned null");
        }
        server.join(2000);
        ln.close();
        check(accepts.get() == 2, "expected 2 accepts (re-dial), got " + accepts.get());
        check(secondQueryServed[0], "server never served the retried query");
        System.out.println("ok  testReconnectOnDrop (accepts=" + accepts.get() + ")");
    }

    /**
     * Opt-out: with maxReconnect=0 a dropped connection surfaces the transport
     * error and the client must NOT re-dial (only one accept).
     */
    private static void testReconnectDisabled() throws Exception {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));
        AtomicInteger accepts = new AtomicInteger(0);

        Thread server = new Thread(() -> {
            try {
                Socket c1 = ln.accept();
                accepts.incrementAndGet();
                srvRead(din(c1));                              // HELLO
                srvWrite(dout(c1), MSG_HELLO_OK, (short) 0, 0, null);
                c1.close();                                    // drop
                // A 2nd accept only happens if the client (wrongly) reconnected.
                ln.setSoTimeout(400);
                try { Socket c2 = ln.accept(); accepts.incrementAndGet(); c2.close(); }
                catch (IOException ignore) { /* expected: no reconnect */ }
            } catch (IOException e) { /* ignore */ }
        });
        server.setDaemon(true);
        server.start();

        int port = ln.getLocalPort();
        boolean threw = false;
        try (TsdbClient cl = new TsdbClient("127.0.0.1", port, 1000)) {
            cl.setMaxReconnect(0); // opt out
            try { cl.query("SELECT 1"); }
            catch (IOException e) { threw = true; }
        }
        server.join(2000);
        ln.close();
        check(threw, "query should have thrown with reconnect disabled");
        check(accepts.get() == 1, "reconnect disabled but client re-dialed: accepts=" + accepts.get());
        System.out.println("ok  testReconnectDisabled (accepts=" + accepts.get() + ")");
    }

    /**
     * WriteBatch idempotency — POST-SEND drop: the server fully reads the batch
     * then drops without acking (client's ack recv = EOF).  The client must
     * reconnect for future calls but NOT auto-resend (would risk duplicate
     * rows); the error is surfaced and conn #2 sees no WRITE_BATCH.
     */
    private static void testWriteBatchNoRetryAfterSend() throws Exception {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));
        final boolean[] sawWriteOnConn2 = { false };

        Thread server = new Thread(() -> {
            try {
                // conn #1: HELLO, READ the batch (fully sent), drop before ack.
                Socket c1 = ln.accept();
                DataInputStream in1 = din(c1);
                srvRead(in1);                                  // HELLO
                srvWrite(dout(c1), MSG_HELLO_OK, (short) 0, 0, null);
                SrvFrame wb = srvRead(in1);                    // WRITE_BATCH
                check(wb.type == MSG_WRITE_BATCH, "conn1: expected WRITE_BATCH");
                c1.close();                                    // drop after read, no ack

                // conn #2: reconnect happens, but NO batch must be auto-resent.
                Socket c2 = ln.accept();
                DataInputStream in2 = din(c2);
                srvRead(in2);                                  // HELLO
                srvWrite(dout(c2), MSG_HELLO_OK, (short) 0, 0, null);
                c2.setSoTimeout(400);
                try {
                    SrvFrame f = srvRead(in2);
                    if (f.type == MSG_WRITE_BATCH) sawWriteOnConn2[0] = true;
                } catch (IOException ignore) { /* expected: no resend */ }
                c2.close();
            } catch (IOException e) { /* ignore */ }
        });
        server.setDaemon(true);
        server.start();

        int port = ln.getLocalPort();
        boolean threw = false;
        try (TsdbClient cl = new TsdbClient("127.0.0.1", port, 1000)) {
            List<TsdbClient.Column> cols = new ArrayList<>();
            cols.add(new TsdbClient.Column("ts", TsdbClient.T_TIMESTAMP));
            cols.add(new TsdbClient.Column("v",  TsdbClient.T_INT64));
            List<TsdbClient.Row> rows = new ArrayList<>();
            TsdbClient.Row row = new TsdbClient.Row();
            row.ts = 1;
            java.util.Map<Integer, Long> i64 = new java.util.HashMap<>();
            i64.put(1, 42L);
            row.i64 = i64;
            rows.add(row);
            try { cl.writeBatch("t", cols, rows); }
            catch (IOException e) { threw = true; }
        }
        server.join(2000);
        ln.close();
        check(threw, "writeBatch must throw on a post-send drop (no silent retry)");
        check(!sawWriteOnConn2[0], "writeBatch was auto-resent after a post-send drop (dup risk)");
        System.out.println("ok  testWriteBatchNoRetryAfterSend");
    }

    /**
     * DROP_TABLE round-trip against the mock listener (reusing the reconnect
     * scaffolding): the server must see MSG_DROP_TABLE carrying the raw table
     * name as payload; a generic-OK reply means success and an MSG_ERROR
     * reply is decoded and thrown.
     */
    private static void testDropTable() throws Exception {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));
        final String[] gotName = { null };

        Thread server = new Thread(() -> {
            try {
                Socket c1 = ln.accept();
                DataInputStream in1 = din(c1);
                DataOutputStream out1 = dout(c1);
                srvRead(in1);                                  // HELLO
                srvWrite(out1, MSG_HELLO_OK, (short) 0, 0, null);

                // Drop #1: succeed.  Payload must be the bare table name.
                SrvFrame d1 = srvRead(in1);
                check(d1.type == MSG_DROP_TABLE, "expected DROP_TABLE");
                gotName[0] = new String(d1.payload, java.nio.charset.StandardCharsets.UTF_8);
                srvWrite(out1, MSG_HELLO_OK, FLAG_FIN, d1.reqId, null); // generic OK

                // Drop #2: fail with a server error frame.
                SrvFrame d2 = srvRead(in1);
                check(d2.type == MSG_DROP_TABLE, "expected 2nd DROP_TABLE");
                srvWrite(out1, MSG_ERROR, FLAG_FIN, d2.reqId, errPayload(-3, "no such table"));
                c1.close();
            } catch (IOException e) { /* ignore */ }
        });
        server.setDaemon(true);
        server.start();

        int port = ln.getLocalPort();
        boolean threw = false;
        String errMsg = "";
        int errCode = 0;
        try (TsdbClient cl = new TsdbClient("127.0.0.1", port, 1000)) {
            cl.dropTable("trades");
            // A decoded MSG_ERROR must surface as the typed TsdbServerException
            // carrying the server code, not a plain IOException.
            try { cl.dropTable("missing"); }
            catch (TsdbServerException e) { threw = true; errMsg = e.getMessage(); errCode = e.code; }
            catch (IOException e) { threw = true; errMsg = e.getMessage(); }
        }
        server.join(2000);
        ln.close();
        check("trades".equals(gotName[0]),
                "DROP_TABLE payload = " + gotName[0] + ", want raw name trades");
        check(threw, "dropTable on MSG_ERROR reply must throw");
        check(errCode == -3, "TsdbServerException.code = " + errCode
                + ", want -3 (plain IOException thrown instead?)");
        check(errMsg != null && errMsg.contains("no such table"),
                "error should carry the server message, got: " + errMsg);
        System.out.println("ok  testDropTable");
    }

    /**
     * SYMBOL decode on the binary query path: the server emits SYMBOL result
     * columns as a length-prefixed string block ({@code [u32 blocklen]} +
     * nrows × {@code [u16 len][bytes]}, see emit_query_chunk) — not dict IDs.
     * The client must therefore yield {@link String} cells, matching the Go
     * SDK and the HTTP driver, and keep the following fixed-width columns
     * correctly aligned.
     */
    private static void testQuerySymbolDecode() throws Exception {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));

        // HDR payload: [ncols u16] then per column [name_len u8][name][type u8].
        byte[] tsName  = "ts".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] tagName = "tag".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] vName   = "v".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer hb = ByteBuffer.allocate(2 + 3 * 2 + tsName.length + tagName.length + vName.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        hb.putShort((short) 3);
        hb.put((byte) tsName.length).put(tsName).put(TsdbClient.T_TIMESTAMP);
        hb.put((byte) tagName.length).put(tagName).put(TsdbClient.T_SYMBOL);
        hb.put((byte) vName.length).put(vName).put(TsdbClient.T_INT64);
        byte[] hdrPayload = hb.array();

        // ROWS payload: [nrows u32][ncols u16], ts col (2×8), SYMBOL block
        // ([u32 blocklen][u16 len][bytes]…), then v col (2×8) AFTER the
        // variable-length block — mis-decoding the symbol block as 8-byte
        // cells would shear v off its values.
        byte[] aa  = "aa".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] bbb = "bbb".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int blockLen = 2 + aa.length + 2 + bbb.length;
        ByteBuffer rb = ByteBuffer.allocate(6 + 16 + 4 + blockLen + 16)
                .order(ByteOrder.LITTLE_ENDIAN);
        rb.putInt(2).putShort((short) 3);
        rb.putLong(111L).putLong(222L);            // ts
        rb.putInt(blockLen);                       // tag (SYMBOL strings)
        rb.putShort((short) aa.length).put(aa);
        rb.putShort((short) bbb.length).put(bbb);
        rb.putLong(7L).putLong(8L);                // v
        byte[] rowsPayload = rb.array();

        Thread server = new Thread(() -> {
            try {
                Socket c1 = ln.accept();
                DataInputStream in1 = din(c1);
                DataOutputStream out1 = dout(c1);
                srvRead(in1);                                  // HELLO
                srvWrite(out1, MSG_HELLO_OK, (short) 0, 0, null);
                SrvFrame q = srvRead(in1);
                check(q.type == MSG_QUERY, "expected QUERY");
                srvWrite(out1, MSG_QUERY_HDR, (short) 0, q.reqId, hdrPayload);
                srvWrite(out1, MSG_QUERY_ROWS, FLAG_FIN, q.reqId, rowsPayload);
                c1.close();
            } catch (IOException e) { /* ignore */ }
        });
        server.setDaemon(true);
        server.start();

        int port = ln.getLocalPort();
        try (TsdbClient cl = new TsdbClient("127.0.0.1", port, 1000)) {
            TsdbClient.QueryResult r = cl.query("SELECT ts, tag, v FROM t");
            check(r.rows.size() == 2, "want 2 rows, got " + r.rows.size());
            Object s0 = r.rows.get(0)[1];
            Object s1 = r.rows.get(1)[1];
            check(s0 instanceof String, "SYMBOL cell is " +
                    (s0 == null ? "null" : s0.getClass().getSimpleName()) + ", want String");
            check("aa".equals(s0),  "row0 tag = " + s0 + ", want aa");
            check("bbb".equals(s1), "row1 tag = " + s1 + ", want bbb");
            check(Long.valueOf(111L).equals(r.rows.get(0)[0]), "row0 ts mangled: " + r.rows.get(0)[0]);
            check(Long.valueOf(7L).equals(r.rows.get(0)[2]), "row0 v misaligned: " + r.rows.get(0)[2]);
            check(Long.valueOf(8L).equals(r.rows.get(1)[2]), "row1 v misaligned: " + r.rows.get(1)[2]);
        }
        server.join(2000);
        ln.close();
        System.out.println("ok  testQuerySymbolDecode");
    }

    /** Table-driven classifier test: transport-worthy vs server/other errors. */
    private static void testTransportClassifier() {
        Object[][] cases = {
            { new EOFException(),                                          true,  "eof" },
            { new SocketException("Broken pipe"),                         true,  "broken pipe" },
            { new SocketException("Connection reset"),                    true,  "connection reset" },
            { new SocketException("Connection reset by peer"),            true,  "connection reset by peer" },
            { new SocketException("Socket closed"),                       true,  "socket closed" },
            { new SocketException("Connection refused"),                  true,  "connection refused" },
            { new SocketException(),                                      true,  "bare SocketException" },
            // Server application error (decoded MSG_ERROR) — must NOT reconnect.
            { new IOException("server rc=7: no such table"),              false, "server app error" },
            { new IOException("crc mismatch"),                            false, "crc mismatch (frame-level)" },
            { new IOException("unexpected HELLO response type=3"),        false, "protocol error" },
            { new IOException("some other failure"),                      false, "generic IOException" },
        };
        for (Object[] c : cases) {
            IOException e = (IOException) c[0];
            boolean want = (Boolean) c[1];
            boolean got = TsdbClient.isTransportError(e);
            check(got == want, "isTransportError(" + c[2] + ") = " + got + ", want " + want);
        }
        System.out.println("ok  testTransportClassifier (" + cases.length + " cases)");
    }

    public static void main(String[] args) throws Exception {
        testTransportClassifier();
        testReconnectOnDrop();
        testReconnectDisabled();
        testWriteBatchNoRetryAfterSend();
        testDropTable();
        testQuerySymbolDecode();
        System.out.println("ALL RECONNECT TESTS PASSED");
    }
}
