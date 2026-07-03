package com.tsdb.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Self-contained test for {@link TsdbClient#registerUdf} /
 * {@link TsdbClient#dropUdf}.  Like {@code ReconnectTest} this is a runnable
 * {@code main} self-test (the SDK ships no JUnit): a mock listener speaks
 * just enough of the wire protocol (HELLO + query replies) to capture the
 * SQL each helper sends, and the test asserts the exact strings against the
 * server's CREATE FUNCTION grammar.  Exits non-zero on the first failure.
 */
public class UdfSqlTest {

    /* ---- server-side wire codec (mirrors TsdbClient.send/recv) ---- */

    private static final int MAGIC = 0x42445354;
    private static final byte VER  = 1;
    private static final int  HDR  = 24;
    private static final byte MSG_HELLO     = 1;
    private static final byte MSG_HELLO_OK  = 2;
    private static final byte MSG_QUERY     = 40;
    private static final byte MSG_QUERY_HDR = 41;
    private static final short FLAG_FIN     = 0x0001;

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
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(h.array(), 4, HDR - 4);
        if (payload != null && payload.length > 0) crc.update(payload, 0, payload.length);
        out.write(h.array());
        if (payload != null && payload.length > 0) out.write(payload);
        ByteBuffer t = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        t.putInt((int) crc.getValue());
        out.write(t.array());
        out.flush();
    }

    /** QUERY_RESULT_HDR payload for a zero-column result (ncols=0). */
    private static byte[] emptyQueryHdr() {
        return new byte[] { 0, 0 };
    }

    private static void check(boolean cond, String msg) {
        if (!cond) { System.err.println("FAIL: " + msg); System.exit(1); }
    }

    public static void main(String[] args) throws Exception {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));

        final int nQueries = 3;
        final List<String> got = new ArrayList<>();

        Thread server = new Thread(() -> {
            try {
                Socket c = ln.accept();
                DataInputStream in = new DataInputStream(c.getInputStream());
                DataOutputStream out = new DataOutputStream(c.getOutputStream());
                SrvFrame hello = srvRead(in);
                check(hello.type == MSG_HELLO, "expected HELLO");
                srvWrite(out, MSG_HELLO_OK, (short) 0, 0, null);
                for (int i = 0; i < nQueries; i++) {
                    SrvFrame q = srvRead(in);
                    check(q.type == MSG_QUERY, "frame " + i + ": expected QUERY");
                    synchronized (got) { got.add(new String(q.payload, "UTF-8")); }
                    srvWrite(out, MSG_QUERY_HDR, FLAG_FIN, q.reqId, emptyQueryHdr());
                }
                c.close();
            } catch (IOException e) {
                // ln.close() below can unblock accept with an error.
            }
        });
        server.setDaemon(true);
        server.start();

        try (TsdbClient cl = new TsdbClient("127.0.0.1", ln.getLocalPort(), 1000)) {
            cl.registerUdf("my_double", new byte[] { 3, 2, 1 }, (byte) 3,
                           "/opt/udf/x.so", "udf_double");
            cl.registerUdf("nofn", new byte[0], (byte) 2, "/opt/udf/y.so", null);
            cl.dropUdf("my_double");

            // Unsupported type byte fails client-side, nothing hits the wire.
            boolean threw = false;
            try {
                cl.registerUdf("bad", new byte[] { 4 }, (byte) 2, "/p.so", null);
            } catch (IllegalArgumentException e) {
                threw = true;
            }
            check(threw, "type byte 4 (SYMBOL) must throw IllegalArgumentException");
        }

        server.join(2000);
        ln.close();

        synchronized (got) {
            check(got.size() == nQueries, "expected " + nQueries + " queries, got " + got.size());
            check(got.get(0).equals(
                "CREATE FUNCTION my_double(FLOAT64, INT64, TIMESTAMP) "
                + "RETURNS FLOAT64 FROM '/opt/udf/x.so' SYMBOL 'udf_double'"),
                "registerUdf SQL mismatch: " + got.get(0));
            check(got.get(1).equals(
                "CREATE FUNCTION nofn() RETURNS INT64 FROM '/opt/udf/y.so'"),
                "registerUdf(no symbol) SQL mismatch: " + got.get(1));
            check(got.get(2).equals("DROP FUNCTION my_double"),
                "dropUdf SQL mismatch: " + got.get(2));
        }

        System.out.println("ok  UdfSqlTest (" + nQueries + " SQL strings verified)");
    }
}
