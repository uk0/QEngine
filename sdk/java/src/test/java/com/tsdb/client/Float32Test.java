package com.tsdb.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;

import com.tsdb.jdbc.TsdbResultSet;

/**
 * Self-contained FLOAT32 round-trip test, in the same runnable-main style as
 * {@link ReconnectTest} (no JUnit on the classpath).  A mock listener speaks
 * just enough of the wire protocol to verify both halves of the FLOAT32
 * contract (server type byte 5 travels as an 8-byte double on the wire):
 *
 * <ul>
 *   <li>encode — {@code writeBatch} on a T_FLOAT32 column must emit
 *       nrows×8 bytes of double bits (values from {@link TsdbClient.Row#f64},
 *       like the Go SDK's Row.F64), verified server-side;</li>
 *   <li>decode — QUERY result cells for a T_FLOAT32 column must come back as
 *       {@link Double} (pre-fix they were silently left null);</li>
 *   <li>JDBC — {@link TsdbResultSet} maps T_FLOAT32 to {@link Types#REAL}
 *       ("FLOAT32") and getFloat/getObject read the cells.</li>
 * </ul>
 *
 * The same mock conversation also drives the other v-next client additions:
 * the typed {@link TsdbServerException}, {@code ping()} (payload echo), and
 * {@code clusterStats()} (kv payload decode).  Exits non-zero on the first
 * failed assertion.
 */
public class Float32Test {

    /* ---- server-side wire codec (mirrors TsdbClient.send/recv) ---- */

    private static final int MAGIC = 0x42445354;
    private static final byte VER  = 1;
    private static final int  HDR  = 24;
    private static final byte MSG_HELLO         = 1;
    private static final byte MSG_HELLO_OK      = 2;
    private static final byte MSG_ERROR         = 3;
    private static final byte MSG_PING          = 4;
    private static final byte MSG_WRITE_BATCH   = 30;
    private static final byte MSG_WRITE_ACK     = 34;
    private static final byte MSG_QUERY         = 40;
    private static final byte MSG_QUERY_HDR     = 41;
    private static final byte MSG_QUERY_ROWS    = 42;
    private static final byte MSG_CLUSTER_STATS = 60;
    private static final short FLAG_FIN  = 0x0001;
    private static final short FLAG_PONG = 0x0008;

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

    /** MSG_ERROR payload in the spec shape [i32 rc][u16 msg_len][msg]. */
    private static byte[] errPayload(int rc, String msg) {
        byte[] m = msg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(6 + m.length).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(rc);
        b.putShort((short) m.length);
        b.put(m);
        return b.array();
    }

    /** CLUSTER_STATS kv payload: [kv_count u16] + per pair [klen u8][key][vlen u8][val]. */
    private static byte[] statsPayload(String[][] pairs) {
        int size = 2;
        for (String[] kv : pairs) size += 2 + kv[0].length() + kv[1].length();
        ByteBuffer b = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        b.putShort((short) pairs.length);
        for (String[] kv : pairs) {
            byte[] k = kv[0].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] v = kv[1].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            b.put((byte) k.length).put(k);
            b.put((byte) v.length).put(v);
        }
        return b.array();
    }

    private static void check(boolean cond, String msg) {
        if (!cond) { System.err.println("FAIL: " + msg); System.exit(1); }
    }

    private static final int    NROWS = 3;
    private static final long[] WANT_TS = { 100L, 200L, 300L };
    private static final double[] WANT_F = { 1.5, -2.25, 42.0 };

    public static void main(String[] args) throws Exception {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));

        Thread server = new Thread(() -> {
            try {
                Socket c = ln.accept();
                DataInputStream in = new DataInputStream(c.getInputStream());
                DataOutputStream out = new DataOutputStream(c.getOutputStream());
                SrvFrame hello = srvRead(in);
                check(hello.type == MSG_HELLO, "expected HELLO");
                srvWrite(out, MSG_HELLO_OK, (short) 0, 0, null);

                // --- WRITE_BATCH: parse the columnar payload and verify the
                //     T_FLOAT32 column travels as nrows×8 bytes of double bits.
                SrvFrame wb = srvRead(in);
                check(wb.type == MSG_WRITE_BATCH, "expected WRITE_BATCH");
                ByteBuffer b = ByteBuffer.wrap(wb.payload).order(ByteOrder.LITTLE_ENDIAN);
                int tlen = b.get() & 0xFF;
                b.position(b.position() + tlen);       // table name
                int ncols = b.getShort() & 0xFFFF;
                int nrows = b.getInt();
                check(ncols == 2, "batch ncols=" + ncols + ", want 2");
                check(nrows == NROWS, "batch nrows=" + nrows + ", want " + NROWS);
                long[][] colBits = new long[ncols][nrows];
                byte[] colTypes = new byte[ncols];
                for (int ci = 0; ci < ncols; ci++) {
                    int nlen = b.get() & 0xFF;
                    b.position(b.position() + nlen);   // column name
                    colTypes[ci] = b.get();
                    b.get();                           // codec
                    int csz = b.getInt();
                    check(csz == nrows * 8, "col " + ci + " csz=" + csz
                            + ", want " + nrows * 8 + " (8 bytes/row)");
                    for (int ri = 0; ri < nrows; ri++) colBits[ci][ri] = b.getLong();
                }
                check(colTypes[1] == TsdbClient.T_FLOAT32,
                        "col 1 wire type=" + colTypes[1] + ", want 5 (T_FLOAT32)");
                for (int ri = 0; ri < nrows; ri++) {
                    double got = Double.longBitsToDouble(colBits[1][ri]);
                    check(got == WANT_F[ri], "encoded f[" + ri + "] = " + got
                            + ", want " + WANT_F[ri] + " (double bits on the wire)");
                }
                srvWrite(out, MSG_WRITE_ACK, FLAG_FIN, wb.reqId,
                        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
                                  .putInt(nrows).array());

                // --- QUERY #1: echo the batch back under a T_FLOAT32 header,
                //     completing the encode→decode round-trip.
                SrvFrame q = srvRead(in);
                check(q.type == MSG_QUERY, "expected QUERY");
                byte[] tsn = "ts".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                byte[] fn  = "f".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ByteBuffer hb = ByteBuffer.allocate(2 + 2 + tsn.length + 2 + fn.length)
                        .order(ByteOrder.LITTLE_ENDIAN);
                hb.putShort((short) 2);
                hb.put((byte) tsn.length).put(tsn).put(TsdbClient.T_TIMESTAMP);
                hb.put((byte) fn.length).put(fn).put(TsdbClient.T_FLOAT32);
                srvWrite(out, MSG_QUERY_HDR, (short) 0, q.reqId, hb.array());
                ByteBuffer rb = ByteBuffer.allocate(6 + 2 * nrows * 8)
                        .order(ByteOrder.LITTLE_ENDIAN);
                rb.putInt(nrows).putShort((short) 2);
                for (int ri = 0; ri < nrows; ri++) rb.putLong(colBits[0][ri]);
                for (int ri = 0; ri < nrows; ri++) rb.putLong(colBits[1][ri]);
                srvWrite(out, MSG_QUERY_ROWS, FLAG_FIN, q.reqId, rb.array());

                // --- QUERY #2: reply MSG_ERROR to exercise TsdbServerException.
                SrvFrame q2 = srvRead(in);
                check(q2.type == MSG_QUERY, "expected 2nd QUERY");
                srvWrite(out, MSG_ERROR, FLAG_FIN, q2.reqId, errPayload(-7, "boom"));

                // --- PING: echo the payload back (PONG|FIN), like server.c.
                SrvFrame pg = srvRead(in);
                check(pg.type == MSG_PING, "expected PING");
                srvWrite(out, MSG_PING, (short) (FLAG_PONG | FLAG_FIN), pg.reqId, pg.payload);

                // --- CLUSTER_STATS: HELLO_OK + kv payload, like handle_cluster_stats.
                SrvFrame cs = srvRead(in);
                check(cs.type == MSG_CLUSTER_STATS, "expected CLUSTER_STATS");
                srvWrite(out, MSG_HELLO_OK, FLAG_FIN, cs.reqId, statsPayload(new String[][] {
                        { "connections_active", "1" },
                        { "rows_written_total", "12345" },
                }));
                c.close();
            } catch (IOException e) {
                // ln.close() below can unblock accept with an error.
            }
        });
        server.setDaemon(true);
        server.start();

        try (TsdbClient cl = new TsdbClient("127.0.0.1", ln.getLocalPort(), 1000)) {
            // Encode: FLOAT32 values ride Row.f64 (a float caller widens, e.g.
            // f64.put(1, (double) 1.5f)) and pre-fix this threw
            // "unsupported column type 5".
            List<TsdbClient.Column> cols = new ArrayList<>();
            cols.add(new TsdbClient.Column("ts", TsdbClient.T_TIMESTAMP));
            cols.add(new TsdbClient.Column("f",  TsdbClient.T_FLOAT32));
            List<TsdbClient.Row> rows = new ArrayList<>();
            for (int i = 0; i < NROWS; i++) {
                TsdbClient.Row r = new TsdbClient.Row();
                r.ts = WANT_TS[i];
                Map<Integer, Double> f64 = new HashMap<>();
                f64.put(1, WANT_F[i]);
                r.f64 = f64;
                rows.add(r);
            }
            int acked = cl.writeBatch("f32t", cols, rows);
            check(acked == NROWS, "writeBatch acked=" + acked + ", want " + NROWS);

            // Decode: FLOAT32 cells must come back as Double (pre-fix: null).
            TsdbClient.QueryResult qr = cl.query("SELECT ts, f FROM f32t");
            check(qr.rows.size() == NROWS, "query rows=" + qr.rows.size() + ", want " + NROWS);
            check(qr.colTypes[1] == TsdbClient.T_FLOAT32, "hdr col type mangled");
            for (int i = 0; i < NROWS; i++) {
                Object cell = qr.rows.get(i)[1];
                check(cell instanceof Double, "f[" + i + "] cell is "
                        + (cell == null ? "null" : cell.getClass().getSimpleName())
                        + ", want Double — FLOAT32 decode regressed");
                check((Double) cell == WANT_F[i],
                        "f[" + i + "] = " + cell + ", want " + WANT_F[i]);
                check(Long.valueOf(WANT_TS[i]).equals(qr.rows.get(i)[0]),
                        "ts[" + i + "] mangled: " + qr.rows.get(i)[0]);
            }
            System.out.println("ok  float32 encode+decode round-trip ("
                    + NROWS + " rows through the mock wire)");

            // JDBC mapping over the same result: Types.REAL / "FLOAT32",
            // getFloat narrows, getObject yields the Double.
            TsdbResultSet rs = new TsdbResultSet(null, qr);
            ResultSetMetaData md = rs.getMetaData();
            check(md.getColumnType(2) == Types.REAL,
                    "getColumnType = " + md.getColumnType(2) + ", want Types.REAL");
            check("FLOAT32".equals(md.getColumnTypeName(2)),
                    "getColumnTypeName = " + md.getColumnTypeName(2) + ", want FLOAT32");
            check(rs.next(), "ResultSet.next");
            check(rs.getFloat(2) == 1.5f, "getFloat = " + rs.getFloat(2) + ", want 1.5");
            check(rs.getObject(2) instanceof Double, "getObject not a Double");
            check(rs.getDouble("f") == 1.5, "getDouble(name) = " + rs.getDouble("f"));
            System.out.println("ok  jdbc FLOAT32 mapping (Types.REAL, getFloat/getObject)");

            // Typed server error: code + message must ride the exception.
            int code = 0; String msg = "";
            try { cl.query("SELECT boom"); check(false, "error query must throw"); }
            catch (TsdbServerException e) { code = e.code; msg = e.getMessage(); }
            check(code == -7, "TsdbServerException.code = " + code + ", want -7");
            check(msg.contains("boom"), "exception message lost: " + msg);
            System.out.println("ok  TsdbServerException (code=" + code + ")");

            // Ping echo + cluster stats kv decode.
            cl.ping();
            System.out.println("ok  ping (payload echoed)");
            String stats = cl.clusterStats();
            check("connections_active=1\nrows_written_total=12345".equals(stats),
                    "clusterStats = " + stats);
            System.out.println("ok  clusterStats (" + stats.replace('\n', ' ') + ")");
        }

        server.join(2000);
        ln.close();
        System.out.println("ALL FLOAT32 TESTS PASSED");
    }
}
