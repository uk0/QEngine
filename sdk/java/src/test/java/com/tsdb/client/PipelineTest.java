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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

/**
 * Self-contained {@link WritePipeline} tests, in the same runnable-main style
 * as {@link ReconnectTest} (no JUnit on the classpath).  Exits non-zero on
 * the first failed assertion.
 *
 * <p>The mock listener speaks just enough of the wire protocol (HELLO
 * handshake + per-frame WRITE_ACKs).  The latency ack loop models LINK
 * latency — delays on outstanding frames overlap, as on a real network —
 * which is exactly the cost pipelining is meant to hide.
 */
public class PipelineTest {

    /* ---- server-side wire codec (mirrors TsdbClient.send/recv) ---- */

    private static final int MAGIC = 0x42445354;
    private static final byte VER  = 1;
    private static final int  HDR  = 24;
    private static final byte MSG_HELLO        = 1;
    private static final byte MSG_HELLO_OK     = 2;
    private static final byte MSG_ERROR        = 3;
    private static final byte MSG_WRITE_BATCH  = 30;
    private static final byte MSG_WRITE_ACK    = 34;
    private static final byte MSG_QUERY        = 40;
    private static final byte MSG_QUERY_HDR    = 41;
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

    private static byte[] writeAck(int n) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(n).array();
    }

    /** Zero-column QUERY_HDR with FIN set: a complete empty result. */
    private static byte[] emptyQueryHdr() {
        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) 0).array();
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

    private static void check(boolean cond, String msg) {
        if (!cond) { System.err.println("FAIL: " + msg); System.exit(1); }
    }

    /* ---- mock server scaffolding ---- */

    private interface ConnHandler { void handle(Socket c) throws IOException; }

    private static ServerSocket listen() throws IOException {
        ServerSocket ln = new ServerSocket();
        ln.bind(new InetSocketAddress("127.0.0.1", 0));
        return ln;
    }

    /** Accepts ONE connection, completes the HELLO handshake, and hands the
     *  socket to the handler.  The socket closes when the handler returns. */
    private static Thread serveOne(ServerSocket ln, ConnHandler h) {
        Thread t = new Thread(() -> {
            try (Socket c = ln.accept()) {
                SrvFrame f = srvRead(din(c));
                check(f.type == MSG_HELLO, "mock server: expected HELLO");
                srvWrite(dout(c), MSG_HELLO_OK, (short) 0, 0, null);
                h.handle(c);
            } catch (IOException ignore) { /* client closed — normal end */ }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Reads WRITE_BATCH frames and acks each IN ORDER, delayed until the
     * frame's arrival time plus delayNs.  Delays on outstanding frames
     * overlap (link latency, not serial server processing).  Returns when
     * the client closes the connection.
     */
    private static void latencyAckLoop(Socket c, long delayNs,
                                       AtomicInteger frames, List<Long> reqIds) throws IOException {
        DataInputStream in = din(c);
        DataOutputStream out = dout(c);
        BlockingQueue<long[]> q = new LinkedBlockingQueue<>();
        Thread acker = new Thread(() -> {
            try {
                for (;;) {
                    long[] p = q.take();
                    if (p.length == 0) return; // poison pill
                    long ns = p[1] - System.nanoTime();
                    if (ns > 0) Thread.sleep(ns / 1_000_000L, (int) (ns % 1_000_000L));
                    srvWrite(out, MSG_WRITE_ACK, (short) 0, p[0], writeAck(1));
                }
            } catch (Exception ignore) { /* conn torn down */ }
        });
        acker.setDaemon(true);
        acker.start();
        try {
            for (;;) {
                SrvFrame f;
                try { f = srvRead(in); } catch (IOException eof) { break; }
                if (f.type != MSG_WRITE_BATCH) break;
                frames.incrementAndGet();
                if (reqIds != null) reqIds.add(f.reqId);
                q.add(new long[] { f.reqId, System.nanoTime() + delayNs });
            }
        } finally {
            q.add(new long[0]);
            try { acker.join(2000); } catch (InterruptedException ignore) { }
        }
    }

    /* ---- batch fixtures ---- */

    private static List<TsdbClient.Column> cols() {
        List<TsdbClient.Column> cols = new ArrayList<>();
        cols.add(new TsdbClient.Column("ts", TsdbClient.T_TIMESTAMP));
        cols.add(new TsdbClient.Column("v",  TsdbClient.T_INT64));
        return cols;
    }

    private static List<TsdbClient.Row> rows(long ts) {
        TsdbClient.Row r = new TsdbClient.Row();
        r.ts = ts;
        Map<Integer, Long> i64 = new HashMap<>();
        i64.put(1, ts * 10);
        r.i64 = i64;
        List<TsdbClient.Row> rows = new ArrayList<>();
        rows.add(r);
        return rows;
    }

    /* ===================== tests ===================== */

    /**
     * (a) Correctness: depth 4, 12 writes + flush against a mock that acks in
     * order after a small delay.  The server must see all 12 frames with
     * strictly ascending (FIFO) req_ids, and flush must consume every ack
     * without error (the client verifies each echoed req_id against the
     * oldest in-flight one).
     */
    private static void testPipelineFifoAcks() throws Exception {
        ServerSocket ln = listen();
        AtomicInteger frames = new AtomicInteger();
        List<Long> reqIds = new ArrayList<>();
        Thread srv = serveOne(ln, c -> latencyAckLoop(c, 3_000_000L, frames, reqIds));

        try (TsdbClient cl = new TsdbClient("127.0.0.1", ln.getLocalPort(), 1000)) {
            WritePipeline p = cl.newWritePipeline(4);
            for (int i = 0; i < 12; i++) p.write("t", cols(), rows(i + 1));
            p.flush(); // drains all 12 acks; any FIFO violation would throw
            p.close();
        } // client close unblocks the mock read loop
        srv.join(3000);
        ln.close();

        check(frames.get() == 12, "server saw " + frames.get() + " WRITE_BATCH frames, want 12");
        check(reqIds.size() == 12, "recorded " + reqIds.size() + " req_ids, want 12");
        for (int i = 1; i < reqIds.size(); i++)
            check(reqIds.get(i) > reqIds.get(i - 1),
                  "req_ids not FIFO/ascending at index " + i + ": " + reqIds);
        System.out.println("ok  testPipelineFifoAcks (frames=12, reqIds FIFO)");
    }

    /**
     * Server rejection of ONE mid-window batch: MSG_ERROR must surface as an
     * IOException naming that batch's index, the drain must continue, and the
     * pipeline must stay usable for further writes afterwards.
     */
    private static void testPipelineServerReject() throws Exception {
        ServerSocket ln = listen();
        AtomicInteger frames = new AtomicInteger();
        Thread srv = serveOne(ln, c -> {
            DataInputStream in = din(c);
            DataOutputStream out = dout(c);
            for (;;) {
                SrvFrame f;
                try { f = srvRead(in); } catch (IOException eof) { break; }
                if (f.type != MSG_WRITE_BATCH) break;
                int idx = frames.getAndIncrement();
                if (idx == 2) srvWrite(out, MSG_ERROR, (short) 0, f.reqId, errPayload(-9, "rejected"));
                else          srvWrite(out, MSG_WRITE_ACK, (short) 0, f.reqId, writeAck(1));
            }
        });

        try (TsdbClient cl = new TsdbClient("127.0.0.1", ln.getLocalPort(), 1000)) {
            WritePipeline p = cl.newWritePipeline(4);
            for (int i = 0; i < 6; i++) p.write("t", cols(), rows(i + 1));
            String msg = "";
            try { p.flush(); check(false, "flush must surface the rejected batch"); }
            catch (IOException e) { msg = e.getMessage(); }
            check(msg.contains("batch #2"), "rejection should name batch #2, got: " + msg);
            check(msg.contains("rejected"), "rejection should carry the server message, got: " + msg);
            // Pipeline NOT broken: another write + clean flush must work.
            p.write("t", cols(), rows(7));
            p.flush();
            p.close();
        }
        srv.join(3000);
        ln.close();
        check(frames.get() == 7, "server saw " + frames.get() + " frames, want 7");
        System.out.println("ok  testPipelineServerReject (batch #2 rejected, pipeline survived)");
    }

    /**
     * (b) Overlap: against per-frame ack latency D, 12 synchronous writeBatch
     * calls cost >= 12*D wall time; a depth-4 pipeline must come in under 60%
     * of that budget (the ideal is ~D + serialization).
     */
    private static void testPipelineOverlapsAcks() throws Exception {
        final long delayNs = 5_000_000L; // 5 ms per-frame link latency
        final int n = 12;

        long syncNs = timedRun(delayNs, n, cl -> {
            for (int i = 0; i < n; i++) cl.writeBatch("t", cols(), rows(i + 1));
        });
        long pipeNs = timedRun(delayNs, n, cl -> {
            try (WritePipeline p = cl.newWritePipeline(4)) {
                for (int i = 0; i < n; i++) p.write("t", cols(), rows(i + 1));
            } // close() flushes inside the timed window
        });

        long budgetNs = n * delayNs;
        double ratio = (double) pipeNs / (double) syncNs;
        System.out.printf("    sync=%.1fms pipelined=%.1fms ratio=%.2f (budget %dms)%n",
                syncNs / 1e6, pipeNs / 1e6, ratio, budgetNs / 1_000_000L);
        check(syncNs >= budgetNs,
              "sync lower bound violated: " + syncNs + "ns < " + budgetNs + "ns — latency model broken");
        check(pipeNs < budgetNs * 6 / 10,
              "pipelining did not overlap acks: pipelined=" + pipeNs / 1_000_000L
              + "ms, want < 60% of " + budgetNs / 1_000_000L + "ms");
        System.out.println("ok  testPipelineOverlapsAcks");
    }

    private interface ClientDriver { void drive(TsdbClient cl) throws IOException; }

    private static long timedRun(long delayNs, int wantFrames, ClientDriver d) throws Exception {
        ServerSocket ln = listen();
        AtomicInteger frames = new AtomicInteger();
        Thread srv = serveOne(ln, c -> latencyAckLoop(c, delayNs, frames, null));
        long t0, t1;
        try (TsdbClient cl = new TsdbClient("127.0.0.1", ln.getLocalPort(), 2000)) {
            t0 = System.nanoTime();
            d.drive(cl);
            t1 = System.nanoTime();
        }
        srv.join(3000);
        ln.close();
        check(frames.get() == wantFrames,
              "server saw " + frames.get() + " frames, want " + wantFrames);
        return t1 - t0;
    }

    /**
     * (c) Transport break: the server acks k frames then hard-closes the
     * connection mid-pipeline.  A write must surface a transport error; the
     * pipeline must be sticky-broken (same error instance from write/flush/
     * close, immediately, with no I/O and no auto-resend of in-flight
     * batches); and after close releases the guard the PARENT client must
     * recover via its normal reconnect path.
     */
    private static void testPipelineBreaksOnDrop() throws Exception {
        final int acksBeforeClose = 3;
        ServerSocket ln = listen();
        final boolean[] sawWriteOnConn2 = { false };

        Thread srv = new Thread(() -> {
            try {
                // conn #1: HELLO, ack k frames, then drop with frames in flight.
                Socket c1 = ln.accept();
                DataInputStream in1 = din(c1);
                DataOutputStream out1 = dout(c1);
                srvRead(in1); // HELLO
                srvWrite(out1, MSG_HELLO_OK, (short) 0, 0, null);
                for (int i = 0; i < acksBeforeClose; i++) {
                    SrvFrame f = srvRead(in1);
                    check(f.type == MSG_WRITE_BATCH, "conn1: expected WRITE_BATCH");
                    srvWrite(out1, MSG_WRITE_ACK, (short) 0, f.reqId, writeAck(1));
                }
                c1.close();

                // conn #2: the parent client's reconnect.  NO batch may be
                // auto-resent — the first frame must be the query.
                Socket c2 = ln.accept();
                DataInputStream in2 = din(c2);
                DataOutputStream out2 = dout(c2);
                srvRead(in2); // HELLO
                srvWrite(out2, MSG_HELLO_OK, (short) 0, 0, null);
                SrvFrame f2 = srvRead(in2);
                if (f2.type == MSG_WRITE_BATCH) sawWriteOnConn2[0] = true;
                if (f2.type == MSG_QUERY)
                    srvWrite(out2, MSG_QUERY_HDR, FLAG_FIN, f2.reqId, emptyQueryHdr());
                c2.close();
            } catch (IOException ignore) { /* teardown */ }
        });
        srv.setDaemon(true);
        srv.start();

        try (TsdbClient cl = new TsdbClient("127.0.0.1", ln.getLocalPort(), 1000)) {
            WritePipeline p = cl.newWritePipeline(2);
            IOException firstErr = null;
            int failedAt = -1;
            for (int i = 0; i < 12; i++) {
                try { p.write("t", cols(), rows(i + 1)); }
                catch (IOException e) { firstErr = e; failedAt = i; break; }
            }
            check(firstErr != null, "expected a transport error mid-pipeline; all 12 writes succeeded");

            // Sticky: the same error instance, immediately, no socket I/O.
            long t0 = System.nanoTime();
            try { p.write("t", cols(), rows(99)); check(false, "write on broken pipeline must throw"); }
            catch (IOException e) { check(e == firstErr, "write rethrew a different error: " + e); }
            try { p.flush(); check(false, "flush on broken pipeline must throw"); }
            catch (IOException e) { check(e == firstErr, "flush rethrew a different error: " + e); }
            long stickyMs = (System.nanoTime() - t0) / 1_000_000L;
            check(stickyMs < 100, "sticky failure took " + stickyMs + "ms, must be immediate");

            try { p.close(); check(false, "close on broken pipeline must rethrow the sticky error"); }
            catch (IOException e) { check(e == firstErr, "close rethrew a different error: " + e); }
            p.close(); // idempotent: second close is a silent no-op

            // Guard released: the parent's next call reconnects and succeeds.
            TsdbClient.QueryResult r = cl.query("SELECT 1");
            check(r != null, "parent query after broken pipeline returned null");
            System.out.println("ok  testPipelineBreaksOnDrop (failedAt=write#" + failedAt
                    + ", sticky, parent reconnected)");
        }
        srv.join(3000);
        ln.close();
        check(!sawWriteOnConn2[0], "a WRITE_BATCH was auto-resent after the pipeline broke (dup risk)");
    }

    /**
     * (d) Guard: while a pipeline is open, query/writeBatch/login and a second
     * newWritePipeline on the parent client throw IllegalStateException;
     * depth < 1 throws IllegalArgumentException; depth > 64 clamps.  close()
     * releases the guard.
     */
    private static void testPipelineGuard() throws Exception {
        ServerSocket ln = listen();
        Thread srv = serveOne(ln, c -> {
            // Served only AFTER the pipeline closes.
            SrvFrame q = srvRead(din(c));
            check(q.type == MSG_QUERY, "guard: expected QUERY, got type=" + q.type);
            srvWrite(dout(c), MSG_QUERY_HDR, FLAG_FIN, q.reqId, emptyQueryHdr());
        });

        try (TsdbClient cl = new TsdbClient("127.0.0.1", ln.getLocalPort(), 1000)) {
            try { cl.newWritePipeline(0); check(false, "depth 0 must throw"); }
            catch (IllegalArgumentException expected) { }

            WritePipeline p = cl.newWritePipeline(4);
            try { cl.query("SELECT 1"); check(false, "query during open pipeline must throw"); }
            catch (IllegalStateException expected) { }
            try { cl.writeBatch("t", cols(), rows(1)); check(false, "writeBatch during open pipeline must throw"); }
            catch (IllegalStateException expected) { }
            try { cl.login("u", "p"); check(false, "login during open pipeline must throw"); }
            catch (IllegalStateException expected) { }
            try { cl.newWritePipeline(4); check(false, "second newWritePipeline must throw"); }
            catch (IllegalStateException expected) { }

            p.close(); // nothing in flight: clean release
            try { p.write("t", cols(), rows(1)); check(false, "write after close must throw"); }
            catch (IllegalStateException expected) { }

            // Guard released: parent works again; depth > 64 clamps, not throws.
            TsdbClient.QueryResult r = cl.query("SELECT 1");
            check(r != null, "query after pipeline close failed");
            cl.newWritePipeline(999).close();
        }
        srv.join(3000);
        ln.close();
        System.out.println("ok  testPipelineGuard");
    }

    public static void main(String[] args) throws Exception {
        testPipelineFifoAcks();
        testPipelineServerReject();
        testPipelineOverlapsAcks();
        testPipelineBreaksOnDrop();
        testPipelineGuard();
        System.out.println("ALL PIPELINE TESTS PASSED");
    }
}
