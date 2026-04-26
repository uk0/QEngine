package com.tsdb.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
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

    public static final short FLAG_FIN  = 0x0001;

    /* Column types */
    public static final byte T_TIMESTAMP = 1;
    public static final byte T_INT64     = 2;
    public static final byte T_FLOAT64   = 3;
    public static final byte T_SYMBOL    = 4;

    private final Socket socket;
    private final DataInputStream  in;
    private final DataOutputStream out;
    private long nextReq = 1;
    private String token = "";

    public TsdbClient(String host, int port) throws IOException {
        this(host, port, 10_000);
    }

    public TsdbClient(String host, int port, int timeoutMs) throws IOException {
        this.socket = new Socket();
        this.socket.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
        this.socket.setTcpNoDelay(true);
        this.in  = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        hello();
    }

    @Override public void close() throws IOException {
        socket.close();
    }

    public String token() { return token; }

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

    public void login(String user, String pass) throws IOException {
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
     * @return number of rows the server confirmed it persisted.
     */
    public int writeBatch(String table, List<Column> cols, List<Row> rows) throws IOException {
        if (rows.isEmpty()) return 0;

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
                        case T_FLOAT64:   bits = Double.doubleToRawLongBits(r.f64.getOrDefault(ci, 0.0)); break;
                        default: throw new IOException("unsupported column type " + c.type);
                    }
                    writeU64LE(dout, bits);
                }
            }
        }

        send(MSG_WRITE_BATCH, (short) 0, out.toByteArray());
        Frame f = recv();
        if (f.type == MSG_ERROR) throw decodeError(f.payload);
        if (f.type != MSG_WRITE_ACK) throw new IOException("unexpected WRITE response type=" + f.type);
        if (f.payload.length < 4) throw new IOException("short WRITE_ACK");
        return ByteBuffer.wrap(f.payload, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
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

    public QueryResult query(String qtl) throws IOException {
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

    private static void parseRowsChunk(QueryResult qr, byte[] payload) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int nrows = b.getInt();
        int ncols = b.getShort() & 0xFFFF;
        if (ncols != qr.colNames.length) throw new IOException("col mismatch");
        int colLen = nrows * 8;
        byte[][] cols = new byte[ncols][];
        for (int ci = 0; ci < ncols; ci++) {
            cols[ci] = new byte[colLen];
            b.get(cols[ci]);
        }
        for (int ri = 0; ri < nrows; ri++) {
            Object[] row = new Object[ncols];
            for (int ci = 0; ci < ncols; ci++) {
                long raw = ByteBuffer.wrap(cols[ci], ri * 8, 8)
                        .order(ByteOrder.LITTLE_ENDIAN).getLong();
                switch (qr.colTypes[ci]) {
                    case T_TIMESTAMP: row[ci] = raw; break;
                    case T_INT64:     row[ci] = raw; break;
                    case T_FLOAT64:   row[ci] = Double.longBitsToDouble(raw); break;
                    case T_SYMBOL:    row[ci] = (int) raw; break;
                }
            }
            qr.rows.add(row);
        }
    }

    /* ----- helpers ----- */

    private static void putPascalStr(ByteBuffer buf, String s) {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        buf.put((byte) b.length).put(b);
    }

    private static IOException decodeError(byte[] payload) {
        if (payload.length < 4) return new IOException("server error");
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int rc = b.getInt();
        String msg = "";
        if (payload.length > 4) {
            msg = new String(payload, 4, payload.length - 4,
                    java.nio.charset.StandardCharsets.UTF_8);
        }
        return new IOException("server rc=" + rc + ": " + msg);
    }
}
