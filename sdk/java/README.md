# tsdb-jdbc

Pure-Java client + JDBC driver for QEngine (tsdb) wire protocol v1.
Java 11+, JDK-only (no runtime dependencies).

## Build

```bash
cd sdk/java
# Maven (standard)
mvn package
# or plain javac (no dependencies either way)
mkdir -p target/classes
find src/main/java -name '*.java' > /tmp/srcs.txt
javac -d target/classes --release 11 @/tmp/srcs.txt
```

## Use via raw client

```java
import com.tsdb.client.TsdbClient;
import java.util.List;

try (TsdbClient c = new TsdbClient("127.0.0.1", 28090)) {
    c.login("admin", "secret");                    // if require_auth
    c.createTable("trades", "ts", List.of(
        new TsdbClient.Column("ts",    TsdbClient.T_TIMESTAMP),
        new TsdbClient.Column("price", TsdbClient.T_FLOAT64)));

    TsdbClient.QueryResult r = c.query("SELECT count(*) FROM trades");
    long n = (long) r.rows.get(0)[0];
}
```

## Use via JDBC

```java
Class.forName("com.tsdb.jdbc.TsdbDriver");  // registers with DriverManager
try (Connection c = DriverManager.getConnection(
         "jdbc:tsdb://127.0.0.1:28090?user=admin&password=secret");
     Statement st = c.createStatement();
     ResultSet r = st.executeQuery("SELECT count(*) FROM trades")) {
    while (r.next()) System.out.println(r.getLong(1));
}
```

## JDBC surface implemented

- [x] `DriverManager.getConnection("jdbc:tsdb://host:port[?user=U&password=P]")`
- [x] `Connection.createStatement() / prepareStatement(sql)`
- [x] `Statement.executeQuery / executeUpdate / execute`
- [x] `PreparedStatement.set{Int,Long,Double,String,Timestamp,...}` with
      client-side inlining of `?` placeholders (not server-side protocol binds)
- [x] `ResultSet.next / getLong / getDouble / getString / getTimestamp /
      getInt / getObject`
- [x] `ResultSetMetaData.getColumnCount / getColumnName / getColumnType`
- [ ] Scrollable / updatable cursors — read-only forward only
- [ ] Binary parameter protocol — current path substitutes string-literal

Every unsupported JDBC method throws `SQLFeatureNotSupportedException`
rather than silently no-op, so bugs surface fast.

## Notes

- Uses `java.util.zip.CRC32C` (JDK 9+) — dispatches to hardware CRC32C
  intrinsics on x86 (SSE4.2) and ARMv8 (`CRC32CD`), matching the C
  server's path.
- Auth tokens are opaque 32-char hex strings held on the `TsdbClient`
  instance.  `Connection.close()` ends them with the TCP connection.
- Timestamps: tsdb uses ns-since-epoch `long`.  `ResultSet.getTimestamp`
  converts to `java.sql.Timestamp` (ms precision) for JDBC clients; keep
  on `getLong` for exact ns round-trip.
