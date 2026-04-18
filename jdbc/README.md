# QEngine JDBC Driver

JDBC 4.x driver for QEngine (the C-based time-series database in this repo),
implemented in pure Java on top of the QEngine TCP wire protocol v1.

## URL

```
jdbc:qengine://host:port/
```

The trailing `/` is optional. Example: `jdbc:qengine://127.0.0.1:9000/`.

## Example (7 lines)

```java
Class.forName("com.qengine.jdbc.QEngineDriver");
try (Connection c = DriverManager.getConnection("jdbc:qengine://127.0.0.1:9000/");
     Statement  s = c.createStatement();
     ResultSet  r = s.executeQuery("SELECT ts, price FROM trades WHERE sym = 'AAPL' LIMIT 10")) {
    while (r.next())
        System.out.printf("%s  %.2f%n", r.getTimestamp(1), r.getDouble(2));
}
```

## Connection properties

| Property     | Default          | Meaning                                 |
|--------------|------------------|-----------------------------------------|
| `client_id`  | `qengine-jdbc`   | sent in HELLO payload                   |
| `token`      | (empty)          | auth token sent in HELLO payload        |
| `timeout_ms` | `10000`          | socket connect + read timeout (ms)      |

## Supported wire types

| QEngine type | JDBC type      | Java class              |
|--------------|----------------|-------------------------|
| `TIMESTAMP`  | `Types.TIMESTAMP` | `java.sql.Timestamp` (ns precision) |
| `INT64`      | `Types.BIGINT` | `java.lang.Long`        |
| `FLOAT64`    | `Types.DOUBLE` | `java.lang.Double`      |
| `SYMBOL`     | `Types.VARCHAR` | `java.lang.String`*    |

\* **Note**: in wire protocol v1 the server emits `SYMBOL` columns as the
UTF-8 byte length of the symbol (not the string itself). `getString()` will
return the length as text. Resolve the real symbol table key out-of-band.

## Not supported

- Connection pooling / `DataSource`
- Transactions / `SAVEPOINT` / `rollback()`
- Batch updates
- `CallableStatement`
- Binary parameter binding in `PreparedStatement` (string replacement only)

## Build

```
mvn -q compile test-compile   # compile
mvn -q test                   # run tests (integration test skips if no server)
```
