# tsdb-go

Pure-Go client for QEngine (tsdb) wire protocol v1.

## Install

```bash
go get github.com/qengine/tsdb-go
```

Or in-tree (this repo): `import "github.com/qengine/tsdb-go"` via the
`sdk/go/go.mod`.

## Quick start

```go
import "github.com/qengine/tsdb-go"

c, _ := tsdb.Open("127.0.0.1:28090", 2*time.Second)
defer c.Close()

c.Login("admin", "secret")                    // if require_auth

c.CreateTable("trades", "ts", []tsdb.Column{
    {"ts",    tsdb.TypeTimestamp},
    {"price", tsdb.TypeFloat64},
    {"qty",   tsdb.TypeInt64},
})

rows := []tsdb.Row{
    {TS: 1_700_000_000_000_000_000,
     F64: map[int]float64{1: 99.5},
     I64: map[int]int64  {2: 100}},
}
n, _ := c.WriteBatch("trades",
    []tsdb.Column{
        {"ts",    tsdb.TypeTimestamp},
        {"price", tsdb.TypeFloat64},
        {"qty",   tsdb.TypeInt64},
    },
    rows)

r, _ := c.Query("SELECT count(*) FROM trades")
fmt.Println(r.Rows[0][0])   // int64
```

## Testing

End-to-end tests require a live server:

```bash
# Terminal 1
build/tsdb-server --bind 127.0.0.1:28190 --data-dir /tmp/tsdb_test

# Terminal 2
cd sdk/go
TSDB_TEST_ADDR=127.0.0.1:28190 go test -v ./...
```

## Status

- [x] HELLO + AUTH_LOGIN
- [x] CREATE TABLE / DROP TABLE
- [x] WRITE_BATCH (INT64/FLOAT64/TIMESTAMP — SYMBOL TODO)
- [x] QUERY with HDR + ROWS streaming
- [ ] SUBSCRIBE (next)
- [ ] TLS (next)
- [ ] Retry / connection pooling (v2)
