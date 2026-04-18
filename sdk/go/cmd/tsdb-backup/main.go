// tsdb-backup — dump one or more tables to a directory.
//
//   tsdb-backup -addr 127.0.0.1:28090 -out backup.d -tables trades,metrics
//
// Layout of backup.d:
//   manifest.json       — schema for every dumped table
//   <table>.csv         — one per table, with header row
//
// Round-trip with tsdb-restore:
//   tsdb-restore -addr 127.0.0.1:28090 -in backup.d
//
// When -tables is omitted the tool tries QTL "LIST TABLES".  If that
// statement isn't available (it isn't in this server yet), the tool
// aborts with an error message — pass -tables explicitly.
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/qengine/tsdb-go"
)

type manifestTable struct {
	Name    string   `json:"name"`
	TsCol   string   `json:"ts_col"`
	Cols    []column `json:"columns"`
	Rows    int      `json:"rows"`
}

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type manifest struct {
	CreatedAt time.Time       `json:"created_at"`
	Server    string          `json:"server"`
	Tables    []manifestTable `json:"tables"`
}

func typeName(t byte) string {
	switch t {
	case tsdb.TypeTimestamp: return "TIMESTAMP"
	case tsdb.TypeInt64:     return "INT64"
	case tsdb.TypeFloat64:   return "FLOAT64"
	case tsdb.TypeSymbol:    return "SYMBOL"
	}
	return "UNKNOWN"
}

func main() {
	addr   := flag.String("addr", "127.0.0.1:28090", "tsdb-server address")
	out    := flag.String("out", "backup.d", "output directory")
	list   := flag.String("tables", "", "comma-separated table names to dump")
	user   := flag.String("user", "", "username (optional)")
	pass   := flag.String("password", "", "password")
	flag.Parse()

	if *list == "" {
		log.Fatal("-tables is required (comma-separated names)")
	}

	c, err := tsdb.Open(*addr, 5*time.Second)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer c.Close()
	if *user != "" {
		if err := c.Login(*user, *pass); err != nil {
			log.Fatalf("login: %v", err)
		}
	}

	if err := os.MkdirAll(*out, 0755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}

	tables := strings.Split(*list, ",")
	m := manifest{CreatedAt: time.Now().UTC(), Server: *addr}

	t0 := time.Now()
	totalRows := 0
	for _, name := range tables {
		name = strings.TrimSpace(name)
		if name == "" { continue }

		hdr, err := c.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", name))
		if err != nil {
			log.Fatalf("describe %s: %v", name, err)
		}
		/* SELECT * puts the ts col at its schema position; use the first
		 * TIMESTAMP column as the designator. */
		tsCol := ""
		for i, t := range hdr.ColTypes {
			if t == tsdb.TypeTimestamp {
				tsCol = hdr.ColNames[i]
				break
			}
		}

		/* Dump rows. */
		r, err := c.Query("SELECT * FROM " + name)
		if err != nil {
			log.Fatalf("select %s: %v", name, err)
		}
		csvPath := filepath.Join(*out, name+".csv")
		f, err := os.Create(csvPath)
		if err != nil {
			log.Fatalf("create %s: %v", csvPath, err)
		}
		cw := csv.NewWriter(f)
		cw.Write(r.ColNames)
		for _, row := range r.Rows {
			rec := make([]string, len(row))
			for i, v := range row {
				rec[i] = fmtCell(v, r.ColTypes[i])
			}
			cw.Write(rec)
		}
		cw.Flush()
		f.Close()

		mc := make([]column, len(hdr.ColNames))
		for i := range hdr.ColNames {
			mc[i] = column{Name: hdr.ColNames[i], Type: typeName(hdr.ColTypes[i])}
		}
		m.Tables = append(m.Tables, manifestTable{
			Name: name, TsCol: tsCol, Cols: mc, Rows: len(r.Rows),
		})
		totalRows += len(r.Rows)
		fmt.Fprintf(os.Stderr, "tsdb-backup: %s → %d rows\n", name, len(r.Rows))
	}

	b, _ := json.MarshalIndent(m, "", "  ")
	manifestPath := filepath.Join(*out, "manifest.json")
	if err := os.WriteFile(manifestPath, append(b, '\n'), 0644); err != nil {
		log.Fatalf("write manifest: %v", err)
	}

	d := time.Since(t0).Seconds()
	fmt.Fprintf(os.Stderr, "tsdb-backup: %d tables, %d rows in %.3fs (-> %s)\n",
		len(m.Tables), totalRows, d, *out)
}

func fmtCell(v interface{}, t byte) string {
	if v == nil { return "" }
	switch t {
	case tsdb.TypeTimestamp, tsdb.TypeInt64:
		return strconv.FormatInt(v.(int64), 10)
	case tsdb.TypeFloat64:
		return strconv.FormatFloat(v.(float64), 'g', -1, 64)
	case tsdb.TypeSymbol:
		return strconv.FormatUint(uint64(v.(uint32)), 10)
	}
	return fmt.Sprintf("%v", v)
}
