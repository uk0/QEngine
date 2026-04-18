// tsdb-restore — replay a tsdb-backup directory into a tsdb-server.
//
//   tsdb-restore -addr 127.0.0.1:28090 -in backup.d
//
// For every table in manifest.json, the tool recreates the table (CREATE
// IF NOT EXISTS-ish — ignores TSDB_ERR_EXISTS) and then loads the
// matching .csv via the same path tsdb-import uses.
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/qengine/tsdb-go"
)

type manifestTable struct {
	Name  string   `json:"name"`
	TsCol string   `json:"ts_col"`
	Cols  []column `json:"columns"`
	Rows  int      `json:"rows"`
}

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type manifest struct {
	Tables []manifestTable `json:"tables"`
}

func typeByte(s string) byte {
	switch strings.ToUpper(s) {
	case "TIMESTAMP": return tsdb.TypeTimestamp
	case "INT64":     return tsdb.TypeInt64
	case "FLOAT64":   return tsdb.TypeFloat64
	case "SYMBOL":    return tsdb.TypeSymbol
	}
	return 0
}

func main() {
	addr  := flag.String("addr", "127.0.0.1:28090", "tsdb-server address")
	in    := flag.String("in", "backup.d", "backup directory")
	batch := flag.Int("batch", 4096, "rows per WRITE_BATCH")
	user  := flag.String("user", "", "username (optional)")
	pass  := flag.String("password", "", "password")
	flag.Parse()

	raw, err := os.ReadFile(filepath.Join(*in, "manifest.json"))
	if err != nil {
		log.Fatalf("read manifest: %v", err)
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		log.Fatalf("parse manifest: %v", err)
	}

	c, err := tsdb.Open(*addr, 5*time.Second)
	if err != nil { log.Fatalf("open: %v", err) }
	defer c.Close()
	if *user != "" {
		if err := c.Login(*user, *pass); err != nil {
			log.Fatalf("login: %v", err)
		}
	}

	t0 := time.Now()
	totalRows := int64(0)
	for _, t := range m.Tables {
		/* Recreate table.  Ignore EXISTS. */
		cols := make([]tsdb.Column, len(t.Cols))
		for i, c := range t.Cols {
			cols[i] = tsdb.Column{Name: c.Name, Type: typeByte(c.Type)}
		}
		if err := c.CreateTable(t.Name, t.TsCol, cols); err != nil {
			if !strings.Contains(err.Error(), "table exists") {
				log.Fatalf("create %s: %v", t.Name, err)
			}
		}

		/* Stream rows from the CSV. */
		f, err := os.Open(filepath.Join(*in, t.Name+".csv"))
		if err != nil {
			log.Fatalf("open csv: %v", err)
		}
		cr := csv.NewReader(f)
		cr.ReuseRecord = true
		hdr, err := cr.Read()
		if err != nil { log.Fatalf("read csv header: %v", err) }

		colByName := make(map[string]int, len(cols))
		typByName := make(map[string]byte, len(cols))
		for i, c := range cols {
			colByName[c.Name] = i
			typByName[c.Name] = c.Type
		}
		type csvCol struct { schemaCol int; typ byte; name string; csvPos int }
		var csvCols []csvCol
		for pos, name := range hdr {
			sc, ok := colByName[name]
			if !ok {
				log.Fatalf("csv column %q not in table %s schema", name, t.Name)
			}
			csvCols = append(csvCols, csvCol{schemaCol: sc, typ: typByName[name], name: name, csvPos: pos})
		}

		rows := make([]tsdb.Row, 0, *batch)
		tableTotal := int64(0)
		for {
			rec, err := cr.Read()
			if err == io.EOF { break }
			if err != nil {
				log.Fatalf("csv read: %v", err)
			}
			r := tsdb.Row{
				I64: make(map[int]int64, len(cols)),
				F64: make(map[int]float64, len(cols)),
			}
			for _, cc := range csvCols {
				raw := rec[cc.csvPos]
				switch cc.typ {
				case tsdb.TypeTimestamp:
					v, err := strconv.ParseInt(raw, 10, 64)
					if err != nil { log.Fatalf("bad ts %q: %v", raw, err) }
					r.TS = v
				case tsdb.TypeInt64:
					v, _ := strconv.ParseInt(raw, 10, 64)
					r.I64[cc.schemaCol] = v
				case tsdb.TypeFloat64:
					v, _ := strconv.ParseFloat(raw, 64)
					r.F64[cc.schemaCol] = v
				}
			}
			rows = append(rows, r)
			if len(rows) >= *batch {
				n, err := c.WriteBatch(t.Name, cols, rows)
				if err != nil { log.Fatalf("write_batch %s: %v", t.Name, err) }
				tableTotal += int64(n)
				rows = rows[:0]
			}
		}
		if len(rows) > 0 {
			n, err := c.WriteBatch(t.Name, cols, rows)
			if err != nil { log.Fatalf("write_batch %s (tail): %v", t.Name, err) }
			tableTotal += int64(n)
		}
		f.Close()
		fmt.Fprintf(os.Stderr, "tsdb-restore: %s ← %d rows\n", t.Name, tableTotal)
		totalRows += tableTotal
	}

	d := time.Since(t0).Seconds()
	fmt.Fprintf(os.Stderr, "tsdb-restore: %d tables, %d rows in %.3fs\n",
		len(m.Tables), totalRows, d)
}
