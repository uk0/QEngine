// tsdb-import — read a CSV file (with header row), batch-insert into a
// tsdb table.  The column names in the CSV header must match existing
// table columns; order doesn't matter.
//
//   tsdb-import -addr 127.0.0.1:28090 -table trades -file trades.csv
//
// Input CSV:
//   ts,price,qty
//   1700000000000000000,99.5,100
//   1700000000001000000,99.4,120
//
// The target table must exist.  Timestamps are ns-since-epoch int64.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/qengine/tsdb-go"
)

func main() {
	addr  := flag.String("addr", "127.0.0.1:28090", "tsdb-server address")
	table := flag.String("table", "", "target table name (required)")
	file  := flag.String("file", "-", "CSV input file, or '-' for stdin")
	user  := flag.String("user", "", "username (optional)")
	pass  := flag.String("password", "", "password")
	batch := flag.Int("batch", 4096, "rows per WRITE_BATCH")
	flag.Parse()

	if *table == "" {
		log.Fatal("missing -table")
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

	/* Describe the target table by running SELECT * LIMIT 0 — the HDR
	 * frame carries column names + types even when no rows match. */
	r0, err := c.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", *table))
	if err != nil {
		log.Fatalf("describe %s: %v", *table, err)
	}
	colIdx := make(map[string]int, len(r0.ColNames))
	colType := make(map[string]byte, len(r0.ColTypes))
	for i, n := range r0.ColNames {
		colIdx[n] = i
		colType[n] = r0.ColTypes[i]
	}

	var in *os.File
	if *file == "-" {
		in = os.Stdin
	} else {
		in, err = os.Open(*file)
		if err != nil {
			log.Fatalf("open input: %v", err)
		}
		defer in.Close()
	}

	cr := csv.NewReader(in)
	cr.ReuseRecord = true
	hdr, err := cr.Read()
	if err != nil {
		log.Fatalf("read header: %v", err)
	}

	/* Map CSV column positions to schema column names + types. */
	type csvCol struct { schemaCol int; typ byte; name string; csvPos int }
	var cols []csvCol
	for pos, name := range hdr {
		sc, ok := colIdx[name]
		if !ok {
			log.Fatalf("csv column %q not in table %s schema", name, *table)
		}
		cols = append(cols, csvCol{schemaCol: sc, typ: colType[name], name: name, csvPos: pos})
	}

	/* Build the proto column list in schema order. */
	tCols := make([]tsdb.Column, len(r0.ColNames))
	for i, n := range r0.ColNames {
		tCols[i] = tsdb.Column{Name: n, Type: r0.ColTypes[i]}
	}

	rows := make([]tsdb.Row, 0, *batch)
	total := int64(0)
	t0 := time.Now()
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("csv read: %v", err)
		}
		r := tsdb.Row{
			I64: make(map[int]int64, len(cols)),
			F64: make(map[int]float64, len(cols)),
		}
		for _, cc := range cols {
			raw := rec[cc.csvPos]
			switch cc.typ {
			case tsdb.TypeTimestamp:
				v, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					log.Fatalf("bad ts %q: %v", raw, err)
				}
				r.TS = v
			case tsdb.TypeInt64:
				v, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					log.Fatalf("bad int64 %q at col %s: %v", raw, cc.name, err)
				}
				r.I64[cc.schemaCol] = v
			case tsdb.TypeFloat64:
				v, err := strconv.ParseFloat(raw, 64)
				if err != nil {
					log.Fatalf("bad float64 %q at col %s: %v", raw, cc.name, err)
				}
				r.F64[cc.schemaCol] = v
			}
		}
		rows = append(rows, r)
		if len(rows) >= *batch {
			n, err := c.WriteBatch(*table, tCols, rows)
			if err != nil {
				log.Fatalf("write_batch: %v", err)
			}
			total += int64(n)
			rows = rows[:0]
		}
	}
	if len(rows) > 0 {
		n, err := c.WriteBatch(*table, tCols, rows)
		if err != nil {
			log.Fatalf("write_batch (tail): %v", err)
		}
		total += int64(n)
	}
	d := time.Since(t0).Seconds()
	fmt.Fprintf(os.Stderr, "tsdb-import: %d rows in %.3fs (%.0f rows/s)\n",
		total, d, float64(total)/d)
}
