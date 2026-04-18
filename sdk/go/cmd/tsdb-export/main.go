// tsdb-export — run a QTL query, write the result as CSV.
//
//   tsdb-export -addr 127.0.0.1:28090 \
//       -qtl "SELECT ts, price FROM trades WHERE qty > 100" \
//       -out out.csv
//
// Defaults: stdout if -out is unset; ns-epoch ints for timestamps; no header
// flag to omit column names.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/qengine/tsdb-go"
)

func main() {
	addr   := flag.String("addr", "127.0.0.1:28090", "tsdb-server address")
	qtl    := flag.String("qtl", "", "QTL query to run (required)")
	out    := flag.String("out", "-", "output file path, or '-' for stdout")
	user   := flag.String("user", "", "username (optional, if require_auth)")
	pass   := flag.String("password", "", "password")
	noHdr  := flag.Bool("no-header", false, "omit CSV header row")
	tsRFC  := flag.Bool("ts-rfc3339", false, "format timestamps as RFC3339 instead of ns-epoch")
	flag.Parse()

	if *qtl == "" {
		log.Fatal("missing -qtl")
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

	r, err := c.Query(*qtl)
	if err != nil {
		log.Fatalf("query: %v", err)
	}

	var w *os.File
	if *out == "-" {
		w = os.Stdout
	} else {
		w, err = os.Create(*out)
		if err != nil {
			log.Fatalf("open out: %v", err)
		}
		defer w.Close()
	}
	cw := csv.NewWriter(w)
	defer cw.Flush()

	if !*noHdr {
		cw.Write(r.ColNames)
	}
	for _, row := range r.Rows {
		rec := make([]string, len(row))
		for i, v := range row {
			rec[i] = format(v, r.ColTypes[i], *tsRFC)
		}
		if err := cw.Write(rec); err != nil {
			log.Fatalf("csv write: %v", err)
		}
	}
	fmt.Fprintf(os.Stderr, "tsdb-export: %d rows\n", len(r.Rows))
}

func format(v interface{}, t byte, tsRFC bool) string {
	if v == nil {
		return ""
	}
	switch t {
	case tsdb.TypeTimestamp:
		if tsRFC {
			ns := v.(int64)
			return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
		}
		return strconv.FormatInt(v.(int64), 10)
	case tsdb.TypeInt64:
		return strconv.FormatInt(v.(int64), 10)
	case tsdb.TypeFloat64:
		return strconv.FormatFloat(v.(float64), 'g', -1, 64)
	case tsdb.TypeSymbol:
		return strconv.FormatUint(uint64(v.(uint32)), 10)
	}
	return fmt.Sprintf("%v", v)
}
