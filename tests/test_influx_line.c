/* test_influx_line.c — unit + integration tests for the InfluxDB Line Protocol
 *                       parser and HTTP ingest endpoint.
 *
 * Tests:
 *  1. Parse: basic line with single field
 *  2. Parse: multiple tags + multiple fields
 *  3. Parse: field type detection (float, int, bool, string)
 *  4. Parse: escape sequences in measurement/tag/field keys
 *  5. Parse: line without timestamp (uses server time)
 *  6. Parse: comment lines and blank lines are rejected
 *  7. Ingest: 3 cpu lines → table auto-created, count=3
 *  8. Ingest: duplicate measurement → idempotent table creation
 *  9. HTTP: POST /write with 3 lines → 204; then query count=3
 * 10. Ingest: a measurement holding ".." writes nothing outside the data dir
 * 11. Ingest: legacy-shaped measurement names still ingest after a restart
 * 12. Parse throughput benchmark (lines/sec)
 * 13. Ingest throughput benchmark (rows/sec)
 */

#include "../src/server/influx_line.h"
#include "../src/server/server.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ---- Test harness -------------------------------------------------------- */

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
        g_fail++; \
    } else { \
        printf("PASS: %s\n", msg); \
        g_pass++; \
    } \
} while(0)

#define ASSERT_OK(rc, msg) CHECK((rc) == TSDB_OK, msg)
#define ASSERT_EQ_STR(a, b, msg) CHECK(strcmp((a), (b)) == 0, msg)
#define ASSERT_EQ_INT(a, b, msg) CHECK((int64_t)(a) == (int64_t)(b), msg)
#define ASSERT_NEAR(a, b, msg)   CHECK(fabs((double)(a) - (double)(b)) < 1e-9, msg)

/* ---- Parse helpers ------------------------------------------------------- */

static void run_parse(const char *line,
                       int *rc_out,
                       char **meas_out,
                       char **tk, char **tv, int *nt,
                       char **fk,
                       tsdb_influx_val_t *fv,
                       tsdb_influx_ftype_t *ft,
                       char **fs,
                       int *nf,
                       tsdb_ts_t *ts_out,
                       char *scratch, size_t scap) {
    *rc_out = tsdb_influx_line_parse(line, strlen(line),
                                      scratch, scap,
                                      meas_out,
                                      tk, tv, nt,
                                      fk, fv, ft, fs,
                                      nf, ts_out);
}

/* ---- Tests --------------------------------------------------------------- */

static void test_parse_basic(void) {
    printf("\n[Test 1] Parse: basic line\n");
    char scratch[4096];
    char *meas;
    char *tk[64], *tv[64]; int nt = 0;
    char *fk[64]; tsdb_influx_val_t fv[64]; tsdb_influx_ftype_t ft[64]; char *fs[64];
    int nf = 0;
    tsdb_ts_t ts = 0;
    int rc;

    const char *line = "cpu usage=0.5 1609459200000000000";
    run_parse(line, &rc, &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts, scratch, sizeof(scratch));
    ASSERT_OK(rc, "basic line parse ok");
    ASSERT_EQ_STR(meas, "cpu", "measurement = cpu");
    ASSERT_EQ_INT(nt, 0, "no tags");
    ASSERT_EQ_INT(nf, 1, "one field");
    ASSERT_EQ_STR(fk[0], "usage", "field key = usage");
    ASSERT_EQ_INT(ft[0], TSDB_INFLUX_FLOAT, "field type = FLOAT");
    ASSERT_NEAR(fv[0].f64, 0.5, "field value = 0.5");
    ASSERT_EQ_INT(ts, (int64_t)1609459200000000000LL, "timestamp correct");
}

static void test_parse_multi_tag_field(void) {
    printf("\n[Test 2] Parse: multiple tags + multiple fields\n");
    char scratch[4096];
    char *meas;
    char *tk[64], *tv[64]; int nt = 0;
    char *fk[64]; tsdb_influx_val_t fv[64]; tsdb_influx_ftype_t ft[64]; char *fs[64];
    int nf = 0;
    tsdb_ts_t ts = 0;
    int rc;

    const char *line = "weather,location=us-midwest,season=summer temperature=82,humidity=71 1465839830100400200";
    run_parse(line, &rc, &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts, scratch, sizeof(scratch));
    ASSERT_OK(rc, "multi tag/field parse ok");
    ASSERT_EQ_STR(meas, "weather", "measurement = weather");
    ASSERT_EQ_INT(nt, 2, "2 tags");
    ASSERT_EQ_STR(tk[0], "location", "tag[0] key = location");
    ASSERT_EQ_STR(tv[0], "us-midwest", "tag[0] val = us-midwest");
    ASSERT_EQ_STR(tk[1], "season", "tag[1] key = season");
    ASSERT_EQ_STR(tv[1], "summer", "tag[1] val = summer");
    ASSERT_EQ_INT(nf, 2, "2 fields");
    ASSERT_EQ_STR(fk[0], "temperature", "field[0] = temperature");
    ASSERT_NEAR(fv[0].f64, 82.0, "temperature = 82");
    ASSERT_NEAR(fv[1].f64, 71.0, "humidity = 71");
}

static void test_parse_field_types(void) {
    printf("\n[Test 3] Parse: field type detection\n");
    char scratch[4096];
    char *meas;
    char *tk[64], *tv[64]; int nt = 0;
    char *fk[64]; tsdb_influx_val_t fv[64]; tsdb_influx_ftype_t ft[64]; char *fs[64];
    int nf = 0;
    tsdb_ts_t ts = 0;
    int rc;

    /* int: 42i, bool: true, string: "hello", float: 3.14 */
    const char *line = "test a=42i,b=true,c=\"hello world\",d=3.14 1000";
    run_parse(line, &rc, &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts, scratch, sizeof(scratch));
    ASSERT_OK(rc, "type detection parse ok");
    ASSERT_EQ_INT(nf, 4, "4 fields");
    ASSERT_EQ_INT(ft[0], TSDB_INFLUX_INT,    "a = INT");
    ASSERT_EQ_INT(fv[0].i64, 42LL,           "a = 42");
    ASSERT_EQ_INT(ft[1], TSDB_INFLUX_BOOL,   "b = BOOL");
    ASSERT_EQ_INT(fv[1].i64, 1LL,            "b = 1 (true)");
    ASSERT_EQ_INT(ft[2], TSDB_INFLUX_STRING, "c = STRING");
    CHECK(fs[2] && strcmp(fs[2], "hello world") == 0, "c = \"hello world\"");
    ASSERT_EQ_INT(ft[3], TSDB_INFLUX_FLOAT,  "d = FLOAT");
    ASSERT_NEAR(fv[3].f64, 3.14, "d = 3.14");
}

static void test_parse_escape(void) {
    printf("\n[Test 4] Parse: escape sequences\n");
    char scratch[4096];
    char *meas;
    char *tk[64], *tv[64]; int nt = 0;
    char *fk[64]; tsdb_influx_val_t fv[64]; tsdb_influx_ftype_t ft[64]; char *fs[64];
    int nf = 0;
    tsdb_ts_t ts = 0;
    int rc;

    /* measurement with escaped comma; tag value with escaped equals */
    const char *line = "my\\,measurement,tag\\=key=val\\=ue field=1.0 2000";
    run_parse(line, &rc, &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts, scratch, sizeof(scratch));
    ASSERT_OK(rc, "escape parse ok");
    ASSERT_EQ_STR(meas, "my,measurement", "measurement with comma");
    ASSERT_EQ_INT(nt, 1, "1 tag");
    ASSERT_EQ_STR(tk[0], "tag=key", "tag key with equals");
    ASSERT_EQ_STR(tv[0], "val=ue",  "tag val with equals");
}

static void test_parse_no_timestamp(void) {
    printf("\n[Test 5] Parse: line without timestamp → server time\n");
    char scratch[4096];
    char *meas;
    char *tk[64], *tv[64]; int nt = 0;
    char *fk[64]; tsdb_influx_val_t fv[64]; tsdb_influx_ftype_t ft[64]; char *fs[64];
    int nf = 0;
    tsdb_ts_t ts = 0;
    int rc;

    const char *line = "cpu usage=0.3";
    tsdb_ts_t before = tsdb_now_ns();
    run_parse(line, &rc, &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts, scratch, sizeof(scratch));
    tsdb_ts_t after  = tsdb_now_ns();
    ASSERT_OK(rc, "no-ts parse ok");
    CHECK(ts >= before && ts <= after, "server time assigned for missing ts");
}

static void test_parse_comment_blank(void) {
    printf("\n[Test 6] Parse: comment/blank lines rejected\n");
    char scratch[4096];
    char *meas = NULL;
    char *tk[64], *tv[64]; int nt = 0;
    char *fk[64]; tsdb_influx_val_t fv[64]; tsdb_influx_ftype_t ft[64]; char *fs[64];
    int nf = 0;
    tsdb_ts_t ts = 0;
    int rc;

    rc = tsdb_influx_line_parse("# this is a comment", 20,
                                 scratch, sizeof(scratch),
                                 &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts);
    CHECK(rc != TSDB_OK, "comment line returns error");

    rc = tsdb_influx_line_parse("", 0,
                                 scratch, sizeof(scratch),
                                 &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts);
    CHECK(rc != TSDB_OK, "empty line returns error");
}

static void test_ingest_basic(void) {
    printf("\n[Test 7] Ingest: 3 cpu rows → auto-create + count=3\n");

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/tsdb_influx_test_%d", (int)getpid());

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(tmpdir, &db);
    ASSERT_OK(rc, "DB open");

    const char *body =
        "cpu,host=a usage=0.5 1000\n"
        "cpu,host=b usage=0.7 2000\n"
        "cpu,host=a usage=0.6 3000\n";

    size_t nlines = 0, nerrors = 0;
    rc = tsdb_influx_ingest(db, body, strlen(body), &nlines, &nerrors);
    ASSERT_OK(rc, "ingest returns OK");
    ASSERT_EQ_INT(nlines,  3, "3 lines processed");
    ASSERT_EQ_INT(nerrors, 0, "0 errors");

    /* Query count. */
    tsdb_result_t *res = NULL;
    rc = tsdb_query(db, "SELECT count(*) FROM cpu", &res);
    ASSERT_OK(rc, "SELECT count(*) ok");

    int rows = 0;
    int64_t count_val = -1;
    if (res) {
        while (tsdb_result_next(res) == 1) {
            rows++;
            count_val = tsdb_result_i64(res, 0);
        }
        tsdb_result_free(res);
    }
    ASSERT_EQ_INT(rows, 1, "count query returns 1 row");
    ASSERT_EQ_INT(count_val, 3, "count(*) = 3");

    tsdb_close(db);
}

static void test_ingest_idempotent(void) {
    printf("\n[Test 8] Ingest: duplicate measurement → idempotent\n");

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/tsdb_influx_test2_%d", (int)getpid());

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(tmpdir, &db);
    ASSERT_OK(rc, "DB open");

    const char *b1 = "mem,host=x used=1024i 100\n";
    const char *b2 = "mem,host=y used=2048i 200\n";

    size_t nl, ne;
    tsdb_influx_ingest(db, b1, strlen(b1), &nl, &ne);
    rc = tsdb_influx_ingest(db, b2, strlen(b2), &nl, &ne);
    ASSERT_OK(rc, "second ingest ok");
    ASSERT_EQ_INT(ne, 0, "no errors on second batch");

    tsdb_result_t *res = NULL;
    rc = tsdb_query(db, "SELECT count(*) FROM mem", &res);
    ASSERT_OK(rc, "count query ok");
    int64_t cnt = 0;
    if (res) {
        while (tsdb_result_next(res) == 1) cnt = tsdb_result_i64(res, 0);
        tsdb_result_free(res);
    }
    ASSERT_EQ_INT(cnt, 2, "count(*) = 2 after two separate ingests");

    tsdb_close(db);
}

/* ---- HTTP test ----------------------------------------------------------- */

static int http_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    for (int i = 0; i < 50; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) return fd;
        if (errno == ECONNREFUSED) { usleep(20000); continue; }
        break;
    }
    close(fd);
    return -1;
}

static void http_send_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t r = write(fd, buf + sent, len - sent);
        if (r <= 0) break;
        sent += (size_t)r;
    }
}

static int http_recv_status(int fd) {
    char buf[512];
    ssize_t r = read(fd, buf, sizeof(buf) - 1);
    if (r <= 0) return -1;
    buf[r] = '\0';
    /* Parse status code from "HTTP/1.1 XXX" */
    if (strncmp(buf, "HTTP/1.1 ", 9) != 0) return -1;
    return atoi(buf + 9);
}

static void test_http_write(void) {
    printf("\n[Test 9] HTTP: POST /write → 204 + count=3\n");

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/tsdb_influx_http_%d", (int)getpid());

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(tmpdir, &db);
    ASSERT_OK(rc, "DB open for HTTP test");

    /* Start influx HTTP on a random-ish port. */
    int http_port = 28091 + (int)(getpid() % 1000);
    char bind_addr[64];
    snprintf(bind_addr, sizeof(bind_addr), "127.0.0.1:%d", http_port);

    rc = tsdb_influx_http_start(bind_addr, db);
    ASSERT_OK(rc, "HTTP server start");

    usleep(50000); /* brief wait for accept thread. */

    /* Connect and send POST /write. */
    const char *lp_body =
        "cpu,host=a usage=0.5 1000\n"
        "cpu,host=b usage=0.7 2000\n"
        "cpu,host=a usage=0.6 3000\n";
    size_t body_len = strlen(lp_body);

    char req[2048];
    int req_len = snprintf(req, sizeof(req),
        "POST /write HTTP/1.1\r\n"
        "Host: 127.0.0.1:%d\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %zu\r\n"
        "\r\n"
        "%s",
        http_port, body_len, lp_body);

    int fd = http_connect(http_port);
    CHECK(fd >= 0, "HTTP connect ok");
    if (fd >= 0) {
        http_send_all(fd, req, (size_t)req_len);
        int status = http_recv_status(fd);
        ASSERT_EQ_INT(status, 204, "HTTP response 204 No Content");
        close(fd);
    }

    /* Small delay to allow ingest to complete. */
    usleep(20000);

    /* Query count via DB API. */
    tsdb_result_t *res = NULL;
    rc = tsdb_query(db, "SELECT count(*) FROM cpu", &res);
    ASSERT_OK(rc, "query after HTTP write ok");
    int64_t cnt = 0;
    if (res) {
        while (tsdb_result_next(res) == 1) cnt = tsdb_result_i64(res, 0);
        tsdb_result_free(res);
    }
    ASSERT_EQ_INT(cnt, 3, "count(*) = 3 after HTTP write");

    tsdb_influx_http_stop();
    tsdb_close(db);
}

/* ---- Table-name path containment ---------------------------------------- */

static void rmrf(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", dir);
    (void)system(cmd);
}

/*
 * The measurement of an unauthenticated line becomes a directory name under
 * the data dir, so a name carrying a path separator or ".." names a directory
 * OUTSIDE it.  Both halves matter: the benign row must still land inside, and
 * the traversal attempt must leave nothing behind anywhere.
 */
static void test_ingest_path_escape(void) {
    printf("\n[Test 10] Ingest: '..' in a measurement writes nothing outside the data dir\n");

    char base[256], data[320], outside[384], outside_schema[448];
    char stray_wal[448], inside_schema[448];
    snprintf(base, sizeof(base), "/tmp/tsdb_influx_esc_%d", (int)getpid());
    /* A previous run that reused this pid would leave its tree here and make
     * "nothing outside the data dir" fail for the wrong reason. */
    rmrf(base);

    snprintf(data,           sizeof(data),           "%s/data", base);
    snprintf(outside,        sizeof(outside),        "%s/pwn", base);        /* <data>/../pwn */
    snprintf(outside_schema, sizeof(outside_schema), "%s/schema.bin", outside);
    snprintf(stray_wal,      sizeof(stray_wal),      "%s/pwn.log", data);    /* <data>/wal/../pwn.log */
    snprintf(inside_schema,  sizeof(inside_schema),  "%s/ok/schema.bin", data);

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(data, &db);
    ASSERT_OK(rc, "DB open for traversal test");

    const char *body =
        "ok,host=a usage=0.5 1000\n"          /* control: must be ingested */
        "../pwn,host=a usage=0.5 2000\n";     /* escape:  must be rejected */

    size_t nlines = 0, nerrors = 0;
    rc = tsdb_influx_ingest(db, body, strlen(body), &nlines, &nerrors);
    ASSERT_OK(rc, "ingest returns OK");
    ASSERT_EQ_INT(nlines,  2, "2 lines parsed");
    ASSERT_EQ_INT(nerrors, 1, "traversal line rejected");

    struct stat st;
    CHECK(stat(inside_schema,  &st) == 0, "control table landed inside the data dir");
    CHECK(stat(outside,        &st) != 0, "no table directory outside the data dir");
    CHECK(stat(outside_schema, &st) != 0, "no schema.bin outside the data dir");
    CHECK(stat(stray_wal,      &st) != 0, "no WAL file for the rejected name");

    tsdb_close(db);
    rmrf(base);
}

/*
 * The containment rule is enforced at CREATE, and the table registry is
 * rebuilt lazily — so after a restart the FIRST write to a table that already
 * exists on disk goes through auto-create again.  A rule wider than the escape
 * needs therefore does not just refuse new names, it stops ingesting into
 * tables an earlier binary created.  These names cannot escape anything, so
 * they must survive that round trip.
 */
static void test_legacy_name_after_restart(void) {
    printf("\n[Test 11] Ingest: legacy measurement names still ingest after a restart\n");

    char base[256], spaced_schema[512], dotted_schema[512], utf8_schema[512];
    snprintf(base, sizeof(base), "/tmp/tsdb_influx_legacy_%d", (int)getpid());
    rmrf(base);
    snprintf(spaced_schema, sizeof(spaced_schema), "%s/cpu load/schema.bin", base);
    snprintf(dotted_schema, sizeof(dotted_schema), "%s/sys.cpu.usage/schema.bin", base);
    /* "\xe6\xb8\xa9\xe5\xba\xa6" — a two-character non-ASCII name in UTF-8. */
    snprintf(utf8_schema,   sizeof(utf8_schema),   "%s/\xe6\xb8\xa9\xe5\xba\xa6/schema.bin", base);

    /* A space (escaped on the wire), interior dots, and non-ASCII bytes — all
     * shapes a real Influx deployment produces. */
    const char *body1 =
        "cpu\\ load,host=a usage=0.5 1000\n"
        "sys.cpu.usage,host=a usage=0.5 1000\n"
        "\xe6\xb8\xa9\xe5\xba\xa6,host=a v=1 1000\n";
    const char *body2 =
        "cpu\\ load,host=a usage=0.6 2000\n"
        "sys.cpu.usage,host=a usage=0.6 2000\n"
        "\xe6\xb8\xa9\xe5\xba\xa6,host=a v=2 2000\n";

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(base, &db);
    ASSERT_OK(rc, "DB open for legacy-name test");

    size_t nlines = 0, nerrors = 0;
    rc = tsdb_influx_ingest(db, body1, strlen(body1), &nlines, &nerrors);
    ASSERT_OK(rc, "first ingest ok");
    ASSERT_EQ_INT(nerrors, 0, "legacy names accepted at first CREATE");
    tsdb_close(db);

    db = NULL;
    rc = tsdb_open(base, &db);
    ASSERT_OK(rc, "DB reopen (restart)");
    nlines = nerrors = 0;
    rc = tsdb_influx_ingest(db, body2, strlen(body2), &nlines, &nerrors);
    ASSERT_OK(rc, "second ingest ok");
    ASSERT_EQ_INT(nlines,  3, "3 lines parsed after restart");
    ASSERT_EQ_INT(nerrors, 0, "legacy names still ingest after restart");
    tsdb_close(db);

    struct stat st;
    CHECK(stat(spaced_schema, &st) == 0, "spaced name is a table dir in the data dir");
    CHECK(stat(dotted_schema, &st) == 0, "dotted name is a table dir in the data dir");
    CHECK(stat(utf8_schema,   &st) == 0, "non-ASCII name is a table dir in the data dir");
    rmrf(base);
}

/* ---- Throughput benchmarks ---------------------------------------------- */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void bench_parse(void) {
    printf("\n[Bench 12] Parse throughput\n");
    const char *line = "cpu,host=web01,region=us-east usage_user=42.5,usage_system=3.1i,ready=true 1609459200000000000";
    size_t linelen = strlen(line);

    char scratch[4096];
    char *meas;
    char *tk[64], *tv[64]; int nt = 0;
    char *fk[64]; tsdb_influx_val_t fv[64]; tsdb_influx_ftype_t ft[64]; char *fs[64];
    int nf = 0;
    tsdb_ts_t ts = 0;

    /* Warm-up. */
    for (int i = 0; i < 10000; i++) {
        tsdb_influx_line_parse(line, linelen, scratch, sizeof(scratch),
                                &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts);
    }

    const int N = 2000000;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        tsdb_influx_line_parse(line, linelen, scratch, sizeof(scratch),
                                &meas, tk, tv, &nt, fk, fv, ft, fs, &nf, &ts);
    }
    double elapsed = now_sec() - t0;
    double rate = (double)N / elapsed;
    printf("  %d lines in %.3fs → %.0f lines/sec (%.1f ns/line)\n",
           N, elapsed, rate, elapsed * 1e9 / N);
    CHECK(rate > 1e6, "parse throughput > 1M lines/sec");
}

static void bench_ingest(void) {
    printf("\n[Bench 13] Ingest throughput (rows/sec)\n");

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/tsdb_influx_bench_%d", (int)getpid());

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(tmpdir, &db);
    if (rc != TSDB_OK) {
        printf("  SKIP: DB open failed\n");
        return;
    }

    /* Build a body with N lines. */
    const int N = 50000;
    char *body = (char *)malloc((size_t)N * 80);
    if (!body) { tsdb_close(db); return; }

    size_t bpos = 0;
    for (int i = 0; i < N; i++) {
        bpos += (size_t)snprintf(body + bpos, 79,
            "bench,host=h%d value=%di %lld\n",
            i % 100, i, (long long)(i * 1000000LL));
    }

    double t0 = now_sec();
    size_t nlines = 0, nerrors = 0;
    tsdb_influx_ingest(db, body, bpos, &nlines, &nerrors);
    double elapsed = now_sec() - t0;

    free(body);
    tsdb_close(db);

    double rate = (double)N / elapsed;
    printf("  %d rows in %.3fs → %.0f rows/sec (errors=%zu)\n",
           N, elapsed, rate, nerrors);
    CHECK(rate > 10000, "ingest throughput > 10K rows/sec");
}

/* ---- main ---------------------------------------------------------------- */

int main(void) {
    test_parse_basic();
    test_parse_multi_tag_field();
    test_parse_field_types();
    test_parse_escape();
    test_parse_no_timestamp();
    test_parse_comment_blank();
    test_ingest_basic();
    test_ingest_idempotent();
    test_http_write();
    test_ingest_path_escape();
    test_legacy_name_after_restart();
    bench_parse();
    bench_ingest();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
