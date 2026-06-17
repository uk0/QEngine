/* tsdb_client.c — TCP client REPL for tsdb-server.
 *
 * Usage:
 *   tsdb-cli [--host HOST] [--port PORT] [--token TOKEN]
 *   Default: host=127.0.0.1, port=28090
 *
 * Commands (case-insensitive):
 *   SELECT / CREATE / DROP / LIST / WRITE / SUBSCRIBE / STATS / EXIT / QUIT / \q
 *
 * Build:
 *   clang -std=c11 -O2 -Icli -o build/tsdb-cli cli/tsdb_client.c cli/tsdb_wire.c
 *   Add -DHAVE_READLINE -lreadline if readline is available.
 */

/* _GNU_SOURCE for strcasestr; _POSIX_C_SOURCE 200809L for strncasecmp/gettimeofday */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE   700
/* _DARWIN_C_SOURCE: strict _POSIX_C_SOURCE hides macOS TCP_KEEPALIVE/INTVL/CNT. */
#define _DARWIN_C_SOURCE

#include "tsdb_wire.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <poll.h>
#include <sys/time.h>

#ifdef HAVE_READLINE
#  include <readline/readline.h>
#  include <readline/history.h>
#endif

/* ─── Globals ─────────────────────────────────────────────────────────────── */

static volatile sig_atomic_t g_sigint   = 0;
static int                   g_sigpipe[2] = {-1, -1}; /* self-pipe for subscribe */

static void handle_sigint(int sig) {
    (void)sig;
    g_sigint = 1;
    if (g_sigpipe[1] >= 0) {
        char b = 1;
        (void)write(g_sigpipe[1], &b, 1);
    }
}

/* ─── Result display (columnar) ──────────────────────────────────────────── */

#define MAX_COLS   64
#define MAX_ROWS   200

typedef struct {
    char name[128];
    uint8_t type;
} col_info_t;

typedef struct {
    col_info_t cols[MAX_COLS];
    int        ncols;
    /* Accumulated row data: each row is ncols cells of char[64] */
    char     **cells;  /* cells[row * ncols + col] — malloc'd */
    int        nrows;
    int        cap;
    int        total;  /* including truncated rows */
} result_set_t;

static result_set_t *rs_new(void) {
    result_set_t *rs = calloc(1, sizeof(*rs));
    return rs;
}

static void rs_free(result_set_t *rs) {
    if (!rs) return;
    if (rs->cells) {
        for (int i = 0; i < rs->nrows * rs->ncols; i++) free(rs->cells[i]);
        free(rs->cells);
    }
    free(rs);
}

static void rs_add_row(result_set_t *rs, char **vals) {
    rs->total++;
    if (rs->nrows >= MAX_ROWS) return;
    if (rs->nrows >= rs->cap) {
        int newcap = rs->cap == 0 ? 64 : rs->cap * 2;
        char **p = realloc(rs->cells, sizeof(char*) * (size_t)(newcap * rs->ncols));
        if (!p) return;
        rs->cells = p;
        rs->cap = newcap;
    }
    int base = rs->nrows * rs->ncols;
    for (int i = 0; i < rs->ncols; i++)
        rs->cells[base + i] = vals[i] ? strdup(vals[i]) : strdup("NULL");
    rs->nrows++;
}

/* Print ASCII table */
static void rs_print(const result_set_t *rs, bool vertical) {
    if (vertical) {
        for (int r = 0; r < rs->nrows; r++) {
            printf("*** row %d ***\n", r + 1);
            for (int c = 0; c < rs->ncols; c++) {
                printf("  %-20s: %s\n",
                       rs->cols[c].name,
                       rs->cells[r * rs->ncols + c]);
            }
        }
        if (rs->total > rs->nrows)
            printf("... (%d more)\n", rs->total - rs->nrows);
        printf("(%d rows)\n", rs->total);
        return;
    }

    /* Compute column widths */
    int widths[MAX_COLS];
    for (int c = 0; c < rs->ncols; c++)
        widths[c] = (int)strlen(rs->cols[c].name);
    for (int r = 0; r < rs->nrows; r++)
        for (int c = 0; c < rs->ncols; c++) {
            int w = (int)strlen(rs->cells[r * rs->ncols + c]);
            if (w > widths[c]) widths[c] = w;
        }

    /* Header */
    for (int c = 0; c < rs->ncols; c++) {
        if (c) printf(" | ");
        printf("%-*s", widths[c], rs->cols[c].name);
    }
    printf("\n");

    /* Separator */
    for (int c = 0; c < rs->ncols; c++) {
        if (c) printf("-+-");
        for (int j = 0; j < widths[c]; j++) printf("-");
    }
    printf("\n");

    /* Rows */
    for (int r = 0; r < rs->nrows; r++) {
        for (int c = 0; c < rs->ncols; c++) {
            if (c) printf(" | ");
            printf("%-*s", widths[c], rs->cells[r * rs->ncols + c]);
        }
        printf("\n");
    }
    if (rs->total > rs->nrows)
        printf("... (%d more)\n", rs->total - rs->nrows);
    printf("(%d rows)\n", rs->total);
}

/* ─── Columnar decoder ────────────────────────────────────────────────────── */

/*
 * Decode a QUERY_RESULT_HDR payload and fill rs->cols / rs->ncols.
 * Returns 0 on success.
 */
static int decode_query_hdr(result_set_t *rs,
                             const uint8_t *p, uint32_t plen) {
    if (plen < 2) return -1;
    uint16_t ncols = get_u16(p);
    if (ncols > MAX_COLS) ncols = MAX_COLS;
    const uint8_t *end = p + plen;
    p += 2;
    for (int c = 0; c < (int)ncols; c++) {
        if (p >= end) return -1;
        uint8_t nlen = *p++;
        if (p + nlen + 1 > end) return -1;
        memcpy(rs->cols[c].name, p, nlen);
        rs->cols[c].name[nlen] = '\0';
        p += nlen;
        rs->cols[c].type = *p++;
    }
    rs->ncols = (int)ncols;
    return 0;
}

/* Render one 8-byte raw value into out[] according to the column type.
 * Column types come from the QUERY_RESULT_HDR (rs->cols[c].type). */
static void render_query_cell(char *out, size_t outcap,
                              uint8_t type, uint64_t raw) {
    switch (type) {
    case TSDB_TYPE_TIMESTAMP: {
        time_t secs = (time_t)((int64_t)raw / 1000000000LL);
        long  nsec  = (long)((int64_t)raw % 1000000000LL);
        struct tm tm;
        gmtime_r(&secs, &tm);
        snprintf(out, outcap, "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, nsec / 1000000);
        break;
    }
    case TSDB_TYPE_INT64:
        snprintf(out, outcap, "%lld", (long long)(int64_t)raw);
        break;
    case TSDB_TYPE_FLOAT64: {
        double d; memcpy(&d, &raw, 8);
        snprintf(out, outcap, "%.6g", d);
        break;
    }
    default:
        snprintf(out, outcap, "%llu", (unsigned long long)raw);
    }
}

/*
 * Decode a QUERY_RESULT_ROWS chunk produced by the server's
 * emit_query_chunk().  Column types are taken from rs->cols[] (set earlier
 * by decode_query_hdr); the chunk itself carries no per-column metadata.
 *
 * Wire layout (matches src/server/server.c:emit_query_chunk):
 *   [nrows u32][ncols u16]
 *   per column c (by rs->cols[c].type):
 *     SYMBOL: [blocklen u32] then nrows × ([len u16][bytes])
 *     other : nrows × 8 bytes (little-endian, 8-byte stride)
 *
 * Appends decoded rows to rs.  Returns 0 on success, -1 on a malformed frame.
 */
static int decode_query_rows(result_set_t *rs,
                             const uint8_t *p, uint32_t plen) {
    const uint8_t *end = p + plen;
    if (p + 6 > end) return -1;

    uint32_t nrows = get_u32(p); p += 4;
    uint16_t ncols = get_u16(p); p += 2;
    if (nrows == 0) return 0;
    if (ncols == 0 || ncols > MAX_COLS) return -1;
    /* The chunk's ncols must agree with the header we already parsed. */
    if (rs->ncols > 0 && (int)ncols != rs->ncols) return -1;

    /* Per-column cursor into the chunk body. */
    struct qcol { uint8_t type; const uint8_t *num; const uint8_t *sym; const uint8_t *symend; };
    struct qcol cols[MAX_COLS];

    for (uint16_t c = 0; c < ncols; c++) {
        cols[c].type = (uint8_t)rs->cols[c].type;
        if (cols[c].type == TSDB_TYPE_SYMBOL) {
            if (p + 4 > end) return -1;
            uint32_t bl = get_u32(p); p += 4;
            if (p + bl > end) return -1;
            cols[c].sym    = p;
            cols[c].symend = p + bl;
            cols[c].num    = NULL;
            p += bl;
        } else {
            size_t need = (size_t)nrows * 8;
            if (p + need > end) return -1;
            cols[c].num    = p;
            cols[c].sym    = NULL;
            cols[c].symend = NULL;
            p += need;
        }
    }

    /* Row-major emit.  Each cell gets its own buffer so symbol strings of
     * varying length survive until rs_add_row copies them. */
    char  cellbuf[MAX_COLS][512];
    char *vals[MAX_COLS];
    for (uint32_t r = 0; r < nrows; r++) {
        for (uint16_t c = 0; c < ncols; c++) {
            if (cols[c].type == TSDB_TYPE_SYMBOL) {
                /* Walk the length-prefixed block to the r-th string. */
                const uint8_t *sp = cols[c].sym;
                int ok = 1;
                for (uint32_t k = 0; k < r; k++) {
                    if (sp + 2 > cols[c].symend) { ok = 0; break; }
                    uint16_t l = get_u16(sp); sp += 2 + l;
                }
                if (!ok || sp + 2 > cols[c].symend) {
                    snprintf(cellbuf[c], sizeof(cellbuf[c]), "?");
                } else {
                    uint16_t l = get_u16(sp); sp += 2;
                    if (sp + l > cols[c].symend) l = (uint16_t)(cols[c].symend - sp);
                    size_t cp = l < sizeof(cellbuf[c]) - 1 ? l : sizeof(cellbuf[c]) - 1;
                    memcpy(cellbuf[c], sp, cp);
                    cellbuf[c][cp] = '\0';
                }
            } else {
                uint64_t raw = get_u64(cols[c].num + (size_t)r * 8);
                render_query_cell(cellbuf[c], sizeof(cellbuf[c]), cols[c].type, raw);
            }
            vals[c] = cellbuf[c];
        }
        rs_add_row(rs, vals);
    }
    return 0;
}

/*
 * Decode a QUERY_RESULT_ROWS / WRITE_BATCH / SUB_EVENT columnar payload.
 * Appends rows to rs.
 *
 * Columnar payload (when used for result rows):
 *   [table_name_len u8][table_name]
 *   [ncols u16][nrows u32]
 *   for each col:
 *     [name_len u8][name][type u8][codec u8]
 *     [compressed_size u32][compressed bytes — codec=RAW means 8 bytes/row]
 *
 * For simplicity the RAW codec is assumed (8 bytes per value, LE).
 * Gorilla/DoD decoding is left as a production extension.
 */
static int decode_columnar_rows(result_set_t *rs,
                                const uint8_t *p, uint32_t plen) {
    const uint8_t *end = p + plen;
    if (p >= end) return -1;

    /* table name */
    uint8_t tlen = *p++;
    if (p + tlen > end) return -1;
    p += tlen;

    if (p + 6 > end) return -1;
    uint16_t ncols = get_u16(p); p += 2;
    uint32_t nrows = get_u32(p); p += 4;
    if (ncols == 0 || nrows == 0) return 0;
    if (ncols > MAX_COLS) return -1;

    /* Read per-column compressed bytes */
    /* We store columns: col_data[c] = array of nrows raw bytes (8 each) */
    struct colbuf { uint8_t type; const uint8_t *data; uint32_t csz; };
    struct colbuf cols[MAX_COLS];

    for (uint16_t c = 0; c < ncols; c++) {
        if (p >= end) return -1;
        uint8_t nl = *p++;
        if (p + nl + 2 + 4 > end) return -1;
        p += nl;               /* col name */
        cols[c].type = *p++;   /* col_type */
        uint8_t codec = *p++;  /* codec */
        (void)codec;           /* only RAW supported in this client */
        cols[c].csz  = get_u32(p); p += 4;
        cols[c].data = p;
        if (p + cols[c].csz > end) return -1;
        p += cols[c].csz;
    }

    /* Decode and add rows */
    char val_buf[64];
    char *vals[MAX_COLS];
    for (uint32_t r = 0; r < nrows; r++) {
        for (uint16_t c = 0; c < ncols; c++) {
            /* Each RAW value is 8 bytes; stride = 8 * nrows */
            const uint8_t *vp = cols[c].data + (size_t)r * 8;
            if (vp + 8 > cols[c].data + cols[c].csz) {
                val_buf[0] = '?'; val_buf[1] = '\0';
            } else {
                uint64_t raw = get_u64(vp);
                switch (cols[c].type) {
                case TSDB_TYPE_TIMESTAMP:
                    /* nanoseconds → "YYYY-MM-DD HH:MM:SS.nnn" */
                    {
                        time_t secs = (time_t)((int64_t)raw / 1000000000LL);
                        long  nsec  = (long)((int64_t)raw % 1000000000LL);
                        struct tm tm;
                        gmtime_r(&secs, &tm);
                        snprintf(val_buf, sizeof(val_buf),
                                 "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
                                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                                 tm.tm_hour, tm.tm_min, tm.tm_sec,
                                 nsec / 1000000);
                    }
                    break;
                case TSDB_TYPE_INT64:
                    snprintf(val_buf, sizeof(val_buf), "%lld", (long long)(int64_t)raw);
                    break;
                case TSDB_TYPE_FLOAT64:
                    {
                        double d;
                        memcpy(&d, &raw, 8);
                        snprintf(val_buf, sizeof(val_buf), "%.6g", d);
                    }
                    break;
                case TSDB_TYPE_SYMBOL:
                    snprintf(val_buf, sizeof(val_buf), "%llu", (unsigned long long)raw);
                    break;
                default:
                    snprintf(val_buf, sizeof(val_buf), "?%llu", (unsigned long long)raw);
                }
            }
            vals[c] = val_buf;
        }
        rs_add_row(rs, vals);
    }
    return 0;
}

/* ─── TCP connect ─────────────────────────────────────────────────────────── */

/* SO_KEEPALIVE + per-platform idle/intvl/cnt on the connected fd so a dropped
 * server (crash, severed link) is detected instead of leaving the CLI wedged on
 * a dead socket.  Gated on TSDB_TCP_KEEPALIVE (default on; "0" disables);
 * tunables TSDB_TCP_KEEPALIVE_IDLE_S / _INTVL_S / _CNT.  Per-option setsockopt
 * failures are non-fatal. */
static void set_tcp_keepalive(int fd) {
    const char *en = getenv("TSDB_TCP_KEEPALIVE");
    if (en && strcmp(en, "0") == 0) return;

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) != 0) return;

    const char *ei = getenv("TSDB_TCP_KEEPALIVE_IDLE_S");
    const char *ev = getenv("TSDB_TCP_KEEPALIVE_INTVL_S");
    const char *ec = getenv("TSDB_TCP_KEEPALIVE_CNT");
    int idle  = (ei && *ei) ? atoi(ei) : 30;
    int intvl = (ev && *ev) ? atoi(ev) : 10;
    int cnt   = (ec && *ec) ? atoi(ec) : 3;

#ifdef __linux__
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE,  &idle,  sizeof(idle));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT,   &cnt,   sizeof(cnt));
#elif defined(__APPLE__)
#  ifdef TCP_KEEPALIVE
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &idle,  sizeof(idle));
#  endif
#  ifdef TCP_KEEPINTVL
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
#  endif
#  ifdef TCP_KEEPCNT
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT,   &cnt,   sizeof(cnt));
#  endif
#else
    (void)idle; (void)intvl; (void)cnt;
#endif
}

static int tcp_connect(const char *host, int port, int timeout_ms) {
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

    struct addrinfo hints = {0};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    int err = getaddrinfo(host, portstr, &hints, &res);
    if (err) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return -1;
    }

    int fd = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;

        /* Non-blocking connect with timeout */
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        int cr = connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (cr < 0 && errno != EINPROGRESS) {
            close(fd); fd = -1; continue;
        }
        if (cr != 0) {
            struct pollfd pfd = { .fd = fd, .events = POLLOUT };
            int pr = poll(&pfd, 1, timeout_ms);
            if (pr <= 0 || !(pfd.revents & POLLOUT)) {
                close(fd); fd = -1; continue;
            }
            int soerr = 0; socklen_t sl = sizeof(soerr);
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &sl);
            if (soerr) { close(fd); fd = -1; continue; }
        }
        /* Restore blocking */
        fcntl(fd, F_SETFL, flags);
        set_tcp_keepalive(fd);
        break;
    }
    freeaddrinfo(res);
    return fd;
}

/* ─── HELLO handshake ─────────────────────────────────────────────────────── */

static int do_hello(tsdb_conn_t *c, const char *token) {
    if (!token) token = "";
    size_t tlen = strlen(token);
    /* [ver u8][client_id_len u8][client_id: "tsdb-cli"][token_len u8][token] */
    const char *cid = "tsdb-cli";
    size_t cidlen = strlen(cid);

    uint8_t buf[512];
    uint8_t *p = buf;
    *p++ = TSDB_WIRE_VER;          /* ver */
    *p++ = (uint8_t)cidlen;
    memcpy(p, cid, cidlen); p += cidlen;
    *p++ = (uint8_t)(tlen > 255 ? 255 : tlen);
    if (tlen > 0) { memcpy(p, token, tlen > 255 ? 255 : tlen); p += (tlen > 255 ? 255 : tlen); }

    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_HELLO, 0, buf, (uint32_t)(p - buf), MSG_HELLO_OK, &resp);
    if (rc < 0) return -1;

    /* Parse HELLO_OK: [server_ver_len u8][server_ver utf8] */
    char server_ver[256] = "unknown";
    if (resp.hdr.payload_len >= 1) {
        uint8_t svl = resp.payload[0];
        if (svl > 0 && svl + 1 <= resp.hdr.payload_len) {
            memcpy(server_ver, resp.payload + 1, svl);
            server_ver[svl] = '\0';
        }
    }
    printf("connected to %s at %s:%d\n", server_ver, c->host, c->port);
    msg_free(&resp);
    return 0;
}

/* ─── QUERY ───────────────────────────────────────────────────────────────── */

/* Re-establish a dropped plaintext connection to the same endpoint with the
 * same credentials: close the dead fd, re-dial with backoff, re-run HELLO.
 * Returns 0 on success, -1 if every attempt failed.  TLS sessions are not
 * auto-reconnected (no client-side TLS teardown is wired). */
static int conn_reconnect(tsdb_conn_t *c) {
    if (c->tls) return -1;                  /* TLS reconnect unsupported */
    if (c->fd >= 0) { close(c->fd); c->fd = -1; }

    static const int backoff_ms[] = { 200, 500, 1000 };
    for (int i = 0; i < 3; i++) {
        struct timespec ts = { .tv_sec  = backoff_ms[i] / 1000,
                               .tv_nsec = (long)(backoff_ms[i] % 1000) * 1000000L };
        nanosleep(&ts, NULL);
        fprintf(stderr, "[reconnecting to %s:%d (%d/3)]\n", c->host, c->port, i + 1);
        int fd = tcp_connect(c->host, c->port, 5000);
        if (fd < 0) continue;
        c->fd = fd;
        if (do_hello(c, c->token) == 0) {
            fprintf(stderr, "[reconnected]\n");
            return 0;
        }
        close(c->fd); c->fd = -1;           /* HELLO failed; retry */
    }
    return -1;
}

/* One query attempt.  Returns 0 on success, -1 on a server/protocol error (do
 * NOT reconnect), or -2 when the transport died (send error or read EOF/reset)
 * — the caller may reconnect and retry, a SELECT being idempotent. */
static int do_query_once(tsdb_conn_t *c, const char *qtl, bool vertical) {
    size_t qlen = strlen(qtl);
    if (qlen > 65535) { fprintf(stderr, "query too long\n"); return -1; }

    uint8_t buf[65537 + 2];
    put_u16(buf, (uint16_t)qlen);
    memcpy(buf + 2, qtl, qlen);

    /* Send QUERY, expect QUERY_RESULT_HDR */
    uint64_t req_id = c->next_req_id++;
    if (frame_send(c, MSG_QUERY, 0, req_id, buf, (uint32_t)(qlen + 2)) < 0) {
        return -2;                          /* peer gone before/while sending */
    }

    result_set_t *rs = rs_new();
    int got_hdr = 0;

    for (;;) {
        tsdb_msg_t msg = {0};
        int rc = frame_recv(c, &msg);
        if (rc < 0) {
            rs_free(rs);
            if (rc == -1) return -2;        /* EOF / reset / timeout */
            fprintf(stderr, "recv error: %s\n",
                    rc == -2 ? "CRC mismatch" : "bad magic");
            return -1;
        }

        if (msg.hdr.type == MSG_ERROR) {
            const uint8_t *ep = msg.payload;
            uint32_t em = msg.hdr.payload_len;
            if (em >= 6) {
                int32_t ec = (int32_t)get_u32(ep);
                uint16_t ml = get_u16(ep + 4);
                fprintf(stderr, "error %d: %.*s\n", ec, (int)(ml < em - 6 ? ml : em - 6),
                        (const char*)(ep + 6));
            }
            msg_free(&msg); rs_free(rs); return -1;
        }

        if (msg.hdr.type == MSG_QUERY_RESULT_HDR) {
            got_hdr = 1;
            decode_query_hdr(rs, msg.payload, msg.hdr.payload_len);
        } else if (msg.hdr.type == MSG_QUERY_RESULT_ROWS) {
            if (got_hdr)
                decode_query_rows(rs, msg.payload, msg.hdr.payload_len);
        }

        bool fin = (msg.hdr.flags & TSDB_FLAG_FIN) != 0;
        msg_free(&msg);
        if (fin) break;
    }

    rs_print(rs, vertical);
    rs_free(rs);
    return 0;
}

/* Run a query, transparently re-dialing ONCE if the connection died mid-flight
 * (server restart, network blip).  A SELECT is idempotent, so a single retry
 * after a successful reconnect is safe. */
static int do_query(tsdb_conn_t *c, const char *qtl, bool vertical) {
    int rc = do_query_once(c, qtl, vertical);
    if (rc == -2 && c->auto_reconnect && !c->tls && conn_reconnect(c) == 0)
        rc = do_query_once(c, qtl, vertical);
    if (rc == -2)
        fprintf(stderr, "connection lost (%s:%d)\n", c->host, c->port);
    return rc == 0 ? 0 : -1;
}

/* ─── STATS ───────────────────────────────────────────────────────────────── */

static int do_stats(tsdb_conn_t *c) {
    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_CLUSTER_STATS, 0, NULL, 0, 0, &resp);
    if (rc < 0) return -1;

    /* Response payload: [kv_count u16] [k_len u8][k][v_len u8][v] ... */
    const uint8_t *p = resp.payload;
    const uint8_t *end = p + resp.hdr.payload_len;
    if (resp.hdr.payload_len >= 2) {
        uint16_t nkv = get_u16(p); p += 2;
        for (uint16_t i = 0; i < nkv && p < end; i++) {
            uint8_t kl = *p++;
            if (p + kl > end) break;
            char key[256]; memcpy(key, p, kl); key[kl] = '\0'; p += kl;
            if (p >= end) break;
            uint8_t vl = *p++;
            if (p + vl > end) break;
            char val[256]; memcpy(val, p, vl); val[vl] = '\0'; p += vl;
            printf("  %-24s: %s\n", key, val);
        }
    } else {
        /* Plain text fallback */
        printf("%.*s\n", (int)resp.hdr.payload_len, (const char*)resp.payload);
    }
    msg_free(&resp);
    return 0;
}

/* ─── CREATE/LIST/DROP GROUP ──────────────────────────────────────────────── */

/*
 * CREATE GROUP name (key='val', ...)
 * Payload: [name_len u8][name][nprops u8] [kl u8][k][vl u8][v] ...
 */
static int do_create_group(tsdb_conn_t *c, const char *line) {
    /* parse: CREATE GROUP <name> ( key='val', ... ) */
    const char *p = line;
    while (isspace((unsigned char)*p)) p++;
    /* skip "CREATE GROUP " */
    if (strncasecmp(p, "CREATE", 6) == 0) p += 6;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "GROUP", 5) == 0) p += 5;
    while (isspace((unsigned char)*p)) p++;

    /* name */
    const char *name_start = p;
    while (*p && !isspace((unsigned char)*p) && *p != '(') p++;
    size_t name_len = (size_t)(p - name_start);
    if (name_len == 0 || name_len > 255) {
        fprintf(stderr, "invalid group name\n"); return -1;
    }

    /* props: skip to '(' */
    while (*p && *p != '(') p++;
    /* Parse key=val pairs */
    uint8_t buf[4096];
    uint8_t *out = buf;
    *out++ = (uint8_t)name_len;
    memcpy(out, name_start, name_len); out += name_len;
    uint8_t *nprops_ptr = out;
    *out++ = 0; /* nprops placeholder */
    uint8_t nprops = 0;

    if (*p == '(') {
        p++;
        while (*p && *p != ')') {
            while (isspace((unsigned char)*p) || *p == ',') p++;
            if (*p == ')' || *p == '\0') break;
            /* key */
            const char *ks = p;
            while (*p && *p != '=' && !isspace((unsigned char)*p)) p++;
            size_t klen = (size_t)(p - ks);
            if (*p == '=') p++;
            /* value: optionally quoted */
            while (isspace((unsigned char)*p)) p++;
            char val[256]; size_t vlen = 0;
            if (*p == '\'') {
                p++;
                while (*p && *p != '\'') { if (vlen < 255) val[vlen++] = *p; p++; }
                if (*p == '\'') p++;
            } else {
                while (*p && *p != ',' && *p != ')') { if (vlen < 255) val[vlen++] = *p; p++; }
            }
            if (klen > 0 && klen <= 255) {
                *out++ = (uint8_t)klen; memcpy(out, ks, klen); out += klen;
                *out++ = (uint8_t)vlen; memcpy(out, val, vlen); out += vlen;
                nprops++;
            }
        }
    }
    *nprops_ptr = nprops;

    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_CREATE_GROUP, 0, buf, (uint32_t)(out - buf), 0, &resp);
    if (rc == 0) { printf("group '%.*s' created\n", (int)name_len, name_start); msg_free(&resp); }
    return rc;
}

static int do_list_groups(tsdb_conn_t *c) {
    /* empty glob = list all */
    uint8_t buf[1] = {0};
    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_LIST_GROUPS, 0, buf, 1, 0, &resp);
    if (rc == 0) {
        printf("%.*s\n", (int)resp.hdr.payload_len, (const char*)resp.payload);
        msg_free(&resp);
    }
    return rc;
}

static int do_drop_group(tsdb_conn_t *c, const char *name) {
    size_t nlen = strlen(name);
    uint8_t buf[256]; buf[0] = (uint8_t)(nlen > 255 ? 255 : nlen);
    memcpy(buf + 1, name, buf[0]);
    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_DROP_GROUP, 0, buf, 1 + buf[0], 0, &resp);
    if (rc == 0) { printf("group '%s' dropped\n", name); msg_free(&resp); }
    return rc;
}

/* ─── CREATE/LIST/DROP DEVICE ─────────────────────────────────────────────── */

/*
 * CREATE DEVICE <id> IN GROUP <group> (key='val', ...)
 * Payload: [group_len u8][group][id_len u8][id][ntags u8] [kl u8][k][vl u8][v] ...
 */
static int do_create_device(tsdb_conn_t *c, const char *line) {
    const char *p = line;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "CREATE", 6) == 0) p += 6;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "DEVICE", 6) == 0) p += 6;
    while (isspace((unsigned char)*p)) p++;

    const char *id_start = p;
    while (*p && !isspace((unsigned char)*p)) p++;
    size_t id_len = (size_t)(p - id_start);
    while (isspace((unsigned char)*p)) p++;

    if (strncasecmp(p, "IN", 2) == 0) p += 2;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "GROUP", 5) == 0) p += 5;
    while (isspace((unsigned char)*p)) p++;

    const char *grp_start = p;
    while (*p && !isspace((unsigned char)*p) && *p != '(') p++;
    size_t grp_len = (size_t)(p - grp_start);

    if (id_len == 0 || id_len > 255 || grp_len == 0 || grp_len > 255) {
        fprintf(stderr, "invalid device spec\n"); return -1;
    }

    uint8_t buf[4096];
    uint8_t *out = buf;
    *out++ = (uint8_t)grp_len; memcpy(out, grp_start, grp_len); out += grp_len;
    *out++ = (uint8_t)id_len;  memcpy(out, id_start,  id_len);  out += id_len;
    uint8_t *ntags_ptr = out; *out++ = 0;
    uint8_t ntags = 0;

    while (*p && *p != '(') p++;
    if (*p == '(') {
        p++;
        while (*p && *p != ')') {
            while (isspace((unsigned char)*p) || *p == ',') p++;
            if (*p == ')' || *p == '\0') break;
            const char *ks = p;
            while (*p && *p != '=' && !isspace((unsigned char)*p)) p++;
            size_t klen = (size_t)(p - ks);
            if (*p == '=') p++;
            while (isspace((unsigned char)*p)) p++;
            char val[256]; size_t vlen = 0;
            if (*p == '\'') {
                p++;
                while (*p && *p != '\'') { if (vlen < 255) val[vlen++] = *p; p++; }
                if (*p == '\'') p++;
            } else {
                while (*p && *p != ',' && *p != ')') { if (vlen < 255) val[vlen++] = *p; p++; }
            }
            if (klen > 0 && klen <= 255) {
                *out++ = (uint8_t)klen; memcpy(out, ks, klen); out += klen;
                *out++ = (uint8_t)vlen; memcpy(out, val, vlen); out += vlen;
                ntags++;
            }
        }
    }
    *ntags_ptr = ntags;

    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_CREATE_DEVICE, 0, buf, (uint32_t)(out - buf), 0, &resp);
    if (rc == 0) {
        printf("device '%.*s' created in group '%.*s'\n",
               (int)id_len, id_start, (int)grp_len, grp_start);
        msg_free(&resp);
    }
    return rc;
}

/*
 * LIST DEVICES IN GROUP <group>
 */
/* Case-insensitive substring search (POSIX-safe alternative to strcasestr) */
static const char *strcistr(const char *hay, const char *needle) {
    size_t nl = strlen(needle);
    for (; *hay; hay++)
        if (strncasecmp(hay, needle, nl) == 0) return hay;
    return NULL;
}

static int do_list_devices(tsdb_conn_t *c, const char *line) {
    const char *p = line;
    /* skip to "GROUP" */
    const char *gp = strcistr(p, "GROUP");
    if (gp) { gp += 5; while (isspace((unsigned char)*gp)) gp++; }
    else gp = "";
    size_t glen = strlen(gp);
    /* trim trailing whitespace */
    while (glen > 0 && isspace((unsigned char)gp[glen-1])) glen--;

    uint8_t buf[256]; buf[0] = (uint8_t)(glen > 255 ? 255 : glen);
    memcpy(buf + 1, gp, buf[0]);

    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_LIST_DEVICES, 0, buf, 1 + buf[0], 0, &resp);
    if (rc == 0) {
        printf("%.*s\n", (int)resp.hdr.payload_len, (const char*)resp.payload);
        msg_free(&resp);
    }
    return rc;
}

/* ─── WRITE TO <table> FROM FILE/STDIN ────────────────────────────────────── */

/*
 * CSV ingestion:
 *  1. Read header line → column names
 *  2. WRITE_STREAM_OPEN to table
 *  3. Accumulate 10000 rows at a time → send WRITE_STREAM_DATA (RAW codec)
 *  4. WRITE_STREAM_END + print summary
 *
 * Column types: all treated as FLOAT64 unless name is "ts"/"time"/"timestamp"
 * (treated as TIMESTAMP). Symbols are not auto-detected from CSV.
 */

#define BATCH_ROWS 10000

static int do_write_stream(tsdb_conn_t *c, const char *table, FILE *fp) {
    char line_buf[65536];

    /* Read header */
    if (!fgets(line_buf, sizeof(line_buf), fp)) {
        fprintf(stderr, "empty CSV\n"); return -1;
    }
    /* Strip newline */
    size_t ln = strlen(line_buf);
    while (ln > 0 && (line_buf[ln-1] == '\n' || line_buf[ln-1] == '\r')) line_buf[--ln] = 0;

    /* Parse column names */
    char col_names[MAX_COLS][128];
    uint8_t col_types[MAX_COLS];
    int ncols = 0;
    {
        char *tok = strtok(line_buf, ",");
        while (tok && ncols < MAX_COLS) {
            /* trim whitespace */
            while (isspace((unsigned char)*tok)) tok++;
            char *end = tok + strlen(tok) - 1;
            while (end > tok && isspace((unsigned char)*end)) *end-- = 0;
            strncpy(col_names[ncols], tok, 127);
            col_names[ncols][127] = '\0';
            /* heuristic type */
            if (strcasecmp(tok, "ts") == 0 ||
                strcasecmp(tok, "time") == 0 ||
                strcasecmp(tok, "timestamp") == 0)
                col_types[ncols] = TSDB_TYPE_TIMESTAMP;
            else
                col_types[ncols] = TSDB_TYPE_FLOAT64;
            ncols++;
            tok = strtok(NULL, ",");
        }
    }
    if (ncols == 0) { fprintf(stderr, "no columns in CSV header\n"); return -1; }

    /* WRITE_STREAM_OPEN */
    size_t tlen = strlen(table);
    {
        uint8_t obuf[256]; obuf[0] = (uint8_t)(tlen > 255 ? 255 : tlen);
        memcpy(obuf + 1, table, obuf[0]);
        uint64_t req_id = c->next_req_id++;
        if (frame_send(c, MSG_WRITE_STREAM_OPEN, 0, req_id, obuf, 1 + obuf[0]) < 0) {
            perror("write stream open"); return -1;
        }
        /* No ack expected for OPEN in this protocol; server buffers until DATA */
    }

    /* Accumulate and send batches */
    double  *col_buf = calloc((size_t)(ncols * BATCH_ROWS), sizeof(double));
    if (!col_buf) return -1;

    struct timeval t0, t1;
    gettimeofday(&t0, NULL);

    int64_t total_rows = 0;
    int64_t batch_rows = 0;

    while (fgets(line_buf, sizeof(line_buf), fp)) {
        ln = strlen(line_buf);
        while (ln > 0 && (line_buf[ln-1] == '\n' || line_buf[ln-1] == '\r')) line_buf[--ln] = 0;
        if (ln == 0) continue;

        /* Parse CSV row */
        char *tok = strtok(line_buf, ",");
        for (int c2 = 0; c2 < ncols; c2++) {
            double val = tok ? atof(tok) : 0.0;
            col_buf[(size_t)c2 * BATCH_ROWS + (size_t)batch_rows] = val;
            if (tok) tok = strtok(NULL, ",");
        }
        batch_rows++;
        total_rows++;

        if (batch_rows >= BATCH_ROWS) {
            /* Build and send WRITE_STREAM_DATA payload */
            size_t csz_per_col = (size_t)batch_rows * 8;
            size_t cap = 1 + tlen + 2 + 4 +
                         (size_t)ncols * (1 + 127 + 1 + 1 + 4 + csz_per_col);
            uint8_t *dbuf = malloc(cap);
            if (!dbuf) { free(col_buf); return -1; }
            uint8_t *dp = dbuf;
            *dp++ = (uint8_t)(tlen > 255 ? 255 : tlen);
            memcpy(dp, table, *dbuf); dp += *dbuf;
            put_u16(dp, (uint16_t)ncols); dp += 2;
            put_u32(dp, (uint32_t)batch_rows); dp += 4;
            for (int ci = 0; ci < ncols; ci++) {
                uint8_t clen = (uint8_t)strlen(col_names[ci]);
                *dp++ = clen; memcpy(dp, col_names[ci], clen); dp += clen;
                *dp++ = col_types[ci];
                *dp++ = TSDB_CODEC_RAW;
                put_u32(dp, (uint32_t)csz_per_col); dp += 4;
                for (int r = 0; r < batch_rows; r++) {
                    uint64_t raw;
                    double dv = col_buf[(size_t)ci * BATCH_ROWS + (size_t)r];
                    memcpy(&raw, &dv, 8);
                    put_u64(dp, raw); dp += 8;
                }
            }
            uint64_t req_id = c->next_req_id++;
            frame_send(c, MSG_WRITE_STREAM_DATA, 0, req_id, dbuf, (uint32_t)(dp - dbuf));
            free(dbuf);
            /* batch sent */
            batch_rows = 0;
        }
    }

    /* Flush remaining rows */
    if (batch_rows > 0) {
        size_t csz_per_col = (size_t)batch_rows * 8;
        size_t cap = 1 + tlen + 2 + 4 +
                     (size_t)ncols * (1 + 128 + 1 + 1 + 4 + csz_per_col);
        uint8_t *dbuf = malloc(cap);
        if (dbuf) {
            uint8_t *dp = dbuf;
            uint8_t tl = (uint8_t)(tlen > 255 ? 255 : tlen);
            *dp++ = tl; memcpy(dp, table, tl); dp += tl;
            put_u16(dp, (uint16_t)ncols); dp += 2;
            put_u32(dp, (uint32_t)batch_rows); dp += 4;
            for (int ci = 0; ci < ncols; ci++) {
                uint8_t clen = (uint8_t)strlen(col_names[ci]);
                *dp++ = clen; memcpy(dp, col_names[ci], clen); dp += clen;
                *dp++ = col_types[ci];
                *dp++ = TSDB_CODEC_RAW;
                put_u32(dp, (uint32_t)csz_per_col); dp += 4;
                for (int r = 0; r < batch_rows; r++) {
                    uint64_t raw;
                    double dv = col_buf[(size_t)ci * BATCH_ROWS + (size_t)r];
                    memcpy(&raw, &dv, 8);
                    put_u64(dp, raw); dp += 8;
                }
            }
            uint64_t req_id = c->next_req_id++;
            frame_send(c, MSG_WRITE_STREAM_DATA, 0, req_id, dbuf, (uint32_t)(dp - dbuf));
            free(dbuf);
            /* batch sent */
        }
    }
    free(col_buf);

    /* WRITE_STREAM_END */
    {
        uint64_t req_id = c->next_req_id++;
        tsdb_msg_t resp = {0};
        /* Re-use frame_send + frame_recv pattern */
        if (frame_send(c, MSG_WRITE_STREAM_END, TSDB_FLAG_FIN, req_id, NULL, 0) == 0) {
            /* Wait for WRITE_ACK */
            int rc2 = frame_recv(c, &resp);
            if (rc2 == 0 && resp.hdr.type == MSG_WRITE_ACK) {
                uint64_t rows_acc = 0, last_seq = 0;
                if (resp.hdr.payload_len >= 16) {
                    rows_acc = get_u64(resp.payload);
                    last_seq = get_u64(resp.payload + 8);
                }
                gettimeofday(&t1, NULL);
                double elapsed = (double)(t1.tv_sec - t0.tv_sec) +
                                 (double)(t1.tv_usec - t0.tv_usec) / 1e6;
                double rate = elapsed > 0 ? (double)total_rows / elapsed / 1e6 : 0;
                printf("wrote %lld rows in %.2fs (%.2f M rows/sec), "
                       "server accepted %llu (last_seq=%llu)\n",
                       (long long)total_rows, elapsed, rate,
                       (unsigned long long)rows_acc,
                       (unsigned long long)last_seq);
                msg_free(&resp);
            } else if (rc2 == 0) {
                gettimeofday(&t1, NULL);
                double elapsed = (double)(t1.tv_sec - t0.tv_sec) +
                                 (double)(t1.tv_usec - t0.tv_usec) / 1e6;
                printf("wrote %lld rows in %.2fs\n", (long long)total_rows, elapsed);
                msg_free(&resp);
            }
        }
    }
    return 0;
}

static int do_write(tsdb_conn_t *c, const char *line) {
    /* WRITE TO <table> FROM FILE <path> | FROM STDIN */
    const char *p = line;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "WRITE", 5) == 0) p += 5;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "TO", 2) == 0) p += 2;
    while (isspace((unsigned char)*p)) p++;

    const char *table_start = p;
    while (*p && !isspace((unsigned char)*p)) p++;
    char table[256];
    size_t tlen = (size_t)(p - table_start);
    if (tlen == 0 || tlen > 255) { fprintf(stderr, "invalid table name\n"); return -1; }
    memcpy(table, table_start, tlen); table[tlen] = '\0';

    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "FROM", 4) == 0) p += 4;
    while (isspace((unsigned char)*p)) p++;

    FILE *fp;
    if (strncasecmp(p, "STDIN", 5) == 0) {
        fp = stdin;
    } else {
        if (strncasecmp(p, "FILE", 4) == 0) p += 4;
        while (isspace((unsigned char)*p)) p++;
        /* trim trailing whitespace/semicolon */
        char path[1024];
        strncpy(path, p, sizeof(path) - 1);
        path[sizeof(path) - 1] = '\0';
        size_t pl = strlen(path);
        while (pl > 0 && (isspace((unsigned char)path[pl-1]) || path[pl-1] == ';')) path[--pl] = 0;
        fp = fopen(path, "r");
        if (!fp) { perror(path); return -1; }
    }

    int rc = do_write_stream(c, table, fp);
    if (fp != stdin) fclose(fp);
    return rc;
}

/* ─── SUBSCRIBE ───────────────────────────────────────────────────────────── */

static int do_subscribe(tsdb_conn_t *c, const char *line) {
    const char *p = line;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "SUBSCRIBE", 9) == 0) p += 9;
    while (isspace((unsigned char)*p)) p++;

    char table[256]; size_t tlen = 0;
    while (*p && !isspace((unsigned char)*p) && *p != ';') {
        if (tlen < 255) table[tlen++] = *p;
        p++;
    }
    table[tlen] = '\0';
    if (tlen == 0) { fprintf(stderr, "SUBSCRIBE requires table name\n"); return -1; }

    /* Build SUBSCRIBE payload: [table_len u8][table][filter_len u16=0] */
    uint8_t buf[260];
    buf[0] = (uint8_t)tlen;
    memcpy(buf + 1, table, tlen);
    put_u16(buf + 1 + tlen, 0); /* no filter */

    uint64_t sub_req_id = c->next_req_id++;
    if (frame_send(c, MSG_SUBSCRIBE, 0, sub_req_id, buf, (uint32_t)(1 + tlen + 2)) < 0) {
        perror("subscribe"); return -1;
    }

    printf("Subscribing to '%s' (Ctrl-C to stop)...\n", table);

    /* Setup self-pipe for Ctrl-C */
    if (pipe(g_sigpipe) < 0) { perror("pipe"); return -1; }
    g_sigint = 0;

    struct sigaction sa = {0};
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);

    /* Event loop */
    for (;;) {
        struct pollfd pfds[2] = {
            { .fd = c->fd,        .events = POLLIN },
            { .fd = g_sigpipe[0], .events = POLLIN },
        };
        int pr = poll(pfds, 2, -1);
        if (pr < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (pfds[1].revents & POLLIN || g_sigint) {
            /* Ctrl-C: send UNSUBSCRIBE */
            printf("\nUnsubscribing...\n");
            uint8_t ubuf[8]; put_u64(ubuf, sub_req_id);
            frame_send(c, MSG_UNSUBSCRIBE, 0, c->next_req_id++, ubuf, 8);
            break;
        }
        if (pfds[0].revents & POLLIN) {
            tsdb_msg_t msg = {0};
            if (frame_recv(c, &msg) < 0) break;
            if (msg.hdr.type == MSG_SUB_EVENT) {
                /* Decode and print single row inline */
                result_set_t *rs = rs_new();
                /* SUB_EVENT has no schema embedded - use previously known schema */
                /* For now: use decode_columnar_rows which extracts col names */
                rs->ncols = 0; /* will be filled from payload */
                decode_columnar_rows(rs, msg.payload, msg.hdr.payload_len);
                for (int r = 0; r < rs->nrows; r++) {
                    for (int c2 = 0; c2 < rs->ncols; c2++) {
                        if (c2) printf(", ");
                        printf("%s", rs->cells[r * rs->ncols + c2]);
                    }
                    printf("\n");
                }
                rs_free(rs);
            }
            msg_free(&msg);
        }
    }

    /* Cleanup self-pipe */
    close(g_sigpipe[0]); close(g_sigpipe[1]);
    g_sigpipe[0] = g_sigpipe[1] = -1;

    /* Restore default SIGINT */
    struct sigaction def = {0};
    def.sa_handler = SIG_DFL;
    sigaction(SIGINT, &def, NULL);
    g_sigint = 0;

    return 0;
}

/* ─── Command dispatch ────────────────────────────────────────────────────── */

/* Session-level context — tsdb-cli is a pure-client REPL, so USE is a
 * purely local construct.  The server stays stateless; we just show
 * the current scope in the prompt and auto-inject IN DATABASE / IN
 * GROUP clauses into LIST VTABLES / LIST PTABLES so users don't have
 * to type them every time.  SET/UNSET via:
 *
 *   USE DATABASE <name>   — scope subsequent LISTs to that db
 *   USE VTABLE   <name>   — shorthand for LIST PTABLES USING <name>
 *   USE PTABLE   <name>   — marker (no auto-inject yet)
 *   USE NONE              — clear everything
 */
static char g_cur_db[64]     = "";
static char g_cur_vtable[64] = "";
static char g_cur_ptable[64] = "";

/* Snapshot of the composed prompt; regenerated whenever context
 * changes so REPL can display "tsdb [iot > meters] > " instead of
 * the bare "tsdb> ". */
static char g_prompt[160] = "tsdb> ";

static void rebuild_prompt(void) {
    if (!g_cur_db[0] && !g_cur_vtable[0] && !g_cur_ptable[0]) {
        snprintf(g_prompt, sizeof(g_prompt), "tsdb> ");
        return;
    }
    char inner[128];
    size_t w = 0;
    if (g_cur_db[0])     w += (size_t)snprintf(inner + w, sizeof(inner) - w,
                                                "%s%s", w ? " > " : "", g_cur_db);
    if (g_cur_vtable[0]) w += (size_t)snprintf(inner + w, sizeof(inner) - w,
                                                "%s%s", w ? " > " : "", g_cur_vtable);
    if (g_cur_ptable[0]) w += (size_t)snprintf(inner + w, sizeof(inner) - w,
                                                "%s%s", w ? " > " : "", g_cur_ptable);
    snprintf(g_prompt, sizeof(g_prompt), "tsdb [%s]> ", inner);
}

/* Match "USE KIND name" case-insensitively; strips trailing whitespace
 * and a terminating ';'.  Returns 1 if matched + sets *out. */
static int parse_use(const char *cmd, const char *kind, char *out, size_t cap) {
    /* Require leading "USE" + space, then kind, then a name. */
    const char *p = cmd;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "USE", 3) != 0) return 0;
    p += 3;
    if (!isspace((unsigned char)*p)) return 0;
    while (isspace((unsigned char)*p)) p++;

    size_t kl = strlen(kind);
    if (strncasecmp(p, kind, kl) != 0) return 0;
    p += kl;
    if (!isspace((unsigned char)*p)) return 0;
    while (isspace((unsigned char)*p)) p++;

    size_t w = 0;
    while (*p && !isspace((unsigned char)*p) && *p != ';' && w + 1 < cap) {
        out[w++] = *p++;
    }
    out[w] = '\0';
    return out[0] ? 1 : 0;
}

/* Rewrite a LIST VTABLES / LIST PTABLES query to carry IN DATABASE
 * <g_cur_db> automatically when a DB context is active AND the user
 * didn't already specify one.  Returns a copy (caller frees). */
static char *maybe_inject_list_scope(const char *cmd) {
    if (!g_cur_db[0]) return NULL;
    /* Cheap keyword sniff — anything case-insensitively starting with
     * LIST VTABLES / LIST STABLES / LIST PTABLES / LIST TABLES. */
    const char *p = cmd;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "LIST", 4) != 0) return NULL;
    p += 4;
    while (isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "VTABLES",  7) != 0 &&
        strncasecmp(p, "STABLES",  7) != 0 &&
        strncasecmp(p, "PTABLES",  7) != 0 &&
        strncasecmp(p, "TABLES",   6) != 0) return NULL;

    /* If the user already wrote IN DATABASE anywhere, don't override. */
    const char *scan = cmd;
    while (*scan) {
        if (strncasecmp(scan, "IN DATABASE", 11) == 0 ||
            strncasecmp(scan, "IN DB", 5) == 0) return NULL;
        scan++;
    }

    size_t cap = strlen(cmd) + 64;
    char *out = malloc(cap);
    if (!out) return NULL;
    snprintf(out, cap, "%s IN DATABASE %s", cmd, g_cur_db);
    return out;
}

static bool do_command(tsdb_conn_t *c, const char *cmd) {
    /* Trim leading whitespace */
    while (isspace((unsigned char)*cmd)) cmd++;
    if (*cmd == '\0' || *cmd == '#') return true; /* empty / comment */

    /* Client-side USE context — intercepted before anything hits the
     * wire so a stateless HTTP-style server stays stateless. */
    char name_buf[64];
    if (parse_use(cmd, "DATABASE", name_buf, sizeof(name_buf)) ||
        parse_use(cmd, "DB",       name_buf, sizeof(name_buf))) {
        snprintf(g_cur_db, sizeof(g_cur_db), "%s", name_buf);
        g_cur_vtable[0] = '\0';
        g_cur_ptable[0] = '\0';
        rebuild_prompt();
        printf("context: database = %s\n", g_cur_db);
        return true;
    }
    if (parse_use(cmd, "VTABLE", name_buf, sizeof(name_buf)) ||
        parse_use(cmd, "STABLE", name_buf, sizeof(name_buf))) {
        snprintf(g_cur_vtable, sizeof(g_cur_vtable), "%s", name_buf);
        g_cur_ptable[0] = '\0';
        rebuild_prompt();
        printf("context: vtable = %s\n", g_cur_vtable);
        return true;
    }
    if (parse_use(cmd, "PTABLE", name_buf, sizeof(name_buf)) ||
        parse_use(cmd, "TABLE",  name_buf, sizeof(name_buf))) {
        snprintf(g_cur_ptable, sizeof(g_cur_ptable), "%s", name_buf);
        rebuild_prompt();
        printf("context: ptable = %s\n", g_cur_ptable);
        return true;
    }
    {
        const char *p = cmd;
        while (isspace((unsigned char)*p)) p++;
        if (strncasecmp(p, "USE NONE", 8) == 0 ||
            strncasecmp(p, "USE CLEAR", 9) == 0) {
            g_cur_db[0] = g_cur_vtable[0] = g_cur_ptable[0] = '\0';
            rebuild_prompt();
            printf("context: cleared\n");
            return true;
        }
    }

    /* Check for \G vertical mode suffix */
    bool vertical = false;
    char buf[65536];
    strncpy(buf, cmd, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    size_t blen = strlen(buf);
    /* Trim trailing semicolons and whitespace */
    while (blen > 0 && (buf[blen-1] == ';' || isspace((unsigned char)buf[blen-1])))
        buf[--blen] = 0;
    if (blen >= 2 && buf[blen-1] == 'G' && buf[blen-2] == '\\') {
        vertical = true;
        buf[blen - 2] = '\0'; blen -= 2;
        while (blen > 0 && isspace((unsigned char)buf[blen-1])) buf[--blen] = 0;
    }

    const char *line = buf;

    if (strcasecmp(line, "EXIT") == 0 || strcasecmp(line, "QUIT") == 0 ||
        strcmp(line, "\\q") == 0) {
        return false;
    }
    if (strcasecmp(line, "STATS") == 0) {
        do_stats(c);
    } else if (strcasecmp(line, "LIST GROUPS") == 0) {
        do_list_groups(c);
    } else if (strncasecmp(line, "LIST DEVICES", 12) == 0) {
        do_list_devices(c, line);
    } else if (strncasecmp(line, "CREATE GROUP", 12) == 0) {
        do_create_group(c, line);
    } else if (strncasecmp(line, "CREATE DEVICE", 13) == 0) {
        do_create_device(c, line);
    } else if (strncasecmp(line, "DROP GROUP", 10) == 0) {
        const char *p = line + 10;
        while (isspace((unsigned char)*p)) p++;
        do_drop_group(c, p);
    } else if (strncasecmp(line, "WRITE", 5) == 0) {
        do_write(c, line);
    } else if (strncasecmp(line, "SUBSCRIBE", 9) == 0) {
        do_subscribe(c, line);
    } else {
        /* Default: treat as QTL query.  If USE DATABASE is active and
         * the statement is a LIST [V|P]TABLES, inject IN DATABASE so
         * the catalog filter kicks in server-side. */
        char *rewritten = maybe_inject_list_scope(line);
        do_query(c, rewritten ? rewritten : line, vertical);
        free(rewritten);
    }
    return true;
}

/* ─── REPL ────────────────────────────────────────────────────────────────── */

static void repl(tsdb_conn_t *c) {
#ifdef HAVE_READLINE
    printf("(readline)\n");
    using_history();
#endif

    rebuild_prompt();
    for (;;) {
#ifdef HAVE_READLINE
        char *input = readline(g_prompt);
        if (!input) break;
        if (*input) add_history(input);
        bool cont = do_command(c, input);
        free(input);
        if (!cont) break;
#else
        fputs(g_prompt, stdout); fflush(stdout);
        char line[65536];
        if (!fgets(line, sizeof(line), stdin)) break;
        size_t n = strlen(line);
        while (n > 0 && (line[n-1] == '\n' || line[n-1] == '\r')) line[--n] = 0;
        if (!do_command(c, line)) break;
#endif
    }
}

/* ─── main ────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    const char *host         = "127.0.0.1";
    int         port         = 28090;
    const char *token        = NULL;
    int         use_tls      = 0;
    const char *tls_ca       = NULL;
    int         tls_insecure = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--host") == 0 && i + 1 < argc)
            host = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc)
            port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--token") == 0 && i + 1 < argc)
            token = argv[++i];
        else if (strcmp(argv[i], "--tls") == 0)
            use_tls = 1;
        else if (strcmp(argv[i], "--tls-ca") == 0 && i + 1 < argc) {
            tls_ca = argv[++i];
            use_tls = 1;  /* --tls-ca implies --tls */
        }
        else if (strcmp(argv[i], "--tls-insecure") == 0) {
            use_tls = 1;
            tls_insecure = 1;
        }
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("usage: tsdb-cli [options]\n"
                   "  --host HOST          server hostname (default 127.0.0.1)\n"
                   "  --port PORT          server port    (default 28090)\n"
                   "  --token TOKEN        auth token\n"
                   "  --tls                enable TLS\n"
                   "  --tls-ca FILE        PEM CA bundle for server cert verification\n"
                   "  --tls-insecure       disable certificate verification (insecure)\n");
            return 0;
        } else {
            fprintf(stderr, "unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    tsdb_conn_t conn = {0};

    if (use_tls) {
        if (tsdb_conn_connect_tls(host, port, tls_ca, tls_insecure, &conn) < 0) {
            fprintf(stderr, "TLS connect to %s:%d failed\n", host, port);
            return 1;
        }
        printf("[tls] connected to %s:%d (TLS)\n", host, port);
    } else {
        int fd = tcp_connect(host, port, 5000);
        if (fd < 0) {
            fprintf(stderr, "cannot connect to %s:%d\n", host, port);
            return 1;
        }
        conn.fd = fd;
        conn.next_req_id = 1;
        conn.port = port;
        conn.timeout_ms = 10000;
        strncpy(conn.host, host, sizeof(conn.host) - 1);
    }

    /* Retain reconnect parameters; auto-reconnect plaintext sessions only
     * (no client-side TLS teardown is wired for a clean TLS re-dial). */
    conn.token          = token;
    conn.use_tls        = use_tls;
    conn.tls_ca         = tls_ca;
    conn.tls_insecure   = tls_insecure;
    conn.auto_reconnect = use_tls ? 0 : 1;

    if (do_hello(&conn, token) < 0) {
        if (conn.tls) { /* tsdb_tls_close equivalent for client */ }
        else close(conn.fd);
        return 1;
    }

    repl(&conn);
    if (!conn.tls) close(conn.fd);
    /* TLS conn cleanup: SSL_shutdown happens when the fd is closed. */
    return 0;
}
