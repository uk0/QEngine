/* outbox.c — durable per-stream sequence allocator.  See outbox.h for why it
 * reserves in chunks and why it must publish `base`. */

#include "outbox.h"
#include "../../include/tsdb.h"
#include "../server/proto.h"   /* tsdb_crc32c */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define OB_MAGIC   0x3142584Fu   /* 'OXB1' */
#define OB_VERSION 1u
#define OB_HDR     16u
#define OB_MAX_STREAMS 64u

typedef struct {
    uint64_t stream;     /* 0 = free */
    uint64_t reserved;   /* durable: no seq above this has ever been handed out */
    uint64_t next;       /* in-memory: the next seq to hand out */
    uint64_t base;       /* the promise: lowest seq we may still send */
} ob_stream_t;

struct tsdb_outbox {
    char        path[4096];
    uint32_t    chunk;
    ob_stream_t s[OB_MAX_STREAMS];
};

static void put32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static void put64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }
static uint32_t get32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static uint64_t get64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }

/* Persist every stream's RESERVATION atomically and durably.  Only `reserved`
 * is written: `next` is in-memory progress within the current chunk, and the
 * whole point of the design is that losing it costs a skip, never a reuse. */
static int ob_flush(tsdb_outbox_t *ob) {
    size_t n = 0;
    for (size_t i = 0; i < OB_MAX_STREAMS; i++) if (ob->s[i].stream) n++;

    size_t body = OB_HDR + n * 16;
    uint8_t *buf = malloc(body + 4);
    if (!buf) return TSDB_ERR_NOMEM;
    put32(buf + 0, OB_MAGIC);
    put32(buf + 4, OB_VERSION);
    put32(buf + 8, (uint32_t)n);
    put32(buf + 12, 0);
    size_t off = OB_HDR;
    for (size_t i = 0; i < OB_MAX_STREAMS; i++) {
        if (!ob->s[i].stream) continue;
        put64(buf + off,     ob->s[i].stream);
        put64(buf + off + 8, ob->s[i].reserved);
        off += 16;
    }
    put32(buf + body, tsdb_crc32c(buf, body));

    char tmp[4200];
    snprintf(tmp, sizeof(tmp), "%s.tmp", ob->path);
    int rc = TSDB_ERR_IO;
    FILE *f = fopen(tmp, "wb");
    if (f) {
        if (fwrite(buf, 1, body + 4, f) == body + 4 &&
            fflush(f) == 0 && fsync(fileno(f)) == 0)
            rc = TSDB_OK;
        fclose(f);
    }
    free(buf);
    if (rc != TSDB_OK) { unlink(tmp); return rc; }
    if (rename(tmp, ob->path) != 0) { unlink(tmp); return TSDB_ERR_IO; }

    char dir[4200];
    snprintf(dir, sizeof(dir), "%s", ob->path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        int dfd = open(dir[0] ? dir : "/", O_RDONLY);
        if (dfd >= 0) { (void)fsync(dfd); close(dfd); }
    }
    return TSDB_OK;
}

int tsdb_outbox_open(const char *path, uint32_t chunk, tsdb_outbox_t **out) {
    if (!path || !out || chunk == 0) return TSDB_ERR_INVAL;
    *out = NULL;

    tsdb_outbox_t *ob = calloc(1, sizeof(*ob));
    if (!ob) return TSDB_ERR_NOMEM;
    snprintf(ob->path, sizeof(ob->path), "%s", path);
    ob->chunk = chunk;

    FILE *f = fopen(path, "rb");
    if (!f) { *out = ob; return TSDB_OK; }      /* fresh sender */

    int rc = TSDB_OK;
    uint8_t *buf = NULL;
    if (fseek(f, 0, SEEK_END) == 0) {
        long sz = ftell(f);
        rewind(f);
        if (sz >= (long)(OB_HDR + 4) && (buf = malloc((size_t)sz)) != NULL &&
            fread(buf, 1, (size_t)sz, f) == (size_t)sz) {
            size_t body = (size_t)sz - 4;
            uint32_t n = get32(buf + 8);
            if (get32(buf) == OB_MAGIC && get32(buf + 4) == OB_VERSION &&
                tsdb_crc32c(buf, body) == get32(buf + body) &&
                OB_HDR + (size_t)n * 16 == body && n <= OB_MAX_STREAMS) {
                for (uint32_t i = 0; i < n; i++) {
                    const uint8_t *r = buf + OB_HDR + (size_t)i * 16;
                    ob->s[i].stream   = get64(r);
                    ob->s[i].reserved = get64(r + 8);
                    /* Resume ABOVE the whole reservation.  The seqs between the
                     * last one actually sent and `reserved` are skipped — which
                     * is precisely what `base` exists to tell the receiver. */
                    ob->s[i].next     = ob->s[i].reserved + 1;
                    ob->s[i].base     = ob->s[i].reserved + 1;
                }
            } else {
                /* Fail CLOSED: a damaged file must not let us reuse seqs.  With
                 * no trustworthy reservation we cannot promise anything, so
                 * refuse to open rather than silently restart from 1. */
                rc = TSDB_ERR_CORRUPT;
            }
        } else {
            rc = TSDB_ERR_CORRUPT;
        }
    } else {
        rc = TSDB_ERR_IO;
    }
    free(buf);
    fclose(f);
    if (rc != TSDB_OK) { free(ob); return rc; }
    *out = ob;
    return TSDB_OK;
}

void tsdb_outbox_close(tsdb_outbox_t *ob) { free(ob); }

static ob_stream_t *ob_find(tsdb_outbox_t *ob, uint64_t stream) {
    for (size_t i = 0; i < OB_MAX_STREAMS; i++)
        if (ob->s[i].stream == stream) return &ob->s[i];
    for (size_t i = 0; i < OB_MAX_STREAMS; i++) {
        if (ob->s[i].stream == 0) {
            ob->s[i].stream   = stream;
            ob->s[i].reserved = 0;
            ob->s[i].next     = 1;
            ob->s[i].base     = 1;
            return &ob->s[i];
        }
    }
    return NULL;
}

int tsdb_outbox_next(tsdb_outbox_t *ob, uint64_t stream,
                     uint64_t *out_seq, uint64_t *out_base)
{
    if (!ob || stream == 0) return TSDB_ERR_INVAL;
    ob_stream_t *s = ob_find(ob, stream);
    if (!s) return TSDB_ERR_NOMEM;

    /* Reserve a fresh chunk BEFORE handing out anything beyond the durable
     * high-water.  A seq is only ever emitted once its reservation is on disk,
     * so a crash can lose progress but never re-issue a number. */
    if (s->next > s->reserved) {
        uint64_t old = s->reserved;
        s->reserved = s->next + ob->chunk - 1;
        int rc = ob_flush(ob);
        if (rc != TSDB_OK) { s->reserved = old; return rc; }
    }

    if (out_seq)  *out_seq  = s->next;
    if (out_base) *out_base = s->base;
    s->next++;
    return TSDB_OK;
}

uint64_t tsdb_outbox_reserved(const tsdb_outbox_t *ob, uint64_t stream) {
    if (!ob) return 0;
    for (size_t i = 0; i < OB_MAX_STREAMS; i++)
        if (ob->s[i].stream == stream) return ob->s[i].reserved;
    return 0;
}
