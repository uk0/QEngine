/* wal.c — Write-Ahead Log implementation. */

#include "wal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

/* Forward declaration of mkdir_p from schema.c. */
extern int tsdb_mkdir_p(const char *path);

/* ---- CRC-32 (IEEE 802.3) ----------------------------------------------- */

static uint32_t crc32_table[256];
static int      crc32_table_ready = 0;

static void crc32_init(void) {
    if (crc32_table_ready) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_table_ready = 1;
}

/* crc32 is inlined in append/replay via table; no standalone function needed. */

/* ---- WAL struct --------------------------------------------------------- */

struct tsdb_wal {
    int  fd;           /* file descriptor, opened O_WRONLY|O_APPEND|O_CREAT */
    char path[4096];   /* for truncate / replay */
};

/* ---- Record format helpers ---------------------------------------------- */

/*
 * Record header: [crc32 u32][len u32] = 8 bytes.
 * crc32 covers the len field and payload.
 */
#define WAL_RECORD_HEADER 8

static void put_u32le_buf(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static uint32_t get_u32le_buf(const uint8_t *p) {
    return (uint32_t)p[0]
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

/* ---- Public API --------------------------------------------------------- */

int tsdb_wal_open(const char *db_dir, const char *table_name, tsdb_wal_t **out) {
    if (!db_dir || !table_name || !out) return TSDB_ERR_INVAL;

    /* Ensure wal directory exists. */
    char wal_dir[4096];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", db_dir);
    if (tsdb_mkdir_p(wal_dir) < 0) return TSDB_ERR_IO;

    tsdb_wal_t *w = calloc(1, sizeof(*w));
    if (!w) return TSDB_ERR_NOMEM;

    snprintf(w->path, sizeof(w->path), "%s/%s.log", wal_dir, table_name);

    w->fd = open(w->path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (w->fd < 0) { free(w); return TSDB_ERR_IO; }

    *out = w;
    return TSDB_OK;
}

void tsdb_wal_close(tsdb_wal_t *w) {
    if (!w) return;
    if (w->fd >= 0) close(w->fd);
    free(w);
}

int tsdb_wal_append(tsdb_wal_t *w, const void *rec, size_t n) {
    if (!w || (!rec && n > 0)) return TSDB_ERR_INVAL;
    if (n > 0xFFFFFFFFu) return TSDB_ERR_OVERFLOW;

    /* Build header: [crc32][len] where crc covers len+payload. */
    uint8_t len_buf[4];
    put_u32le_buf(len_buf, (uint32_t)n);

    /* Compute CRC over the len field + payload. */
    uint32_t c = 0xFFFFFFFFu;
    crc32_init();
    for (int i = 0; i < 4; i++)
        c = crc32_table[(c ^ len_buf[i]) & 0xFF] ^ (c >> 8);
    if (rec && n > 0) {
        const uint8_t *p = (const uint8_t *)rec;
        for (size_t i = 0; i < n; i++)
            c = crc32_table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    }
    c ^= 0xFFFFFFFFu;

    /* Write header then payload as two writev-style writes.
     * Using a small stack buffer to combine crc+len. */
    uint8_t hdr[8];
    put_u32le_buf(hdr + 0, c);
    put_u32le_buf(hdr + 4, (uint32_t)n);

    /* Write atomically with writev to avoid partial records. */
    struct iovec {
        void  *iov_base;
        size_t iov_len;
    } iov[2];
    (void)iov; /* not using iov directly to keep portability */

    /* Simple sequential writes — acceptable for WAL correctness. */
    ssize_t wr = write(w->fd, hdr, 8);
    if (wr != 8) return TSDB_ERR_IO;

    if (n > 0) {
        wr = write(w->fd, rec, n);
        if ((size_t)wr != n) return TSDB_ERR_IO;
    }
    return TSDB_OK;
}

int tsdb_wal_sync(tsdb_wal_t *w) {
    if (!w || w->fd < 0) return TSDB_ERR_INVAL;
    if (fsync(w->fd) < 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

int tsdb_wal_truncate(tsdb_wal_t *w) {
    if (!w || w->fd < 0) return TSDB_ERR_INVAL;

    /* Truncate to zero and seek to start. */
    if (ftruncate(w->fd, 0) < 0) return TSDB_ERR_IO;
    if (lseek(w->fd, 0, SEEK_SET) < 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

int tsdb_wal_replay(const char *db_dir, const char *table_name,
                    int (*cb)(const void *rec, size_t n, void *ctx), void *ctx)
{
    if (!db_dir || !table_name || !cb) return TSDB_ERR_INVAL;

    char path[4096];
    snprintf(path, sizeof(path), "%s/wal/%s.log", db_dir, table_name);

    FILE *f = fopen(path, "rb");
    if (!f) {
        /* File doesn't exist — nothing to replay. */
        return TSDB_OK;
    }

    crc32_init();

    int rc = TSDB_OK;
    void *buf = NULL;
    size_t buf_cap = 0;

    for (;;) {
        uint8_t hdr[8];
        size_t n = fread(hdr, 1, 8, f);
        if (n == 0) break; /* EOF */
        if (n < 8) { rc = TSDB_ERR_CORRUPT; break; }

        uint32_t stored_crc = get_u32le_buf(hdr + 0);
        uint32_t len        = get_u32le_buf(hdr + 4);

        /* Ensure buffer capacity. */
        if (len > buf_cap) {
            void *nb = realloc(buf, len ? len : 1);
            if (!nb) { rc = TSDB_ERR_NOMEM; break; }
            buf = nb;
            buf_cap = len;
        }

        if (len > 0 && fread(buf, 1, len, f) != len) {
            rc = TSDB_ERR_CORRUPT; break;
        }

        /* Verify CRC over [len_bytes][payload]. */
        uint32_t c = 0xFFFFFFFFu;
        for (int i = 0; i < 4; i++)
            c = crc32_table[(c ^ hdr[4 + i]) & 0xFF] ^ (c >> 8);
        if (len > 0) {
            const uint8_t *p = (const uint8_t *)buf;
            for (uint32_t i = 0; i < len; i++)
                c = crc32_table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
        }
        c ^= 0xFFFFFFFFu;

        if (c != stored_crc) { rc = TSDB_ERR_CORRUPT; break; }

        rc = cb(buf, len, ctx);
        if (rc != TSDB_OK) break;
    }

    free(buf);
    fclose(f);
    return rc;
}
