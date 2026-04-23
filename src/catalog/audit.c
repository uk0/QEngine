/* audit.c — JSONL append-only audit log (see audit.h). */

#include "audit.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define TSDB_AUDIT_FIELD_MAX 256

struct tsdb_audit {
    FILE            *fp;            /* opened in append mode */
    char             path[4096];
    pthread_mutex_t  lock;
};

/* RFC 3339 timestamp with millisecond resolution, e.g.
 *   "2026-04-23T03:30:00.123Z".  Writes into ts[0..cap-1], NUL-term. */
static void fmt_ts(char *ts, size_t cap) {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    struct tm g;
    gmtime_r(&now.tv_sec, &g);
    int ms = (int)(now.tv_nsec / 1000000);
    snprintf(ts, cap, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
             g.tm_year + 1900, g.tm_mon + 1, g.tm_mday,
             g.tm_hour, g.tm_min, g.tm_sec, ms);
}

/* Minimal JSON string escape — \\, \", control chars → \uXXXX. */
static void j_escape(char *dst, size_t cap, const char *src) {
    if (!src) { if (cap > 0) dst[0] = '\0'; return; }
    size_t w = 0;
    size_t capped = 0;
    for (size_t i = 0; src[i] && w + 8 < cap && capped < TSDB_AUDIT_FIELD_MAX; i++) {
        unsigned char c = (unsigned char)src[i];
        capped++;
        switch (c) {
            case '"':  dst[w++] = '\\'; dst[w++] = '"';  break;
            case '\\': dst[w++] = '\\'; dst[w++] = '\\'; break;
            case '\n': dst[w++] = '\\'; dst[w++] = 'n';  break;
            case '\r': dst[w++] = '\\'; dst[w++] = 'r';  break;
            case '\t': dst[w++] = '\\'; dst[w++] = 't';  break;
            default:
                if (c < 0x20) {
                    w += (size_t)snprintf(dst + w, cap - w, "\\u%04x", c);
                } else {
                    dst[w++] = (char)c;
                }
        }
    }
    dst[w] = '\0';
}

int tsdb_audit_open(const char *data_dir, tsdb_audit_t **out) {
    if (!data_dir || !out) return -1;
    *out = NULL;

    /* Ensure <data_dir>/catalog exists; best-effort (catalog module
     * almost always runs first). */
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s/catalog", data_dir);
    (void)mkdir(dir, 0755);

    tsdb_audit_t *a = calloc(1, sizeof(*a));
    if (!a) return -1;
    snprintf(a->path, sizeof(a->path), "%s/audit.log", dir);

    a->fp = fopen(a->path, "a");
    if (!a->fp) {
        /* Best-effort: surface the error but keep the handle alive so
         * tsdb_audit_write becomes a silent no-op instead of a crash. */
        fprintf(stderr, "[audit] cannot open %s: %s\n",
                a->path, strerror(errno));
        free(a);
        return -1;
    }
    setvbuf(a->fp, NULL, _IOLBF, 0); /* line-buffered */
    pthread_mutex_init(&a->lock, NULL);
    *out = a;
    return 0;
}

void tsdb_audit_close(tsdb_audit_t *a) {
    if (!a) return;
    pthread_mutex_lock(&a->lock);
    if (a->fp) { fflush(a->fp); fclose(a->fp); a->fp = NULL; }
    pthread_mutex_unlock(&a->lock);
    pthread_mutex_destroy(&a->lock);
    free(a);
}

int tsdb_audit_write(tsdb_audit_t *a,
                     const char  *user,
                     const char  *event,
                     const char  *action,
                     const char  *object,
                     int          result,
                     const char  *detail)
{
    if (!a || !a->fp) return 0; /* silent no-op when disabled */

    char ts[64];
    char e_user[2 * TSDB_AUDIT_FIELD_MAX + 8];
    char e_event[2 * TSDB_AUDIT_FIELD_MAX + 8];
    char e_action[2 * TSDB_AUDIT_FIELD_MAX + 8];
    char e_object[2 * TSDB_AUDIT_FIELD_MAX + 8];
    char e_detail[2 * TSDB_AUDIT_FIELD_MAX + 8];

    fmt_ts(ts, sizeof(ts));
    j_escape(e_user,   sizeof(e_user),   user);
    j_escape(e_event,  sizeof(e_event),  event);
    j_escape(e_action, sizeof(e_action), action);
    j_escape(e_object, sizeof(e_object), object);
    j_escape(e_detail, sizeof(e_detail), detail);

    pthread_mutex_lock(&a->lock);
    int rc = fprintf(a->fp,
        "{\"ts\":\"%s\",\"user\":\"%s\",\"event\":\"%s\","
        "\"action\":\"%s\",\"object\":\"%s\","
        "\"result\":%d,\"detail\":\"%s\"}\n",
        ts, e_user, e_event, e_action, e_object, result, e_detail);
    pthread_mutex_unlock(&a->lock);
    return rc < 0 ? -1 : 0;
}

int tsdb_audit_tail(tsdb_audit_t *a, int max_rows,
                    char *buf, size_t cap)
{
    if (!a || !a->fp || !buf || cap == 0 || max_rows <= 0) return 0;

    /* Open a read handle — cannot mix reads and writes on the same
     * FILE* reliably.  The log is line-buffered on the write side so
     * readers always see complete records. */
    pthread_mutex_lock(&a->lock);
    FILE *r = fopen(a->path, "r");
    pthread_mutex_unlock(&a->lock);
    if (!r) return 0;

    if (fseek(r, 0, SEEK_END) != 0) { fclose(r); return -1; }
    long end = ftell(r);
    if (end <= 0) { fclose(r); return 0; }

    /* Walk backwards counting newlines until we've covered max_rows
     * records or reached the head.  For a multi-MB log and the usual
     * 500-row tail, this scans ~128 KiB at most. */
    const long CHUNK = 64 * 1024;
    long pos = end;
    long start = end;
    int  newlines = 0;
    char page[64 * 1024];
    while (pos > 0 && newlines <= max_rows) {
        long take = pos >= CHUNK ? CHUNK : pos;
        pos -= take;
        if (fseek(r, pos, SEEK_SET) != 0) { fclose(r); return -1; }
        if (fread(page, 1, (size_t)take, r) != (size_t)take) { fclose(r); return -1; }
        for (long i = take - 1; i >= 0; i--) {
            if (page[i] == '\n') {
                newlines++;
                if (newlines > max_rows) { start = pos + i + 1; break; }
            }
        }
        if (newlines > max_rows) break;
        start = pos;
    }
    if (start < 0) start = 0;

    /* Stream [start, end) into buf.  Truncate to cap (minus room for
     * a terminating NUL) if the log chunk doesn't fit. */
    if (fseek(r, start, SEEK_SET) != 0) { fclose(r); return -1; }
    size_t want = (size_t)(end - start);
    if (want >= cap) want = cap - 1;
    size_t got = fread(buf, 1, want, r);
    buf[got] = '\0';
    fclose(r);
    return (int)got;
}
