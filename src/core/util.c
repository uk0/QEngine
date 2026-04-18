/* Expose timegm / strptime on musl (Alpine) and GNU libc alike.
 * musl gates timegm behind _BSD_SOURCE; glibc behind _DEFAULT_SOURCE.
 * This block must precede every system header. */
#ifndef _DEFAULT_SOURCE
#  define _DEFAULT_SOURCE 1
#endif
#ifndef _BSD_SOURCE
#  define _BSD_SOURCE 1
#endif

#include "../../include/tsdb.h"
#include <string.h>
#include <time.h>
#include <stdio.h>

#define STR_(x) #x
#define STR(x) STR_(x)

const char *tsdb_version(void) {
    return STR(TSDB_VERSION_MAJOR) "." STR(TSDB_VERSION_MINOR) "." STR(TSDB_VERSION_PATCH);
}

const char *tsdb_errstr(int err) {
    switch (err) {
    case TSDB_OK:              return "ok";
    case TSDB_ERR_INVAL:       return "invalid argument";
    case TSDB_ERR_NOMEM:       return "out of memory";
    case TSDB_ERR_IO:          return "I/O error";
    case TSDB_ERR_CORRUPT:     return "data corrupt";
    case TSDB_ERR_NOTFOUND:    return "not found";
    case TSDB_ERR_EXISTS:      return "already exists";
    case TSDB_ERR_FULL:        return "full";
    case TSDB_ERR_OVERFLOW:    return "overflow";
    case TSDB_ERR_UNSUPPORTED: return "unsupported";
    case TSDB_ERR_PARSE:       return "parse error";
    case TSDB_ERR_SCHEMA:      return "schema mismatch";
    case TSDB_ERR_INTERNAL:    return "internal error";
    case TSDB_ERR_PERMISSION:  return "permission denied";
    default:                   return "unknown";
    }
}

tsdb_ts_t tsdb_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (tsdb_ts_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Parse ISO-8601 or "YYYY-MM-DD HH:MM:SS[.nnnnnnnnn]" to epoch nanoseconds.
 * Returns TSDB_TS_MIN on failure. Accepts trailing Z, accepts date-only. */
tsdb_ts_t tsdb_parse_ts(const char *s) {
    if (!s) return TSDB_TS_MIN;
    struct tm tm; memset(&tm, 0, sizeof(tm));
    int y, mo, d, h = 0, mi = 0, se = 0;
    long ns = 0;
    int consumed = 0;

    /* Date only */
    if (sscanf(s, "%d-%d-%d%n", &y, &mo, &d, &consumed) < 3) return TSDB_TS_MIN;
    tm.tm_year = y - 1900; tm.tm_mon = mo - 1; tm.tm_mday = d;

    const char *p = s + consumed;
    if (*p == ' ' || *p == 'T') {
        p++;
        int c2 = 0;
        if (sscanf(p, "%d:%d:%d%n", &h, &mi, &se, &c2) < 3) return TSDB_TS_MIN;
        tm.tm_hour = h; tm.tm_min = mi; tm.tm_sec = se;
        p += c2;
        if (*p == '.') {
            p++;
            int digits = 0;
            long frac = 0;
            while (*p >= '0' && *p <= '9' && digits < 9) {
                frac = frac * 10 + (*p - '0');
                digits++;
                p++;
            }
            /* Skip remaining digits if > 9 */
            while (*p >= '0' && *p <= '9') p++;
            while (digits < 9) { frac *= 10; digits++; }
            ns = frac;
        }
    }
    /* Z or timezone offset: treat as UTC. We ignore +HH:MM since timegm handles UTC. */
    tm.tm_isdst = 0;
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__linux__)
    time_t t = timegm(&tm);
#else
    time_t t = mktime(&tm); /* local TZ fallback */
#endif
    if (t == (time_t)-1) return TSDB_TS_MIN;
    return (tsdb_ts_t)t * 1000000000LL + ns;
}
