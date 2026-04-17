/* log.c — implementation of the tsdb structured logger. */

#define _POSIX_C_SOURCE 200809L

#include "log.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

/* ─── global state ──────────────────────────────────────────────────────── */

static pthread_mutex_t  g_mu = PTHREAD_MUTEX_INITIALIZER;
static tsdb_log_level_t g_level = TSDB_LOG_INFO;
static tsdb_log_sink_t  g_sink  = TSDB_SINK_STDERR;
static int              g_json  = 0;
static FILE            *g_file  = NULL;
static char             g_file_path[TSDB_CFG_MAX_PATH];
static int              g_syslog_open = 0;

/* Copy of the debug_flags list. Flat linear scan — expected size < 16. */
static char  *g_flags[TSDB_CFG_MAX_DEBUG_FLAGS];
static int    g_nflags = 0;

static void clear_flags(void) {
    for (int i = 0; i < g_nflags; i++) { free(g_flags[i]); g_flags[i] = NULL; }
    g_nflags = 0;
}

static const char *level_str(tsdb_log_level_t lvl) {
    switch (lvl) {
    case TSDB_LOG_ERROR: return "ERROR";
    case TSDB_LOG_WARN:  return "WARN";
    case TSDB_LOG_INFO:  return "INFO";
    case TSDB_LOG_DEBUG: return "DEBUG";
    case TSDB_LOG_TRACE: return "TRACE";
    default:             return "?";
    }
}

static int level_to_syslog(tsdb_log_level_t lvl) {
    switch (lvl) {
    case TSDB_LOG_ERROR: return LOG_ERR;
    case TSDB_LOG_WARN:  return LOG_WARNING;
    case TSDB_LOG_INFO:  return LOG_INFO;
    case TSDB_LOG_DEBUG: return LOG_DEBUG;
    case TSDB_LOG_TRACE: return LOG_DEBUG;
    default:             return LOG_NOTICE;
    }
}

/* ─── init / shutdown ───────────────────────────────────────────────────── */

int tsdb_log_init(const tsdb_config_t *cfg) {
    if (!cfg) return -1;

    pthread_mutex_lock(&g_mu);

    g_level = cfg->log_level;
    g_sink  = cfg->log_sink;
    g_json  = (strcasecmp(cfg->log_format, "json") == 0);

    /* Re-open file sink if needed. */
    if (g_file) { fclose(g_file); g_file = NULL; g_file_path[0] = 0; }

    if ((g_sink == TSDB_SINK_FILE || g_sink == TSDB_SINK_BOTH)
        && cfg->log_file[0]) {
        g_file = fopen(cfg->log_file, "a");
        if (!g_file) {
            /* Fall back to stderr so we don't lose messages. */
            fprintf(stderr, "log: cannot open %s: %s; falling back to stderr\n",
                    cfg->log_file, strerror(errno));
            g_sink = TSDB_SINK_STDERR;
        } else {
            setvbuf(g_file, NULL, _IOLBF, 0);
            snprintf(g_file_path, sizeof(g_file_path), "%s", cfg->log_file);
        }
    }

    if (g_sink == TSDB_SINK_SYSLOG && !g_syslog_open) {
        openlog("tsdb", LOG_PID | LOG_NDELAY, LOG_DAEMON);
        g_syslog_open = 1;
    }

    clear_flags();
    for (int i = 0; i < cfg->n_debug_flags && g_nflags < TSDB_CFG_MAX_DEBUG_FLAGS; i++) {
        g_flags[g_nflags++] = strdup(cfg->debug_flags[i]);
    }

    pthread_mutex_unlock(&g_mu);

    /* Announce config at INFO level. */
    TSDB_LOG_INFO("log", "level=%s sink=%s format=%s flags=%d",
                  level_str(g_level),
                  g_sink == TSDB_SINK_STDERR ? "stderr" :
                  g_sink == TSDB_SINK_FILE   ? "file"   :
                  g_sink == TSDB_SINK_SYSLOG ? "syslog" : "both",
                  g_json ? "json" : "text", g_nflags);
    return 0;
}

void tsdb_log_shutdown(void) {
    pthread_mutex_lock(&g_mu);
    if (g_file) { fclose(g_file); g_file = NULL; }
    if (g_syslog_open) { closelog(); g_syslog_open = 0; }
    clear_flags();
    pthread_mutex_unlock(&g_mu);
}

/* ─── predicates ────────────────────────────────────────────────────────── */

bool tsdb_log_enabled(tsdb_log_level_t lvl) {
    /* Atomic int read is fine — stale value can only disable one log. */
    return lvl <= g_level;
}

bool tsdb_log_flag(const char *name) {
    if (!name) return false;
    for (int i = 0; i < g_nflags; i++) {
        if (strcasecmp(g_flags[i], name) == 0) return true;
        if (strcasecmp(g_flags[i], "all") == 0) return true;
    }
    return false;
}

/* ─── writef ────────────────────────────────────────────────────────────── */

/* Format current time as "YYYY-MM-DDTHH:MM:SS.mmmZ" (UTC). */
static void fmt_ts(char *buf, size_t cap) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    gmtime_r(&ts.tv_sec, &tm);
    snprintf(buf, cap, "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec,
             ts.tv_nsec / 1000000);
}

/* Escape message for JSON output: backslash, quote, control chars. */
static void json_escape(const char *in, char *out, size_t cap) {
    size_t o = 0;
    for (size_t i = 0; in[i] && o + 2 < cap; i++) {
        unsigned char c = (unsigned char)in[i];
        if (c == '"' || c == '\\')       { out[o++] = '\\'; out[o++] = (char)c; }
        else if (c == '\n')              { out[o++] = '\\'; out[o++] = 'n'; }
        else if (c == '\t')              { out[o++] = '\\'; out[o++] = 't'; }
        else if (c < 0x20)               { /* drop other control chars */ }
        else                             { out[o++] = (char)c; }
    }
    out[o] = 0;
}

void tsdb_log_writef(tsdb_log_level_t lvl, const char *src,
                     const char *fmt, ...)
{
    if (lvl > g_level) return;

    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    char when[48];
    fmt_ts(when, sizeof(when));

    char line[2048];
    if (g_json) {
        char msg_j[sizeof(msg) * 2];
        json_escape(msg, msg_j, sizeof(msg_j));
        snprintf(line, sizeof(line),
                 "{\"ts\":\"%s\",\"lvl\":\"%s\",\"src\":\"%s\",\"msg\":\"%s\"}\n",
                 when, level_str(lvl), src ? src : "-", msg_j);
    } else {
        snprintf(line, sizeof(line), "%s %-5s %s: %s\n",
                 when, level_str(lvl), src ? src : "-", msg);
    }

    pthread_mutex_lock(&g_mu);
    if (g_sink == TSDB_SINK_STDERR || g_sink == TSDB_SINK_BOTH) {
        fputs(line, stderr);
    }
    if ((g_sink == TSDB_SINK_FILE || g_sink == TSDB_SINK_BOTH) && g_file) {
        fputs(line, g_file);
    }
    if (g_sink == TSDB_SINK_SYSLOG && g_syslog_open) {
        syslog(level_to_syslog(lvl), "%s: %s", src ? src : "-", msg);
    }
    pthread_mutex_unlock(&g_mu);
}
