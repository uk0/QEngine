/* log.h — structured logging wired to tsd.conf.
 *
 * Level + sink + feature-flag gating in one place. Thread-safe via a
 * single mutex; format overhead is zero when the call is gated out.
 */
#ifndef TSDB_SERVER_LOG_H
#define TSDB_SERVER_LOG_H

#include "config.h"
#include <stdarg.h>
#include <stdbool.h>

/* Install log subsystem from a fully-populated config. Should be called
 * once during startup. Safe to call again to reload (e.g. on SIGHUP). */
int  tsdb_log_init(const tsdb_config_t *cfg);
void tsdb_log_shutdown(void);

/* True if the current level permits a log at 'lvl'. */
bool tsdb_log_enabled(tsdb_log_level_t lvl);

/* True if the named debug feature-flag is enabled.
 * Examples: "scan_plan", "flush", "rawblock", "zone_map". */
bool tsdb_log_flag(const char *name);

/* The core logging call. Use the macros below to get source tagging. */
void tsdb_log_writef(tsdb_log_level_t lvl, const char *src,
                     const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

#define TSDB_LOG_ERROR(src, ...) tsdb_log_writef(TSDB_LOG_ERROR, src, __VA_ARGS__)
#define TSDB_LOG_WARN(src,  ...) tsdb_log_writef(TSDB_LOG_WARN,  src, __VA_ARGS__)
#define TSDB_LOG_INFO(src,  ...) tsdb_log_writef(TSDB_LOG_INFO,  src, __VA_ARGS__)

/* Debug/trace are gated cheaply at the call site by an `if` check to
 * skip argument evaluation when disabled. */
#define TSDB_LOG_DEBUG(src, ...) do { \
    if (tsdb_log_enabled(TSDB_LOG_DEBUG)) \
        tsdb_log_writef(TSDB_LOG_DEBUG, src, __VA_ARGS__); \
} while (0)

#define TSDB_LOG_TRACE(src, ...) do { \
    if (tsdb_log_enabled(TSDB_LOG_TRACE)) \
        tsdb_log_writef(TSDB_LOG_TRACE, src, __VA_ARGS__); \
} while (0)

/* Feature-flag-gated DEBUG: cheap when the flag is off. */
#define TSDB_DBG(flag, src, ...) do { \
    if (tsdb_log_enabled(TSDB_LOG_DEBUG) && tsdb_log_flag(flag)) \
        tsdb_log_writef(TSDB_LOG_DEBUG, src, __VA_ARGS__); \
} while (0)

#endif /* TSDB_SERVER_LOG_H */
