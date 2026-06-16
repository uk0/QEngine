/* retention.c — background GC: delete partitions older than retention policy.
 *
 * See retention.h for design notes.
 */

#include "retention.h"
#include "../server/log.h"
#include "../server/metrics.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fnmatch.h>
#include <stdatomic.h>

/* Forward declaration from db.h to avoid circular include. */
struct tsdb_db;
extern const char *tsdb_db_data_dir(struct tsdb_db *db);

/* ---- Duration parser ------------------------------------------------------ */

/* Parse "30d", "7d", "24h", "90d" etc. Returns ns, or -1 on error. */
static int64_t parse_duration_ns(const char *s) {
    if (!s || !*s) return -1;
    char *end = NULL;
    long long v = strtoll(s, &end, 10);
    if (end == s || v <= 0) return -1;
    switch (*end) {
        case 'd': case 'D': return (int64_t)v * 86400LL  * 1000000000LL;
        case 'h': case 'H': return (int64_t)v * 3600LL   * 1000000000LL;
        case 'm': case 'M': return (int64_t)v * 60LL     * 1000000000LL;
        case 's': case 'S': return (int64_t)v             * 1000000000LL;
        case '\0':          return (int64_t)v             * 1000000000LL; /* bare seconds */
        default:            return -1;
    }
}

/* ---- Retention rule store ------------------------------------------------ */

#define MAX_RULES 256

typedef struct {
    char    pattern[256];   /* fnmatch(3) pattern */
    int64_t retention_ns;
} retention_rule_t;

typedef struct {
    retention_rule_t rules[MAX_RULES];
    int              n_rules;
} retention_conf_t;

/*
 * Load retention.conf from path.
 * Format (per line):
 *   pattern = duration   # comment
 * Blank lines and lines starting with '#' are ignored.
 */
static int retention_conf_load(const char *path, retention_conf_t *conf) {
    conf->n_rules = 0;
    FILE *f = fopen(path, "r");
    if (!f) {
        /* Conf file missing is non-fatal — no rules means no GC. */
        return TSDB_OK;
    }

    char line[512];
    while (fgets(line, sizeof(line), f)) {
        /* Strip trailing newline / comment. */
        char *hash = strchr(line, '#');
        if (hash) *hash = '\0';
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';

        /* Skip blank lines. */
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '\0') continue;

        /* Split on '='. */
        char *eq = strchr(p, '=');
        if (!eq) continue;
        *eq = '\0';
        char *lhs = p;
        char *rhs = eq + 1;

        /* Trim whitespace from both sides. */
        while (*lhs && (lhs[strlen(lhs)-1] == ' ' || lhs[strlen(lhs)-1] == '\t'))
            lhs[strlen(lhs)-1] = '\0';
        while (*rhs == ' ' || *rhs == '\t') rhs++;
        char *re = rhs + strlen(rhs) - 1;
        while (re >= rhs && (*re == ' ' || *re == '\t')) { *re = '\0'; re--; }

        if (!*lhs || !*rhs) continue;

        int64_t ns = parse_duration_ns(rhs);
        if (ns <= 0) {
            TSDB_LOG_WARN("retention", "invalid duration '%s' for pattern '%s'", rhs, lhs);
            continue;
        }

        if (conf->n_rules >= MAX_RULES) {
            TSDB_LOG_WARN("retention", "too many rules in retention.conf (max %d)", MAX_RULES);
            break;
        }

        strncpy(conf->rules[conf->n_rules].pattern, lhs, sizeof(conf->rules[0].pattern) - 1);
        conf->rules[conf->n_rules].retention_ns = ns;
        conf->n_rules++;
    }

    fclose(f);
    return TSDB_OK;
}

/* Find first matching rule for table_name. Returns -1 if none. */
static int64_t retention_conf_lookup(const retention_conf_t *conf, const char *table_name) {
    for (int i = 0; i < conf->n_rules; i++) {
        if (fnmatch(conf->rules[i].pattern, table_name, 0) == 0) {
            return conf->rules[i].retention_ns;
        }
    }
    return -1;   /* no match → skip table */
}

/* ---- Recursive directory size + unlink ----------------------------------- */

/* Sum file sizes under dir (non-recursive, one level deep).
 * Part dirs only contain .col and .idx files so one level suffices. */
static uint64_t dir_size_bytes(const char *dir) {
    uint64_t total = 0;
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
        struct stat st;
        if (stat(path, &st) == 0 && S_ISREG(st.st_mode))
            total += (uint64_t)st.st_size;
    }
    closedir(d);
    return total;
}

/* Recursively remove dir and all contents. Like rm -rf. */
static int rmrf(const char *dir) {
    DIR *d = opendir(dir);
    if (!d) {
        /* If already gone, treat as success. */
        if (errno == ENOENT) return TSDB_OK;
        return TSDB_ERR_IO;
    }
    struct dirent *de;
    int rc = TSDB_OK;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        char child[4096];
        snprintf(child, sizeof(child), "%s/%s", dir, de->d_name);
        struct stat st;
        if (lstat(child, &st) != 0) { rc = TSDB_ERR_IO; continue; }
        if (S_ISDIR(st.st_mode)) {
            if (rmrf(child) != TSDB_OK) rc = TSDB_ERR_IO;
        } else {
            if (unlink(child) != 0 && errno != ENOENT) rc = TSDB_ERR_IO;
        }
    }
    closedir(d);
    if (rmdir(dir) != 0 && errno != ENOENT) rc = TSDB_ERR_IO;
    return rc;
}

/* ---- Partition end-timestamp from directory name ------------------------- */

/*
 * Detect partition unit from directory name length:
 *   8 chars  → DAY  "YYYYMMDD"
 *  10 chars  → HOUR "YYYYMMDDHH"
 *  otherwise → not a partition dir (skip)
 *
 * Returns partition end timestamp (exclusive) in ns, or -1 if name invalid.
 * "End" = start of the next period (so end_ts > any ts in this partition).
 */
static int64_t part_dir_end_ts_ns(const char *name) {
    size_t len = strlen(name);
    unsigned y = 0, mo = 0, dy = 0, hh = 0;

    if (len == 8) {
        /* DAY: YYYYMMDD */
        if (sscanf(name, "%4u%2u%2u", &y, &mo, &dy) != 3) return -1;
        /* Validate roughly. */
        if (y < 1970 || y > 9999 || mo < 1 || mo > 12 || dy < 1 || dy > 31) return -1;
        /* End ts = next day midnight UTC. Use timegm via tm. */
        struct tm tm;
        memset(&tm, 0, sizeof(tm));
        tm.tm_year = (int)y - 1900;
        tm.tm_mon  = (int)mo - 1;
        tm.tm_mday = (int)dy + 1;  /* +1 day */
        tm.tm_hour = 0;
        tm.tm_isdst = -1;
        time_t t = timegm(&tm);
        if (t == (time_t)-1) return -1;
        return (int64_t)t * 1000000000LL;
    } else if (len == 10) {
        /* HOUR: YYYYMMDDHH */
        if (sscanf(name, "%4u%2u%2u%2u", &y, &mo, &dy, &hh) != 4) return -1;
        if (y < 1970 || y > 9999 || mo < 1 || mo > 12 || dy < 1 || dy > 31 || hh > 23) return -1;
        struct tm tm;
        memset(&tm, 0, sizeof(tm));
        tm.tm_year = (int)y - 1900;
        tm.tm_mon  = (int)mo - 1;
        tm.tm_mday = (int)dy;
        tm.tm_hour = (int)hh + 1;  /* +1 hour */
        tm.tm_isdst = -1;
        time_t t = timegm(&tm);
        if (t == (time_t)-1) return -1;
        return (int64_t)t * 1000000000LL;
    }
    return -1;   /* not a partition dir */
}

/* ---- Stale gc dir cleanup ------------------------------------------------ */

/*
 * Scan table_dir for leftover ".gc_<ts>_<orig>" dirs from prior crashes
 * and remove them.  Called at start of each sweep for each table.
 */
static void cleanup_stale_gc_dirs(const char *table_dir) {
    DIR *d = opendir(table_dir);
    if (!d) return;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (strncmp(de->d_name, ".gc_", 4) != 0) continue;
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s", table_dir, de->d_name);
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            TSDB_LOG_INFO("retention", "removing stale gc dir: %s", path);
            rmrf(path);
        }
    }
    closedir(d);
}

/* ---- Main GC object ------------------------------------------------------ */

struct tsdb_retention {
    char              data_dir[4096];
    char              conf_path[4096];
    tsdb_retention_opts_t opts;

    /* Stats — updated under lock, read lock-free via copy. */
    pthread_mutex_t   lock;
    tsdb_retention_stats_t stats;

    /* Background thread control. */
    pthread_t         thread;
    pthread_mutex_t   cond_lock;
    pthread_cond_t    cond;
    int               stop;    /* 1 = signal thread to exit */
    int               thread_started;
};

/* ---- Wall clock ---------------------------------------------------------- */

static int64_t now_realtime_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ---- Core sweep implementation ------------------------------------------ */

static int do_sweep(struct tsdb_retention *r, int *deleted_out) {
    int deleted = 0;
    uint64_t bytes = 0;

    /* Reload conf on each sweep so operators can update it without restart.
     * conf is ~66 KB (256 rules × 264 B); keep it off the stack so this
     * function is safe to call from threads with a small stack — e.g. the
     * metrics-server HTTP handler that backs POST /retention/sweep, whose
     * frame already reserves large buffers.  A stack-resident conf overflowed
     * the handler thread's guard page → SIGBUS on the memset below. */
    retention_conf_t *conf = calloc(1, sizeof(*conf));
    if (!conf) return TSDB_ERR_NOMEM;
    retention_conf_load(r->conf_path, conf);

    int64_t now_ns = now_realtime_ns();

    DIR *root = opendir(r->data_dir);
    if (!root) {
        TSDB_LOG_WARN("retention", "cannot open data_dir '%s': %s",
                      r->data_dir, strerror(errno));
        free(conf);
        return TSDB_ERR_IO;
    }

    struct dirent *tde;
    while ((tde = readdir(root)) != NULL) {
        /* Skip hidden and special entries. */
        if (tde->d_name[0] == '.') continue;

        /* Skip non-directory entries (schema.bin, *.wal, etc. at root). */
        char table_dir[4096];
        snprintf(table_dir, sizeof(table_dir), "%s/%s", r->data_dir, tde->d_name);
        struct stat tst;
        if (stat(table_dir, &tst) != 0 || !S_ISDIR(tst.st_mode)) continue;

        /* Skip known non-table directories. */
        if (strcmp(tde->d_name, "catalog") == 0 ||
            strcmp(tde->d_name, "wal") == 0) continue;

        const char *table_name = tde->d_name;

        /* Look up retention policy for this table. */
        int64_t retention_ns = retention_conf_lookup(conf, table_name);
        if (retention_ns <= 0) {
            /* No rule → skip. */
            continue;
        }

        /* Remove leftover .gc_* dirs from prior interrupted sweeps. */
        cleanup_stale_gc_dirs(table_dir);

        /* Scan partition directories.
         *
         * POSIX readdir behaviour during directory mutation:
         * We rename matching entries out of tdir (to a .gc_* name) while
         * readdir is iterating over it.  POSIX does not guarantee whether a
         * renamed entry will be seen again by subsequent readdir calls.  Both
         * outcomes are safe here:
         *   - If re-surfaced: the new .gc_* name starts with '.' so the
         *     leading-dot check below skips it without further processing.
         *   - If not re-surfaced: nothing to do; it is already gone from the
         *     visible namespace.
         * We intentionally do NOT close/reopen tdir mid-scan to avoid O(n^2)
         * directory traversal for tables with many partitions.
         */
        DIR *tdir = opendir(table_dir);
        if (!tdir) continue;

        struct dirent *pde;
        while ((pde = readdir(tdir)) != NULL) {
            if (pde->d_name[0] == '.') continue;  /* skip hidden + .gc_* */

            /* Check partition dir name format and compute end timestamp. */
            int64_t part_end_ns = part_dir_end_ts_ns(pde->d_name);
            if (part_end_ns < 0) continue;  /* not a partition dir */

            /* Partition is expired if: now_ns - part_end_ns > retention_ns
             * Note: if part_end_ns > now_ns (active/future partition), diff is
             * negative, so the condition is always false — safe automatically. */
            int64_t age_ns = now_ns - part_end_ns;
            if (age_ns <= retention_ns) continue;

            char part_path[4096];
            snprintf(part_path, sizeof(part_path), "%s/%s", table_dir, pde->d_name);

            struct stat pst;
            if (stat(part_path, &pst) != 0 || !S_ISDIR(pst.st_mode)) continue;

            /* Measure bytes before deletion. */
            uint64_t part_bytes = dir_size_bytes(part_path);

            /* Build rename target: <table_dir>/.gc_<epoch_ns>_<orig>  */
            char gc_path[4096];
            snprintf(gc_path, sizeof(gc_path), "%s/.gc_%lld_%s",
                     table_dir, (long long)now_realtime_ns(), pde->d_name);

            TSDB_LOG_INFO("retention",
                          "%s: partition %s age=%.1fd retention=%.1fd → %s",
                          table_name, pde->d_name,
                          (double)age_ns / 86400e9,
                          (double)retention_ns / 86400e9,
                          r->opts.dry_run ? "DRY-RUN skip" : "deleting");

            if (r->opts.dry_run) {
                /* Dry-run: count but don't touch. */
                pthread_mutex_lock(&r->lock);
                r->stats.partitions_deleted++;
                r->stats.bytes_reclaimed += part_bytes;
                pthread_mutex_unlock(&r->lock);
                deleted++;
                continue;
            }

            /* Rename-first (atomic from reader's perspective). */
            if (rename(part_path, gc_path) != 0) {
                TSDB_LOG_WARN("retention",
                              "rename failed for %s → %s: %s",
                              part_path, gc_path, strerror(errno));
                continue;
            }

            /* Recursive unlink of the renamed dir. */
            if (rmrf(gc_path) != TSDB_OK) {
                TSDB_LOG_WARN("retention", "rmrf failed for %s: %s",
                              gc_path, strerror(errno));
                /* Directory might be partially gone; don't double-count. */
            }

            bytes += part_bytes;
            deleted++;

            pthread_mutex_lock(&r->lock);
            r->stats.partitions_deleted++;
            r->stats.bytes_reclaimed += part_bytes;
            pthread_mutex_unlock(&r->lock);
        }
        closedir(tdir);
    }
    closedir(root);

    pthread_mutex_lock(&r->lock);
    r->stats.sweeps_done++;
    r->stats.last_sweep_ns = now_realtime_ns();
    pthread_mutex_unlock(&r->lock);

    /* Publish to process-wide metrics so /metrics observers can see
     * retention activity without opening the internal handle. */
    tsdb_metric_inc("qengine_retention_sweeps_total");
    if (deleted > 0) {
        tsdb_metric_add("qengine_retention_partitions_deleted_total",
                        (uint64_t)deleted);
    }
    if (bytes > 0) {
        tsdb_metric_add("qengine_retention_bytes_reclaimed_total", bytes);
    }

    if (deleted_out) *deleted_out = deleted;
    TSDB_LOG_INFO("retention",
                  "sweep done: %d partitions removed, %.1f MB reclaimed",
                  deleted, (double)bytes / (1024.0 * 1024.0));
    free(conf);
    return TSDB_OK;
}

/* ---- Background thread --------------------------------------------------- */

static void *retention_thread(void *arg) {
    struct tsdb_retention *r = (struct tsdb_retention *)arg;
    int64_t interval_ns = r->opts.sweep_interval_ns;
    if (interval_ns <= 0) interval_ns = 3600LL * 1000000000LL;  /* 1 hour default */

    pthread_mutex_lock(&r->cond_lock);
    while (!r->stop) {
        /* Convert interval_ns to an absolute timespec for timedwait. */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        int64_t abs_ns = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec + interval_ns;
        ts.tv_sec  = (time_t)(abs_ns / 1000000000LL);
        ts.tv_nsec = (long)(abs_ns % 1000000000LL);

        int wait_rc = pthread_cond_timedwait(&r->cond, &r->cond_lock, &ts);
        (void)wait_rc;  /* ETIMEDOUT is expected and fine */

        if (r->stop) break;

        pthread_mutex_unlock(&r->cond_lock);
        do_sweep(r, NULL);
        pthread_mutex_lock(&r->cond_lock);
    }
    pthread_mutex_unlock(&r->cond_lock);
    return NULL;
}

/* ---- Public API ---------------------------------------------------------- */

int tsdb_retention_start(const char *data_dir,
                          const char *conf_path,
                          const tsdb_retention_opts_t *opts,
                          tsdb_retention_t **out)
{
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    struct tsdb_retention *r = calloc(1, sizeof(*r));
    if (!r) return TSDB_ERR_NOMEM;

    snprintf(r->data_dir, sizeof(r->data_dir), "%s", data_dir);

    if (conf_path) {
        snprintf(r->conf_path, sizeof(r->conf_path), "%s", conf_path);
    } else {
        snprintf(r->conf_path, sizeof(r->conf_path), "%s/retention.conf", data_dir);
    }

    if (opts) {
        r->opts = *opts;
    } else {
        r->opts.sweep_interval_ns = 0;
        r->opts.dry_run           = 0;
    }

    pthread_mutex_init(&r->lock, NULL);
    pthread_mutex_init(&r->cond_lock, NULL);
    pthread_cond_init(&r->cond, NULL);

    /* Start background thread. */
    int rc = pthread_create(&r->thread, NULL, retention_thread, r);
    if (rc != 0) {
        pthread_cond_destroy(&r->cond);
        pthread_mutex_destroy(&r->cond_lock);
        pthread_mutex_destroy(&r->lock);
        free(r);
        return TSDB_ERR_INTERNAL;
    }
    r->thread_started = 1;

    *out = r;
    TSDB_LOG_INFO("retention", "started; conf=%s interval=%.1fh dry_run=%d",
                  r->conf_path,
                  (r->opts.sweep_interval_ns > 0
                   ? (double)r->opts.sweep_interval_ns / 3600e9
                   : 1.0),
                  r->opts.dry_run);
    return TSDB_OK;
}

void tsdb_retention_stop(tsdb_retention_t *r) {
    if (!r) return;

    /* Signal thread to exit. */
    pthread_mutex_lock(&r->cond_lock);
    r->stop = 1;
    pthread_cond_signal(&r->cond);
    pthread_mutex_unlock(&r->cond_lock);

    if (r->thread_started) {
        pthread_join(r->thread, NULL);
        r->thread_started = 0;
    }

    pthread_cond_destroy(&r->cond);
    pthread_mutex_destroy(&r->cond_lock);
    pthread_mutex_destroy(&r->lock);
    free(r);
}

int tsdb_retention_sweep_once(tsdb_retention_t *r, int *deleted_out) {
    if (!r) return TSDB_ERR_INVAL;
    return do_sweep(r, deleted_out);
}

void tsdb_retention_stats(const tsdb_retention_t *r, tsdb_retention_stats_t *out) {
    if (!r || !out) return;
    pthread_mutex_lock((pthread_mutex_t *)&r->lock);  /* const-safe via cast */
    *out = r->stats;
    pthread_mutex_unlock((pthread_mutex_t *)&r->lock);
}

/* ---- DB integration ------------------------------------------------------ */

/*
 * tsdb_db_set_retention: attach retention GC to a tsdb_db_t instance.
 * The db.h / db.c pair stores the handle and tears it down in tsdb_close().
 * We use the public accessor tsdb_db_data_dir() to get the path; the handle
 * is stashed via tsdb_db_attach_retention() defined in db.c.
 */
extern void tsdb_db_attach_retention(struct tsdb_db *db, tsdb_retention_t *r);

int tsdb_db_set_retention(struct tsdb_db *db,
                           const char *conf_path,
                           const tsdb_retention_opts_t *opts)
{
    if (!db) return TSDB_ERR_INVAL;

    const char *data_dir = tsdb_db_data_dir(db);
    tsdb_retention_t *r  = NULL;
    int rc = tsdb_retention_start(data_dir, conf_path, opts, &r);
    if (rc != TSDB_OK) return rc;

    tsdb_db_attach_retention(db, r);
    return TSDB_OK;
}
