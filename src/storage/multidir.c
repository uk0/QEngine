/* multidir.c — multi-directory table placement.
 *
 * Registry layout (`<primary>/tables.idx`, append-only text):
 *     <table_name>\t<absolute_path_to_data_dir>\n
 * Lines are applied in order; last wins. Drops append a line with path
 * "" so the reader treats the table as forgotten.
 */

#include "multidir.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#define TSDB_MD_MAX_DIRS   16
#define TSDB_MD_MAX_TABLES 4096

typedef struct {
    char *name;
    char *dir;     /* pointer into md->paths[] */
} md_entry_t;

struct tsdb_multidir {
    char               primary[4096];
    char              *paths[TSDB_MD_MAX_DIRS];
    int                n_paths;
    tsdb_md_strategy_t strategy;

    /* Round-robin cursor. */
    int                rr_cursor;

    /* Registry: flat array, linear scan (thousands of tables is plenty). */
    md_entry_t        *entries;
    int                n_entries;
    int                cap_entries;

    pthread_mutex_t    lock;
};

/* ─── helpers ──────────────────────────────────────────────────────────── */

static uint64_t fnv1a_u64(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    for (; *s; s++) { h ^= (uint8_t)*s; h *= 1099511628211ULL; }
    return h;
}

/* Return bytes free on the filesystem containing `path`, or 0 on error. */
static uint64_t dir_free_bytes(const char *path) {
    struct statvfs vfs;
    if (statvfs(path, &vfs) != 0) return 0;
    return (uint64_t)vfs.f_bsize * (uint64_t)vfs.f_bavail;
}

/* Look up the slot for a table name (no locking — caller must hold lock). */
static int registry_find(tsdb_multidir_t *md, const char *name) {
    for (int i = 0; i < md->n_entries; i++) {
        if (strcmp(md->entries[i].name, name) == 0) return i;
    }
    return -1;
}

static int registry_ensure_cap(tsdb_multidir_t *md) {
    if (md->n_entries < md->cap_entries) return 0;
    int ncap = md->cap_entries ? md->cap_entries * 2 : 32;
    md_entry_t *nn = realloc(md->entries, sizeof(md_entry_t) * (size_t)ncap);
    if (!nn) return -1;
    md->entries = nn;
    md->cap_entries = ncap;
    return 0;
}

static const char *match_path(tsdb_multidir_t *md, const char *dir) {
    for (int i = 0; i < md->n_paths; i++)
        if (strcmp(md->paths[i], dir) == 0) return md->paths[i];
    return NULL;
}

/* Replay `tables.idx` at open time. Silent on missing file. */
static int registry_replay(tsdb_multidir_t *md) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/tables.idx", md->primary);

    FILE *f = fopen(path, "r");
    if (!f) return 0;

    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        size_t n = strlen(line);
        while (n > 0 && (line[n-1] == '\n' || line[n-1] == '\r')) line[--n] = 0;
        if (n == 0) continue;

        char *tab = strchr(line, '\t');
        if (!tab) continue;
        *tab = 0;
        const char *name = line;
        const char *dir  = tab + 1;

        int idx = registry_find(md, name);
        if (idx >= 0) {
            /* Update or forget. */
            if (!*dir) {
                /* Drop: remove entry. */
                free(md->entries[idx].name);
                for (int j = idx; j < md->n_entries - 1; j++)
                    md->entries[j] = md->entries[j + 1];
                md->n_entries--;
            } else {
                const char *matched = match_path(md, dir);
                if (matched) md->entries[idx].dir = (char *)matched;
            }
        } else if (*dir) {
            /* New placement. */
            if (registry_ensure_cap(md) < 0) continue;
            const char *matched = match_path(md, dir);
            if (!matched) continue;   /* dir not in current config; skip */
            md->entries[md->n_entries].name = strdup(name);
            md->entries[md->n_entries].dir  = (char *)matched;
            md->n_entries++;
        }
    }
    fclose(f);
    return 0;
}

static int registry_persist(tsdb_multidir_t *md,
                             const char *name, const char *dir) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/tables.idx", md->primary);
    FILE *f = fopen(path, "a");
    if (!f) return -1;
    fprintf(f, "%s\t%s\n", name, dir);
    fflush(f);
    fclose(f);
    return 0;
}

/* ─── public API ───────────────────────────────────────────────────────── */

int tsdb_multidir_new(const char *primary,
                      const char *const *extra, int n_extra,
                      tsdb_md_strategy_t strategy,
                      tsdb_multidir_t **out)
{
    if (!primary || !out) return TSDB_ERR_INVAL;
    if (n_extra < 0 || n_extra >= TSDB_MD_MAX_DIRS) return TSDB_ERR_INVAL;

    tsdb_multidir_t *md = calloc(1, sizeof(*md));
    if (!md) return TSDB_ERR_NOMEM;

    snprintf(md->primary, sizeof(md->primary), "%s", primary);
    pthread_mutex_init(&md->lock, NULL);
    md->strategy = strategy;

    md->paths[0] = strdup(primary);
    md->n_paths = 1;
    for (int i = 0; i < n_extra; i++) {
        if (!extra[i] || !*extra[i]) continue;
        md->paths[md->n_paths++] = strdup(extra[i]);
    }

    /* Ensure each directory exists (lazily; the storage layer will also
     * mkdir_p each table dir). */
    for (int i = 0; i < md->n_paths; i++) {
        struct stat st;
        if (stat(md->paths[i], &st) != 0) {
            if (mkdir(md->paths[i], 0755) != 0 && errno != EEXIST) {
                /* Non-fatal; a later table creation under this dir will
                 * surface the error. */
            }
        }
    }

    registry_replay(md);

    *out = md;
    return TSDB_OK;
}

void tsdb_multidir_free(tsdb_multidir_t *md) {
    if (!md) return;
    pthread_mutex_destroy(&md->lock);
    for (int i = 0; i < md->n_paths; i++) free(md->paths[i]);
    for (int i = 0; i < md->n_entries; i++) free(md->entries[i].name);
    free(md->entries);
    free(md);
}

const char *tsdb_multidir_place_table(tsdb_multidir_t *md,
                                      const char *table_name)
{
    if (!md || !table_name) return NULL;

    pthread_mutex_lock(&md->lock);

    /* Already placed? */
    int idx = registry_find(md, table_name);
    if (idx >= 0) {
        const char *p = md->entries[idx].dir;
        pthread_mutex_unlock(&md->lock);
        return p;
    }

    /* Pick a dir according to strategy. */
    const char *chosen = md->paths[0];
    if (md->n_paths > 1) {
        switch (md->strategy) {
        case TSDB_MD_ROUND_ROBIN:
            chosen = md->paths[md->rr_cursor];
            md->rr_cursor = (md->rr_cursor + 1) % md->n_paths;
            break;

        case TSDB_MD_LEAST_USED: {
            uint64_t best = 0;
            int      best_i = 0;
            for (int i = 0; i < md->n_paths; i++) {
                uint64_t f = dir_free_bytes(md->paths[i]);
                if (f > best) { best = f; best_i = i; }
            }
            chosen = md->paths[best_i];
            break;
        }

        case TSDB_MD_HASH: {
            uint64_t h = fnv1a_u64(table_name);
            chosen = md->paths[h % (uint64_t)md->n_paths];
            break;
        }
        }
    }

    /* Insert into registry. */
    if (registry_ensure_cap(md) == 0) {
        md->entries[md->n_entries].name = strdup(table_name);
        md->entries[md->n_entries].dir  = (char *)chosen;
        md->n_entries++;
        registry_persist(md, table_name, chosen);
    }

    pthread_mutex_unlock(&md->lock);
    return chosen;
}

const char *tsdb_multidir_find_table(tsdb_multidir_t *md,
                                     const char *table_name)
{
    if (!md || !table_name) return NULL;
    pthread_mutex_lock(&md->lock);
    int idx = registry_find(md, table_name);
    const char *p = (idx >= 0) ? md->entries[idx].dir : NULL;
    pthread_mutex_unlock(&md->lock);
    /* Fallback: if no entry but primary exists, caller can probe paths. */
    return p ? p : md->primary;
}

void tsdb_multidir_forget_table(tsdb_multidir_t *md, const char *table_name) {
    if (!md || !table_name) return;
    pthread_mutex_lock(&md->lock);
    int idx = registry_find(md, table_name);
    if (idx >= 0) {
        free(md->entries[idx].name);
        for (int j = idx; j < md->n_entries - 1; j++)
            md->entries[j] = md->entries[j + 1];
        md->n_entries--;
        registry_persist(md, table_name, "");
    }
    pthread_mutex_unlock(&md->lock);
}

int tsdb_multidir_count(const tsdb_multidir_t *md) {
    return md ? md->n_paths : 0;
}

const char *tsdb_multidir_path(const tsdb_multidir_t *md, int idx) {
    if (!md || idx < 0 || idx >= md->n_paths) return NULL;
    return md->paths[idx];
}
