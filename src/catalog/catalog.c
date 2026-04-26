/* catalog.c — Group & Device catalog implementation.
 *
 * Persistence model:
 *   <data_dir>/catalog/groups.log  — append-only, one record per line
 *   <data_dir>/catalog/devices.log — append-only, one record per line
 *
 * Record format (tab-separated fields, values URL-percent-encoded):
 *   Groups:  +group <name> <region> <retention_ns> <codec_profile> <replica_factor> <tags> <created_at>
 *            -group <name>
 *   Devices: +device <group> <id> <type> <location> <tags> <first_seen> <last_seen> <status>
 *            -device <group> <id>
 *            ~device <group> <id> <last_seen>   (update last_seen)
 *
 * On open, the log is replayed top-to-bottom; tombstones (-) remove entries.
 * All strings are percent-encoded (at minimum: TAB=>\t, NL=>\n, %=>%25).
 */

#include "group.h"
#include "device.h"
#include "stable.h"
#include "database.h"
#include "../../include/tsdb.h"
#include "../core/types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdarg.h>
#include <ctype.h>

/* ---- mkdir_p (forward from schema.c / reimplemented locally) ------------ */

extern int tsdb_mkdir_p(const char *path);

/* ---- hash map helpers --------------------------------------------------- */

/* Simple open-addressing hash map mapping C-string keys to void* values.
 * Load factor capped at 0.7. */

#define HMAP_INIT_CAP 16

typedef struct hmap_entry {
    char *key;
    void *val;
} hmap_entry_t;

typedef struct hmap {
    hmap_entry_t *buckets;
    size_t        cap;
    size_t        size;
} hmap_t;

static size_t hmap_hash(const char *s) {
    size_t h = 14695981039346656037ULL;
    while (*s) { h ^= (unsigned char)*s++; h *= 1099511628211ULL; }
    return h;
}

static int hmap_init(hmap_t *m) {
    m->cap = HMAP_INIT_CAP;
    m->size = 0;
    m->buckets = calloc(m->cap, sizeof(hmap_entry_t));
    return m->buckets ? 0 : -1;
}

static void hmap_free_values(hmap_t *m) {
    for (size_t i = 0; i < m->cap; i++) {
        if (m->buckets[i].key) {
            free(m->buckets[i].key);
            free(m->buckets[i].val);
            m->buckets[i].key = NULL;
            m->buckets[i].val = NULL;
        }
    }
    m->size = 0;
}

static void hmap_destroy(hmap_t *m) {
    hmap_free_values(m);
    free(m->buckets);
    m->buckets = NULL;
}

/* Insert without rehash (internal). */
static void hmap_insert_raw(hmap_entry_t *buckets, size_t cap,
                             char *key, void *val) {
    size_t idx = hmap_hash(key) & (cap - 1);
    while (buckets[idx].key) idx = (idx + 1) & (cap - 1);
    buckets[idx].key = key;
    buckets[idx].val = val;
}

static int hmap_grow(hmap_t *m) {
    size_t newcap = m->cap * 2;
    hmap_entry_t *nb = calloc(newcap, sizeof(hmap_entry_t));
    if (!nb) return -1;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->buckets[i].key)
            hmap_insert_raw(nb, newcap, m->buckets[i].key, m->buckets[i].val);
    }
    free(m->buckets);
    m->buckets = nb;
    m->cap = newcap;
    return 0;
}

/* Returns 0 on success. key is strdup'd. val is NOT freed on conflict
 * (caller checks return). If key exists, overwrites val (frees old). */
static int hmap_put(hmap_t *m, const char *key, void *val) {
    if (m->size * 10 >= m->cap * 7) {
        if (hmap_grow(m) != 0) return -1;
    }
    size_t idx = hmap_hash(key) & (m->cap - 1);
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0) {
            free(m->buckets[idx].val);
            m->buckets[idx].val = val;
            return 0;
        }
        idx = (idx + 1) & (m->cap - 1);
    }
    m->buckets[idx].key = strdup(key);
    if (!m->buckets[idx].key) return -1;
    m->buckets[idx].val = val;
    m->size++;
    return 0;
}

static void *hmap_get(hmap_t *m, const char *key) {
    if (!m->cap) return NULL;
    size_t idx = hmap_hash(key) & (m->cap - 1);
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0)
            return m->buckets[idx].val;
        idx = (idx + 1) & (m->cap - 1);
    }
    return NULL;
}

/* Delete by key. free_val=1 frees the value pointer, 0 leaves it. Returns 1 if found. */
static int hmap_del_ex(hmap_t *m, const char *key, int free_val) {
    if (!m->cap) return 0;
    size_t idx = hmap_hash(key) & (m->cap - 1);
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0) {
            free(m->buckets[idx].key);
            if (free_val) free(m->buckets[idx].val);
            m->buckets[idx].key = NULL;
            m->buckets[idx].val = NULL;
            m->size--;
            /* Rehash subsequent entries in cluster. */
            size_t j = (idx + 1) & (m->cap - 1);
            while (m->buckets[j].key) {
                char *k = m->buckets[j].key;
                void *v = m->buckets[j].val;
                m->buckets[j].key = NULL;
                m->buckets[j].val = NULL;
                m->size--;
                hmap_insert_raw(m->buckets, m->cap, k, v);
                m->size++;
                j = (j + 1) & (m->cap - 1);
            }
            return 1;
        }
        idx = (idx + 1) & (m->cap - 1);
    }
    return 0;
}

/* Default: frees the value. */
static int hmap_del(hmap_t *m, const char *key) {
    return hmap_del_ex(m, key, 1);
}

/* Delete without freeing val (caller owns the value). */
static int hmap_del_nofreeval(hmap_t *m, const char *key) {
    return hmap_del_ex(m, key, 0);
}

/* ---- percent encoding / decoding ---------------------------------------- */

/* Encode src into dst (must be >= src_len*3+1 bytes). Returns written. */
static size_t pct_encode(const char *src, char *dst, size_t dstsz) {
    size_t w = 0;
    for (const char *p = src; *p && w + 4 < dstsz; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '\t' || c == '\n' || c == '\r' || c == '%' || c == ' ') {
            w += (size_t)snprintf(dst + w, dstsz - w, "%%%02X", c);
        } else {
            dst[w++] = (char)c;
        }
    }
    if (w < dstsz) dst[w] = '\0';
    return w;
}

static int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    return -1;
}

/* Decode in-place. dst == src is safe. */
static void pct_decode(const char *src, char *dst, size_t dstsz) {
    size_t w = 0;
    for (const char *p = src; *p && w + 1 < dstsz; p++) {
        if (*p == '%' && p[1] && p[2]) {
            int hi = hex_val(p[1]), lo = hex_val(p[2]);
            if (hi >= 0 && lo >= 0) {
                dst[w++] = (char)((hi << 4) | lo);
                p += 2;
                continue;
            }
        }
        dst[w++] = *p;
    }
    if (w < dstsz) dst[w] = '\0';
}

/* ---- Catalog struct ------------------------------------------------------ */

/* Per-group device index: hmap of device_id -> tsdb_device_t* */
typedef struct {
    hmap_t devices; /* device_id -> tsdb_device_t* (owned) */
} group_entry_t;

struct tsdb_catalog {
    char          data_dir[4096];
    char          cat_dir[4096];

    pthread_mutex_t lock;

    /* databases: name -> tsdb_database_t* (owned) */
    hmap_t         databases;

    /* groups: name -> tsdb_group_t* (owned) */
    hmap_t         groups;

    /* device_index: group_name -> group_entry_t* (owned) */
    hmap_t         device_index;

    /* stables: name -> tsdb_stable_t* (owned) */
    hmap_t         stables;

    /* child_tables: child_name -> tsdb_child_table_t* (owned) */
    hmap_t         child_tables;

    FILE          *databases_log;
    FILE          *groups_log;
    FILE          *devices_log;
    FILE          *stables_log;
    FILE          *children_log;
};

/* ---- Log helpers --------------------------------------------------------- */

/* Forward decls — definitions live further down in this file. */
static int  split_tab(char *line, char **fields, int max_fields);

/* ---- Catalog log compaction ------------------------------------------- *
 *
 * Each catalog log (databases / groups / stables / child_tables) is an
 * append-only stream of `+create`/`-drop` records.  After enough churn
 * the file dwarfs the live in-memory state — a heavily-tested cluster
 * accumulated 75 KB in databases.log for ~5 live DBs in the v5 audit.
 *
 * Compaction rewrites the file from the post-replay hmap state in one
 * pass: open <path>.tmp, write a fresh `+create` line per live entry,
 * fsync, rename atomically over the original.  No tombstones survive
 * because the live state already represents net effect.
 *
 * Invoked from tsdb_catalog_open after replay completes — so the
 * append handle that opens immediately afterwards lands on the
 * already-compacted file.  Triggered when current file size exceeds
 * a threshold (default 16 KB), keeping the steady-state cost low.
 *
 * Atomic recovery: a crash between rename(tmp,path) and the next
 * fsync is safe — the tmp's fsync committed all live entries before
 * rename, and rename(2) is atomic on POSIX.  A crash between fopen(tmp)
 * and rename leaves the tmp orphaned and the original untouched; the
 * orphan is harmlessly overwritten next compaction. */
#include <sys/stat.h>
#include <unistd.h>

#define CATALOG_COMPACT_MIN_BYTES 16384

/* Forward decl — definition lives below the group writer. */
static int compact_groups(tsdb_catalog_t *c, const char *path);

static off_t file_size_or_zero(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return st.st_size;
}

/* ---- Database log + replay + CRUD ---------------------------------- *
 *
 * databases.log record format (tab-separated, percent-encoded):
 *   +db  <name>  <description>  <retention_ns>  <created_at>
 *   -db  <name>
 *
 * Mirrors groups.log's shape so future versions that add columns can
 * append them without breaking older readers.
 */

static int log_database(tsdb_catalog_t *c, char op, const tsdb_database_t *d) {
    char enc_name[128], enc_desc[512];
    pct_encode(d->name,        enc_name, sizeof(enc_name));
    pct_encode(d->description, enc_desc, sizeof(enc_desc));
    /* V1 records had 5 fields (+ name desc retention created).  We now
     * append a 6th — protected_flag.  Readers that only know V1 shape
     * still parse the first 4; writers always emit V2. */
    int n = fprintf(c->databases_log,
                    "%c\t%s\t%s\t%lld\t%lld\t%u\n",
                    op, enc_name, enc_desc,
                    (long long)d->retention_ns,
                    (long long)d->created_at,
                    (unsigned)d->protected_flag);
    if (n < 0) return TSDB_ERR_IO;
    if (fflush(c->databases_log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_database_drop(tsdb_catalog_t *c, const char *name) {
    char enc_name[128];
    pct_encode(name, enc_name, sizeof(enc_name));
    int n = fprintf(c->databases_log, "-\t%s\n", enc_name);
    if (n < 0) return TSDB_ERR_IO;
    if (fflush(c->databases_log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int replay_databases(tsdb_catalog_t *c, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return TSDB_OK;
    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        size_t ln = strlen(line);
        while (ln > 0 && (line[ln-1] == '\n' || line[ln-1] == '\r'))
            line[--ln] = '\0';
        if (!line[0]) continue;
        char *fields[8];
        char cp[4096];
        strncpy(cp, line, sizeof(cp) - 1);
        cp[sizeof(cp) - 1] = '\0';
        int nf = split_tab(cp, fields, 8);
        if (nf < 1) continue;
        char op = fields[0][0];
        if (op == '+' && nf >= 5) {
            tsdb_database_t *d = calloc(1, sizeof(*d));
            if (!d) { fclose(f); return TSDB_ERR_NOMEM; }
            char name[64], desc[192];
            pct_decode(fields[1], name, sizeof(name));
            pct_decode(fields[2], desc, sizeof(desc));
            strncpy(d->name, name, sizeof(d->name) - 1);
            strncpy(d->description, desc, sizeof(d->description) - 1);
            d->retention_ns = (int64_t)strtoll(fields[3], NULL, 10);
            d->created_at   = (tsdb_ts_t)strtoll(fields[4], NULL, 10);
            d->protected_flag = (nf >= 6) ? (uint8_t)strtoul(fields[5], NULL, 10)
                                          : (uint8_t)0;
            /* sysdb is always protected — defence in depth against hand-
             * edited logs. */
            if (strcmp(d->name, TSDB_SYSDB_NAME) == 0) d->protected_flag = 1;
            hmap_put(&c->databases, d->name, d);
        } else if (op == '-' && nf >= 2) {
            char name[64];
            pct_decode(fields[1], name, sizeof(name));
            if (strcmp(name, TSDB_SYSDB_NAME) == 0) continue; /* ignore */
            hmap_del(&c->databases, name);
        }
    }
    fclose(f);
    return TSDB_OK;
}

/* Rewrite databases.log from the post-replay hmap.  Caller passes
 * `path` so this can be invoked at open time before the live append
 * handle is opened. */
static int compact_databases(tsdb_catalog_t *c, const char *path) {
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    FILE *new_log = fopen(tmp, "w");
    if (!new_log) return TSDB_ERR_IO;

    FILE *saved = c->databases_log;
    c->databases_log = new_log;
    int rc = TSDB_OK;
    for (size_t i = 0; i < c->databases.cap; i++) {
        if (!c->databases.buckets[i].key) continue;
        tsdb_database_t *d = (tsdb_database_t *)c->databases.buckets[i].val;
        if (log_database(c, '+', d) != TSDB_OK) { rc = TSDB_ERR_IO; break; }
    }
    fflush(new_log);
    fsync(fileno(new_log));
    fclose(new_log);
    c->databases_log = saved;
    if (rc != TSDB_OK) { unlink(tmp); return rc; }
    if (rename(tmp, path) != 0) { unlink(tmp); return TSDB_ERR_IO; }
    return TSDB_OK;
}

int tsdb_database_create(tsdb_catalog_t *c, const tsdb_database_t *db) {
    if (!c || !db || !db->name[0]) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    if (hmap_get(&c->databases, db->name)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_EXISTS;
    }
    tsdb_database_t *copy = malloc(sizeof(*copy));
    if (!copy) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    *copy = *db;
    if (copy->created_at == 0) copy->created_at = (tsdb_ts_t)tsdb_now_ns();
    int rc = log_database(c, '+', copy);
    if (rc != TSDB_OK) { free(copy); pthread_mutex_unlock(&c->lock); return rc; }
    hmap_put(&c->databases, copy->name, copy);
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_database_get(tsdb_catalog_t *c, const char *name, tsdb_database_t *out) {
    if (!c || !name || !out) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    tsdb_database_t *d = (tsdb_database_t *)hmap_get(&c->databases, name);
    if (!d) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    *out = *d;
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_database_exists(tsdb_catalog_t *c, const char *name) {
    if (!c || !name) return 0;
    pthread_mutex_lock(&c->lock);
    int yes = (hmap_get(&c->databases, name) != NULL);
    pthread_mutex_unlock(&c->lock);
    return yes;
}

int tsdb_database_drop(tsdb_catalog_t *c, const char *name) {
    if (!c || !name) return TSDB_ERR_INVAL;
    /* System database is forever — callers receive a dedicated rc so
     * the executor can surface a distinct error message. */
    if (strcmp(name, TSDB_SYSDB_NAME) == 0) return TSDB_ERR_PERMISSION;
    pthread_mutex_lock(&c->lock);
    tsdb_database_t *d = (tsdb_database_t *)hmap_get(&c->databases, name);
    if (!d) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    if (d->protected_flag) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_PERMISSION;
    }
    int rc = log_database_drop(c, name);
    if (rc != TSDB_OK) { pthread_mutex_unlock(&c->lock); return rc; }
    hmap_del(&c->databases, name);
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_database_list(tsdb_catalog_t *c,
                        tsdb_database_t **out_arr, size_t *out_n)
{
    if (!c || !out_arr || !out_n) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    size_t n = c->databases.size;
    tsdb_database_t *arr = n ? malloc(n * sizeof(*arr)) : NULL;
    if (n && !arr) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    size_t k = 0;
    for (size_t i = 0; i < c->databases.cap; i++) {
        if (c->databases.buckets[i].key) {
            arr[k++] = *(tsdb_database_t *)c->databases.buckets[i].val;
        }
    }
    *out_arr = arr;
    *out_n = k;
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

void tsdb_database_list_free(tsdb_database_t *arr) { free(arr); }

/* ---- Group log (continues) ---------------------------------------- */

/* Write a group record to groups.log. '+' for create, '-' for drop.
 *
 * V1 shape:  + name region retention codec replica tags created_at
 * V2 shape:  + name region retention codec replica tags created_at database
 *
 * We always write V2; readers that see 8 fields assume empty database.
 */
static int log_group(tsdb_catalog_t *c, char op, const tsdb_group_t *g) {
    char enc_name[128], enc_region[96], enc_profile[96], enc_tags[1600];
    char enc_db[128];
    pct_encode(g->name, enc_name, sizeof(enc_name));
    pct_encode(g->region, enc_region, sizeof(enc_region));
    pct_encode(g->codec_profile, enc_profile, sizeof(enc_profile));
    pct_encode(g->tags, enc_tags, sizeof(enc_tags));
    pct_encode(g->database, enc_db, sizeof(enc_db));

    int n = fprintf(c->groups_log,
        "%c\t%s\t%s\t%lld\t%s\t%d\t%s\t%lld\t%s\n",
        op,
        enc_name, enc_region,
        (long long)g->retention_ns,
        enc_profile,
        g->replica_factor,
        enc_tags,
        (long long)g->created_at,
        enc_db);
    if (n < 0) return TSDB_ERR_IO;
    if (fflush(c->groups_log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_group_drop(tsdb_catalog_t *c, const char *name) {
    char enc_name[128];
    pct_encode(name, enc_name, sizeof(enc_name));
    int n = fprintf(c->groups_log, "-\t%s\n", enc_name);
    if (n < 0) return TSDB_ERR_IO;
    if (fflush(c->groups_log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

/* Write a device record. op: '+' create, '-' drop, '~' update */
static int log_device(tsdb_catalog_t *c, char op, const tsdb_device_t *d) {
    char enc_group[192], enc_id[384], enc_type[96], enc_loc[384], enc_tags[1600];
    pct_encode(d->group, enc_group, sizeof(enc_group));
    pct_encode(d->id, enc_id, sizeof(enc_id));
    pct_encode(d->type, enc_type, sizeof(enc_type));
    pct_encode(d->location, enc_loc, sizeof(enc_loc));
    pct_encode(d->tags, enc_tags, sizeof(enc_tags));

    int n = fprintf(c->devices_log,
        "%c\t%s\t%s\t%s\t%s\t%s\t%lld\t%lld\t%d\n",
        op,
        enc_group, enc_id, enc_type, enc_loc, enc_tags,
        (long long)d->first_seen,
        (long long)d->last_seen,
        d->status);
    if (n < 0) return TSDB_ERR_IO;
    if (fflush(c->devices_log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_device_drop(tsdb_catalog_t *c, const char *group, const char *id) {
    char enc_group[192], enc_id[384];
    pct_encode(group, enc_group, sizeof(enc_group));
    pct_encode(id, enc_id, sizeof(enc_id));
    int n = fprintf(c->devices_log, "-\t%s\t%s\n", enc_group, enc_id);
    if (n < 0) return TSDB_ERR_IO;
    if (fflush(c->devices_log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_device_update(tsdb_catalog_t *c, const char *group,
                              const char *id, tsdb_ts_t ts) {
    char enc_group[192], enc_id[384];
    pct_encode(group, enc_group, sizeof(enc_group));
    pct_encode(id, enc_id, sizeof(enc_id));
    int n = fprintf(c->devices_log, "~\t%s\t%s\t%lld\n",
                    enc_group, enc_id, (long long)ts);
    if (n < 0) return TSDB_ERR_IO;
    if (fflush(c->devices_log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

/* ---- Log parsing helpers ------------------------------------------------- */

/* Split line by TAB into up to max_fields fields. Returns count. */
static int split_tab(char *line, char **fields, int max_fields) {
    int n = 0;
    char *p = line;
    while (n < max_fields) {
        fields[n++] = p;
        p = strchr(p, '\t');
        if (!p) break;
        *p++ = '\0';
    }
    return n;
}

/* Trim trailing newline. */
static void chomp(char *s) {
    size_t n = strlen(s);
    while (n > 0 && (s[n-1] == '\n' || s[n-1] == '\r')) s[--n] = '\0';
}



/* Ensure group_entry for given group exists in device_index. */
static group_entry_t *ensure_group_entry(tsdb_catalog_t *c, const char *group) {
    group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, group);
    if (ge) return ge;
    ge = calloc(1, sizeof(group_entry_t));
    if (!ge) return NULL;
    if (hmap_init(&ge->devices) != 0) { free(ge); return NULL; }
    if (hmap_put(&c->device_index, group, ge) != 0) {
        hmap_destroy(&ge->devices);
        free(ge);
        return NULL;
    }
    return ge;
}

/* ---- Replay functions ---------------------------------------------------- */

static int replay_groups(tsdb_catalog_t *c, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return TSDB_OK; /* file may not exist yet */

    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        chomp(line);
        if (!line[0]) continue;

        char *fields[16];
        char linecopy[4096];
        strncpy(linecopy, line, sizeof(linecopy) - 1);
        linecopy[sizeof(linecopy) - 1] = '\0';
        int nf = split_tab(linecopy, fields, 16);
        if (nf < 1) continue;

        char op = fields[0][0];
        if (op == '+' && nf >= 8) {
            tsdb_group_t *g = calloc(1, sizeof(tsdb_group_t));
            if (!g) { fclose(f); return TSDB_ERR_NOMEM; }

            /* Decode each field */
            char name[64], region[32], profile[32], tags[512], dbn[64];
            pct_decode(fields[1], name, sizeof(name));
            pct_decode(fields[2], region, sizeof(region));
            pct_decode(fields[4], profile, sizeof(profile));
            pct_decode(fields[6], tags, sizeof(tags));
            if (nf >= 9) pct_decode(fields[8], dbn, sizeof(dbn));
            else         dbn[0] = '\0';

            strncpy(g->name, name, sizeof(g->name) - 1);
            strncpy(g->region, region, sizeof(g->region) - 1);
            g->retention_ns = (int64_t)strtoll(fields[3], NULL, 10);
            strncpy(g->codec_profile, profile, sizeof(g->codec_profile) - 1);
            g->replica_factor = atoi(fields[5]);
            strncpy(g->tags, tags, sizeof(g->tags) - 1);
            g->created_at = (tsdb_ts_t)strtoll(fields[7], NULL, 10);
            strncpy(g->database, dbn, sizeof(g->database) - 1);

            hmap_put(&c->groups, g->name, g);
            /* ensure device index entry */
            ensure_group_entry(c, g->name);

        } else if (op == '-' && nf >= 2) {
            char name[64];
            pct_decode(fields[1], name, sizeof(name));
            /* Also remove all devices for this group from memory index. */
            group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, name);
            if (ge) {
                hmap_free_values(&ge->devices);
                hmap_destroy(&ge->devices);
                free(ge);
                /* Use nofreeval since we already freed ge above. */
                hmap_del_nofreeval(&c->device_index, name);
            }
            hmap_del(&c->groups, name);
        }
    }
    fclose(f);
    return TSDB_OK;
}

static int replay_devices(tsdb_catalog_t *c, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return TSDB_OK;

    char line[8192];
    while (fgets(line, sizeof(line), f)) {
        chomp(line);
        if (!line[0]) continue;

        char *fields[16];
        char linecopy[8192];
        strncpy(linecopy, line, sizeof(linecopy) - 1);
        linecopy[sizeof(linecopy) - 1] = '\0';
        int nf = split_tab(linecopy, fields, 16);
        if (nf < 1) continue;

        char op = fields[0][0];

        if (op == '+' && nf >= 9) {
            tsdb_device_t *d = calloc(1, sizeof(tsdb_device_t));
            if (!d) { fclose(f); return TSDB_ERR_NOMEM; }

            char grp[64], id[128], type[32], loc[128], tags[512];
            pct_decode(fields[1], grp, sizeof(grp));
            pct_decode(fields[2], id, sizeof(id));
            pct_decode(fields[3], type, sizeof(type));
            pct_decode(fields[4], loc, sizeof(loc));
            pct_decode(fields[5], tags, sizeof(tags));

            strncpy(d->group, grp, sizeof(d->group) - 1);
            strncpy(d->id, id, sizeof(d->id) - 1);
            strncpy(d->type, type, sizeof(d->type) - 1);
            strncpy(d->location, loc, sizeof(d->location) - 1);
            strncpy(d->tags, tags, sizeof(d->tags) - 1);
            d->first_seen = (tsdb_ts_t)strtoll(fields[6], NULL, 10);
            d->last_seen  = (tsdb_ts_t)strtoll(fields[7], NULL, 10);
            d->status     = atoi(fields[8]);

            group_entry_t *ge = ensure_group_entry(c, d->group);
            if (!ge) { free(d); fclose(f); return TSDB_ERR_NOMEM; }
            hmap_put(&ge->devices, d->id, d);

        } else if (op == '-' && nf >= 3) {
            char grp[64], id[128];
            pct_decode(fields[1], grp, sizeof(grp));
            pct_decode(fields[2], id, sizeof(id));

            group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, grp);
            if (ge) hmap_del(&ge->devices, id);

        } else if (op == '~' && nf >= 4) {
            char grp[64], id[128];
            pct_decode(fields[1], grp, sizeof(grp));
            pct_decode(fields[2], id, sizeof(id));
            tsdb_ts_t ts = (tsdb_ts_t)strtoll(fields[3], NULL, 10);

            group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, grp);
            if (ge) {
                tsdb_device_t *d = (tsdb_device_t *)hmap_get(&ge->devices, id);
                if (d) d->last_seen = ts;
            }
        }
    }
    fclose(f);
    return TSDB_OK;
}

/* ---- tsdb_catalog_open / close ------------------------------------------ */

int tsdb_catalog_open(const char *data_dir, tsdb_catalog_t **out) {
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    tsdb_catalog_t *c = calloc(1, sizeof(*c));
    if (!c) return TSDB_ERR_NOMEM;

    snprintf(c->data_dir, sizeof(c->data_dir), "%s", data_dir);
    snprintf(c->cat_dir, sizeof(c->cat_dir), "%s/catalog", data_dir);

    if (tsdb_mkdir_p(c->cat_dir) < 0) {
        free(c);
        return TSDB_ERR_IO;
    }

    pthread_mutex_init(&c->lock, NULL);

    if (hmap_init(&c->databases) != 0
        || hmap_init(&c->groups) != 0 || hmap_init(&c->device_index) != 0
        || hmap_init(&c->stables) != 0 || hmap_init(&c->child_tables) != 0) {
        free(c);
        return TSDB_ERR_NOMEM;
    }

    /* Replay existing logs. */
    char databases_path[4096], groups_path[4096], devices_path[4096];
    char stables_path[4096], children_path[4096];
    snprintf(databases_path,sizeof(databases_path),"%s/databases.log",    c->cat_dir);
    snprintf(groups_path,   sizeof(groups_path),   "%s/groups.log",       c->cat_dir);
    snprintf(devices_path,  sizeof(devices_path),  "%s/devices.log",      c->cat_dir);
    snprintf(stables_path,  sizeof(stables_path),  "%s/stables.log",      c->cat_dir);
    snprintf(children_path, sizeof(children_path), "%s/child_tables.log", c->cat_dir);

    int rc = replay_databases(c, databases_path);
    if (rc != TSDB_OK) { free(c); return rc; }
    rc = replay_groups(c, groups_path);
    if (rc != TSDB_OK) { free(c); return rc; }
    rc = replay_devices(c, devices_path);
    if (rc != TSDB_OK) { free(c); return rc; }
    /* Stable/child replays declared below. */
    extern int tsdb_catalog_replay_stables(tsdb_catalog_t *c, const char *path);
    extern int tsdb_catalog_replay_children(tsdb_catalog_t *c, const char *path);
    rc = tsdb_catalog_replay_stables(c, stables_path);
    if (rc != TSDB_OK) { free(c); return rc; }
    rc = tsdb_catalog_replay_children(c, children_path);
    if (rc != TSDB_OK) { free(c); return rc; }

    /* Compact any log file that's grown beyond the threshold.  Runs
     * after replay (so the in-memory state already reflects every
     * surviving entry) and before the live append handles open below
     * (so they land on the freshly compacted file).  The threshold
     * keeps fresh installs untouched and only kicks in after enough
     * churn that bloat is measurable.  Errors are logged-and-ignored:
     * the original log is still intact via the temp+rename pattern. */
    extern int tsdb_catalog_compact_stables(tsdb_catalog_t *, const char *);
    extern int tsdb_catalog_compact_children(tsdb_catalog_t *, const char *);
    if (file_size_or_zero(databases_path) > CATALOG_COMPACT_MIN_BYTES)
        (void)compact_databases(c, databases_path);
    if (file_size_or_zero(groups_path) > CATALOG_COMPACT_MIN_BYTES)
        (void)compact_groups(c, groups_path);
    if (file_size_or_zero(stables_path) > CATALOG_COMPACT_MIN_BYTES)
        (void)tsdb_catalog_compact_stables(c, stables_path);
    if (file_size_or_zero(children_path) > CATALOG_COMPACT_MIN_BYTES)
        (void)tsdb_catalog_compact_children(c, children_path);

    /* Open logs for appending. */
    c->databases_log = fopen(databases_path, "a");
    if (!c->databases_log) { free(c); return TSDB_ERR_IO; }
    c->groups_log   = fopen(groups_path,   "a");
    if (!c->groups_log) { fclose(c->databases_log); free(c); return TSDB_ERR_IO; }
    c->devices_log  = fopen(devices_path,  "a");
    if (!c->devices_log) { fclose(c->databases_log); fclose(c->groups_log); free(c); return TSDB_ERR_IO; }
    c->stables_log  = fopen(stables_path,  "a");
    if (!c->stables_log) { fclose(c->databases_log); fclose(c->groups_log); fclose(c->devices_log); free(c); return TSDB_ERR_IO; }
    c->children_log = fopen(children_path, "a");
    if (!c->children_log) { fclose(c->databases_log); fclose(c->groups_log); fclose(c->devices_log); fclose(c->stables_log); free(c); return TSDB_ERR_IO; }

    /* Auto-bootstrap sysdb on first open.  It's always protected, so
     * DROP DATABASE sysdb is rejected downstream. */
    if (!hmap_get(&c->databases, TSDB_SYSDB_NAME)) {
        tsdb_database_t sysdb;
        memset(&sysdb, 0, sizeof(sysdb));
        snprintf(sysdb.name, sizeof(sysdb.name), "%s", TSDB_SYSDB_NAME);
        snprintf(sysdb.description, sizeof(sysdb.description),
                 "system database — cluster state, users, RBAC, load "
                 "(read-only; cannot be dropped)");
        sysdb.created_at     = (tsdb_ts_t)tsdb_now_ns();
        sysdb.protected_flag = 1;
        tsdb_database_t *copy = malloc(sizeof(*copy));
        if (copy) {
            *copy = sysdb;
            if (log_database(c, '+', copy) == TSDB_OK) {
                hmap_put(&c->databases, copy->name, copy);
            } else {
                free(copy);
            }
        }
    }

    *out = c;
    return TSDB_OK;
}

void tsdb_catalog_close(tsdb_catalog_t *c) {
    if (!c) return;

    pthread_mutex_lock(&c->lock);

    if (c->databases_log) fclose(c->databases_log);
    if (c->groups_log)    fclose(c->groups_log);
    if (c->devices_log)   fclose(c->devices_log);
    if (c->stables_log)   fclose(c->stables_log);
    if (c->children_log)  fclose(c->children_log);

    /* Free stables + child_tables hmaps (values are malloc'd). */
    hmap_free_values(&c->stables);      free(c->stables.buckets);
    hmap_free_values(&c->child_tables); free(c->child_tables.buckets);

    /* Free device_index: group_entry_t entries. */
    for (size_t i = 0; i < c->device_index.cap; i++) {
        hmap_entry_t *e = &c->device_index.buckets[i];
        if (!e->key) continue;
        group_entry_t *ge = (group_entry_t *)e->val;
        if (ge) {
            /* free devices inside this group */
            hmap_free_values(&ge->devices);
            hmap_destroy(&ge->devices);
            free(ge);
            e->val = NULL;
        }
        free(e->key);
        e->key = NULL;
    }
    free(c->device_index.buckets);

    /* Free groups map. */
    hmap_free_values(&c->groups);
    free(c->groups.buckets);

    /* Free databases map. */
    hmap_free_values(&c->databases);
    free(c->databases.buckets);

    pthread_mutex_unlock(&c->lock);
    pthread_mutex_destroy(&c->lock);
    free(c);
}

/* ---- tsdb_group_* -------------------------------------------------------- */

/* See compact_databases — same atomic rewrite pattern. */
static int compact_groups(tsdb_catalog_t *c, const char *path) {
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    FILE *new_log = fopen(tmp, "w");
    if (!new_log) return TSDB_ERR_IO;

    FILE *saved = c->groups_log;
    c->groups_log = new_log;
    int rc = TSDB_OK;
    for (size_t i = 0; i < c->groups.cap; i++) {
        if (!c->groups.buckets[i].key) continue;
        tsdb_group_t *g = (tsdb_group_t *)c->groups.buckets[i].val;
        if (log_group(c, '+', g) != TSDB_OK) { rc = TSDB_ERR_IO; break; }
    }
    fflush(new_log);
    fsync(fileno(new_log));
    fclose(new_log);
    c->groups_log = saved;
    if (rc != TSDB_OK) { unlink(tmp); return rc; }
    if (rename(tmp, path) != 0) { unlink(tmp); return TSDB_ERR_IO; }
    return TSDB_OK;
}

int tsdb_group_create(tsdb_catalog_t *c, const tsdb_group_t *g) {
    if (!c || !g || !g->name[0]) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&c->lock);

    if (hmap_get(&c->groups, g->name)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_EXISTS;
    }

    tsdb_group_t *copy = malloc(sizeof(*copy));
    if (!copy) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    *copy = *g;
    if (copy->created_at == 0) copy->created_at = tsdb_now_ns();
    if (copy->replica_factor <= 0) copy->replica_factor = 3;

    if (hmap_put(&c->groups, copy->name, copy) != 0) {
        free(copy);
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_NOMEM;
    }
    /* Ensure device index entry. */
    ensure_group_entry(c, copy->name);

    int rc = log_group(c, '+', copy);
    pthread_mutex_unlock(&c->lock);
    return rc;
}

int tsdb_group_get(tsdb_catalog_t *c, const char *name, tsdb_group_t *out) {
    if (!c || !name || !out) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    tsdb_group_t *g = (tsdb_group_t *)hmap_get(&c->groups, name);
    if (!g) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    *out = *g;
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_group_list(tsdb_catalog_t *c, tsdb_group_t **out_arr, size_t *out_n) {
    if (!c || !out_arr || !out_n) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);

    size_t n = c->groups.size;
    if (n == 0) {
        *out_arr = NULL;
        *out_n = 0;
        pthread_mutex_unlock(&c->lock);
        return TSDB_OK;
    }

    tsdb_group_t *arr = malloc(n * sizeof(tsdb_group_t));
    if (!arr) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }

    size_t idx = 0;
    for (size_t i = 0; i < c->groups.cap; i++) {
        if (c->groups.buckets[i].key) {
            arr[idx++] = *(tsdb_group_t *)c->groups.buckets[i].val;
        }
    }
    *out_arr = arr;
    *out_n = idx;
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

void tsdb_group_list_free(tsdb_group_t *arr) {
    free(arr);
}

int tsdb_group_drop(tsdb_catalog_t *c, const char *name) {
    if (!c || !name) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);

    if (!hmap_get(&c->groups, name)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_NOTFOUND;
    }

    /* Cascade: write tombstones for all devices in this group first. */
    group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, name);
    if (ge) {
        for (size_t i = 0; i < ge->devices.cap; i++) {
            if (ge->devices.buckets[i].key) {
                tsdb_device_t *d = (tsdb_device_t *)ge->devices.buckets[i].val;
                log_device_drop(c, name, d->id);
            }
        }
        hmap_free_values(&ge->devices);
        hmap_destroy(&ge->devices);
        free(ge);
        /* Use nofreeval since we already freed ge above. */
        hmap_del_nofreeval(&c->device_index, name);
    }

    /* Log group drop tombstone. */
    log_group_drop(c, name);
    hmap_del(&c->groups, name);

    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

/* ---- tsdb_device_* ------------------------------------------------------- */

int tsdb_device_create(tsdb_catalog_t *c, const tsdb_device_t *d) {
    if (!c || !d || !d->group[0] || !d->id[0]) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&c->lock);

    /* Group must exist. */
    if (!hmap_get(&c->groups, d->group)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_NOTFOUND;
    }

    group_entry_t *ge = ensure_group_entry(c, d->group);
    if (!ge) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }

    if (hmap_get(&ge->devices, d->id)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_EXISTS;
    }

    tsdb_device_t *copy = malloc(sizeof(*copy));
    if (!copy) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    *copy = *d;
    tsdb_ts_t now = tsdb_now_ns();
    if (copy->first_seen == 0) copy->first_seen = now;
    if (copy->last_seen == 0) copy->last_seen = now;

    if (hmap_put(&ge->devices, copy->id, copy) != 0) {
        free(copy);
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_NOMEM;
    }

    int rc = log_device(c, '+', copy);
    pthread_mutex_unlock(&c->lock);
    return rc;
}

int tsdb_device_get(tsdb_catalog_t *c, const char *group, const char *id,
                    tsdb_device_t *out) {
    if (!c || !group || !id || !out) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, group);
    if (!ge) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    tsdb_device_t *d = (tsdb_device_t *)hmap_get(&ge->devices, id);
    if (!d) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    *out = *d;
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_device_list(tsdb_catalog_t *c, const char *group,
                     tsdb_device_t **out_arr, size_t *out_n) {
    if (!c || !out_arr || !out_n) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&c->lock);

    if (group) {
        /* List devices in a specific group. */
        group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, group);
        if (!ge || ge->devices.size == 0) {
            *out_arr = NULL; *out_n = 0;
            pthread_mutex_unlock(&c->lock);
            return TSDB_OK;
        }
        size_t n = ge->devices.size;
        tsdb_device_t *arr = malloc(n * sizeof(tsdb_device_t));
        if (!arr) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
        size_t idx = 0;
        for (size_t i = 0; i < ge->devices.cap; i++) {
            if (ge->devices.buckets[i].key)
                arr[idx++] = *(tsdb_device_t *)ge->devices.buckets[i].val;
        }
        *out_arr = arr; *out_n = idx;
        pthread_mutex_unlock(&c->lock);
        return TSDB_OK;
    } else {
        /* List all devices across all groups. */
        size_t total = 0;
        for (size_t i = 0; i < c->device_index.cap; i++) {
            if (c->device_index.buckets[i].key) {
                group_entry_t *ge = (group_entry_t *)c->device_index.buckets[i].val;
                if (ge) total += ge->devices.size;
            }
        }
        if (total == 0) { *out_arr = NULL; *out_n = 0; pthread_mutex_unlock(&c->lock); return TSDB_OK; }
        tsdb_device_t *arr = malloc(total * sizeof(tsdb_device_t));
        if (!arr) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
        size_t idx = 0;
        for (size_t i = 0; i < c->device_index.cap; i++) {
            if (!c->device_index.buckets[i].key) continue;
            group_entry_t *ge = (group_entry_t *)c->device_index.buckets[i].val;
            if (!ge) continue;
            for (size_t j = 0; j < ge->devices.cap; j++) {
                if (ge->devices.buckets[j].key)
                    arr[idx++] = *(tsdb_device_t *)ge->devices.buckets[j].val;
            }
        }
        *out_arr = arr; *out_n = idx;
        pthread_mutex_unlock(&c->lock);
        return TSDB_OK;
    }
}

void tsdb_device_list_free(tsdb_device_t *arr) {
    free(arr);
}

int tsdb_device_drop(tsdb_catalog_t *c, const char *group, const char *id) {
    if (!c || !group || !id) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);

    group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, group);
    if (!ge || !hmap_get(&ge->devices, id)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_NOTFOUND;
    }

    log_device_drop(c, group, id);
    hmap_del(&ge->devices, id);

    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_device_update_last_seen(tsdb_catalog_t *c, const char *group,
                                  const char *id, tsdb_ts_t ts) {
    if (!c || !group || !id) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);

    group_entry_t *ge = (group_entry_t *)hmap_get(&c->device_index, group);
    if (!ge) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    tsdb_device_t *d = (tsdb_device_t *)hmap_get(&ge->devices, id);
    if (!d) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }

    d->last_seen = ts;
    int rc = log_device_update(c, group, id, ts);
    pthread_mutex_unlock(&c->lock);
    return rc;
}
