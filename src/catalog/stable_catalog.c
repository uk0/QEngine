/* stable_catalog.c — STable + Child-Table catalog extensions.
 *
 * Persists into <data_dir>/catalog/stables.log and child_tables.log as
 * append-only records. Format is compact + forward-compatible:
 *
 *   STABLE:   +stable <name> <ncols> <ts_col_idx> <ntags> <created_at> \
 *                    <col1_name>:<col1_type> ... <tag1_name>:<tag1_type> ...
 *             -stable <name>
 *
 *   CHILD:    +child <name> <stable> <ntags> <created_at> \
 *                    <tag1_type>:<tag1_val> ...
 *             -child <name>
 *
 * All tokens tab-separated; values percent-encoded where needed.
 *
 * This file plugs into the tsdb_catalog_t machinery defined in catalog.c.
 * It declares no new hmap infrastructure — it reuses the functions already
 * in catalog.c via shared-state access through struct layout-compatible
 * typedefs.
 */

#include "stable.h"
#include "group.h"
#include "device.h"
#include "../../include/tsdb.h"

#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Shared state from catalog.c ──────────────────────────────────────── */

typedef struct hmap_entry {
    char *key;
    void *val;
} hmap_entry_t;

typedef struct hmap {
    hmap_entry_t *buckets;
    size_t        cap;
    size_t        size;
} hmap_t;

typedef struct { hmap_t devices; } group_entry_t;

struct tsdb_catalog {
    char          data_dir[4096];
    char          cat_dir[4096];
    pthread_mutex_t lock;
    hmap_t         groups;
    hmap_t         device_index;
    hmap_t         stables;
    hmap_t         child_tables;
    FILE          *groups_log;
    FILE          *devices_log;
    FILE          *stables_log;
    FILE          *children_log;
};

/* Forward-decl helpers exported from catalog.c.
 * catalog.c kept them static; we copy the minimal ones here. */

static size_t sc_hash(const char *s) {
    size_t h = 14695981039346656037ULL;
    while (*s) { h ^= (unsigned char)*s++; h *= 1099511628211ULL; }
    return h;
}

static int sc_hmap_grow(hmap_t *m) {
    size_t newcap = m->cap * 2;
    hmap_entry_t *nb = calloc(newcap, sizeof(hmap_entry_t));
    if (!nb) return -1;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->buckets[i].key) {
            size_t idx = sc_hash(m->buckets[i].key) & (newcap - 1);
            while (nb[idx].key) idx = (idx + 1) & (newcap - 1);
            nb[idx] = m->buckets[i];
        }
    }
    free(m->buckets);
    m->buckets = nb; m->cap = newcap;
    return 0;
}

static int sc_hmap_put(hmap_t *m, const char *key, void *val) {
    if (m->size * 10 >= m->cap * 7) if (sc_hmap_grow(m) != 0) return -1;
    size_t idx = sc_hash(key) & (m->cap - 1);
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

static void *sc_hmap_get(hmap_t *m, const char *key) {
    if (!m->cap) return NULL;
    size_t idx = sc_hash(key) & (m->cap - 1);
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0) return m->buckets[idx].val;
        idx = (idx + 1) & (m->cap - 1);
    }
    return NULL;
}

static int sc_hmap_del(hmap_t *m, const char *key) {
    if (!m->cap) return 0;
    size_t idx = sc_hash(key) & (m->cap - 1);
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0) {
            free(m->buckets[idx].key);
            free(m->buckets[idx].val);
            m->buckets[idx].key = NULL;
            m->buckets[idx].val = NULL;
            m->size--;
            size_t j = (idx + 1) & (m->cap - 1);
            while (m->buckets[j].key) {
                char *k = m->buckets[j].key;
                void *v = m->buckets[j].val;
                m->buckets[j].key = NULL;
                m->buckets[j].val = NULL;
                size_t h = sc_hash(k) & (m->cap - 1);
                while (m->buckets[h].key) h = (h + 1) & (m->cap - 1);
                m->buckets[h].key = k;
                m->buckets[h].val = v;
                j = (j + 1) & (m->cap - 1);
            }
            return 1;
        }
        idx = (idx + 1) & (m->cap - 1);
    }
    return 0;
}

/* ── Simple tab-separated line parser (no pct-decode needed for our use) ─ */

static int split_tab(char *line, char **out, int cap) {
    int n = 0;
    char *p = line;
    while (n < cap && *p) {
        out[n++] = p;
        while (*p && *p != '\t') p++;
        if (*p) { *p = 0; p++; }
    }
    return n;
}

/* ── Persistence ──────────────────────────────────────────────────────── */

static int stable_log_write(tsdb_catalog_t *c, char op, const tsdb_stable_t *s) {
    if (!c->stables_log) return TSDB_OK;
    if (op == '-') {
        fprintf(c->stables_log, "-stable\t%s\n", s->name);
    } else {
        fprintf(c->stables_log,
                "+stable\t%s\t%d\t%d\t%d\t%lld",
                s->name, s->ncols, s->ts_col_idx, s->ntag_cols,
                (long long)s->created_at);
        for (int i = 0; i < s->ncols; i++)
            fprintf(c->stables_log, "\t%s:%d", s->cols[i].name, (int)s->cols[i].type);
        for (int i = 0; i < s->ntag_cols; i++)
            fprintf(c->stables_log, "\t%s:%d", s->tag_cols[i].name, (int)s->tag_cols[i].type);
        fprintf(c->stables_log, "\n");
    }
    fflush(c->stables_log);
    return TSDB_OK;
}

static int child_log_write(tsdb_catalog_t *c, char op, const tsdb_child_table_t *ct) {
    if (!c->children_log) return TSDB_OK;
    if (op == '-') {
        fprintf(c->children_log, "-child\t%s\n", ct->name);
    } else {
        fprintf(c->children_log,
                "+child\t%s\t%s\t%d\t%lld",
                ct->name, ct->stable_name, ct->ntags, (long long)ct->created_at);
        for (int i = 0; i < ct->ntags; i++) {
            switch (ct->tags[i].type) {
            case TSDB_TYPE_TIMESTAMP:
            case TSDB_TYPE_INT64:
                fprintf(c->children_log, "\t%d:%lld",
                        (int)ct->tags[i].type, (long long)ct->tags[i].v.i);
                break;
            case TSDB_TYPE_FLOAT64:
                fprintf(c->children_log, "\t%d:%g",
                        (int)ct->tags[i].type, ct->tags[i].v.f);
                break;
            case TSDB_TYPE_SYMBOL:
                fprintf(c->children_log, "\t%d:%s",
                        (int)ct->tags[i].type, ct->tags[i].v.s);
                break;
            default:
                fprintf(c->children_log, "\t0:");
            }
        }
        fprintf(c->children_log, "\n");
    }
    fflush(c->children_log);
    return TSDB_OK;
}

/* ── Replay (called from catalog_open via extern decl) ────────────────── */

int tsdb_catalog_replay_stables(tsdb_catalog_t *c, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return TSDB_OK;
    char line[8192];
    while (fgets(line, sizeof(line), f)) {
        size_t n = strlen(line);
        while (n > 0 && (line[n-1] == '\n' || line[n-1] == '\r')) line[--n] = 0;
        if (!line[0]) continue;

        char *fields[128];
        int nf = split_tab(line, fields, 128);
        if (nf < 2) continue;

        if (!strcmp(fields[0], "+stable") && nf >= 6) {
            tsdb_stable_t *s = calloc(1, sizeof(*s));
            if (!s) { fclose(f); return TSDB_ERR_NOMEM; }
            snprintf(s->name, sizeof(s->name), "%s", fields[1]);
            s->ncols       = atoi(fields[2]);
            s->ts_col_idx  = atoi(fields[3]);
            s->ntag_cols   = atoi(fields[4]);
            s->created_at  = strtoll(fields[5], NULL, 10);
            int fi = 6;
            for (int i = 0; i < s->ncols && fi < nf; i++, fi++) {
                char *colon = strchr(fields[fi], ':');
                if (!colon) { free(s); fclose(f); return TSDB_ERR_CORRUPT; }
                *colon = 0;
                snprintf(s->cols[i].name, sizeof(s->cols[i].name), "%s", fields[fi]);
                s->cols[i].type = (tsdb_type_t)atoi(colon + 1);
            }
            for (int i = 0; i < s->ntag_cols && fi < nf; i++, fi++) {
                char *colon = strchr(fields[fi], ':');
                if (!colon) { free(s); fclose(f); return TSDB_ERR_CORRUPT; }
                *colon = 0;
                snprintf(s->tag_cols[i].name, sizeof(s->tag_cols[i].name), "%s", fields[fi]);
                s->tag_cols[i].type = (tsdb_type_t)atoi(colon + 1);
            }
            sc_hmap_put(&c->stables, s->name, s);
        } else if (!strcmp(fields[0], "-stable") && nf >= 2) {
            sc_hmap_del(&c->stables, fields[1]);
        }
    }
    fclose(f);
    return TSDB_OK;
}

int tsdb_catalog_replay_children(tsdb_catalog_t *c, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return TSDB_OK;
    char line[8192];
    while (fgets(line, sizeof(line), f)) {
        size_t n = strlen(line);
        while (n > 0 && (line[n-1] == '\n' || line[n-1] == '\r')) line[--n] = 0;
        if (!line[0]) continue;

        char *fields[128];
        int nf = split_tab(line, fields, 128);
        if (nf < 2) continue;

        if (!strcmp(fields[0], "+child") && nf >= 5) {
            tsdb_child_table_t *ct = calloc(1, sizeof(*ct));
            if (!ct) { fclose(f); return TSDB_ERR_NOMEM; }
            snprintf(ct->name,        sizeof(ct->name),        "%s", fields[1]);
            snprintf(ct->stable_name, sizeof(ct->stable_name), "%s", fields[2]);
            ct->ntags      = atoi(fields[3]);
            ct->created_at = strtoll(fields[4], NULL, 10);
            int fi = 5;
            for (int i = 0; i < ct->ntags && fi < nf; i++, fi++) {
                char *colon = strchr(fields[fi], ':');
                if (!colon) { free(ct); fclose(f); return TSDB_ERR_CORRUPT; }
                *colon = 0;
                ct->tags[i].type = (tsdb_type_t)atoi(fields[fi]);
                const char *v = colon + 1;
                switch (ct->tags[i].type) {
                case TSDB_TYPE_TIMESTAMP:
                case TSDB_TYPE_INT64:   ct->tags[i].v.i = strtoll(v, NULL, 10); break;
                case TSDB_TYPE_FLOAT64: ct->tags[i].v.f = strtod(v, NULL);      break;
                case TSDB_TYPE_SYMBOL:
                    snprintf(ct->tags[i].v.s, sizeof(ct->tags[i].v.s), "%s", v);
                    break;
                default: break;
                }
            }
            sc_hmap_put(&c->child_tables, ct->name, ct);
        } else if (!strcmp(fields[0], "-child") && nf >= 2) {
            sc_hmap_del(&c->child_tables, fields[1]);
        }
    }
    fclose(f);
    return TSDB_OK;
}

/* ── Public API ───────────────────────────────────────────────────────── */

int tsdb_stable_create(tsdb_catalog_t *c, const tsdb_stable_t *s) {
    if (!c || !s || !s->name[0]) return TSDB_ERR_INVAL;
    if (s->ncols <= 0 || s->ncols > TSDB_STABLE_MAX_COLS) return TSDB_ERR_INVAL;
    if (s->ntag_cols < 0 || s->ntag_cols > TSDB_STABLE_MAX_TAG_COLS) return TSDB_ERR_INVAL;
    if (s->ts_col_idx < 0 || s->ts_col_idx >= s->ncols) return TSDB_ERR_INVAL;
    if (s->cols[s->ts_col_idx].type != TSDB_TYPE_TIMESTAMP) return TSDB_ERR_SCHEMA;

    pthread_mutex_lock(&c->lock);
    if (sc_hmap_get(&c->stables, s->name)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_EXISTS;
    }
    tsdb_stable_t *dup = malloc(sizeof(*dup));
    if (!dup) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    memcpy(dup, s, sizeof(*dup));
    if (dup->created_at == 0) dup->created_at = tsdb_now_ns();
    sc_hmap_put(&c->stables, dup->name, dup);
    stable_log_write(c, '+', dup);
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_stable_get(tsdb_catalog_t *c, const char *name, tsdb_stable_t *out) {
    if (!c || !name || !out) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    tsdb_stable_t *s = sc_hmap_get(&c->stables, name);
    if (!s) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    memcpy(out, s, sizeof(*out));
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_stable_exists(tsdb_catalog_t *c, const char *name) {
    if (!c || !name) return 0;
    pthread_mutex_lock(&c->lock);
    int r = sc_hmap_get(&c->stables, name) != NULL;
    pthread_mutex_unlock(&c->lock);
    return r;
}

int tsdb_stable_drop(tsdb_catalog_t *c, const char *name) {
    if (!c || !name) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    tsdb_stable_t *s = sc_hmap_get(&c->stables, name);
    if (!s) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    tsdb_stable_t snap = *s;
    sc_hmap_del(&c->stables, name);
    stable_log_write(c, '-', &snap);

    /* Cascade: drop all child tables pointing at this stable. */
    for (size_t i = 0; i < c->child_tables.cap; ) {
        hmap_entry_t *e = &c->child_tables.buckets[i];
        if (e->key) {
            tsdb_child_table_t *ct = (tsdb_child_table_t *)e->val;
            if (ct && !strcmp(ct->stable_name, name)) {
                tsdb_child_table_t snap2 = *ct;
                char *k = strdup(ct->name);
                sc_hmap_del(&c->child_tables, k);
                child_log_write(c, '-', &snap2);
                free(k);
                continue;  /* re-read same slot; layout may have shifted */
            }
        }
        i++;
    }

    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_stable_list(tsdb_catalog_t *c, tsdb_stable_t **out_arr, size_t *out_n) {
    if (!c || !out_arr || !out_n) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    size_t n = c->stables.size;
    *out_n = n;
    *out_arr = NULL;
    if (n == 0) { pthread_mutex_unlock(&c->lock); return TSDB_OK; }
    tsdb_stable_t *arr = malloc(n * sizeof(tsdb_stable_t));
    if (!arr) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    size_t k = 0;
    for (size_t i = 0; i < c->stables.cap && k < n; i++) {
        if (c->stables.buckets[i].key) {
            memcpy(&arr[k++], c->stables.buckets[i].val, sizeof(tsdb_stable_t));
        }
    }
    pthread_mutex_unlock(&c->lock);
    *out_arr = arr;
    return TSDB_OK;
}

int tsdb_child_table_create(tsdb_catalog_t *c, const tsdb_child_table_t *ct) {
    if (!c || !ct || !ct->name[0] || !ct->stable_name[0]) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    tsdb_stable_t *st = sc_hmap_get(&c->stables, ct->stable_name);
    if (!st) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    if (ct->ntags != st->ntag_cols) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_SCHEMA; }
    if (sc_hmap_get(&c->child_tables, ct->name)) {
        pthread_mutex_unlock(&c->lock);
        return TSDB_ERR_EXISTS;
    }
    tsdb_child_table_t *dup = malloc(sizeof(*dup));
    if (!dup) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    memcpy(dup, ct, sizeof(*dup));
    if (dup->created_at == 0) dup->created_at = tsdb_now_ns();
    sc_hmap_put(&c->child_tables, dup->name, dup);
    child_log_write(c, '+', dup);
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_child_table_get(tsdb_catalog_t *c, const char *name, tsdb_child_table_t *out) {
    if (!c || !name || !out) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    tsdb_child_table_t *ct = sc_hmap_get(&c->child_tables, name);
    if (!ct) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    memcpy(out, ct, sizeof(*out));
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}

int tsdb_child_table_list(tsdb_catalog_t *c, const char *stable_name_filter,
                          tsdb_child_table_t **out_arr, size_t *out_n)
{
    if (!c || !out_arr || !out_n) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    size_t count = 0;
    for (size_t i = 0; i < c->child_tables.cap; i++) {
        if (!c->child_tables.buckets[i].key) continue;
        tsdb_child_table_t *ct = c->child_tables.buckets[i].val;
        if (!stable_name_filter || !strcmp(ct->stable_name, stable_name_filter))
            count++;
    }
    *out_n = count;
    *out_arr = NULL;
    if (count == 0) { pthread_mutex_unlock(&c->lock); return TSDB_OK; }
    tsdb_child_table_t *arr = malloc(count * sizeof(tsdb_child_table_t));
    if (!arr) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOMEM; }
    size_t k = 0;
    for (size_t i = 0; i < c->child_tables.cap && k < count; i++) {
        if (!c->child_tables.buckets[i].key) continue;
        tsdb_child_table_t *ct = c->child_tables.buckets[i].val;
        if (!stable_name_filter || !strcmp(ct->stable_name, stable_name_filter)) {
            memcpy(&arr[k++], ct, sizeof(tsdb_child_table_t));
        }
    }
    pthread_mutex_unlock(&c->lock);
    *out_arr = arr;
    return TSDB_OK;
}

int tsdb_child_table_drop(tsdb_catalog_t *c, const char *name) {
    if (!c || !name) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    tsdb_child_table_t *ct = sc_hmap_get(&c->child_tables, name);
    if (!ct) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    tsdb_child_table_t snap = *ct;
    sc_hmap_del(&c->child_tables, name);
    child_log_write(c, '-', &snap);
    pthread_mutex_unlock(&c->lock);
    return TSDB_OK;
}
