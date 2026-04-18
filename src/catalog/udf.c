/* udf.c — user-defined scalar function catalog implementation. */

#include "udf.h"

#include <dlfcn.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* ─── File-local helpers ─────────────────────────────────────────────────── */

extern int tsdb_mkdir_p(const char *path);

static int type_from_str(const char *s, tsdb_udf_type_t *out) {
    if (!strcasecmp(s, "TIMESTAMP")) { *out = TSDB_UDF_TYPE_TIMESTAMP; return 0; }
    if (!strcasecmp(s, "INT64"))     { *out = TSDB_UDF_TYPE_INT64;     return 0; }
    if (!strcasecmp(s, "FLOAT64"))   { *out = TSDB_UDF_TYPE_FLOAT64;   return 0; }
    return -1;
}

static const char *type_to_str(tsdb_udf_type_t t) {
    switch (t) {
    case TSDB_UDF_TYPE_TIMESTAMP: return "TIMESTAMP";
    case TSDB_UDF_TYPE_INT64:     return "INT64";
    case TSDB_UDF_TYPE_FLOAT64:   return "FLOAT64";
    }
    return "?";
}

/* Canonicalise a name to lower-case ASCII for stable comparisons. */
static void name_lower(const char *in, char *out, size_t cap) {
    size_t n = 0;
    for (; in[n] && n + 1 < cap; n++) {
        char c = in[n];
        if (c >= 'A' && c <= 'Z') c = (char)(c - 'A' + 'a');
        out[n] = c;
    }
    out[n] = 0;
}

/* ─── Builtin name collision guard ───────────────────────────────────────── */

int tsdb_udf_name_is_builtin(const char *name) {
    static const char *const BUILTINS[] = {
        /* aggregates */
        "sum", "avg", "min", "max", "count",
        "p50", "p90", "p99", "percentile", "stddev",
        "first", "last", "last_row", "twa",
        /* window fns */
        "diff", "derivative", "csum", "mavg", "interp",
        /* misc */
        "time_bucket",
        NULL,
    };
    for (int i = 0; BUILTINS[i]; i++)
        if (!strcasecmp(BUILTINS[i], name)) return 1;
    return 0;
}

/* ─── Catalog struct ─────────────────────────────────────────────────────── */

struct tsdb_udf_catalog {
    pthread_mutex_t   mu;

    tsdb_udf_entry_t  entries[TSDB_UDF_MAX_ENTRIES];
    int               n;

    char              log_path[TSDB_UDF_PATH_MAX];
};

/* ─── Append-only log writer ─────────────────────────────────────────────── */

static int log_append_create(tsdb_udf_catalog_t *c, const tsdb_udf_entry_t *e) {
    FILE *f = fopen(c->log_path, "ab");
    if (!f) return TSDB_ERR_IO;

    fprintf(f, "+fn\t%s\t%s\t%s\t%d\t",
            e->name, e->so_path, e->symbol, e->nargs);
    for (int i = 0; i < e->nargs; i++) {
        if (i) fputc(',', f);
        fputs(type_to_str(e->arg_types[i]), f);
    }
    fprintf(f, "\t%s\t%" PRIi64 "\n", type_to_str(e->ret_type),
            (int64_t)e->created_at);
    int rc = (fflush(f) == 0) ? TSDB_OK : TSDB_ERR_IO;
    fclose(f);
    return rc;
}

static int log_append_drop(tsdb_udf_catalog_t *c, const char *name) {
    FILE *f = fopen(c->log_path, "ab");
    if (!f) return TSDB_ERR_IO;
    fprintf(f, "-fn\t%s\n", name);
    int rc = (fflush(f) == 0) ? TSDB_OK : TSDB_ERR_IO;
    fclose(f);
    return rc;
}

/* ─── Log replay ─────────────────────────────────────────────────────────── */

/* Parse a single line of the catalog log. Lines starting with "+fn" add an
 * entry, "-fn" removes. Comments / blanks are ignored. Malformed lines abort
 * replay — if the catalog on disk is corrupt the caller sees the error at
 * open time rather than silently losing a function. */
static int replay_line(tsdb_udf_catalog_t *c, char *line) {
    if (!line || !line[0] || line[0] == '#' || line[0] == '\n') return TSDB_OK;

    char *tok = strtok(line, "\t\n");
    if (!tok) return TSDB_OK;

    if (!strcmp(tok, "+fn")) {
        if (c->n >= TSDB_UDF_MAX_ENTRIES) return TSDB_ERR_FULL;
        tsdb_udf_entry_t *e = &c->entries[c->n];
        memset(e, 0, sizeof(*e));

        const char *t_name   = strtok(NULL, "\t");
        const char *t_path   = strtok(NULL, "\t");
        const char *t_sym    = strtok(NULL, "\t");
        const char *t_nargs  = strtok(NULL, "\t");
        const char *t_types  = strtok(NULL, "\t");
        const char *t_ret    = strtok(NULL, "\t");
        const char *t_cts    = strtok(NULL, "\t\n");
        if (!t_name || !t_path || !t_sym || !t_nargs || !t_ret) return TSDB_ERR_CORRUPT;

        name_lower(t_name, e->name, sizeof(e->name));
        snprintf(e->so_path, sizeof(e->so_path), "%s", t_path);
        snprintf(e->symbol,  sizeof(e->symbol),  "%s", t_sym);
        e->nargs = atoi(t_nargs);
        if (e->nargs < 0 || e->nargs > TSDB_UDF_MAX_ARGS) return TSDB_ERR_CORRUPT;

        /* Parse comma-separated types. Empty string ⇒ zero-arg function. */
        if (e->nargs > 0) {
            if (!t_types) return TSDB_ERR_CORRUPT;
            char  buf[256];
            snprintf(buf, sizeof(buf), "%s", t_types);
            char *p = buf;
            int i = 0;
            for (char *s = strsep(&p, ","); s && i < e->nargs; s = strsep(&p, ","), i++) {
                if (type_from_str(s, &e->arg_types[i]) < 0) return TSDB_ERR_CORRUPT;
            }
            if (i != e->nargs) return TSDB_ERR_CORRUPT;
        }
        if (type_from_str(t_ret, &e->ret_type) < 0) return TSDB_ERR_CORRUPT;
        e->created_at = t_cts ? (tsdb_ts_t)atoll(t_cts) : 0;
        c->n++;
        return TSDB_OK;
    }

    if (!strcmp(tok, "-fn")) {
        const char *t_name = strtok(NULL, "\t\n");
        if (!t_name) return TSDB_ERR_CORRUPT;
        char canon[TSDB_UDF_NAME_MAX];
        name_lower(t_name, canon, sizeof(canon));
        for (int i = 0; i < c->n; i++) {
            if (!strcmp(c->entries[i].name, canon)) {
                if (c->entries[i].dl_handle) {
                    dlclose(c->entries[i].dl_handle);
                    c->entries[i].dl_handle = NULL;
                    c->entries[i].fn = NULL;
                }
                memmove(&c->entries[i], &c->entries[i + 1],
                        (size_t)(c->n - i - 1) * sizeof(c->entries[0]));
                c->n--;
                break;
            }
        }
        return TSDB_OK;
    }

    /* Unknown record type — ignore, stay forward-compatible. */
    return TSDB_OK;
}

/* ─── Open / close ───────────────────────────────────────────────────────── */

int tsdb_udf_catalog_open(const char *data_dir, tsdb_udf_catalog_t **out) {
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    tsdb_udf_catalog_t *c = calloc(1, sizeof(*c));
    if (!c) return TSDB_ERR_NOMEM;
    pthread_mutex_init(&c->mu, NULL);

    char dir[TSDB_UDF_PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/catalog", data_dir);
    if (tsdb_mkdir_p(dir) < 0) { free(c); return TSDB_ERR_IO; }

    snprintf(c->log_path, sizeof(c->log_path), "%s/functions.log", dir);

    FILE *f = fopen(c->log_path, "rb");
    if (f) {
        char buf[4096];
        while (fgets(buf, sizeof(buf), f)) {
            int rc = replay_line(c, buf);
            if (rc != TSDB_OK) {
                fclose(f);
                pthread_mutex_destroy(&c->mu);
                free(c);
                return rc;
            }
        }
        fclose(f);
    }

    *out = c;
    return TSDB_OK;
}

void tsdb_udf_catalog_close(tsdb_udf_catalog_t *c) {
    if (!c) return;
    pthread_mutex_lock(&c->mu);
    for (int i = 0; i < c->n; i++) {
        if (c->entries[i].dl_handle) {
            dlclose(c->entries[i].dl_handle);
            c->entries[i].dl_handle = NULL;
        }
    }
    pthread_mutex_unlock(&c->mu);
    pthread_mutex_destroy(&c->mu);
    free(c);
}

/* ─── Create / drop ──────────────────────────────────────────────────────── */

int tsdb_udf_catalog_create(tsdb_udf_catalog_t *c,
                             const tsdb_udf_entry_t *in)
{
    if (!c || !in) return TSDB_ERR_INVAL;
    if (in->nargs < 0 || in->nargs > TSDB_UDF_MAX_ARGS) return TSDB_ERR_INVAL;
    if (!in->name[0] || !in->so_path[0] || !in->symbol[0]) return TSDB_ERR_INVAL;

    /* Canonical name */
    char canon[TSDB_UDF_NAME_MAX];
    name_lower(in->name, canon, sizeof(canon));

    if (tsdb_udf_name_is_builtin(canon)) return TSDB_ERR_EXISTS;

    pthread_mutex_lock(&c->mu);

    for (int i = 0; i < c->n; i++) {
        if (!strcmp(c->entries[i].name, canon)) {
            pthread_mutex_unlock(&c->mu);
            return TSDB_ERR_EXISTS;
        }
    }
    if (c->n >= TSDB_UDF_MAX_ENTRIES) {
        pthread_mutex_unlock(&c->mu);
        return TSDB_ERR_FULL;
    }

    tsdb_udf_entry_t *e = &c->entries[c->n];
    memset(e, 0, sizeof(*e));
    memcpy(e->name,    canon, sizeof(e->name));
    snprintf(e->so_path, sizeof(e->so_path), "%s", in->so_path);
    snprintf(e->symbol,  sizeof(e->symbol),  "%s", in->symbol);
    e->nargs      = in->nargs;
    for (int i = 0; i < in->nargs; i++) e->arg_types[i] = in->arg_types[i];
    e->ret_type   = in->ret_type;
    e->created_at = in->created_at;

    int rc = log_append_create(c, e);
    if (rc != TSDB_OK) {
        pthread_mutex_unlock(&c->mu);
        return rc;
    }
    c->n++;
    pthread_mutex_unlock(&c->mu);
    return TSDB_OK;
}

int tsdb_udf_catalog_drop(tsdb_udf_catalog_t *c, const char *name) {
    if (!c || !name) return TSDB_ERR_INVAL;
    char canon[TSDB_UDF_NAME_MAX];
    name_lower(name, canon, sizeof(canon));

    pthread_mutex_lock(&c->mu);
    int idx = -1;
    for (int i = 0; i < c->n; i++) {
        if (!strcmp(c->entries[i].name, canon)) { idx = i; break; }
    }
    if (idx < 0) { pthread_mutex_unlock(&c->mu); return TSDB_ERR_NOTFOUND; }

    if (c->entries[idx].dl_handle) {
        dlclose(c->entries[idx].dl_handle);
        c->entries[idx].dl_handle = NULL;
        c->entries[idx].fn = NULL;
    }
    memmove(&c->entries[idx], &c->entries[idx + 1],
            (size_t)(c->n - idx - 1) * sizeof(c->entries[0]));
    c->n--;

    int rc = log_append_drop(c, canon);
    pthread_mutex_unlock(&c->mu);
    return rc;
}

/* ─── Lookup + lazy dlopen ───────────────────────────────────────────────── */

/* Validate the ABI version the .so advertises. Returns TSDB_OK or
 * TSDB_ERR_UNSUPPORTED; writes human-readable message into errbuf. */
static int check_abi(void *handle, char *errbuf, size_t errcap) {
    typedef uint32_t (*abi_fn_t)(void);
    abi_fn_t ver = (abi_fn_t)dlsym(handle, "tsdb_udf_abi_version");
    if (!ver) {
        if (errbuf) snprintf(errbuf, errcap,
            "library exports no tsdb_udf_abi_version symbol — not a valid tsdb UDF");
        return TSDB_ERR_UNSUPPORTED;
    }
    uint32_t got = ver();
    if (got != TSDB_UDF_ABI_V1) {
        if (errbuf) snprintf(errbuf, errcap,
            "library ABI v%u does not match server v%u", got, TSDB_UDF_ABI_V1);
        return TSDB_ERR_UNSUPPORTED;
    }
    return TSDB_OK;
}

int tsdb_udf_catalog_lookup(tsdb_udf_catalog_t *c,
                             const char *name,
                             const tsdb_udf_entry_t **out_entry,
                             char *errbuf, size_t errcap)
{
    if (!c || !name || !out_entry) return TSDB_ERR_INVAL;
    char canon[TSDB_UDF_NAME_MAX];
    name_lower(name, canon, sizeof(canon));

    pthread_mutex_lock(&c->mu);
    tsdb_udf_entry_t *e = NULL;
    for (int i = 0; i < c->n; i++) {
        if (!strcmp(c->entries[i].name, canon)) { e = &c->entries[i]; break; }
    }
    if (!e) {
        pthread_mutex_unlock(&c->mu);
        return TSDB_ERR_NOTFOUND;
    }

    if (e->fn) {
        *out_entry = e;
        pthread_mutex_unlock(&c->mu);
        return TSDB_OK;
    }

    /* First use — try dlopen + dlsym + ABI check. */
    void *h = dlopen(e->so_path, RTLD_NOW | RTLD_LOCAL);
    if (!h) {
        if (errbuf) snprintf(errbuf, errcap,
            "dlopen('%s'): %s", e->so_path, dlerror());
        pthread_mutex_unlock(&c->mu);
        return TSDB_ERR_NOTFOUND;
    }

    int rc = check_abi(h, errbuf, errcap);
    if (rc != TSDB_OK) {
        dlclose(h);
        pthread_mutex_unlock(&c->mu);
        return rc;
    }

    tsdb_udf_fn_t sym = (tsdb_udf_fn_t)dlsym(h, e->symbol);
    if (!sym) {
        if (errbuf) snprintf(errbuf, errcap,
            "dlsym('%s' in '%s'): %s", e->symbol, e->so_path, dlerror());
        dlclose(h);
        pthread_mutex_unlock(&c->mu);
        return TSDB_ERR_NOTFOUND;
    }

    e->dl_handle = h;
    e->fn        = sym;
    *out_entry = e;
    pthread_mutex_unlock(&c->mu);
    return TSDB_OK;
}

/* ─── List ───────────────────────────────────────────────────────────────── */

int tsdb_udf_catalog_list(tsdb_udf_catalog_t *c,
                           tsdb_udf_entry_t **out_arr, size_t *out_n)
{
    if (!c || !out_arr || !out_n) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->mu);
    *out_n = (size_t)c->n;
    if (c->n == 0) { *out_arr = NULL; pthread_mutex_unlock(&c->mu); return TSDB_OK; }
    *out_arr = calloc((size_t)c->n, sizeof(**out_arr));
    if (!*out_arr) { pthread_mutex_unlock(&c->mu); return TSDB_ERR_NOMEM; }
    memcpy(*out_arr, c->entries, (size_t)c->n * sizeof(**out_arr));
    /* Zero dl_handle / fn — they're process-local, caller must not close. */
    for (int i = 0; i < c->n; i++) {
        (*out_arr)[i].dl_handle = NULL;
        (*out_arr)[i].fn        = NULL;
    }
    pthread_mutex_unlock(&c->mu);
    return TSDB_OK;
}
