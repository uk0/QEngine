/* catalog_v2.c — unified ID-keyed metadata catalog (Track B P2).  See catalog_v2.h.
 *
 * On disk: one append-only binary log <data_dir>/catalog/catalog.log of records
 *
 *   u32 rec_len | u8 op('+'/'-') | u8 ent | u16 pad | u64 oid | u64 lamport | payload
 *
 * In memory: every entity is a cat2_node_t kept in `idx` under key "o:<oid>"
 * (live AND tombstoned, so compaction can re-emit tombstones — the oid-merge in
 * P5 must never resurrect a dropped object).  Name keys ("d:<name>",
 * "g:<db>:<name>", "t:<db>:<name>") point at the same node and exist only while
 * the node is live.
 */
#include "catalog_v2.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/* ── small open-addressing string→ptr map (backward-shift delete) ─────────── */
typedef struct { char *key; void *val; } h_ent;
typedef struct { h_ent *b; size_t cap, size; } hmap;

static uint64_t h_hash(const char *s) {            /* FNV-1a */
    uint64_t h = 1469598103934665603ULL;
    for (; *s; s++) { h ^= (unsigned char)*s; h *= 1099511628211ULL; }
    return h;
}
static int h_init(hmap *m, size_t cap) {
    m->cap = cap; m->size = 0;
    m->b = calloc(cap, sizeof(h_ent));
    return m->b ? TSDB_OK : TSDB_ERR_NOMEM;
}
static int h_put(hmap *m, const char *key, void *val);
static int h_grow(hmap *m) {
    size_t ncap = m->cap * 2;
    h_ent *nb = calloc(ncap, sizeof(h_ent));
    if (!nb) return TSDB_ERR_NOMEM;
    h_ent *ob = m->b; size_t ocap = m->cap;
    m->b = nb; m->cap = ncap; m->size = 0;
    for (size_t i = 0; i < ocap; i++)
        if (ob[i].key) { h_put(m, ob[i].key, ob[i].val); free(ob[i].key); }
    free(ob);
    return TSDB_OK;
}
static int h_put(hmap *m, const char *key, void *val) {
    if ((m->size + 1) * 10 >= m->cap * 7) { if (h_grow(m) != TSDB_OK) return TSDB_ERR_NOMEM; }
    size_t i = h_hash(key) & (m->cap - 1);
    while (m->b[i].key) {
        if (strcmp(m->b[i].key, key) == 0) { m->b[i].val = val; return TSDB_OK; }
        i = (i + 1) & (m->cap - 1);
    }
    m->b[i].key = strdup(key);
    if (!m->b[i].key) return TSDB_ERR_NOMEM;
    m->b[i].val = val; m->size++;
    return TSDB_OK;
}
static void *h_get(hmap *m, const char *key) {
    if (!m->cap) return NULL;
    size_t i = h_hash(key) & (m->cap - 1);
    while (m->b[i].key) {
        if (strcmp(m->b[i].key, key) == 0) return m->b[i].val;
        i = (i + 1) & (m->cap - 1);
    }
    return NULL;
}
static void h_del(hmap *m, const char *key) {
    if (!m->cap) return;
    size_t i = h_hash(key) & (m->cap - 1);
    while (m->b[i].key && strcmp(m->b[i].key, key) != 0) i = (i + 1) & (m->cap - 1);
    if (!m->b[i].key) return;
    free(m->b[i].key); m->b[i].key = NULL; m->b[i].val = NULL; m->size--;
    /* backward-shift to keep the probe chain intact */
    size_t j = (i + 1) & (m->cap - 1);
    while (m->b[j].key) {
        size_t home = h_hash(m->b[j].key) & (m->cap - 1);
        int movable = (i <= j) ? (home <= i || home > j) : (home <= i && home > j);
        if (movable) { m->b[i] = m->b[j]; m->b[j].key = NULL; m->b[j].val = NULL; i = j; }
        j = (j + 1) & (m->cap - 1);
    }
}
static void h_free(hmap *m) {
    if (m->b) { for (size_t i = 0; i < m->cap; i++) free(m->b[i].key); free(m->b); }
    m->b = NULL; m->cap = m->size = 0;
}

/* ── entity node ──────────────────────────────────────────────────────────── */
typedef struct {
    tsdb_cat2_ent_t ent;
    uint64_t        lamport;
    int             live;
    union { tsdb_db_meta_t db; tsdb_group_meta_t grp; tsdb_table_meta_t tbl; } u;
} cat2_node_t;

struct tsdb_catalog_v2 {
    char             cat_dir[4096];
    char             log_path[4160];
    FILE            *log;
    hmap             idx;            /* "o:<oid>" + name keys → cat2_node_t* */
    tsdb_oid_alloc_t alloc;
    uint64_t         lamport;        /* per-node monotonic op clock */
    pthread_mutex_t  lock;
};

/* ── buffer cursor (little-endian) ────────────────────────────────────────── */
typedef struct { uint8_t *p; size_t cap, len; } wbuf;
static void w_bytes(wbuf *b, const void *s, size_t n) { memcpy(b->p + b->len, s, n); b->len += n; }
static void w_u8 (wbuf *b, uint8_t v)  { b->p[b->len++] = v; }
static void w_u16(wbuf *b, uint16_t v) { w_bytes(b, &v, 2); }
static void w_u32(wbuf *b, uint32_t v) { w_bytes(b, &v, 4); }
static void w_u64(wbuf *b, uint64_t v) { w_bytes(b, &v, 8); }
static void w_str(wbuf *b, const char *s) { uint16_t n = (uint16_t)strlen(s); w_u16(b, n); w_bytes(b, s, n); }
static void w_col(wbuf *b, const tsdb_col_def_t *c) { w_str(b, c->name); w_u32(b, (uint32_t)c->type); }

typedef struct { const uint8_t *p; size_t len, off; int err; } rbuf;
static void r_bytes(rbuf *b, void *o, size_t n) { if (b->off + n > b->len) { b->err = 1; return; } memcpy(o, b->p + b->off, n); b->off += n; }
static uint8_t  r_u8 (rbuf *b) { uint8_t v = 0;  r_bytes(b, &v, 1); return v; }
static uint16_t r_u16(rbuf *b) { uint16_t v = 0; r_bytes(b, &v, 2); return v; }
static uint32_t r_u32(rbuf *b) { uint32_t v = 0; r_bytes(b, &v, 4); return v; }
static uint64_t r_u64(rbuf *b) { uint64_t v = 0; r_bytes(b, &v, 8); return v; }
static void r_str(rbuf *b, char *o, size_t cap) {
    uint16_t n = r_u16(b); if (b->err) return;
    if ((size_t)n >= cap) { b->err = 1; return; }
    r_bytes(b, o, n); if (!b->err) o[n] = '\0';
}
static void r_col(rbuf *b, tsdb_col_def_t *c) { r_str(b, c->name, sizeof(c->name)); c->type = (tsdb_type_t)r_u32(b); }

/* ── payload (de)serialization per entity ─────────────────────────────────── */
static void ser_payload(wbuf *b, const cat2_node_t *n) {
    if (n->ent == CAT2_ENT_DB) {
        const tsdb_db_meta_t *d = &n->u.db;
        w_str(b, d->name); w_str(b, d->description);
        w_u64(b, (uint64_t)d->retention_ns); w_u64(b, (uint64_t)d->created_at);
        w_u8(b, d->protected_flag);
    } else if (n->ent == CAT2_ENT_GROUP) {
        const tsdb_group_meta_t *g = &n->u.grp;
        w_u64(b, g->db_id); w_str(b, g->name); w_str(b, g->region);
        w_str(b, g->codec_profile); w_u64(b, (uint64_t)g->retention_ns);
        w_u32(b, (uint32_t)g->replica_factor); w_u64(b, (uint64_t)g->created_at);
    } else {
        const tsdb_table_meta_t *t = &n->u.tbl;
        w_u64(b, t->db_id); w_u64(b, t->group_id); w_u64(b, t->parent_id);
        w_u8(b, (uint8_t)t->kind); w_str(b, t->name);
        w_u32(b, (uint32_t)t->ncols); w_u32(b, (uint32_t)t->ts_col_idx);
        for (int i = 0; i < t->ncols; i++) w_col(b, &t->cols[i]);
        w_u32(b, (uint32_t)t->ntag_cols);
        for (int i = 0; i < t->ntag_cols; i++) w_col(b, &t->tag_cols[i]);
        w_u32(b, (uint32_t)t->ntag_vals);
        for (int i = 0; i < t->ntag_vals; i++) {
            w_u32(b, (uint32_t)t->tag_vals[i].type);
            w_bytes(b, &t->tag_vals[i].v, sizeof(t->tag_vals[i].v));
        }
        w_u64(b, (uint64_t)t->created_at);
    }
}
static void deser_payload(rbuf *b, cat2_node_t *n) {
    if (n->ent == CAT2_ENT_DB) {
        tsdb_db_meta_t *d = &n->u.db; memset(d, 0, sizeof(*d));
        r_str(b, d->name, sizeof(d->name)); r_str(b, d->description, sizeof(d->description));
        d->retention_ns = (int64_t)r_u64(b); d->created_at = (tsdb_ts_t)r_u64(b);
        d->protected_flag = r_u8(b);
    } else if (n->ent == CAT2_ENT_GROUP) {
        tsdb_group_meta_t *g = &n->u.grp; memset(g, 0, sizeof(*g));
        g->db_id = r_u64(b); r_str(b, g->name, sizeof(g->name)); r_str(b, g->region, sizeof(g->region));
        r_str(b, g->codec_profile, sizeof(g->codec_profile)); g->retention_ns = (int64_t)r_u64(b);
        g->replica_factor = (int)r_u32(b); g->created_at = (tsdb_ts_t)r_u64(b);
    } else {
        tsdb_table_meta_t *t = &n->u.tbl; memset(t, 0, sizeof(*t));
        t->db_id = r_u64(b); t->group_id = r_u64(b); t->parent_id = r_u64(b);
        t->kind = (tsdb_table_kind_t)r_u8(b); r_str(b, t->name, sizeof(t->name));
        t->ncols = (int)r_u32(b); t->ts_col_idx = (int)r_u32(b);
        if (t->ncols < 0 || t->ncols > TSDB_CAT2_MAX_COLS) { b->err = 1; return; }
        for (int i = 0; i < t->ncols; i++) r_col(b, &t->cols[i]);
        t->ntag_cols = (int)r_u32(b);
        if (t->ntag_cols < 0 || t->ntag_cols > TSDB_CAT2_MAX_TAGS) { b->err = 1; return; }
        for (int i = 0; i < t->ntag_cols; i++) r_col(b, &t->tag_cols[i]);
        t->ntag_vals = (int)r_u32(b);
        if (t->ntag_vals < 0 || t->ntag_vals > TSDB_CAT2_MAX_TAGS) { b->err = 1; return; }
        for (int i = 0; i < t->ntag_vals; i++) {
            t->tag_vals[i].type = (tsdb_type_t)r_u32(b);
            r_bytes(b, &t->tag_vals[i].v, sizeof(t->tag_vals[i].v));
        }
        t->created_at = (tsdb_ts_t)r_u64(b);
    }
}

/* ── key helpers ──────────────────────────────────────────────────────────── */
static void k_oid(char *o, size_t n, tsdb_oid_t oid) { snprintf(o, n, "o:%llu", (unsigned long long)oid); }
static void k_db (char *o, size_t n, const char *name) { snprintf(o, n, "d:%s", name); }
static void k_grp(char *o, size_t n, tsdb_oid_t db, const char *name) { snprintf(o, n, "g:%llu:%s", (unsigned long long)db, name); }
static void k_tbl(char *o, size_t n, tsdb_oid_t db, const char *name) { snprintf(o, n, "t:%llu:%s", (unsigned long long)db, name); }

static cat2_node_t *node_by_oid(tsdb_catalog_v2_t *c, tsdb_oid_t oid) {
    char k[40]; k_oid(k, sizeof(k), oid);
    return (cat2_node_t *)h_get(&c->idx, k);
}

/* Insert/remove a node's name keys (live entities only). */
static void index_name(tsdb_catalog_v2_t *c, cat2_node_t *n, int add) {
    char k[160];
    if (n->ent == CAT2_ENT_DB)        k_db (k, sizeof(k), n->u.db.name);
    else if (n->ent == CAT2_ENT_GROUP) k_grp(k, sizeof(k), n->u.grp.db_id, n->u.grp.name);
    else                               k_tbl(k, sizeof(k), n->u.tbl.db_id, n->u.tbl.name);
    if (add) h_put(&c->idx, k, n); else h_del(&c->idx, k);
}

/* Apply a parsed node into the in-memory index (used by replay + create). */
static void apply_node(tsdb_catalog_v2_t *c, cat2_node_t *parsed) {
    tsdb_oid_t oid = parsed->ent == CAT2_ENT_DB ? parsed->u.db.oid
                   : parsed->ent == CAT2_ENT_GROUP ? parsed->u.grp.oid : parsed->u.tbl.oid;
    cat2_node_t *ex = node_by_oid(c, oid);
    if (ex) {
        if (parsed->lamport < ex->lamport) return;        /* stale — ignore */
        if (ex->live) index_name(c, ex, 0);               /* drop old name keys */
        *ex = *parsed;
        if (ex->live) index_name(c, ex, 1);
    } else {
        cat2_node_t *n = malloc(sizeof(*n));
        if (!n) return;
        *n = *parsed;
        char k[40]; k_oid(k, sizeof(k), oid);
        h_put(&c->idx, k, n);
        if (n->live) index_name(c, n, 1);
    }
    tsdb_oid_observe(&c->alloc, oid);
    if (parsed->lamport >= c->lamport) c->lamport = parsed->lamport + 1;
}

/* ── log I/O ──────────────────────────────────────────────────────────────── */
static int log_emit(tsdb_catalog_v2_t *c, FILE *f, char op, const cat2_node_t *n, tsdb_oid_t oid) {
    uint8_t buf[8192]; wbuf b = { buf, sizeof(buf), 0 };
    w_u32(&b, 0);                       /* rec_len placeholder */
    w_u8(&b, (uint8_t)op); w_u8(&b, (uint8_t)n->ent); w_u16(&b, 0);
    w_u64(&b, oid); w_u64(&b, n->lamport);
    if (op == '+') ser_payload(&b, n);
    uint32_t rec_len = (uint32_t)(b.len - 4);
    memcpy(buf, &rec_len, 4);
    if (fwrite(buf, 1, b.len, f) != b.len) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int replay(tsdb_catalog_v2_t *c) {
    FILE *f = fopen(c->log_path, "rb");
    if (!f) return TSDB_OK;
    for (;;) {
        uint32_t rec_len;
        if (fread(&rec_len, 1, 4, f) != 4) break;        /* EOF */
        if (rec_len < 18 || rec_len > 16 * 1024 * 1024) break;  /* corrupt tail → stop */
        uint8_t *rec = malloc(rec_len);
        if (!rec) { fclose(f); return TSDB_ERR_NOMEM; }
        if (fread(rec, 1, rec_len, f) != rec_len) { free(rec); break; }  /* torn tail */
        rbuf b = { rec, rec_len, 0, 0 };
        cat2_node_t n; memset(&n, 0, sizeof(n));
        uint8_t op = r_u8(&b); n.ent = (tsdb_cat2_ent_t)r_u8(&b); (void)r_u16(&b);
        tsdb_oid_t oid = r_u64(&b); n.lamport = r_u64(&b);
        n.live = (op == '+');
        if (op == '+') {
            deser_payload(&b, &n);
            if (n.ent == CAT2_ENT_DB) n.u.db.oid = oid;
            else if (n.ent == CAT2_ENT_GROUP) n.u.grp.oid = oid;
            else n.u.tbl.oid = oid;
        } else {
            /* tombstone: keep ent + oid so the node lingers for merge/compaction */
            if (n.ent == CAT2_ENT_DB) n.u.db.oid = oid;
            else if (n.ent == CAT2_ENT_GROUP) n.u.grp.oid = oid;
            else n.u.tbl.oid = oid;
        }
        if (!b.err) apply_node(c, &n);
        free(rec);
    }
    fclose(f);
    return TSDB_OK;
}

int tsdb_cat2_compact(tsdb_catalog_v2_t *c) {
    char tmp[4200]; snprintf(tmp, sizeof(tmp), "%s.tmp", c->log_path);
    FILE *nf = fopen(tmp, "wb");
    if (!nf) return TSDB_ERR_IO;
    int rc = TSDB_OK;
    for (size_t i = 0; i < c->idx.cap; i++) {
        if (!c->idx.b[i].key || strncmp(c->idx.b[i].key, "o:", 2) != 0) continue; /* nodes only */
        cat2_node_t *n = (cat2_node_t *)c->idx.b[i].val;
        tsdb_oid_t oid = n->ent == CAT2_ENT_DB ? n->u.db.oid
                       : n->ent == CAT2_ENT_GROUP ? n->u.grp.oid : n->u.tbl.oid;
        /* Re-emit live records AND tombstones — retaining tombstones is what
         * keeps the cluster oid-merge from resurrecting a dropped object. */
        if (log_emit(c, nf, n->live ? '+' : '-', n, oid) != TSDB_OK) { rc = TSDB_ERR_IO; break; }
    }
    fflush(nf); fsync(fileno(nf)); fclose(nf);
    if (rc != TSDB_OK) { unlink(tmp); return rc; }
    if (c->log) { fclose(c->log); c->log = NULL; }
    if (rename(tmp, c->log_path) != 0) { unlink(tmp); return TSDB_ERR_IO; }
    c->log = fopen(c->log_path, "ab");
    return c->log ? TSDB_OK : TSDB_ERR_IO;
}

/* ── open / close ─────────────────────────────────────────────────────────── */
static int materialize_reserved(tsdb_catalog_v2_t *c) {
    if (!node_by_oid(c, TSDB_OID_DEFAULTDB)) {
        tsdb_db_meta_t d; memset(&d, 0, sizeof(d));
        d.oid = TSDB_OID_DEFAULTDB; snprintf(d.name, sizeof(d.name), "default");
        d.protected_flag = 1; d.created_at = tsdb_now_ns();
        int rc = tsdb_cat2_db_create(c, &d); if (rc != TSDB_OK) return rc;
    }
    if (!node_by_oid(c, TSDB_OID_SYSDB)) {
        tsdb_db_meta_t d; memset(&d, 0, sizeof(d));
        d.oid = TSDB_OID_SYSDB; snprintf(d.name, sizeof(d.name), "sysdb");
        d.protected_flag = 1; d.created_at = tsdb_now_ns();
        int rc = tsdb_cat2_db_create(c, &d); if (rc != TSDB_OK) return rc;
    }
    return TSDB_OK;
}

int tsdb_cat2_open(const char *data_dir, uint16_t node_id, tsdb_catalog_v2_t **out) {
    if (!data_dir || !out) return TSDB_ERR_INVAL;
    tsdb_catalog_v2_t *c = calloc(1, sizeof(*c));
    if (!c) return TSDB_ERR_NOMEM;
    snprintf(c->cat_dir, sizeof(c->cat_dir), "%s/catalog", data_dir);
    mkdir(data_dir, 0755); mkdir(c->cat_dir, 0755);
    snprintf(c->log_path, sizeof(c->log_path), "%s/catalog.log", c->cat_dir);
    pthread_mutex_init(&c->lock, NULL);
    c->lamport = 1;
    if (h_init(&c->idx, 256) != TSDB_OK) { free(c); return TSDB_ERR_NOMEM; }
    if (tsdb_oid_alloc_open(&c->alloc, c->cat_dir, node_id) != TSDB_OK) { h_free(&c->idx); free(c); return TSDB_ERR_IO; }
    if (replay(c) != TSDB_OK) { tsdb_oid_alloc_close(&c->alloc); h_free(&c->idx); free(c); return TSDB_ERR_IO; }
    c->log = fopen(c->log_path, "ab");
    if (!c->log) { tsdb_oid_alloc_close(&c->alloc); h_free(&c->idx); free(c); return TSDB_ERR_IO; }
    if (materialize_reserved(c) != TSDB_OK) { tsdb_cat2_close(c); return TSDB_ERR_IO; }
    *out = c;
    return TSDB_OK;
}

void tsdb_cat2_close(tsdb_catalog_v2_t *c) {
    if (!c) return;
    if (c->log) fclose(c->log);
    /* free nodes (each lives once under its "o:" key) */
    for (size_t i = 0; i < c->idx.cap; i++)
        if (c->idx.b[i].key && strncmp(c->idx.b[i].key, "o:", 2) == 0) free(c->idx.b[i].val);
    h_free(&c->idx);
    tsdb_oid_alloc_close(&c->alloc);
    pthread_mutex_destroy(&c->lock);
    free(c);
}

/* ── create ───────────────────────────────────────────────────────────────── */
/* Referential integrity: a parent oid must reference a currently-live node. */
static int require_live(tsdb_catalog_v2_t *c, tsdb_oid_t oid) {
    cat2_node_t *n = node_by_oid(c, oid);
    return n && n->live;
}

static int create_common(tsdb_catalog_v2_t *c, cat2_node_t *n, tsdb_oid_t *oid_io) {
    if (*oid_io == TSDB_OID_NONE) {
        *oid_io = tsdb_oid_next(&c->alloc);
        if (*oid_io == TSDB_OID_NONE) return TSDB_ERR_IO;
    }
    /* Stamp the assigned oid into the entity so apply_node + ser_payload agree
     * with the header oid (identity lives in the header). */
    if (n->ent == CAT2_ENT_DB)         n->u.db.oid  = *oid_io;
    else if (n->ent == CAT2_ENT_GROUP) n->u.grp.oid = *oid_io;
    else                               n->u.tbl.oid = *oid_io;
    n->live = 1; n->lamport = c->lamport++;
    if (log_emit(c, c->log, '+', n, *oid_io) != TSDB_OK) return TSDB_ERR_IO;
    fflush(c->log);
    apply_node(c, n);
    return TSDB_OK;
}

int tsdb_cat2_db_create(tsdb_catalog_v2_t *c, tsdb_db_meta_t *db) {
    if (!c || !db || !db->name[0]) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    if (tsdb_cat2_db_by_name(c, db->name)) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_EXISTS; }
    if (db->created_at == 0) db->created_at = tsdb_now_ns();
    cat2_node_t n; memset(&n, 0, sizeof(n)); n.ent = CAT2_ENT_DB; n.u.db = *db;
    int rc = create_common(c, &n, &db->oid);
    pthread_mutex_unlock(&c->lock);
    return rc;
}

int tsdb_cat2_group_create(tsdb_catalog_v2_t *c, tsdb_group_meta_t *g) {
    if (!c || !g || !g->name[0]) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    if (!require_live(c, g->db_id)) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    char k[160]; k_grp(k, sizeof(k), g->db_id, g->name);
    if (h_get(&c->idx, k)) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_EXISTS; }
    if (g->created_at == 0) g->created_at = tsdb_now_ns();
    cat2_node_t n; memset(&n, 0, sizeof(n)); n.ent = CAT2_ENT_GROUP; n.u.grp = *g;
    int rc = create_common(c, &n, &g->oid);
    pthread_mutex_unlock(&c->lock);
    return rc;
}

int tsdb_cat2_table_create(tsdb_catalog_v2_t *c, tsdb_table_meta_t *t) {
    if (!c || !t || !t->name[0]) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    /* RI: db live; group live if set; for a child, parent must be a live SUPER
     * with a matching tag-value arity.  A child/stable can never be created
     * under a missing parent → orphans are unrepresentable. */
    if (!require_live(c, t->db_id)) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND; }
    if (t->group_id != TSDB_OID_NONE && !require_live(c, t->group_id)) {
        pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND;
    }
    if (t->kind == TBL_CHILD) {
        cat2_node_t *p = node_by_oid(c, t->parent_id);
        if (!p || !p->live || p->ent != CAT2_ENT_TABLE || p->u.tbl.kind != TBL_SUPER) {
            pthread_mutex_unlock(&c->lock); return TSDB_ERR_NOTFOUND;
        }
        if (t->ntag_vals != p->u.tbl.ntag_cols) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_SCHEMA; }
    }
    if (tsdb_cat2_table_by_name(c, t->db_id, t->name)) { pthread_mutex_unlock(&c->lock); return TSDB_ERR_EXISTS; }
    if (t->created_at == 0) t->created_at = tsdb_now_ns();
    cat2_node_t n; memset(&n, 0, sizeof(n)); n.ent = CAT2_ENT_TABLE; n.u.tbl = *t;
    int rc = create_common(c, &n, &t->oid);
    pthread_mutex_unlock(&c->lock);
    return rc;
}

/* ── get / lookup ─────────────────────────────────────────────────────────── */
int tsdb_cat2_db_get(tsdb_catalog_v2_t *c, tsdb_oid_t oid, tsdb_db_meta_t *out) {
    pthread_mutex_lock(&c->lock);
    cat2_node_t *n = node_by_oid(c, oid);
    int rc = (n && n->live && n->ent == CAT2_ENT_DB) ? (*out = n->u.db, TSDB_OK) : TSDB_ERR_NOTFOUND;
    pthread_mutex_unlock(&c->lock);
    return rc;
}
int tsdb_cat2_group_get(tsdb_catalog_v2_t *c, tsdb_oid_t oid, tsdb_group_meta_t *out) {
    pthread_mutex_lock(&c->lock);
    cat2_node_t *n = node_by_oid(c, oid);
    int rc = (n && n->live && n->ent == CAT2_ENT_GROUP) ? (*out = n->u.grp, TSDB_OK) : TSDB_ERR_NOTFOUND;
    pthread_mutex_unlock(&c->lock);
    return rc;
}
int tsdb_cat2_table_get(tsdb_catalog_v2_t *c, tsdb_oid_t oid, tsdb_table_meta_t *out) {
    pthread_mutex_lock(&c->lock);
    cat2_node_t *n = node_by_oid(c, oid);
    int rc = (n && n->live && n->ent == CAT2_ENT_TABLE) ? (*out = n->u.tbl, TSDB_OK) : TSDB_ERR_NOTFOUND;
    pthread_mutex_unlock(&c->lock);
    return rc;
}

tsdb_oid_t tsdb_cat2_db_by_name(tsdb_catalog_v2_t *c, const char *name) {
    char k[160]; k_db(k, sizeof(k), name);
    cat2_node_t *n = (cat2_node_t *)h_get(&c->idx, k);
    return (n && n->live) ? n->u.db.oid : TSDB_OID_NONE;
}
tsdb_oid_t tsdb_cat2_table_by_name(tsdb_catalog_v2_t *c, tsdb_oid_t db_id, const char *name) {
    char k[160]; k_tbl(k, sizeof(k), db_id, name);
    cat2_node_t *n = (cat2_node_t *)h_get(&c->idx, k);
    return (n && n->live) ? n->u.tbl.oid : TSDB_OID_NONE;
}

/* ── structural cascade drop (P3) ─────────────────────────────────────────── */

/* The single cascade edge that owns an entity: dropping this oid drops the
 * entity.  DB is the subtree root; a group hangs off its db; a child hangs off
 * its super; a plain/super hangs off its group (or db if ungrouped).  A
 * recursive walk over this single-parent tree reaches every descendant of a
 * DROP DATABASE / DROP GROUP / DROP STABLE with no name matching and no orphan. */
static tsdb_oid_t cascade_parent(const cat2_node_t *n) {
    if (n->ent == CAT2_ENT_DB)    return TSDB_OID_NONE;
    if (n->ent == CAT2_ENT_GROUP) return n->u.grp.db_id;
    if (n->u.tbl.kind == TBL_CHILD)              return n->u.tbl.parent_id;
    if (n->u.tbl.group_id != TSDB_OID_NONE)      return n->u.tbl.group_id;
    return n->u.tbl.db_id;
}

static tsdb_oid_t node_oid(const cat2_node_t *n) {
    return n->ent == CAT2_ENT_DB ? n->u.db.oid
         : n->ent == CAT2_ENT_GROUP ? n->u.grp.oid : n->u.tbl.oid;
}

static int drop_one(tsdb_catalog_v2_t *c, cat2_node_t *n, tsdb_oid_t oid) {
    index_name(c, n, 0);
    n->live = 0; n->lamport = c->lamport++;
    int rc = log_emit(c, c->log, '-', n, oid);
    if (rc == TSDB_OK) fflush(c->log);
    return rc;
}

/* Tombstone `oid` and, depth-first, every live entity whose cascade_parent
 * resolves into the drop.  Caller holds c->lock. */
static int drop_subtree_locked(tsdb_catalog_v2_t *c, tsdb_oid_t oid) {
    cat2_node_t *n = node_by_oid(c, oid);
    if (!n || !n->live) return TSDB_ERR_NOTFOUND;
    /* Snapshot direct live children before mutating (idx.size bounds them). */
    tsdb_oid_t *kids = malloc((c->idx.size + 1) * sizeof(tsdb_oid_t));
    size_t nk = 0;
    if (kids) {
        for (size_t i = 0; i < c->idx.cap; i++) {
            if (!c->idx.b[i].key || strncmp(c->idx.b[i].key, "o:", 2) != 0) continue;
            cat2_node_t *m = (cat2_node_t *)c->idx.b[i].val;
            if (m->live && cascade_parent(m) == oid) kids[nk++] = node_oid(m);
        }
        for (size_t i = 0; i < nk; i++) (void)drop_subtree_locked(c, kids[i]);
        free(kids);
    }
    return drop_one(c, n, oid);
}

int tsdb_cat2_drop(tsdb_catalog_v2_t *c, tsdb_oid_t oid) {
    if (!c) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&c->lock);
    int rc = drop_subtree_locked(c, oid);
    pthread_mutex_unlock(&c->lock);
    return rc;
}

size_t tsdb_cat2_count(tsdb_catalog_v2_t *c) {
    size_t live = 0;
    pthread_mutex_lock(&c->lock);
    for (size_t i = 0; i < c->idx.cap; i++)
        if (c->idx.b[i].key && strncmp(c->idx.b[i].key, "o:", 2) == 0) {
            cat2_node_t *n = (cat2_node_t *)c->idx.b[i].val;
            if (n->live) live++;
        }
    pthread_mutex_unlock(&c->lock);
    return live;
}
