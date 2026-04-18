/* schema.c — table schema persistence and management. */

#include "schema.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* Binary schema format.
 *
 * v1 (legacy):
 *   [magic u32][version u16][ncols u16][ts_col_idx u32]
 *   [name: TSDB_MAX_NAME+1 bytes]
 *   per-col: [type u8][_pad u8][name: TSDB_MAX_NAME+1 bytes]
 *
 * v2 (2026-04):
 *   ... v1 fields ...
 *   [partition_unit u8][_pad u8×3]   -- appended; defaults to DAY on v1 read
 *
 * v3 (2026-04):
 *   ... v2 fields ...
 *   [block_points u32]              -- per-table block size; v1/v2 → 8192
 *
 * Readers accept all three; older writers always emitted 8192 so v1/v2 files
 * resolve unambiguously.
 */
#define SCHEMA_MAGIC   0x53434D41u  /* "SCMA" */
#define SCHEMA_VERSION 3

static int clamp_block_points(int bp) {
    if (bp <= 0) return TSDB_BLOCK_POINTS;
    if (bp < 1024) return 1024;
    if (bp > TSDB_BLOCK_POINTS) return TSDB_BLOCK_POINTS;
    return bp;
}

/* ---- portable mkdir-p -------------------------------------------------- */

/*
 * Recursively create directories (like mkdir -p).
 * Works on a mutable copy of the path. No error if leaf already exists.
 */
static int mkdir_p(const char *path, mode_t mode) {
    char buf[4096];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(buf)) return -1;
    memcpy(buf, path, len + 1);

    for (size_t i = 1; i <= len; i++) {
        if (buf[i] == '/' || buf[i] == '\0') {
            char saved = buf[i];
            buf[i] = '\0';
            int r = mkdir(buf, mode);
            if (r < 0 && errno != EEXIST) return -1;
            buf[i] = saved;
        }
    }
    return 0;
}

/* Exposed helper for other storage modules. */
int tsdb_mkdir_p(const char *path) {
    return mkdir_p(path, 0755);
}

/* ---- schema_save -------------------------------------------------------- */

static int schema_save(tsdb_schema_t *s) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/schema.bin", s->dir);

    FILE *f = fopen(path, "wb");
    if (!f) return TSDB_ERR_IO;

    uint32_t magic   = SCHEMA_MAGIC;
    uint16_t version = SCHEMA_VERSION;
    uint16_t ncols   = (uint16_t)s->ncols;
    uint32_t ts_idx  = (uint32_t)s->ts_col_idx;

    int rc = 0;
#define WR(p, n) do { if (!rc && fwrite((p), 1, (n), f) != (n)) rc = TSDB_ERR_IO; } while(0)
    WR(&magic,   4);
    WR(&version, 2);
    WR(&ncols,   2);
    WR(&ts_idx,  4);
    /* table name — fixed TSDB_MAX_NAME+1 bytes */
    WR(s->name, TSDB_MAX_NAME + 1);

    for (int i = 0; i < s->ncols && !rc; i++) {
        uint8_t type = (uint8_t)s->cols[i].type;
        uint8_t pad  = 0;
        WR(&type, 1);
        WR(&pad,  1);
        WR(s->cols[i].name, TSDB_MAX_NAME + 1);
    }

    /* v2 tail: partition_unit + 3 bytes reserved pad. */
    {
        uint8_t part_unit = (uint8_t)s->partition_unit;
        uint8_t reserved[3] = {0, 0, 0};
        WR(&part_unit, 1);
        WR(reserved,  3);
    }
    /* v3 tail: block_points u32. */
    {
        uint32_t bp = (uint32_t)s->block_points;
        WR(&bp, 4);
    }
#undef WR

    fclose(f);

    /* Persist each SYMBOL column's symtab. */
    for (int i = 0; i < s->ncols && !rc; i++) {
        if (s->cols[i].type == TSDB_TYPE_SYMBOL && s->cols[i].symtab) {
            char sym_path[4096];
            snprintf(sym_path, sizeof(sym_path), "%s/%s.sym", s->dir, s->cols[i].name);
            rc = tsdb_symtab_save(s->cols[i].symtab, sym_path);
        }
    }
    return rc;
}

/* ---- tsdb_schema_create ------------------------------------------------ */

int tsdb_schema_create(const char *dir, const char *name,
                       const tsdb_col_t *cols, int ncols,
                       const char *ts_col,
                       tsdb_schema_t **out)
{
    return tsdb_schema_create_ex(dir, name, cols, ncols, ts_col,
                                  TSDB_PARTITION_DAY, 0, out);
}

int tsdb_schema_create_ex(const char *dir, const char *name,
                          const tsdb_col_t *cols, int ncols,
                          const char *ts_col,
                          tsdb_partition_unit_t partition_unit,
                          int block_points,
                          tsdb_schema_t **out)
{
    if (!dir || !name || !cols || ncols <= 0 || ncols > TSDB_MAX_COLS || !ts_col || !out)
        return TSDB_ERR_INVAL;
    if (partition_unit != TSDB_PARTITION_DAY && partition_unit != TSDB_PARTITION_HOUR)
        return TSDB_ERR_INVAL;

    if (strlen(name) > TSDB_MAX_NAME)
        return TSDB_ERR_INVAL;

    /* Validate column names, find ts_col. */
    int ts_idx = -1;
    for (int i = 0; i < ncols; i++) {
        if (!cols[i].name || strlen(cols[i].name) > TSDB_MAX_NAME)
            return TSDB_ERR_INVAL;
        if (strcmp(cols[i].name, ts_col) == 0) ts_idx = i;
    }
    if (ts_idx < 0) return TSDB_ERR_SCHEMA;
    if (cols[ts_idx].type != TSDB_TYPE_TIMESTAMP) return TSDB_ERR_SCHEMA;

    /* Ensure table directory exists. */
    if (mkdir_p(dir, 0755) < 0) return TSDB_ERR_IO;

    /* Allocate schema. */
    tsdb_schema_t *s = calloc(1, sizeof(*s));
    if (!s) return TSDB_ERR_NOMEM;

    strncpy(s->name, name, TSDB_MAX_NAME);
    s->ncols          = ncols;
    s->ts_col_idx     = ts_idx;
    s->partition_unit = partition_unit;
    s->block_points   = clamp_block_points(block_points);
    s->dir            = strdup(dir);
    if (!s->dir) { free(s); return TSDB_ERR_NOMEM; }

    s->cols = calloc((size_t)ncols, sizeof(tsdb_col_info_t));
    if (!s->cols) { free(s->dir); free(s); return TSDB_ERR_NOMEM; }

    for (int i = 0; i < ncols; i++) {
        strncpy(s->cols[i].name, cols[i].name, TSDB_MAX_NAME);
        s->cols[i].type   = cols[i].type;
        s->cols[i].symtab = NULL;

        /* Lazily create symtab for SYMBOL columns. */
        if (cols[i].type == TSDB_TYPE_SYMBOL) {
            int rc = tsdb_symtab_new(&s->cols[i].symtab);
            if (rc != TSDB_OK) {
                /* Cleanup and return. */
                for (int j = 0; j < i; j++) {
                    if (s->cols[j].symtab) tsdb_symtab_free(s->cols[j].symtab);
                }
                free(s->cols); free(s->dir); free(s);
                return rc;
            }
        }
    }

    /* Persist to disk. */
    int rc = schema_save(s);
    if (rc != TSDB_OK) {
        tsdb_schema_free(s);
        return rc;
    }

    *out = s;
    return TSDB_OK;
}

/* ---- tsdb_schema_add_column -------------------------------------------- */

int tsdb_schema_add_column(tsdb_schema_t *s, const char *col_name,
                            tsdb_type_t col_type)
{
    if (!s || !col_name) return TSDB_ERR_INVAL;
    if (strlen(col_name) > TSDB_MAX_NAME) return TSDB_ERR_INVAL;
    if (s->ncols >= TSDB_MAX_COLS) return TSDB_ERR_INVAL;

    /* Duplicate-name guard. */
    for (int i = 0; i < s->ncols; i++) {
        if (strcmp(s->cols[i].name, col_name) == 0) return TSDB_ERR_EXISTS;
    }

    tsdb_col_info_t *nc = realloc(s->cols, (size_t)(s->ncols + 1) * sizeof(*s->cols));
    if (!nc) return TSDB_ERR_NOMEM;
    s->cols = nc;

    tsdb_col_info_t *slot = &s->cols[s->ncols];
    memset(slot, 0, sizeof(*slot));
    strncpy(slot->name, col_name, TSDB_MAX_NAME);
    slot->type   = col_type;
    slot->symtab = NULL;

    if (col_type == TSDB_TYPE_SYMBOL) {
        int rc = tsdb_symtab_new(&slot->symtab);
        if (rc != TSDB_OK) return rc;
    }

    s->ncols++;

    int rc = schema_save(s);
    if (rc != TSDB_OK) {
        /* Best-effort rollback. */
        if (slot->symtab) { tsdb_symtab_free(slot->symtab); slot->symtab = NULL; }
        s->ncols--;
        return rc;
    }
    return TSDB_OK;
}

/* ---- tsdb_schema_open -------------------------------------------------- */

int tsdb_schema_open(const char *dir, tsdb_schema_t **out) {
    if (!dir || !out) return TSDB_ERR_INVAL;

    char path[4096];
    snprintf(path, sizeof(path), "%s/schema.bin", dir);

    FILE *f = fopen(path, "rb");
    if (!f) return TSDB_ERR_NOTFOUND;

    uint32_t magic   = 0;
    uint16_t version = 0, ncols_raw = 0;
    uint32_t ts_idx  = 0;
    char     tbl_name[TSDB_MAX_NAME + 1];

    int rc = TSDB_OK;
#define RD(p, n) do { if (!rc && fread((p), 1, (n), f) != (n)) rc = TSDB_ERR_CORRUPT; } while(0)
    RD(&magic,    4);
    if (magic != SCHEMA_MAGIC) { fclose(f); return TSDB_ERR_CORRUPT; }
    RD(&version, 2);
    /* Accept v1, v2, v3. Higher versions are unknown. */
    if (version != 1 && version != 2 && version != 3) {
        fclose(f); return TSDB_ERR_UNSUPPORTED;
    }
    RD(&ncols_raw, 2);
    RD(&ts_idx,    4);
    RD(tbl_name,   TSDB_MAX_NAME + 1);

    if (rc != TSDB_OK) { fclose(f); return rc; }

    int ncols = (int)ncols_raw;
    if (ncols <= 0 || ncols > TSDB_MAX_COLS) { fclose(f); return TSDB_ERR_CORRUPT; }

    tsdb_schema_t *s = calloc(1, sizeof(*s));
    if (!s) { fclose(f); return TSDB_ERR_NOMEM; }

    memcpy(s->name, tbl_name, TSDB_MAX_NAME + 1);
    s->ncols          = ncols;
    s->ts_col_idx     = (int)ts_idx;
    s->partition_unit = TSDB_PARTITION_DAY;   /* v1 default; v2+ overrides below */
    s->block_points   = TSDB_BLOCK_POINTS;    /* v1/v2 default; v3 overrides below */
    s->dir            = strdup(dir);
    if (!s->dir) { free(s); fclose(f); return TSDB_ERR_NOMEM; }

    s->cols = calloc((size_t)ncols, sizeof(tsdb_col_info_t));
    if (!s->cols) { free(s->dir); free(s); fclose(f); return TSDB_ERR_NOMEM; }

    for (int i = 0; i < ncols && !rc; i++) {
        uint8_t type_raw = 0, pad = 0;
        RD(&type_raw, 1);
        RD(&pad, 1);
        RD(s->cols[i].name, TSDB_MAX_NAME + 1);
        s->cols[i].type   = (tsdb_type_t)type_raw;
        s->cols[i].symtab = NULL;
    }

    /* v2 tail: partition_unit + 3 reserved bytes. Missing on v1 files. */
    if (rc == TSDB_OK && version >= 2) {
        uint8_t part_unit = 0;
        uint8_t reserved[3];
        RD(&part_unit, 1);
        RD(reserved,  3);
        if (rc == TSDB_OK) {
            if (part_unit != TSDB_PARTITION_DAY && part_unit != TSDB_PARTITION_HOUR) {
                rc = TSDB_ERR_CORRUPT;
            } else {
                s->partition_unit = (tsdb_partition_unit_t)part_unit;
            }
        }
    }
    /* v3 tail: block_points u32. Absent on v1/v2 — keep the default. */
    if (rc == TSDB_OK && version >= 3) {
        uint32_t bp = 0;
        RD(&bp, 4);
        if (rc == TSDB_OK) s->block_points = clamp_block_points((int)bp);
    }
#undef RD
    fclose(f);
    if (rc != TSDB_OK) { tsdb_schema_free(s); return rc; }

    /* Load or create symtabs for SYMBOL columns. */
    for (int i = 0; i < ncols; i++) {
        if (s->cols[i].type == TSDB_TYPE_SYMBOL) {
            char sym_path[4096];
            snprintf(sym_path, sizeof(sym_path), "%s/%s.sym", dir, s->cols[i].name);

            /* Try to load existing; create fresh if not present. */
            int r = tsdb_symtab_load(sym_path, &s->cols[i].symtab);
            if (r == TSDB_ERR_IO || r == TSDB_ERR_NOTFOUND) {
                r = tsdb_symtab_new(&s->cols[i].symtab);
            }
            if (r != TSDB_OK) { tsdb_schema_free(s); return r; }
        }
    }

    *out = s;
    return TSDB_OK;
}

/* ---- tsdb_schema_free -------------------------------------------------- */

void tsdb_schema_free(tsdb_schema_t *s) {
    if (!s) return;
    if (s->cols) {
        for (int i = 0; i < s->ncols; i++) {
            if (s->cols[i].symtab) tsdb_symtab_free(s->cols[i].symtab);
        }
        free(s->cols);
    }
    free(s->dir);
    free(s);
}

/* ---- tsdb_schema_col_idx ----------------------------------------------- */

int tsdb_schema_col_idx(tsdb_schema_t *s, const char *name) {
    if (!s || !name) return -1;
    for (int i = 0; i < s->ncols; i++) {
        if (strcmp(s->cols[i].name, name) == 0) return i;
    }
    return -1;
}
