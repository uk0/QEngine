/* udf.h — user-defined scalar function catalog.
 *
 * A registered UDF carries:
 *   - name (globally unique, case-insensitive)
 *   - source path to the .so / .dylib
 *   - exported symbol name inside the library
 *   - argument type list (up to TSDB_UDF_MAX_ARGS)
 *   - return type
 *
 * Persistence: <data_dir>/catalog/functions.log   (append-only)
 *
 *   +fn   <name> <so_path> <symbol> <nargs> <type1>,<type2>,... <ret_type> <created_at>
 *   -fn   <name>
 *
 * On db open the log is replayed top-to-bottom; tombstones remove entries.
 * dlopen() is LAZY — handles are resolved on first call, not on replay.
 * Missing libraries produce TSDB_ERR_NOTFOUND at call time (logged), never
 * silent zero-rows.
 */
#ifndef TSDB_CATALOG_UDF_H
#define TSDB_CATALOG_UDF_H

#include "../../include/tsdb.h"
#include "../../include/tsdb_udf.h"
#include "../core/types.h"

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_UDF_NAME_MAX    64
#define TSDB_UDF_PATH_MAX    1024
#define TSDB_UDF_SYM_MAX     128
#define TSDB_UDF_MAX_ENTRIES 256

typedef struct {
    char              name[TSDB_UDF_NAME_MAX];
    char              so_path[TSDB_UDF_PATH_MAX];
    char              symbol[TSDB_UDF_SYM_MAX];
    int               nargs;
    tsdb_udf_type_t   arg_types[TSDB_UDF_MAX_ARGS];
    tsdb_udf_type_t   ret_type;
    tsdb_ts_t         created_at;

    /* Lazy-resolved on first invocation — not persisted, not replayed. */
    void             *dl_handle;
    tsdb_udf_fn_t     fn;
} tsdb_udf_entry_t;

typedef struct tsdb_udf_catalog tsdb_udf_catalog_t;

/* ─── Lifecycle ──────────────────────────────────────────────────────────── */

int  tsdb_udf_catalog_open (const char *data_dir, tsdb_udf_catalog_t **out);
void tsdb_udf_catalog_close(tsdb_udf_catalog_t *c);

/* ─── Registration ───────────────────────────────────────────────────────── */

/* Register a new UDF. Rejects if:
 *   - name already present      → TSDB_ERR_EXISTS
 *   - name clashes with builtin → TSDB_ERR_EXISTS (prevents shadowing)
 *   - nargs out of range        → TSDB_ERR_INVAL
 *   - bad type enum             → TSDB_ERR_INVAL
 *
 * Does NOT dlopen — that happens lazily at first call. */
int tsdb_udf_catalog_create(tsdb_udf_catalog_t *c,
                             const tsdb_udf_entry_t *e);

/* Drop a UDF. Closes any dlopen handle. Returns TSDB_OK or TSDB_ERR_NOTFOUND. */
int tsdb_udf_catalog_drop(tsdb_udf_catalog_t *c, const char *name);

/* ─── Lookup ─────────────────────────────────────────────────────────────── */

/* Case-insensitive name lookup. Resolves dlopen/dlsym on first call — returns
 * an entry with `fn != NULL` on success.
 *
 * Returns:
 *   TSDB_OK          entry found and resolved
 *   TSDB_ERR_NOTFOUND name not registered, OR .so missing on disk, OR symbol
 *                     not found, OR ABI version mismatch (see errbuf).
 *   TSDB_ERR_UNSUPPORTED  ABI version on the .so does not match
 *
 * If errbuf != NULL, detailed message (dlerror() text etc.) is copied into it.
 */
int tsdb_udf_catalog_lookup(tsdb_udf_catalog_t *c,
                             const char *name,
                             const tsdb_udf_entry_t **out_entry,
                             char *errbuf, size_t errcap);

/* Case-insensitive name clash check against builtin functions. Helper shared
 * by CREATE FUNCTION and the exec dispatcher. */
int tsdb_udf_name_is_builtin(const char *name);

/* ─── Introspection ──────────────────────────────────────────────────────── */

int tsdb_udf_catalog_list(tsdb_udf_catalog_t *c,
                           tsdb_udf_entry_t **out_arr, size_t *out_n);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CATALOG_UDF_H */
