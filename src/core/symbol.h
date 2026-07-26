/* symbol.h — per-table SYMBOL dictionary.
 *
 * Maps strings to monotonically increasing uint32 codes. Thread-safe on
 * lookup and insert via a single rwlock. Persisted via binary
 * (string_heap || offsets_index || hash_map_snapshot).
 */
#ifndef TSDB_CORE_SYMBOL_H
#define TSDB_CORE_SYMBOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#define TSDB_SYMBOL_INVALID UINT32_MAX

typedef struct tsdb_symtab tsdb_symtab_t;

/* Create empty dict. `name` used for persistence path (can be NULL). */
int tsdb_symtab_new(tsdb_symtab_t **out);
void tsdb_symtab_free(tsdb_symtab_t *st);

/* Lookup or insert. Returns assigned code; TSDB_SYMBOL_INVALID on OOM. */
uint32_t tsdb_symtab_intern(tsdb_symtab_t *st, const char *s);
uint32_t tsdb_symtab_intern_n(tsdb_symtab_t *st, const char *s, size_t n);

/* Pure lookup — no insert. Returns INVALID if missing. */
uint32_t tsdb_symtab_lookup(tsdb_symtab_t *st, const char *s);

/* code → string. Returned pointer is valid until table is freed. */
const char *tsdb_symtab_str(tsdb_symtab_t *st, uint32_t code);

/* Count of distinct symbols. */
size_t tsdb_symtab_size(tsdb_symtab_t *st);

/* Persist to / load from file:
 *   [magic u32]["SYST"] [count u32] [heap_size u32]
 *   [offsets u32 x count] [heap u8 x heap_size]
 * The layout is public because the migration SDK ships these bytes between
 * clusters and has to check the codes agree before adopting them. */
#define TSDB_SYMTAB_MAGIC 0x54535953u   /* "SYST" */
int tsdb_symtab_save(tsdb_symtab_t *st, const char *path);
int tsdb_symtab_load(const char *path, tsdb_symtab_t **out);

#endif
