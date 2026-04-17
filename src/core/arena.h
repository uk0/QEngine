/* arena.h — bump-pointer arena allocator.
 *
 * Arenas own chunks of memory and hand out aligned slices with O(1) alloc and
 * O(1) bulk free (no per-object free). Intended for parser ASTs, transient
 * query plans, per-request scratch. Not thread-safe.
 */
#ifndef TSDB_CORE_ARENA_H
#define TSDB_CORE_ARENA_H

#include <stddef.h>
#include <stdint.h>

typedef struct tsdb_arena_chunk tsdb_arena_chunk_t;

typedef struct {
    tsdb_arena_chunk_t *head;
    size_t              chunk_size;
    size_t              total_allocated;
    size_t              total_used;
} tsdb_arena_t;

/* Initialize an arena with a target chunk size (will grow dynamically). */
void  tsdb_arena_init(tsdb_arena_t *a, size_t chunk_size);
void *tsdb_arena_alloc(tsdb_arena_t *a, size_t sz);
void *tsdb_arena_alloc_aligned(tsdb_arena_t *a, size_t sz, size_t align);
char *tsdb_arena_strdup(tsdb_arena_t *a, const char *s);
char *tsdb_arena_strndup(tsdb_arena_t *a, const char *s, size_t n);
void  tsdb_arena_reset(tsdb_arena_t *a);
void  tsdb_arena_free(tsdb_arena_t *a);

#endif
