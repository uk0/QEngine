#include "arena.h"
#include <stdlib.h>
#include <string.h>

struct tsdb_arena_chunk {
    tsdb_arena_chunk_t *next;
    size_t              cap;
    size_t              used;
    unsigned char       data[];
};

#define DEFAULT_CHUNK (64 * 1024)

static tsdb_arena_chunk_t *new_chunk(size_t cap) {
    tsdb_arena_chunk_t *c = malloc(sizeof(*c) + cap);
    if (!c) return NULL;
    c->next = NULL;
    c->cap  = cap;
    c->used = 0;
    return c;
}

void tsdb_arena_init(tsdb_arena_t *a, size_t chunk_size) {
    a->head = NULL;
    a->chunk_size = chunk_size ? chunk_size : DEFAULT_CHUNK;
    a->total_allocated = 0;
    a->total_used = 0;
}

void *tsdb_arena_alloc_aligned(tsdb_arena_t *a, size_t sz, size_t align) {
    if (!a->head || align == 0) {
        if (align == 0) align = 8;
    }
    if (align == 0) align = 8;

    /* Try head */
    tsdb_arena_chunk_t *c = a->head;
    if (c) {
        uintptr_t base = (uintptr_t)(c->data + c->used);
        uintptr_t aligned = (base + align - 1) & ~((uintptr_t)align - 1);
        size_t pad = (size_t)(aligned - base);
        if (c->used + pad + sz <= c->cap) {
            void *p = c->data + c->used + pad;
            c->used += pad + sz;
            a->total_used += pad + sz;
            return p;
        }
    }

    /* Need a new chunk. Oversize allocations get dedicated chunks. */
    size_t new_cap = a->chunk_size;
    if (sz + align > new_cap) new_cap = sz + align;
    tsdb_arena_chunk_t *nc = new_chunk(new_cap);
    if (!nc) return NULL;
    nc->next = a->head;
    a->head  = nc;
    a->total_allocated += new_cap;

    uintptr_t base = (uintptr_t)nc->data;
    uintptr_t aligned = (base + align - 1) & ~((uintptr_t)align - 1);
    size_t pad = (size_t)(aligned - base);
    void *p = nc->data + pad;
    nc->used = pad + sz;
    a->total_used += pad + sz;
    return p;
}

void *tsdb_arena_alloc(tsdb_arena_t *a, size_t sz) {
    return tsdb_arena_alloc_aligned(a, sz, 8);
}

char *tsdb_arena_strdup(tsdb_arena_t *a, const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s);
    return tsdb_arena_strndup(a, s, n);
}

char *tsdb_arena_strndup(tsdb_arena_t *a, const char *s, size_t n) {
    char *p = tsdb_arena_alloc(a, n + 1);
    if (!p) return NULL;
    memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

void tsdb_arena_reset(tsdb_arena_t *a) {
    for (tsdb_arena_chunk_t *c = a->head; c; c = c->next) c->used = 0;
    a->total_used = 0;
}

void tsdb_arena_free(tsdb_arena_t *a) {
    tsdb_arena_chunk_t *c = a->head;
    while (c) {
        tsdb_arena_chunk_t *n = c->next;
        free(c);
        c = n;
    }
    a->head = NULL;
    a->total_allocated = 0;
    a->total_used = 0;
}
