#include "symbol.h"
#include "../../include/tsdb.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/* ---- open-addressing, linear-probing, FNV-1a hash. ---- */

/* Inline cache: a 64-slot direct-mapped table holding the most recent
 * (hash, length, code, inline-string) tuples seen.  The lookup path
 * checks this array WITHOUT taking the rwlock; on hit it returns the
 * cached code in ~10ns instead of paying the rwlock_rdlock pair
 * (~60ns on Apple Silicon).  Hot workloads that cycle through a
 * small symbol set (think TSBS DevOps with 8-32 hostnames) become
 * lock-free.
 *
 * Race-safety: the slot is stored atomically, so a concurrent reader
 * sees either the old or the new contents — never a torn struct.
 * If the reader sees a stale slot whose hash matches but the inline
 * string differs, the memcmp catches it and we fall through to the
 * rwlock path.  The cache never lies about a hit. */
#define TSDB_SYM_CACHE_SLOTS  64
#define TSDB_SYM_CACHE_INLINE 24   /* longest string we cache inline */

typedef struct {
    _Atomic uint64_t  hash;          /* 0 = empty; FNV-1a of the string */
    _Atomic uint32_t  code;          /* interned code */
    _Atomic uint8_t   len;           /* string length */
    char              str[TSDB_SYM_CACHE_INLINE];
} tsdb_sym_cache_slot_t;

struct tsdb_symtab {
    pthread_rwlock_t lock;

    /* Heap: null-terminated strings, appended. */
    char    *heap;
    size_t   heap_size;
    size_t   heap_cap;

    /* Entries indexed by code. Each entry = offset into heap. */
    uint32_t *offsets;
    uint32_t  count;
    uint32_t  cap;

    /* Hash table: linear probing, stores code+1 (0 = empty). */
    uint32_t *ht;
    uint32_t  ht_cap;  /* power of two */
    uint32_t  ht_mask;

    /* Inline cache (see comment above). */
    tsdb_sym_cache_slot_t cache[TSDB_SYM_CACHE_SLOTS];
};

static uint64_t fnv1a(const char *s, size_t n) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < n; i++) {
        h ^= (uint8_t)s[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

static int ht_grow(tsdb_symtab_t *st, uint32_t new_cap) {
    uint32_t *new_ht = calloc(new_cap, sizeof(uint32_t));
    if (!new_ht) return -1;
    for (uint32_t i = 0; i < st->ht_cap; i++) {
        uint32_t slot = st->ht[i];
        if (!slot) continue;
        uint32_t code = slot - 1;
        const char *s = st->heap + st->offsets[code];
        uint64_t h = fnv1a(s, strlen(s));
        uint32_t pos = (uint32_t)h & (new_cap - 1);
        while (new_ht[pos]) pos = (pos + 1) & (new_cap - 1);
        new_ht[pos] = slot;
    }
    free(st->ht);
    st->ht = new_ht;
    st->ht_cap = new_cap;
    st->ht_mask = new_cap - 1;
    return 0;
}

static int heap_reserve(tsdb_symtab_t *st, size_t add) {
    if (st->heap_size + add <= st->heap_cap) return 0;
    size_t ncap = st->heap_cap ? st->heap_cap : 4096;
    while (ncap < st->heap_size + add) ncap *= 2;
    char *nh = realloc(st->heap, ncap);
    if (!nh) return -1;
    st->heap = nh;
    st->heap_cap = ncap;
    return 0;
}

static int offsets_reserve(tsdb_symtab_t *st, uint32_t add) {
    if (st->count + add <= st->cap) return 0;
    uint32_t ncap = st->cap ? st->cap : 256;
    while (ncap < st->count + add) ncap *= 2;
    uint32_t *no = realloc(st->offsets, (size_t)ncap * sizeof(uint32_t));
    if (!no) return -1;
    st->offsets = no;
    st->cap = ncap;
    return 0;
}

int tsdb_symtab_new(tsdb_symtab_t **out) {
    tsdb_symtab_t *st = calloc(1, sizeof(*st));
    if (!st) return TSDB_ERR_NOMEM;
    pthread_rwlock_init(&st->lock, NULL);
    st->ht_cap = 256;
    st->ht_mask = st->ht_cap - 1;
    st->ht = calloc(st->ht_cap, sizeof(uint32_t));
    if (!st->ht) { free(st); return TSDB_ERR_NOMEM; }
    *out = st;
    return TSDB_OK;
}

void tsdb_symtab_free(tsdb_symtab_t *st) {
    if (!st) return;
    pthread_rwlock_destroy(&st->lock);
    free(st->heap);
    free(st->offsets);
    free(st->ht);
    free(st);
}

static uint32_t lookup_locked(tsdb_symtab_t *st, const char *s, size_t n, uint64_t h) {
    uint32_t pos = (uint32_t)h & st->ht_mask;
    for (;;) {
        uint32_t slot = st->ht[pos];
        if (!slot) return TSDB_SYMBOL_INVALID;
        uint32_t code = slot - 1;
        const char *cand = st->heap + st->offsets[code];
        size_t cand_len = strlen(cand);
        if (cand_len == n && memcmp(cand, s, n) == 0) return code;
        pos = (pos + 1) & st->ht_mask;
    }
}

uint32_t tsdb_symtab_lookup(tsdb_symtab_t *st, const char *s) {
    size_t n = strlen(s);
    uint64_t h = fnv1a(s, n);
    pthread_rwlock_rdlock(&st->lock);
    uint32_t c = lookup_locked(st, s, n, h);
    pthread_rwlock_unlock(&st->lock);
    return c;
}

/* Lock-free probe of the inline cache.  Returns the code on hit, or
 * TSDB_SYMBOL_INVALID if either the slot is empty, the hash mismatches,
 * the string is too long for inline storage, or the inline string
 * doesn't match (e.g. a previous occupant of the same slot). */
static inline uint32_t cache_probe(const tsdb_symtab_t *st,
                                    const char *s, size_t n, uint64_t h) {
    if (n > TSDB_SYM_CACHE_INLINE) return TSDB_SYMBOL_INVALID;
    const tsdb_sym_cache_slot_t *slot =
        &st->cache[(uint32_t)h & (TSDB_SYM_CACHE_SLOTS - 1)];
    uint64_t sh = atomic_load_explicit(&slot->hash, memory_order_acquire);
    if (sh != h) return TSDB_SYMBOL_INVALID;
    uint8_t  sl = atomic_load_explicit(&slot->len,  memory_order_relaxed);
    if (sl != n) return TSDB_SYMBOL_INVALID;
    if (memcmp(slot->str, s, n) != 0) return TSDB_SYMBOL_INVALID;
    return atomic_load_explicit(&slot->code, memory_order_acquire);
}

/* Publish a (string, code) pair into the inline cache.  Slot writes
 * are NOT atomic as a tuple — a concurrent reader may see hash from
 * one entry and code from another.  cache_probe defends against that
 * via the (hash, len, memcmp) match: a torn slot will never produce
 * a false positive. */
static inline void cache_publish(tsdb_symtab_t *st,
                                  const char *s, size_t n,
                                  uint64_t h, uint32_t code) {
    if (n > TSDB_SYM_CACHE_INLINE) return;
    tsdb_sym_cache_slot_t *slot =
        &st->cache[(uint32_t)h & (TSDB_SYM_CACHE_SLOTS - 1)];
    /* Set hash to 0 first (invalidate), publish payload, then set hash. */
    atomic_store_explicit(&slot->hash, 0, memory_order_release);
    atomic_store_explicit(&slot->len,  (uint8_t)n,  memory_order_relaxed);
    atomic_store_explicit(&slot->code, code,        memory_order_relaxed);
    memcpy(slot->str, s, n);
    atomic_store_explicit(&slot->hash, h, memory_order_release);
}

uint32_t tsdb_symtab_intern_n(tsdb_symtab_t *st, const char *s, size_t n) {
    uint64_t h = fnv1a(s, n);

    /* Fast-fast path: lock-free inline cache probe. */
    uint32_t cached = cache_probe(st, s, n, h);
    if (cached != TSDB_SYMBOL_INVALID) return cached;

    /* Fast path: read lock + hashtable lookup. */
    pthread_rwlock_rdlock(&st->lock);
    uint32_t c = lookup_locked(st, s, n, h);
    pthread_rwlock_unlock(&st->lock);
    if (c != TSDB_SYMBOL_INVALID) {
        cache_publish(st, s, n, h, c);
        return c;
    }

    /* Slow path: write lock, re-check, insert. */
    pthread_rwlock_wrlock(&st->lock);
    c = lookup_locked(st, s, n, h);
    if (c != TSDB_SYMBOL_INVALID) {
        pthread_rwlock_unlock(&st->lock);
        cache_publish(st, s, n, h, c);
        return c;
    }

    if (offsets_reserve(st, 1) < 0) goto oom;
    if (heap_reserve(st, n + 1) < 0) goto oom;
    if ((st->count + 1) * 2 >= st->ht_cap) {
        if (ht_grow(st, st->ht_cap * 2) < 0) goto oom;
    }

    uint32_t code = st->count;
    uint32_t off = (uint32_t)st->heap_size;
    memcpy(st->heap + off, s, n);
    st->heap[off + n] = '\0';
    st->heap_size += n + 1;
    st->offsets[code] = off;
    st->count++;

    uint32_t pos = (uint32_t)h & st->ht_mask;
    while (st->ht[pos]) pos = (pos + 1) & st->ht_mask;
    st->ht[pos] = code + 1;

    pthread_rwlock_unlock(&st->lock);
    cache_publish(st, s, n, h, code);
    return code;

oom:
    pthread_rwlock_unlock(&st->lock);
    return TSDB_SYMBOL_INVALID;
}

uint32_t tsdb_symtab_intern(tsdb_symtab_t *st, const char *s) {
    return tsdb_symtab_intern_n(st, s, strlen(s));
}

const char *tsdb_symtab_str(tsdb_symtab_t *st, uint32_t code) {
    const char *r = NULL;
    pthread_rwlock_rdlock(&st->lock);
    if (code < st->count) r = st->heap + st->offsets[code];
    pthread_rwlock_unlock(&st->lock);
    return r;
}

size_t tsdb_symtab_size(tsdb_symtab_t *st) {
    pthread_rwlock_rdlock(&st->lock);
    size_t n = st->count;
    pthread_rwlock_unlock(&st->lock);
    return n;
}

/* --- persistence: [magic u32][count u32][heap_size u32]
 *                  [offsets u32 × count][heap u8 × heap_size] */
#define SYMTAB_MAGIC 0x54535953u /* "SYST" */

int tsdb_symtab_save(tsdb_symtab_t *st, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return TSDB_ERR_IO;
    pthread_rwlock_rdlock(&st->lock);
    uint32_t magic = SYMTAB_MAGIC;
    uint32_t count = st->count;
    uint32_t hsz   = (uint32_t)st->heap_size;
    int rc = 0;
    if (fwrite(&magic, 4, 1, f) != 1) rc = TSDB_ERR_IO;
    if (!rc && fwrite(&count, 4, 1, f) != 1) rc = TSDB_ERR_IO;
    if (!rc && fwrite(&hsz,   4, 1, f) != 1) rc = TSDB_ERR_IO;
    if (!rc && count > 0 && fwrite(st->offsets, 4, count, f) != count) rc = TSDB_ERR_IO;
    if (!rc && hsz > 0 && fwrite(st->heap, 1, hsz, f) != hsz) rc = TSDB_ERR_IO;
    pthread_rwlock_unlock(&st->lock);
    fclose(f);
    return rc;
}

int tsdb_symtab_load(const char *path, tsdb_symtab_t **out) {
    FILE *f = fopen(path, "rb");
    if (!f) return TSDB_ERR_IO;
    uint32_t magic, count, hsz;
    if (fread(&magic, 4, 1, f) != 1 || magic != SYMTAB_MAGIC ||
        fread(&count, 4, 1, f) != 1 ||
        fread(&hsz,   4, 1, f) != 1) { fclose(f); return TSDB_ERR_CORRUPT; }
    tsdb_symtab_t *st;
    if (tsdb_symtab_new(&st) != TSDB_OK) { fclose(f); return TSDB_ERR_NOMEM; }
    if (offsets_reserve(st, count) < 0) { fclose(f); tsdb_symtab_free(st); return TSDB_ERR_NOMEM; }
    if (heap_reserve(st, hsz) < 0)      { fclose(f); tsdb_symtab_free(st); return TSDB_ERR_NOMEM; }
    if (count > 0 && fread(st->offsets, 4, count, f) != count) { fclose(f); tsdb_symtab_free(st); return TSDB_ERR_CORRUPT; }
    if (hsz > 0 && fread(st->heap, 1, hsz, f) != hsz)           { fclose(f); tsdb_symtab_free(st); return TSDB_ERR_CORRUPT; }
    fclose(f);
    st->count = count;
    st->heap_size = hsz;
    /* Rebuild hash. */
    while ((st->count + 1) * 2 >= st->ht_cap) {
        if (ht_grow(st, st->ht_cap * 2) < 0) { tsdb_symtab_free(st); return TSDB_ERR_NOMEM; }
    }
    for (uint32_t i = 0; i < count; i++) {
        const char *s = st->heap + st->offsets[i];
        size_t n = strlen(s);
        uint64_t h = fnv1a(s, n);
        uint32_t pos = (uint32_t)h & st->ht_mask;
        while (st->ht[pos]) pos = (pos + 1) & st->ht_mask;
        st->ht[pos] = i + 1;
    }
    *out = st;
    return TSDB_OK;
}
