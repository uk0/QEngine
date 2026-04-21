/* raft_log.c — persistent Raft state (currentTerm, votedFor, log).
 *
 * Durability is the entire point of this module, so every mutating
 * call ends with a write + fsync before it returns.  Readers hit the
 * in-memory cache (an array of entry offsets) to avoid scanning.
 *
 * Log layout on disk:
 *
 *   raft/log/seg-00000001.bin     single append-only segment for now
 *   raft/log/index.bin            u64 starts[] indexed by Raft index
 *
 * We keep a single segment until snapshots land; that keeps truncate
 * trivial (ftruncate + index shrink).  Entry format on disk:
 *
 *   u32 magic    = 'RFTE'
 *   u64 index
 *   u64 term
 *   u32 type
 *   u32 payload_len
 *   u8  payload[payload_len]
 *   u32 trailer   = payload_len (for reverse-scan self-check)
 *
 * meta.bin + vote.bin stay in a fixed 16-byte format so every write
 * is a full rewrite + fsync.  A half-written 16-byte metadata file
 * is detected by a bad magic and reset to zero — a fresh cluster.
 */

#include "raft_log.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define META_MAGIC     0x4D544652u /* 'RFTM' in LE */
#define VOTE_MAGIC     0x56544652u /* 'RFTV' */
#define LOG_ENTRY_MAGIC 0x45544652u /* 'RFTE' */
#define LOG_VERSION     1u

/* Cap on a single log entry payload so a malformed file can't make us
 * allocate gigabytes.  Catalog QTL statements are always small. */
#define MAX_ENTRY_PAYLOAD (1u << 20) /* 1 MB */

struct tsdb_raft_log {
    pthread_mutex_t lock;
    char            base_dir[4096];   /* <data_dir>/raft */
    char            meta_path[4128];  /* base_dir/meta.bin */
    char            vote_path[4128];  /* base_dir/vote.bin */
    char            log_path[4128];   /* base_dir/log/seg.bin */
    char            idx_path[4128];   /* base_dir/log/index.bin */

    uint64_t        current_term;
    uint64_t        voted_for;

    FILE           *log_fp;
    FILE           *idx_fp;

    /* In-memory offset index: offsets[i] = byte offset in log_fp of the
     * entry whose Raft index is (first_index + i).  first_index is 1
     * while snapshotting hasn't landed. */
    uint64_t        first_index;
    uint64_t        last_index;
    uint64_t        last_term;
    uint64_t       *offsets;          /* size = (last_index - first_index + 1) */
    size_t          offsets_cap;
};

/* ---- tiny fs helpers ----------------------------------------------------- */

static int mkdir_p(const char *path) {
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755) == 0 || errno == EEXIST ? 0 : -1;
}

static int fsync_file(FILE *fp) {
    if (fflush(fp) != 0) return -1;
    return fsync(fileno(fp));
}

/* ---- meta / vote persistence --------------------------------------------- */

static int meta_write(tsdb_raft_log_t *rl) {
    FILE *fp = fopen(rl->meta_path, "wb");
    if (!fp) return -1;
    uint32_t m = META_MAGIC, v = LOG_VERSION;
    uint64_t t = rl->current_term;
    int ok = (fwrite(&m, 4, 1, fp) == 1) &&
             (fwrite(&v, 4, 1, fp) == 1) &&
             (fwrite(&t, 8, 1, fp) == 1) &&
             (fsync_file(fp) == 0);
    fclose(fp);
    return ok ? 0 : -1;
}

static int vote_write(tsdb_raft_log_t *rl) {
    FILE *fp = fopen(rl->vote_path, "wb");
    if (!fp) return -1;
    uint32_t m = VOTE_MAGIC, v = LOG_VERSION;
    uint64_t vf = rl->voted_for;
    int ok = (fwrite(&m, 4, 1, fp) == 1) &&
             (fwrite(&v, 4, 1, fp) == 1) &&
             (fwrite(&vf, 8, 1, fp) == 1) &&
             (fsync_file(fp) == 0);
    fclose(fp);
    return ok ? 0 : -1;
}

static void meta_load(tsdb_raft_log_t *rl) {
    FILE *fp = fopen(rl->meta_path, "rb");
    if (!fp) return;
    uint32_t m = 0, v = 0;
    uint64_t t = 0;
    if (fread(&m, 4, 1, fp) == 1 && fread(&v, 4, 1, fp) == 1 &&
        fread(&t, 8, 1, fp) == 1 && m == META_MAGIC) {
        rl->current_term = t;
    }
    fclose(fp);
}

static void vote_load(tsdb_raft_log_t *rl) {
    FILE *fp = fopen(rl->vote_path, "rb");
    if (!fp) return;
    uint32_t m = 0, v = 0;
    uint64_t vf = 0;
    if (fread(&m, 4, 1, fp) == 1 && fread(&v, 4, 1, fp) == 1 &&
        fread(&vf, 8, 1, fp) == 1 && m == VOTE_MAGIC) {
        rl->voted_for = vf;
    }
    fclose(fp);
}

/* ---- log segment scan ---------------------------------------------------- */

static int offsets_reserve(tsdb_raft_log_t *rl, size_t need) {
    if (need <= rl->offsets_cap) return 0;
    size_t cap = rl->offsets_cap ? rl->offsets_cap * 2 : 64;
    while (cap < need) cap *= 2;
    uint64_t *nb = realloc(rl->offsets, cap * sizeof(uint64_t));
    if (!nb) return -1;
    rl->offsets = nb;
    rl->offsets_cap = cap;
    return 0;
}

/* Rebuild the in-memory offset table by scanning the log segment from
 * the beginning.  Called once at open; after that append/truncate keep
 * the table fresh without re-scanning. */
static int log_rebuild_offsets(tsdb_raft_log_t *rl) {
    if (!rl->log_fp) return 0;
    rewind(rl->log_fp);
    rl->first_index = 1;
    rl->last_index  = 0;
    rl->last_term   = 0;

    for (;;) {
        long off_before = ftell(rl->log_fp);
        if (off_before < 0) return -1;
        uint32_t magic = 0;
        size_t n = fread(&magic, 4, 1, rl->log_fp);
        if (n == 0) break; /* clean EOF */
        if (magic != LOG_ENTRY_MAGIC) {
            /* Torn tail — truncate from here, consider it gone. */
            if (ftruncate(fileno(rl->log_fp), off_before) != 0) return -1;
            fseek(rl->log_fp, off_before, SEEK_SET);
            break;
        }
        uint64_t idx, term;
        uint32_t type, plen;
        if (fread(&idx,  8, 1, rl->log_fp) != 1 ||
            fread(&term, 8, 1, rl->log_fp) != 1 ||
            fread(&type, 4, 1, rl->log_fp) != 1 ||
            fread(&plen, 4, 1, rl->log_fp) != 1) {
            ftruncate(fileno(rl->log_fp), off_before);
            fseek(rl->log_fp, off_before, SEEK_SET);
            break;
        }
        if (plen > MAX_ENTRY_PAYLOAD) {
            ftruncate(fileno(rl->log_fp), off_before);
            fseek(rl->log_fp, off_before, SEEK_SET);
            break;
        }
        if (fseek(rl->log_fp, (long)plen, SEEK_CUR) != 0) {
            return -1;
        }
        uint32_t trailer = 0;
        if (fread(&trailer, 4, 1, rl->log_fp) != 1 || trailer != plen) {
            ftruncate(fileno(rl->log_fp), off_before);
            fseek(rl->log_fp, off_before, SEEK_SET);
            break;
        }

        /* Record this offset and advance. */
        if (offsets_reserve(rl, (size_t)idx + 1) != 0) return -1;
        if (rl->last_index == 0) rl->first_index = idx;
        rl->offsets[idx - rl->first_index] = (uint64_t)off_before;
        rl->last_index = idx;
        rl->last_term  = term;
        (void)type;
    }
    return 0;
}

/* ---- public open / close ------------------------------------------------- */

tsdb_raft_log_t *tsdb_raft_log_open(const char *data_dir) {
    if (!data_dir || !*data_dir) return NULL;

    tsdb_raft_log_t *rl = calloc(1, sizeof(*rl));
    if (!rl) return NULL;
    pthread_mutex_init(&rl->lock, NULL);

    snprintf(rl->base_dir, sizeof(rl->base_dir), "%s/raft",          data_dir);
    snprintf(rl->meta_path,sizeof(rl->meta_path),"%s/meta.bin",     rl->base_dir);
    snprintf(rl->vote_path,sizeof(rl->vote_path),"%s/vote.bin",     rl->base_dir);
    snprintf(rl->log_path, sizeof(rl->log_path), "%s/log/seg.bin",  rl->base_dir);
    snprintf(rl->idx_path, sizeof(rl->idx_path), "%s/log/index.bin",rl->base_dir);

    char log_dir[4128];
    snprintf(log_dir, sizeof(log_dir), "%s/log", rl->base_dir);
    if (mkdir_p(rl->base_dir) != 0 || mkdir_p(log_dir) != 0) {
        tsdb_raft_log_close(rl);
        return NULL;
    }

    meta_load(rl);
    vote_load(rl);

    rl->log_fp = fopen(rl->log_path, "a+b");
    if (!rl->log_fp) { tsdb_raft_log_close(rl); return NULL; }
    /* Replace fully-buffered a+ so reads can seek freely. */
    setvbuf(rl->log_fp, NULL, _IONBF, 0);

    if (log_rebuild_offsets(rl) != 0) {
        tsdb_raft_log_close(rl);
        return NULL;
    }
    return rl;
}

void tsdb_raft_log_close(tsdb_raft_log_t *rl) {
    if (!rl) return;
    if (rl->log_fp) fclose(rl->log_fp);
    free(rl->offsets);
    pthread_mutex_destroy(&rl->lock);
    free(rl);
}

/* ---- public accessors ---------------------------------------------------- */

uint64_t tsdb_raft_log_current_term(tsdb_raft_log_t *rl) {
    if (!rl) return 0;
    pthread_mutex_lock(&rl->lock);
    uint64_t v = rl->current_term;
    pthread_mutex_unlock(&rl->lock);
    return v;
}

uint64_t tsdb_raft_log_voted_for(tsdb_raft_log_t *rl) {
    if (!rl) return 0;
    pthread_mutex_lock(&rl->lock);
    uint64_t v = rl->voted_for;
    pthread_mutex_unlock(&rl->lock);
    return v;
}

int tsdb_raft_log_set_term(tsdb_raft_log_t *rl, uint64_t term) {
    if (!rl) return -1;
    pthread_mutex_lock(&rl->lock);
    rl->current_term = term;
    rl->voted_for    = 0;   /* a new term invalidates prior vote */
    int rc_m = meta_write(rl);
    int rc_v = vote_write(rl);
    pthread_mutex_unlock(&rl->lock);
    return (rc_m == 0 && rc_v == 0) ? 0 : -1;
}

int tsdb_raft_log_set_voted_for(tsdb_raft_log_t *rl, uint64_t node_id) {
    if (!rl) return -1;
    pthread_mutex_lock(&rl->lock);
    rl->voted_for = node_id;
    int rc = vote_write(rl);
    pthread_mutex_unlock(&rl->lock);
    return rc;
}

uint64_t tsdb_raft_log_last_index(tsdb_raft_log_t *rl) {
    if (!rl) return 0;
    pthread_mutex_lock(&rl->lock);
    uint64_t v = rl->last_index;
    pthread_mutex_unlock(&rl->lock);
    return v;
}

uint64_t tsdb_raft_log_last_term(tsdb_raft_log_t *rl) {
    if (!rl) return 0;
    pthread_mutex_lock(&rl->lock);
    uint64_t v = rl->last_term;
    pthread_mutex_unlock(&rl->lock);
    return v;
}

uint64_t tsdb_raft_log_term_at(tsdb_raft_log_t *rl, uint64_t index) {
    if (!rl || index == 0) return 0;
    pthread_mutex_lock(&rl->lock);
    uint64_t term = 0;
    if (index >= rl->first_index && index <= rl->last_index) {
        long off = (long)rl->offsets[index - rl->first_index];
        if (fseek(rl->log_fp, off, SEEK_SET) == 0) {
            uint32_t magic = 0;
            uint64_t idx_on_disk = 0;
            uint64_t t = 0;
            if (fread(&magic, 4, 1, rl->log_fp) == 1 && magic == LOG_ENTRY_MAGIC &&
                fread(&idx_on_disk, 8, 1, rl->log_fp) == 1 &&
                fread(&t,    8, 1, rl->log_fp) == 1 &&
                idx_on_disk == index) {
                term = t;
            }
        }
    }
    pthread_mutex_unlock(&rl->lock);
    return term;
}

/* ---- append / read / truncate -------------------------------------------- */

int tsdb_raft_log_append(tsdb_raft_log_t *rl, tsdb_raft_entry_t *entry) {
    if (!rl || !entry) return -1;
    if (entry->payload_len > MAX_ENTRY_PAYLOAD) return -1;

    pthread_mutex_lock(&rl->lock);

    /* Assign the next index. */
    uint64_t idx = rl->last_index + 1;
    if (rl->last_index == 0) rl->first_index = idx;
    entry->index = idx;

    if (offsets_reserve(rl, (size_t)(idx - rl->first_index + 1)) != 0) {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }

    if (fseek(rl->log_fp, 0, SEEK_END) != 0) {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }
    long off = ftell(rl->log_fp);
    if (off < 0) {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }

    uint32_t magic  = LOG_ENTRY_MAGIC;
    uint32_t type   = entry->type;
    uint32_t plen   = entry->payload_len;
    uint64_t idx64  = entry->index;
    uint64_t term64 = entry->term;

    int ok =
        fwrite(&magic,  4, 1, rl->log_fp) == 1 &&
        fwrite(&idx64,  8, 1, rl->log_fp) == 1 &&
        fwrite(&term64, 8, 1, rl->log_fp) == 1 &&
        fwrite(&type,   4, 1, rl->log_fp) == 1 &&
        fwrite(&plen,   4, 1, rl->log_fp) == 1 &&
        (plen == 0 || fwrite(entry->payload, plen, 1, rl->log_fp) == 1) &&
        fwrite(&plen,   4, 1, rl->log_fp) == 1 && /* trailer */
        fsync_file(rl->log_fp) == 0;

    if (!ok) {
        /* Roll back whatever made it to disk. */
        (void)ftruncate(fileno(rl->log_fp), off);
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }

    rl->offsets[idx - rl->first_index] = (uint64_t)off;
    rl->last_index = idx;
    rl->last_term  = entry->term;

    pthread_mutex_unlock(&rl->lock);
    return 0;
}

int tsdb_raft_log_read(tsdb_raft_log_t *rl, uint64_t index,
                        tsdb_raft_entry_t *out)
{
    if (!rl || !out || index == 0) return -1;
    pthread_mutex_lock(&rl->lock);
    if (index < rl->first_index || index > rl->last_index) {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }
    long off = (long)rl->offsets[index - rl->first_index];
    if (fseek(rl->log_fp, off, SEEK_SET) != 0) {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }
    uint32_t magic = 0, type = 0, plen = 0;
    uint64_t idx64 = 0, term64 = 0;
    if (fread(&magic,  4, 1, rl->log_fp) != 1 || magic != LOG_ENTRY_MAGIC ||
        fread(&idx64,  8, 1, rl->log_fp) != 1 ||
        fread(&term64, 8, 1, rl->log_fp) != 1 ||
        fread(&type,   4, 1, rl->log_fp) != 1 ||
        fread(&plen,   4, 1, rl->log_fp) != 1 ||
        plen > MAX_ENTRY_PAYLOAD)
    {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }

    void *buf = NULL;
    if (plen > 0) {
        buf = malloc(plen);
        if (!buf) {
            pthread_mutex_unlock(&rl->lock);
            return -1;
        }
        if (fread(buf, plen, 1, rl->log_fp) != 1) {
            free(buf);
            pthread_mutex_unlock(&rl->lock);
            return -1;
        }
    }

    out->index       = idx64;
    out->term        = term64;
    out->type        = type;
    out->payload_len = plen;
    out->payload     = buf;

    pthread_mutex_unlock(&rl->lock);
    return 0;
}

int tsdb_raft_log_truncate(tsdb_raft_log_t *rl, uint64_t index) {
    if (!rl) return -1;
    pthread_mutex_lock(&rl->lock);
    if (index == 0 || index > rl->last_index) {
        pthread_mutex_unlock(&rl->lock);
        return 0;
    }
    if (index < rl->first_index) {
        /* Can't truncate past the start; treat as no-op. */
        pthread_mutex_unlock(&rl->lock);
        return 0;
    }
    long cut_off = (long)rl->offsets[index - rl->first_index];
    if (ftruncate(fileno(rl->log_fp), cut_off) != 0) {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }
    if (fseek(rl->log_fp, cut_off, SEEK_SET) != 0 ||
        fsync(fileno(rl->log_fp)) != 0) {
        pthread_mutex_unlock(&rl->lock);
        return -1;
    }
    rl->last_index = index - 1;
    if (rl->last_index < rl->first_index) {
        /* Log became empty. */
        rl->first_index = 1;
        rl->last_index  = 0;
        rl->last_term   = 0;
    } else {
        /* Read term of the new last entry. */
        long off = (long)rl->offsets[rl->last_index - rl->first_index];
        if (fseek(rl->log_fp, off + 12, SEEK_SET) == 0) {
            uint64_t t = 0;
            if (fread(&t, 8, 1, rl->log_fp) == 1) rl->last_term = t;
        }
    }
    pthread_mutex_unlock(&rl->lock);
    return 0;
}
