/* raft_config.c — persistent master set.
 *
 * On-disk format (raft/config.bin):
 *   magic u32 = 'RFCF'
 *   version u32 = 1
 *   count u32
 *   [u64 id | u8 addr_len | addr[addr_len]] * count
 *
 * Every mutation rewrites the whole file + fsync — the file is tiny
 * (< 1 KB even with 16 members) so copy-on-write overhead is trivial
 * relative to the safety guarantee.
 */

#include "raft_config.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CFG_MAGIC 0x46434652u /* 'RFCF' */
#define CFG_VERSION 1u

struct tsdb_raft_config {
    pthread_mutex_t lock;
    char            path[4200];
    tsdb_raft_cfg_member_t members[TSDB_RAFT_CFG_MAX_MASTERS];
    int             count;
    int             loaded;  /* 1 = read from disk at open; 0 = fresh */
};

static int cfg_write_locked(tsdb_raft_config_t *cfg) {
    char tmp[4232];
    snprintf(tmp, sizeof(tmp), "%s.tmp", cfg->path);
    FILE *fp = fopen(tmp, "wb");
    if (!fp) return -1;
    uint32_t m = CFG_MAGIC, v = CFG_VERSION, c = (uint32_t)cfg->count;
    if (fwrite(&m, 4, 1, fp) != 1 ||
        fwrite(&v, 4, 1, fp) != 1 ||
        fwrite(&c, 4, 1, fp) != 1) { fclose(fp); unlink(tmp); return -1; }
    for (int i = 0; i < cfg->count; i++) {
        uint64_t id = cfg->members[i].id;
        uint8_t  al = (uint8_t)strlen(cfg->members[i].addr);
        if (fwrite(&id, 8, 1, fp) != 1 ||
            fwrite(&al, 1, 1, fp) != 1 ||
            (al > 0 && fwrite(cfg->members[i].addr, al, 1, fp) != 1))
        { fclose(fp); unlink(tmp); return -1; }
    }
    if (fflush(fp) != 0 || fsync(fileno(fp)) != 0) {
        fclose(fp); unlink(tmp); return -1;
    }
    fclose(fp);
    if (rename(tmp, cfg->path) != 0) return -1;
    return 0;
}

static void cfg_load_locked(tsdb_raft_config_t *cfg) {
    FILE *fp = fopen(cfg->path, "rb");
    if (!fp) return;
    uint32_t m = 0, v = 0, c = 0;
    if (fread(&m, 4, 1, fp) != 1 || m != CFG_MAGIC ||
        fread(&v, 4, 1, fp) != 1 ||
        fread(&c, 4, 1, fp) != 1 ||
        c > TSDB_RAFT_CFG_MAX_MASTERS) {
        fclose(fp); return;
    }
    for (uint32_t i = 0; i < c; i++) {
        uint64_t id = 0;
        uint8_t  al = 0;
        if (fread(&id, 8, 1, fp) != 1 || fread(&al, 1, 1, fp) != 1) {
            fclose(fp); return;
        }
        char addr[80] = {0};
        if (al > 79) al = 79;
        if (al > 0 && fread(addr, al, 1, fp) != 1) { fclose(fp); return; }
        addr[al] = '\0';
        cfg->members[cfg->count].id = id;
        snprintf(cfg->members[cfg->count].addr,
                 sizeof(cfg->members[cfg->count].addr), "%s", addr);
        cfg->count++;
    }
    cfg->loaded = 1;
    fclose(fp);
}

tsdb_raft_config_t *tsdb_raft_config_open(const char *data_dir) {
    if (!data_dir) return NULL;
    tsdb_raft_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) return NULL;
    pthread_mutex_init(&cfg->lock, NULL);
    snprintf(cfg->path, sizeof(cfg->path), "%s/raft/config.bin", data_dir);
    cfg_load_locked(cfg);
    return cfg;
}

void tsdb_raft_config_close(tsdb_raft_config_t *cfg) {
    if (!cfg) return;
    pthread_mutex_destroy(&cfg->lock);
    free(cfg);
}

int tsdb_raft_config_is_initialised(const tsdb_raft_config_t *cfg) {
    return cfg && cfg->loaded;
}

int tsdb_raft_config_set(tsdb_raft_config_t *cfg,
                          const tsdb_raft_cfg_member_t *members, int n)
{
    if (!cfg || n < 0 || n > TSDB_RAFT_CFG_MAX_MASTERS) return -1;
    pthread_mutex_lock(&cfg->lock);
    cfg->count = n;
    for (int i = 0; i < n; i++) {
        cfg->members[i].id = members[i].id;
        snprintf(cfg->members[i].addr,
                 sizeof(cfg->members[i].addr), "%s", members[i].addr);
    }
    int rc = cfg_write_locked(cfg);
    if (rc == 0) cfg->loaded = 1;
    pthread_mutex_unlock(&cfg->lock);
    return rc;
}

int tsdb_raft_config_add(tsdb_raft_config_t *cfg,
                          uint64_t id, const char *addr)
{
    if (!cfg) return -1;
    pthread_mutex_lock(&cfg->lock);
    for (int i = 0; i < cfg->count; i++) {
        if (cfg->members[i].id == id) {
            /* Update addr if different; idempotent on id. */
            if (addr) snprintf(cfg->members[i].addr,
                               sizeof(cfg->members[i].addr), "%s", addr);
            int rc = cfg_write_locked(cfg);
            pthread_mutex_unlock(&cfg->lock);
            return rc;
        }
    }
    if (cfg->count >= TSDB_RAFT_CFG_MAX_MASTERS) {
        pthread_mutex_unlock(&cfg->lock);
        return -1;
    }
    cfg->members[cfg->count].id = id;
    snprintf(cfg->members[cfg->count].addr,
             sizeof(cfg->members[cfg->count].addr), "%s",
             addr ? addr : "");
    cfg->count++;
    int rc = cfg_write_locked(cfg);
    if (rc == 0) cfg->loaded = 1;
    pthread_mutex_unlock(&cfg->lock);
    return rc;
}

int tsdb_raft_config_remove(tsdb_raft_config_t *cfg, uint64_t id) {
    if (!cfg) return -1;
    pthread_mutex_lock(&cfg->lock);
    int found = -1;
    for (int i = 0; i < cfg->count; i++) {
        if (cfg->members[i].id == id) { found = i; break; }
    }
    if (found < 0) {
        pthread_mutex_unlock(&cfg->lock);
        return 0; /* idempotent */
    }
    for (int i = found; i < cfg->count - 1; i++) {
        cfg->members[i] = cfg->members[i + 1];
    }
    cfg->count--;
    int rc = cfg_write_locked(cfg);
    pthread_mutex_unlock(&cfg->lock);
    return rc;
}

int tsdb_raft_config_snapshot(tsdb_raft_config_t *cfg,
                               tsdb_raft_cfg_member_t *out, int cap)
{
    if (!cfg || !out || cap <= 0) return 0;
    pthread_mutex_lock(&cfg->lock);
    int n = cfg->count < cap ? cfg->count : cap;
    for (int i = 0; i < n; i++) out[i] = cfg->members[i];
    pthread_mutex_unlock(&cfg->lock);
    return n;
}

int tsdb_raft_config_count(tsdb_raft_config_t *cfg) {
    if (!cfg) return 0;
    pthread_mutex_lock(&cfg->lock);
    int n = cfg->count;
    pthread_mutex_unlock(&cfg->lock);
    return n;
}
