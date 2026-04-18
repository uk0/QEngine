/* tmq.c — TMQ consumer-group metadata store.
 *
 * See tmq.h for the log format and state-machine description.
 *
 * Locking model: one coarse pthread_mutex per tsdb_tmq_t. All mutating
 * operations take the lock, write the log record, fsync, then release.
 * Reads also take the lock to copy group state into caller buffers.
 *
 * This follows the pattern established by src/catalog/catalog.c closely:
 *  - Tab-separated records with percent-encoded strings.
 *  - Append-only log; tombstones remove entries.
 *  - Open → replay → hold FILE* for append.
 */

#include "tmq.h"
#include "../core/types.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <ctype.h>

/* Reuse mkdir_p from schema.c. */
extern int tsdb_mkdir_p(const char *path);

/* ---- percent encode / decode (identical policy to catalog.c) ----------- */

static size_t pct_encode(const char *src, char *dst, size_t dstsz) {
    size_t w = 0;
    for (const char *p = src; *p && w + 4 < dstsz; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '\t' || c == '\n' || c == '\r' || c == '%' || c == ' ') {
            w += (size_t)snprintf(dst + w, dstsz - w, "%%%02X", c);
        } else {
            dst[w++] = (char)c;
        }
    }
    if (w < dstsz) dst[w] = '\0';
    return w;
}

static int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    return -1;
}

static void pct_decode(const char *src, char *dst, size_t dstsz) {
    size_t w = 0;
    for (const char *p = src; *p && w + 1 < dstsz; p++) {
        if (*p == '%' && p[1] && p[2]) {
            int hi = hex_val(p[1]), lo = hex_val(p[2]);
            if (hi >= 0 && lo >= 0) {
                dst[w++] = (char)((hi << 4) | lo);
                p += 2;
                continue;
            }
        }
        dst[w++] = *p;
    }
    if (w < dstsz) dst[w] = '\0';
}

static void chomp(char *s) {
    size_t n = strlen(s);
    while (n > 0 && (s[n-1] == '\n' || s[n-1] == '\r')) s[--n] = '\0';
}

static int split_tab(char *line, char **fields, int max_fields) {
    int n = 0;
    char *p = line;
    while (n < max_fields) {
        fields[n++] = p;
        p = strchr(p, '\t');
        if (!p) break;
        *p++ = '\0';
    }
    return n;
}

/* ---- tsdb_tmq struct ---------------------------------------------------- */

struct tsdb_tmq {
    char              cat_dir[4096];
    char              log_path[4096];
    FILE             *log;
    pthread_mutex_t   lock;

    tsdb_tmq_group_t  groups[TSDB_TMQ_MAX_GROUPS];
    int               ngroups;
};

/* ---- Internal helpers -------------------------------------------------- */

/* Find group by name; returns index in groups[] or -1. Caller holds lock. */
static int find_group(tsdb_tmq_t *t, const char *name) {
    for (int i = 0; i < t->ngroups; i++) {
        if (strcmp(t->groups[i].name, name) == 0) return i;
    }
    return -1;
}

/* Find member index in a group; returns -1 if absent. Caller holds lock. */
static int find_member(const tsdb_tmq_group_t *g, const char *consumer_id) {
    for (int i = 0; i < g->nmembers; i++) {
        if (strcmp(g->members[i].consumer_id, consumer_id) == 0) return i;
    }
    return -1;
}

/* Round-robin reassign partitions across current members. If nmembers == 0,
 * all partitions become unowned (-1). */
static void rebalance(tsdb_tmq_group_t *g) {
    for (int p = 0; p < g->npart; p++) {
        g->partition_owner[p] = (g->nmembers > 0) ? (p % g->nmembers) : -1;
    }
}

/* ---- Log I/O ----------------------------------------------------------- */

static int log_group_create(tsdb_tmq_t *t, const tsdb_tmq_group_t *g) {
    char en[128], et[128];
    pct_encode(g->name, en, sizeof(en));
    pct_encode(g->topic, et, sizeof(et));
    int n = fprintf(t->log, "+group\t%s\t%s\t%d\t%lld\n",
                    en, et, g->npart, (long long)g->created_at);
    if (n < 0 || fflush(t->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_group_drop(tsdb_tmq_t *t, const char *name) {
    char en[128];
    pct_encode(name, en, sizeof(en));
    int n = fprintf(t->log, "-group\t%s\n", en);
    if (n < 0 || fflush(t->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_member_add(tsdb_tmq_t *t, const char *group,
                          const char *consumer_id, tsdb_ts_t joined_at) {
    char eg[128], ec[128];
    pct_encode(group, eg, sizeof(eg));
    pct_encode(consumer_id, ec, sizeof(ec));
    int n = fprintf(t->log, "+member\t%s\t%s\t%lld\n",
                    eg, ec, (long long)joined_at);
    if (n < 0 || fflush(t->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_member_drop(tsdb_tmq_t *t, const char *group,
                           const char *consumer_id) {
    char eg[128], ec[128];
    pct_encode(group, eg, sizeof(eg));
    pct_encode(consumer_id, ec, sizeof(ec));
    int n = fprintf(t->log, "-member\t%s\t%s\n", eg, ec);
    if (n < 0 || fflush(t->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_offset(tsdb_tmq_t *t, const char *group,
                      const char *consumer_id, int partition, int64_t seq) {
    char eg[128], ec[128];
    pct_encode(group, eg, sizeof(eg));
    pct_encode(consumer_id, ec, sizeof(ec));
    int n = fprintf(t->log, "~offset\t%s\t%s\t%d\t%lld\n",
                    eg, ec, partition, (long long)seq);
    if (n < 0 || fflush(t->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

/* ---- Replay ------------------------------------------------------------ */

static void apply_group_create(tsdb_tmq_t *t, const char *name,
                               const char *topic, int npart,
                               tsdb_ts_t created_at) {
    if (find_group(t, name) >= 0) return; /* idempotent */
    if (t->ngroups >= TSDB_TMQ_MAX_GROUPS) return;
    tsdb_tmq_group_t *g = &t->groups[t->ngroups++];
    memset(g, 0, sizeof(*g));
    snprintf(g->name, sizeof(g->name), "%s", name);
    snprintf(g->topic, sizeof(g->topic), "%s", topic);
    g->npart = (npart > 0 && npart <= TSDB_TMQ_NPARTITIONS)
               ? npart : TSDB_TMQ_NPARTITIONS;
    g->created_at = created_at;
    rebalance(g); /* all -1 at this point */
}

static void apply_group_drop(tsdb_tmq_t *t, const char *name) {
    int idx = find_group(t, name);
    if (idx < 0) return;
    /* Swap-remove last into idx. */
    if (idx != t->ngroups - 1) {
        t->groups[idx] = t->groups[t->ngroups - 1];
    }
    t->ngroups--;
}

static void apply_member_add(tsdb_tmq_t *t, const char *group,
                             const char *consumer_id, tsdb_ts_t joined_at) {
    int gi = find_group(t, group);
    if (gi < 0) return;
    tsdb_tmq_group_t *g = &t->groups[gi];
    if (find_member(g, consumer_id) >= 0) return;
    if (g->nmembers >= TSDB_TMQ_MAX_MEMBERS) return;
    tsdb_tmq_member_t *m = &g->members[g->nmembers++];
    memset(m, 0, sizeof(*m));
    snprintf(m->consumer_id, sizeof(m->consumer_id), "%s", consumer_id);
    m->joined_at = joined_at;
    rebalance(g);
}

static void apply_member_drop(tsdb_tmq_t *t, const char *group,
                              const char *consumer_id) {
    int gi = find_group(t, group);
    if (gi < 0) return;
    tsdb_tmq_group_t *g = &t->groups[gi];
    int mi = find_member(g, consumer_id);
    if (mi < 0) return;
    if (mi != g->nmembers - 1) {
        g->members[mi] = g->members[g->nmembers - 1];
    }
    g->nmembers--;
    rebalance(g);
}

static void apply_offset(tsdb_tmq_t *t, const char *group,
                         const char *consumer_id, int partition, int64_t seq) {
    int gi = find_group(t, group);
    if (gi < 0) return;
    tsdb_tmq_group_t *g = &t->groups[gi];
    int mi = find_member(g, consumer_id);
    if (mi < 0) return;
    if (partition < 0 || partition >= g->npart) return;
    g->members[mi].committed_offsets[partition] = seq;
}

static int replay_log(tsdb_tmq_t *t) {
    FILE *f = fopen(t->log_path, "r");
    if (!f) return TSDB_OK;  /* no log yet */

    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        chomp(line);
        if (!line[0]) continue;

        char linecopy[4096];
        snprintf(linecopy, sizeof(linecopy), "%s", line);
        char *fields[8];
        int nf = split_tab(linecopy, fields, 8);
        if (nf < 1) continue;

        if (strcmp(fields[0], "+group") == 0 && nf >= 5) {
            char name[TSDB_TMQ_NAME_MAX], topic[TSDB_TMQ_TOPIC_MAX];
            pct_decode(fields[1], name,  sizeof(name));
            pct_decode(fields[2], topic, sizeof(topic));
            int npart = atoi(fields[3]);
            tsdb_ts_t ca = (tsdb_ts_t)strtoll(fields[4], NULL, 10);
            apply_group_create(t, name, topic, npart, ca);

        } else if (strcmp(fields[0], "-group") == 0 && nf >= 2) {
            char name[TSDB_TMQ_NAME_MAX];
            pct_decode(fields[1], name, sizeof(name));
            apply_group_drop(t, name);

        } else if (strcmp(fields[0], "+member") == 0 && nf >= 4) {
            char g[TSDB_TMQ_NAME_MAX], c[TSDB_TMQ_ID_MAX];
            pct_decode(fields[1], g, sizeof(g));
            pct_decode(fields[2], c, sizeof(c));
            tsdb_ts_t ja = (tsdb_ts_t)strtoll(fields[3], NULL, 10);
            apply_member_add(t, g, c, ja);

        } else if (strcmp(fields[0], "-member") == 0 && nf >= 3) {
            char g[TSDB_TMQ_NAME_MAX], c[TSDB_TMQ_ID_MAX];
            pct_decode(fields[1], g, sizeof(g));
            pct_decode(fields[2], c, sizeof(c));
            apply_member_drop(t, g, c);

        } else if (strcmp(fields[0], "~offset") == 0 && nf >= 5) {
            char g[TSDB_TMQ_NAME_MAX], c[TSDB_TMQ_ID_MAX];
            pct_decode(fields[1], g, sizeof(g));
            pct_decode(fields[2], c, sizeof(c));
            int part = atoi(fields[3]);
            int64_t seq = strtoll(fields[4], NULL, 10);
            apply_offset(t, g, c, part, seq);
        }
    }
    fclose(f);
    return TSDB_OK;
}

/* ---- Open / close ------------------------------------------------------ */

int tsdb_tmq_open(const char *data_dir, tsdb_tmq_t **out) {
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    tsdb_tmq_t *t = calloc(1, sizeof(*t));
    if (!t) return TSDB_ERR_NOMEM;

    snprintf(t->cat_dir, sizeof(t->cat_dir), "%s/catalog", data_dir);
    snprintf(t->log_path, sizeof(t->log_path),
             "%s/consumer_groups.log", t->cat_dir);

    if (tsdb_mkdir_p(t->cat_dir) < 0) {
        free(t);
        return TSDB_ERR_IO;
    }

    pthread_mutex_init(&t->lock, NULL);

    int rc = replay_log(t);
    if (rc != TSDB_OK) {
        pthread_mutex_destroy(&t->lock);
        free(t);
        return rc;
    }

    t->log = fopen(t->log_path, "a");
    if (!t->log) {
        pthread_mutex_destroy(&t->lock);
        free(t);
        return TSDB_ERR_IO;
    }

    *out = t;
    return TSDB_OK;
}

void tsdb_tmq_close(tsdb_tmq_t *t) {
    if (!t) return;
    pthread_mutex_lock(&t->lock);
    if (t->log) { fclose(t->log); t->log = NULL; }
    pthread_mutex_unlock(&t->lock);
    pthread_mutex_destroy(&t->lock);
    free(t);
}

/* ---- Group lifecycle --------------------------------------------------- */

int tsdb_tmq_group_create(tsdb_tmq_t *t,
                          const char *name,
                          const char *topic)
{
    if (!t || !name || !*name || !topic || !*topic) return TSDB_ERR_INVAL;
    if (strlen(name) >= TSDB_TMQ_NAME_MAX) return TSDB_ERR_INVAL;
    if (strlen(topic) >= TSDB_TMQ_TOPIC_MAX) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&t->lock);

    if (find_group(t, name) >= 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_EXISTS;
    }
    if (t->ngroups >= TSDB_TMQ_MAX_GROUPS) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_FULL;
    }

    tsdb_tmq_group_t g;
    memset(&g, 0, sizeof(g));
    snprintf(g.name,  sizeof(g.name),  "%s", name);
    snprintf(g.topic, sizeof(g.topic), "%s", topic);
    g.npart      = TSDB_TMQ_NPARTITIONS;
    g.created_at = tsdb_now_ns();
    rebalance(&g);

    int rc = log_group_create(t, &g);
    if (rc != TSDB_OK) {
        pthread_mutex_unlock(&t->lock);
        return rc;
    }
    t->groups[t->ngroups++] = g;

    pthread_mutex_unlock(&t->lock);
    return TSDB_OK;
}

int tsdb_tmq_group_drop(tsdb_tmq_t *t, const char *name) {
    if (!t || !name) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&t->lock);
    int idx = find_group(t, name);
    if (idx < 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_NOTFOUND;
    }
    int rc = log_group_drop(t, name);
    if (rc == TSDB_OK) apply_group_drop(t, name);
    pthread_mutex_unlock(&t->lock);
    return rc;
}

int tsdb_tmq_group_get(tsdb_tmq_t *t, const char *name,
                       tsdb_tmq_group_t *out) {
    if (!t || !name || !out) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&t->lock);
    int idx = find_group(t, name);
    if (idx < 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_NOTFOUND;
    }
    *out = t->groups[idx];
    pthread_mutex_unlock(&t->lock);
    return TSDB_OK;
}

int tsdb_tmq_list(tsdb_tmq_t *t,
                  tsdb_tmq_group_t **out_arr,
                  size_t *out_n)
{
    if (!t || !out_arr || !out_n) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&t->lock);
    size_t n = (size_t)t->ngroups;
    if (n == 0) {
        *out_arr = NULL; *out_n = 0;
        pthread_mutex_unlock(&t->lock);
        return TSDB_OK;
    }
    tsdb_tmq_group_t *arr = malloc(n * sizeof(tsdb_tmq_group_t));
    if (!arr) { pthread_mutex_unlock(&t->lock); return TSDB_ERR_NOMEM; }
    memcpy(arr, t->groups, n * sizeof(tsdb_tmq_group_t));
    *out_arr = arr;
    *out_n   = n;
    pthread_mutex_unlock(&t->lock);
    return TSDB_OK;
}

/* ---- Membership -------------------------------------------------------- */

int tsdb_tmq_join(tsdb_tmq_t *t,
                  const char *group,
                  const char *consumer_id)
{
    if (!t || !group || !consumer_id || !*consumer_id) return TSDB_ERR_INVAL;
    if (strlen(consumer_id) >= TSDB_TMQ_ID_MAX) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&t->lock);
    int gi = find_group(t, group);
    if (gi < 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_NOTFOUND;
    }
    tsdb_tmq_group_t *g = &t->groups[gi];
    if (find_member(g, consumer_id) >= 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_EXISTS;
    }
    if (g->nmembers >= TSDB_TMQ_MAX_MEMBERS) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_FULL;
    }

    tsdb_ts_t now = tsdb_now_ns();
    int rc = log_member_add(t, group, consumer_id, now);
    if (rc == TSDB_OK) {
        apply_member_add(t, group, consumer_id, now);
    }
    pthread_mutex_unlock(&t->lock);
    return rc;
}

int tsdb_tmq_leave(tsdb_tmq_t *t,
                   const char *group,
                   const char *consumer_id)
{
    if (!t || !group || !consumer_id) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&t->lock);
    int gi = find_group(t, group);
    if (gi < 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_NOTFOUND;
    }
    tsdb_tmq_group_t *g = &t->groups[gi];
    if (find_member(g, consumer_id) < 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_NOTFOUND;
    }
    int rc = log_member_drop(t, group, consumer_id);
    if (rc == TSDB_OK) {
        apply_member_drop(t, group, consumer_id);
    }
    pthread_mutex_unlock(&t->lock);
    return rc;
}

/* ---- Offsets ----------------------------------------------------------- */

int tsdb_tmq_commit(tsdb_tmq_t *t,
                    const char *group,
                    const char *consumer_id,
                    int64_t seq)
{
    if (!t || !group || !consumer_id) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&t->lock);
    int gi = find_group(t, group);
    if (gi < 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_NOTFOUND;
    }
    tsdb_tmq_group_t *g = &t->groups[gi];
    int mi = find_member(g, consumer_id);
    if (mi < 0) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_NOTFOUND;
    }

    /* Collect owned partitions first so we reject early if ANY would go
     * backwards (monotonic offset across the set). */
    int owned[TSDB_TMQ_NPARTITIONS];
    int nown = 0;
    for (int p = 0; p < g->npart; p++) {
        if (g->partition_owner[p] == mi) {
            owned[nown++] = p;
        }
    }
    /* Enforce monotone per owned partition. */
    for (int i = 0; i < nown; i++) {
        int64_t cur = g->members[mi].committed_offsets[owned[i]];
        if (seq < cur) {
            pthread_mutex_unlock(&t->lock);
            return TSDB_ERR_INVAL;
        }
    }

    /* Persist + apply. */
    int rc = TSDB_OK;
    for (int i = 0; i < nown && rc == TSDB_OK; i++) {
        rc = log_offset(t, group, consumer_id, owned[i], seq);
        if (rc == TSDB_OK) {
            g->members[mi].committed_offsets[owned[i]] = seq;
        }
    }
    pthread_mutex_unlock(&t->lock);
    return rc;
}

int tsdb_tmq_get_offset(tsdb_tmq_t *t,
                        const char *group,
                        const char *consumer_id,
                        int partition,
                        int64_t *out_seq)
{
    if (!t || !group || !consumer_id || !out_seq) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&t->lock);
    int gi = find_group(t, group);
    if (gi < 0) { pthread_mutex_unlock(&t->lock); return TSDB_ERR_NOTFOUND; }
    tsdb_tmq_group_t *g = &t->groups[gi];
    int mi = find_member(g, consumer_id);
    if (mi < 0) { pthread_mutex_unlock(&t->lock); return TSDB_ERR_NOTFOUND; }
    if (partition < 0 || partition >= g->npart) {
        pthread_mutex_unlock(&t->lock);
        return TSDB_ERR_INVAL;
    }
    *out_seq = g->members[mi].committed_offsets[partition];
    pthread_mutex_unlock(&t->lock);
    return TSDB_OK;
}
