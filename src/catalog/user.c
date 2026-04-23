/* user.c — RBAC implementation (see user.h for format + semantics).
 *
 * Locking model: single pthread_mutex per tsdb_auth_t protects users[],
 * grants[], tokens[], and the log file.  All mutating operations take the
 * lock, mutate in-memory state, append + fflush the log, then unlock.
 *
 * Pattern mirrors src/catalog/tmq.c (append-only log, percent-encoded
 * fields, replay on open).
 */

#include "user.h"
#include "../../include/tsdb.h"
#include "../core/types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctype.h>
#include <time.h>

#include <openssl/evp.h>
#include <openssl/rand.h>

extern int tsdb_mkdir_p(const char *path);

/* ---- percent encode / decode (policy matches catalog.c / tmq.c) -------- */

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

static int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    return -1;
}

static void pct_decode(const char *src, char *dst, size_t dstsz) {
    size_t w = 0;
    for (const char *p = src; *p && w + 1 < dstsz; p++) {
        if (*p == '%' && p[1] && p[2]) {
            int hi = hex_nibble(p[1]), lo = hex_nibble(p[2]);
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

/* ---- hex conversion ---------------------------------------------------- */

static void bytes_to_hex(const uint8_t *in, size_t n, char *out) {
    static const char H[] = "0123456789abcdef";
    for (size_t i = 0; i < n; i++) {
        out[2*i]     = H[(in[i] >> 4) & 0xF];
        out[2*i + 1] = H[ in[i]       & 0xF];
    }
    out[2*n] = '\0';
}

/* Returns 0 on success, -1 on bad input. out must hold n bytes. */
static int hex_to_bytes(const char *in, size_t n, uint8_t *out) {
    for (size_t i = 0; i < n; i++) {
        int hi = hex_nibble(in[2*i]);
        int lo = hex_nibble(in[2*i + 1]);
        if (hi < 0 || lo < 0) return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 0;
}

/* ---- Password hashing -------------------------------------------------- */

/* PBKDF2-HMAC-SHA256.  Returns 0 on success, -1 on failure. */
static int derive_hash(const char *password,
                        const uint8_t *salt, size_t saltlen,
                        uint8_t *out, size_t outlen) {
    if (!password || !salt || !out) return -1;
    int rc = PKCS5_PBKDF2_HMAC(password, (int)strlen(password),
                                salt, (int)saltlen,
                                TSDB_USER_PBKDF2_ROUNDS,
                                EVP_sha256(),
                                (int)outlen, out);
    return rc == 1 ? 0 : -1;
}

/* Constant-time compare. */
static int ct_memcmp(const void *a, const void *b, size_t n) {
    const uint8_t *x = (const uint8_t *)a;
    const uint8_t *y = (const uint8_t *)b;
    uint8_t d = 0;
    for (size_t i = 0; i < n; i++) d |= (uint8_t)(x[i] ^ y[i]);
    return d == 0 ? 0 : 1;
}

/* ---- In-memory state --------------------------------------------------- */

typedef struct {
    char               token[TSDB_USER_TOKEN_CHARS + 1];
    char               user[TSDB_USER_NAME_MAX];
    tsdb_user_role_t   role;
} token_entry_t;

struct tsdb_auth {
    char              cat_dir[4096];
    char              log_path[4096];
    FILE             *log;
    pthread_mutex_t   lock;

    tsdb_user_t       users[TSDB_USER_MAX_USERS];
    int               nusers;

    tsdb_user_grant_t grants[TSDB_USER_MAX_GRANTS];
    int               ngrants;

    token_entry_t     tokens[TSDB_USER_MAX_TOKENS];
    int               ntokens;
};

/* ---- Helpers (caller holds lock) --------------------------------------- */

static int find_user_idx(tsdb_auth_t *a, const char *name) {
    for (int i = 0; i < a->nusers; i++) {
        if (strcmp(a->users[i].name, name) == 0) return i;
    }
    return -1;
}

static int find_grant_idx(tsdb_auth_t *a, const char *user,
                           int priv, const char *resource) {
    for (int i = 0; i < a->ngrants; i++) {
        if (strcmp(a->grants[i].user, user) == 0 &&
            a->grants[i].priv == priv &&
            strcmp(a->grants[i].resource, resource) == 0) {
            return i;
        }
    }
    return -1;
}

/* Drop all grants owned by <user> (called when the user is dropped). */
static void drop_user_grants(tsdb_auth_t *a, const char *user) {
    int w = 0;
    for (int r = 0; r < a->ngrants; r++) {
        if (strcmp(a->grants[r].user, user) != 0) {
            if (w != r) a->grants[w] = a->grants[r];
            w++;
        }
    }
    a->ngrants = w;
}

/* Drop any tokens owned by <user> (e.g. on drop or password change). */
static void drop_user_tokens(tsdb_auth_t *a, const char *user) {
    int w = 0;
    for (int r = 0; r < a->ntokens; r++) {
        if (strcmp(a->tokens[r].user, user) != 0) {
            if (w != r) a->tokens[w] = a->tokens[r];
            w++;
        }
    }
    a->ntokens = w;
}

/* ---- Log I/O ----------------------------------------------------------- */

static int log_user_create(tsdb_auth_t *a, const tsdb_user_t *u) {
    char en[TSDB_USER_NAME_MAX * 3 + 4];
    char salt_hex[TSDB_USER_SALT_BYTES * 2 + 1];
    char hash_hex[TSDB_USER_HASH_BYTES * 2 + 1];
    pct_encode(u->name, en, sizeof(en));
    bytes_to_hex(u->salt, TSDB_USER_SALT_BYTES, salt_hex);
    bytes_to_hex(u->hash, TSDB_USER_HASH_BYTES, hash_hex);

    int n = fprintf(a->log, "+user\t%s\t%s\t%s\t%d\t%lld\n",
                    en, salt_hex, hash_hex,
                    (int)u->role, (long long)u->created_at);
    if (n < 0 || fflush(a->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_user_drop(tsdb_auth_t *a, const char *name) {
    char en[TSDB_USER_NAME_MAX * 3 + 4];
    pct_encode(name, en, sizeof(en));
    int n = fprintf(a->log, "-user\t%s\n", en);
    if (n < 0 || fflush(a->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

static int log_grant(tsdb_auth_t *a, char op, const char *user,
                      int priv, const char *resource) {
    char eu[TSDB_USER_NAME_MAX * 3 + 4];
    char er[TSDB_USER_RESOURCE_MAX * 3 + 4];
    pct_encode(user,     eu, sizeof(eu));
    pct_encode(resource, er, sizeof(er));
    const char *tag = (op == '+') ? "+grant" : "-grant";
    int n = fprintf(a->log, "%s\t%s\t%d\t%s\n", tag, eu, priv, er);
    if (n < 0 || fflush(a->log) != 0) return TSDB_ERR_IO;
    return TSDB_OK;
}

/* ---- Replay ------------------------------------------------------------ */

static void apply_user_create(tsdb_auth_t *a, const tsdb_user_t *u) {
    int idx = find_user_idx(a, u->name);
    if (idx >= 0) { a->users[idx] = *u; return; }
    if (a->nusers >= TSDB_USER_MAX_USERS) return;
    a->users[a->nusers++] = *u;
}

static void apply_user_drop(tsdb_auth_t *a, const char *name) {
    int idx = find_user_idx(a, name);
    if (idx < 0) return;
    if (idx != a->nusers - 1) a->users[idx] = a->users[a->nusers - 1];
    a->nusers--;
    drop_user_grants(a, name);
    drop_user_tokens(a, name);
}

static void apply_grant(tsdb_auth_t *a, const char *user,
                         int priv, const char *resource) {
    if (find_grant_idx(a, user, priv, resource) >= 0) return;
    if (a->ngrants >= TSDB_USER_MAX_GRANTS) return;
    tsdb_user_grant_t *g = &a->grants[a->ngrants++];
    snprintf(g->user,     sizeof(g->user),     "%s", user);
    snprintf(g->resource, sizeof(g->resource), "%s", resource);
    g->priv = priv;
}

static void apply_revoke(tsdb_auth_t *a, const char *user,
                          int priv, const char *resource) {
    int idx = find_grant_idx(a, user, priv, resource);
    if (idx < 0) return;
    if (idx != a->ngrants - 1) a->grants[idx] = a->grants[a->ngrants - 1];
    a->ngrants--;
}

static int replay_log(tsdb_auth_t *a) {
    FILE *f = fopen(a->log_path, "r");
    if (!f) return TSDB_OK; /* no log yet */

    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        chomp(line);
        if (!line[0]) continue;

        char linecopy[4096];
        snprintf(linecopy, sizeof(linecopy), "%s", line);
        char *fields[8];
        int nf = split_tab(linecopy, fields, 8);
        if (nf < 1) continue;

        if (strcmp(fields[0], "+user") == 0 && nf >= 6) {
            tsdb_user_t u;
            memset(&u, 0, sizeof(u));
            char nm[TSDB_USER_NAME_MAX];
            pct_decode(fields[1], nm, sizeof(nm));
            snprintf(u.name, sizeof(u.name), "%s", nm);
            if (strlen(fields[2]) != TSDB_USER_SALT_BYTES * 2) continue;
            if (strlen(fields[3]) != TSDB_USER_HASH_BYTES * 2) continue;
            if (hex_to_bytes(fields[2], TSDB_USER_SALT_BYTES, u.salt) != 0) continue;
            if (hex_to_bytes(fields[3], TSDB_USER_HASH_BYTES, u.hash) != 0) continue;
            u.role       = (tsdb_user_role_t)atoi(fields[4]);
            u.created_at = (tsdb_ts_t)strtoll(fields[5], NULL, 10);
            apply_user_create(a, &u);

        } else if (strcmp(fields[0], "-user") == 0 && nf >= 2) {
            char nm[TSDB_USER_NAME_MAX];
            pct_decode(fields[1], nm, sizeof(nm));
            apply_user_drop(a, nm);

        } else if (strcmp(fields[0], "+grant") == 0 && nf >= 4) {
            char u[TSDB_USER_NAME_MAX], r[TSDB_USER_RESOURCE_MAX];
            pct_decode(fields[1], u, sizeof(u));
            int priv = atoi(fields[2]);
            pct_decode(fields[3], r, sizeof(r));
            apply_grant(a, u, priv, r);

        } else if (strcmp(fields[0], "-grant") == 0 && nf >= 4) {
            char u[TSDB_USER_NAME_MAX], r[TSDB_USER_RESOURCE_MAX];
            pct_decode(fields[1], u, sizeof(u));
            int priv = atoi(fields[2]);
            pct_decode(fields[3], r, sizeof(r));
            apply_revoke(a, u, priv, r);
        }
    }
    fclose(f);
    return TSDB_OK;
}

/* ---- Open / close ------------------------------------------------------ */

int tsdb_auth_open(const char *data_dir, tsdb_auth_t **out) {
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    tsdb_auth_t *a = calloc(1, sizeof(*a));
    if (!a) return TSDB_ERR_NOMEM;

    snprintf(a->cat_dir, sizeof(a->cat_dir), "%s/catalog", data_dir);
    snprintf(a->log_path, sizeof(a->log_path), "%s/users.log", a->cat_dir);

    if (tsdb_mkdir_p(a->cat_dir) < 0) { free(a); return TSDB_ERR_IO; }

    pthread_mutex_init(&a->lock, NULL);

    int rc = replay_log(a);
    if (rc != TSDB_OK) {
        pthread_mutex_destroy(&a->lock);
        free(a);
        return rc;
    }

    a->log = fopen(a->log_path, "a");
    if (!a->log) {
        pthread_mutex_destroy(&a->lock);
        free(a);
        return TSDB_ERR_IO;
    }

    /* Seed a default admin when the user table is empty so a fresh
     * install has a working login credential out of the box.  The
     * TSDB_DEFAULT_ADMIN_USER / _PASSWORD env vars let operators
     * override at deploy time; absent both, use root/123456 — a
     * convenience for dev clusters that MUST be changed in prod.
     * Any later CREATE USER / DROP USER will persist through the
     * append-only log as usual. */
    if (a->nusers == 0) {
        const char *duser = getenv("TSDB_DEFAULT_ADMIN_USER");
        const char *dpass = getenv("TSDB_DEFAULT_ADMIN_PASSWORD");
        if (!duser || !*duser) duser = "root";
        if (!dpass || !*dpass) dpass = "123456";
        *out = a;
        int rc2 = tsdb_auth_user_create(a, duser, dpass, TSDB_USER_ROLE_ADMIN);
        if (rc2 == TSDB_OK) {
            (void)tsdb_auth_grant(a, duser, TSDB_PRIV_ALL, "*");
            /* Banner gated by TSDB_AUTH_SEED_BANNER=1 so unit tests
             * (which open dozens of fresh DBs) don't flood stderr.
             * Enabled explicitly by docker-compose / cluster bring-up. */
            const char *banner = getenv("TSDB_AUTH_SEED_BANNER");
            if (banner && *banner && *banner != '0') {
                fprintf(stderr,
                    "[auth] seeded default admin '%s' (role=admin, grant ALL on *); "
                    "CHANGE THE PASSWORD in production\n", duser);
            }
        }
        return TSDB_OK;
    }

    *out = a;
    return TSDB_OK;
}

void tsdb_auth_close(tsdb_auth_t *a) {
    if (!a) return;
    pthread_mutex_lock(&a->lock);
    if (a->log) { fclose(a->log); a->log = NULL; }
    pthread_mutex_unlock(&a->lock);
    pthread_mutex_destroy(&a->lock);
    free(a);
}

/* ---- User management --------------------------------------------------- */

int tsdb_auth_user_create(tsdb_auth_t *a,
                           const char *name,
                           const char *password,
                           tsdb_user_role_t role)
{
    if (!a || !name || !*name || !password) return TSDB_ERR_INVAL;
    if (strlen(name) >= TSDB_USER_NAME_MAX) return TSDB_ERR_INVAL;

    tsdb_user_t u;
    memset(&u, 0, sizeof(u));
    snprintf(u.name, sizeof(u.name), "%s", name);
    u.role       = (role == TSDB_USER_ROLE_ADMIN) ? TSDB_USER_ROLE_ADMIN
                                                  : TSDB_USER_ROLE_NORMAL;
    u.created_at = tsdb_now_ns();

    if (RAND_bytes(u.salt, TSDB_USER_SALT_BYTES) != 1) return TSDB_ERR_INTERNAL;
    if (derive_hash(password, u.salt, TSDB_USER_SALT_BYTES,
                     u.hash, TSDB_USER_HASH_BYTES) != 0) {
        return TSDB_ERR_INTERNAL;
    }

    pthread_mutex_lock(&a->lock);

    if (find_user_idx(a, name) >= 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_EXISTS;
    }
    if (a->nusers >= TSDB_USER_MAX_USERS) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_FULL;
    }

    int rc = log_user_create(a, &u);
    if (rc == TSDB_OK) apply_user_create(a, &u);
    pthread_mutex_unlock(&a->lock);
    return rc;
}

int tsdb_auth_user_drop(tsdb_auth_t *a, const char *name) {
    if (!a || !name) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&a->lock);
    if (find_user_idx(a, name) < 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_NOTFOUND;
    }
    /* Persist grants-drop before user-drop so replay matches memory state. */
    for (int i = 0; i < a->ngrants; i++) {
        if (strcmp(a->grants[i].user, name) == 0) {
            log_grant(a, '-', name, a->grants[i].priv, a->grants[i].resource);
        }
    }
    int rc = log_user_drop(a, name);
    if (rc == TSDB_OK) apply_user_drop(a, name);
    pthread_mutex_unlock(&a->lock);
    return rc;
}

int tsdb_auth_user_set_password(tsdb_auth_t *a,
                                 const char *name,
                                 const char *new_password)
{
    if (!a || !name || !new_password) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&a->lock);
    int idx = find_user_idx(a, name);
    if (idx < 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_NOTFOUND;
    }
    tsdb_user_t u = a->users[idx];   /* copy existing fields */

    if (RAND_bytes(u.salt, TSDB_USER_SALT_BYTES) != 1) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_INTERNAL;
    }
    if (derive_hash(new_password, u.salt, TSDB_USER_SALT_BYTES,
                     u.hash, TSDB_USER_HASH_BYTES) != 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_INTERNAL;
    }

    /* Append a +user record — replay semantics are "last one wins" for
     * matching names because apply_user_create overwrites in place. */
    int rc = log_user_create(a, &u);
    if (rc == TSDB_OK) {
        a->users[idx] = u;
        drop_user_tokens(a, name);
    }
    pthread_mutex_unlock(&a->lock);
    return rc;
}

int tsdb_auth_user_list(tsdb_auth_t *a, tsdb_user_t **out_arr, size_t *out_n) {
    if (!a || !out_arr || !out_n) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&a->lock);
    size_t n = (size_t)a->nusers;
    if (n == 0) {
        *out_arr = NULL; *out_n = 0;
        pthread_mutex_unlock(&a->lock);
        return TSDB_OK;
    }
    tsdb_user_t *arr = malloc(n * sizeof(*arr));
    if (!arr) { pthread_mutex_unlock(&a->lock); return TSDB_ERR_NOMEM; }
    memcpy(arr, a->users, n * sizeof(*arr));
    *out_arr = arr; *out_n = n;
    pthread_mutex_unlock(&a->lock);
    return TSDB_OK;
}

/* ---- Grants ------------------------------------------------------------ */

/* Normalise privilege: ALL collapses to TSDB_PRIV_ALL bitmask. */
static int normalise_priv(int priv) {
    priv &= TSDB_PRIV_ALL;
    return priv;
}

int tsdb_auth_grant(tsdb_auth_t *a,
                     const char *user,
                     int priv,
                     const char *resource)
{
    if (!a || !user || !resource) return TSDB_ERR_INVAL;
    priv = normalise_priv(priv);
    if (priv == 0) return TSDB_ERR_INVAL;
    if (strlen(resource) >= TSDB_USER_RESOURCE_MAX) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&a->lock);
    if (find_user_idx(a, user) < 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_NOTFOUND;
    }
    if (find_grant_idx(a, user, priv, resource) >= 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_OK;   /* idempotent */
    }
    if (a->ngrants >= TSDB_USER_MAX_GRANTS) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_FULL;
    }
    int rc = log_grant(a, '+', user, priv, resource);
    if (rc == TSDB_OK) apply_grant(a, user, priv, resource);
    pthread_mutex_unlock(&a->lock);
    return rc;
}

int tsdb_auth_revoke(tsdb_auth_t *a,
                      const char *user,
                      int priv,
                      const char *resource)
{
    if (!a || !user || !resource) return TSDB_ERR_INVAL;
    priv = normalise_priv(priv);
    if (priv == 0) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&a->lock);
    if (find_user_idx(a, user) < 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_NOTFOUND;
    }
    if (find_grant_idx(a, user, priv, resource) < 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_OK;   /* idempotent */
    }
    int rc = log_grant(a, '-', user, priv, resource);
    if (rc == TSDB_OK) apply_revoke(a, user, priv, resource);
    pthread_mutex_unlock(&a->lock);
    return rc;
}

/* ---- Authentication + check ------------------------------------------- */

static int generate_token(char *out_buf) {
    uint8_t raw[TSDB_USER_TOKEN_BYTES];
    if (RAND_bytes(raw, sizeof(raw)) != 1) return -1;
    bytes_to_hex(raw, sizeof(raw), out_buf);
    return 0;
}

int tsdb_auth_login(tsdb_auth_t *a,
                     const char *name,
                     const char *password,
                     char       *token_buf,
                     size_t      token_cap)
{
    if (!a || !name || !password || !token_buf) return TSDB_ERR_INVAL;
    if (token_cap < TSDB_USER_TOKEN_CHARS + 1) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&a->lock);
    int idx = find_user_idx(a, name);
    if (idx < 0) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_ERR_PERMISSION;
    }
    tsdb_user_t u = a->users[idx];    /* snapshot under lock */
    pthread_mutex_unlock(&a->lock);

    uint8_t candidate[TSDB_USER_HASH_BYTES];
    if (derive_hash(password, u.salt, TSDB_USER_SALT_BYTES,
                     candidate, TSDB_USER_HASH_BYTES) != 0) {
        return TSDB_ERR_INTERNAL;
    }
    if (ct_memcmp(candidate, u.hash, TSDB_USER_HASH_BYTES) != 0) {
        return TSDB_ERR_PERMISSION;
    }

    char tok[TSDB_USER_TOKEN_CHARS + 1];
    if (generate_token(tok) != 0) return TSDB_ERR_INTERNAL;

    pthread_mutex_lock(&a->lock);
    if (a->ntokens >= TSDB_USER_MAX_TOKENS) {
        /* Evict the oldest entry to make room (FIFO). */
        for (int i = 0; i < a->ntokens - 1; i++) a->tokens[i] = a->tokens[i + 1];
        a->ntokens--;
    }
    token_entry_t *te = &a->tokens[a->ntokens++];
    snprintf(te->token, sizeof(te->token), "%s", tok);
    snprintf(te->user,  sizeof(te->user),  "%s", name);
    te->role = u.role;
    pthread_mutex_unlock(&a->lock);

    snprintf(token_buf, token_cap, "%s", tok);
    return TSDB_OK;
}

/* priv_granted returns 1 iff (have, have_res) covers (need, need_res). */
static int grant_matches(int have_priv, const char *have_res,
                          int need_priv, const char *need_res) {
    if ((have_priv & need_priv) != need_priv) return 0;
    if (strcmp(have_res, "*") == 0) return 1;
    if (strcmp(have_res, need_res) == 0) return 1;
    return 0;
}

int tsdb_auth_verify(tsdb_auth_t *a,
                      const char *token,
                      int         priv,
                      const char *resource)
{
    if (!a || !token || !resource) return TSDB_ERR_PERMISSION;
    priv = normalise_priv(priv);
    if (priv == 0) return TSDB_ERR_PERMISSION;

    pthread_mutex_lock(&a->lock);

    /* Find token. */
    int ti = -1;
    for (int i = 0; i < a->ntokens; i++) {
        if (strcmp(a->tokens[i].token, token) == 0) { ti = i; break; }
    }
    if (ti < 0) { pthread_mutex_unlock(&a->lock); return TSDB_ERR_PERMISSION; }

    /* ADMIN short-circuit — always OK. */
    if (a->tokens[ti].role == TSDB_USER_ROLE_ADMIN) {
        pthread_mutex_unlock(&a->lock);
        return TSDB_OK;
    }

    const char *user = a->tokens[ti].user;
    for (int i = 0; i < a->ngrants; i++) {
        if (strcmp(a->grants[i].user, user) != 0) continue;
        if (grant_matches(a->grants[i].priv, a->grants[i].resource,
                           priv, resource)) {
            pthread_mutex_unlock(&a->lock);
            return TSDB_OK;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return TSDB_ERR_PERMISSION;
}

int tsdb_auth_token_role(tsdb_auth_t      *a,
                          const char       *token,
                          tsdb_user_role_t *out_role)
{
    if (!a || !token || !out_role) return TSDB_ERR_PERMISSION;
    pthread_mutex_lock(&a->lock);
    for (int i = 0; i < a->ntokens; i++) {
        if (strcmp(a->tokens[i].token, token) == 0) {
            *out_role = a->tokens[i].role;
            pthread_mutex_unlock(&a->lock);
            return TSDB_OK;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return TSDB_ERR_PERMISSION;
}

int tsdb_auth_token_user(tsdb_auth_t *a, const char *token,
                          char *out_user, size_t out_cap)
{
    if (!a || !token || !out_user || out_cap == 0) return TSDB_ERR_PERMISSION;
    pthread_mutex_lock(&a->lock);
    for (int i = 0; i < a->ntokens; i++) {
        if (strcmp(a->tokens[i].token, token) == 0) {
            snprintf(out_user, out_cap, "%s", a->tokens[i].user);
            pthread_mutex_unlock(&a->lock);
            return TSDB_OK;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return TSDB_ERR_PERMISSION;
}
