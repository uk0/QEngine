/* user.h — RBAC: User catalog + authentication + privilege check.
 *
 * Scope (single-node RBAC):
 *   - Users are persisted per-node; no cross-node replication.
 *   - Passwords are stored as PBKDF2-HMAC-SHA256 hashes (10000 rounds) with
 *     per-user 16-byte random salts.
 *   - Privileges are (priv_bitmask, resource) pairs.  resource is a table
 *     name or "*" meaning any table.
 *   - ADMIN role implicitly has ALL on "*"; not materialised in the grants
 *     list, handled as a short-circuit in tsdb_auth_check.
 *
 * Persistence:
 *   <data_dir>/catalog/users.log  (append-only, tab-separated, percent-encoded).
 *
 *   Record ops:
 *     +user   <name> <salt_hex> <hash_hex> <role> <created_at>
 *     -user   <name>
 *     +grant  <name> <priv_bitmask> <resource>
 *     -grant  <name> <priv_bitmask> <resource>
 *
 *   On open, log is replayed top-to-bottom; tombstones remove entries.
 *
 * Tokens are issued by tsdb_auth_authenticate as 16 random bytes hex-encoded
 * (32 chars + NUL) and held only in memory — restart invalidates all tokens.
 */
#ifndef TSDB_CATALOG_USER_H
#define TSDB_CATALOG_USER_H

#include "../../include/tsdb.h"
#include "../core/types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_USER_NAME_MAX       64
#define TSDB_USER_RESOURCE_MAX   64
#define TSDB_USER_SALT_BYTES     16
#define TSDB_USER_HASH_BYTES     32    /* SHA-256 output length            */
#define TSDB_USER_PBKDF2_ROUNDS  10000
#define TSDB_USER_TOKEN_BYTES    16    /* 32 hex chars                     */
#define TSDB_USER_TOKEN_CHARS    32
#define TSDB_USER_MAX_USERS      64
#define TSDB_USER_MAX_GRANTS     256
#define TSDB_USER_MAX_TOKENS     64

typedef enum {
    TSDB_USER_ROLE_NORMAL = 0,
    TSDB_USER_ROLE_ADMIN  = 1
} tsdb_user_role_t;

typedef struct {
    char            name[TSDB_USER_NAME_MAX];
    uint8_t         salt[TSDB_USER_SALT_BYTES];
    uint8_t         hash[TSDB_USER_HASH_BYTES];
    tsdb_user_role_t role;
    tsdb_ts_t       created_at;
} tsdb_user_t;

typedef struct {
    char     user[TSDB_USER_NAME_MAX];
    int      priv;                                 /* bitmask                  */
    char     resource[TSDB_USER_RESOURCE_MAX];     /* table name or "*"        */
} tsdb_user_grant_t;

typedef struct tsdb_auth tsdb_auth_t;

/* ---- Lifecycle --------------------------------------------------------- */

int  tsdb_auth_open(const char *data_dir, tsdb_auth_t **out);
void tsdb_auth_close(tsdb_auth_t *a);

/* ---- User management --------------------------------------------------- */

int tsdb_auth_user_create(tsdb_auth_t *a,
                           const char *name,
                           const char *password,
                           tsdb_user_role_t role);

int tsdb_auth_user_drop(tsdb_auth_t *a, const char *name);

int tsdb_auth_user_set_password(tsdb_auth_t *a,
                                 const char *name,
                                 const char *new_password);

/* List all users.  Caller frees *out_arr with free(). */
int tsdb_auth_user_list(tsdb_auth_t *a, tsdb_user_t **out_arr, size_t *out_n);

/* Grants. */
int tsdb_auth_grant(tsdb_auth_t *a,
                     const char *user,
                     int priv,
                     const char *resource);

int tsdb_auth_revoke(tsdb_auth_t *a,
                      const char *user,
                      int priv,
                      const char *resource);

/* ---- Authentication / check (direct — used by tsdb_auth_* public API) --- */

/* Verify password; on success, allocates a token.  Returns TSDB_OK or
 *  TSDB_ERR_PERMISSION. token_buf must have room for >= 33 bytes. */
int tsdb_auth_login(tsdb_auth_t *a,
                     const char *name,
                     const char *password,
                     char       *token_buf,
                     size_t      token_cap);

/* Check whether the token carries priv on resource.  Returns TSDB_OK or
 *  TSDB_ERR_PERMISSION. */
int tsdb_auth_verify(tsdb_auth_t *a,
                      const char *token,
                      int         priv,
                      const char *resource);

/* Resolve the role associated with <token>.  Used by tsdb_query_auth to gate
 * ADMIN-only statements (CREATE FUNCTION, user management) where the standard
 * privilege bitmask is too coarse.
 *   Returns TSDB_OK + *out_role on known token, TSDB_ERR_PERMISSION otherwise. */
int tsdb_auth_token_role(tsdb_auth_t      *a,
                          const char       *token,
                          tsdb_user_role_t *out_role);

/* Resolve the username owning a session token — used by the audit log so
 * every record can name the actor.  Returns TSDB_OK and writes up to
 * out_cap bytes (NUL-terminated) into out_user; TSDB_ERR_PERMISSION when
 * the token is unknown. */
int tsdb_auth_token_user(tsdb_auth_t *a,
                          const char  *token,
                          char        *out_user,
                          size_t       out_cap);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CATALOG_USER_H */
