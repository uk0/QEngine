/* test_tls_defaults_and_versions.c — a dangerous TLS option must be loud, a
 * stream from a newer build must not read as corruption, and the release
 * identity must have exactly one authority.
 *
 * 1. SEC-6a.  tsdb_tls_client_ctx(skip_verify=1) installs SSL_VERIFY_NONE, so
 *    the context accepts ANY certificate: traffic is encrypted but the peer is
 *    not authenticated and a man in the middle is invisible.  The escape hatch
 *    stays — a lab bootstrap legitimately needs it — but it has to announce
 *    itself, or a cluster runs unverified for months and nobody finds out
 *    until an incident.  stderr is the only surface this library has, so that
 *    is what is asserted, together with the control that a verifying context
 *    stays quiet.
 *
 * 2. SEC-6b.  src/cluster/rpc.c read TSDB_RPC_TLS_SKIP_VERIFY as "set, and
 *    not starting with 0" — so TSDB_RPC_TLS_SKIP_VERIFY=false, =no and =off
 *    all DISABLED peer verification, the exact opposite of what the operator
 *    wrote, and without a word.  Only an affirmative value may disable it, and
 *    either resolution is printed.  Observed end to end through the real
 *    tsdb_rpc_connect: the tls.c banner from (1) fires exactly when the
 *    context is built with SSL_VERIFY_NONE, so its presence/absence is direct
 *    evidence of the decision rpc.c made.
 *
 * 3. FMT-6.  A migration stream whose header carries a version this build does
 *    not implement must report UNSUPPORTED and name both versions, not
 *    TSDB_ERR_CORRUPT — an operator told "your data is corrupt" when the real
 *    answer is "your binary is too old" goes looking in the wrong place.  A
 *    bad magic must still be CORRUPT.
 *
 * 4. TCRE-8.  RELEASE_NOTES.md must carry the version include/tsdb.h declares.
 */
#include "tsdb.h"
#include "tsdb_migrate.h"
#include "../src/server/tls.h"
#include "../src/cluster/rpc.h"

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>

static int g_fail = 0;
#define CHECK(cond, msg) do {                                        \
    if (cond) printf("  PASS  %s\n", (msg));                         \
    else { g_fail++; fprintf(stdout, "  FAIL  %s\n", (msg)); }       \
} while (0)

/* The banner tls.c must print when it honours skip_verify.  Kept as one
 * literal here so the assertion names exactly what an operator greps for. */
#define VERIFY_OFF_MARKER "PEER CERTIFICATE VERIFICATION IS DISABLED"

#define TMPDIR "/tmp/tsdb_test_tls_defaults"

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* ---- stderr capture ------------------------------------------------------ */

static int cap_begin(const char *path) {
    fflush(stderr);
    int saved = dup(STDERR_FILENO);
    int f = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (f < 0 || saved < 0) { perror("cap_begin"); exit(1); }
    dup2(f, STDERR_FILENO);
    close(f);
    return saved;
}

static void cap_end(int saved) {
    fflush(stderr);
    dup2(saved, STDERR_FILENO);
    close(saved);
}

/* Whole file as a NUL-terminated heap string; "" when absent. */
static char *slurp(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return calloc(1, 1);
    size_t cap = 8192, len = 0;
    char *b = malloc(cap);
    for (;;) {
        if (len + 4096 + 1 > cap) { cap *= 2; b = realloc(b, cap); }
        ssize_t r = read(fd, b + len, 4096);
        if (r <= 0) break;
        len += (size_t)r;
    }
    close(fd);
    b[len] = '\0';
    return b;
}

/* ---- 1. skip_verify announces itself ------------------------------------- */

static void test_skip_verify_is_loud(void) {
    printf("-- skip_verify announces itself --\n");
    if (!tsdb_tls_available()) {
        printf("  SKIP  no TLS backend compiled in\n");
        return;
    }

    tsdb_tls_ctx_t *ctx = NULL;
    int saved = cap_begin(TMPDIR "/skip.err");
    int rc = tsdb_tls_client_ctx(NULL, 1, NULL, NULL, &ctx);
    cap_end(saved);
    char *loud = slurp(TMPDIR "/skip.err");
    if (rc == 0 && ctx) tsdb_tls_free(ctx);
    ctx = NULL;

    CHECK(rc == 0, "skip_verify context still builds (escape hatch kept)");
    CHECK(strstr(loud, VERIFY_OFF_MARKER) != NULL,
          "skip_verify=1 announces '" VERIFY_OFF_MARKER "' on stderr");
    free(loud);

    saved = cap_begin(TMPDIR "/verify.err");
    rc = tsdb_tls_client_ctx(NULL, 0, NULL, NULL, &ctx);
    cap_end(saved);
    char *quiet = slurp(TMPDIR "/verify.err");
    if (rc == 0 && ctx) tsdb_tls_free(ctx);

    CHECK(strstr(quiet, VERIFY_OFF_MARKER) == NULL,
          "a verifying context does NOT print the banner");
    free(quiet);
}

/* ---- 2. only an affirmative env value disables verification --------------- */

/* Drive the real tsdb_rpc_connect against a plain TCP listener with
 * TSDB_RPC_TLS=1.  The connect reaches rpc.c's lazy client-context build (the
 * decision under test), then fails its handshake because the listener is not
 * a TLS peer — by which point the context has already been built, and the
 * tls.c banner has fired or not.  Each value needs its own process: the client
 * context is pthread_once-cached per process.
 *
 * Returns the child's stderr. */
static char *rpc_connect_stderr(int listen_fd, int port, const char *skip_val,
                                const char *errfile) {
    pid_t pid = fork();
    if (pid == 0) {
        setenv("TSDB_RPC_TLS", "1", 1);
        if (skip_val) setenv("TSDB_RPC_TLS_SKIP_VERIFY", skip_val, 1);
        else          unsetenv("TSDB_RPC_TLS_SKIP_VERIFY");
        /* Bound every socket read so a wedged handshake cannot hang the run. */
        setenv("TSDB_RPC_IO_TIMEOUT_MS", "2000", 1);
        close(listen_fd);
        int saved = cap_begin(errfile);
        char addr[64];
        snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
        tsdb_rpc_conn_t *c = tsdb_rpc_connect(addr, 2000);
        if (c) tsdb_rpc_conn_close(c);
        cap_end(saved);
        _exit(0);
    }

    /* Complete the TCP handshake, then drop it: the child's TLS handshake read
     * then fails at once instead of blocking on a peer that never speaks. */
    struct pollfd pfd = { .fd = listen_fd, .events = POLLIN };
    if (poll(&pfd, 1, 5000) > 0) {
        int c = accept(listen_fd, NULL, NULL);
        if (c >= 0) close(c);
    }
    int st = 0;
    waitpid(pid, &st, 0);
    return slurp(errfile);
}

static void test_skip_verify_needs_affirmative_value(void) {
    printf("-- TSDB_RPC_TLS_SKIP_VERIFY needs an affirmative value --\n");
    if (!tsdb_tls_available()) {
        printf("  SKIP  no TLS backend compiled in\n");
        return;
    }

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { CHECK(0, "listen socket"); return; }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa = { 0 };
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (bind(lfd, (struct sockaddr *)&sa, sizeof(sa)) != 0 ||
        listen(lfd, 8) != 0) { CHECK(0, "bind/listen"); close(lfd); return; }
    socklen_t sl = sizeof(sa);
    getsockname(lfd, (struct sockaddr *)&sa, &sl);
    int port = ntohs(sa.sin_port);

    char *on = rpc_connect_stderr(lfd, port, "1", TMPDIR "/rpc_on.err");
    CHECK(strstr(on, VERIFY_OFF_MARKER) != NULL,
          "SKIP_VERIFY=1 really disables verification, and says so");
    free(on);

    char *off = rpc_connect_stderr(lfd, port, "false", TMPDIR "/rpc_false.err");
    CHECK(strstr(off, VERIFY_OFF_MARKER) == NULL,
          "SKIP_VERIFY=false does NOT disable verification");
    CHECK(strstr(off, "TSDB_RPC_TLS_SKIP_VERIFY") != NULL,
          "SKIP_VERIFY=false is reported, not silently ignored");
    free(off);

    close(lfd);
}

/* ---- 3. a newer stream version is UNSUPPORTED, not CORRUPT ---------------- */

#define DAY1 1700000000000000000LL

static void build_stream(const char *dbdir, const char *stream) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dbdir, &db) != TSDB_OK) { CHECK(0, "open source db"); return; }
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_FLOAT64   },
    };
    tsdb_create_table(db, "m", cols, 2, "ts");
    tsdb_table_t *t = NULL;
    tsdb_open_table(db, "m", &t);
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    for (int i = 0; i < 64; i++) {
        tsdb_batch_row_ts(b, DAY1 + i * 1000000LL);
        tsdb_batch_row_f64(b, 1, (double)i);
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);

    int fd = open(stream, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    tsdb_mig_stats_t st = { 0 };
    int rc = tsdb_migrate_export(db, "m", fd, NULL, &st);
    close(fd);
    tsdb_close(db);
    CHECK(rc == TSDB_OK, "export built a stream to mangle");
}

/* Copy `src` to `dst` with a 4-byte little-endian patch at `off`. */
static void patch_u32(const char *src, const char *dst, size_t off, uint32_t v) {
    char *buf = slurp(src);
    int fd = open(src, O_RDONLY);
    struct stat sb; fstat(fd, &sb); close(fd);
    size_t n = (size_t)sb.st_size;
    buf[off + 0] = (char)(uint8_t)v;
    buf[off + 1] = (char)(uint8_t)(v >> 8);
    buf[off + 2] = (char)(uint8_t)(v >> 16);
    buf[off + 3] = (char)(uint8_t)(v >> 24);
    int o = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    ssize_t w = write(o, buf, n);
    (void)w;
    close(o);
    free(buf);
}

static int import_rc(const char *dbdir, const char *stream, char **err_out) {
    rm_rf(dbdir);
    tsdb_db_t *db = NULL;
    if (tsdb_open(dbdir, &db) != TSDB_OK) return TSDB_ERR_IO;
    int fd = open(stream, O_RDONLY);
    tsdb_mig_stats_t st = { 0 };
    int saved = cap_begin(TMPDIR "/import.err");
    int rc = tsdb_migrate_import(db, fd, NULL, &st);
    cap_end(saved);
    close(fd);
    tsdb_close(db);
    if (err_out) *err_out = slurp(TMPDIR "/import.err");
    return rc;
}

static void test_stream_version_is_not_corruption(void) {
    printf("-- a newer stream version is UNSUPPORTED, not CORRUPT --\n");
    rm_rf(TMPDIR "/src");
    build_stream(TMPDIR "/src", TMPDIR "/good.stream");

    /* Sanity: the unmangled stream still imports. */
    CHECK(import_rc(TMPDIR "/dst", TMPDIR "/good.stream", NULL) == TSDB_OK,
          "the unmangled stream imports (control)");

    /* Version u32 sits at offset 4, right behind the magic. */
    patch_u32(TMPDIR "/good.stream", TMPDIR "/newer.stream", 4,
              TSDB_MIG_VERSION + 1u);
    char *err = NULL;
    int rc = import_rc(TMPDIR "/dst", TMPDIR "/newer.stream", &err);
    CHECK(rc == TSDB_ERR_UNSUPPORTED,
          "a newer stream version returns TSDB_ERR_UNSUPPORTED");
    if (rc != TSDB_ERR_UNSUPPORTED)
        printf("        got rc=%d (%s)\n", rc, tsdb_errstr(rc));

    char want_found[32], want_known[32];
    snprintf(want_found, sizeof(want_found), "%u", TSDB_MIG_VERSION + 1u);
    snprintf(want_known, sizeof(want_known), "%u", (unsigned)TSDB_MIG_VERSION);
    CHECK(err && strstr(err, want_found) && strstr(err, want_known),
          "the message names the version found and the version understood");
    if (err && !(strstr(err, want_found) && strstr(err, want_known)))
        printf("        stderr was: %s\n", err);
    free(err);

    /* A bad magic is still corruption — the two must stay distinguishable. */
    patch_u32(TMPDIR "/good.stream", TMPDIR "/badmagic.stream", 0, 0xDEADBEEFu);
    CHECK(import_rc(TMPDIR "/dst", TMPDIR "/badmagic.stream", NULL)
              == TSDB_ERR_CORRUPT,
          "a bad magic is still TSDB_ERR_CORRUPT");
}

/* ---- 4. one authoritative release version -------------------------------- */

static void test_release_identity(void) {
    printf("-- release identity has one authority --\n");

    char *hdr = slurp("include/tsdb.h");
    int hmaj = -1, hmin = -1, hpat = -1;
    const char *p;
    if ((p = strstr(hdr, "#define TSDB_VERSION_MAJOR"))) sscanf(p + 26, "%d", &hmaj);
    if ((p = strstr(hdr, "#define TSDB_VERSION_MINOR"))) sscanf(p + 26, "%d", &hmin);
    if ((p = strstr(hdr, "#define TSDB_VERSION_PATCH"))) sscanf(p + 26, "%d", &hpat);
    free(hdr);
    CHECK(hmaj >= 0 && hmin >= 0 && hpat >= 0,
          "include/tsdb.h declares TSDB_VERSION_MAJOR/MINOR/PATCH");

    char *rn = slurp("RELEASE_NOTES.md");
    int rmaj = -1, rmin = -1, rpat = -1;
    if ((p = strstr(rn, "# tsdb v"))) sscanf(p + 8, "%d.%d.%d", &rmaj, &rmin, &rpat);
    free(rn);
    CHECK(rmaj >= 0, "RELEASE_NOTES.md titles itself '# tsdb vX.Y.Z'");

    int agree = (rmaj == hmaj && rmin == hmin && rpat == hpat);
    CHECK(agree, "RELEASE_NOTES.md carries the version include/tsdb.h declares");
    if (!agree)
        printf("        RELEASE_NOTES v%d.%d.%d vs include/tsdb.h %d.%d.%d\n",
               rmaj, rmin, rpat, hmaj, hmin, hpat);
}

int main(void) {
    printf("=== test_tls_defaults_and_versions ===\n");
    rm_rf(TMPDIR);
    mkdir(TMPDIR, 0700);

    test_skip_verify_is_loud();
    test_skip_verify_needs_affirmative_value();
    test_stream_version_is_not_corruption();
    test_release_identity();

    rm_rf(TMPDIR);
    if (g_fail) { printf("FAILED (%d)\n", g_fail); return 1; }
    printf("ALL PASS\n");
    return 0;
}
