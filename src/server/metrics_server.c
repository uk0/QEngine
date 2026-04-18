/* metrics_server.c — Minimal HTTP/1.1 server for GET /metrics.
 *
 * Threading model: one accept thread + per-connection threads (detached).
 * Each connection:
 *   1. Reads until "\r\n\r\n" (end of HTTP headers).
 *   2. If "GET /metrics", render and send 200 OK.
 *   3. Otherwise send 404.
 *   4. Close connection (Connection: close).
 *
 * No keep-alive, no chunked encoding — simple enough for Prometheus scrape.
 */

#define _POSIX_C_SOURCE 200809L

#include "metrics_server.h"
#include "metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

/* ---- Server struct -------------------------------------------------------- */

struct tsdb_metrics_server {
    int          listen_fd;
    int          port;
    volatile int running;
    pthread_t    accept_thread;
};

/* ---- Connection handler -------------------------------------------------- */

static void write_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, buf + sent, len - sent);
        if (n <= 0) break;
        sent += (size_t)n;
    }
}

static void *handle_connection(void *arg) {
    int fd = (int)(intptr_t)arg;

    /* Read request headers (stop at \r\n\r\n or buffer full). */
    char req[2048];
    size_t pos = 0;
    int got_end = 0;

    while (pos < sizeof(req) - 1) {
        ssize_t n = read(fd, req + pos, sizeof(req) - 1 - pos);
        if (n <= 0) break;
        pos += (size_t)n;
        req[pos] = '\0';
        if (strstr(req, "\r\n\r\n") || strstr(req, "\n\n")) {
            got_end = 1;
            break;
        }
    }

    if (!got_end) {
        close(fd);
        return NULL;
    }

    /* Parse first line: "GET /metrics HTTP/1.x" */
    int is_metrics = 0;
    if (strncmp(req, "GET /metrics", 12) == 0) {
        is_metrics = 1;
    }

    if (is_metrics) {
        size_t body_len = 0;
        char *body = tsdb_metrics_render(&body_len);
        if (!body) {
            const char *err =
                "HTTP/1.1 500 Internal Server Error\r\n"
                "Content-Length: 0\r\n"
                "Connection: close\r\n\r\n";
            write_all(fd, err, strlen(err));
        } else {
            /* Build response header. */
            char hdr[256];
            int hlen = snprintf(hdr, sizeof(hdr),
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                "Content-Length: %zu\r\n"
                "Connection: close\r\n\r\n",
                body_len);
            write_all(fd, hdr, (size_t)hlen);
            write_all(fd, body, body_len);
            free(body);
        }
    } else {
        const char *not_found =
            "HTTP/1.1 404 Not Found\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        write_all(fd, not_found, strlen(not_found));
    }

    close(fd);
    return NULL;
}

/* ---- Accept loop --------------------------------------------------------- */

static void *accept_loop(void *arg) {
    struct tsdb_metrics_server *ms = (struct tsdb_metrics_server *)arg;

    while (ms->running) {
        struct pollfd pfd = { ms->listen_fd, POLLIN, 0 };
        int r = poll(&pfd, 1, 200);
        if (r <= 0) continue;
        if (!(pfd.revents & POLLIN)) continue;

        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(ms->listen_fd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }

        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, handle_connection, (void *)(intptr_t)cfd) != 0) {
            close(cfd);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

/* ---- Public API ---------------------------------------------------------- */

static int parse_bind(const char *addr, char *host_out, size_t hcap, int *port_out) {
    if (!addr || addr[0] == '\0') return -1;
    const char *colon = strrchr(addr, ':');
    if (!colon) return -1;
    size_t hlen = (size_t)(colon - addr);
    if (hlen >= hcap) hlen = hcap - 1;
    memcpy(host_out, addr, hlen);
    host_out[hlen] = '\0';
    *port_out = atoi(colon + 1);
    return 0;
}

int tsdb_metrics_server_start(const char *bind_addr, tsdb_metrics_server_t **out) {
    *out = NULL;
    if (!bind_addr || bind_addr[0] == '\0') return 0; /* disabled */

    char host[128];
    int  port;
    if (parse_bind(bind_addr, host, sizeof(host), &port) != 0) return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) <= 0) sa.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 32) < 0) {
        close(fd);
        return -1;
    }

    /* Get actual port (if port was 0). */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);
    int actual_port = ntohs(bound.sin_port);

    struct tsdb_metrics_server *ms =
        (struct tsdb_metrics_server *)calloc(1, sizeof(*ms));
    if (!ms) { close(fd); return -1; }

    ms->listen_fd = fd;
    ms->port      = actual_port;
    ms->running   = 1;

    if (pthread_create(&ms->accept_thread, NULL, accept_loop, ms) != 0) {
        close(fd);
        free(ms);
        return -1;
    }

    *out = ms;
    return 0;
}

void tsdb_metrics_server_stop(tsdb_metrics_server_t *ms) {
    if (!ms) return;
    ms->running = 0;
    close(ms->listen_fd);
    pthread_join(ms->accept_thread, NULL);
    free(ms);
}

int tsdb_metrics_server_port(const tsdb_metrics_server_t *ms) {
    if (!ms) return -1;
    return ms->port;
}
