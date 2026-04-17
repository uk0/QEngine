/* gossip.c — SWIM-lite UDP gossip engine. */

#include "gossip.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <time.h>

/* ---- Constants ----------------------------------------------------------- */

#define GOSSIP_INTERVAL_MS   500    /* full round interval */
#define PING_TIMEOUT_MS      200    /* direct ping timeout */
#define SUSPECT_THRESHOLD    3      /* failures before SUSPECT */
#define DEAD_TIMEOUT_NS      (10000000000LL)  /* 10s as SUSPECT → DEAD */
#define MAX_SEEDS            32
#define RECV_BUF_SIZE        8192

/* ---- Wire encode/decode -------------------------------------------------- */

int tsdb_gossip_encode(uint8_t *buf, size_t buf_cap,
                       tsdb_gossip_msg_t type,
                       const uint8_t *payload, uint16_t payload_len)
{
    if (buf_cap < TSDB_GOSSIP_HDR_SIZE + payload_len) return -1;

    uint32_t magic = TSDB_GOSSIP_MAGIC;
    memcpy(buf, &magic, 4);
    buf[4] = TSDB_GOSSIP_VER;
    buf[5] = (uint8_t)type;
    uint16_t pl = payload_len;
    memcpy(buf + 6, &pl, 2);
    if (payload && payload_len > 0) memcpy(buf + 8, payload, payload_len);
    return (int)(TSDB_GOSSIP_HDR_SIZE + payload_len);
}

int tsdb_gossip_decode(const uint8_t *buf, size_t len,
                       tsdb_gossip_msg_t *out_type,
                       const uint8_t **out_payload, uint16_t *out_payload_len)
{
    if (len < TSDB_GOSSIP_HDR_SIZE) return -1;

    uint32_t magic; memcpy(&magic, buf, 4);
    if (magic != TSDB_GOSSIP_MAGIC) return -1;
    if (buf[4] != TSDB_GOSSIP_VER)  return -1;

    *out_type = (tsdb_gossip_msg_t)buf[5];
    uint16_t pl; memcpy(&pl, buf + 6, 2);
    *out_payload_len = pl;
    *out_payload = (pl > 0) ? (buf + TSDB_GOSSIP_HDR_SIZE) : NULL;
    return 0;
}

/* ---- Helper: send state-sync datagram ------------------------------------ */

static int64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Parse "host:port" → sockaddr_in. */
static int addr_to_sin(const char *addr, struct sockaddr_in *out) {
    const char *colon = strrchr(addr, ':');
    if (!colon) return -1;
    char host[128];
    size_t hlen = (size_t)(colon - addr);
    if (hlen >= sizeof(host)) return -1;
    memcpy(host, addr, hlen);
    host[hlen] = '\0';
    int port = atoi(colon + 1);

    memset(out, 0, sizeof(*out));
    out->sin_family = AF_INET;
    out->sin_port   = htons((uint16_t)port);

    /* Try numeric first, then resolve. */
    if (inet_aton(host, &out->sin_addr) == 0) {
        struct hostent *h = gethostbyname(host);
        if (!h || !h->h_addr_list[0]) return -1;
        memcpy(&out->sin_addr, h->h_addr_list[0], sizeof(out->sin_addr));
    }
    return 0;
}

/* ---- Gossip struct ------------------------------------------------------- */

struct tsdb_gossip {
    int                  udp_fd;
    int                  port;
    tsdb_node_manager_t *node_mgr;

    pthread_t            recv_thread;
    pthread_t            tick_thread;
    volatile int         running;

    /* Seed addresses (for initial bootstrap before gossip converges). */
    char                 seeds[MAX_SEEDS][TSDB_ADDR_MAX];
    int                  nseeds;
    pthread_mutex_t      seed_lock;

    /* Probe tracking for SWIM failure detector. */
    pthread_mutex_t      probe_lock;
    tsdb_node_id_t       probe_target;
    int64_t              probe_sent_ns;
    int                  probe_waiting;
    int                  probe_fails[TSDB_CLUSTER_MAX_NODES];  /* per-node fail count */
    tsdb_node_id_t       probe_ids[TSDB_CLUSTER_MAX_NODES];
    int                  nprobe_ids;
};

/* ---- Send helpers -------------------------------------------------------- */

static void send_state_sync(struct tsdb_gossip *g, const char *dest_addr) {
    uint8_t state_buf[8192];
    int slen = tsdb_node_manager_encode(g->node_mgr, state_buf, sizeof(state_buf));
    if (slen <= 0) return;

    uint8_t frame[8192 + TSDB_GOSSIP_HDR_SIZE];
    int flen = tsdb_gossip_encode(frame, sizeof(frame),
                                  TSDB_GOSSIP_STATE_SYNC,
                                  state_buf, (uint16_t)slen);
    if (flen <= 0) return;

    struct sockaddr_in sa;
    if (addr_to_sin(dest_addr, &sa) < 0) return;
    sendto(g->udp_fd, frame, (size_t)flen, 0,
           (struct sockaddr *)&sa, sizeof(sa));
}

static void send_ping(struct tsdb_gossip *g, const char *dest_addr,
                      tsdb_node_id_t target_id) {
    uint8_t payload[8];
    memcpy(payload, &target_id, 8);

    uint8_t frame[64];
    int flen = tsdb_gossip_encode(frame, sizeof(frame),
                                  TSDB_GOSSIP_PING,
                                  payload, 8);
    if (flen <= 0) return;

    struct sockaddr_in sa;
    if (addr_to_sin(dest_addr, &sa) < 0) return;
    sendto(g->udp_fd, frame, (size_t)flen, 0,
           (struct sockaddr *)&sa, sizeof(sa));
}

static void send_ack(struct tsdb_gossip *g, struct sockaddr_in *dest,
                     socklen_t dest_len, tsdb_node_id_t target_id) {
    uint8_t payload[8];
    memcpy(payload, &target_id, 8);
    uint8_t frame[64];
    int flen = tsdb_gossip_encode(frame, sizeof(frame),
                                  TSDB_GOSSIP_ACK,
                                  payload, 8);
    if (flen <= 0) return;
    sendto(g->udp_fd, frame, (size_t)flen, 0,
           (struct sockaddr *)dest, dest_len);
}

/* ---- Recv thread --------------------------------------------------------- */

static void *recv_loop(void *arg) {
    struct tsdb_gossip *g = (struct tsdb_gossip *)arg;

    uint8_t buf[RECV_BUF_SIZE];
    struct sockaddr_in src;
    socklen_t srclen;

    while (g->running) {
        struct pollfd pfd = { g->udp_fd, POLLIN, 0 };
        if (poll(&pfd, 1, 100) <= 0) continue;

        srclen = sizeof(src);
        ssize_t n = recvfrom(g->udp_fd, buf, sizeof(buf), 0,
                             (struct sockaddr *)&src, &srclen);
        if (n <= 0) continue;

        tsdb_gossip_msg_t type;
        const uint8_t *payload;
        uint16_t plen;
        if (tsdb_gossip_decode(buf, (size_t)n, &type, &payload, &plen) < 0)
            continue;

        switch (type) {
        case TSDB_GOSSIP_PING:
            if (plen >= 8) {
                tsdb_node_id_t target;
                memcpy(&target, payload, 8);
                /* ACK back the ping. */
                send_ack(g, &src, srclen, target);
                /* Also update heartbeat for the sender. */
                tsdb_node_manager_alive(g->node_mgr, target, 0);
            }
            break;

        case TSDB_GOSSIP_ACK:
            if (plen >= 8) {
                tsdb_node_id_t acked_id;
                memcpy(&acked_id, payload, 8);
                /* Mark that node as alive. */
                tsdb_node_manager_alive(g->node_mgr, acked_id, 0);

                /* Clear probe_waiting if this matches our pending probe. */
                pthread_mutex_lock(&g->probe_lock);
                if (g->probe_waiting && g->probe_target == acked_id) {
                    g->probe_waiting = 0;
                    /* Reset fail count. */
                    for (int i = 0; i < g->nprobe_ids; i++) {
                        if (g->probe_ids[i] == acked_id) {
                            g->probe_fails[i] = 0;
                            break;
                        }
                    }
                }
                pthread_mutex_unlock(&g->probe_lock);
            }
            break;

        case TSDB_GOSSIP_PING_REQ:
            /* Indirect ping: forward a PING to the target and relay ACK back.
             * For simplicity: we ping the target directly, then if we get ACK
             * we send an ACK back to the requester (src). */
            if (plen >= 8) {
                tsdb_node_id_t target;
                memcpy(&target, payload, 8);
                tsdb_node_info_t info = {0};
                if (tsdb_node_manager_get(g->node_mgr, target, &info) == 0) {
                    send_ping(g, info.gossip_addr, target);
                    /* TODO: relay ACK back to src if we receive it */
                }
            }
            break;

        case TSDB_GOSSIP_STATE_SYNC:
            if (plen > 0) {
                tsdb_node_manager_decode(g->node_mgr, payload, plen);
            }
            break;
        }
    }
    return NULL;
}

/* ---- Tick thread --------------------------------------------------------- */

static void *tick_loop(void *arg) {
    struct tsdb_gossip *g = (struct tsdb_gossip *)arg;

    while (g->running) {
        /* Sleep GOSSIP_INTERVAL_MS ms in 10ms increments so we can exit cleanly. */
        for (int i = 0; i < GOSSIP_INTERVAL_MS / 10 && g->running; i++) {
            struct timespec sl = { 0, 10000000L };
            nanosleep(&sl, NULL);
        }
        if (!g->running) break;

        tsdb_node_manager_heartbeat(g->node_mgr);

        /* 1. Fan out STATE_SYNC to 3 random ALIVE peers. */
        tsdb_node_id_t peers[3];
        int np = tsdb_node_manager_random_alive(g->node_mgr, peers, 3);
        for (int i = 0; i < np; i++) {
            tsdb_node_info_t info = {0};
            if (tsdb_node_manager_get(g->node_mgr, peers[i], &info) == 0) {
                send_state_sync(g, info.gossip_addr);
            }
        }

        /* 2. Also send state sync to seeds (for bootstrapping). */
        pthread_mutex_lock(&g->seed_lock);
        for (int s = 0; s < g->nseeds; s++) {
            send_state_sync(g, g->seeds[s]);
        }
        pthread_mutex_unlock(&g->seed_lock);

        /* 3. SWIM probe: pick one random ALIVE peer and PING it. */
        tsdb_node_id_t probe_target = 0;
        int n1 = tsdb_node_manager_random_alive(g->node_mgr, &probe_target, 1);

        if (n1 == 1) {
            tsdb_node_info_t pinfo = {0};
            if (tsdb_node_manager_get(g->node_mgr, probe_target, &pinfo) == 0) {
                pthread_mutex_lock(&g->probe_lock);
                g->probe_target  = probe_target;
                g->probe_sent_ns = mono_ns();
                g->probe_waiting = 1;
                pthread_mutex_unlock(&g->probe_lock);

                send_ping(g, pinfo.gossip_addr, probe_target);

                /* Wait PING_TIMEOUT_MS for ACK. */
                struct timespec sl = { 0, (long)PING_TIMEOUT_MS * 1000000L };
                nanosleep(&sl, NULL);

                pthread_mutex_lock(&g->probe_lock);
                int still_waiting = g->probe_waiting;
                pthread_mutex_unlock(&g->probe_lock);

                if (still_waiting) {
                    /* No direct ACK: try indirect via 2 peers. */
                    tsdb_node_id_t helpers[2];
                    int nh = tsdb_node_manager_random_alive(g->node_mgr, helpers, 2);
                    for (int h = 0; h < nh; h++) {
                        if (helpers[h] == probe_target) continue;
                        tsdb_node_info_t hinfo = {0};
                        if (tsdb_node_manager_get(g->node_mgr, helpers[h], &hinfo) == 0) {
                            /* Send PING_REQ with target id payload. */
                            uint8_t payload[8];
                            memcpy(payload, &probe_target, 8);
                            uint8_t frame[64];
                            int flen = tsdb_gossip_encode(frame, sizeof(frame),
                                                          TSDB_GOSSIP_PING_REQ,
                                                          payload, 8);
                            if (flen > 0) {
                                struct sockaddr_in hsa;
                                if (addr_to_sin(hinfo.gossip_addr, &hsa) == 0) {
                                    sendto(g->udp_fd, frame, (size_t)flen, 0,
                                           (struct sockaddr *)&hsa, sizeof(hsa));
                                }
                            }
                        }
                    }

                    /* Wait another PING_TIMEOUT_MS for indirect ACK. */
                    nanosleep(&sl, NULL);

                    pthread_mutex_lock(&g->probe_lock);
                    still_waiting = g->probe_waiting;
                    pthread_mutex_unlock(&g->probe_lock);

                    if (still_waiting) {
                        /* Count failure. */
                        pthread_mutex_lock(&g->probe_lock);
                        int fidx = -1;
                        for (int fi = 0; fi < g->nprobe_ids; fi++) {
                            if (g->probe_ids[fi] == probe_target) { fidx = fi; break; }
                        }
                        if (fidx < 0 && g->nprobe_ids < TSDB_CLUSTER_MAX_NODES) {
                            fidx = g->nprobe_ids++;
                            g->probe_ids[fidx]   = probe_target;
                            g->probe_fails[fidx]  = 0;
                        }
                        if (fidx >= 0) {
                            g->probe_fails[fidx]++;
                            int fails = g->probe_fails[fidx];
                            pthread_mutex_unlock(&g->probe_lock);

                            if (fails >= SUSPECT_THRESHOLD) {
                                tsdb_node_manager_suspect(g->node_mgr, probe_target);
                            }
                        } else {
                            pthread_mutex_unlock(&g->probe_lock);
                        }
                    }
                }
            }
        }

        /* 4. Check for SUSPECT → DEAD transitions. */
        {
            tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
            int nsn = tsdb_node_manager_snapshot(g->node_mgr, snap, TSDB_CLUSTER_MAX_NODES);
            int64_t now = mono_ns();
            tsdb_node_id_t local = tsdb_node_manager_local_id(g->node_mgr);
            for (int i = 0; i < nsn; i++) {
                if (snap[i].id == local) continue;
                if (snap[i].state == TSDB_NODE_SUSPECT) {
                    int64_t elapsed = now - snap[i].last_heartbeat_ns;
                    if (elapsed > DEAD_TIMEOUT_NS) {
                        tsdb_node_manager_dead(g->node_mgr, snap[i].id);
                        printf("[gossip] node %llu declared DEAD (no heartbeat for 10s)\n",
                               (unsigned long long)snap[i].id);
                    }
                }
            }
        }
    }
    return NULL;
}

/* ---- Public API ---------------------------------------------------------- */

tsdb_gossip_t *tsdb_gossip_new(const char *bind_addr,
                               tsdb_node_manager_t *node_mgr)
{
    char host[128] = "0.0.0.0";
    int  port = TSDB_GOSSIP_DEFAULT_PORT;
    if (bind_addr) {
        const char *colon = strrchr(bind_addr, ':');
        if (colon) {
            size_t hlen = (size_t)(colon - bind_addr);
            if (hlen < sizeof(host)) {
                memcpy(host, bind_addr, hlen);
                host[hlen] = '\0';
            }
            port = atoi(colon + 1);
        }
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return NULL;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton(host, &sa.sin_addr);

    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd); return NULL;
    }

    /* Get actual port. */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);

    struct tsdb_gossip *g = calloc(1, sizeof(*g));
    if (!g) { close(fd); return NULL; }
    g->udp_fd   = fd;
    g->port     = ntohs(bound.sin_port);
    g->node_mgr = node_mgr;
    g->running  = 1;
    pthread_mutex_init(&g->seed_lock, NULL);
    pthread_mutex_init(&g->probe_lock, NULL);

    pthread_create(&g->recv_thread, NULL, recv_loop, g);
    pthread_create(&g->tick_thread, NULL, tick_loop, g);

    return g;
}

void tsdb_gossip_stop(tsdb_gossip_t *g) {
    if (!g) return;
    g->running = 0;
    pthread_join(g->recv_thread, NULL);
    pthread_join(g->tick_thread, NULL);
    close(g->udp_fd);
    pthread_mutex_destroy(&g->seed_lock);
    pthread_mutex_destroy(&g->probe_lock);
    free(g);
}

void tsdb_gossip_add_seed(tsdb_gossip_t *g, const char *addr) {
    if (!g || !addr) return;
    pthread_mutex_lock(&g->seed_lock);
    if (g->nseeds < MAX_SEEDS) {
        snprintf(g->seeds[g->nseeds++], TSDB_ADDR_MAX, "%s", addr);
    }
    pthread_mutex_unlock(&g->seed_lock);

    /* Immediate STATE_SYNC to the new seed. */
    send_state_sync(g, addr);
}

int tsdb_gossip_port(tsdb_gossip_t *g) {
    return g ? g->port : -1;
}
