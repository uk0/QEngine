/* gossip.h — SWIM-lite UDP gossip protocol.
 *
 * Every 500ms:
 *  1. Pick 3 random ALIVE peers.
 *  2. Send STATE_SYNC to each (our full membership digest).
 *  3. Run SWIM ping toward one random peer; if no ACK within 200ms,
 *     issue indirect PING_REQ to 2 other peers; 3 consecutive failures
 *     → SUSPECT; 10s in SUSPECT → DEAD.
 *
 * Wire format per UDP datagram:
 *   magic        u32  = 'TSGS' (0x53475354)
 *   ver          u8   = 1
 *   msg_type     u8
 *   payload_len  u16
 *   payload      [payload_len bytes]
 *
 * msg_type:
 *   PING=1, ACK=2, PING_REQ=3, STATE_SYNC=4
 */
#ifndef TSDB_CLUSTER_GOSSIP_H
#define TSDB_CLUSTER_GOSSIP_H

#include "node.h"
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_GOSSIP_MAGIC     0x53475354u  /* "TSGS" LE */
#define TSDB_GOSSIP_VER       1
#define TSDB_GOSSIP_HDR_SIZE  8u

/* Default UDP gossip port. */
#define TSDB_GOSSIP_DEFAULT_PORT 28080

/* Gossip message types. */
typedef enum {
    TSDB_GOSSIP_PING      = 1,
    TSDB_GOSSIP_ACK       = 2,
    TSDB_GOSSIP_PING_REQ  = 3,
    TSDB_GOSSIP_STATE_SYNC = 4
} tsdb_gossip_msg_t;

/* Opaque gossip engine. */
typedef struct tsdb_gossip tsdb_gossip_t;

/*
 * Create and start the gossip engine.
 * bind_addr   - local "host:port" to bind UDP socket
 * node_mgr    - shared membership state
 *
 * Seeds are added via tsdb_node_manager_upsert before calling this,
 * or via tsdb_gossip_add_seed() after.
 */
tsdb_gossip_t *tsdb_gossip_new(const char *bind_addr,
                               tsdb_node_manager_t *node_mgr);

/* Stop gossip threads and free resources. */
void tsdb_gossip_stop(tsdb_gossip_t *g);

/* Add a seed node that we haven't yet heard from.
 * addr = "host:port" (gossip UDP port).
 * This causes an immediate STATE_SYNC to that address. */
void tsdb_gossip_add_seed(tsdb_gossip_t *g, const char *addr);

/* Return the UDP port the gossip socket is bound to. */
int tsdb_gossip_port(tsdb_gossip_t *g);

/* Encode a gossip frame.
 * buf must hold at least TSDB_GOSSIP_HDR_SIZE + payload_len bytes.
 * Returns total bytes written or -1. */
int tsdb_gossip_encode(uint8_t *buf, size_t buf_cap,
                       tsdb_gossip_msg_t type,
                       const uint8_t *payload, uint16_t payload_len);

/* Decode a gossip frame header. Fills type, payload pointer, payload_len.
 * Returns 0 on success, -1 on bad magic/ver. */
int tsdb_gossip_decode(const uint8_t *buf, size_t len,
                       tsdb_gossip_msg_t *out_type,
                       const uint8_t **out_payload, uint16_t *out_payload_len);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_GOSSIP_H */
