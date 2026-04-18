/* proto.c — wire-protocol frame encode / decode.
 *
 * CRC32C (Castagnoli) polynomial: 0x82F63B78 (reflected).
 * Known test vector: crc32c("123456789") == 0xE3069283.
 */

#include "tls.h"   /* must come before proto.h to expose tsdb_io_t */
#include "proto.h"
#include "../../include/tsdb.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

/* ---- CRC32C hardware dispatch ------------------------------------------- *
 *
 * Three implementations register at init time via pthread_once; the best
 * one available at runtime is picked and stored in crc32c_step_fn:
 *
 *   1. x86 SSE4.2 — _mm_crc32_u64  (~15–20 GB/s)
 *   2. ARMv8 CRC  — __crc32cd      (~15 GB/s on Apple Silicon / Neoverse)
 *   3. Sarwate table, 8 bytes/step (~1.5 GB/s, C11 fallback)
 *
 * The `step` functions take a running CRC in its internal form (before the
 * final XOR 0xFFFFFFFF) and return the updated running CRC.  Public
 * tsdb_crc32c applies the XOR at the boundaries; tsdb_crc32c_update lets
 * callers compose multiple chunks without paying twice.
 *
 * Castagnoli (iSCSI) reflected polynomial: 0x82F63B78.
 * Test vector: tsdb_crc32c("123456789", 9) == 0xE3069283.
 */

#define CRC32C_POLY 0x82F63B78u

static uint32_t crc32c_table[8][256];
static pthread_once_t crc32c_once = PTHREAD_ONCE_INIT;

static void crc32c_build_table(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (CRC32C_POLY ^ (c >> 1)) : (c >> 1);
        crc32c_table[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = crc32c_table[0][i];
        for (int s = 1; s < 8; s++) {
            c = crc32c_table[0][c & 0xFF] ^ (c >> 8);
            crc32c_table[s][i] = c;
        }
    }
}

/* Scalar Sarwate step — always compiled as the lowest-common-denominator
 * fallback.  Operates on the internal running CRC (pre-final-XOR). */
static uint32_t crc32c_step_sw(uint32_t crc, const uint8_t *p, size_t n) {
    while (n >= 8) {
        uint32_t w0, w1;
        memcpy(&w0, p,     4);
        memcpy(&w1, p + 4, 4);
        uint32_t c0 = crc ^ w0;
        uint32_t c1 = w1;
        crc = crc32c_table[7][ c0        & 0xFF]
            ^ crc32c_table[6][(c0 >>  8) & 0xFF]
            ^ crc32c_table[5][(c0 >> 16) & 0xFF]
            ^ crc32c_table[4][ c0 >> 24        ]
            ^ crc32c_table[3][ c1        & 0xFF]
            ^ crc32c_table[2][(c1 >>  8) & 0xFF]
            ^ crc32c_table[1][(c1 >> 16) & 0xFF]
            ^ crc32c_table[0][ c1 >> 24        ];
        p += 8;
        n -= 8;
    }
    while (n-- > 0)
        crc = crc32c_table[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
    return crc;
}

#if defined(__x86_64__) || defined(__i386__)
#  include <nmmintrin.h>        /* _mm_crc32_u64 / _mm_crc32_u8 */
#  include <cpuid.h>

__attribute__((target("sse4.2")))
static uint32_t crc32c_step_hw_x86(uint32_t crc, const uint8_t *p, size_t n) {
#  if defined(__x86_64__)
    while (n >= 8) {
        uint64_t v;
        memcpy(&v, p, 8);
        crc = (uint32_t)_mm_crc32_u64((uint64_t)crc, v);
        p += 8; n -= 8;
    }
#  endif
    while (n >= 4) {
        uint32_t v;
        memcpy(&v, p, 4);
        crc = _mm_crc32_u32(crc, v);
        p += 4; n -= 4;
    }
    while (n-- > 0) crc = _mm_crc32_u8(crc, *p++);
    return crc;
}

static int x86_has_sse42(void) {
    unsigned eax, ebx, ecx, edx;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) return 0;
    return (ecx & (1u << 20)) != 0;   /* CPUID.1:ECX.SSE4_2 */
}
#endif  /* x86 */

#if defined(__aarch64__)
#  include <arm_acle.h>
#  if defined(__linux__)
#    include <sys/auxv.h>
#    ifdef __has_include
#      if __has_include(<asm/hwcap.h>)
#        include <asm/hwcap.h>
#      endif
#    endif
#  endif

__attribute__((target("+crc")))
static uint32_t crc32c_step_hw_arm(uint32_t crc, const uint8_t *p, size_t n) {
    while (n >= 8) {
        uint64_t v;
        memcpy(&v, p, 8);
        crc = __crc32cd(crc, v);
        p += 8; n -= 8;
    }
    while (n >= 4) {
        uint32_t v;
        memcpy(&v, p, 4);
        crc = __crc32cw(crc, v);
        p += 4; n -= 4;
    }
    while (n-- > 0) crc = __crc32cb(crc, *p++);
    return crc;
}

static int arm_has_crc(void) {
#  if defined(__APPLE__)
    return 1;   /* every arm64 Apple chip has CRC32 */
#  elif defined(__linux__) && defined(HWCAP_CRC32)
    return (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
#  else
    return 0;
#  endif
}
#endif  /* aarch64 */

typedef uint32_t (*crc32c_step_fn_t)(uint32_t, const uint8_t *, size_t);
static crc32c_step_fn_t crc32c_step_fn = NULL;
static const char       *crc32c_impl_name = "sarwate";

static void crc32c_init(void) {
    crc32c_build_table();
    crc32c_step_fn   = crc32c_step_sw;
    crc32c_impl_name = "sarwate";

#if defined(__x86_64__) || defined(__i386__)
    if (x86_has_sse42()) {
        crc32c_step_fn   = crc32c_step_hw_x86;
        crc32c_impl_name = "sse4.2";
    }
#elif defined(__aarch64__)
    if (arm_has_crc()) {
        crc32c_step_fn   = crc32c_step_hw_arm;
        crc32c_impl_name = "armv8-crc";
    }
#endif
}

/* Incremental update — callers supply the running CRC in its finalised form
 * (i.e. the value they would send on the wire).  Internally we convert to
 * the pre-finalisation domain, step, then re-finalise. */
uint32_t tsdb_crc32c_update(uint32_t crc, const void *data, size_t n) {
    pthread_once(&crc32c_once, crc32c_init);
    uint32_t running = crc ^ 0xFFFFFFFFu;
    running = crc32c_step_fn(running, (const uint8_t *)data, n);
    return running ^ 0xFFFFFFFFu;
}

uint32_t tsdb_crc32c(const void *data, size_t n) {
    pthread_once(&crc32c_once, crc32c_init);
    uint32_t running = 0xFFFFFFFFu;
    running = crc32c_step_fn(running, (const uint8_t *)data, n);
    return running ^ 0xFFFFFFFFu;
}

/* Diagnostic: returns "sse4.2" / "armv8-crc" / "sarwate". */
const char *tsdb_crc32c_impl(void) {
    pthread_once(&crc32c_once, crc32c_init);
    return crc32c_impl_name;
}

/* ---- Header serialise / parse ------------------------------------------- */

/*
 * Wire layout (24 bytes, all LE):
 *   [0..4)   magic
 *   [4]      ver
 *   [5]      type
 *   [6..8)   flags
 *   [8..12)  reserved (0)
 *   [12..20) req_id
 *   [20..24) payload_len
 */
void tsdb_frame_write_hdr(const tsdb_frame_hdr_t *h, uint8_t out[24]) {
    memset(out, 0, 24);
    /* magic */
    out[0] = (uint8_t)(h->magic);
    out[1] = (uint8_t)(h->magic >> 8);
    out[2] = (uint8_t)(h->magic >> 16);
    out[3] = (uint8_t)(h->magic >> 24);
    /* ver, type */
    out[4] = h->ver;
    out[5] = h->type;
    /* flags (LE) */
    out[6] = (uint8_t)(h->flags);
    out[7] = (uint8_t)(h->flags >> 8);
    /* reserved [8..12) = 0 */
    /* req_id (LE) */
    out[12] = (uint8_t)(h->req_id);
    out[13] = (uint8_t)(h->req_id >> 8);
    out[14] = (uint8_t)(h->req_id >> 16);
    out[15] = (uint8_t)(h->req_id >> 24);
    out[16] = (uint8_t)(h->req_id >> 32);
    out[17] = (uint8_t)(h->req_id >> 40);
    out[18] = (uint8_t)(h->req_id >> 48);
    out[19] = (uint8_t)(h->req_id >> 56);
    /* payload_len (LE) */
    out[20] = (uint8_t)(h->payload_len);
    out[21] = (uint8_t)(h->payload_len >> 8);
    out[22] = (uint8_t)(h->payload_len >> 16);
    out[23] = (uint8_t)(h->payload_len >> 24);
}

int tsdb_frame_read_hdr(const uint8_t in[24], tsdb_frame_hdr_t *out) {
    uint32_t magic = (uint32_t)in[0]
                   | ((uint32_t)in[1] << 8)
                   | ((uint32_t)in[2] << 16)
                   | ((uint32_t)in[3] << 24);
    if (magic != TSDB_PROTO_MAGIC)
        return TSDB_ERR_CORRUPT;

    out->magic   = magic;
    out->ver     = in[4];
    out->type    = in[5];
    out->flags   = (uint16_t)in[6] | ((uint16_t)in[7] << 8);
    /* reserved in[8..12) ignored */
    out->req_id  = (uint64_t)in[12]
                 | ((uint64_t)in[13] << 8)
                 | ((uint64_t)in[14] << 16)
                 | ((uint64_t)in[15] << 24)
                 | ((uint64_t)in[16] << 32)
                 | ((uint64_t)in[17] << 40)
                 | ((uint64_t)in[18] << 48)
                 | ((uint64_t)in[19] << 56);
    out->payload_len = (uint32_t)in[20]
                     | ((uint32_t)in[21] << 8)
                     | ((uint32_t)in[22] << 16)
                     | ((uint32_t)in[23] << 24);
    return TSDB_OK;
}

/* ---- I/O helpers --------------------------------------------------------- */

static int write_all(int fd, const uint8_t *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, buf + done, n - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            return TSDB_ERR_IO;
        }
        if (w == 0) return TSDB_ERR_IO;
        done += (size_t)w;
    }
    return TSDB_OK;
}

static int read_all(int fd, uint8_t *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, buf + done, n - done);
        if (r < 0) {
            if (errno == EINTR) continue;
            return TSDB_ERR_IO;
        }
        if (r == 0) return TSDB_ERR_IO;  /* EOF */
        done += (size_t)r;
    }
    return TSDB_OK;
}

/* ---- Public send / recv -------------------------------------------------- */

/*
 * Frame on wire:
 *   24-byte header
 *   payload_len bytes of payload
 *   4-byte CRC32C LE — computed over bytes [4..24+payload_len)
 *                       i.e. ver + type + flags + reserved + req_id + payload_len + payload
 */
int tsdb_proto_send(int fd, uint8_t type, uint16_t flags, uint64_t req_id,
                    const void *payload, size_t n)
{
    if (n > TSDB_PROTO_MAX_PAYLOAD)
        return TSDB_ERR_INVAL;

    tsdb_frame_hdr_t hdr = {
        .magic       = TSDB_PROTO_MAGIC,
        .ver         = TSDB_PROTO_VER,
        .type        = type,
        .flags       = flags,
        .req_id      = req_id,
        .payload_len = (uint32_t)n,
    };

    uint8_t raw_hdr[TSDB_PROTO_HDR_SIZE];
    tsdb_frame_write_hdr(&hdr, raw_hdr);

    /* CRC32C over header[4..24) + payload, via hardware-dispatched step. */
    uint32_t crc = 0;
    crc = tsdb_crc32c_update(crc, raw_hdr + 4, TSDB_PROTO_HDR_SIZE - 4);
    if (payload && n > 0) crc = tsdb_crc32c_update(crc, payload, n);

    uint8_t crc_bytes[4];
    crc_bytes[0] = (uint8_t)(crc);
    crc_bytes[1] = (uint8_t)(crc >> 8);
    crc_bytes[2] = (uint8_t)(crc >> 16);
    crc_bytes[3] = (uint8_t)(crc >> 24);

    int rc;
    if ((rc = write_all(fd, raw_hdr, TSDB_PROTO_HDR_SIZE)) != TSDB_OK) return rc;
    if (n > 0 && payload) {
        if ((rc = write_all(fd, (const uint8_t *)payload, n)) != TSDB_OK) return rc;
    }
    return write_all(fd, crc_bytes, 4);
}

int tsdb_proto_recv(int fd, tsdb_frame_hdr_t *hdr, uint8_t **out_payload) {
    uint8_t raw_hdr[TSDB_PROTO_HDR_SIZE];
    int rc;

    if ((rc = read_all(fd, raw_hdr, TSDB_PROTO_HDR_SIZE)) != TSDB_OK)
        return rc;

    if ((rc = tsdb_frame_read_hdr(raw_hdr, hdr)) != TSDB_OK)
        return rc;

    if (hdr->payload_len > TSDB_PROTO_MAX_PAYLOAD)
        return TSDB_ERR_CORRUPT;

    uint8_t *payload = NULL;
    if (hdr->payload_len > 0) {
        payload = (uint8_t *)malloc(hdr->payload_len);
        if (!payload) return TSDB_ERR_NOMEM;
        if ((rc = read_all(fd, payload, hdr->payload_len)) != TSDB_OK) {
            free(payload);
            return rc;
        }
    }

    /* Read and verify CRC32C tail. */
    uint8_t crc_bytes[4];
    if ((rc = read_all(fd, crc_bytes, 4)) != TSDB_OK) {
        free(payload);
        return rc;
    }

    uint32_t stored_crc = (uint32_t)crc_bytes[0]
                        | ((uint32_t)crc_bytes[1] << 8)
                        | ((uint32_t)crc_bytes[2] << 16)
                        | ((uint32_t)crc_bytes[3] << 24);

    uint32_t computed_crc = 0;
    computed_crc = tsdb_crc32c_update(computed_crc, raw_hdr + 4, TSDB_PROTO_HDR_SIZE - 4);
    if (payload && hdr->payload_len > 0)
        computed_crc = tsdb_crc32c_update(computed_crc, payload, hdr->payload_len);

    if (computed_crc != stored_crc) {
        free(payload);
        return TSDB_ERR_CORRUPT;
    }

    *out_payload = payload;
    return TSDB_OK;
}

/* ---- TLS-aware I/O helpers ----------------------------------------------- */

static int write_all_io(tsdb_io_t *io, const uint8_t *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t w = tsdb_io_write(io, buf + done, n - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            return TSDB_ERR_IO;
        }
        if (w == 0) return TSDB_ERR_IO;
        done += (size_t)w;
    }
    return TSDB_OK;
}

static int read_all_io(tsdb_io_t *io, uint8_t *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = tsdb_io_read(io, buf + done, n - done);
        if (r < 0) {
            if (errno == EINTR) continue;
            return TSDB_ERR_IO;
        }
        if (r == 0) return TSDB_ERR_IO;  /* EOF / clean shutdown */
        done += (size_t)r;
    }
    return TSDB_OK;
}

/* Send a frame over an io abstraction (plain fd or TLS). */
int tsdb_proto_send_io(tsdb_io_t *io, uint8_t type, uint16_t flags,
                       uint64_t req_id, const void *payload, size_t n)
{
    if (!io) return TSDB_ERR_INVAL;
    if (n > TSDB_PROTO_MAX_PAYLOAD) return TSDB_ERR_INVAL;

    tsdb_frame_hdr_t hdr = {
        .magic       = TSDB_PROTO_MAGIC,
        .ver         = TSDB_PROTO_VER,
        .type        = type,
        .flags       = flags,
        .req_id      = req_id,
        .payload_len = (uint32_t)n,
    };

    uint8_t raw_hdr[TSDB_PROTO_HDR_SIZE];
    tsdb_frame_write_hdr(&hdr, raw_hdr);

    uint32_t crc = 0;
    crc = tsdb_crc32c_update(crc, raw_hdr + 4, TSDB_PROTO_HDR_SIZE - 4);
    if (payload && n > 0) crc = tsdb_crc32c_update(crc, payload, n);

    uint8_t crc_bytes[4];
    crc_bytes[0]=(uint8_t)(crc);      crc_bytes[1]=(uint8_t)(crc>>8);
    crc_bytes[2]=(uint8_t)(crc>>16);  crc_bytes[3]=(uint8_t)(crc>>24);

    int rc;
    if ((rc = write_all_io(io, raw_hdr, TSDB_PROTO_HDR_SIZE)) != TSDB_OK) return rc;
    if (n > 0 && payload)
        if ((rc = write_all_io(io, (const uint8_t *)payload, n)) != TSDB_OK) return rc;
    return write_all_io(io, crc_bytes, 4);
}

/* Receive a frame over an io abstraction (plain fd or TLS). */
int tsdb_proto_recv_io(tsdb_io_t *io, tsdb_frame_hdr_t *hdr,
                       uint8_t **out_payload)
{
    if (!io) return TSDB_ERR_INVAL;

    uint8_t raw_hdr[TSDB_PROTO_HDR_SIZE];
    int rc;

    if ((rc = read_all_io(io, raw_hdr, TSDB_PROTO_HDR_SIZE)) != TSDB_OK)
        return rc;

    if ((rc = tsdb_frame_read_hdr(raw_hdr, hdr)) != TSDB_OK)
        return rc;

    if (hdr->payload_len > TSDB_PROTO_MAX_PAYLOAD)
        return TSDB_ERR_CORRUPT;

    uint8_t *payload = NULL;
    if (hdr->payload_len > 0) {
        payload = (uint8_t *)malloc(hdr->payload_len);
        if (!payload) return TSDB_ERR_NOMEM;
        if ((rc = read_all_io(io, payload, hdr->payload_len)) != TSDB_OK) {
            free(payload); return rc;
        }
    }

    uint8_t crc_bytes[4];
    if ((rc = read_all_io(io, crc_bytes, 4)) != TSDB_OK) {
        free(payload); return rc;
    }

    uint32_t stored_crc = (uint32_t)crc_bytes[0]
                        | ((uint32_t)crc_bytes[1] << 8)
                        | ((uint32_t)crc_bytes[2] << 16)
                        | ((uint32_t)crc_bytes[3] << 24);

    uint32_t computed_crc = 0;
    computed_crc = tsdb_crc32c_update(computed_crc, raw_hdr + 4, TSDB_PROTO_HDR_SIZE - 4);
    if (payload && hdr->payload_len > 0)
        computed_crc = tsdb_crc32c_update(computed_crc, payload, hdr->payload_len);

    if (computed_crc != stored_crc) {
        free(payload);
        return TSDB_ERR_CORRUPT;
    }

    *out_payload = payload;
    return TSDB_OK;
}
