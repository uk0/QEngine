/* disk_weight.h — derive a weighted-hashring vn count from disk capacity.
 *
 * The weighted hashring (src/cluster/hashring.h) takes a `vn_count` per
 * node that determines its share of the keyspace.  Operators can pass
 * this explicitly, but most deployments want "just use whatever disk
 * this node happens to have."  This helper walks statvfs(data_dir) and
 * scales the result to a reasonable vn count.
 *
 *   4 TB → 400 vn
 *   3 TB → 300 vn
 *   5 TB → 500 vn
 *   (caller supplies base_vn_per_tb; default 100 is sensible)
 *
 * Clamped into [TSDB_DISK_WEIGHT_MIN_VN, TSDB_DISK_WEIGHT_MAX_VN] so a
 * tiny dev box (say 64 GiB) or a monster (100 TB) still behaves.
 */
#ifndef TSDB_CLUSTER_DISK_WEIGHT_H
#define TSDB_CLUSTER_DISK_WEIGHT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_DISK_WEIGHT_MIN_VN  32
#define TSDB_DISK_WEIGHT_MAX_VN  10000
#define TSDB_DISK_WEIGHT_DEFAULT_PER_TB  100

/* Query the filesystem at <data_dir> and return a vn_count proportional
 * to its total capacity.  base_vn_per_tb is the scale factor (100 is a
 * sensible default — results in 100 vn per TB).  Pass 0 for default.
 *
 * Returns -1 on statvfs failure (caller may fall back to a fixed
 * vn_count). */
int tsdb_disk_weight_from_statvfs(const char *data_dir, int base_vn_per_tb);

/* Same but also report the raw total_bytes via out_total.  Either
 * out pointer can be NULL. */
int tsdb_disk_weight_detail(const char *data_dir, int base_vn_per_tb,
                             uint64_t *out_total_bytes,
                             uint64_t *out_free_bytes);

#ifdef __cplusplus
}
#endif
#endif /* TSDB_CLUSTER_DISK_WEIGHT_H */
