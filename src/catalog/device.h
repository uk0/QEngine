/* device.h — Device catalog: per-group device metadata.
 *
 * Devices belong to a group and represent individual physical entities.
 * Persisted as append-only text log: <data_dir>/catalog/devices.log
 * Memory index: group_name -> (device_id -> tsdb_device_t*)
 */
#ifndef TSDB_CATALOG_DEVICE_H
#define TSDB_CATALOG_DEVICE_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"
#include "group.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char      group[64];
    char      id[128];           /* device unique id within group */
    char      type[32];          /* "temperature" / "pressure" / ... */
    char      location[128];
    char      tags[512];         /* flat key=value pairs, comma-separated */
    tsdb_ts_t first_seen;
    tsdb_ts_t last_seen;
    int       status;            /* 0=active 1=offline 2=retired */
} tsdb_device_t;

int tsdb_device_create(tsdb_catalog_t *c, const tsdb_device_t *d);
int tsdb_device_get(tsdb_catalog_t *c, const char *group, const char *id,
                    tsdb_device_t *out);
/* Returns array in *out_arr (caller calls tsdb_device_list_free). */
int tsdb_device_list(tsdb_catalog_t *c, const char *group,
                     tsdb_device_t **out_arr, size_t *out_n);
void tsdb_device_list_free(tsdb_device_t *arr);
int tsdb_device_drop(tsdb_catalog_t *c, const char *group, const char *id);
int tsdb_device_update_last_seen(tsdb_catalog_t *c, const char *group,
                                 const char *id, tsdb_ts_t ts);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CATALOG_DEVICE_H */
