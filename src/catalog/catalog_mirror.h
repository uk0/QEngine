/* catalog_mirror.h — dual-write mirror of the v1 catalog into v2 (Track B P4).
 *
 * Translates the whole v1 catalog state (the 4 name-keyed op-logs) into a v2
 * catalog (catalog_v2.h).  Used behind TSDB_CATALOG_V2 to prove the new model
 * faithfully holds the old one before any read path switches over, and as the
 * basis for the P6 greenfield migration.  Mirror into a FRESH v2 (it creates,
 * never reconciles).
 */
#ifndef TSDB_CATALOG_MIRROR_H
#define TSDB_CATALOG_MIRROR_H

#include "catalog_v2.h"

struct tsdb_catalog;   /* v1, opaque (defined in catalog.c) */

/* Mirror v1 → v2.  databases→db, groups→group, stables→TBL_SUPER,
 * children→TBL_CHILD; an orphan child whose parent stable didn't mirror is
 * skipped (matching v2's no-orphan invariant).  Counts land in the optional
 * out-params.  Returns TSDB_OK or a negative error. */
int tsdb_catalog_mirror_to_v2(struct tsdb_catalog *v1, tsdb_catalog_v2_t *v2,
                              size_t *out_mirrored, size_t *out_skipped);

#endif /* TSDB_CATALOG_MIRROR_H */
