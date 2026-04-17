/* tsdb_cli — interactive REPL over a tsdb database.
 *
 * Usage: tsdb_cli <data_dir>
 */
#include "tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

static void print_result(tsdb_result_t *r) {
    int nc = tsdb_result_ncols(r);
    for (int i = 0; i < nc; i++) {
        if (i) printf(" | ");
        printf("%s", tsdb_result_col_name(r, i));
    }
    printf("\n");
    for (int i = 0; i < nc; i++) {
        if (i) printf("-+-");
        int w = (int)strlen(tsdb_result_col_name(r, i));
        for (int j = 0; j < w; j++) printf("-");
    }
    printf("\n");
    int rows = 0;
    while (tsdb_result_next(r)) {
        for (int i = 0; i < nc; i++) {
            if (i) printf(" | ");
            switch (tsdb_result_col_type(r, i)) {
            case TSDB_TYPE_TIMESTAMP:
                printf("%lld", (long long)tsdb_result_ts(r, i));
                break;
            case TSDB_TYPE_INT64:
                printf("%lld", (long long)tsdb_result_i64(r, i));
                break;
            case TSDB_TYPE_FLOAT64:
                printf("%.4f", tsdb_result_f64(r, i));
                break;
            case TSDB_TYPE_SYMBOL: {
                const char *s = tsdb_result_sym(r, i);
                printf("%s", s ? s : "NULL");
                break;
            }
            default: printf("?");
            }
        }
        printf("\n");
        rows++;
    }
    printf("(%d rows)\n", rows);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <data_dir>\n", argv[0]);
        return 1;
    }
    tsdb_db_t *db = NULL;
    int rc = tsdb_open(argv[1], &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "open failed: %s\n", tsdb_errstr(rc));
        return 1;
    }
    printf("tsdb %s — type QTL queries, empty line to quit.\n", tsdb_version());

    char line[4096];
    for (;;) {
        printf("qtl> "); fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;
        /* trim */
        size_t n = strlen(line);
        while (n > 0 && (line[n-1] == '\n' || line[n-1] == '\r' || line[n-1] == ' ')) line[--n] = 0;
        if (n == 0) break;

        tsdb_result_t *r = NULL;
        rc = tsdb_query(db, line, &r);
        if (rc != TSDB_OK) { fprintf(stderr, "error: %s\n", tsdb_errstr(rc)); continue; }
        print_result(r);
        tsdb_result_free(r);
    }
    tsdb_close(db);
    return 0;
}
