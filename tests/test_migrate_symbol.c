/* test_migrate_symbol.c — a migrated SYMBOL column must decode to the SAME
 * strings on the target.
 *
 * THE BUG (stream v1): the migration stream carried the schema and the
 * compressed blocks, but NOT the symbol dictionaries.  A SYMBOL column's
 * blocks hold dictionary CODES, so the target decoded them against its own
 * freshly-created (empty) table: import returned TSDB_OK and every tag value
 * queried back as ZERO rows.  Silent, total loss of every tag column.
 *
 * THE FIX (stream v2): export persists each SYMBOL column's live symtab and
 * ships the .sym bytes in the header; import reconciles them with the target's
 * live symtab before any block lands.  A target whose dictionary DISAGREES is
 * refused rather than merged — its codes came from a different dictionary, and
 * reconciling them would mean decoding every block, which is the one thing this
 * SDK exists to avoid.  A dictionary that agrees (identical, or a prefix of the
 * stream's — what a resumed migration finds) is adopted and extended; see
 * tests/test_migrate_resolve.c for that case.
 */
#include "tsdb.h"
#include "tsdb_migrate.h"
#include "../src/storage/db.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int main(void){
  printf("=== test_migrate_symbol ===\n");
  system("rm -rf /tmp/msym_a /tmp/msym_b /tmp/msym.stream");
  tsdb_db_t *a=NULL; tsdb_open("/tmp/msym_a",&a);
  tsdb_col_t c[]={{"ts",TSDB_TYPE_TIMESTAMP},{"host",TSDB_TYPE_SYMBOL},{"v",TSDB_TYPE_FLOAT64}};
  tsdb_create_table(a,"m",c,3,"ts");
  tsdb_table_t *t=NULL; tsdb_open_table(a,"m",&t);
  const char *hosts[3]={"alpha","bravo","charlie"};
  tsdb_batch_t *b=NULL; tsdb_batch_begin(t,&b);
  for(int i=0;i<9000;i++){tsdb_batch_row_ts(b,1700000000000000000LL+i*1000000LL);
    tsdb_batch_row_sym(b,1,hosts[i%3]); tsdb_batch_row_f64(b,2,(double)i); tsdb_batch_row_end(b);}
  tsdb_batch_commit(b); tsdb_db_flush_all(a);
  /* count per host on SOURCE */
  for(int h=0;h<3;h++){char q[128]; snprintf(q,sizeof(q),"SELECT count(*) FROM m WHERE host='%s'",hosts[h]);
    tsdb_result_t*r=NULL; long long n=-1; if(tsdb_query(a,q,&r)==TSDB_OK&&r&&tsdb_result_next(r)>0)n=tsdb_result_i64(r,0);
    if(r)tsdb_result_free(r); printf("SRC %-8s = %lld\n",hosts[h],n);}
  int fd=open("/tmp/msym.stream",O_CREAT|O_TRUNC|O_WRONLY,0644);
  tsdb_mig_stats_t st; int rc=tsdb_migrate_export(a,"m",fd,NULL,&st); close(fd);
  printf("export rc=%d blocks=%llu\n",rc,(unsigned long long)st.blocks);
  tsdb_close(a);
  tsdb_db_t *d=NULL; tsdb_open("/tmp/msym_b",&d);
  fd=open("/tmp/msym.stream",O_RDONLY); rc=tsdb_migrate_import(d,fd,NULL,NULL); close(fd);
  printf("import rc=%d\n",rc);
  for(int h=0;h<3;h++){char q[128]; snprintf(q,sizeof(q),"SELECT count(*) FROM m WHERE host='%s'",hosts[h]);
    tsdb_result_t*r=NULL; long long n=-1; if(tsdb_query(d,q,&r)==TSDB_OK&&r&&tsdb_result_next(r)>0)n=tsdb_result_i64(r,0);
    if(r)tsdb_result_free(r); printf("DST %-8s = %lld  %s\n",hosts[h],n,n==3000?"ok":"<<< WRONG");}
  int bad = 0;
  for (int h = 0; h < 3; h++) {
    char q[128]; snprintf(q, sizeof(q), "SELECT count(*) FROM m WHERE host='%s'", hosts[h]);
    tsdb_result_t *r = NULL; long long n = -1;
    if (tsdb_query(d, q, &r) == TSDB_OK && r && tsdb_result_next(r) > 0) n = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);
    if (n != 3000) bad++;
  }
  tsdb_close(d);
  system("rm -rf /tmp/msym_a /tmp/msym_b /tmp/msym.stream");
  if (bad) { fprintf(stderr, "FAIL: %d tag value(s) did not survive migration\n", bad); return 1; }
  printf("\n=== test_migrate_symbol PASSED ===\n");
  return 0;
}
