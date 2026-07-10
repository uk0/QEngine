/* test_compact_swap_recover.c — regression for interrupted compaction swap.
 *
 * compact_partition renames every produced column's <c>.col.tmp/<c>.idx.tmp
 * into place under one lock, but the renames are not atomic as a SET: a crash
 * (or this engine's fast-exit "controlled crash" shutdown) between them leaves
 * some columns swapped and others not — a block-desynced partition that reads
 * as "data corrupt" with a collapsed row count.
 *
 * The fix journals the produced-column list to <part>/.compact_swap (fsynced
 * before the first rename) and has tsdb_part_open roll a found journal FORWARD
 * by renaming the remaining .tmp pairs. This test reproduces an interrupted
 * swap on disk (good data staged in .tmp, live files torn, marker present) and
 * asserts reopen restores the full row set and retires the journal + .tmp files.
 */
#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static int g_pass = 0, g_fail = 0;
#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *d){ char c[4096]; snprintf(c,sizeof(c),"rm -rf %s",d); (void)system(c); }
static void cp(const char *a, const char *b){ char c[8300]; snprintf(c,sizeof(c),"cp %s %s",a,b); (void)system(c); }
static int exists(const char *p){ struct stat s; return stat(p,&s)==0; }

static int64_t q_count(tsdb_db_t *db, const char *sql){
    tsdb_result_t *r=NULL; if (tsdb_query(db,sql,&r)!=TSDB_OK||!r){ if(r)tsdb_result_free(r); return -1; }
    int64_t v=-1; if (tsdb_result_next(r)) v=tsdb_result_i64(r,0); tsdb_result_free(r); return v;
}

/* First "<table_dir>/<YYYYMMDD>" partition dir. */
static int find_part_dir(const char *table_dir, char *out, size_t cap){
    DIR *d=opendir(table_dir); if(!d) return 0; struct dirent *e; int f=0;
    while((e=readdir(d))){ if(e->d_name[0]<'0'||e->d_name[0]>'9') continue;
        snprintf(out,cap,"%s/%s",table_dir,e->d_name); f=1; break; }
    closedir(d); return f;
}

#define NROWS 20000  /* > block_points → multiple on-disk blocks */

int main(void){
    printf("=== test_compact_swap_recover ===\n");
    const char *dir="/tmp/tsdb_test_compact_swap"; rmrf(dir);
    char part[4096]={0}, table_dir[4096];
    int64_t baseline;

    /* Phase 1: write+flush, capture baseline. */
    {
        tsdb_db_t *db=NULL; if(tsdb_open(dir,&db)!=TSDB_OK){ fprintf(stderr,"open failed\n"); return 1; }
        tsdb_result_t *r=NULL;
        tsdb_query(db,"CREATE STABLE cs (ts TIMESTAMP, val FLOAT64) TAGS (loc SYMBOL)",&r); if(r){tsdb_result_free(r);r=NULL;}
        tsdb_query(db,"CREATE TABLE c0 USING cs TAGS ('east')",&r); if(r){tsdb_result_free(r);r=NULL;}
        tsdb_table_t *tbl=NULL; if(tsdb_open_table(db,"c0",&tbl)!=TSDB_OK){ fprintf(stderr,"open c0 failed\n"); return 1; }
        tsdb_batch_t *b=NULL; tsdb_batch_begin(tbl,&b);
        for(int i=0;i<NROWS;i++){ tsdb_batch_row_ts(b,(tsdb_ts_t)(1000000000000LL+(int64_t)i*1000000LL));
            tsdb_batch_row_f64(b,1,(double)i); tsdb_batch_row_end(b); }
        tsdb_batch_commit(b);
        baseline=q_count(db,"SELECT count(*) FROM c0");
        CHECK(baseline==NROWS,"baseline count == NROWS");
        tsdb_close(db);
    }

    /* Phase 2: fabricate an interrupted swap.
     * Good data staged in .tmp (a copy of the intact live files), live files
     * then torn (truncated), journal names the columns. This is exactly the
     * on-disk state a crash after "journal fsynced, renames not yet done"
     * leaves. */
    snprintf(table_dir,sizeof(table_dir),"%s/c0",dir);
    if(!find_part_dir(table_dir,part,sizeof(part))){ fprintf(stderr,"FAIL: no partition (flushed?)\n");
        printf("\n=== RESULTS: %d passed, %d failed ===\n",g_pass,++g_fail); return 1; }
    {
        const char *cols[2]={"ts","val"}; char live[4096],tmp[4096];
        for(int k=0;k<2;k++){
            for(int e=0;e<2;e++){ const char *ext=e?"idx":"col";
                snprintf(live,sizeof(live),"%s/%s.%s",part,cols[k],ext);
                snprintf(tmp,sizeof(tmp),"%s/%s.%s.tmp",part,cols[k],ext);
                cp(live,tmp);                 /* stage good data in .tmp */
                if(!e) (void)truncate(live,64); /* tear the live .col */
            }
        }
        char marker[4096]; snprintf(marker,sizeof(marker),"%s/.compact_swap",part);
        FILE *mf=fopen(marker,"w"); if(mf){ fprintf(mf,"ts\nval\n"); fclose(mf); }
        CHECK(exists(marker),"journal present before reopen");
    }

    /* Phase 3: reopen → tsdb_part_open recovery rolls the swap forward. */
    {
        tsdb_db_t *db=NULL; if(tsdb_open(dir,&db)!=TSDB_OK){ fprintf(stderr,"reopen failed\n"); return 1; }
        int64_t c=q_count(db,"SELECT count(*) FROM c0");
        printf("  recovered count = %lld (baseline %lld)\n",(long long)c,(long long)baseline);
        CHECK(c==NROWS,"interrupted swap rolled forward → full row set recovered");
        tsdb_close(db);
    }

    /* Phase 4: journal + .tmp files retired. */
    {
        char p[4096];
        snprintf(p,sizeof(p),"%s/.compact_swap",part); CHECK(!exists(p),"journal removed after recovery");
        snprintf(p,sizeof(p),"%s/ts.col.tmp",part);     CHECK(!exists(p),"ts.col.tmp consumed");
        snprintf(p,sizeof(p),"%s/val.col.tmp",part);    CHECK(!exists(p),"val.col.tmp consumed");
    }

    rmrf(dir);
    printf("\n=== RESULTS: %d passed, %d failed ===\n",g_pass,g_fail);
    return g_fail>0?1:0;
}
