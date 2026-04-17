/* pool.h — simple pthread worker-pool for parallel query scan.
 *
 * Usage:
 *   tsdb_pool_t *p;
 *   tsdb_pool_new(0, &p);                  // 0 = detect CPU count
 *   tsdb_pool_submit(p, my_fn, my_arg);
 *   tsdb_pool_wait(p);                     // wait until all pending done
 *   tsdb_pool_free(p);
 *
 * All pending tasks share a single barrier; tsdb_pool_wait() blocks until the
 * pending count (incremented by submit, decremented after task fn returns)
 * reaches zero.  Multiple concurrent callers to submit/wait is safe.
 */
#ifndef TSDB_EXEC_POOL_H
#define TSDB_EXEC_POOL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque pool handle. */
typedef struct tsdb_pool tsdb_pool_t;

/* Task function signature. */
typedef void (*tsdb_task_fn)(void *arg);

/*
 * Create a thread pool with nthreads workers.
 * Pass nthreads = 0 to auto-detect (sysconf(_SC_NPROCESSORS_ONLN)).
 * Returns 0 (TSDB_OK) on success, negative on error.
 */
int  tsdb_pool_new(int nthreads, tsdb_pool_t **out);

/* Destroy the pool: drains pending work, then shuts workers down. */
void tsdb_pool_free(tsdb_pool_t *p);

/*
 * Submit a task.  fn(arg) will be called by one worker thread.
 * Returns 0 on success.  Increments the pool's pending counter.
 */
int  tsdb_pool_submit(tsdb_pool_t *p, tsdb_task_fn fn, void *arg);

/*
 * Block until all tasks that were submitted before this call have completed.
 * (Pending counter == 0.)
 */
void tsdb_pool_wait(tsdb_pool_t *p);

/* Return the number of worker threads in the pool. */
int  tsdb_pool_size(tsdb_pool_t *p);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_POOL_H */
