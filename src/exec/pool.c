/* pool.c — pthread worker-pool implementation.
 *
 * Design: mutex + condition variable queue (simple, correct).
 * Each worker loops: lock, dequeue task, unlock, run task, decrement pending.
 * tsdb_pool_wait() spins on pending==0 cond broadcast.
 */
#include "pool.h"
#include "../../include/tsdb.h"

#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

/* Task queue node (single-linked list). */
typedef struct task_node {
    tsdb_task_fn     fn;
    void            *arg;
    struct task_node *next;
} task_node_t;

struct tsdb_pool {
    /* Queue of pending tasks. */
    task_node_t *head;          /* dequeue from head */
    task_node_t *tail;          /* enqueue at tail   */

    /* Synchronisation. */
    pthread_mutex_t lock;
    pthread_cond_t  work_cond;  /* signaled when task enqueued or shutdown */
    pthread_cond_t  idle_cond;  /* signaled when pending reaches 0 */

    /* Atomic-ish pending counter (protected by lock). */
    int pending;

    /* Shutdown flag. */
    int shutdown;

    /* Worker threads. */
    int         nthreads;
    pthread_t  *threads;
};

/* Worker loop. */
static void *worker_loop(void *arg) {
    tsdb_pool_t *p = (tsdb_pool_t *)arg;

    for (;;) {
        pthread_mutex_lock(&p->lock);

        /* Wait for a task or shutdown. */
        while (!p->shutdown && !p->head)
            pthread_cond_wait(&p->work_cond, &p->lock);

        if (p->shutdown && !p->head) {
            pthread_mutex_unlock(&p->lock);
            return NULL;
        }

        /* Dequeue one task. */
        task_node_t *t = p->head;
        p->head = t->next;
        if (!p->head) p->tail = NULL;

        pthread_mutex_unlock(&p->lock);

        /* Execute task (outside lock for maximum parallelism). */
        t->fn(t->arg);
        free(t);

        /* Decrement pending; signal waiters if we hit zero. */
        pthread_mutex_lock(&p->lock);
        p->pending--;
        if (p->pending == 0)
            pthread_cond_broadcast(&p->idle_cond);
        pthread_mutex_unlock(&p->lock);
    }
    return NULL;
}

int tsdb_pool_new(int nthreads, tsdb_pool_t **out) {
    if (!out) return TSDB_ERR_INVAL;

    /* Auto-detect CPU count when nthreads == 0. */
    if (nthreads <= 0) {
#ifdef _SC_NPROCESSORS_ONLN
        long ncpus = sysconf(_SC_NPROCESSORS_ONLN);
        nthreads = (ncpus > 0) ? (int)ncpus : 1;
#else
        nthreads = 1;
#endif
    }
    /* Cap at a sane maximum. */
    if (nthreads > 64) nthreads = 64;

    tsdb_pool_t *p = calloc(1, sizeof(*p));
    if (!p) return TSDB_ERR_NOMEM;

    p->nthreads = nthreads;
    p->threads  = calloc((size_t)nthreads, sizeof(pthread_t));
    if (!p->threads) { free(p); return TSDB_ERR_NOMEM; }

    pthread_mutex_init(&p->lock, NULL);
    pthread_cond_init(&p->work_cond, NULL);
    pthread_cond_init(&p->idle_cond, NULL);

    for (int i = 0; i < nthreads; i++) {
        if (pthread_create(&p->threads[i], NULL, worker_loop, p) != 0) {
            /* Partial init: shut down what we started. */
            pthread_mutex_lock(&p->lock);
            p->shutdown = 1;
            pthread_cond_broadcast(&p->work_cond);
            pthread_mutex_unlock(&p->lock);
            for (int j = 0; j < i; j++)
                pthread_join(p->threads[j], NULL);
            pthread_mutex_destroy(&p->lock);
            pthread_cond_destroy(&p->work_cond);
            pthread_cond_destroy(&p->idle_cond);
            free(p->threads);
            free(p);
            return TSDB_ERR_INTERNAL;
        }
    }

    *out = p;
    return TSDB_OK;
}

void tsdb_pool_free(tsdb_pool_t *p) {
    if (!p) return;

    /* Wait for all in-flight tasks first. */
    tsdb_pool_wait(p);

    /* Signal shutdown. */
    pthread_mutex_lock(&p->lock);
    p->shutdown = 1;
    pthread_cond_broadcast(&p->work_cond);
    pthread_mutex_unlock(&p->lock);

    for (int i = 0; i < p->nthreads; i++)
        pthread_join(p->threads[i], NULL);

    pthread_mutex_destroy(&p->lock);
    pthread_cond_destroy(&p->work_cond);
    pthread_cond_destroy(&p->idle_cond);
    free(p->threads);
    free(p);
}

int tsdb_pool_submit(tsdb_pool_t *p, tsdb_task_fn fn, void *arg) {
    if (!p || !fn) return TSDB_ERR_INVAL;

    task_node_t *t = malloc(sizeof(*t));
    if (!t) return TSDB_ERR_NOMEM;
    t->fn   = fn;
    t->arg  = arg;
    t->next = NULL;

    pthread_mutex_lock(&p->lock);
    if (p->shutdown) {
        pthread_mutex_unlock(&p->lock);
        free(t);
        return TSDB_ERR_INTERNAL;
    }
    /* Enqueue. */
    if (p->tail) { p->tail->next = t; p->tail = t; }
    else          { p->head = p->tail = t; }
    p->pending++;
    pthread_cond_signal(&p->work_cond);
    pthread_mutex_unlock(&p->lock);

    return TSDB_OK;
}

void tsdb_pool_wait(tsdb_pool_t *p) {
    if (!p) return;
    pthread_mutex_lock(&p->lock);
    while (p->pending > 0)
        pthread_cond_wait(&p->idle_cond, &p->lock);
    pthread_mutex_unlock(&p->lock);
}

int tsdb_pool_size(tsdb_pool_t *p) {
    return p ? p->nthreads : 0;
}
