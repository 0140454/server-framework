#include "async.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

#define WORK_QUEUE_POWER    16
#define WORK_QUEUE_SIZE     (1 << WORK_QUEUE_POWER)
#define WORK_QUEUE_MASK     (WORK_QUEUE_SIZE - 1)

#define thread_front_val(thread)    (__sync_val_compare_and_swap(&(thread)->front, 0, 0))
#define thread_queue_len(thread)    ((thread)->rear - thread_front_val(thread))
#define thread_queue_empty(thread)  (thread_queue_len(thread) == 0)
#define thread_queue_full(thread)   (thread_queue_len(thread) == WORK_QUEUE_SIZE)
#define queue_offset(val)           ((val) & WORK_QUEUE_MASK)
#define nth_thread(async, i)        (async->threads + i)

/* suppress compilation warnings */
static inline ssize_t write_wrapper(int fd, const void *buf, size_t count)
{
    ssize_t s;
    if ((s = write(fd, buf, count)) < count) perror("write");
    return s;
}
#undef write
#define write write_wrapper

/* the actual working thread */
static void *worker_thread_cycle(void *async);

/* signaling to finish */
static void async_signal(async_p async);

/* the destructor */
static void async_destroy(async_p queue);

static void *join_thread(pthread_t thr)
{
    void *ret;
    pthread_join(thr, &ret);
    return ret;
}

static int create_thread(pthread_t *thr,
                         void *(*thread_func)(void *),
                         void *async)
{
    return pthread_create(thr, NULL, thread_func, async);
}

/** A task node */
struct AsyncTask {
    void (*task)(void *);
    void *arg;
};

/** Thread struct */
struct Thread {
    struct AsyncTask work_queue[WORK_QUEUE_SIZE]; /**< Ring buffer for tasks */

    /** The pipe used for thread wakeup */
    struct {
        int in;  /**< read incoming data (opaque data), used for wakeup */
        int out; /**< write opaque data (single byte),
                      used for wakeup signaling */
    } pipe;

    pthread_t handle; /**< Pthread handle for this thread */
    volatile unsigned int front; /**< The position for next task */
    volatile unsigned int rear; /**< The position for new task */
};

/** The Async struct */
struct Async {
    int count; /**< the number of initialized threads */

    unsigned run : 1; /**< the running flag */

    /** the thread pool */
    struct Thread threads[];
};

/* Task Management - add a task and perform all tasks in queue */

static struct Thread *round_robin_schedule(async_p async) {
    static int cur_thread_index = -1;

	cur_thread_index = __sync_add_and_fetch(&cur_thread_index, 1) % async->count;
	return &async->threads[cur_thread_index];
}

static int dispatch_task(struct Thread *thread, void (*task)(void *), void *arg) {
    struct AsyncTask *c = NULL;

    if (thread_queue_full(thread)) {
        return -1;
    }

    c = thread->work_queue + queue_offset(__sync_fetch_and_add(&(thread->rear), 1));
    c->task = task;
    c->arg = arg;

    /* wakeup any sleeping thread */
    if (thread_queue_len(thread) >= 1) {
        write(thread->pipe.out, c->task, 1);
    }

    return 0;
}

static int async_run(async_p async, void (*task)(void *), void *arg)
{
    struct Thread *thread = round_robin_schedule(async);
	return dispatch_task(thread, task, arg);
}

/** Performs all the existing tasks in the queue. */
static void perform_tasks(struct Thread *thread)
{
    struct AsyncTask *c = NULL;

    while (!thread_queue_empty(thread)) {
        /* grab a task from the queue. */
        c = thread->work_queue + queue_offset(thread->front++);
        /* perform the task */
        if (c->task) c->task(c->arg);
    }
}

/* The worker threads */

/* The worker cycle */
static void *worker_thread_cycle(void *_async)
{
    /* setup signal and thread's local-storage async variable. */
    struct Async *async = _async;
    struct Thread *thread = NULL;
    char sig_buf;

    /* find current thread object */
    for (int i = 0; i < async->count; ++i) {
        if (nth_thread(async, i)->handle == pthread_self()) {
            thread = nth_thread(async, i);
            break;
        }
    }

    /* pause for signal for as long as we're active. */
    while (async->run && (read(thread->pipe.in, &sig_buf, 1) >= 0)) {
        perform_tasks(thread);
        sched_yield();
    }

    perform_tasks(thread);
    return 0;
}

/* Signal and finish */

static void async_signal(async_p async)
{
    async->run = 0;
    /* send `async->count` number of wakeup signales.
     * data content is irrelevant. */
     for (int i = 0; i < async->count; ++i) {
        write(nth_thread(async, i)->pipe.out, async, async->count);
    }
}

static void async_wait(async_p async)
{
    if (!async) return;

    /* wake threads (just in case) by sending `async->count`
     * number of wakeups
     */
    for (int i = 0; i < async->count; ++i) {
        if (nth_thread(async, i)->pipe.out)
            write(nth_thread(async, i)->pipe.out, async, async->count);
    }
    /* join threads */
    for (int i = 0; i < async->count; i++) {
        join_thread(nth_thread(async, i)->handle);
        /* perform any pending tasks */
        perform_tasks(nth_thread(async, i));
    }
    /* release queue memory and resources */
    async_destroy(async);
}

static void async_finish(async_p async)
{
    async_signal(async);
    async_wait(async);
}

/* Object creation and destruction */

/** Destroys the Async object, releasing its memory. */
static void async_destroy(async_p async)
{
    struct Thread *thread = NULL;
    for (int i = 0; i < async->count; ++i) {
        thread = nth_thread(async, i);
        /* close pipe */
        if (thread->pipe.in) {
            close(thread->pipe.in);
            thread->pipe.in = 0;
        }
        if (thread->pipe.out) {
            close(thread->pipe.out);
            thread->pipe.out = 0;
        }
    }
    free(async);
}

static async_p async_create(int threads)
{
    async_p async = malloc(sizeof(*async) + (threads * sizeof(struct Thread)));
    struct Thread *thread = NULL;

    async->run = 1;
    /* create threads */
    for (async->count = 1; async->count <= threads; async->count++) {
        thread = nth_thread(async, async->count - 1);
        /* initialize pipe */
        thread->pipe.in = 0;
        thread->pipe.out = 0;
        if (pipe((int *) &(thread->pipe))) {
            free(async);
            return NULL;
        };
        fcntl(thread->pipe.out, F_SETFL, O_NONBLOCK | O_WRONLY);

        /* initialize work queue */
        thread->front = 0;
        thread->rear = 0;
        memset(thread->work_queue, 0, WORK_QUEUE_SIZE * sizeof(struct AsyncTask));

        /* create thread */
        if (create_thread(&(thread->handle),
                          worker_thread_cycle, async)) {
            /* signal */
            async_signal(async);
            /* wait for threads and destroy object */
            async_wait(async);
            /* return error */
            return NULL;
        };
    }
    async->count--;

    return async;
}

/* API gateway */
struct __ASYNC_API__ Async = {
    .create = async_create,
    .signal = async_signal,
    .wait = async_wait,
    .finish = async_finish,
    .run = async_run,
};
