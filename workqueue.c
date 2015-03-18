/*
 * Rated submission helpers
 *
 * Copyright (C) 2015 Jens Axboe <axboe@kernel.dk>
 *
 */
#include <unistd.h>

#include "fio.h"
#include "ioengine.h"
#include "flist.h"
#include "workqueue.h"
#include "lib/getrusage.h"

#define MAX_IDLE_THREADS	  8
#define MIN_IDLE_THREADS	  1

struct submit_worker {
	pthread_t thread;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct flist_head work_list;
	unsigned int flags;
	unsigned int index;
	uint64_t seq;
	struct workqueue *wq;
	struct thread_data *parent;
	struct thread_data td;
};

enum {
	SW_F_IDLE	= 1 << 0,
	SW_F_EXIT	= 1 << 1,
	SW_F_EXITED	= 1 << 2,
};

static struct submit_worker *get_submit_worker(struct workqueue *wq)
{
	unsigned int start, next = wq->next_free_worker;
	struct submit_worker *sw, *best = NULL;

	assert(next < wq->max_workers);

	start = next;
	while (next < wq->max_workers) {
		sw = &wq->workers[next];
		if (sw->flags & SW_F_IDLE)
			goto done;
		if (!best || sw->seq < best->seq)
			best = sw;
		next++;
	}
	if (start) {
		next = 0;
		while (next <= start) {
			sw = &wq->workers[next];
			if (sw->flags & SW_F_IDLE)
				goto done;
			if (!best || sw->seq < best->seq)
				best = sw;
			next++;
		}
	}

	/*
	 * No truly idle found, use best match
	 */
	sw = best;
	next = sw->index;
done:
	if (next == wq->next_free_worker) {
		if (next + 1 < wq->max_workers)
			wq->next_free_worker++;
		else
			wq->next_free_worker = 0;
	}

	return sw;
}

int workqueue_enqueue(struct workqueue *wq, struct io_u *io_u)
{
	struct submit_worker *sw;

	sw = get_submit_worker(wq);
	if (sw) {
		io_u->parent = sw->parent;
		pthread_mutex_lock(&sw->lock);
		flist_add_tail(&io_u->verify_list, &sw->work_list);
		sw->seq = ++wq->work_seq;
		sw->flags &= ~SW_F_IDLE;
		pthread_mutex_unlock(&sw->lock);
		pthread_cond_signal(&sw->cond);
		return FIO_Q_QUEUED;
	}

	return FIO_Q_BUSY;
}

static void handle_list(struct submit_worker *sw, struct flist_head *list)
{
	struct workqueue *wq = sw->wq;
	struct io_u *io_u;

	while (!flist_empty(list)) {
		io_u = flist_first_entry(list, struct io_u, verify_list);
		flist_del_init(&io_u->verify_list);
		wq->fn(&sw->td, io_u);
	}
}

static void init_submit_worker(struct submit_worker *sw)
{
	struct thread_data *td = &sw->td;
	int fio_unused ret;

	memcpy(&td->o, &sw->parent->o, sizeof(td->o));
	memcpy(&td->ts, &sw->parent->ts, sizeof(td->ts));
	td->o.uid = td->o.gid = -1U;
	dup_files(td, sw->parent);
	fio_options_mem_dupe(td);

	td->parent = sw->parent;

	ioengine_load(td);

	if (td->o.odirect)
		td->io_ops->flags |= FIO_RAWIO;

	td->pid = gettid();

	INIT_FLIST_HEAD(&td->io_log_list);
	INIT_FLIST_HEAD(&td->io_hist_list);
	INIT_FLIST_HEAD(&td->verify_list);
	INIT_FLIST_HEAD(&td->trim_list);
	INIT_FLIST_HEAD(&td->next_rand_list);
	td->io_hist_tree = RB_ROOT;

	td->o.iodepth = 1;
	ret = td_io_init(td);

	fio_gettime(&td->epoch, NULL);
	fio_getrusage(&td->ru_start);
	clear_io_state(td);

	td_set_runstate(td, TD_RUNNING);
	td->flags |= TD_F_FAKE;
}

static void *worker_thread(void *data)
{
	struct submit_worker *sw = data;
	struct thread_data *td = &sw->td;
	FLIST_HEAD(local_list);

	init_submit_worker(sw);

	while (1) {
		pthread_mutex_lock(&sw->lock);

		if (flist_empty(&sw->work_list)) {
			if (sw->flags & SW_F_EXIT) {
				pthread_mutex_unlock(&sw->lock);
				break;
			}

			pthread_mutex_unlock(&sw->lock);
			io_u_quiesce(td);
			pthread_mutex_lock(&sw->lock);

			/*
			 * We dropped and reaquired the lock, check
			 * state again.
			 */
			if (!flist_empty(&sw->work_list))
				goto handle_work;
			if (sw->flags & SW_F_EXIT) {
				pthread_mutex_unlock(&sw->lock);
				break;
			}
			if (!(sw->flags & SW_F_IDLE)) {
				sw->flags |= SW_F_IDLE;
				sw->wq->next_free_worker = sw->index;
			}
			pthread_cond_wait(&sw->cond, &sw->lock);
		} else {
handle_work:
			flist_splice_init(&sw->work_list, &local_list);
}

		pthread_mutex_unlock(&sw->lock);
		handle_list(sw, &local_list);
	}

	pthread_mutex_lock(&sw->lock);
	sw->flags |= SW_F_EXITED;
	pthread_mutex_unlock(&sw->lock);
	return NULL;
}

static void free_worker(struct submit_worker *sw)
{
	struct thread_data *td = &sw->td;

	fio_options_free(td);
	close_and_free_files(td);
	close_ioengine(td);
	td_set_runstate(td, TD_EXITED);

	pthread_cond_destroy(&sw->cond);
	pthread_mutex_destroy(&sw->lock);
}

static void exit_worker(struct submit_worker *sw)
{
	pthread_mutex_lock(&sw->lock);
	sw->flags |= SW_F_EXIT;
	pthread_cond_broadcast(&sw->cond);
	pthread_mutex_unlock(&sw->lock);
	pthread_join(sw->thread, NULL);
}

static void shutdown_worker(struct submit_worker *sw, unsigned int *sum_cnt)
{
	exit_worker(sw);
	(*sum_cnt)++;
	sum_thread_stats(&sw->parent->ts, &sw->td.ts, *sum_cnt);
	free_worker(sw);
}

void workqueue_exit(struct workqueue *wq)
{
	unsigned int shutdown, sum_cnt = 0;
	struct submit_worker *sw;
	int i;

	for (i = 0; i < wq->max_workers; i++) {
		sw = &wq->workers[i];
		sw->flags |= SW_F_EXIT;
		pthread_cond_signal(&sw->cond);
	}

	do {

		shutdown = 0;
		for (i = 0; i < wq->max_workers; i++) {
			sw = &wq->workers[i];
			if (sw->flags & SW_F_EXITED)
				continue;
			shutdown_worker(sw, &sum_cnt);
			shutdown++;
		}
	} while (shutdown);
}

static int start_worker(struct workqueue *wq, unsigned int index)
{
	struct submit_worker *sw = &wq->workers[index];
	int ret;

	memset(sw, 0, sizeof(*sw));

	INIT_FLIST_HEAD(&sw->work_list);
	pthread_cond_init(&sw->cond, NULL);
	pthread_mutex_init(&sw->lock, NULL);
	sw->parent = wq->td;
	sw->wq = wq;
	sw->index = index;

	ret = pthread_create(&sw->thread, NULL, worker_thread, sw);
	if (!ret) {
		pthread_mutex_lock(&sw->lock);
		sw->flags = SW_F_IDLE;
		pthread_mutex_unlock(&sw->lock);
		return 0;
	}

	free_worker(sw);
	return 1;
}


int workqueue_init(struct thread_data *td, struct workqueue *wq,
		   workqueue_fn *fn, unsigned max_pending)
{
	int i;

	wq->max_workers = max_pending;
	wq->td = td;
	wq->fn = fn;
	wq->work_seq = 0;
	wq->next_free_worker = 0;

	wq->workers = calloc(wq->max_workers, sizeof(struct submit_worker));

	for (i = 0; i < wq->max_workers; i++) {
		if (start_worker(wq, i))
			break;
	}

	if (i)
		return 0;

	log_err("Can't create rate workqueue\n");
	workqueue_exit(wq);
	return 1;
}

static void sum_and_clear(uint64_t *dst, uint64_t *src)
{
	*dst += *src;
	*src = 0;
}

static void sum_counts(struct thread_data *dst, struct submit_worker *sw)
{
	struct thread_data *src = &sw->td;
	int i;

	for (i = 0; i < DDIR_RWDIR_CNT; i++) {
		sum_and_clear(&dst->io_bytes[i], &src->io_bytes[i]);
		sum_and_clear(&dst->io_blocks[i], &src->io_blocks[i]);
		sum_and_clear(&dst->this_io_blocks[i], &src->this_io_blocks[i]);
		sum_and_clear(&dst->this_io_bytes[i], &src->this_io_bytes[i]);
		sum_and_clear(&dst->bytes_done[i], &src->bytes_done[i]);
	}
}

void workqueue_update_counts(struct workqueue *wq)
{
	struct submit_worker *sw;
	int i;

	for (i = 0; i < wq->max_workers; i++) {
		sw = &wq->workers[i];
		sum_counts(wq->td, sw);
	}
}
