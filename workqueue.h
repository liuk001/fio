#ifndef FIO_RATE_H
#define FIO_RATE_H

#include "flist.h"

typedef void (workqueue_fn)(struct thread_data *, struct io_u *);

struct workqueue {
	unsigned int max_workers;

	struct thread_data *td;
	workqueue_fn *fn;

	uint64_t work_seq;
	struct submit_worker *workers;
	unsigned int next_free_worker;
};

int workqueue_init(struct thread_data *td, struct workqueue *wq, workqueue_fn *fn, unsigned int max_workers);
void workqueue_exit(struct workqueue *wq);

int workqueue_enqueue(struct workqueue *wq, struct io_u *io_u);
uint64_t workqueue_bytes_done(struct thread_data *td);
uint64_t __workqueue_bytes_done(struct thread_data *td, enum fio_ddir);
void workqueue_update_counts(struct workqueue *wq);

#endif
