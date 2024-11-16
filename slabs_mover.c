/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include "slabs_mover.h"
#include "slab_automove.h"
#ifdef EXTSTORE
#include "slab_automove_extstore.h"
#endif
#include "storage.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

struct slab_rebalance {
    void *slab_start;
    void *slab_end;
    void *slab_pos;
    unsigned int s_clsid;
    unsigned int d_clsid;
    uint32_t cls_size;
    uint32_t busy_items;
    uint32_t rescues;
    uint32_t inline_reclaim;
    uint32_t chunk_rescues;
    uint32_t busy_nomem;
    uint32_t busy_deletes;
    uint32_t busy_loops;
    uint8_t done;
    uint8_t *completed;
};

struct slab_rebal_thread {
    bool run_thread;
    bool mem_full; // all memory alloced and global pool is 0 as of start.
    void *storage; // extstore instance.
    item *new_it; // memory for swapping out valid items.
    // TODO: logger instance.
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_t tid;
    logger *l;
    unsigned int am_version; // re-generate am object if version changes
    struct timespec am_last; // last time automover algo ran
    slab_automove_reg_t *sam; // active algorithm module
    void *active_am; // automover memory
    struct slab_rebalance rebal;
};

enum move_status {
    MOVE_PASS=0, MOVE_FROM_SLAB, MOVE_FROM_LRU, MOVE_BUSY,
    MOVE_BUSY_UPLOADING, MOVE_BUSY_ACTIVE, MOVE_BUSY_FLOATING, MOVE_LOCKED
};

static slab_automove_reg_t slab_automove_default = {
    .init = slab_automove_init,
    .free = slab_automove_free,
    .run = slab_automove_run
};
#ifdef EXTSTORE
static slab_automove_reg_t slab_automove_extstore = {
    .init = slab_automove_extstore_init,
    .free = slab_automove_extstore_free,
    .run = slab_automove_extstore_run
};
#endif

// FIXME: delete if unused
#define SLAB_MOVE_MAX_LOOPS 1000
static enum reassign_result_type do_slabs_reassign(struct slab_rebal_thread *t, int src, int dst);

static int slab_rebalance_start(struct slab_rebal_thread *t) {
    uint32_t size;
    uint32_t perslab;
    bool mem_limit_reached;

    if (t->rebal.s_clsid == t->rebal.d_clsid) {
        return -1;
    }

    // check once at the start of a page move if the global pool is full.
    // since we're the only thing that can put memory back into the global
    // pool, this can't change until we complete.
    // unless the user changes the memory limit manually, which should be
    // rare.
    unsigned int global = global_page_pool_size(&mem_limit_reached);
    if (mem_limit_reached && global == 0) {
        t->mem_full = true;
    } else {
        t->mem_full = false;
    }

    void *page = slabs_peek_page(t->rebal.s_clsid, &size, &perslab);

    // Bit-vector to keep track of completed chunks
    t->rebal.completed = (uint8_t*)calloc(perslab,sizeof(uint8_t));
    if (!t->rebal.completed) {
        return -1;
    }

    /* Always kill the first available slab page as it is most likely to
     * contain the oldest items
     */
    t->rebal.slab_start = t->rebal.slab_pos = page;
    t->rebal.slab_end   = (char *)page + (size * perslab);
    t->rebal.done       = 0;
    t->rebal.cls_size   = size;
    // Don't need to do chunk move work if page is in global pool.
    if (t->rebal.s_clsid == SLAB_GLOBAL_PAGE_POOL) {
        t->rebal.done = 1;
    }

    // FIXME: remove this. query the structure from outside to see if we're
    // running.
    STATS_LOCK();
    stats_state.slab_reassign_running = true;
    STATS_UNLOCK();

    return 0;
}

static void *slab_rebalance_alloc(struct slab_rebal_thread *t, unsigned int id) {
    item *new_it = NULL;

    // We will either wipe the whole page if unused, or run out of memory in
    // the page and return NULL.
    while (1) {
        new_it = slabs_alloc(id, SLABS_ALLOC_NO_NEWPAGE);
        if (new_it == NULL) {
            break;
        }
        /* check that memory isn't within the range to clear */
        if ((void *)new_it >= t->rebal.slab_start
            && (void *)new_it < t->rebal.slab_end) {
            /* Pulled something we intend to free. Mark it as freed since
             * we've already done the work of unlinking it from the freelist.
             */
            new_it->refcount = 0;
            new_it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
#ifdef DEBUG_SLAB_MOVER
            memcpy(ITEM_key(new_it), "deadbeef", 8);
#endif
            new_it = NULL;
            t->rebal.inline_reclaim++;
        } else {
            break;
        }
    }
    return new_it;
}

// To call move, we first need a free chunk of memory.
// TODO: flag to indicate if the page mover was a manual request.
// If so, always do an eviction to move forward.
// - use t->mem_full (rename it?) unless mem_full gets used elsewhere.
static void slab_rebalance_prep(struct slab_rebal_thread *t) {
    unsigned int s_clsid = t->rebal.s_clsid;
    if (t->new_it) {
        // move didn't use the memory from the last loop.
        return;
    }

    t->new_it = slab_rebalance_alloc(t, s_clsid);
    // we could free the entire page in the above alloc call, but not get any
    // other memory to work with.
    // We try to busy-loop the page mover at least a few times in this case,
    // so it will pick up on all of the memory being freed already.
    if (t->new_it == NULL && t->mem_full) {
        // global is empty and memory limit is reached. we have to evict
        // memory to move forward.
        for (int x = 0; x < 10; x++) {
            if (lru_pull_tail(s_clsid, COLD_LRU, 0, LRU_PULL_EVICT, 0, NULL) <= 0) {
                if (settings.lru_segmented) {
                    lru_pull_tail(s_clsid, HOT_LRU, 0, 0, 0, NULL);
                }
            }
            t->new_it = slab_rebalance_alloc(t, s_clsid);
            if (t->new_it != NULL) {
                break;
            }
        }
    }
}

/* refcount == 0 is safe since nobody can incr while item_lock is held.
 * refcount != 0 is impossible since flags/etc can be modified in other
 * threads. instead, note we found a busy one and bail.
 * NOTE: This is checking it_flags outside of an item lock. I believe this
 * works since it_flags is 8 bits, and we're only ever comparing a single bit
 * regardless. ITEM_SLABBED bit will always be correct since we're holding the
 * lock which modifies that bit. ITEM_LINKED won't exist if we're between an
 * item having ITEM_SLABBED removed, and the key hasn't been added to the item
 * yet. The memory barrier from the slabs lock should order the key write and the
 * flags to the item?
 * If ITEM_LINKED did exist and was just removed, but we still see it, that's
 * still safe since it will have a valid key, which we then lock, and then
 * recheck everything.
 * This may not be safe on all platforms; If not, slabs_alloc() will need to
 * seed the item key while holding slabs_lock.
 */

struct _locked_st {
    item *it;
    item_chunk *ch;
    void *hold_lock; // held lock from trylock.
    uint32_t hv;
    unsigned int s_clsid;
    unsigned int d_clsid;
};

// called while slabs lock is held so we can safely inspect a chunk of memory
// and do an inverted item lock.
static int _slabs_locked_cb(void *arg) {
    struct _locked_st *a = arg;
    int status = MOVE_PASS;
    item *it = a->it;

    if (it->it_flags & ITEM_CHUNK) {
        /* This chunk is a chained part of a larger item. */
        a->ch = (item_chunk *) it;
        /* Instead, we use the head chunk to find the item and effectively
         * lock the entire structure. If a chunk has ITEM_CHUNK flag, its
         * head cannot be slabbed, so the normal routine is safe. */
        it = a->ch->head;
        assert(it->it_flags & ITEM_CHUNKED);
    }

    /* ITEM_FETCHED when ITEM_SLABBED is overloaded to mean we've cleared
     * the chunk for move. Only these two flags should exist.
     */
    // TODO: bad failure mode if MOVE_PASS and we decide to skip later
    // but the item is actually alive for whatever reason.
    // default to MOVE_BUSY and set MOVE_PASS explicitly if the item is S|F?
    if (it->it_flags != (ITEM_SLABBED|ITEM_FETCHED)) {
        int refcount = 0;

        /* ITEM_SLABBED can only be added/removed under the slabs_lock */
        if (it->it_flags & ITEM_SLABBED) {
            assert(a->ch == NULL);
            status = MOVE_FROM_SLAB;
        } else if ((it->it_flags & ITEM_LINKED) != 0) {
            /* If it doesn't have ITEM_SLABBED, the item could be in any
             * state on its way to being freed or written to. If no
             * ITEM_SLABBED, but it's had ITEM_LINKED, it must be active
             * and have the key written to it already.
             */
            a->hv = hash(ITEM_key(it), it->nkey);
            if ((a->hold_lock = item_trylock(a->hv)) == NULL) {
                status = MOVE_LOCKED;
            } else {
                bool is_linked = (it->it_flags & ITEM_LINKED);
                refcount = refcount_incr(it);
                if (refcount == 2) { /* item is linked but not busy */
                    /* Double check ITEM_LINKED flag here, since we're
                     * past a memory barrier from the mutex. */
                    if (is_linked) {
                        status = MOVE_FROM_LRU;
                    } else {
                        /* refcount == 1 + !ITEM_LINKED means the item is being
                         * uploaded to, or was just unlinked but hasn't been freed
                         * yet. Let it bleed off on its own and try again later */
                        status = MOVE_BUSY_UPLOADING;
                    }
                } else if (refcount > 2 && is_linked) {
                    status = MOVE_BUSY_ACTIVE;
                } else {
                    if (settings.verbose > 2) {
                        fprintf(stderr, "Slab reassign hit a busy item: refcount: %d (%d -> %d)\n",
                            it->refcount, a->s_clsid, a->d_clsid);
                    }
                    status = MOVE_BUSY;
                }
            }
        } else {
            /* See above comment. No ITEM_SLABBED or ITEM_LINKED. Mark
             * busy and wait for item to complete its upload. */
            status = MOVE_BUSY_FLOATING;
        }
    }

    return status;
}

static void slab_rebalance_rescue(struct slab_rebal_thread *t, struct _locked_st *a) {
    int cls_size = t->rebal.cls_size;
    item *it = a->it;
    item_chunk *ch = a->ch;
    item *new_it = t->new_it;

    if (ch == NULL) {
        assert((new_it->it_flags & ITEM_CHUNKED) == 0);
        /* if free memory, memcpy. clear prev/next/h_bucket */
        memcpy(new_it, it, cls_size);
        new_it->prev = 0;
        new_it->next = 0;
        new_it->h_next = 0;
        /* These are definitely required. else fails assert */
        new_it->it_flags &= ~ITEM_LINKED;
        new_it->refcount = 0;
        do_item_replace(it, new_it, a->hv, ITEM_get_cas(it));
        /* Need to walk the chunks and repoint head  */
        if (new_it->it_flags & ITEM_CHUNKED) {
            item_chunk *fch = (item_chunk *) ITEM_schunk(new_it);
            fch->next->prev = fch;
            while (fch) {
                fch->head = new_it;
                fch = fch->next;
            }
        }
        it->refcount = 0;
        it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
#ifdef DEBUG_SLAB_MOVER
        memcpy(ITEM_key(it), "deadbeef", 8);
#endif
        t->rebal.rescues++;
    } else {
        item_chunk *nch = (item_chunk *) new_it;
        /* Chunks always have head chunk (the main it) */
        ch->prev->next = nch;
        if (ch->next)
            ch->next->prev = nch;
        memcpy(nch, ch, ch->used + sizeof(item_chunk));
        ch->refcount = 0;
        ch->it_flags = ITEM_SLABBED|ITEM_FETCHED;
        t->rebal.chunk_rescues++;
#ifdef DEBUG_SLAB_MOVER
        memcpy(ITEM_key((item *)ch), "deadbeef", 8);
#endif
        refcount_decr(it);
    }

    // we've used the temporary memory.
    t->new_it = NULL;
}

// try to free up a chunk of memory, if not already free.
// if we have memory available outside of the source page, rescue any valid
// items.
// we still attempt to move data even if no memory is available for a rescue,
// in case the item is already free, expired, busy, etc.
static int slab_rebalance_move(struct slab_rebal_thread *t) {
    int was_busy = 0;
    struct _locked_st cbarg;
    memset(&cbarg, 0, sizeof(cbarg));

    // the offset to check if completed or not
    int offset = ((char*)t->rebal.slab_pos-(char*)t->rebal.slab_start)/(t->rebal.cls_size);

    // skip acquiring the slabs lock for items we've already fully processed.
    if (t->rebal.completed[offset] == 0) {
        item *it;
        cbarg.it = it = t->rebal.slab_pos;
        cbarg.s_clsid = t->rebal.s_clsid;
        cbarg.d_clsid = t->rebal.d_clsid;
        // it is returned _locked_ if successful. _must_ unlock it!
        int status = slabs_locked_callback(_slabs_locked_cb, &cbarg);

        item_chunk *ch = cbarg.ch;
        switch (status) {
            case MOVE_FROM_LRU:
                /* Lock order is LRU locks -> slabs_lock. unlink uses LRU lock.
                 * We only need to hold the slabs_lock while initially looking
                 * at an item, and at this point we have an exclusive refcount
                 * (2) + the item is locked. Drop slabs lock, drop item to
                 * refcount 1 (just our own, then fall through and wipe it
                 */
                /* Check if expired or flushed */
                if ((it->exptime != 0 && it->exptime < current_time)
                    || item_is_flushed(it)) {
                    /* Expired, don't save. */
                    /* unlink and mark as done if it's not
                     * a chunked item as they require more book-keeping) */
                    STORAGE_delete(t->storage, it);
                    if (!ch && (it->it_flags & ITEM_CHUNKED) == 0) {
                        do_item_unlink(it, cbarg.hv);
                        it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
                        it->refcount = 0;
#ifdef DEBUG_SLAB_MOVER
                        memcpy(ITEM_key(it), "deadbeef", 8);
#endif
                        t->rebal.completed[offset] = 1;
                    } else {
                        do_item_unlink(it, cbarg.hv);
                        slabs_free(it, t->rebal.s_clsid);
                        /* Swing around again later to remove it from the freelist. */
                        t->rebal.busy_items++;
                        was_busy++;
                    }
                } else {
                    // we should try to rescue the item.
                    if (t->new_it == NULL) {
                        // we don't actually have memory: need to mark as busy
                        // and try again in a future loop.
                        t->rebal.busy_items++;
                        t->rebal.busy_nomem++;
                        was_busy++;
                        refcount_decr(it);
                    } else {
                        slab_rebalance_rescue(t, &cbarg);
                        t->rebal.completed[offset] = 1;
                    }
                }

                item_trylock_unlock(cbarg.hold_lock);
                break;
            case MOVE_FROM_SLAB:
                slabs_unlink_free_chunk(t->rebal.s_clsid, it);
                it->refcount = 0;
                it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
#ifdef DEBUG_SLAB_MOVER
                memcpy(ITEM_key(it), "deadbeef", 8);
#endif
                t->rebal.completed[offset] = 1;
                break;
            case MOVE_BUSY:
            case MOVE_BUSY_UPLOADING:
            case MOVE_BUSY_ACTIVE:
                // TODO: for active, replace in place?
                // for not chunked, same logic as MOVE_FROM_LRU
                // double check with store_item() for overwriting SET's
                // for chunked... more difficult.
                // SEE: _store_item_copy_chunks|data
                // from append/prepend/etc code. reusable maybe?
                // could implement that later as well.
                // it might be better in the short term to abandon slabs which
                // are stuck busy without progress, and re-sort the slab page
                // list before trying again.
                refcount_decr(it);
                item_trylock_unlock(cbarg.hold_lock);
                break;
            case MOVE_LOCKED:
            case MOVE_BUSY_FLOATING:
                t->rebal.busy_items++;
                was_busy++;
                break;
            case MOVE_PASS:
                // already freed and unlinked, probably during an alloc
                t->rebal.completed[offset] = 1;
                break;
        }

    }

    t->rebal.slab_pos = (char *)t->rebal.slab_pos + t->rebal.cls_size;

    if (t->rebal.slab_pos >= t->rebal.slab_end) {
        /* Some items were busy, start again from the top */
        if (t->rebal.busy_items) {
            t->rebal.slab_pos = t->rebal.slab_start;
            STATS_LOCK();
            stats.slab_reassign_busy_items += t->rebal.busy_items;
            STATS_UNLOCK();
            t->rebal.busy_items = 0;
            t->rebal.busy_loops++;
        } else {
            t->rebal.done++;
        }
    }

    return was_busy;
}

static void slab_rebalance_finish(struct slab_rebal_thread *t) {
#ifdef DEBUG_SLAB_MOVER
    /* If the algorithm is broken, live items can sneak in. */
    slab_rebal.slab_pos = slab_rebal.slab_start;
    while (1) {
        item *it = slab_rebal.slab_pos;
        assert(it->it_flags == (ITEM_SLABBED|ITEM_FETCHED));
        assert(memcmp(ITEM_key(it), "deadbeef", 8) == 0);
        it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
        slab_rebal.slab_pos = (char *)slab_rebal.slab_pos + slab_rebal.cls_size;
        if (slab_rebal.slab_pos >= slab_rebal.slab_end)
            break;
    }
#endif

    // release any temporary memory we didn't end up using.
    if (t->new_it) {
        slabs_free(t->new_it, t->rebal.s_clsid);
        t->new_it = NULL;
    }

    /* At this point the stolen slab is completely clear.
     * We always kill the "first"/"oldest" slab page in the slab_list, so
     * shuffle the page list backwards and decrement.
     */
    slabs_finalize_page_move(t->rebal.s_clsid, t->rebal.d_clsid,
            t->rebal.slab_start);

    STATS_LOCK();
    stats.slabs_moved++;
    stats.slab_reassign_rescues += t->rebal.rescues;
    stats.slab_reassign_inline_reclaim += t->rebal.inline_reclaim;
    stats.slab_reassign_chunk_rescues += t->rebal.chunk_rescues;
    stats.slab_reassign_busy_deletes += t->rebal.busy_deletes;
    // TODO: busy_nomem
    stats_state.slab_reassign_running = false;
    STATS_UNLOCK();

    free(t->rebal.completed);
    memset(&t->rebal, 0, sizeof(t->rebal));
}

static int slab_rebalance_check_automove(struct slab_rebal_thread *t,
        struct timespec *now) {
    int src, dst;
    if (settings.slab_automove == 0) {
        // not enabled
        return 0;
    }

    if (t->am_last.tv_sec == now->tv_sec) {
        // run once per second-ish.
        return 0;
    }

    if (settings.slab_automove_version != t->am_version) {
        void *am_new = t->sam->init(&settings);
        // only replace if we successfully re-init'ed
        if (am_new) {
            t->sam->free(t->active_am);
            t->active_am = am_new;
        }
        t->am_version = settings.slab_automove_version;
    }

    t->sam->run(t->active_am, &src, &dst);
    if (src != -1 && dst != -1) {
        // rebalancer lock already held, call directly.
        do_slabs_reassign(t, src, dst);
        // TODO: log the _result_ of the do_slabs_reassign call as well?
        LOGGER_LOG(t->l, LOG_SYSEVENTS, LOGGER_SLAB_MOVE, NULL, src, dst);
        if (dst != 0) {
            // if not reclaiming to global, rate limit to one per second.
            t->am_last.tv_sec = now->tv_sec;
        }
        // run the thread since we're moving a page.
        return 1;
    }

    return 0;
}

/* Slab mover thread.
 * Sits waiting for a condition to jump off and shovel some memory about
 */
// TODO: add back the "max busy loops" and bail the page move
static void *slab_rebalance_thread(void *arg) {
    struct slab_rebal_thread *t = arg;
    struct slab_rebalance *r = &t->rebal;
    int was_busy = 0;
    int backoff_timer = 1;
    int backoff_max = 1000;
    // create logger in thread for setspecific
    t->l = logger_create();
    /* Go into cond_wait with the mutex held */
    mutex_lock(&t->lock);

    /* Must finish moving page before stopping */
    while (t->run_thread) {
        // are we running a rebalance?
        if (r->s_clsid != 0 || r->d_clsid != 0) {
            // do we need to kick it off?
            if (r->slab_start == NULL) {
                if (slab_rebalance_start(t) < 0) {
                    // TODO: logger
                    r->s_clsid = 0;
                    r->d_clsid = 0;
                    continue;
                }
            }

            if (r->done) {
                slab_rebalance_finish(t);
            } else {
                // attempt to get some prepared memory
                slab_rebalance_prep(t);
                // attempt to free up memory in a page
                was_busy = slab_rebalance_move(t);
                if (was_busy) {
                    /* Stuck waiting for some items to unlock, so slow down a bit
                     * to give them a chance to free up */
                    usleep(backoff_timer);
                    backoff_timer = backoff_timer * 2;
                    if (backoff_timer > backoff_max)
                        backoff_timer = backoff_max;
                }
            }
        } else {
            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);
            if (slab_rebalance_check_automove(t, &now) == 0) {
                // standard delay
                now.tv_sec++;
                // wait for signal to start another move.
                pthread_cond_timedwait(&t->cond, &t->lock, &now);
            } // else don't wait, run again immediately.
        }
    }

    // TODO: cancel in-flight slab page move
    mutex_unlock(&t->lock);
    return NULL;
}

static enum reassign_result_type do_slabs_reassign(struct slab_rebal_thread *t, int src, int dst) {
    if (src == dst)
        return REASSIGN_SRC_DST_SAME;

    /* Special indicator to choose ourselves. */
    if (src == -1) {
        src = slabs_pick_any_for_reassign(dst);
        /* TODO: If we end up back at -1, return a new error type */
    }

    if (src < SLAB_GLOBAL_PAGE_POOL || src > MAX_NUMBER_OF_SLAB_CLASSES||
        dst < SLAB_GLOBAL_PAGE_POOL || dst > MAX_NUMBER_OF_SLAB_CLASSES)
        return REASSIGN_BADCLASS;

    if (slabs_page_count(src) < 2) {
        return REASSIGN_NOSPARE;
    }

    t->rebal.s_clsid = src;
    t->rebal.d_clsid = dst;

    pthread_cond_signal(&t->cond);

    return REASSIGN_OK;
}

enum reassign_result_type slabs_reassign(struct slab_rebal_thread *t, int src, int dst) {
    enum reassign_result_type ret;
    if (pthread_mutex_trylock(&t->lock) != 0) {
        return REASSIGN_RUNNING;
    }
    ret = do_slabs_reassign(t, src, dst);
    pthread_mutex_unlock(&t->lock);
    return ret;
}

/* If we hold this lock, rebalancer can't wake up or move */
void slab_maintenance_pause(struct slab_rebal_thread *t) {
    pthread_mutex_lock(&t->lock);
}

void slab_maintenance_resume(struct slab_rebal_thread *t) {
    pthread_mutex_unlock(&t->lock);
}

struct slab_rebal_thread *start_slab_maintenance_thread(void *storage) {
    int ret;
    struct slab_rebal_thread *t = calloc(1, sizeof(*t));
    if (t == NULL)
        return NULL;

    pthread_mutex_init(&t->lock, NULL);
    pthread_cond_init(&t->cond, NULL);
    t->run_thread = true;
    if (storage) {
        t->storage = storage;
        t->sam = &slab_automove_extstore;
    } else {
        t->sam = &slab_automove_default;
    }
    t->active_am = t->sam->init(&settings);
    if (t->active_am == NULL) {
        fprintf(stderr, "Can't create slab rebalancer thread: failed to allocate automover memory\n");
        return NULL;
    }

    if ((ret = pthread_create(&t->tid, NULL,
                              slab_rebalance_thread, t)) != 0) {
        fprintf(stderr, "Can't create slab rebalancer thread: %s\n", strerror(ret));
        return NULL;
    }
    thread_setname(t->tid, "mc-slabmaint");
    return t;
}

/* The maintenance thread is on a sleep/loop cycle, so it should join after a
 * short wait */
void stop_slab_maintenance_thread(struct slab_rebal_thread *t) {
    pthread_mutex_lock(&t->lock);
    t->run_thread = false;
    pthread_cond_signal(&t->cond);
    pthread_mutex_unlock(&t->lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(t->tid, NULL);

    pthread_mutex_destroy(&t->lock);
    pthread_cond_destroy(&t->cond);
    if (t->rebal.completed) {
        free(t->rebal.completed);
    }
    t->sam->free(t->active_am);
    free(t);

    // TODO: there is no logger_destroy() yet.
}
