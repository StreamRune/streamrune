package org.streamrune.runtime;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;

/**
 * The slots one {@link SagaRunner} shares between the steps of its pages and the deadline claims of
 * the {@link SagaTimeoutRunner} built from it: a step or a claim runs only while it holds a slot,
 * so the steps and deadline claims of one saga type together never exceed the runner's budgeted
 * steps — its parallelism per bound subscription, the number the connection budget counts ({@link
 * SagaParallelismDefault#stepsInFlightAtMost}).
 *
 * <p><b>The limit is read at every acquire.</b> It is the supplier's value at that moment, at least
 * 1. The runner tells the gate when its parallelism or its bound subscriptions change ({@link
 * #limitChanged()}), so a raised limit lets waiting threads in at once, and a lowered one admits
 * nobody new until enough holders have left. A lowered limit never takes a slot from its holder.
 *
 * <p><b>Arrival order.</b> Waiting threads are let in strictly in the order they asked, steps and
 * claims alike, so a page that keeps its workers busy cannot starve a claim, and a run of claims
 * cannot starve a page.
 *
 * <p><b>Interruptible.</b> A thread interrupted while it waits leaves the queue with {@link
 * InterruptedException} and holds nothing. A thread whose interrupt status is already set takes a
 * free slot that nobody waits for, keeping its status, and fails only where it would have to wait —
 * as a step takes a free {@link SagaStepLock}.
 *
 * <p><b>Reentrant per thread.</b> A thread that already holds a slot of this gate enters again at
 * once, without taking a second slot; the slot is given back when its outermost hold is released.
 * This is what a delivery made on a thread that holds a slot needs: a claim whose compensation
 * command appends an event that a subscription delivers on the appending thread (the test module's
 * in-memory subscription does), or listener plumbing that delivers inside a step. Such a delivery
 * runs its page on the calling thread, one item after the other, under the slot that thread already
 * holds ({@link #heldByCurrentThread()}), instead of handing the items to workers that would queue
 * for slots while the caller holds one and waits for them.
 *
 * <p><b>Lock order.</b> A slot is taken first: before the saga's {@link SagaStepLock} and before
 * any aggregate lock. No thread asks for a slot while it holds a step lock or an aggregate lock;
 * the reentrant entry above takes nothing. A thread that waits for a slot therefore waits for
 * holders that run, or that wait for a lock whose holder is a slot holder too, or a path that takes
 * no slot (a replay feed, an operator's call, a sweeper resume) and never waits for one: no cycle
 * of waits passes through the slots.
 *
 * <p><b>A timed acquire.</b> {@link #tryAcquire} waits in the same queue for at most a bound and
 * gives up holding nothing. The timeout runner's claims use it, bounded by the claim wait: a slot
 * held by a step that does not return then delays a claim by one claim wait, not for ever.
 */
final class SagaSlots {

  private final IntSupplier limit;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private final ArrayDeque<Object> waiting = new ArrayDeque<>();
  private final ThreadLocal<int[]> depth = ThreadLocal.withInitial(() -> new int[1]);

  /** The slots held; guarded by {@link #lock}. */
  private int inUse;

  /**
   * @param limit the most slots held at once, read at every acquire; a value below 1 counts as 1
   */
  SagaSlots(IntSupplier limit) {
    this.limit = Objects.requireNonNull(limit, "limit is required");
  }

  /** A slot the calling thread holds; closing it gives it back, closing it again does nothing. */
  final class Slot implements AutoCloseable {
    private boolean released;

    private Slot() {}

    @Override
    public void close() {
      if (released) {
        return;
      }
      released = true;
      int[] held = depth.get();
      held[0]--;
      if (held[0] > 0) {
        return;
      }
      depth.remove();
      lock.lock();
      try {
        inUse--;
        changed.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Takes a slot, waiting in arrival order while every slot is held. A thread that holds a slot of
   * this gate enters at once.
   *
   * @return the slot; close it on the same thread to give it back
   * @throws InterruptedException when the thread is interrupted while it waits, or has to wait with
   *     its interrupt status set; nothing is held
   */
  Slot acquire() throws InterruptedException {
    return acquire(-1L);
  }

  /**
   * Takes a slot as {@link #acquire()} does, waiting at most {@code timeout} for one.
   *
   * @return the slot, or {@code null} when no slot was free within {@code timeout}; a timed-out
   *     waiter leaves the queue and holds nothing
   * @throws InterruptedException as {@link #acquire()}
   */
  Slot tryAcquire(java.time.Duration timeout) throws InterruptedException {
    return acquire(Math.max(0L, timeout.toNanos()));
  }

  /** The acquire; {@code timeoutNanos} below zero waits without a bound. */
  private Slot acquire(long timeoutNanos) throws InterruptedException {
    int[] held = depth.get();
    if (held[0] > 0) {
      held[0]++;
      return new Slot();
    }
    Object ticket = new Object();
    // Not lockInterruptibly: the gate's own lock is held for a few statements only, and a thread
    // whose interrupt status is set takes a free slot nobody waits for, as a step takes a free
    // step lock. It fails only where it would have to wait (the await below).
    lock.lock();
    try {
      waiting.addLast(ticket);
      long left = timeoutNanos;
      try {
        while (waiting.peekFirst() != ticket || inUse >= currentLimit()) {
          if (timeoutNanos < 0) {
            changed.await();
          } else if (left <= 0) {
            waiting.remove(ticket);
            changed.signalAll(); // the thread behind this one may be first now
            return null;
          } else {
            left = changed.awaitNanos(left);
          }
        }
      } catch (InterruptedException interrupted) {
        waiting.remove(ticket);
        changed.signalAll(); // the thread behind this one may be first now
        throw interrupted;
      }
      waiting.removeFirst();
      inUse++;
      changed.signalAll(); // the next in line may fit as well
    } finally {
      lock.unlock();
    }
    held[0] = 1;
    return new Slot();
  }

  /** Whether the calling thread holds a slot of this gate. */
  boolean heldByCurrentThread() {
    if (depth.get()[0] > 0) {
      return true;
    }
    depth.remove();
    return false;
  }

  /** Wakes the waiting threads to read the limit again; called when it may have changed. */
  void limitChanged() {
    lock.lock();
    try {
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** The limit an acquire made now reads. */
  int currentLimit() {
    return Math.max(1, limit.getAsInt());
  }

  /** How many slots are held at this moment. */
  int inUse() {
    lock.lock();
    try {
      return inUse;
    } finally {
      lock.unlock();
    }
  }

  /** How many threads wait for a slot at this moment. */
  int waiting() {
    lock.lock();
    try {
      return waiting.size();
    } finally {
      lock.unlock();
    }
  }
}
