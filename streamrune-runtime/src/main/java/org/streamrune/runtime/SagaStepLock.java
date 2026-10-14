package org.streamrune.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * The in-process lock of a saga's steps: within one process, a compensation claim that has no event
 * of its own waits for a step in flight on the same saga.
 *
 * <p><b>What it closes.</b> A step reads the saga row, dispatches the forward commands of one event
 * and writes the row; nothing it does between the read and the write is visible on the row. A claim
 * that opens a compensation episode at the version the step read — the timeout runner at the saga's
 * deadline, an operator's compensation of a faulted saga, another delivery whose own command was
 * rejected — therefore succeeds underneath the step and compensates from a state that does not
 * include it. The step's next forward command is then refused at its commit when its runner fences
 * its forward commands ({@link SagaRunner.Builder#forwardFence}, the default), and the step resumes
 * the episode; without the fence it is stored after its compensation with nothing to undo it. With
 * this lock, in one process, exactly one of two things happens: the step reads a row the claim has
 * already written, and resumes the episode without dispatching a forward command; or the claim runs
 * after the step's write, and compensates from a state that includes the step.
 *
 * <p><b>It is a lock of one process.</b> It writes nothing and no other process sees it. A timeout
 * runner on another node, and a delivery on another node, claim underneath a step in flight on this
 * one exactly as they do without it. Across processes the forward fence keeps the order: the event
 * store refuses a forward command of a saga that has left its forward phase, in the transaction
 * that would store the command, so each forward command is stored wholly before such a claim or not
 * at all. With the fence on, this lock keeps a claim from undoing a step it could have waited for
 * and two deliveries of one saga from doing the same work twice; the fence, not this lock, keeps a
 * forward command from following its compensation.
 *
 * <p><b>The rules.</b>
 *
 * <ul>
 *   <li><b>A step holds the lock from before its read of the row to after its write</b> ({@link
 *       #enterStep}): every path that runs forward commands for an event — a live start event, a
 *       live correlated event, a dead-letter replay feed — including the claim and the compensation
 *       a step runs itself after a rejected command, and the resume of an episode by an event. Two
 *       steps of one saga in one process therefore run one after the other, which also keeps a
 *       second delivery of an event from claiming underneath the first.
 *   <li><b>A claim without an event holds the lock around its read of the row and its claim, and
 *       around nothing else</b> ({@link #tryEnterClaim}): the timeout runner's claim and the
 *       operator's compensation of a forward-faulted saga. It is released before the claimed
 *       episode's stamp is read back and before any compensation command is dispatched, so a slow
 *       compensation never keeps a delivery of that saga waiting. A step that arrives while the
 *       claim holds the lock waits for two store statements, then reads a stamped {@code
 *       COMPENSATING} row and resumes the episode, as it does for any claimed row.
 *   <li><b>A claim never waits behind a step without a bound, and never runs underneath one.</b>
 *       While a step holds the lock the claim is not made: the caller is told so and comes back
 *       (the timeout runner on its next poll; the saga is past its deadline and stays selected,
 *       because nothing is written). The claim asks with a try-lock, or with a bounded wait when
 *       the caller says the saga has been passed over before; a claim that waits queues in arrival
 *       order (the lock is fair), so deliveries that keep arriving cannot overtake it.
 *   <li><b>A step waits for the lock for at most {@link #DEFAULT_STEP_WAIT}</b> and then fails with
 *       {@link SagaStepLockException}, a {@link RuntimeException} of its own that {@code
 *       SagaCommandDispatch.isRetryLater} counts as retry-later evidence: the delivery leaves its
 *       listener, nothing was read or dispatched, and the subscription delivers the event again.
 *       The bound is what turns a cycle of waits into a redelivery (see lock order).
 *   <li><b>Reentrant for steps, never for a claim.</b> A step of a saga that starts on a thread
 *       already inside a step of that saga — an event delivered synchronously inside the first
 *       step's command dispatch — enters without waiting, since a thread cannot wait for itself,
 *       and runs as it does without the lock: a claim that nested step makes after a rejected
 *       command lands under the outer step, whose next forward command the fence then refuses. None
 *       of the framework's subscriptions nests a delivery — the production ones deliver on their
 *       own thread, and the test module's in-memory subscription queues an append made inside its
 *       listener — so this is listener plumbing of an application. A claim without an event
 *       attempted on a thread that is inside a step of that saga is refused like any claim under a
 *       step in flight.
 *   <li><b>Lock order.</b> A step takes the lock of its own saga and of no other, and a claim holds
 *       nothing else while it holds one. The framework's subscriptions deliver one event at a time
 *       per listener and release the lock before the next event, so no thread holds two saga locks.
 *       Listener plumbing of an application that delivers a second saga's event inside the first
 *       saga's command dispatch does hold two; a cycle between two such threads ends at the step
 *       bound, as a failed delivery that is redelivered. A step also holds its saga's lock while
 *       the command bus waits for an aggregate's lock. The reverse order — a thread inside a
 *       command, holding the aggregate's lock, that enters a step — exists only where events are
 *       delivered on the appending thread, as the test module's in-memory subscription does. Two
 *       cycles exist there, both bounded. A step that waits for an aggregate's lock held by an
 *       appender whose delivery waits for the step: the bus gives up after its lock timeout on each
 *       attempt of its retry policy (three attempts of 5 s by default) and the step fails
 *       retry-later. A step that appends while another thread's delivery holds that subscription's
 *       delivery monitor and waits for the step: the waiting delivery fails at the step bound, and
 *       the event is then delivered on the appending thread.
 *   <li><b>Released in {@code finally}.</b> A step that ends with an exception or an {@link Error}
 *       releases the lock; a process that dies holds nothing, because the lock is memory. An
 *       interrupt while waiting ends the wait with {@link SagaStepLockException} and leaves the
 *       thread's interrupt status set.
 *   <li><b>A thread whose interrupt status is set</b> enters a step when the lock is free and
 *       nobody waits for it, and keeps its interrupt status; it fails only where it would have to
 *       wait.
 *   <li><b>One entry per saga in flight.</b> Entries are keyed by saga type and saga id, created on
 *       demand and removed when the last thread that holds or waits for one leaves. No two sagas
 *       share a lock, so one saga's step never delays another saga, and a finished saga leaves
 *       nothing behind.
 * </ul>
 *
 * <p>The locks are {@link ReentrantLock}s, not monitors, so a virtual thread that waits for one
 * parks without pinning its carrier.
 *
 * <p><b>Sharing.</b> The lock works between the pieces that hold the same instance. A {@link
 * SagaRunner} creates one and uses it for every delivery, replay feed and operator call that goes
 * through it, which covers the compensation-retry sweeper and the dead-letter replayer built from
 * that runner. A {@link SagaTimeoutRunner} shares it when it is built with {@link
 * SagaTimeoutRunner.Builder#sagaRunner}. An application passes an instance of its own to both
 * builders ({@link SagaRunner.Builder#stepLock}, {@link SagaTimeoutRunner.Builder#stepLock}) only
 * to arrange the sharing itself. A timeout runner names one of the two; its builder refuses to
 * build without. Two {@code SagaRunner}s of one saga type that an application wires by hand have
 * two locks and exclude nothing between them; a saga type has one runner ({@link
 * SagaTypeOwnership}).
 *
 * <p><b>What a step's end does not prove.</b> A forward command whose dispatch fails on the caller
 * after the store accepted its commit — a connection cut inside the commit — ends its step with a
 * retry-later failure and releases the lock, while the commit can still land. A claim made after
 * that step ended can precede that commit; the lock orders steps and claims, not a commit its own
 * caller never saw.
 */
public final class SagaStepLock {

  /** How long a step waits for the lock before its delivery fails and is redelivered. */
  static final Duration DEFAULT_STEP_WAIT = Duration.ofSeconds(30);

  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
  private final Duration stepWait;

  /** Creates a lock whose steps wait for at most 30 seconds. */
  public SagaStepLock() {
    this(DEFAULT_STEP_WAIT);
  }

  /** A lock with a step bound of the caller's choosing; for tests of the bound. */
  SagaStepLock(Duration stepWait) {
    this.stepWait = Objects.requireNonNull(stepWait, "stepWait is required");
  }

  /** The lock of one saga: its type and its id. Saga ids are unique per saga type only. */
  private record Key(SagaType sagaType, SagaId sagaId) {}

  /**
   * The lock of one saga and the number of threads that hold it or wait for it. The count is read
   * and written only inside {@link ConcurrentHashMap#compute}, which is atomic per key.
   */
  private static final class Entry {
    final ReentrantLock lock = new ReentrantLock(true);
    int threads;
  }

  /**
   * What a thread holds between entering and leaving. Closing it releases the lock; closing it a
   * second time does nothing, so a caller that hands it on and also closes it in a {@code finally}
   * releases once.
   */
  final class Hold implements AutoCloseable {
    private final Key key;
    private final Entry entry;
    private boolean released;

    private Hold(Key key, Entry entry) {
      this.key = key;
      this.entry = entry;
    }

    @Override
    public void close() {
      if (released) {
        return;
      }
      entry.lock.unlock();
      released = true;
      leave(key);
    }
  }

  /**
   * Enters a step of the saga: waits until no other thread is inside a step or a claim of it, for
   * at most the step bound. A thread already inside a step of the saga enters at once.
   *
   * @throws SagaStepLockException when the bound passes, or the thread is interrupted while it
   *     waits (its interrupt status stays set); nothing is held
   */
  Hold enterStep(SagaType sagaType, SagaId sagaId) {
    Key key = new Key(sagaType, sagaId);
    Entry entry = arrive(key);
    boolean entered = false;
    try {
      // The timed form tests the interrupt status before it tests the lock. A thread that arrives
      // interrupted takes a lock nobody holds or waits for, and keeps its status for its caller.
      entered =
          Thread.currentThread().isInterrupted()
              && !entry.lock.hasQueuedThreads()
              && entry.lock.tryLock();
      if (!entered) {
        entered = entry.lock.tryLock(stepWait.toNanos(), TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new SagaStepLockException(
          "Interrupted while waiting for the step in flight of saga "
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + "; this delivery read nothing and dispatched nothing",
          interrupted);
    } finally {
      if (!entered) {
        leave(key);
      }
    }
    if (!entered) {
      throw new SagaStepLockException(
          "Saga "
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + " has had a step in flight in this process for more than "
              + stepWait
              + "; this delivery read nothing and dispatched nothing, and is delivered again");
    }
    return new Hold(key, entry);
  }

  /**
   * Enters a claim of the saga if no step of it is in flight in this process.
   *
   * @param wait how long to wait for a step in flight to end; zero asks once and does not wait
   * @return what the claim holds, or {@code null} when a step of the saga is in flight — on another
   *     thread and still there after {@code wait}, or on this very thread — and the claim must not
   *     be made
   * @throws SagaStepLockException when the thread is interrupted while it waits (its interrupt
   *     status stays set); nothing is held
   */
  Hold tryEnterClaim(SagaType sagaType, SagaId sagaId, Duration wait) {
    Key key = new Key(sagaType, sagaId);
    Entry entry = arrive(key);
    boolean entered = false;
    try {
      if (entry.lock.isHeldByCurrentThread()) {
        return null;
      }
      entered =
          wait.isZero() || wait.isNegative()
              ? entry.lock.tryLock()
              : entry.lock.tryLock(wait.toNanos(), TimeUnit.NANOSECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new SagaStepLockException(
          "Interrupted while a compensation claim waited for the step in flight of saga "
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + "; nothing was claimed",
          interrupted);
    } finally {
      if (!entered) {
        leave(key);
      }
    }
    return entered ? new Hold(key, entry) : null;
  }

  /** Counts this thread on the saga's entry, creating the entry for the first one. */
  private Entry arrive(Key key) {
    return entries.compute(
        key,
        (k, existing) -> {
          Entry entry = existing != null ? existing : new Entry();
          entry.threads++;
          return entry;
        });
  }

  /** Takes this thread off the saga's entry and removes the entry with the last one. */
  private void leave(Key key) {
    entries.compute(
        key,
        (k, entry) -> {
          entry.threads--;
          return entry.threads == 0 ? null : entry;
        });
  }

  /** How many sagas have a thread that holds their lock or waits for it at this moment. */
  int sagasInFlight() {
    return entries.size();
  }

  /** How many threads wait for the saga's lock at this moment, the holder not counted. */
  int waitingFor(SagaType sagaType, SagaId sagaId) {
    Entry entry = entries.get(new Key(sagaType, sagaId));
    return entry == null ? 0 : entry.lock.getQueueLength();
  }
}
