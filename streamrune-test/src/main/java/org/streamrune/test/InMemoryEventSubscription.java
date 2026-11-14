package org.streamrune.test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.ScopedValueReads;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.EventSubscription;
import org.streamrune.core.types.GlobalOffset;

/**
 * In-memory {@link EventSubscription} that mimics PostgreSQL {@code LISTEN}/{@code NOTIFY}
 * semantics for unit-testing push-based projection wiring without Docker.
 *
 * <p>Notification semantics mirror {@code pg_notify('streamrune_events', lastOffset)} exactly:
 *
 * <ul>
 *   <li><b>At-most-once</b>: a notification dropped (see {@link #dropNotifications(boolean)}) or
 *       arriving while the subscription is not running is lost forever — never redelivered.
 *   <li><b>Payload is the last offset only</b>: the notification carries no events; the
 *       subscription reads pending events from the store starting at its own checkpoint.
 *   <li><b>Wake-up, not delivery</b>: each notification triggers a catch-up read, so one
 *       notification can deliver events from several appends — and a missed notification's events
 *       are picked up by the next notification or by {@link #poll()}.
 * </ul>
 *
 * <p>{@link #poll()} stands in for the fallback polling cycle of {@code
 * PostgresNotificationSubscription}/{@code HybridEventSubscription}: tests drop notifications,
 * assert nothing was delivered, then call {@code poll()} to verify polling-based recovery.
 *
 * <p>Listener failures follow the {@link EventListener} contract: a throwing listener does not
 * advance the checkpoint, the failure is counted ({@link #deliveryFailureCount()}), and the same
 * batch is redelivered on the next notification or {@link #poll()}.
 *
 * <p>Unlike the production subscriptions, delivery is <b>synchronous on the appending thread</b> —
 * deterministic for unit tests: when {@code append} returns and no delivery is running on another
 * thread, the listener has run. During a delivery on another thread the append returns at once and
 * that delivery delivers the event (see below), on its own thread. Catch-up on {@link #start()}
 * mirrors the production poll-after-LISTEN: events appended before the subscription started are
 * delivered immediately.
 *
 * <p><b>An append from inside the listener starts no nested delivery.</b> When the listener appends
 * (it executes a command, as a saga runner or a process manager does) or calls {@link #poll()}, the
 * delivery in progress on that thread picks the new events up after the batch in hand is
 * acknowledged, and delivers them before the outermost {@code append} returns. Every event is
 * delivered once, in offset order. An append made by another thread while a delivery is in progress
 * does not wait for it: it returns at once, and a delivery already under way delivers its events,
 * in offset order — the one in progress, before the call that started it returns, or, when another
 * thread's append or poll takes the delivery over as it ends, before that call returns. A delivery
 * that fails leaves them pending with its batch. When the listener waits for the threads it handed
 * work to, their events are delivered before the listener's own delivery returns. That work must
 * not call {@link #poll()}, which waits for the delivery the listener is part of and would
 * deadlock, and must not need a lock that the thread running the listener holds, such as the
 * aggregate lock of the command whose append started the delivery: that lock is held until the
 * delivery returns, and the listener waits for the work. {@link #poll()} and {@link #start()} do
 * wait for a delivery in progress, and then deliver everything pending.
 *
 * <p><b>What the listener finds bound.</b> A production subscription calls its listener on a thread
 * of its own, where no request context, saga flag or dispatch cause is bound. This subscription
 * calls the listener on the appending thread (for an event handed to a delivery in progress, on the
 * delivering thread, with that thread's bindings), so it takes those three away for the call:
 * {@link StreamRuneContext#isSagaDispatch()} is {@code false} and {@link StreamRuneContext#cause()}
 * is {@code null}, even when the event was appended by a saga's dispatch. A command the listener
 * executes is therefore not a saga dispatch and its events carry no saga origin, exactly as when
 * the same event reaches the listener through {@link #poll()}. When the thread that calls the
 * listener has nothing bound, nothing is bound for the listener either. A listener that continues
 * an event's flow says so, as it does in production: it dispatches inside {@link
 * StreamRuneContext#continuing(EventEnvelope)}, and the events of that command carry the event's id
 * as their causation id and its saga origin one hop further.
 *
 * <p>Two differences from a production subscription remain, both because the listener shares the
 * calling thread, the appender's or the delivering thread's:
 *
 * <ul>
 *   <li>When the calling thread has a request context bound, the listener runs under an empty one
 *       (a generated correlation id; no trace id, user, baggage or captured authority) instead of
 *       none: {@link StreamRuneContext#capture()} is not {@code null}, and commands the listener
 *       executes directly while handling one batch share that correlation id, where each gets its
 *       own in production.
 *   <li>Everything else the calling thread holds stays visible to the listener: scoped values other
 *       than the three above, thread-locals, and locks. A command the listener executes on the
 *       stream of the command whose append started the delivery re-enters the stream lock that
 *       command still holds, where in production it waits for the lock to be released.
 * </ul>
 *
 * <p>Thread-safe; batches are delivered in strict offset order, one delivery at a time (delivery is
 * serialized on an internal lock).
 */
public final class InMemoryEventSubscription implements EventSubscription {

  private static final int DEFAULT_BATCH_SIZE = 100;

  private final InMemoryEventStore eventStore;
  private final EventListener listener;
  private final int batchSize;
  private final InMemoryEventStore.AppendListener appendListener = this::onNotification;

  private final AtomicBoolean startInvoked = new AtomicBoolean(false);
  private volatile boolean running;
  private volatile boolean droppingNotifications;
  private final AtomicLong droppedNotifications = new AtomicLong();
  private final AtomicLong deliveryFailures = new AtomicLong();

  private final ReentrantLock deliveryLock = new ReentrantLock();
  private final AtomicBoolean deliveryRequested = new AtomicBoolean();
  private volatile long lastDeliveredOffset; // written under deliveryLock
  private volatile Thread deliveringThread; // written under deliveryLock

  /** Creates a subscription delivering batches of at most 100 events. */
  public InMemoryEventSubscription(InMemoryEventStore eventStore, EventListener listener) {
    this(eventStore, listener, DEFAULT_BATCH_SIZE);
  }

  /**
   * Creates a subscription delivering batches of at most {@code batchSize} events.
   *
   * @param eventStore the store to subscribe to (required)
   * @param listener receives event batches per the {@link EventListener} contract (required)
   * @param batchSize maximum events per {@link EventListener#onEvents(List)} call (must be > 0)
   */
  public InMemoryEventSubscription(
      InMemoryEventStore eventStore, EventListener listener, int batchSize) {
    this.eventStore = Objects.requireNonNull(eventStore, "eventStore is required");
    this.listener = Objects.requireNonNull(listener, "listener is required");
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
    }
    this.batchSize = batchSize;
  }

  @Override
  public void start() {
    if (!startInvoked.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "Subscription already started — subscriptions are not restartable, create a new"
              + " instance instead");
    }
    running = true;
    eventStore.addAppendListener(appendListener);
    // Mirror the production poll-after-LISTEN: events appended before start (whose
    // notifications were necessarily missed) are caught up immediately.
    deliverPending(true);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void close() {
    running = false;
    startInvoked.set(true); // start() after close() must throw, even if never started
    eventStore.removeAppendListener(appendListener);
  }

  /**
   * Toggles notification dropping. While {@code true}, every incoming notification is discarded
   * (counted in {@link #droppedNotificationCount()}) and never redelivered — simulating the window
   * where a LISTEN connection is down and {@code pg_notify} wake-ups are lost.
   */
  public void dropNotifications(boolean drop) {
    this.droppingNotifications = drop;
  }

  /**
   * Delivers all pending events now, regardless of notifications — the test's stand-in for the
   * fallback polling cycle that recovers from missed notifications in production.
   *
   * @throws IllegalStateException if the subscription is not running
   */
  public void poll() {
    if (!running) {
      throw new IllegalStateException("Subscription is not running");
    }
    deliverPending(true);
  }

  /** Returns the highest global offset acknowledged by the listener so far. */
  public long lastDeliveredOffset() {
    return lastDeliveredOffset;
  }

  /** Returns how many notifications were discarded while dropping was enabled. */
  public long droppedNotificationCount() {
    return droppedNotifications.get();
  }

  /** Returns how many batch deliveries failed because the listener threw. */
  public long deliveryFailureCount() {
    return deliveryFailures.get();
  }

  private void onNotification(long lastGlobalOffset) {
    // The payload (lastGlobalOffset) is deliberately unused: pg_notify carries only the latest
    // offset, and subscriptions read events from the store starting at their own checkpoint.
    if (!running) {
      return; // not listening — the notification is lost (at-most-once)
    }
    if (droppingNotifications) {
      droppedNotifications.incrementAndGet();
      return; // dropped notifications are never redelivered (at-most-once)
    }
    deliverPending(false);
  }

  private void deliverPending(boolean wait) {
    if (Thread.currentThread() == deliveringThread) {
      // Called from inside the listener (its own append, or a poll): the loop below is already
      // running on this thread and reads the new events once the batch in hand is acknowledged.
      return;
    }
    deliveryRequested.set(true);
    if (wait) {
      deliveryLock.lock();
    } else if (!deliveryLock.tryLock()) {
      return; // the delivery in progress sees the request after it releases the lock
    }
    while (true) {
      boolean failed;
      try {
        deliveryRequested.set(false);
        failed = !drainPending();
      } finally {
        deliveryLock.unlock();
      }
      if (failed) {
        return; // a failed batch waits for the next notification or poll
      }
      if (!deliveryRequested.get() || !deliveryLock.tryLock()) {
        return;
      }
    }
  }

  /** Reads and delivers pages until none is pending; false when the listener threw. */
  private boolean drainPending() {
    deliveringThread = Thread.currentThread();
    try {
      while (true) {
        List<EventEnvelope> batch =
            eventStore.readGlobalStream(GlobalOffset.of(lastDeliveredOffset), batchSize);
        if (batch.isEmpty()) {
          return true;
        }
        try {
          deliver(batch);
        } catch (RuntimeException _) {
          // EventListener contract: checkpoint NOT advanced; the same batch is redelivered on
          // the next notification or poll().
          deliveryFailures.incrementAndGet();
          return false;
        }
        lastDeliveredOffset = batch.getLast().globalOffset().value();
      }
    } finally {
      deliveringThread = null;
    }
  }

  /**
   * Calls the listener outside the request context, the saga flag and the dispatch cause of the
   * thread that calls it, the one that appended, polled or took the delivery over. A {@link
   * ScopedValue} cannot be unbound, so each of the three that is bound on this thread is bound
   * again around the call, to a value that reads as absent.
   */
  private void deliver(List<EventEnvelope> batch) {
    ScopedValue.Carrier detached = null;
    if (ScopedValueReads.isBound(StreamRuneContext.CURRENT)) {
      detached = ScopedValue.where(StreamRuneContext.CURRENT, emptyContext());
    }
    if (ScopedValueReads.isBound(StreamRuneContext.SAGA_OWNED)) {
      detached =
          detached == null
              ? ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.FALSE)
              : detached.where(StreamRuneContext.SAGA_OWNED, Boolean.FALSE);
    }
    if (ScopedValueReads.isBound(StreamRuneContext.CAUSE)) {
      detached =
          detached == null
              ? ScopedValue.where(StreamRuneContext.CAUSE, null)
              : detached.where(StreamRuneContext.CAUSE, null);
    }
    if (detached == null) {
      listener.onEvents(batch);
    } else {
      detached.run(() -> listener.onEvents(batch));
    }
  }

  /** A context that carries nothing of a caller: a correlation id of its own and no more. */
  private static StreamRuneContext.RequestContext emptyContext() {
    return new StreamRuneContext.RequestContext(
        null, null, IdGenerator.generateCorrelationId(), Instant.now(), Map.of());
  }
}
