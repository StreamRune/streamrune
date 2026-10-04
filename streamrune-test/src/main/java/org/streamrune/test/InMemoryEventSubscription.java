package org.streamrune.test;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.streamrune.core.EventEnvelope;
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
 * deterministic for unit tests: when {@code append} returns, the listener has run. Catch-up on
 * {@link #start()} mirrors the production poll-after-LISTEN: events appended before the
 * subscription started are delivered immediately.
 *
 * <p>Thread-safe; concurrent appends deliver batches in strict offset order (delivery is serialized
 * on an internal lock).
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

  private final Object deliveryLock = new Object();
  private long lastDeliveredOffset; // guarded by deliveryLock

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
    deliverPending();
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
    deliverPending();
  }

  /** Returns the highest global offset acknowledged by the listener so far. */
  public long lastDeliveredOffset() {
    synchronized (deliveryLock) {
      return lastDeliveredOffset;
    }
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
    deliverPending();
  }

  private void deliverPending() {
    synchronized (deliveryLock) {
      while (true) {
        List<EventEnvelope> batch =
            eventStore.readGlobalStream(GlobalOffset.of(lastDeliveredOffset), batchSize);
        if (batch.isEmpty()) {
          return;
        }
        try {
          listener.onEvents(batch);
        } catch (RuntimeException _) {
          // EventListener contract: checkpoint NOT advanced; the same batch is redelivered on
          // the next notification or poll().
          deliveryFailures.incrementAndGet();
          return;
        }
        lastDeliveredOffset = batch.getLast().globalOffset().value();
      }
    }
  }
}
