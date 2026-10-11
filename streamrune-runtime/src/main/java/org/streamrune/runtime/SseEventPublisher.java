package org.streamrune.runtime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;

/**
 * Event publisher for Server-Sent Events.
 *
 * <p><b>Who publishes.</b> When {@code streamrune.sse.enabled=true} the Spring, Quarkus and
 * Micronaut integrations run an {@link SseEventFeed} that publishes every event stored from then
 * on, so an application does not call {@link #publish} for the shipped endpoint — a second
 * publisher would deliver every frame twice. An event is always routed by its own {@link
 * EventEnvelope#streamId()}: a subscriber of one stream cannot be handed the event of another.
 *
 * <p>Fan-out is asynchronous and isolated per subscriber. Each subscriber owns a bounded queue and
 * a dedicated virtual-thread delivery worker; {@link #publish} only enqueues (never blocks) and
 * then returns, so a slow or stuck subscriber can never stall delivery to other subscribers nor the
 * publishing caller (typically an event-subscription thread).
 *
 * <p><b>Slow-consumer policy — evict and complete.</b> When a subscriber's queue is full (the
 * consumer cannot keep up), that subscriber is evicted: removed from the stream, its worker
 * stopped, and its disconnect hook invoked with a {@link SlowConsumerException}. We do not drop
 * individual events, because a silent gap in an event-sourced stream would corrupt the client's
 * view; instead the client is completed and expected to reconnect (matching the {@code
 * BackPressureStrategy.ERROR} semantics already used by the Quarkus/Micronaut SSE controllers). A
 * subscriber whose {@link SseSubscriber#send} throws is evicted the same way, with the thrown
 * exception passed to its hook.
 *
 * <p>Eviction unregisters the subscriber from the stream map and signals its worker to stop, and
 * the disconnect hook lets the transport layer (the framework SSE controllers) tear down the
 * underlying connection. <b>What eviction does NOT do is force a parked worker to return
 * immediately.</b> {@code stop()} interrupts the worker, which breaks it out of {@code
 * queue.take()} — but a worker blocked inside a subscriber's {@code send} cannot be interrupted out
 * of a blocking transport write (Spring's servlet {@code SseEmitter.send()} on a TCP zero-window
 * client is the case that matters), so that worker and the envelopes still in its queue stay
 * reachable until the container's own write timeout expires. That is bounded and transient rather
 * than a leak, but it is not instantaneous, and it is why the eviction path exists in the first
 * place: without it the SUBSCRIPTION would stay registered forever and keep being fed.
 *
 * <p><b>Two independent slow-consumer bounds on Quarkus/Micronaut.</b> Those controllers front this
 * publisher with a reactive buffer of their own ({@code onOverflow().buffer(256)} / {@code
 * onBackpressureBuffer(256)}), which absorbs each {@code emit} promptly, so the delivery worker
 * rarely stalls and this publisher's queue rarely fills: for a steadily slow client the REACTIVE
 * bound trips first and terminates the stream with its own failure type, and this publisher's
 * eviction is reached only when the publish rate outruns the worker's drain rate. Consequently a
 * slow client can hold up to {@link #DEFAULT_QUEUE_CAPACITY} envelopes here plus the controller's
 * buffered items — decrypted domain events in both — before either bound fires. Spring has no
 * second buffer, so this queue is its only bound.
 *
 * <p>This class is {@link AutoCloseable}: {@link #close()} stops every worker. The integrations
 * construct it with no arguments; workers are created lazily per subscription.
 */
public class SseEventPublisher implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SseEventPublisher.class);

  /**
   * Default per-subscriber queue capacity. Sized to absorb short bursts while keeping a slow
   * consumer's memory bounded; once exceeded the subscriber is evicted per the slow-consumer
   * policy.
   */
  static final int DEFAULT_QUEUE_CAPACITY = 256;

  private final int queueCapacity;

  private final Map<StreamId, List<Subscription>> subscribers = new ConcurrentHashMap<>();

  /** Creates a publisher with the {@link #DEFAULT_QUEUE_CAPACITY default per-subscriber queue}. */
  public SseEventPublisher() {
    this(DEFAULT_QUEUE_CAPACITY);
  }

  /**
   * Creates a publisher with an explicit per-subscriber queue capacity.
   *
   * @param queueCapacity maximum events buffered per subscriber before the slow-consumer policy
   *     evicts it; must be {@code >= 1}
   */
  public SseEventPublisher(int queueCapacity) {
    if (queueCapacity < 1) {
      throw new IllegalArgumentException("queueCapacity must be >= 1, was " + queueCapacity);
    }
    this.queueCapacity = queueCapacity;
  }

  /**
   * Registers a subscriber for a stream with no disconnect hook.
   *
   * <p><b>Only for subscribers with no transport to tear down.</b> Without a hook an eviction
   * unregisters the subscriber and stops its worker, but nothing tells the transport: an SSE client
   * whose stream was evicted keeps an open, apparently-healthy connection that receives zero
   * further events. Every shipped SSE controller passes a real hook via {@link #subscribe(StreamId,
   * SseSubscriber, Consumer)}; use this overload only when the subscriber is purely in-process.
   *
   * @param streamId the stream to subscribe to
   * @param subscriber the subscriber to deliver events to
   */
  public void subscribe(StreamId streamId, SseSubscriber subscriber) {
    subscribe(streamId, subscriber, ignored -> {});
  }

  /**
   * Registers a subscriber for a stream with a disconnect hook.
   *
   * <p>The hook is invoked exactly once if the subscriber is evicted — because its queue overflowed
   * (a {@link SlowConsumerException}) or its {@link SseSubscriber#send} threw — so the transport
   * layer can complete or fail the underlying connection. It is <em>not</em> invoked for an
   * explicit {@link #unsubscribe} or for {@link #close}.
   *
   * <p><b>Which thread runs the hook, and why it must not block.</b> It depends on the eviction
   * path:
   *
   * <ul>
   *   <li><b>Send failure</b> — on the subscriber's own delivery thread, which is on its way out of
   *       the delivery loop. Its interrupt flag is deliberately left clear so the hook's own
   *       interruptible work (completing an emitter, taking a lock, writing a final frame)
   *       succeeds.
   *   <li><b>Slow consumer</b> — on the PUBLISHING thread (typically a projection or subscription
   *       poll thread), inline in {@link #publish}. It cannot be handed to the delivery worker:
   *       that worker is, by construction, stuck inside the very {@code send} whose blocking filled
   *       the queue, so deferring the hook to it would defer transport teardown for exactly as long
   *       as the stalled client holds the write — the failure this eviction exists to end.
   * </ul>
   *
   * <p>The hook must therefore be non-blocking and prompt: {@link #publish}'s "only enqueues, never
   * blocks" guarantee extends only as far as the hook honours it. Release what the subscriber holds
   * and signal the transport handle without waiting; do not take a lock a stalled writer may hold,
   * and do not perform I/O. A transport whose completion waits for a write in progress (Spring's
   * {@code SseEmitter}) is completed from a thread of the stream's own.
   *
   * @param streamId the stream to subscribe to
   * @param subscriber the subscriber to deliver events to
   * @param onDisconnect invoked once with the cause when the subscriber is evicted; must not block
   */
  public void subscribe(
      StreamId streamId, SseSubscriber subscriber, Consumer<Throwable> onDisconnect) {
    var subscription = new Subscription(streamId, subscriber, onDisconnect, queueCapacity);
    // The add must happen inside compute(): with computeIfAbsent(...).add(...) the add runs
    // outside the map's per-key lock, so a concurrent unsubscribe() can empty the list and
    // remove the mapping in between — the subscriber then lands on an orphaned list and
    // silently never receives events.
    subscribers.compute(
        streamId,
        (k, list) -> {
          if (list == null) {
            list = new CopyOnWriteArrayList<>();
          }
          list.add(subscription);
          return list;
        });
    subscription.start();
  }

  /**
   * Unregisters a subscriber and stops its delivery worker. No-op if the subscriber is not
   * registered. Does <em>not</em> invoke the disconnect hook — this is a clean, caller-initiated
   * removal (e.g. the transport's completion/timeout callback).
   *
   * @param streamId the stream to unsubscribe from
   * @param subscriber the subscriber to remove (matched by identity)
   */
  public void unsubscribe(StreamId streamId, SseSubscriber subscriber) {
    var removed = new AtomicBoolean(false);
    subscribers.compute(
        streamId,
        (k, list) -> {
          if (list == null) {
            return null;
          }
          list.removeIf(
              sub -> {
                if (sub.subscriber == subscriber) {
                  sub.stop();
                  removed.set(true);
                  return true;
                }
                return false;
              });
          return list.isEmpty() ? null : list;
        });
  }

  /**
   * Enqueues the envelope for every subscriber of the envelope's own stream ({@link
   * EventEnvelope#streamId()}) and returns immediately. The stream is not a parameter, so an event
   * cannot be routed to the subscribers of a stream it does not belong to.
   *
   * <p>Delivery happens asynchronously on each subscriber's own worker thread, so a slow or blocked
   * subscriber never delays any other subscriber or this caller. A subscriber whose bounded queue
   * is full is evicted per the {@linkplain SseEventPublisher class-level} slow-consumer policy.
   *
   * @param envelope the event to deliver to the subscribers of its stream
   */
  public void publish(EventEnvelope envelope) {
    StreamId streamId = envelope.streamId();
    var list = subscribers.get(streamId);
    if (list == null) {
      return;
    }
    // CopyOnWriteArrayList iteration is snapshot-based: concurrent (un)subscribes never throw
    // ConcurrentModificationException and don't affect this delivery pass.
    for (var subscription : list) {
      if (!subscription.enqueue(envelope)) {
        log.warn(
            "Evicting slow SSE subscriber for stream {}: queue of {} full",
            LogSanitizer.sanitizeForLog(streamId.value()),
            queueCapacity);
        evict(subscription, new SlowConsumerException(streamId, queueCapacity));
      }
    }
  }

  /**
   * Removes a subscription from its stream, stops its worker, and fires its disconnect hook once.
   * Called for slow-consumer eviction and for delivery failures.
   *
   * <p><b>The hook fires OUTSIDE the {@code computeIfPresent} lambda, deliberately.</b> Every
   * shipped SSE controller's disconnect hook runs the same cleanup the transport's completion
   * callback runs, and that cleanup calls {@link #unsubscribe} — {@code subscribers.compute(...)}
   * on the key this method is already computing. {@link java.util.concurrent.ConcurrentHashMap}
   * explicitly documents that a mapping function must not attempt to update the map, and it would
   * run an arbitrary transport callback while holding the bin lock, blocking every other
   * subscribe/unsubscribe on that stream for its duration. Keeping the hook after the lambda makes
   * both moot. ({@code SseEventPublisherEvictionTest} pins that a re-entrant unsubscribe from the
   * hook stays safe.)
   */
  private void evict(Subscription subscription, Throwable cause) {
    subscribers.computeIfPresent(
        subscription.streamId,
        (k, list) -> {
          list.remove(subscription);
          return list.isEmpty() ? null : list;
        });
    subscription.fail(cause);
  }

  /** The number of subscriptions registered on {@code streamId}; for tests. */
  int subscriberCount(StreamId streamId) {
    var list = subscribers.get(streamId);
    return list == null ? 0 : list.size();
  }

  /** Stops every subscriber's delivery worker. The disconnect hooks are not invoked. */
  @Override
  public void close() {
    subscribers.values().forEach(list -> list.forEach(Subscription::stop));
    subscribers.clear();
  }

  /**
   * One registered subscriber: its bounded queue, its dedicated virtual-thread delivery worker, and
   * its disconnect hook. The worker drains the queue and invokes {@link SseSubscriber#send}; a full
   * queue or a throwing {@code send} triggers eviction via {@link #fail}.
   */
  private final class Subscription {

    private final StreamId streamId;

    /**
     * {@link #streamId} rendered once for the worker's thread name and this subscription's log
     * sites — computed here so every sink shares one sanitized form.
     */
    private final String logSafeStreamId;

    private final SseSubscriber subscriber;
    private final Consumer<Throwable> onDisconnect;
    private final BlockingQueue<EventEnvelope> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);
    private volatile Thread worker;

    Subscription(
        StreamId streamId,
        SseSubscriber subscriber,
        Consumer<Throwable> onDisconnect,
        int capacity) {
      this.streamId = streamId;
      this.logSafeStreamId = LogSanitizer.sanitizeForLog(streamId.value());
      this.subscriber = subscriber;
      this.onDisconnect = onDisconnect;
      this.queue = new ArrayBlockingQueue<>(capacity);
    }

    void start() {
      worker =
          Thread.ofVirtual().name("streamrune-sse-" + logSafeStreamId).start(this::deliverLoop);
    }

    /**
     * Non-blocking enqueue. Returns {@code false} when the queue is full (slow consumer) so the
     * caller can evict; never blocks the publisher.
     */
    boolean enqueue(EventEnvelope envelope) {
      if (!running.get()) {
        return true; // already being reaped; treat as accepted so publish() does not re-evict
      }
      return queue.offer(envelope);
    }

    private void deliverLoop() {
      try {
        while (running.get()) {
          EventEnvelope envelope = queue.take();
          try {
            subscriber.send(envelope);
          } catch (Throwable t) {
            // Throwable, not Exception. An Error out of send (a NoClassDefFoundError or
            // ExceptionInInitializerError rethrown through Jackson inside the transport's send, an
            // OutOfMemoryError) that ended this worker WITHOUT evict() would leave the
            // subscription registered and running, its disconnect hook unfired and the transport's
            // keepalives keeping the connection healthy: the client would never reconnect and
            // receive nothing further (the silent gap). Evict first so the hook tears the transport
            // down, then let an Error propagate: it is not this loop's to swallow.
            // A transport's failure text can carry what the client sent; like every value in these
            // log lines it goes through the sanitizer (CWE-117).
            log.warn(
                "SSE subscriber send failed for stream {}: {}",
                logSafeStreamId,
                LogSanitizer.sanitizeForLog(t.toString()));
            evict(this, t);
            if (t instanceof Error error) {
              throw error;
            }
            return;
          }
        }
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
      }
    }

    /**
     * Stops the worker without firing the disconnect hook (clean removal / shutdown).
     *
     * <p><b>Never interrupts the calling thread.</b> On the send-failure path the worker evicts
     * ITSELF ({@code deliverLoop} → {@code evict} → {@code fail} → {@code stop}), so an
     * unconditional {@code worker.interrupt()} would set the flag on the very thread that then runs
     * the disconnect hook — and the hook's job is interruptible work (completing an emitter,
     * acquiring a lock, writing a final frame). The worker is returning from {@code deliverLoop} on
     * that path anyway; the interrupt exists only to break another thread out of {@code
     * queue.take()}.
     */
    void stop() {
      if (running.compareAndSet(true, false)) {
        Thread t = worker;
        if (t != null && t != Thread.currentThread()) {
          t.interrupt();
        }
      }
    }

    /** Stops the worker and fires the disconnect hook once (eviction). */
    void fail(Throwable cause) {
      stop();
      if (disconnectFired.compareAndSet(false, true)) {
        try {
          onDisconnect.accept(cause);
        } catch (Exception e) {
          log.warn(
              "SSE disconnect hook failed for stream {}: {}",
              logSafeStreamId,
              LogSanitizer.sanitizeForLog(e.toString()));
        }
      }
    }
  }

  /**
   * Receives events for a subscribed stream.
   *
   * <p>Contract: {@link #send} is invoked on the subscriber's own delivery thread, one envelope at
   * a time, and should return promptly — events queue up behind a slow {@code send}, and once the
   * bounded queue fills the subscriber is evicted per the {@linkplain SseEventPublisher
   * class-level} slow-consumer policy. Throwing from {@code send} also evicts the subscriber and
   * invokes its disconnect hook; an {@link Error} does so too and is then rethrown on the delivery
   * thread. A slow {@code send} stalls neither other subscribers nor the publisher.
   */
  @FunctionalInterface
  public interface SseSubscriber {
    void send(EventEnvelope envelope);
  }

  /** Signals that a subscriber was evicted because it could not keep up with its event stream. */
  public static final class SlowConsumerException extends RuntimeException {

    private final transient StreamId streamId;
    private final int queueCapacity;

    SlowConsumerException(StreamId streamId, int queueCapacity) {
      // The MESSAGE is log-safe (this exception is logged by every SSE controller's disconnect
      // hook); streamId() below still carries the exact value the caller supplied, because that is
      // data, not a log line.
      super(
          "SSE subscriber for stream "
              + LogSanitizer.sanitizeForLog(streamId.value())
              + " evicted: queue capacity "
              + queueCapacity
              + " exceeded");
      this.streamId = streamId;
      this.queueCapacity = queueCapacity;
    }

    /** The stream the evicted subscriber was reading. */
    public StreamId streamId() {
      return streamId;
    }

    /** The per-subscriber queue capacity that was exceeded. */
    public int queueCapacity() {
      return queueCapacity;
    }
  }
}
