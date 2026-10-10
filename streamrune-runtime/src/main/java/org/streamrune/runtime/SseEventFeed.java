package org.streamrune.runtime;

import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * Feeds an {@link SseEventPublisher} from the event store: the source of the frames the Server-Sent
 * Events endpoint ({@code GET /api/sse/{aggregateType}/{aggregateId}}) writes. The Spring, Quarkus
 * and Micronaut integrations create, start and close one feed per application when {@code
 * streamrune.sse.enabled=true}.
 *
 * <p>The feed is a {@link PollingEventSubscription} over the global stream. Every event it reads is
 * handed to {@link SseEventPublisher#publish(EventEnvelope)}, which routes it to the subscribers of
 * the event's own stream.
 *
 * <p><b>Live, from the head.</b> {@link #start()} reads {@link EventStore#lastGlobalOffset()} and
 * the feed delivers the events stored after that offset. It never replays history, and its position
 * lives in memory only: no row is written to the offset store, and a restarted feed begins at the
 * head of that moment. Each replica runs its own feed for the clients connected to it.
 *
 * <p><b>Delivery guarantee: best-effort, at-most-once.</b> A frame reaches a client only while the
 * client is connected and the feed is running. Nothing is redelivered: an event appended while the
 * application is down or restarting, while a client is reconnecting, or before the client connected
 * is never sent to that client, and a client evicted as a slow consumer loses the frames queued for
 * it. Within one stream, frames arrive in version order. A client that needs every event reads the
 * state it missed from a query or a projection after each (re)connect and treats the frames as a
 * notification; durable processing belongs in a projection.
 *
 * <p><b>Latency and cost.</b> A frame is written within one polling interval of the commit. The
 * feed reads every event of the global stream once per replica, whether or not a client is
 * subscribed to its stream, with the same deserialization and decryption as any other global
 * reader.
 *
 * <p><b>Failures.</b> A failed read is retried with backoff, from the same position. While an event
 * of the global stream cannot be read (an unregistered event type, a corrupt payload) the feed
 * delivers nothing, and continues once the read succeeds or the application restarts.
 *
 * <p>Thread-safe. {@link #start()} and {@link #close()} are idempotent, and a closed feed can be
 * started again.
 */
public final class SseEventFeed implements AutoCloseable {

  /** The name of the feed's polling subscription; its thread is named after it. */
  public static final String SUBSCRIPTION_NAME = "streamrune-sse-feed";

  /** The polling interval the integrations use when none is configured. */
  public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(1);

  private static final int FETCH_SIZE = 100;

  private static final Logger log = LoggerFactory.getLogger(SseEventFeed.class);

  private final EventStore eventStore;
  private final SseEventPublisher publisher;
  private final SubscriptionConfig config;

  /** The running subscription; {@code null} while the feed is stopped. Guarded by {@code this}. */
  private PollingEventSubscription subscription;

  /**
   * Creates a stopped feed.
   *
   * @param eventStore the store whose global stream is read (required); it must implement {@link
   *     EventStore#lastGlobalOffset()}
   * @param publisher the fan-out the events are published to (required)
   * @param pollingInterval how often the global stream is read for new events (required, positive)
   * @throws IllegalArgumentException if an argument is missing or the interval is not positive
   */
  public SseEventFeed(
      EventStore eventStore, SseEventPublisher publisher, Duration pollingInterval) {
    if (eventStore == null) {
      throw new IllegalArgumentException("eventStore is required");
    }
    if (publisher == null) {
      throw new IllegalArgumentException("publisher is required");
    }
    if (pollingInterval == null || pollingInterval.isZero() || pollingInterval.isNegative()) {
      throw new IllegalArgumentException(
          "pollingInterval (streamrune.sse.polling-interval) must be positive: " + pollingInterval);
    }
    this.eventStore = eventStore;
    this.publisher = publisher;
    // No jitter: one feed per replica, and the interval is the latency bound the docs state.
    this.config = new SubscriptionConfig(false, pollingInterval, Duration.ZERO);
  }

  /**
   * Starts the feed at the current head of the global stream. No-op when it is already running.
   *
   * @throws IllegalStateException if the event store does not implement {@link
   *     EventStore#lastGlobalOffset()}
   * @throws RuntimeException if the head of the global stream cannot be read
   */
  public synchronized void start() {
    if (subscription != null) {
      return;
    }
    GlobalOffset head = head();
    PollingEventSubscription started =
        PollingEventSubscription.builder()
            .subscriptionName(SUBSCRIPTION_NAME)
            .eventStore(eventStore)
            .offsetStore(new InMemoryPosition(head))
            .listener(this::publish)
            .config(config)
            .fetchSize(FETCH_SIZE)
            .build();
    started.start();
    subscription = started;
    log.info(
        "SSE event feed started after global offset {} (polling every {})",
        head.value(),
        config.pollingInterval());
  }

  /**
   * Whether the feed is started and its polling thread has not stopped.
   *
   * @return {@code true} between {@link #start()} and {@link #close()}
   */
  public synchronized boolean isRunning() {
    return subscription != null && subscription.isRunning();
  }

  /**
   * Stops the feed and waits for a read in flight to finish. No-op when it is not running. The
   * publisher and its subscribers are left as they are.
   */
  @Override
  public synchronized void close() {
    PollingEventSubscription running = subscription;
    if (running == null) {
      return;
    }
    subscription = null;
    running.close();
    log.info("SSE event feed stopped");
  }

  private GlobalOffset head() {
    GlobalOffset head;
    try {
      head = eventStore.lastGlobalOffset();
    } catch (UnsupportedOperationException e) {
      throw new IllegalStateException(headUnavailable(), e);
    }
    if (head == null) {
      throw new IllegalStateException(headUnavailable());
    }
    return head;
  }

  private String headUnavailable() {
    return "The SSE event feed starts at the head of the global stream, and "
        + eventStore.getClass().getName()
        + " does not report it: implement EventStore.lastGlobalOffset(), or disable the endpoint"
        + " (streamrune.sse.enabled=false)";
  }

  /**
   * Publishes one page. A failure on one event is logged and the page continues: rethrowing would
   * make the subscription read the page again and deliver its earlier events twice.
   */
  private void publish(List<EventEnvelope> events) {
    for (EventEnvelope envelope : events) {
      try {
        publisher.publish(envelope);
      } catch (RuntimeException e) {
        log.warn(
            "SSE event feed could not publish the event at global offset {} of stream {}; it is"
                + " not delivered",
            envelope.globalOffset().value(),
            LogSanitizer.sanitizeForLog(envelope.streamId().value()),
            e);
      }
    }
  }

  /** The feed's position: the head at the start, then the last page delivered. Never stored. */
  private static final class InMemoryPosition implements OffsetStore {

    private volatile GlobalOffset offset;

    InMemoryPosition(GlobalOffset head) {
      this.offset = head;
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return offset;
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      this.offset = offset;
    }
  }
}
