package org.streamrune.runtime;

import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

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
 * <p><b>On demand.</b> The feed reads events only while a client is subscribed to the publisher
 * ({@link SseEventPublisher#hasSubscribers()}), on whatever stream. A poll that finds nobody
 * subscribed reads no event and decrypts nothing: it reads the head of the global stream, {@link
 * EventStore#lastGlobalOffset()}, and moves its position there, so what was stored for nobody is
 * never read. A poll that finds a subscriber reads every event stored after the position, of every
 * stream. The subscription this feed runs is unchanged by that: it reads through a view of the
 * event store ({@code OnDemandGlobalStream}) that answers an idle poll with no events, and its
 * position is an offset store the feed moves.
 *
 * <p><b>Why an idle poll loses no event.</b> A client is <em>registered</em> from the moment {@link
 * SseEventPublisher#subscribe} has returned for it until it is unsubscribed or evicted; an endpoint
 * writes its opening comment after that moment. An idle poll takes three steps, in this order:
 *
 * <ol>
 *   <li>it <em>captures</em> the head of the global stream;
 *   <li>it <em>checks</em> whether a client is registered;
 *   <li>only when the check found none, it <em>moves</em> its position to the head captured in step
 *       1.
 * </ol>
 *
 * <p>Take a client registered at a moment R and an event E stored after R. The position of the feed
 * gets past E in two ways only. A read returns E: the feed publishes E right after the read, to the
 * client if it is still registered. Or step 3 moves the position to a captured head at or beyond E:
 * then E was stored before that capture, because the head holds stored events only and an event
 * stored later has a greater offset, so the check of step 2, which follows the capture, ran after E
 * was stored and therefore after R. That check found nobody, and the publisher never answers
 * "nobody" while a client stays registered, so the client had left by then. The position therefore
 * never passes an event stored after a client registered while that client is still registered,
 * except by reading and publishing it. The order matters: checking first and capturing second loses
 * E when the client registers and E is stored between the two.
 *
 * <p>What the argument needs from the event store is the contract of {@link
 * EventStore#lastGlobalOffset()}: an event that becomes readable after that method returned has a
 * greater offset than the one returned. The PostgreSQL store reports the highest committed offset
 * of a counter that assigns offsets in commit order; the in-memory store reports the head of the
 * completed appends.
 *
 * <p><b>Every interleaving.</b> With L the look for subscribers that opens a poll, C the capture, K
 * the check and M the move of an idle poll:
 *
 * <ul>
 *   <li><i>A client registers before L.</i> The poll is not idle: it reads from the position.
 *   <li><i>A client registers between L and C, or between C and K.</i> K finds it: nothing moves
 *       and the poll reads from the position. An event stored after the registration is in that
 *       read or a later one, whether or not the captured head already held it.
 *   <li><i>A client registers after K.</i> The position moves to the head captured before the
 *       registration; an event stored after the registration is beyond it and the next poll, which
 *       finds the client, reads it.
 *   <li><i>An event is stored while nobody is registered.</i> Stored before C, it is behind the
 *       position after M and never read. Stored after C, it stays ahead of the position: the next
 *       idle poll passes it, or a client that registers before that poll receives it (below).
 *   <li><i>The last client leaves during a read.</i> The page is published to whoever is registered
 *       at that moment, the position moves past it, and the next poll is idle. A client that leaves
 *       between L and the read costs one page read for nobody.
 *   <li><i>{@link #start()}.</i> The position is the head at the start. A client registered before
 *       the start receives the events stored after the start, not the ones stored before it.
 *   <li><i>{@link #close()}.</i> The poll in flight finishes; nothing is delivered afterwards, and
 *       the position is dropped.
 *   <li><i>{@code start()} replaces a subscription whose thread has died.</i> The replacement
 *       begins at the head of that moment: the events stored while no thread polled are not
 *       delivered, to a registered client either.
 *   <li><i>The read of the head fails.</i> The poll ends before K, the position stays where it is,
 *       the failure is counted ({@link #consecutiveFailures()}, health {@code DEGRADED}) and the
 *       poll is retried with backoff. A client that registers meanwhile turns the next poll into a
 *       read from that unchanged position: nothing stored after its registration is lost.
 * </ul>
 *
 * <p><b>Delivery guarantee: best-effort, at-most-once.</b> A frame reaches a client only while the
 * client is connected and the feed is running. Nothing is redelivered, and a client evicted as a
 * slow consumer loses the frames queued for it. An event appended while the application is down or
 * restarting, while a client is reconnecting, or before the client connected is not sent to that
 * client, with one exception: an event of its stream stored shortly before the client registered,
 * which the feed had not passed yet, can still arrive (next paragraph). Within one stream, frames
 * arrive in version order. A client that needs every event reads the state it missed from a query
 * or a projection after each (re)connect and treats the frames as a notification; durable
 * processing belongs in a projection.
 *
 * <p><b>Events from just before a registration.</b> A client can receive events of its stream that
 * were stored shortly before it registered: the ones beyond the feed's position at that moment.
 * With other clients subscribed those are the events the feed has not read yet, about one polling
 * interval's worth. With the feed idle they are the events stored since the last capture of the
 * head, again about one polling interval's worth. Both windows grow to the time since the last read
 * that succeeded while reads fail. A client never receives an event behind the position.
 *
 * <p><b>Latency.</b> A frame is normally written within one polling interval of the commit, for the
 * first client of an idle feed as for any other. That is the usual delay, not a bound: a failed
 * read is retried with backoff, up to 60 seconds between attempts.
 *
 * <p><b>Cost.</b> With no client subscribed, a poll is one read of the head of the global stream:
 * no event is read, deserialized or decrypted, however many are stored. While at least one client
 * is subscribed to this replica's publisher, on any stream, the feed reads every event of the
 * global stream once and decrypts it, whether or not a client is subscribed to that event's stream:
 * the same deserialization and decryption as any other global reader, paid by every replica that
 * has a client.
 *
 * <p><b>Failures.</b> A failed read, of events or of the head, is retried with backoff, from the
 * same position. While an event of the global stream cannot be read (an unregistered event type, a
 * corrupt payload) the feed delivers nothing; it continues once the read succeeds, once a poll
 * finds nobody subscribed and moves the position to the head, past that event, or when the
 * application restarts. An event the publisher refuses is logged and skipped; the rest of its page
 * is published. An {@link Error} out of a read or a publish ends the polling thread: the feed then
 * reports {@link #isStarted()} and not {@link #isRunning()}, and the next {@link #start()} replaces
 * the dead subscription. Nothing calls {@code start()} a second time on its own: the integrations
 * start their feed once, so a feed in that state stays in it until the application restarts or
 * calls {@code start()} itself. Every replica with a client reads the same events, so an event that
 * kills one replica's feed kills theirs too; a replacement begins past it.
 *
 * <p><b>Health.</b> {@link BackgroundRelayHealthContributor#registerSseEventFeed} reports the feed
 * with the framework's other polling threads: {@code DOWN} when its thread has died, until the feed
 * is started again, and {@code DEGRADED} while its reads fail.
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

  /**
   * The subscription of a started feed; {@code null} while the feed is stopped. Not running any
   * more when its polling thread has died. Guarded by {@code this}.
   */
  private PollingEventSubscription subscription;

  /**
   * Creates a stopped feed.
   *
   * @param eventStore the store whose global stream is read (required); it must implement {@link
   *     EventStore#lastGlobalOffset()} under the contract stated there: an event that becomes
   *     readable after that method returned has a greater offset than the one it returned
   * @param publisher the fan-out the events are published to (required)
   * @param pollingInterval how often the global stream is read for new events (required, at least
   *     one millisecond: the polling thread sleeps whole milliseconds, so a shorter interval would
   *     be no pause at all between two reads)
   * @throws IllegalArgumentException if an argument is missing or the interval is below one
   *     millisecond
   */
  public SseEventFeed(
      EventStore eventStore, SseEventPublisher publisher, Duration pollingInterval) {
    if (eventStore == null) {
      throw new IllegalArgumentException("eventStore is required");
    }
    if (publisher == null) {
      throw new IllegalArgumentException("publisher is required");
    }
    if (pollingInterval == null || pollingInterval.toMillis() < 1) {
      throw new IllegalArgumentException(
          "pollingInterval (streamrune.sse.polling-interval) must be at least 1ms: "
              + pollingInterval);
    }
    this.eventStore = eventStore;
    this.publisher = publisher;
    // No jitter: one feed per replica, and the interval is the delay the documentation states.
    this.config = new SubscriptionConfig(false, pollingInterval, Duration.ZERO);
  }

  /**
   * Starts the feed at the current head of the global stream. No-op when it is already running. A
   * feed whose polling thread has died is started again: its subscription is replaced by one that
   * begins at the head of this moment, like the feed of a restarted application.
   *
   * <p>A feed started with nobody subscribed stays idle: see the class documentation.
   *
   * @throws IllegalStateException if the event store does not implement {@link
   *     EventStore#lastGlobalOffset()}
   * @throws RuntimeException if the head of the global stream cannot be read
   */
  public synchronized void start() {
    if (subscription != null) {
      if (subscription.isRunning()) {
        return;
      }
      log.warn("SSE event feed: the polling thread has stopped; starting a new subscription");
      subscription.close();
      subscription = null;
    }
    GlobalOffset head = head();
    InMemoryPosition position = new InMemoryPosition(head);
    PollingEventSubscription started =
        PollingEventSubscription.builder()
            .subscriptionName(SUBSCRIPTION_NAME)
            .eventStore(new OnDemandGlobalStream(position))
            .offsetStore(position)
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
   * Whether the feed is started and its polling thread is alive.
   *
   * @return {@code true} after {@link #start()} for as long as the polling thread runs; {@code
   *     false} before the start, after {@link #close()}, and once an {@link Error} has ended the
   *     polling thread
   */
  public synchronized boolean isRunning() {
    return subscription != null && subscription.isRunning();
  }

  /**
   * Whether the feed has been started and not closed, whatever became of its polling thread. A feed
   * that is started and not {@linkplain #isRunning() running} has lost its thread.
   *
   * @return {@code true} between {@link #start()} and {@link #close()}
   */
  public synchronized boolean isStarted() {
    return subscription != null;
  }

  /**
   * The number of polls that have failed in a row, on a read of events or, with nobody subscribed,
   * on the read of the head; {@code 0} after a poll that succeeded and while the feed is stopped. A
   * non-zero value means the feed is retrying with backoff and delivers nothing meanwhile.
   *
   * @return the consecutive failed polls
   */
  public synchronized int consecutiveFailures() {
    return subscription == null ? 0 : subscription.consecutiveFailures();
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

  /**
   * What the feed's subscription reads through: the global stream of the event store, and only
   * while a client is subscribed. The subscription asks for the events after its position on every
   * poll; this is where a poll with nobody subscribed turns into a read of the head.
   */
  private final class OnDemandGlobalStream implements EventStore {

    private final InMemoryPosition position;

    OnDemandGlobalStream(InMemoryPosition position) {
      this.position = position;
    }

    /**
     * One read of the subscription: the events after {@code afterOffset} while a client is
     * subscribed, and otherwise nothing, with the position moved to the head. The order of the
     * three idle steps is what the delivery guarantee rests on; see the class documentation.
     */
    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      if (publisher.hasSubscribers()) {
        // Somebody is subscribed: read as a feed that never idles. This look saves the read of the
        // head; the guarantee does not depend on it.
        return eventStore.readGlobalStream(afterOffset, maxCount);
      }
      // 1. Capture the head, before the check below.
      GlobalOffset capturedHead = head();
      // 2. Check for subscribers, after the capture.
      if (publisher.hasSubscribers()) {
        // A client registered meanwhile. The captured head may already hold an event stored after
        // that registration, so the position stays where it is and the events after it are read.
        return eventStore.readGlobalStream(afterOffset, maxCount);
      }
      // 3. Nobody was subscribed at a moment after the capture: every event up to the captured
      // head was stored before that moment, with nobody to deliver it to.
      position.skipTo(capturedHead);
      return List.of();
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw globalStreamOnly();
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      throw globalStreamOnly();
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw globalStreamOnly();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw globalStreamOnly();
    }

    private UnsupportedOperationException globalStreamOnly() {
      return new UnsupportedOperationException(
          "The SSE event feed's subscription reads the global stream only");
    }
  }

  /**
   * The feed's position: the head at the start, then the last page delivered or, while nobody is
   * subscribed, the head an idle poll captured. Never stored.
   */
  private static final class InMemoryPosition implements OffsetStore {

    private volatile GlobalOffset offset;

    InMemoryPosition(GlobalOffset head) {
      this.offset = head;
    }

    /** Moves the position forward to {@code head}; a head behind the position changes nothing. */
    void skipTo(GlobalOffset head) {
      if (head.value() > offset.value()) {
        offset = head;
      }
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
