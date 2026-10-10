package org.streamrune.micronaut;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.runtime.SseEventFeed;
import org.streamrune.runtime.SseEventPublisher;

/**
 * Owns the feed of the Server-Sent Events endpoint: the {@link SseEventFeed} that publishes every
 * event stored after the application started to the {@link SseEventPublisher} the {@link
 * SseController} subscribes its clients to.
 *
 * <p>Registered under the same condition as the controller ({@code streamrune.sse.enabled=true}).
 * On {@link StartupEvent} the feed starts at the head of the global stream; when the application
 * context closes, it stops.
 *
 * <p>Delivery is live, best-effort and at-most-once; see {@link SseEventFeed}.
 */
@Singleton
@Requires(property = "streamrune.sse.enabled", value = "true")
public class SseEventFeedLifecycle
    implements ApplicationEventListener<StartupEvent>, AutoCloseable, Ordered {

  private static final Logger logger = LoggerFactory.getLogger(SseEventFeedLifecycle.class);

  private final SseEventFeed feed;

  /**
   * Creates the lifecycle and its stopped feed.
   *
   * @param eventStore the store whose global stream the feed reads
   * @param publisher the fan-out the SSE controller subscribes its clients to
   * @param properties supplies {@code streamrune.sse.polling-interval}
   * @throws IllegalArgumentException if the polling interval is not positive
   */
  public SseEventFeedLifecycle(
      EventStore eventStore,
      SseEventPublisher publisher,
      StreamRuneMicronautProperties properties) {
    Duration pollingInterval = properties.ssePollingInterval();
    this.feed =
        new SseEventFeed(
            eventStore,
            publisher,
            pollingInterval != null ? pollingInterval : SseEventFeed.DEFAULT_POLLING_INTERVAL);
  }

  // The same order as StreamRuneLifecycle: after every fail-closed validator (1000).
  @Override
  public int getOrder() {
    return 1100;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    feed.start();
    logger.info("Started SseEventFeed");
  }

  /** Stops the feed. */
  @PreDestroy
  @Override
  public void close() {
    try {
      feed.close();
    } catch (RuntimeException e) {
      logger.warn("SseEventFeed failed to stop cleanly", e);
    }
  }

  /**
   * Whether the feed is started and its polling thread has not stopped.
   *
   * @return {@code true} between the startup event and the close of the application context
   */
  public boolean isRunning() {
    return feed.isRunning();
  }
}
