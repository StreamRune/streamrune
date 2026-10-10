package org.streamrune.micronaut;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.runtime.SseEventFeed;
import org.streamrune.runtime.SseEventPublisher;

/**
 * Starts and stops the feed of the Server-Sent Events endpoint: the {@link SseEventFeed} that
 * publishes every event stored after the application started to the {@link SseEventPublisher} the
 * {@link SseController} subscribes its clients to.
 *
 * <p>Registered under the same condition as the controller ({@code streamrune.sse.enabled=true}).
 * On {@link StartupEvent} the feed starts at the head of the global stream; when the application
 * context closes, it stops.
 *
 * <p><b>Whose feed.</b> The feed is the bean {@link StreamRuneMicronautModule#sseEventFeed} creates
 * and registers for health reporting, injected here by its framework name. An application that
 * declares its own {@link SseEventFeed} bean replaces that bean; this lifecycle then has nothing to
 * start, and the application starts and stops its feed itself, as with the other runners the
 * framework would otherwise assemble.
 *
 * <p>Delivery is live, best-effort and at-most-once; see {@link SseEventFeed}.
 */
@Singleton
@Requires(property = "streamrune.sse.enabled", value = "true")
public class SseEventFeedLifecycle
    implements ApplicationEventListener<StartupEvent>, AutoCloseable, Ordered {

  private static final Logger logger = LoggerFactory.getLogger(SseEventFeedLifecycle.class);

  /** The framework's feed; {@code null} when the application declares its own. */
  private final SseEventFeed feed;

  /**
   * Creates the lifecycle of the framework's feed.
   *
   * @param feed the feed {@link StreamRuneMicronautModule#sseEventFeed} created; {@code null} when
   *     the application declares its own {@link SseEventFeed} bean
   */
  public SseEventFeedLifecycle(
      @Nullable @Named(StreamRuneMicronautModule.FRAMEWORK_SSE_EVENT_FEED) SseEventFeed feed) {
    this.feed = feed;
  }

  // The same order as StreamRuneLifecycle: after every fail-closed validator (1000).
  @Override
  public int getOrder() {
    return 1100;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (feed == null) {
      logger.info("The application declares its own SseEventFeed; the framework starts none");
      return;
    }
    feed.start();
    logger.info("Started SseEventFeed");
  }

  /** Stops the framework's feed. */
  @PreDestroy
  @Override
  public void close() {
    if (feed == null) {
      return;
    }
    try {
      feed.close();
    } catch (RuntimeException e) {
      logger.warn("SseEventFeed failed to stop cleanly", e);
    }
  }

  /**
   * Whether the framework's feed is running: started and with a live polling thread.
   *
   * @return {@code true} between the startup event and the close of the application context, for as
   *     long as the feed's polling thread is alive; {@code false} when the application declares its
   *     own feed
   */
  public boolean isRunning() {
    return feed != null && feed.isRunning();
  }
}
