package org.streamrune.quarkus;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
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
 * <p>Registered under the same build-time condition as the controller ({@code
 * streamrune.sse.enabled=true}). On {@link StartupEvent} the feed starts at the head of the global
 * stream, unless the endpoint is switched off at runtime ({@code streamrune.sse.enabled=false}: the
 * controller answers {@code 404}, so nothing is read for it). On {@link ShutdownEvent} the feed
 * stops.
 *
 * <p>Delivery is live, best-effort and at-most-once; see {@link SseEventFeed}.
 */
@ApplicationScoped
@IfBuildProperty(name = "streamrune.sse.enabled", stringValue = "true")
public class SseEventFeedLifecycle {

  private static final Logger logger = LoggerFactory.getLogger(SseEventFeedLifecycle.class);

  private final EventStore eventStore;
  private final SseEventPublisher publisher;
  private final StreamRuneQuarkusProperties properties;

  private volatile SseEventFeed feed;

  @Inject
  public SseEventFeedLifecycle(
      EventStore eventStore, SseEventPublisher publisher, StreamRuneQuarkusProperties properties) {
    this.eventStore = eventStore;
    this.publisher = publisher;
    this.properties = properties;
  }

  /**
   * Client-proxy constructor — see {@link StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance.
   */
  protected SseEventFeedLifecycle() {
    this.eventStore = null;
    this.publisher = null;
    this.properties = null;
  }

  // The same priority as StreamRuneLifecycle: after every fail-closed validator
  // (Interceptor.Priority.LIBRARY_BEFORE). @Priority sits on the event parameter and the method is
  // public for the reasons given on StreamRuneConfigValidator#validate.
  public void onStart(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE + 100) StartupEvent event) {
    if (!properties.sse().enabled()) {
      logger.info("StreamRune: SSE endpoint is disabled at runtime; its event feed is not started");
      return;
    }
    SseEventFeed started =
        new SseEventFeed(eventStore, publisher, properties.sse().pollingInterval());
    started.start();
    feed = started;
    logger.info("StreamRune: started SseEventFeed");
  }

  public void onStop(@Observes ShutdownEvent event) {
    SseEventFeed running = feed;
    if (running == null) {
      return;
    }
    feed = null;
    try {
      running.close();
      logger.info("StreamRune: stopped SseEventFeed");
    } catch (RuntimeException e) {
      logger.warn("StreamRune: SseEventFeed failed to stop cleanly", e);
    }
  }

  /**
   * Whether the feed is started and its polling thread has not stopped.
   *
   * @return {@code true} between the startup and the shutdown event of an application whose
   *     endpoint is enabled
   */
  public boolean isRunning() {
    SseEventFeed running = feed;
    return running != null && running.isRunning();
  }
}
