package org.streamrune.quarkus;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
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
 * stops. A {@code streamrune.sse.polling-interval} below one millisecond fails the start-up.
 *
 * <p><b>Health.</b> The started feed is registered with the {@link
 * BackgroundRelayHealthContributor} as {@code sse-event-feed}, so the readiness check reports
 * {@code DOWN} once its polling thread has died. A polling thread dies only of a JVM {@link Error},
 * and this lifecycle starts the feed once: the check stays {@code DOWN} until the application
 * restarts. The feed is an object of this lifecycle, not a bean, so an application cannot start it
 * again; one that wants to restart a feed without restarting itself declares its own.
 *
 * <p><b>A feed of the application's own.</b> When the application declares an {@link SseEventFeed}
 * bean, this lifecycle creates and starts none: the application owns its feed, starts and stops it,
 * may call {@link SseEventFeed#start()} again on a feed whose polling thread has died, and
 * registers it with {@link BackgroundRelayHealthContributor#registerSseEventFeed} to have it
 * reported — as with the other runners the framework would otherwise assemble.
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
  private final Instance<BackgroundRelayHealthContributor> relayHealthInstance;
  private final Instance<SseEventFeed> applicationFeedInstance;

  private volatile SseEventFeed feed;

  /**
   * The constructor Quarkus/Arc uses.
   *
   * @param eventStore the store whose global stream the feed reads
   * @param publisher the fan-out the SSE controller subscribes its clients to
   * @param properties supplies {@code streamrune.sse.polling-interval} and the runtime switch
   * @param relayHealthInstance the health contributor the started feed is registered with, when the
   *     application has one
   * @param applicationFeedInstance the feed the application declares, if any; this lifecycle then
   *     starts none
   */
  @Inject
  public SseEventFeedLifecycle(
      EventStore eventStore,
      SseEventPublisher publisher,
      StreamRuneQuarkusProperties properties,
      Instance<BackgroundRelayHealthContributor> relayHealthInstance,
      Instance<SseEventFeed> applicationFeedInstance) {
    this.eventStore = eventStore;
    this.publisher = publisher;
    this.properties = properties;
    this.relayHealthInstance = relayHealthInstance;
    this.applicationFeedInstance = applicationFeedInstance;
  }

  /**
   * Client-proxy constructor — see {@link StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance.
   */
  protected SseEventFeedLifecycle() {
    this.eventStore = null;
    this.publisher = null;
    this.properties = null;
    this.relayHealthInstance = null;
    this.applicationFeedInstance = null;
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
    if (!applicationFeedInstance.isUnsatisfied()) {
      logger.info(
          "StreamRune: the application declares its own SseEventFeed; the framework starts none");
      return;
    }
    SseEventFeed started =
        new SseEventFeed(eventStore, publisher, properties.sse().pollingInterval());
    if (relayHealthInstance.isResolvable()) {
      relayHealthInstance.get().registerSseEventFeed(started);
    }
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
   * Whether the feed this lifecycle started is running: started and with a live polling thread.
   *
   * @return {@code true} between the startup and the shutdown event of an application whose
   *     endpoint is enabled, for as long as the feed's polling thread is alive; {@code false} when
   *     the endpoint is switched off at runtime or the application declares its own feed
   */
  public boolean isRunning() {
    SseEventFeed running = feed;
    return running != null && running.isRunning();
  }
}
