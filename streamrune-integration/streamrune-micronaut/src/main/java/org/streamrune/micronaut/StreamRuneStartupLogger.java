package org.streamrune.micronaut;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.Projection;

/**
 * Logs a single StreamRune startup summary line on {@link StartupEvent} — the Micronaut counterpart
 * of the Spring integration's {@code StreamRuneStartupLogger}.
 *
 * <p>Disabled with {@code streamrune.startup-log=false}. Presence checks use bean definitions, so
 * no bean is instantiated just for the log line.
 */
@Singleton
@Requires(property = "streamrune.startup-log", notEquals = "false")
public class StreamRuneStartupLogger implements ApplicationEventListener<StartupEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamRuneStartupLogger.class);

  private final ApplicationContext ctx;

  /**
   * Creates the startup logger.
   *
   * @param ctx the application context used for bean presence checks
   */
  public StreamRuneStartupLogger(ApplicationContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    String version = getClass().getPackage().getImplementationVersion();
    if (version == null) {
      version = "dev";
    }
    LOG.info(
        "StreamRune v{} | EventStore: {} | Crypto: {} | Projections: {} active",
        version,
        ctx.containsBean(EventStore.class) ? "present" : "none",
        ctx.containsBean(CryptoEngine.class) ? "active" : "none",
        ctx.getBeanDefinitions(Projection.class).size());
  }
}
