package org.streamrune.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;

/** Logs a single startup summary line after all StreamRune beans have been initialized. */
public class StreamRuneStartupLogger implements SmartInitializingSingleton {

  private static final Logger LOG = LoggerFactory.getLogger(StreamRuneStartupLogger.class);
  private final ApplicationContext ctx;

  public StreamRuneStartupLogger(ApplicationContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void afterSingletonsInstantiated() {
    String version = getClass().getPackage().getImplementationVersion();
    if (version == null) version = "dev";

    String eventStoreType = ctx.containsBean("eventStore") ? "postgres" : "none";

    // Count bean definitions instead of fetching the bean: getBean threw on two
    // engines (and on a lazy engine that failed to create), and the swallowed exception logged
    // "none" for an application with encryption configured. It also instantiated a lazy bean just
    // to log a line.
    String crypto =
        ctx.getBeanNamesForType(org.streamrune.core.crypto.CryptoEngine.class).length > 0
            ? "active"
            : "none";

    String[] projections = ctx.getBeanNamesForType(org.streamrune.core.projection.Projection.class);

    LOG.info(
        "StreamRune v{} | EventStore: {} | Crypto: {} | Projections: {} active",
        version,
        eventStoreType,
        crypto,
        projections.length);
  }
}
