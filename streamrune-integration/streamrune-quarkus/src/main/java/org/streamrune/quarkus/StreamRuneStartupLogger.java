package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.Projection;

/**
 * Logs a single StreamRune startup summary line on {@link StartupEvent} — the Quarkus counterpart
 * of the Spring integration's {@code StreamRuneStartupLogger}.
 *
 * <p>Controlled by {@code streamrune.startup-log} (default {@code true}). The flag is checked at
 * runtime so it can be flipped per environment without a rebuild.
 */
@ApplicationScoped
public class StreamRuneStartupLogger {

  private static final Logger LOG = LoggerFactory.getLogger(StreamRuneStartupLogger.class);

  private final StreamRuneQuarkusProperties properties;
  private final Instance<EventStore> eventStore;
  private final Instance<CryptoEngine> cryptoEngine;
  private final Instance<Projection> projections;

  public StreamRuneStartupLogger(
      StreamRuneQuarkusProperties properties,
      Instance<EventStore> eventStore,
      Instance<CryptoEngine> cryptoEngine,
      Instance<Projection> projections) {
    this.properties = properties;
    this.eventStore = eventStore;
    this.cryptoEngine = cryptoEngine;
    this.projections = projections;
  }

  void onStart(@Observes StartupEvent event) {
    if (!properties.startupLog()) {
      return;
    }
    String version = getClass().getPackage().getImplementationVersion();
    if (version == null) {
      version = "dev";
    }
    long projectionCount = projections.stream().count();
    LOG.info(
        "StreamRune v{} | EventStore: {} | Crypto: {} | Projections: {} active",
        version,
        eventStore.isResolvable() ? "present" : "none",
        cryptoEngine.isResolvable() ? "active" : "none",
        projectionCount);
  }
}
