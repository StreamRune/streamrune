package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.runtime.BackgroundRelayHealthContributor;

/**
 * Registers the forget channel of the {@link CryptoEngine} bean with the {@link
 * BackgroundRelayHealthContributor} on {@link StartupEvent}, as the {@code crypto-forget-listen}
 * component, when the engine is a {@code CachedCryptoEngine} whose channel listens for peers'
 * forgets (the PostgreSQL {@code LISTEN/NOTIFY} channel). The component reads {@code DEGRADED}
 * while this replica does not hear them. An application without a crypto engine, with the cache
 * disabled, or with a channel that does not listen registers nothing (see {@link
 * BackgroundRelayHealthContributor#registerCryptoEngine(CryptoEngine)}).
 */
@ApplicationScoped
public class CryptoForgetListenHealthRegistrar {

  private final Instance<CryptoEngine> cryptoEngines;
  private final Instance<BackgroundRelayHealthContributor> relayHealthInstance;

  @Inject
  public CryptoForgetListenHealthRegistrar(
      Instance<CryptoEngine> cryptoEngines,
      Instance<BackgroundRelayHealthContributor> relayHealthInstance) {
    this.cryptoEngines = cryptoEngines;
    this.relayHealthInstance = relayHealthInstance;
  }

  /**
   * Client-proxy constructor — see {@link StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance: the injecting constructor above is the one
   * Arc calls.
   */
  protected CryptoForgetListenHealthRegistrar() {
    this.cryptoEngines = null;
    this.relayHealthInstance = null;
  }

  // `public` so that the observer invoker Arc generates, a separate class that may live in a
  // different classloader, can call it; see StreamRuneConfigValidator#validate.
  public void onStart(@Observes StartupEvent event) {
    registerCryptoForgetListenHealth();
  }

  /** Package-private for tests: register the forget channel of every resolvable engine. */
  void registerCryptoForgetListenHealth() {
    if (relayHealthInstance.isUnsatisfied()) {
      return;
    }
    BackgroundRelayHealthContributor health = relayHealthInstance.get();
    for (CryptoEngine engine : cryptoEngines) {
      health.registerCryptoEngine(engine);
    }
  }
}
