package org.streamrune.micronaut;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import java.util.List;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.runtime.BackgroundRelayHealthContributor;

/**
 * Registers the forget channel of every {@link CryptoEngine} bean with the {@link
 * BackgroundRelayHealthContributor} on {@link StartupEvent}, as the {@code crypto-forget-listen}
 * component, when the engine is a {@code CachedCryptoEngine} whose channel listens for peers'
 * forgets (the PostgreSQL {@code LISTEN/NOTIFY} channel). The component reads {@code DEGRADED}
 * while this replica does not hear them. An application without a crypto engine, with the cache
 * disabled, or with a channel that does not listen registers nothing (see {@link
 * BackgroundRelayHealthContributor#registerCryptoEngine(CryptoEngine)}).
 */
@Singleton
@Requires(beans = BackgroundRelayHealthContributor.class)
public class CryptoForgetListenHealthRegistrar implements ApplicationEventListener<StartupEvent> {

  private final List<CryptoEngine> cryptoEngines;
  private final BackgroundRelayHealthContributor relayHealth;

  public CryptoForgetListenHealthRegistrar(
      List<CryptoEngine> cryptoEngines, BackgroundRelayHealthContributor relayHealth) {
    this.cryptoEngines = cryptoEngines;
    this.relayHealth = relayHealth;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    registerCryptoForgetListenHealth();
  }

  /** Package-private for tests: register the forget channel of every discovered engine. */
  void registerCryptoForgetListenHealth() {
    if (cryptoEngines == null) {
      return;
    }
    cryptoEngines.forEach(relayHealth::registerCryptoEngine);
  }
}
