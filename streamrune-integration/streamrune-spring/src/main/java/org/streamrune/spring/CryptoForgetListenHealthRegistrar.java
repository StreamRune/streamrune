package org.streamrune.spring;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.runtime.BackgroundRelayHealthContributor;

/**
 * Registers the forget channel of every {@link CryptoEngine} bean with the {@link
 * BackgroundRelayHealthContributor}, as the {@code crypto-forget-listen} component, when the engine
 * is a {@code CachedCryptoEngine} whose channel listens for peers' forgets (the PostgreSQL {@code
 * LISTEN/NOTIFY} channel). The component reads {@code DEGRADED} while this replica does not hear
 * them. An application without a crypto engine, with the cache disabled, or with a channel that
 * does not listen registers nothing (see {@link
 * BackgroundRelayHealthContributor#registerCryptoEngine(CryptoEngine)}).
 */
public final class CryptoForgetListenHealthRegistrar implements SmartLifecycle {

  private final ObjectProvider<CryptoEngine> cryptoEngines;
  private final BackgroundRelayHealthContributor relayHealth;
  private volatile boolean running;
  private boolean registered;

  public CryptoForgetListenHealthRegistrar(
      ObjectProvider<CryptoEngine> cryptoEngines, BackgroundRelayHealthContributor relayHealth) {
    this.cryptoEngines = cryptoEngines;
    this.relayHealth = relayHealth;
  }

  @Override
  public void start() {
    // Register exactly once: a stop/start cycle does not reset `registered`.
    if (!registered) {
      cryptoEngines.orderedStream().forEach(relayHealth::registerCryptoEngine);
      registered = true;
    }
    running = true;
  }

  @Override
  public void stop() {
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /** Same phase as the runners the contributor reports on. */
  @Override
  public int getPhase() {
    return RunnerLifecycle.PHASE;
  }
}
