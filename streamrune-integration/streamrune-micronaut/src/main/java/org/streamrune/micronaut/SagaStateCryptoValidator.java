package org.streamrune.micronaut;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import java.util.List;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaStateCryptoValidation;

/**
 * Fails application startup when a registered saga carries an {@code @Encrypted} saga-state field
 * but no {@link CryptoEngine} bean is configured — otherwise that PII would be persisted as
 * PLAINTEXT in {@code saga_state} and could not be crypto-shredded. Mirrors the command/event/state
 * {@code @Encrypted} validation and the {@link StreamRuneAuthorizationValidator} startup-guard
 * pattern.
 */
@Singleton
public class SagaStateCryptoValidator implements ApplicationEventListener<StartupEvent>, Ordered {

  private final List<SagaRunner<?>> sagaRunners;
  private final List<CryptoEngine> cryptoEngines;

  public SagaStateCryptoValidator(
      List<SagaRunner<?>> sagaRunners, List<CryptoEngine> cryptoEngines) {
    this.sagaRunners = sagaRunners;
    this.cryptoEngines = cryptoEngines;
  }

  // Without Ordered, Micronaut's ordering among the several
  // ApplicationEventListener<StartupEvent> beans in this module is unspecified, so this
  // fail-closed check could run AFTER StreamRuneLifecycle's had already started the runners —
  // whose first poll (ResilientPollLoop, immediate) can persist data before an invalid
  // configuration is ever reported. A value strictly below StreamRuneLifecycle's own puts every
  // validator ahead of it (Ordered: LOWER value runs first). Mirrors Spring, where
  // SmartInitializingSingleton always completes before any SmartLifecycle starts.
  @Override
  public int getOrder() {
    return 1000;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (cryptoEngines.size() > 1) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans found ("
              + cryptoEngines.size()
              + "); register exactly one or qualify them, otherwise @Encrypted saga-state fields "
              + "could be persisted as PLAINTEXT. Found: "
              + cryptoEngines);
    }
    CryptoEngine engine = cryptoEngines.isEmpty() ? null : cryptoEngines.get(0);
    SagaStateCryptoValidation.validate(sagaRunners, engine);
  }
}
