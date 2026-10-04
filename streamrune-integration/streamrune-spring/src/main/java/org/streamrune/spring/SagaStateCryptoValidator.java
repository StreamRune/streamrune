package org.streamrune.spring;

import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaStateCryptoValidation;

/**
 * Fails application startup when a registered saga carries an {@code @Encrypted} saga-state field
 * but no {@link CryptoEngine} bean is configured — otherwise that PII would be persisted as
 * PLAINTEXT in {@code saga_state} and could not be crypto-shredded. Mirrors the command/event/state
 * {@code @Encrypted} validation already performed in the command bus and event store factory, and
 * the {@link StreamRuneAuthorizationValidator} startup-guard pattern.
 *
 * <p>Runs after all singletons are instantiated so every {@link SagaRunner} bean is visible.
 */
public class SagaStateCryptoValidator implements SmartInitializingSingleton {

  private final ObjectProvider<SagaRunner<?>> sagaRunnerProvider;
  private final ObjectProvider<CryptoEngine> cryptoEngineProvider;

  public SagaStateCryptoValidator(
      ObjectProvider<SagaRunner<?>> sagaRunnerProvider,
      ObjectProvider<CryptoEngine> cryptoEngineProvider) {
    this.sagaRunnerProvider = sagaRunnerProvider;
    this.cryptoEngineProvider = cryptoEngineProvider;
  }

  @Override
  public void afterSingletonsInstantiated() {
    CryptoEngine cryptoEngine;
    try {
      cryptoEngine = cryptoEngineProvider.getIfAvailable();
    } catch (NoUniqueBeanDefinitionException e) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is marked @Primary — StreamRune cannot "
              + "decide which engine to validate saga-state types against, and skipping encryption "
              + "would persist @Encrypted saga-state fields as PLAINTEXT. Mark exactly one "
              + "CryptoEngine bean as @Primary.",
          e);
    }
    SagaStateCryptoValidation.validate(sagaRunnerProvider.orderedStream().toList(), cryptoEngine);
  }
}
