package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CryptoConfigValidator;

/**
 * Startup guard that fails fast when a registered saga carries an {@code @Encrypted} saga-state
 * field but no {@link CryptoEngine} is configured.
 *
 * <p><b>Why this exists:</b> {@link CryptoConfigValidator} already fails startup for
 * {@code @Encrypted} event, aggregate-state, and command types, but saga-state types are resolved
 * by explicit {@code Class} through {@code SagaStore.load(SagaId, SagaType, Class)} rather than the
 * event type registry, so they fall outside that scan. Without this guard, a {@code SagaState}
 * record with an {@code @Encrypted} PII field deployed without a {@code CryptoEngine} bean gets
 * zero error signal: the saga store's {@code ObjectMapper} carries no {@code
 * CryptoShreddingModule}, so the field is written as PLAINTEXT into {@code saga_state.state_json}
 * and a later GDPR forget cannot erase it.
 *
 * <p>This validator lives in {@code org.streamrune.runtime} so it can read each {@link
 * SagaRunner}'s package-private saga-state type; the three integration auto-configs invoke {@link
 * #validate(Collection, CryptoEngine)} at startup from their registered {@code SagaRunner} beans,
 * next to the existing command/event/state validations, giving {@code @Encrypted} saga fields the
 * same fail-closed treatment.
 */
public final class SagaStateCryptoValidation {

  private SagaStateCryptoValidation() {}

  /**
   * Collects the saga-state type of every runner and delegates to {@link
   * CryptoConfigValidator#validate(Collection, CryptoEngine)}, which throws when {@code engine} is
   * {@code null} and any saga-state type carries an {@code @Encrypted} field (recursing into nested
   * records exactly as for events and commands).
   *
   * @param sagaRunners the registered saga runners; {@code null}/empty is a no-op
   * @param engine the configured {@link CryptoEngine}, or {@code null} if none is wired
   * @throws IllegalStateException if {@code engine} is {@code null} and at least one saga-state
   *     type has an {@code @Encrypted} field — the message names every offending class and field
   */
  public static void validate(
      Collection<? extends SagaRunner<?>> sagaRunners, CryptoEngine engine) {
    if (sagaRunners == null || sagaRunners.isEmpty()) {
      return;
    }
    List<Class<?>> sagaStateTypes = new ArrayList<>();
    for (SagaRunner<?> runner : sagaRunners) {
      if (runner != null) {
        sagaStateTypes.add(runner.stateType());
      }
    }
    CryptoConfigValidator.validate(sagaStateTypes, engine);
  }
}
