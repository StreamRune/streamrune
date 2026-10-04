package org.streamrune.quarkus;

import io.quarkus.arc.All;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
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
@ApplicationScoped
public class SagaStateCryptoValidator {

  private final List<SagaRunner<?>> sagaRunners;
  private final Instance<CryptoEngine> cryptoEngine;

  @jakarta.inject.Inject
  public SagaStateCryptoValidator(
      @All List<SagaRunner<?>> sagaRunners, Instance<CryptoEngine> cryptoEngine) {
    this.sagaRunners = sagaRunners;
    this.cryptoEngine = cryptoEngine;
  }

  /**
   * Client-proxy constructor — see {@link StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance.
   */
  protected SagaStateCryptoValidator() {
    this.sagaRunners = null;
    this.cryptoEngine = null;
  }

  // See StreamRuneConfigValidator's identical annotation for the rationale — every
  // validator must run before StreamRuneLifecycle.onStart, which CDI does not guarantee without an
  // explicit @Priority.
  //
  // @Priority must sit on the EVENT PARAMETER, not the method — see
  // StreamRuneConfigValidator#validate for why. `public` for the same cross-classloader reason as
  // the constructor above.
  public void validate(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event) {
    if (cryptoEngine.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is resolvable as default — StreamRune cannot "
              + "decide which engine to validate saga-state types against, and skipping encryption "
              + "would persist @Encrypted saga-state fields as PLAINTEXT. Ensure exactly one "
              + "CryptoEngine bean is resolvable (remove duplicates or qualify one with @Default).");
    }
    CryptoEngine engine = cryptoEngine.isUnsatisfied() ? null : cryptoEngine.get();
    SagaStateCryptoValidation.validate(sagaRunners, engine);
  }
}
