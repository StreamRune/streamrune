package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.runtime.SagaRunner;

/**
 * Saga state crypto validation (Quarkus): startup fails when a registered saga carries an
 * {@code @Encrypted} saga-state field and no {@link CryptoEngine} is configured.
 */
class SagaStateCryptoValidatorTest {

  record EncryptedSagaState(
      String customerId, @Encrypted(subjectId = "customerId") String customerEmail)
      implements SagaState {
    @Override
    public SagaStatus status() {
      return SagaStatus.RUNNING;
    }
  }

  record PlainSagaState(String orderId) implements SagaState {
    @Override
    public SagaStatus status() {
      return SagaStatus.RUNNING;
    }
  }

  private static <S extends SagaState> SagaRunner<S> runnerFor(Class<S> stateType, S initial) {
    SagaDecider<S> decider =
        new SagaDecider<>() {
          @Override
          public Class<S> stateType() {
            return stateType;
          }

          @Override
          public S initialState(SagaId sagaId) {
            return initial;
          }

          @Override
          public S evolve(S state, EventEnvelope event) {
            return state;
          }

          @Override
          public List<SagaCommand> handle(S state, EventEnvelope event) {
            return List.of();
          }
        };
    CommandBus commandBus = mock(CommandBus.class);
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    return SagaRunner.<S>builder()
        .decider(decider)
        .startWhen(event -> true)
        .extractSagaId(event -> SagaId.of("saga-1"))
        .correlateBy(event -> Optional.empty())
        .sagaStore(mock(SagaStore.class))
        .commandBus(commandBus)
        .sagaDeadLetterStore(mock(SagaDeadLetterStore.class))
        .build();
  }

  @SuppressWarnings("unchecked")
  private static Instance<CryptoEngine> engine(CryptoEngine engine, boolean ambiguous) {
    Instance<CryptoEngine> instance = mock(Instance.class);
    when(instance.isAmbiguous()).thenReturn(ambiguous);
    when(instance.isUnsatisfied()).thenReturn(engine == null && !ambiguous);
    if (engine != null) {
      when(instance.get()).thenReturn(engine);
    }
    return instance;
  }

  @Test
  void startup_throws_whenEncryptedSagaStateAndNoEngine() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com"))),
            engine(null, false));

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> validator.validate(new StartupEvent()));
    assertTrue(ex.getMessage().contains(EncryptedSagaState.class.getName() + ".customerEmail"));
  }

  @Test
  void startup_passes_whenEncryptedSagaStateAndEngine() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com"))),
            engine(mock(CryptoEngine.class), false));

    assertDoesNotThrow(() -> validator.validate(new StartupEvent()));
  }

  @Test
  void startup_passes_whenNoEncryptedSagaState() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(PlainSagaState.class, new PlainSagaState("o"))), engine(null, false));

    assertDoesNotThrow(() -> validator.validate(new StartupEvent()));
  }

  @Test
  void startup_throws_whenAmbiguousCryptoEngines() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com"))),
            engine(null, true));

    assertThrows(IllegalStateException.class, () -> validator.validate(new StartupEvent()));
  }
}
