package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

/**
 * A saga whose state carries an {@code @Encrypted} field must fail startup when no {@link
 * CryptoEngine} is configured (fail-closed), the same way {@code @Encrypted} event and command
 * fields already do.
 */
class SagaStateCryptoValidationTest {

  /** Saga state with an {@code @Encrypted} PII component and no CryptoEngine anywhere. */
  record EncryptedSagaState(
      String customerId, @Encrypted(subjectId = "customerId") String customerEmail)
      implements SagaState {
    @Override
    public SagaStatus status() {
      return SagaStatus.RUNNING;
    }
  }

  /** Saga state with no encrypted fields — safe without a CryptoEngine. */
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

  @Test
  void encryptedSagaStateWithNoCryptoEngine_failsStartup() {
    SagaRunner<EncryptedSagaState> runner =
        runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c-1", "pii@example.com"));

    assertThatThrownBy(() -> SagaStateCryptoValidation.validate(List.of(runner), null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(EncryptedSagaState.class.getName() + ".customerEmail")
        .hasMessageContaining("CryptoEngine");
  }

  @Test
  void encryptedSagaStateWithCryptoEngine_startsCleanly() {
    SagaRunner<EncryptedSagaState> runner =
        runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c-1", "pii@example.com"));

    assertThatCode(
            () -> SagaStateCryptoValidation.validate(List.of(runner), mock(CryptoEngine.class)))
        .doesNotThrowAnyException();
  }

  @Test
  void plainSagaStateWithNoCryptoEngine_startsCleanly() {
    SagaRunner<PlainSagaState> runner =
        runnerFor(PlainSagaState.class, new PlainSagaState("order-1"));

    assertThatCode(() -> SagaStateCryptoValidation.validate(List.of(runner), null))
        .doesNotThrowAnyException();
  }

  @Test
  void noSagaRunners_isNoOp() {
    assertThatCode(() -> SagaStateCryptoValidation.validate(List.of(), null))
        .doesNotThrowAnyException();
    assertThatCode(() -> SagaStateCryptoValidation.validate(null, null)).doesNotThrowAnyException();
  }

  @Test
  void validatorReadsStateTypeFromRunner() {
    // Guards the package-private SagaRunner.stateType() access the validator relies on.
    SagaRunner<PlainSagaState> runner =
        runnerFor(PlainSagaState.class, new PlainSagaState("order-1"));
    assertThat(runner.stateType()).isEqualTo(PlainSagaState.class);
  }
}
