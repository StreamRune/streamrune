package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.context.event.StartupEvent;
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
 * Saga state crypto validation (Micronaut): startup fails when a registered saga carries an
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

  @Test
  void startup_throws_whenEncryptedSagaStateAndNoEngine() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com"))),
            List.of());

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                validator.onApplicationEvent(
                    new StartupEvent(mock(io.micronaut.context.BeanContext.class))));
    assertTrue(ex.getMessage().contains(EncryptedSagaState.class.getName() + ".customerEmail"));
  }

  @Test
  void startup_passes_whenEncryptedSagaStateAndEngine() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com"))),
            List.of(mock(CryptoEngine.class)));

    assertDoesNotThrow(
        () ->
            validator.onApplicationEvent(
                new StartupEvent(mock(io.micronaut.context.BeanContext.class))));
  }

  @Test
  void startup_passes_whenNoEncryptedSagaState() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(PlainSagaState.class, new PlainSagaState("o"))), List.of());

    assertDoesNotThrow(
        () ->
            validator.onApplicationEvent(
                new StartupEvent(mock(io.micronaut.context.BeanContext.class))));
  }

  @Test
  void startup_throws_whenMultipleCryptoEngines() {
    var validator =
        new SagaStateCryptoValidator(
            List.of(runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com"))),
            List.of(mock(CryptoEngine.class), mock(CryptoEngine.class)));

    assertThrows(
        IllegalStateException.class,
        () ->
            validator.onApplicationEvent(
                new StartupEvent(mock(io.micronaut.context.BeanContext.class))));
  }
}
