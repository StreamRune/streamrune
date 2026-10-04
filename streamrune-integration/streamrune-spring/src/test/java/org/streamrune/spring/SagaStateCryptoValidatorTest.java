package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
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
 * Saga state crypto validation (Spring): startup fails when a registered saga carries an
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
  private static ObjectProvider<SagaRunner<?>> runnerProvider(List<SagaRunner<?>> runners) {
    ObjectProvider<SagaRunner<?>> provider = mock(ObjectProvider.class);
    when(provider.orderedStream()).thenAnswer(inv -> runners.stream());
    return provider;
  }

  @SuppressWarnings("unchecked")
  private static ObjectProvider<CryptoEngine> engineProvider(CryptoEngine engine) {
    ObjectProvider<CryptoEngine> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(engine);
    return provider;
  }

  @Test
  void startup_fails_whenEncryptedSagaStateAndNoEngine() {
    var validator =
        new SagaStateCryptoValidator(
            runnerProvider(
                List.of(
                    runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com")))),
            engineProvider(null));

    assertThatThrownBy(validator::afterSingletonsInstantiated)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(EncryptedSagaState.class.getName() + ".customerEmail");
  }

  @Test
  void startup_succeeds_whenEncryptedSagaStateAndEngine() {
    var validator =
        new SagaStateCryptoValidator(
            runnerProvider(
                List.of(
                    runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com")))),
            engineProvider(mock(CryptoEngine.class)));

    assertThatCode(validator::afterSingletonsInstantiated).doesNotThrowAnyException();
  }

  @Test
  void startup_succeeds_whenNoEncryptedSagaState() {
    var validator =
        new SagaStateCryptoValidator(
            runnerProvider(List.of(runnerFor(PlainSagaState.class, new PlainSagaState("o")))),
            engineProvider(null));

    assertThatCode(validator::afterSingletonsInstantiated).doesNotThrowAnyException();
  }

  @Test
  @SuppressWarnings("unchecked")
  void startup_fails_whenMultipleAmbiguousCryptoEngines() {
    ObjectProvider<CryptoEngine> ambiguous = mock(ObjectProvider.class);
    when(ambiguous.getIfAvailable())
        .thenThrow(new NoUniqueBeanDefinitionException(CryptoEngine.class));
    var validator =
        new SagaStateCryptoValidator(
            runnerProvider(
                List.of(
                    runnerFor(EncryptedSagaState.class, new EncryptedSagaState("c", "pii@x.com")))),
            ambiguous);

    assertThatThrownBy(validator::afterSingletonsInstantiated)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("@Primary");
  }
}
