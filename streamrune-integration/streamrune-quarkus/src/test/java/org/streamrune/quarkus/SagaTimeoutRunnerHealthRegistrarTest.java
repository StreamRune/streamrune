package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.test.InMemorySagaStore;

/**
 * The SagaTimeoutRunner liveness registration lives in an always-present registrar, NOT inside the
 * compensation-retry sweeper lifecycle, so a timeout runner's dead-thread signal survives even when
 * the sweeper is disabled (the documented sole-driver config) — matching Spring and Micronaut.
 */
class SagaTimeoutRunnerHealthRegistrarTest {

  record FakeState(SagaStatus status) implements SagaState {}

  static final class FakeDecider implements SagaDecider<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED);
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofHours(1));
    }
  }

  static final CommandBus NOOP_BUS =
      new CommandBus() {
        @Override
        public boolean supportsIdempotentExecution() {
          return true;
        }

        @Override
        public <C extends Command> CommandResult execute(C command) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          throw new UnsupportedOperationException();
        }
      };

  private static SagaTimeoutRunner<FakeState> timeoutRunner() {
    return SagaTimeoutRunner.<FakeState>builder()
        .decider(new FakeDecider())
        .sagaStore(new InMemorySagaStore())
        .commandBus(NOOP_BUS)
        .sagaType(SagaType.fromClass(FakeState.class))
        .build();
  }

  @Test
  @SuppressWarnings("unchecked")
  void registersTimeoutRunnerHealth_independentlyOfTheSweeper() {
    var contributor = new BackgroundRelayHealthContributor();
    Instance<BackgroundRelayHealthContributor> relayHealthInstance = mock(Instance.class);
    when(relayHealthInstance.isUnsatisfied()).thenReturn(false);
    when(relayHealthInstance.get()).thenReturn(contributor);

    var registrar =
        new SagaTimeoutRunnerHealthRegistrar(List.of(timeoutRunner()), relayHealthInstance);
    registrar.registerTimeoutRunnerHealth();

    assertTrue(
        contributor.components().stream()
            .anyMatch(
                c ->
                    c.name().equals("saga-timeout:" + SagaType.fromClass(FakeState.class).value())),
        "the timeout runner must be health-registered even with the sweeper disabled");
  }

  @Test
  @SuppressWarnings("unchecked")
  void noOp_whenNoHealthContributor() {
    Instance<BackgroundRelayHealthContributor> relayHealthInstance = mock(Instance.class);
    when(relayHealthInstance.isUnsatisfied()).thenReturn(true);

    var registrar =
        new SagaTimeoutRunnerHealthRegistrar(List.of(timeoutRunner()), relayHealthInstance);
    // No health contributor bean → nothing to register into; must not throw, and must not try to
    // resolve the unsatisfied instance.
    assertDoesNotThrow(registrar::registerTimeoutRunnerHealth);
    verify(relayHealthInstance, never()).get();
  }
}
