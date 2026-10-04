package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Default saga event-path retention wiring (Micronaut). {@code streamrune.inbox.retention-max-age}
 * must reach the event path's key-age guard on an application-constructed {@link SagaRunner} bean,
 * exactly as it already reaches the compensation-retry sweeper.
 */
class SagaEventPathRetentionWiringTest {

  record FakeState(SagaStatus status, String id) implements SagaState {}

  static final class FakeOrchestrator implements SagaOrchestrator<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("x");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
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
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
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

  private static SagaRunner<FakeState> sagaRunner() {
    var sagaStore = new InMemorySagaStore();
    return SagaRunner.<FakeState>builder()
        .orchestrator(new FakeOrchestrator())
        .sagaStore(sagaStore)
        .commandBus(NOOP_BUS)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
        .build();
  }

  private static SagaRunner<FakeState> sagaRunnerWithExplicitWindow(Duration window) {
    var sagaStore = new InMemorySagaStore();
    return SagaRunner.<FakeState>builder()
        .orchestrator(new FakeOrchestrator())
        .sagaStore(sagaStore)
        .commandBus(NOOP_BUS)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
        .inboxRetentionMaxAge(window)
        .build();
  }

  @Test
  void configurerBeanIsRegistered() {
    try (var ctx = ApplicationContext.builder().start()) {
      assertTrue(ctx.containsBean(SagaEventPathRetentionConfigurer.class));
    }
  }

  @Test
  void configuredInboxRetention_reachesTheEventPathGuard() {
    var runner = sagaRunner();
    // Real bound StreamRuneMicronautProperties from the container — the wiring gap this closes is
    // invisible to a hand-built runner.
    try (var ctx =
        ApplicationContext.builder()
            .properties(
                Map.of(
                    "streamrune.inbox.retention-max-age", "3d",
                    // The saga dead-letter window must not exceed the inbox window.
                    "streamrune.saga.dead-letter-retention-max-age", "1d"))
            .start()) {
      ctx.getBean(SagaEventPathRetentionConfigurer.class).apply(List.of(runner));
    }
    assertEquals(Duration.ofDays(3), SagaEventPathRetention.effectiveWindowOf(runner));
  }

  @Test
  void defaultInboxRetention_reachesTheEventPathGuard() {
    var runner = sagaRunner();
    try (var ctx = ApplicationContext.builder().start()) {
      ctx.getBean(SagaEventPathRetentionConfigurer.class).apply(List.of(runner));
    }
    assertEquals(Duration.ofDays(7), SagaEventPathRetention.effectiveWindowOf(runner));
  }

  @Test
  void disabledInboxRetention_leavesTheGuardInert() {
    var runner = sagaRunner();
    try (var ctx =
        ApplicationContext.builder()
            .properties(Map.of("streamrune.inbox.retention-max-age", "0s"))
            .start()) {
      ctx.getBean(SagaEventPathRetentionConfigurer.class).apply(List.of(runner));
    }
    assertNull(SagaEventPathRetention.effectiveWindowOf(runner));
  }

  @Test
  void applicationSuppliedWindowIsNeverOverwritten() {
    var runner = sagaRunnerWithExplicitWindow(Duration.ofHours(2));
    try (var ctx =
        ApplicationContext.builder()
            .properties(
                Map.of(
                    "streamrune.inbox.retention-max-age", "3d",
                    "streamrune.saga.dead-letter-retention-max-age", "1d"))
            .start()) {
      ctx.getBean(SagaEventPathRetentionConfigurer.class).apply(List.of(runner));
    }
    assertEquals(Duration.ofHours(2), SagaEventPathRetention.effectiveWindowOf(runner));
  }
}
