package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Default wiring. The event-path key-age guard only activates when the runner knows {@code
 * streamrune.inbox.retention-max-age}; a {@code SagaRunner} is application-constructed, so without
 * an auto-configured default a deployment that sets the property got the guard on the sweeper and
 * NOT on the event path — the one re-driver with no leadership gate, running on every replica.
 *
 * <p>Exercised through the real auto-configuration context, because a hand-built {@code SagaRunner}
 * cannot observe a wiring gap.
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

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
          .withBean(SagaStore.class, InMemorySagaStore::new);

  @Test
  @SuppressWarnings("unchecked")
  void configuredInboxRetention_reachesTheEventPathGuard() {
    runner
        .withPropertyValues(
            "streamrune.inbox.retention-max-age=3d",
            // The dead-letter window must not exceed the inbox window (validated at
            // startup), so lower it with the inbox window rather than leaving the 7d default.
            "streamrune.saga.dead-letter-retention-max-age=1d")
        .withBean("orderSagaRunner", SagaRunner.class, SagaEventPathRetentionWiringTest::sagaRunner)
        .run(
            ctx ->
                assertThat(
                        SagaEventPathRetention.effectiveWindowOf(
                            (SagaRunner<FakeState>) ctx.getBean("orderSagaRunner")))
                    .as(
                        "a deployment that sets streamrune.inbox.retention-max-age must get the"
                            + " event-path guard automatically — it is the only re-driver with no"
                            + " leadership gate")
                    .isEqualTo(Duration.ofDays(3)));
  }

  @Test
  @SuppressWarnings("unchecked")
  void defaultInboxRetention_reachesTheEventPathGuard() {
    runner
        .withBean("orderSagaRunner", SagaRunner.class, SagaEventPathRetentionWiringTest::sagaRunner)
        .run(
            ctx ->
                assertThat(
                        SagaEventPathRetention.effectiveWindowOf(
                            (SagaRunner<FakeState>) ctx.getBean("orderSagaRunner")))
                    .isEqualTo(Duration.ofDays(7)));
  }

  @Test
  @SuppressWarnings("unchecked")
  void disabledInboxRetention_leavesTheGuardInert() {
    // Zero/negative is the documented "pruning disabled" value: keys are never swept, so the
    // key-age guard has nothing to protect against and must be inert — NOT "refuse everything
    // older than zero", which would FAULT every compensating resume.
    runner
        .withPropertyValues("streamrune.inbox.retention-max-age=0s")
        .withBean("orderSagaRunner", SagaRunner.class, SagaEventPathRetentionWiringTest::sagaRunner)
        .run(
            ctx ->
                assertThat(
                        SagaEventPathRetention.effectiveWindowOf(
                            (SagaRunner<FakeState>) ctx.getBean("orderSagaRunner")))
                    .isNull());
  }

  @Test
  @SuppressWarnings("unchecked")
  void applicationSuppliedWindowIsNeverOverwritten() {
    runner
        .withPropertyValues(
            "streamrune.inbox.retention-max-age=3d",
            // The dead-letter window must not exceed the inbox window (validated at
            // startup), so lower it with the inbox window rather than leaving the 7d default.
            "streamrune.saga.dead-letter-retention-max-age=1d")
        .withBean(
            "orderSagaRunner",
            SagaRunner.class,
            () -> sagaRunnerWithExplicitWindow(Duration.ofHours(2)))
        .run(
            ctx ->
                assertThat(
                        SagaEventPathRetention.effectiveWindowOf(
                            (SagaRunner<FakeState>) ctx.getBean("orderSagaRunner")))
                    .isEqualTo(Duration.ofHours(2)));
  }
}
