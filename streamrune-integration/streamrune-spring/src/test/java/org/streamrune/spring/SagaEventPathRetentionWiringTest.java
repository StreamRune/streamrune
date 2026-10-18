package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaForwardFenceDefault;
import org.streamrune.runtime.SagaLeaseWiring;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.SagaEnvelopes;

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

  static class FakeOrchestrator implements SagaOrchestrator<FakeState> {
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
        // The bus is a test double that stores nothing; the fence is tested in
        // SagaForwardFence*Test.
        .forwardFence(false)
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
        // The bus is a test double that stores nothing; the fence is tested in
        // SagaForwardFence*Test.
        .forwardFence(false)
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

  @Test
  @SuppressWarnings("unchecked")
  void aSagaRunnerBeanWithAndWithoutAnEventDataHook_isWiredAndRoutesAsItsSagaDeclares() {
    var withoutAHook = RegisteredSaga.of(new FakeOrchestrator());
    var withAHook = RegisteredSaga.of(new EventDataRoutingOrchestrator());
    runner
        .withBean("plainSagaRunner", SagaRunner.class, withoutAHook::runner)
        .withBean("routingSagaRunner", SagaRunner.class, withAHook::runner)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              for (String bean : List.of("plainSagaRunner", "routingSagaRunner")) {
                assertThat(
                        SagaEventPathRetention.effectiveWindowOf(
                            (SagaRunner<FakeState>) ctx.getBean(bean)))
                    .isEqualTo(Duration.ofDays(7));
              }
              assertEachRoutesAsItsSagaDeclares(withoutAHook, withAHook);
            });
  }

  /**
   * A bus that, like the one over a fencing event store, answers the saga rows its forward commands
   * are fenced against.
   */
  private static CommandBus fencingBus(InMemoryEventStore eventStore) {
    return new CommandBus() {
      @Override
      public boolean supportsIdempotentExecution() {
        return true;
      }

      @Override
      public Object sagaFenceStorageIdentity() {
        return eventStore.sagaFenceStorageIdentity();
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
  }

  /** A runner over a store the event store fences against; {@code named} null names no fence. */
  private static SagaRunner<FakeState> fencedRunner(Boolean named) {
    var sagaStore = new InMemorySagaStore();
    var eventStore =
        new InMemoryEventStore()
            .withCommandInbox(new InMemoryCommandInbox())
            .withSagaStore(sagaStore);
    var builder =
        SagaRunner.<FakeState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(fencingBus(eventStore))
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore));
    if (named != null) {
      builder.forwardFence(named);
    }
    return builder.build();
  }

  @Test
  void auto_withSingleActiveConsumer_fencesARunnerThatNamedNothing() {
    runner
        .withBean("orderSagaRunner", SagaRunner.class, () -> fencedRunner(null))
        .run(ctx -> assertThat(fenceOf(ctx.getBean("orderSagaRunner"))).isTrue());
  }

  @Test
  void auto_withoutSingleActiveConsumer_switchesItOff() {
    runner
        .withPropertyValues("streamrune.subscription.single-active-consumer.enabled=false")
        .withBean("orderSagaRunner", SagaRunner.class, () -> fencedRunner(null))
        .run(ctx -> assertThat(fenceOf(ctx.getBean("orderSagaRunner"))).isFalse());
  }

  @Test
  void on_fences() {
    runner
        .withPropertyValues(
            "streamrune.subscription.single-active-consumer.enabled=false",
            "streamrune.saga.forward-fence=on")
        .withBean("orderSagaRunner", SagaRunner.class, () -> fencedRunner(null))
        .run(ctx -> assertThat(fenceOf(ctx.getBean("orderSagaRunner"))).isTrue());
  }

  @Test
  void off_switchesItOff() {
    runner
        .withPropertyValues("streamrune.saga.forward-fence=off")
        .withBean("orderSagaRunner", SagaRunner.class, () -> fencedRunner(null))
        .run(ctx -> assertThat(fenceOf(ctx.getBean("orderSagaRunner"))).isFalse());
  }

  @Test
  void aRunnerThatNamedItsFenceKeepsIt() {
    runner
        .withPropertyValues("streamrune.saga.forward-fence=off")
        .withBean("fencedRunner", SagaRunner.class, () -> fencedRunner(true))
        .run(ctx -> assertThat(fenceOf(ctx.getBean("fencedRunner"))).isTrue());
    runner
        .withPropertyValues("streamrune.saga.forward-fence=on")
        .withBean("unfencedRunner", SagaRunner.class, () -> fencedRunner(false))
        .run(ctx -> assertThat(fenceOf(ctx.getBean("unfencedRunner"))).isFalse());
  }

  @Test
  void anUnknownValueFailsStartup() {
    runner
        .withPropertyValues("streamrune.saga.forward-fence=maybe")
        .withBean("orderSagaRunner", SagaRunner.class, () -> fencedRunner(null))
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining(
                        "streamrune.saga.forward-fence must be auto, on or off")
                    .hasStackTraceContaining("got: 'maybe'"));
  }

  /**
   * A runner built without a lease, next to a real leadership bean, delivers its saga type on every
   * replica: the startup hook says so, once per such runner.
   */
  @Test
  void anUngatedRunnerUnderRealLeadershipIsReported() {
    var log = (Logger) LoggerFactory.getLogger(SagaLeaseWiring.class);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    log.addAppender(appender);
    try {
      runner
          .withBean(SubscriptionLeadership.class, SagaEventPathRetentionWiringTest::aLeadership)
          .withBean(
              "orderSagaRunner", SagaRunner.class, SagaEventPathRetentionWiringTest::sagaRunner)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(appender.list)
                    .filteredOn(e -> e.getLevel() == Level.WARN)
                    .singleElement()
                    .satisfies(
                        e ->
                            assertThat(e.getFormattedMessage())
                                .contains(FakeState.class.getName())
                                .contains("is delivered on every replica"));
              });
    } finally {
      log.detachAppender(appender);
    }
  }

  @Test
  void nothingIsReportedWithoutLeadership() {
    var log = (Logger) LoggerFactory.getLogger(SagaLeaseWiring.class);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    log.addAppender(appender);
    try {
      runner
          .withPropertyValues("streamrune.subscription.single-active-consumer.enabled=false")
          .withBean(
              "orderSagaRunner", SagaRunner.class, SagaEventPathRetentionWiringTest::sagaRunner)
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(ctx).doesNotHaveBean(SubscriptionLeadership.class);
                assertThat(appender.list).isEmpty();
              });
    } finally {
      log.detachAppender(appender);
    }
  }

  /**
   * A leadership that is not {@link SubscriptionLeadership#NOOP}: holds nothing, grants nothing.
   */
  private static SubscriptionLeadership aLeadership() {
    return new SubscriptionLeadership() {
      @Override
      public Optional<Lease> tryAcquire(String consumerName) {
        return Optional.empty();
      }

      @Override
      public Optional<Lease> current(String consumerName) {
        return Optional.empty();
      }

      @Override
      public void resign(String consumerName) {}

      @Override
      public void close() {}
    };
  }

  private static boolean fenceOf(Object sagaRunnerBean) {
    return SagaForwardFenceDefault.effectiveOf((SagaRunner<?>) sagaRunnerBean);
  }

  /**
   * Two runner beans over one saga state class and one store are one saga type with two sets of
   * logic: the context does not start, and the failure names the state class and both sagas.
   */
  @Test
  void twoSagaRunnerBeansOverOneStateClassAndOneStore_stopTheContext() {
    var store = new InMemorySagaStore();
    runner
        .withBean(
            "firstSagaRunner", SagaRunner.class, () -> runnerOver(store, new FakeOrchestrator()))
        .withBean(
            "secondSagaRunner",
            SagaRunner.class,
            () -> runnerOver(store, new EventDataRoutingOrchestrator()))
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("the saga type " + FakeState.class.getName())
                    .hasMessageContaining(FakeOrchestrator.class.getName())
                    .hasMessageContaining(EventDataRoutingOrchestrator.class.getName()));
  }

  /** The same state class over two stores shares no row: both beans are wired. */
  @Test
  void twoSagaRunnerBeansOverOneStateClassAndTwoStores_start() {
    runner
        .withBean(
            "firstSagaRunner",
            SagaRunner.class,
            () -> runnerOver(new InMemorySagaStore(), new FakeOrchestrator()))
        .withBean(
            "secondSagaRunner",
            SagaRunner.class,
            () -> runnerOver(new InMemorySagaStore(), new FakeOrchestrator()))
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  private static SagaRunner<FakeState> runnerOver(
      InMemorySagaStore store, SagaOrchestrator<FakeState> orchestrator) {
    return SagaRunner.<FakeState>builder()
        .orchestrator(orchestrator)
        .sagaStore(store)
        .commandBus(NOOP_BUS)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
        // The bus is a test double that stores nothing; the fence is tested in
        // SagaForwardFence*Test.
        .forwardFence(false)
        .build();
  }

  /** An event no saga caused; it names the saga it belongs to in its own data. */
  record Nudged(String sagaId) implements DomainEvent {}

  /** {@link FakeOrchestrator} that routes a {@link Nudged} it did not cause by the event's data. */
  static final class EventDataRoutingOrchestrator extends FakeOrchestrator {
    @Override
    public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
      return event instanceof Nudged nudged
          ? Optional.of(SagaId.of(nudged.sagaId()))
          : Optional.empty();
    }
  }

  /** A runner over a store of its own that holds one running saga, {@code x}. */
  private record RegisteredSaga(SagaRunner<FakeState> runner, InMemorySagaStore store) {
    static RegisteredSaga of(SagaOrchestrator<FakeState> orchestrator) {
      var store = new InMemorySagaStore();
      store.create(
          SagaId.of("x"),
          SagaType.fromClass(FakeState.class),
          new FakeState(SagaStatus.RUNNING, "x"),
          SagaStatus.RUNNING);
      return new RegisteredSaga(
          SagaRunner.<FakeState>builder()
              .orchestrator(orchestrator)
              .sagaStore(store)
              .commandBus(NOOP_BUS)
              .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
              // The bus is a test double that stores nothing; the fence is tested in
              // SagaForwardFence*Test.
              .forwardFence(false)
              .build(),
          store);
    }

    long versionAfter(EventEnvelope event) {
      runner.asEventListener().onEvents(List.of(event));
      return store
          .load(SagaId.of("x"), SagaType.fromClass(FakeState.class), FakeState.class)
          .orElseThrow()
          .version();
    }
  }

  private static EventEnvelope nudged(long offset, EventMetadata metadata) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(AggregateType.of("nudge"), AggregateId.of("n-1")),
        Version.initial(),
        EventType.fromClass(Nudged.class),
        new Nudged("x"),
        metadata);
  }

  /** Metadata of an event a request stored: no saga origin, the saga's id as the correlation id. */
  private static EventMetadata storedByARequestThatNamesTheSagaAsItsCorrelationId() {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of("x"),
        null,
        null,
        Instant.now());
  }

  /**
   * After the wiring, each registered runner routes as its saga declares: an event that no saga
   * caused reaches only the saga that routes it by its data, and the reply to a saga's own command
   * reaches the saga by its origin with or without that routing.
   */
  private static void assertEachRoutesAsItsSagaDeclares(
      RegisteredSaga withoutAHook, RegisteredSaga withAHook) {
    EventEnvelope ofAnotherCause = nudged(5L, storedByARequestThatNamesTheSagaAsItsCorrelationId());
    assertEquals(1L, withoutAHook.versionAfter(ofAnotherCause), "no hook: the event is no saga's");
    assertEquals(2L, withAHook.versionAfter(ofAnotherCause), "the hook routes it by its data");

    EventEnvelope ownReply =
        nudged(6L, SagaEnvelopes.metadataCausedBy(FakeState.class, SagaId.of("x")));
    assertEquals(2L, withoutAHook.versionAfter(ownReply), "the origin routes the saga's own reply");
    assertEquals(3L, withAHook.versionAfter(ownReply), "the origin routes the saga's own reply");
  }
}
