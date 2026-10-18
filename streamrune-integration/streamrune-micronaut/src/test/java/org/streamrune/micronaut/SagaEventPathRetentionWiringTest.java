package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micronaut.context.ApplicationContext;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
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
 * Default saga event-path retention wiring (Micronaut). {@code streamrune.inbox.retention-max-age}
 * must reach the event path's key-age guard on an application-constructed {@link SagaRunner} bean,
 * exactly as it already reaches the compensation-retry sweeper.
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
    try (var ctx = ApplicationContext.builder().build()) {
      ctx.registerSingleton(SubscriptionLeadership.class, aLeadership());
      ctx.start();

      ctx.getBean(SagaEventPathRetentionConfigurer.class).apply(List.of(sagaRunner()));

      var warnings = appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
      assertEquals(1, warnings.size());
      assertTrue(warnings.getFirst().getFormattedMessage().contains(FakeState.class.getName()));
      assertTrue(
          warnings.getFirst().getFormattedMessage().contains("is delivered on every replica"));
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
    try (var ctx = ApplicationContext.builder().start()) {
      ctx.getBean(SagaEventPathRetentionConfigurer.class).apply(List.of(sagaRunner()));

      assertTrue(appender.list.isEmpty());
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

  @Test
  void aSagaRunnerWithAndWithoutAnEventDataHook_isWiredAndRoutesAsItsSagaDeclares() {
    var withoutAHook = RegisteredSaga.of(new FakeOrchestrator());
    var withAHook = RegisteredSaga.of(new EventDataRoutingOrchestrator());
    try (var ctx = ApplicationContext.builder().start()) {
      ctx.getBean(SagaEventPathRetentionConfigurer.class)
          .apply(List.of(withoutAHook.runner(), withAHook.runner()));
    }
    assertEquals(
        Duration.ofDays(7), SagaEventPathRetention.effectiveWindowOf(withoutAHook.runner()));
    assertEquals(Duration.ofDays(7), SagaEventPathRetention.effectiveWindowOf(withAHook.runner()));

    assertEachRoutesAsItsSagaDeclares(withoutAHook, withAHook);
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

  private static void applyWith(Map<String, Object> properties, SagaRunner<?>... runners) {
    try (var ctx = ApplicationContext.builder().properties(properties).start()) {
      ctx.getBean(SagaEventPathRetentionConfigurer.class).apply(List.of(runners));
    }
  }

  @Test
  void auto_withSingleActiveConsumer_fencesARunnerThatNamedNothing() {
    var runner = fencedRunner(null);
    applyWith(Map.of(), runner);
    assertTrue(SagaForwardFenceDefault.effectiveOf(runner));
  }

  @Test
  void auto_withoutSingleActiveConsumer_switchesItOff() {
    var runner = fencedRunner(null);
    applyWith(Map.of("streamrune.subscription.single-active-consumer.enabled", "false"), runner);
    assertFalse(SagaForwardFenceDefault.effectiveOf(runner));
  }

  @Test
  void on_fences() {
    var runner = fencedRunner(null);
    applyWith(
        Map.of(
            "streamrune.subscription.single-active-consumer.enabled", "false",
            "streamrune.saga.forward-fence", "on"),
        runner);
    assertTrue(SagaForwardFenceDefault.effectiveOf(runner));
  }

  @Test
  void off_switchesItOff() {
    var runner = fencedRunner(null);
    applyWith(Map.of("streamrune.saga.forward-fence", "off"), runner);
    assertFalse(SagaForwardFenceDefault.effectiveOf(runner));
  }

  @Test
  void aRunnerThatNamedItsFenceKeepsIt() {
    var fenced = fencedRunner(true);
    applyWith(Map.of("streamrune.saga.forward-fence", "off"), fenced);
    assertTrue(SagaForwardFenceDefault.effectiveOf(fenced));

    var unfenced = fencedRunner(false);
    applyWith(Map.of("streamrune.saga.forward-fence", "on"), unfenced);
    assertFalse(SagaForwardFenceDefault.effectiveOf(unfenced));
  }

  @Test
  void anUnknownValueFailsStartup() {
    var runner = fencedRunner(null);
    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () -> applyWith(Map.of("streamrune.saga.forward-fence", "maybe"), runner));
    assertTrue(
        refused.getMessage().contains("streamrune.saga.forward-fence must be auto, on or off"));
    assertTrue(refused.getMessage().contains("got: 'maybe'"));
  }

  /**
   * Two runners over one saga state class and one store are one saga type with two sets of logic:
   * the configurer throws, and the failure names the state class and both sagas. The runners are
   * built by hand here; {@link SagaTypeOwnershipContainerWiringTest} declares them as beans and
   * starts the container.
   */
  @Test
  void twoSagaRunnersOverOneStateClassAndOneStore_areRefusedByTheConfigurer() {
    var store = new InMemorySagaStore();
    var runners =
        List.<SagaRunner<?>>of(
            runnerOver(store, new FakeOrchestrator()),
            runnerOver(store, new EventDataRoutingOrchestrator()));
    try (var ctx = ApplicationContext.builder().start()) {
      var configurer = ctx.getBean(SagaEventPathRetentionConfigurer.class);

      var refused = assertThrows(IllegalStateException.class, () -> configurer.apply(runners));

      assertTrue(refused.getMessage().contains("the saga type " + FakeState.class.getName()));
      assertTrue(refused.getMessage().contains(FakeOrchestrator.class.getName()));
      assertTrue(refused.getMessage().contains(EventDataRoutingOrchestrator.class.getName()));
    }
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
