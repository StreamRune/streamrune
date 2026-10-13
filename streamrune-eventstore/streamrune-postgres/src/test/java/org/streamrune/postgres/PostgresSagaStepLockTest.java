package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SagaTimeoutRunnerTestAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * On {@link PostgresSagaStore}: a saga's deadline passes while a delivery of its start event is
 * between its read of the row and its forward command, and the timeout runner is built from the
 * {@code SagaRunner} that delivers. The poll made during the step leaves the row as it is; the step
 * stores its command and commits; the next poll claims, and the compensation is stored after the
 * forward command, from a row whose genesis is applied.
 *
 * <p>The delivery runs on a second thread and is held before its forward command by a latch; the
 * timeout runner polls once on the test's thread, through the package's test access.
 */
@Testcontainers
class PostgresSagaStepLockTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_step_lock");

  record Opened(String orderId) implements DomainEvent {}

  record Reserve(String orderId) implements Command {}

  record Release(String orderId) implements Command {}

  record Refund(String orderId) implements Command {}

  record Reserved(String orderId) implements DomainEvent {}

  record Released(String orderId) implements DomainEvent {}

  record Refunded(String orderId) implements DomainEvent {}

  record Untouched() implements AggregateState {}

  record State(SagaStatus status, String orderId) implements SagaState {}

  private static final String ORDER = "42";
  private static final SagaType TYPE = SagaType.fromClass(State.class);
  private static final Duration TIMEOUT = Duration.ofMinutes(5);

  /** Stores one event per command. */
  record OneEventDecider<C extends Command>(java.util.function.Function<C, DomainEvent> eventOf)
      implements Decider<C, Untouched, DomainEvent> {

    @Override
    public Untouched initialState() {
      return new Untouched();
    }

    @Override
    public List<DomainEvent> decide(C command, Untouched state) {
      return List.of(eventOf.apply(command));
    }

    @Override
    public Untouched evolve(Untouched state, DomainEvent event) {
      return state;
    }
  }

  /** The start event issues one forward command; the compensation is a release and a refund. */
  static final class OrderSaga implements SagaDecider<State> {
    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, null);
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return new State(SagaStatus.RUNNING, ORDER);
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return List.of(SagaCommand.of(new Reserve(ORDER), AggregateId.of(ORDER)));
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(new Release(ORDER), AggregateId.of(ORDER)),
          SagaCommand.of(new Refund(ORDER), AggregateId.of(ORDER)));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(TIMEOUT);
    }
  }

  /** Runs a hook once, right before the first {@link Reserve} reaches the real bus. */
  static final class HookedBus implements CommandBus {
    private final CommandBus delegate;
    private volatile Runnable beforeReserve;

    HookedBus(CommandBus delegate) {
      this.delegate = delegate;
    }

    void runOnceBeforeReserve(Runnable action) {
      beforeReserve = action;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return delegate.execute(command);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (beforeReserve != null && command instanceof Reserve) {
        Runnable once = beforeReserve;
        beforeReserve = null;
        once.run();
      }
      return delegate.execute(command, key);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return delegate.supportsIdempotentExecution();
    }
  }

  @Test
  void aStartStepIsInFlight_theTimeoutClaimIsNotMade_andTheNextPollCompensatesAfterTheStep()
      throws Exception {
    var dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    new PostgresEventStoreFactory(
            dataSource,
            new EventTypeRegistry() {
              @Override
              public Class<?> resolveEventType(EventType eventType) {
                return Object.class;
              }

              @Override
              public Class<?> resolveStateType(String stateType) {
                return Object.class;
              }

              @Override
              public Collection<Class<?>> registeredTypes() {
                return List.of();
              }
            })
        .create();
    SagaStore sagaStore =
        new PostgresSagaStore(dataSource, new ObjectMapper().registerModule(new JavaTimeModule()));
    var inbox = new InMemoryCommandInbox();
    var eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    SagaId sagaId = SagaId.of("order-saga-" + UUID.randomUUID());
    var saga = new OrderSaga();
    try (VirtualThreadCommandBus realBus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .register(
                AggregateType.of("reservation"),
                Reserve.class,
                c -> AggregateId.of(c.orderId()),
                new OneEventDecider<Reserve>(c -> new Reserved(c.orderId())))
            .register(
                AggregateType.of("release"),
                Release.class,
                c -> AggregateId.of(c.orderId()),
                new OneEventDecider<Release>(c -> new Released(c.orderId())))
            .register(
                AggregateType.of("refund"),
                Refund.class,
                c -> AggregateId.of(c.orderId()),
                new OneEventDecider<Refund>(c -> new Refunded(c.orderId())))
            .build()) {
      var bus = new HookedBus(realBus);
      SagaRunner<State> runner =
          SagaRunner.<State>builder()
              .decider(saga)
              .startWhen(e -> e.event() instanceof Opened)
              .extractSagaId(e -> sagaId)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
              .build();
      // The row's created_at is the database's now(); the runner's clock is past its deadline.
      SagaTimeoutRunner<State> timeoutRunner =
          SagaTimeoutRunner.<State>builder()
              .decider(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(TYPE)
              .clock(Clock.fixed(Instant.now().plus(TIMEOUT).plus(TIMEOUT), ZoneOffset.UTC))
              .sagaRunner(runner)
              .build();
      EventEnvelope opened = append(eventStore, new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      bus.runOnceBeforeReserve(
          () -> {
            stepAtItsCommand.countDown();
            awaitQuietly(letTheStepGo);
          });
      CompletableFuture<Void> step =
          CompletableFuture.runAsync(
              () -> runner.asEventListener().onEvents(List.of(opened)),
              command -> Thread.ofPlatform().start(command));
      assertThat(stepAtItsCommand.await(20, TimeUnit.SECONDS)).isTrue();

      SagaTimeoutRunnerTestAccess.pollOnce(timeoutRunner);

      LoadedSaga<State> underTheStep = sagaStore.load(sagaId, TYPE, State.class).orElseThrow();
      assertThat(underTheStep.status())
          .as("not claimed under the step")
          .isEqualTo(SagaStatus.RUNNING);
      assertThat(underTheStep.version()).isEqualTo(1L);
      assertThat(underTheStep.episodeVersion()).isNull();
      assertThat(storedOrder(eventStore)).isEmpty();

      letTheStepGo.countDown();
      step.get(20, TimeUnit.SECONDS); // the step's event write is not refused
      assertThat(storedOrder(eventStore)).containsExactly("Reserved");

      SagaTimeoutRunnerTestAccess.pollOnce(timeoutRunner);

      assertThat(storedOrder(eventStore)).containsExactly("Reserved", "Released", "Refunded");
      LoadedSaga<State> row = sagaStore.load(sagaId, TYPE, State.class).orElseThrow();
      assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(row.genesisApplied()).as("the claim follows the step's write").isTrue();
      assertThat(row.episodeVersion()).as("insert, event write, claim").isEqualTo(3L);
      assertThat(row.episodeCauseEventId()).as("a timeout claim has no cause").isNull();
    }
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      if (!latch.await(20, TimeUnit.SECONDS)) {
        throw new IllegalStateException("the test never released the step");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }

  private static List<String> storedOrder(InMemoryEventStore eventStore) {
    return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
        .map(e -> (DomainEvent) e.event())
        .filter(e -> e instanceof Reserved || e instanceof Released || e instanceof Refunded)
        .map(e -> e.getClass().getSimpleName())
        .toList();
  }

  private static EventEnvelope append(InMemoryEventStore eventStore, DomainEvent event) {
    StreamId streamId =
        StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + UUID.randomUUID()));
    var appended =
        eventStore.append(
            streamId,
            List.of(
                new EventEnvelope(
                    GlobalOffset.initial(),
                    streamId,
                    Version.initial(),
                    EventType.fromClass(event.getClass()),
                    event,
                    new EventMetadata(
                        EventId.of("evt-" + UUID.randomUUID()),
                        CommandId.of("cmd-" + UUID.randomUUID()),
                        null,
                        null,
                        CorrelationId.of("request-" + UUID.randomUUID()),
                        null,
                        null,
                        Instant.now()))),
            Version.initial());
    return eventStore
        .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
        .get(0);
  }
}
