package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
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
import org.streamrune.runtime.SagaStepLock;
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
 * On {@link PostgresSagaStore}: the timeout runner claims a saga while a delivery of its start
 * event is between its read of the row and its forward command. The command is stored after the
 * compensation, the delivery's event write loses its compare-and-swap, and the executor reports it:
 * one WARN and one {@code streamrune.saga.compensation_under_forward_step} sample. The stamp and
 * the genesis flag the report reads are the ones the PostgreSQL statements write.
 *
 * <p>The timeout runner polls once inside a hook, on the delivery's thread, right before the
 * forward command reaches the bus.
 */
@Testcontainers
class PostgresSagaCompensationUnderForwardStepSignalTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_late_forward");

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
    private Runnable beforeReserve;

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

  static final class ReportCountingMetrics implements StreamRuneMetrics {
    final List<String> reportedSagaTypes = new CopyOnWriteArrayList<>();

    @Override
    public void recordSagaCompensationUnderForwardStep(String sagaType) {
      reportedSagaTypes.add(sagaType);
    }
  }

  @Test
  void theTimeoutRunnerClaimsUnderAStartStep_theForwardCommandStoredAfterItIsReported() {
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
    var metrics = new ReportCountingMetrics();
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
              .metrics(metrics)
              .build();
      // The row's created_at is the database's now(); the runner's clock is past its deadline.
      SagaTimeoutRunner<State> timeoutRunner =
          SagaTimeoutRunner.<State>builder()
              .decider(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(TYPE)
              .clock(Clock.fixed(Instant.now().plus(TIMEOUT).plus(TIMEOUT), ZoneOffset.UTC))
              // A lock of its own: this timeout runner stands for one on another node, whose
              // claim the step lock of this node's runner does not hold back.
              .stepLock(new SagaStepLock())
              .build();
      EventEnvelope opened = append(eventStore, new Opened(ORDER));
      bus.runOnceBeforeReserve(() -> SagaTimeoutRunnerTestAccess.pollOnce(timeoutRunner));

      List<String> warnings =
          executorWarningsDuring(
              () ->
                  assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(opened)))
                      .isInstanceOf(OptimisticLockException.class));

      assertThat(storedOrder(eventStore)).containsExactly("Released", "Refunded", "Reserved");
      LoadedSaga<State> row = sagaStore.load(sagaId, TYPE, State.class).orElseThrow();
      assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(row.episodeVersion()).as("stamped by the timeout claim").isEqualTo(2L);
      assertThat(row.episodeCauseEventId()).as("a timeout claim has no cause").isNull();
      assertThat(row.genesisApplied()).as("the start event is not recorded").isFalse();
      assertThat(metrics.reportedSagaTypes).containsExactly(TYPE.value());
      assertThat(warnings)
          .filteredOn(line -> line.contains("underneath the forward step"))
          .singleElement()
          .satisfies(
              line -> {
                assertThat(line).contains(TYPE.value());
                assertThat(line).contains(sagaId.value());
                assertThat(line).contains("offset " + opened.globalOffset().value());
                assertThat(line).contains("executed 1 forward command");
              });
    }
  }

  private static List<String> executorWarningsDuring(Runnable body) {
    var logger =
        (ch.qos.logback.classic.Logger)
            LoggerFactory.getLogger("org.streamrune.runtime.SagaStepExecutor");
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
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
