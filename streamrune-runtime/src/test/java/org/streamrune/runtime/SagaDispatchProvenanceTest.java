package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaDeadLetterReplayer.CompensateOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayer.ResumeOutcome;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * Every path on which a saga dispatches a command stores the saga's origin in the metadata of the
 * events that command produces: the same origin on the event path, the timeout runner, the
 * compensation-retry sweeper and the dead-letter replayer. Forward events name the triggering event
 * as their causation id; compensation events name none.
 *
 * <p>The commands run on the real {@link VirtualThreadCommandBus} over an in-memory event store and
 * command inbox, so what is asserted is what the bus stored.
 */
class SagaDispatchProvenanceTest {

  record Opened(String orderId) implements DomainEvent {}

  record Advanced(String orderId) implements DomainEvent {}

  record Reserve(String orderId) implements Command {}

  record Ship(String orderId) implements Command {}

  record Release(String orderId) implements Command {}

  record Reserved(String orderId) implements DomainEvent {}

  record Shipped(String orderId) implements DomainEvent {}

  record Released(String orderId) implements DomainEvent {}

  record Untouched() implements AggregateState {}

  record State(SagaStatus status, String orderId) implements SagaState {}

  private static final String ORDER = "42";
  private static final SagaId SAGA = SagaId.of("order-saga-" + ORDER);
  private static final SagaType TYPE = SagaType.fromClass(State.class);
  private static final SagaOrigin ORIGIN = new SagaOrigin(TYPE, SAGA, 0);
  private static final Duration TIMEOUT = Duration.ofMinutes(5);
  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(30);
  private static final Duration WINDOW = Duration.ofDays(7);

  /** Stores one event per command; {@code failure}, while set, makes {@code decide} throw. */
  static final class OneEventDecider<C extends Command>
      implements Decider<C, Untouched, DomainEvent> {
    private final Function<C, DomainEvent> eventOf;
    volatile Supplier<RuntimeException> failure;

    OneEventDecider(Function<C, DomainEvent> eventOf) {
      this.eventOf = eventOf;
    }

    @Override
    public Untouched initialState() {
      return new Untouched();
    }

    @Override
    public List<DomainEvent> decide(C command, Untouched state) {
      Supplier<RuntimeException> failing = failure;
      if (failing != null) {
        throw failing.get();
      }
      return List.of(eventOf.apply(command));
    }

    @Override
    public Untouched evolve(Untouched state, DomainEvent event) {
      return state;
    }
  }

  /** Reserve on the start event, ship on the correlated event, release to compensate. */
  static final class OrderSagaDecider implements SagaDecider<State> {
    volatile boolean poisonAdvanced;

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
      if (event.event() instanceof Advanced && poisonAdvanced) {
        throw new IllegalStateException("evolve refuses this event");
      }
      return new State(SagaStatus.RUNNING, ORDER);
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return switch (event.event()) {
        case Opened opened ->
            List.of(SagaCommand.of(new Reserve(opened.orderId()), AggregateId.of(ORDER)));
        case Advanced advanced ->
            List.of(SagaCommand.of(new Ship(advanced.orderId()), AggregateId.of(ORDER)));
        default -> List.of();
      };
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new Release(ORDER), AggregateId.of(ORDER)));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(TIMEOUT);
    }
  }

  static final class Harness implements AutoCloseable {
    final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    final InMemorySagaStore sagaStore = new InMemorySagaStore(clock);
    final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(sagaStore);
    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OrderSagaDecider saga = new OrderSagaDecider();
    final VirtualThreadCommandBus bus =
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
                reserve)
            .register(
                AggregateType.of("shipment"), Ship.class, c -> AggregateId.of(c.orderId()), ship)
            .register(
                AggregateType.of("release"),
                Release.class,
                c -> AggregateId.of(c.orderId()),
                release)
            .build();
    final SagaRunner<State> runner =
        SagaRunner.<State>builder()
            .decider(saga)
            .startWhen(e -> e.event() instanceof Opened)
            .extractSagaId(e -> SagaId.of("order-saga-" + ((Opened) e.event()).orderId()))
            .correlateBy(
                e ->
                    e.event() instanceof Advanced advanced
                        ? Optional.of(SagaId.of("order-saga-" + advanced.orderId()))
                        : Optional.empty())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetters)
            .inboxRetentionMaxAge(WINDOW)
            .clock(clock)
            .build();
    final SagaDeadLetterReplayer replayer =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(deadLetters)
            .sagaStore(sagaStore)
            .eventStore(eventStore)
            .sagaRunner(runner)
            .inboxRetentionMaxAge(WINDOW)
            .clock(clock)
            .build();

    /** Appends a trigger event as a request stores it: no origin, a correlation id of its own. */
    EventEnvelope append(DomainEvent event) {
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

    void deliver(EventEnvelope event) {
      runner.asEventListener().onEvents(List.of(event));
    }

    /** Delivers the start event; the saga is RUNNING with its reservation stored. */
    EventEnvelope open() {
      EventEnvelope opened = append(new Opened(ORDER));
      deliver(opened);
      assertThat(row().status()).isEqualTo(SagaStatus.RUNNING);
      return opened;
    }

    LoadedSaga<State> row() {
      return sagaStore.load(SAGA, TYPE, State.class).orElseThrow();
    }

    /** The stored events of {@code type}, in global order. */
    List<EventEnvelope> stored(Class<? extends DomainEvent> type) {
      return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .filter(e -> type.isInstance(e.event()))
          .toList();
    }

    @Override
    public void close() {
      bus.close();
    }
  }

  private static void assertForwardEvent(EventEnvelope produced, EventEnvelope trigger) {
    assertThat(produced.metadata().sagaOrigin()).isEqualTo(ORIGIN);
    assertThat(produced.metadata().correlationId()).isEqualTo(CorrelationId.of(SAGA.value()));
    assertThat(produced.metadata().causationId())
        .isEqualTo(CausationId.of(trigger.metadata().eventId().value()));
  }

  private static void assertCompensationEvent(EventEnvelope produced) {
    assertThat(produced.metadata().sagaOrigin()).isEqualTo(ORIGIN);
    assertThat(produced.metadata().correlationId()).isEqualTo(CorrelationId.of(SAGA.value()));
    assertThat(produced.metadata().causationId()).isNull();
  }

  @Test
  void startEvent() {
    try (var h = new Harness()) {
      EventEnvelope opened = h.open();

      assertThat(opened.metadata().sagaOrigin()).as("the trigger itself has no origin").isNull();
      assertThat(h.stored(Reserved.class))
          .singleElement()
          .satisfies(reserved -> assertForwardEvent(reserved, opened));
    }
  }

  @Test
  void correlatedEvent() {
    try (var h = new Harness()) {
      h.open();
      EventEnvelope advanced = h.append(new Advanced(ORDER));

      h.deliver(advanced);

      assertThat(h.stored(Shipped.class))
          .singleElement()
          .satisfies(shipped -> assertForwardEvent(shipped, advanced));
    }
  }

  @Test
  void eventPathCompensation() {
    try (var h = new Harness()) {
      h.open();
      h.ship.failure = () -> new IllegalArgumentException("address rejected");

      h.deliver(h.append(new Advanced(ORDER)));

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.stored(Shipped.class)).isEmpty();
      assertThat(h.stored(Released.class))
          .singleElement()
          .satisfies(SagaDispatchProvenanceTest::assertCompensationEvent);
    }
  }

  @Test
  void timeoutClaim() {
    try (var h = new Harness()) {
      h.open();
      SagaTimeoutRunner<State> timeoutRunner =
          SagaTimeoutRunner.<State>builder()
              .decider(h.saga)
              .sagaStore(h.sagaStore)
              .commandBus(h.bus)
              .sagaType(TYPE)
              .clock(h.clock)
              .build();
      h.clock.advance(TIMEOUT.plusSeconds(1));

      timeoutRunner.pollOnce();

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.stored(Released.class))
          .singleElement()
          .satisfies(SagaDispatchProvenanceTest::assertCompensationEvent);
    }
  }

  @Test
  void sweeperResume() {
    try (var h = new Harness()) {
      leftCompensating(h);
      SagaCompensationRetrySweeper<State> sweeper =
          SagaCompensationRetrySweeper.<State>builder()
              .sagaRunner(h.runner)
              .sagaStore(h.sagaStore)
              .leadership(SubscriptionLeadership.NOOP)
              .retryInterval(RETRY_INTERVAL)
              .giveUpAfter(Duration.ofHours(1))
              .clock(h.clock)
              .build();
      h.clock.advance(RETRY_INTERVAL.plusSeconds(1));

      sweeper.sweepOnce();

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.stored(Released.class))
          .singleElement()
          .satisfies(SagaDispatchProvenanceTest::assertCompensationEvent);
    }
  }

  @Test
  void replayFeed() {
    try (var h = new Harness()) {
      h.open();
      EventEnvelope advanced = h.append(new Advanced(ORDER));
      h.saga.poisonAdvanced = true;
      h.deliver(advanced);
      assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(h.deadLetters.findBySaga(SAGA)).hasSize(1);
      assertThat(h.stored(Shipped.class)).isEmpty();
      h.saga.poisonAdvanced = false;

      h.replayer.replayAll(SAGA);

      assertThat(h.deadLetters.findBySaga(SAGA)).isEmpty();
      assertThat(h.stored(Shipped.class))
          .singleElement()
          .satisfies(shipped -> assertForwardEvent(shipped, advanced));
    }
  }

  @Test
  void resumeFaulted() {
    try (var h = new Harness()) {
      leftCompensating(h);
      assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
      assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);

      assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.stored(Released.class))
          .singleElement()
          .satisfies(SagaDispatchProvenanceTest::assertCompensationEvent);
    }
  }

  @Test
  void compensateFaulted() {
    try (var h = new Harness()) {
      h.open();
      h.saga.poisonAdvanced = true;
      h.deliver(h.append(new Advanced(ORDER)));
      assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
      var entry = h.deadLetters.findBySaga(SAGA).get(0);
      assertThat(h.replayer.discard(SAGA, entry.eventOffset())).isTrue();

      assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.TERMINAL);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.stored(Released.class))
          .singleElement()
          .satisfies(SagaDispatchProvenanceTest::assertCompensationEvent);
    }
  }

  /**
   * The shipment is rejected, so the event path claims the compensation episode; the release then
   * fails once on infrastructure and the saga is left COMPENSATING with nothing released.
   */
  private static void leftCompensating(Harness h) {
    h.open();
    h.ship.failure = () -> new IllegalArgumentException("address rejected");
    h.release.failure = () -> new IllegalStateException("connection reset");
    h.deliver(h.append(new Advanced(ORDER)));
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.stored(Released.class)).isEmpty();
    h.release.failure = null;
  }
}
