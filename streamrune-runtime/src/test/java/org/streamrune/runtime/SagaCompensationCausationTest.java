package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
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
import org.streamrune.runtime.ForwardingSagaStore.SimulatedCrash;
import org.streamrune.runtime.SagaDeadLetterReplayer.CompensateOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayer.ResumeOutcome;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * The causation id of a saga's compensation events is the id of the event that opened the
 * compensation episode, and it is the same whichever path commits the compensation command: the
 * delivery that opened the episode, a redelivery, a later event, the timeout runner, the
 * compensation-retry sweeper or an operator. An episode opened with no event in hand (a timeout
 * claim, an operator's compensation of a faulted forward step) stores none, on every path.
 *
 * <p>The cause is recorded with the episode stamp, so each crash point of opening and resuming an
 * episode is covered here: before the claim is written, between the claim and the first
 * compensation command, between two compensation commands, between the last compensation command
 * and the terminal write, and a fault inside the episode.
 *
 * <p>The commands run on the real {@link VirtualThreadCommandBus} over an in-memory event store and
 * command inbox, so what is asserted is what the bus stored. No test waits on a clock or a polling
 * loop: the runners are driven by their single-pass methods.
 */
class SagaCompensationCausationTest {

  record Opened(String orderId) implements DomainEvent {}

  /** A correlated event whose forward command is {@link Ship}. */
  record Advanced(String orderId) implements DomainEvent {}

  /** A correlated event the saga reacts to with no command. */
  record Nudged(String orderId) implements DomainEvent {}

  /** A correlated event whose evolve yields {@code COMPENSATING}. */
  record Cancelled(String orderId) implements DomainEvent {}

  record Reserve(String orderId) implements Command {}

  record Ship(String orderId) implements Command {}

  record Release(String orderId) implements Command {}

  record Refund(String orderId) implements Command {}

  record Reserved(String orderId) implements DomainEvent {}

  record Shipped(String orderId) implements DomainEvent {}

  record Released(String orderId) implements DomainEvent {}

  record Refunded(String orderId) implements DomainEvent {}

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

  /**
   * Reserve on the start event, ship on {@link Advanced}; compensation is two commands, a release
   * and a refund. {@link Cancelled}, and the start event while {@code startsCompensating} is set,
   * evolve to {@code COMPENSATING} and dispatch nothing.
   */
  static final class OrderSagaDecider implements SagaDecider<State> {
    volatile boolean startsCompensating;
    volatile boolean poisonAdvanced;
    volatile boolean poisonCompensation;

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
      boolean compensating =
          event.event() instanceof Cancelled
              || (event.event() instanceof Opened && startsCompensating);
      return new State(compensating ? SagaStatus.COMPENSATING : SagaStatus.RUNNING, ORDER);
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      if (state.status() == SagaStatus.COMPENSATING) {
        return List.of();
      }
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
      if (poisonCompensation) {
        throw new IllegalStateException("compensate refuses this state");
      }
      return List.of(
          SagaCommand.of(new Release(ORDER), AggregateId.of(ORDER)),
          SagaCommand.of(new Refund(ORDER), AggregateId.of(ORDER)));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(TIMEOUT);
    }
  }

  /** A forwarding store that can also fail a write before it reaches the delegate. */
  static final class CrashingSagaStore extends ForwardingSagaStore {
    private boolean failNextClaim;
    private boolean failNextTerminalWrite;

    CrashingSagaStore(InMemorySagaStore delegate) {
      super(delegate);
    }

    /** The next claim throws before anything is written. */
    void crashOnceBeforeTheClaim() {
      failNextClaim = true;
    }

    /** The next write of a terminal status throws before anything is written. */
    void crashOnceBeforeTheTerminalWrite() {
      failNextTerminalWrite = true;
    }

    @Override
    public void claimCompensating(
        SagaId id, SagaType type, SagaState state, long version, EventId episodeCause) {
      if (failNextClaim) {
        failNextClaim = false;
        throw new SimulatedCrash("nothing: the claim did not reach the store");
      }
      super.claimCompensating(id, type, state, version, episodeCause);
    }

    @Override
    public void update(SagaId id, SagaType type, SagaState state, SagaStatus status, long version) {
      if (failNextTerminalWrite && status.isTerminal()) {
        failNextTerminalWrite = false;
        throw new SimulatedCrash("nothing: the terminal write did not reach the store");
      }
      super.update(id, type, state, status, version);
    }
  }

  /** What a stored compensation event states about its cause. */
  record Provenance(
      String event, SagaOrigin origin, CausationId causationId, CorrelationId correlationId) {}

  static final class Harness implements AutoCloseable {
    final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    final InMemorySagaStore rows = new InMemorySagaStore(clock);
    final CrashingSagaStore sagaStore = new CrashingSagaStore(rows);
    final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);
    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
    final OrderSagaDecider saga = new OrderSagaDecider();

    /** Trigger events get the ids {@code evt-1}, {@code evt-2}, … so two harnesses agree. */
    private final AtomicInteger triggerIds = new AtomicInteger();

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
            .register(
                AggregateType.of("refund"), Refund.class, c -> AggregateId.of(c.orderId()), refund)
            .build();
    final SagaRunner<State> runner =
        SagaRunner.<State>builder()
            .decider(saga)
            .startWhen(e -> e.event() instanceof Opened)
            .extractSagaId(e -> SagaId.of("order-saga-" + ((Opened) e.event()).orderId()))
            .correlateByEventData(
                (streamId, e) ->
                    switch (e) {
                      case Advanced advanced ->
                          Optional.of(SagaId.of("order-saga-" + advanced.orderId()));
                      case Nudged nudged ->
                          Optional.of(SagaId.of("order-saga-" + nudged.orderId()));
                      case Cancelled cancelled ->
                          Optional.of(SagaId.of("order-saga-" + cancelled.orderId()));
                      default -> Optional.empty();
                    })
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
    final SagaTimeoutRunner<State> timeoutRunner =
        SagaTimeoutRunner.<State>builder()
            .decider(saga)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaType(TYPE)
            .clock(clock)
            .build();
    final SagaCompensationRetrySweeper<State> sweeper =
        SagaCompensationRetrySweeper.<State>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .leadership(SubscriptionLeadership.NOOP)
            .retryInterval(RETRY_INTERVAL)
            .giveUpAfter(Duration.ofHours(1))
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
                          EventId.of("evt-" + triggerIds.incrementAndGet()),
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
      return rows.load(SAGA, TYPE, State.class).orElseThrow();
    }

    /** The stored events of {@code type}, in global order. */
    List<EventEnvelope> stored(Class<? extends DomainEvent> type) {
      return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .filter(e -> type.isInstance(e.event()))
          .toList();
    }

    /** The stored compensation events of the saga, in global order. */
    List<EventEnvelope> compensationEvents() {
      return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .filter(e -> e.event() instanceof Released || e.event() instanceof Refunded)
          .toList();
    }

    /** What the stored compensation events of the saga state about their cause, by event name. */
    List<Provenance> compensationProvenance() {
      return compensationEvents().stream()
          .map(
              e ->
                  new Provenance(
                      e.event().getClass().getSimpleName(),
                      e.metadata().sagaOrigin(),
                      e.metadata().causationId(),
                      e.metadata().correlationId()))
          .sorted(Comparator.comparing(Provenance::event))
          .toList();
    }

    @Override
    public void close() {
      bus.close();
    }
  }

  /**
   * What the compensation events of one episode state when {@code opening} opened it ({@code null}:
   * no event did): one refund and one release, each with the saga's origin, the saga's correlation
   * id and the opening event's id as its causation id.
   */
  private static List<Provenance> compensationCausedBy(EventEnvelope opening) {
    CausationId cause =
        opening == null ? null : CausationId.of(opening.metadata().eventId().value());
    CorrelationId correlation = CorrelationId.of("streamrune:saga:" + SAGA.value());
    return List.of(
        new Provenance("Refunded", ORIGIN, cause, correlation),
        new Provenance("Released", ORIGIN, cause, correlation));
  }

  /**
   * The shipment is rejected, so the delivery of {@link Advanced} claims the compensation episode
   * with that event as its cause. The release then fails once on infrastructure while the refund
   * commits, so the saga is left {@code COMPENSATING} with one of its two compensation events
   * stored by the delivery that opened the episode. Returns the opening event.
   */
  private static EventEnvelope leftCompensatingByARejectedShipment(Harness h) {
    h.open();
    EventEnvelope advanced = h.append(new Advanced(ORDER));
    h.ship.failure = () -> new IllegalArgumentException("address rejected");
    h.release.failure = () -> new IllegalStateException("connection reset");
    h.deliver(advanced);
    h.release.failure = null;
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.row().episodeCauseEventId()).isEqualTo(advanced.metadata().eventId());
    assertThat(h.stored(Released.class)).isEmpty();
    assertThat(h.stored(Refunded.class)).hasSize(1);
    return advanced;
  }

  /** The paths that resume a {@code COMPENSATING} episode; the value drives one resume. */
  private static Map<String, Consumer<Harness>> resumePaths() {
    Map<String, Consumer<Harness>> paths = new LinkedHashMap<>();
    paths.put("aLaterEventsResume", h -> h.deliver(h.append(new Nudged(ORDER))));
    paths.put(
        "timeoutRunnerRePick",
        h -> {
          h.clock.advance(TIMEOUT.plusSeconds(1));
          h.timeoutRunner.pollOnce();
        });
    paths.put(
        "sweeper",
        h -> {
          h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
          h.sweeper.sweepOnce();
        });
    paths.put(
        "operatorResumeFaulted",
        h -> {
          assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
          assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
          assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
        });
    paths.put(
        "operatorReplayOfAnEventHeldBehindTheFault",
        h -> {
          assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
          h.deliver(h.append(new Nudged(ORDER)));
          assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
          assertThat(h.deadLetters.findBySaga(SAGA)).hasSize(1);
          h.replayer.replayAll(SAGA);
        });
    return paths;
  }

  @TestFactory
  Stream<DynamicTest> everyResumePathStoresTheSameCausationId() {
    Stream<DynamicTest> uninterrupted =
        Stream.of(
            DynamicTest.dynamicTest(
                "sameDelivery",
                () -> {
                  try (var h = new Harness()) {
                    h.open();
                    EventEnvelope advanced = h.append(new Advanced(ORDER));
                    h.ship.failure = () -> new IllegalArgumentException("address rejected");

                    h.deliver(advanced);

                    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                    assertThat(h.compensationProvenance())
                        .isEqualTo(compensationCausedBy(advanced));
                  }
                }),
            DynamicTest.dynamicTest(
                "redeliveryOfTheOpeningEvent",
                () -> {
                  try (var h = new Harness()) {
                    EventEnvelope advanced = leftCompensatingByARejectedShipment(h);

                    h.deliver(advanced);

                    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                    assertThat(h.compensationProvenance())
                        .isEqualTo(compensationCausedBy(advanced));
                  }
                }));
    Stream<DynamicTest> resumed =
        resumePaths().entrySet().stream()
            .map(
                path ->
                    DynamicTest.dynamicTest(
                        path.getKey(),
                        () -> {
                          try (var h = new Harness()) {
                            EventEnvelope advanced = leftCompensatingByARejectedShipment(h);

                            path.getValue().accept(h);

                            assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                            assertThat(h.row().episodeCauseEventId())
                                .isEqualTo(advanced.metadata().eventId());
                            // One event of each kind: the refund the opening delivery committed
                            // deduplicates, the release is committed by the resuming path.
                            assertThat(h.compensationProvenance())
                                .isEqualTo(compensationCausedBy(advanced));
                          }
                        }));
    return Stream.concat(uninterrupted, resumed);
  }

  /**
   * An episode opened with no event in hand stores no causation id, also when a later event is the
   * one that resumes it: the resuming event is not the cause of the episode.
   */
  @TestFactory
  Stream<DynamicTest> anEpisodeOpenedWithoutAnEvent_storesNoCausationId() {
    Map<String, Consumer<Harness>> openings = new LinkedHashMap<>();
    openings.put(
        "timeoutClaim",
        h -> {
          h.open();
          h.clock.advance(TIMEOUT.plusSeconds(1));
          h.timeoutRunner.pollOnce();
        });
    openings.put(
        "compensateFaulted",
        h -> {
          h.open();
          h.saga.poisonAdvanced = true;
          h.deliver(h.append(new Advanced(ORDER)));
          assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
          var entry = h.deadLetters.findBySaga(SAGA).get(0);
          assertThat(h.replayer.discard(SAGA, entry.eventOffset())).isTrue();
          assertThat(h.replayer.compensateFaulted(SAGA))
              .isIn(CompensateOutcome.TERMINAL, CompensateOutcome.COMPENSATING);
        });
    return openings.entrySet().stream()
        .flatMap(
            opening ->
                Stream.of(
                    DynamicTest.dynamicTest(
                        opening.getKey(),
                        () -> {
                          try (var h = new Harness()) {
                            opening.getValue().accept(h);

                            assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                            assertThat(h.row().episodeVersion()).isNotNull();
                            assertThat(h.row().episodeCauseEventId()).isNull();
                            assertThat(h.compensationProvenance())
                                .isEqualTo(compensationCausedBy(null));
                          }
                        }),
                    DynamicTest.dynamicTest(
                        opening.getKey() + "_resumedByALaterEvent",
                        () -> {
                          try (var h = new Harness()) {
                            h.release.failure = () -> new IllegalStateException("connection reset");
                            opening.getValue().accept(h);
                            h.release.failure = null;
                            assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
                            assertThat(h.stored(Refunded.class)).hasSize(1);

                            h.deliver(h.append(new Nudged(ORDER)));

                            assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                            assertThat(h.row().episodeCauseEventId()).isNull();
                            assertThat(h.compensationProvenance())
                                .isEqualTo(compensationCausedBy(null));
                          }
                        })));
  }

  /**
   * A dead-letter replay feeds the stored event, so an episode the feed opens names that event, as
   * a live delivery of it does.
   */
  @Test
  void anEpisodeOpenedByADeadLetterReplay_namesTheReplayedEvent() {
    try (var h = new Harness()) {
      h.open();
      EventEnvelope advanced = h.append(new Advanced(ORDER));
      h.saga.poisonAdvanced = true;
      h.deliver(advanced);
      assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(h.deadLetters.findBySaga(SAGA)).hasSize(1);
      h.saga.poisonAdvanced = false;
      h.ship.failure = () -> new IllegalArgumentException("address rejected");

      h.replayer.replayAll(SAGA);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.row().episodeCauseEventId()).isEqualTo(advanced.metadata().eventId());
      assertThat(h.compensationProvenance()).isEqualTo(compensationCausedBy(advanced));
    }
  }

  /**
   * The claim fails before it is written: no row names a cause. Whichever path claims next writes
   * the stamp and its own cause in one statement — the redelivered event names itself, the timeout
   * runner names none.
   */
  @TestFactory
  Stream<DynamicTest> aCrashBeforeTheClaim_leavesNoCause_andTheNextClaimantRecordsItsOwn() {
    return Stream.of(
        DynamicTest.dynamicTest(
            "redeliveryOfTheEvent",
            () -> {
              try (var h = new Harness()) {
                EventEnvelope advanced = crashedBeforeTheClaim(h);

                h.deliver(advanced);

                assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                assertThat(h.row().episodeCauseEventId()).isEqualTo(advanced.metadata().eventId());
                assertThat(h.compensationProvenance()).isEqualTo(compensationCausedBy(advanced));
              }
            }),
        DynamicTest.dynamicTest(
            "timeoutRunner",
            () -> {
              try (var h = new Harness()) {
                crashedBeforeTheClaim(h);

                h.clock.advance(TIMEOUT.plusSeconds(1));
                h.timeoutRunner.pollOnce();

                assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                assertThat(h.row().episodeCauseEventId()).isNull();
                assertThat(h.compensationProvenance()).isEqualTo(compensationCausedBy(null));
              }
            }));
  }

  private static EventEnvelope crashedBeforeTheClaim(Harness h) {
    h.open();
    EventEnvelope advanced = h.append(new Advanced(ORDER));
    h.ship.failure = () -> new IllegalArgumentException("address rejected");
    h.sagaStore.crashOnceBeforeTheClaim();
    assertThatThrownBy(() -> h.deliver(advanced)).isInstanceOf(SimulatedCrash.class);
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().episodeVersion()).isNull();
    assertThat(h.row().episodeCauseEventId()).isNull();
    assertThat(h.compensationEvents()).isEmpty();
    return advanced;
  }

  /**
   * The process dies after the claim commits and before any compensation command is dispatched. The
   * row holds the cause, so a resume on a path that has no event in hand, or another event, stores
   * what the uninterrupted run stores.
   */
  @TestFactory
  Stream<DynamicTest>
      aCrashBetweenTheClaimAndTheFirstCompensation_resumedOnAnotherPath_storesWhatTheUninterruptedRunStores() {
    return resumePaths().entrySet().stream()
        .map(
            path ->
                DynamicTest.dynamicTest(
                    path.getKey(),
                    () -> {
                      try (var h = new Harness()) {
                        h.open();
                        EventEnvelope advanced = h.append(new Advanced(ORDER));
                        h.ship.failure = () -> new IllegalArgumentException("address rejected");
                        h.sagaStore.crashOnceAfter("claimCompensating");
                        assertThatThrownBy(() -> h.deliver(advanced))
                            .isInstanceOf(SimulatedCrash.class);
                        assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
                        assertThat(h.row().episodeCauseEventId())
                            .isEqualTo(advanced.metadata().eventId());
                        assertThat(h.compensationEvents()).isEmpty();

                        path.getValue().accept(h);

                        assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                        assertThat(h.compensationProvenance())
                            .isEqualTo(provenanceOfTheUninterruptedRun());
                      }
                    }));
  }

  /**
   * The same scenario with no crash: the delivery of {@link Advanced} claims the episode, commits
   * both compensation commands and writes the terminal status. Trigger event ids are assigned in
   * order, so the opening event has the same id here as in the crashed run.
   */
  private static List<Provenance> provenanceOfTheUninterruptedRun() {
    try (var h = new Harness()) {
      h.open();
      h.ship.failure = () -> new IllegalArgumentException("address rejected");
      h.deliver(h.append(new Advanced(ORDER)));
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      List<Provenance> stored = h.compensationProvenance();
      assertThat(stored).hasSize(2).allSatisfy(p -> assertThat(p.causationId()).isNotNull());
      return stored;
    }
  }

  /**
   * Both compensation commands committed and the terminal write was lost. A resume re-dispatches
   * under the episode keys, the inbox answers for both, and the stored events are the ones the
   * first attempt committed.
   */
  @Test
  void aCrashBetweenTheLastCompensationAndTheTerminalWrite_resumedByTheSweeper_storesNothingNew() {
    try (var h = new Harness()) {
      h.open();
      EventEnvelope advanced = h.append(new Advanced(ORDER));
      h.ship.failure = () -> new IllegalArgumentException("address rejected");
      h.sagaStore.crashOnceBeforeTheTerminalWrite();
      assertThatThrownBy(() -> h.deliver(advanced)).isInstanceOf(SimulatedCrash.class);
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
      List<EventEnvelope> committed = h.compensationEvents();
      assertThat(committed).hasSize(2);

      h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
      h.sweeper.sweepOnce();

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.row().episodeCauseEventId()).isEqualTo(advanced.metadata().eventId());
      assertThat(h.compensationEvents()).isEqualTo(committed);
      assertThat(h.compensationProvenance()).isEqualTo(compensationCausedBy(advanced));
    }
  }

  /**
   * The episode faults while the sweeper resumes it (a throwing {@code compensate}) and the
   * operator resumes it: the fault write and the resume keep the cause the claim recorded.
   */
  @Test
  void aFaultInsideTheEpisode_keepsTheCause() {
    try (var h = new Harness()) {
      EventEnvelope advanced = leftCompensatingByARejectedShipment(h);
      h.saga.poisonCompensation = true;
      h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
      h.sweeper.sweepOnce();
      assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(h.row().episodeCauseEventId()).isEqualTo(advanced.metadata().eventId());
      h.saga.poisonCompensation = false;

      assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.row().episodeCauseEventId()).isEqualTo(advanced.metadata().eventId());
      assertThat(h.compensationProvenance()).isEqualTo(compensationCausedBy(advanced));
    }
  }

  /**
   * A saga whose own {@code evolve} yields {@code COMPENSATING} enters the episode through the
   * write that persists that state, and that write names the applied event. No compensation command
   * is dispatched in that delivery; the paths that resume the episode read the cause from the row.
   */
  @TestFactory
  Stream<DynamicTest> anEvolvedCompensatingStatus_namesTheEventThatWasApplied() {
    Map<String, Function<Harness, EventEnvelope>> openings = new LinkedHashMap<>();
    openings.put(
        "aCorrelatedEvent",
        h -> {
          h.open();
          EventEnvelope cancelled = h.append(new Cancelled(ORDER));
          h.deliver(cancelled);
          return cancelled;
        });
    openings.put(
        "theStartEvent",
        h -> {
          h.saga.startsCompensating = true;
          EventEnvelope opened = h.append(new Opened(ORDER));
          h.deliver(opened);
          return opened;
        });
    return openings.entrySet().stream()
        .flatMap(
            opening ->
                resumePaths().entrySet().stream()
                    .map(
                        resume ->
                            DynamicTest.dynamicTest(
                                opening.getKey() + "_" + resume.getKey(),
                                () -> {
                                  try (var h = new Harness()) {
                                    EventEnvelope applied = opening.getValue().apply(h);
                                    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
                                    assertThat(h.row().episodeCauseEventId())
                                        .isEqualTo(applied.metadata().eventId());
                                    assertThat(h.compensationEvents()).isEmpty();

                                    resume.getValue().accept(h);

                                    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                                    assertThat(h.compensationProvenance())
                                        .isEqualTo(compensationCausedBy(applied));
                                  }
                                })));
  }
}
