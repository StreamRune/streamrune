package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
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
import org.streamrune.core.types.IdempotencyKey;
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
 * The executor takes the stamp of a compensation episode — the episode version that scopes the
 * compensation idempotency keys, and the cause that becomes the causation id — from the saga row,
 * on the delivery that opens the episode as on every resume. It computes neither.
 *
 * <p>The opening that tells a read from a computation is a start event whose own {@code evolve}
 * yields {@code COMPENSATING} and whose forward command is then rejected: the insert stamps the
 * episode at version 1 and the claim that follows lands the row at version 2 and keeps the stamp.
 * Every case here runs that opening, or runs every opening on a store whose stamps are not "the row
 * version plus one" ({@link Harness#Harness(boolean, long)}), and compares the keys the command bus
 * received with the keys the row's stamp gives.
 *
 * <p>Concurrency is driven without threads: a second path runs inside a hook, at a chosen point of
 * the opening delivery (after its claim, before a chosen command, before its terminal write).
 */
class SagaEpisodeStampReadFromRowTest {

  record Opened(String orderId) implements DomainEvent {}

  /** A correlated event whose forward command is {@link Ship}; the saga stays RUNNING. */
  record Advanced(String orderId) implements DomainEvent {}

  /** A correlated event whose evolve yields COMPENSATING and which still issues {@link Ship}. */
  record Cancelled(String orderId) implements DomainEvent {}

  /** A correlated event the saga reacts to with no command. */
  record Nudged(String orderId) implements DomainEvent {}

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
   * The start event issues {@link Reserve}; with {@code bornCompensating} it also evolves to
   * COMPENSATING. {@link Advanced} issues {@link Ship} and stays RUNNING; {@link Cancelled} issues
   * {@link Ship} and evolves to COMPENSATING. Compensation is a release and a refund.
   */
  static final class OrderSaga implements SagaDecider<State> {
    private final boolean bornCompensating;
    volatile boolean poisonAdvanced;

    OrderSaga(boolean bornCompensating) {
      this.bornCompensating = bornCompensating;
    }

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
              || (event.event() instanceof Opened && bornCompensating);
      return new State(compensating ? SagaStatus.COMPENSATING : SagaStatus.RUNNING, ORDER);
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return switch (event.event()) {
        case Opened opened -> List.of(SagaCommand.of(new Reserve(ORDER), AggregateId.of(ORDER)));
        case Advanced advanced -> List.of(SagaCommand.of(new Ship(ORDER), AggregateId.of(ORDER)));
        case Cancelled cancelled -> List.of(SagaCommand.of(new Ship(ORDER), AggregateId.of(ORDER)));
        default -> List.of();
      };
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

  /**
   * Records the idempotency key of every keyed command, then runs it on the real bus. A hook, when
   * set, runs once, right before the first command of the chosen class.
   */
  static final class KeyRecordingBus implements CommandBus {
    private final CommandBus delegate;
    final List<String> keys = new ArrayList<>();
    private Class<? extends Command> hookBefore;
    private Runnable hook;

    KeyRecordingBus(CommandBus delegate) {
      this.delegate = delegate;
    }

    void runOnceBefore(Class<? extends Command> command, Runnable action) {
      hookBefore = command;
      hook = action;
    }

    /** The recorded keys of the compensation commands, in dispatch order. */
    List<String> compensationKeys() {
      return keys.stream()
          .filter(k -> k.startsWith("Release ") || k.startsWith("Refund "))
          .toList();
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return delegate.execute(command);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (hook != null && hookBefore.isInstance(command)) {
        Runnable once = hook;
        hook = null;
        once.run();
      }
      keys.add(command.getClass().getSimpleName() + " " + key.value());
      return delegate.execute(command, key);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return delegate.supportsIdempotentExecution();
    }
  }

  /**
   * A forwarding store with seams around the claim and the terminal write, and with stamps the
   * executor cannot compute: every loaded row reports its episode version shifted by {@code
   * stampOffset}. The shift is the same on every load, so the stamp is still one immutable value
   * per episode — only not "the version the claim landed at".
   */
  static final class SeamedSagaStore extends ForwardingSagaStore {
    private final long stampOffset;
    private boolean failNextClaim;
    private Runnable afterClaim;
    private Runnable beforeTerminalWrite;
    private boolean failNextLoad;

    SeamedSagaStore(InMemorySagaStore delegate, long stampOffset) {
      super(delegate);
      this.stampOffset = stampOffset;
    }

    /** Runs once, right after the next claim has committed. */
    void runOnceAfterTheClaim(Runnable action) {
      afterClaim = action;
    }

    /** Runs once, right before the next write of a terminal status. */
    void runOnceBeforeTheTerminalWrite(Runnable action) {
      beforeTerminalWrite = action;
    }

    @Override
    public void claimCompensating(
        SagaId id, SagaType type, SagaState state, long version, EventId episodeCause) {
      if (failNextClaim) {
        failNextClaim = false;
        throw new SimulatedCrash("nothing: the claim did not reach the store");
      }
      super.claimCompensating(id, type, state, version, episodeCause);
      if (afterClaim != null) {
        Runnable once = afterClaim;
        afterClaim = null;
        once.run();
      }
    }

    @Override
    public void update(SagaId id, SagaType type, SagaState state, SagaStatus status, long version) {
      if (beforeTerminalWrite != null && status.isTerminal()) {
        Runnable once = beforeTerminalWrite;
        beforeTerminalWrite = null;
        once.run();
      }
      super.update(id, type, state, status, version);
    }

    @Override
    public <S extends SagaState> Optional<LoadedSaga<S>> load(
        SagaId id, SagaType type, Class<S> stateType) {
      if (failNextLoad) {
        failNextLoad = false;
        throw new SimulatedCrash("the claim: the row could not be read back");
      }
      return super.load(id, type, stateType)
          .map(
              row ->
                  row.episodeVersion() == null
                      ? row
                      : new LoadedSaga<>(
                          row.state(),
                          row.status(),
                          row.version(),
                          row.updatedAt(),
                          row.episodeVersion() + stampOffset,
                          row.episodeClaimedAt(),
                          row.episodeCauseEventId(),
                          row.genesisApplied(),
                          row.preFaultStatus(),
                          row.deadLetterPending(),
                          row.lastAppliedOffset(),
                          row.lastReplayedOffset()));
    }
  }

  static final class Harness implements AutoCloseable {
    final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    final InMemorySagaStore rows = new InMemorySagaStore(clock);
    final SeamedSagaStore sagaStore;
    final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);
    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
    final OrderSaga saga;
    private final AtomicInteger triggerIds = new AtomicInteger();
    final VirtualThreadCommandBus realBus =
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
    final KeyRecordingBus bus = new KeyRecordingBus(realBus);
    final SagaRunner<State> runner;

    /** A second listener on the same stores and bus: another replica's delivery. */
    final SagaRunner<State> secondReplica;

    final SagaDeadLetterReplayer replayer;
    final SagaTimeoutRunner<State> timeoutRunner;
    final SagaCompensationRetrySweeper<State> sweeper;

    /**
     * @param bornCompensating whether the start event's evolve yields COMPENSATING
     * @param stampOffset what every loaded row adds to its stored episode version: 0 for the
     *     in-memory store as it is, another value for stamps that are not "row version plus one"
     */
    Harness(boolean bornCompensating, long stampOffset) {
      saga = new OrderSaga(bornCompensating);
      sagaStore = new SeamedSagaStore(rows, stampOffset);
      runner = newRunner();
      secondReplica = newRunner();
      replayer =
          SagaDeadLetterReplayer.builder()
              .sagaDeadLetterStore(deadLetters)
              .sagaStore(sagaStore)
              .eventStore(eventStore)
              .sagaRunner(runner)
              .inboxRetentionMaxAge(WINDOW)
              .clock(clock)
              .build();
      timeoutRunner =
          SagaTimeoutRunner.<State>builder()
              .decider(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(TYPE)
              .clock(clock)
              .build();
      sweeper =
          SagaCompensationRetrySweeper.<State>builder()
              .sagaRunner(runner)
              .sagaStore(sagaStore)
              .leadership(SubscriptionLeadership.NOOP)
              .retryInterval(RETRY_INTERVAL)
              .giveUpAfter(Duration.ofHours(1))
              .clock(clock)
              .build();
    }

    private SagaRunner<State> newRunner() {
      return SagaRunner.<State>builder()
          .decider(saga)
          .startWhen(e -> e.event() instanceof Opened)
          .extractSagaId(e -> SAGA)
          .correlateBy(
              e ->
                  e.event() instanceof Advanced
                          || e.event() instanceof Cancelled
                          || e.event() instanceof Nudged
                      ? Optional.of(SAGA)
                      : Optional.empty())
          .sagaStore(sagaStore)
          .commandBus(bus)
          .sagaDeadLetterStore(deadLetters)
          .inboxRetentionMaxAge(WINDOW)
          .clock(clock)
          .build();
    }

    /** Appends a trigger event as a request stores it. */
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

    /** The row as the executor reads it: through the store the runners use. */
    LoadedSaga<State> row() {
      return sagaStore.load(SAGA, TYPE, State.class).orElseThrow();
    }

    List<EventEnvelope> stored(Class<? extends DomainEvent> type) {
      return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .filter(e -> type.isInstance(e.event()))
          .toList();
    }

    /** The two compensation keys the row's stamp gives, in dispatch order. */
    List<String> keysOfTheRowsEpisode() {
      String scope = "saga:" + SAGA.value() + ":episode:" + row().episodeVersion() + ":comp:";
      return List.of("Release " + scope + 0, "Refund " + scope + 1);
    }

    /** The distinct causation ids of the stored compensation events ({@code null}: none). */
    List<Optional<CausationId>> compensationCauses() {
      return Stream.concat(stored(Released.class).stream(), stored(Refunded.class).stream())
          .map(e -> Optional.ofNullable(e.metadata().causationId()))
          .distinct()
          .toList();
    }

    /** The cause the row records, as a causation id ({@code null}: none). */
    Optional<CausationId> causeOfTheRowsEpisode() {
      return Optional.ofNullable(row().episodeCauseEventId()).map(id -> CausationId.of(id.value()));
    }

    void assertEachCompensationStoredOnce() {
      assertThat(stored(Released.class)).as("the release is stored once").hasSize(1);
      assertThat(stored(Refunded.class)).as("the refund is stored once").hasSize(1);
    }

    @Override
    public void close() {
      realBus.close();
    }
  }

  /**
   * The openings that dispatch compensation commands in the call that opens the episode. Each one
   * leaves the harness right after that call.
   */
  private static Map<String, Consumer<Harness>> openings() {
    Map<String, Consumer<Harness>> openings = new LinkedHashMap<>();
    openings.put(
        "claimOnACorrelatedEvent",
        h -> {
          h.deliver(h.append(new Opened(ORDER)));
          h.ship.failure = () -> new IllegalArgumentException("address rejected");
          h.deliver(h.append(new Advanced(ORDER)));
        });
    openings.put(
        "claimOnTheStartEvent",
        h -> {
          h.reserve.failure = () -> new IllegalArgumentException("out of stock");
          h.deliver(h.append(new Opened(ORDER)));
        });
    openings.put(
        "correlatedEventEvolvedToCompensating_forwardCommandRejected",
        h -> {
          h.deliver(h.append(new Opened(ORDER)));
          h.ship.failure = () -> new IllegalArgumentException("address rejected");
          h.deliver(h.append(new Cancelled(ORDER)));
        });
    openings.put(
        "timeoutClaim",
        h -> {
          h.deliver(h.append(new Opened(ORDER)));
          h.clock.advance(TIMEOUT.plusSeconds(1));
          h.timeoutRunner.pollOnce();
        });
    openings.put(
        "compensateFaulted",
        h -> {
          h.deliver(h.append(new Opened(ORDER)));
          h.saga.poisonAdvanced = true;
          h.deliver(h.append(new Advanced(ORDER)));
          var entry = h.deadLetters.findBySaga(SAGA).get(0);
          assertThat(h.replayer.discard(SAGA, entry.eventOffset())).isTrue();
          assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.COMPENSATING);
        });
    return openings;
  }

  /** The start event evolves to COMPENSATING and its forward command is rejected. */
  private static EventEnvelope openBornCompensating(Harness h) {
    h.reserve.failure = () -> new IllegalArgumentException("out of stock");
    EventEnvelope opened = h.append(new Opened(ORDER));
    h.deliver(opened);
    return opened;
  }

  /**
   * Every opening, on the store as it is and on a store whose stamps the executor cannot compute:
   * the refund fails once on infrastructure, so the opening call commits the release and leaves the
   * episode COMPENSATING, and the sweeper resumes it. Both attempts present the keys of the row's
   * stamp, each compensation is stored once, and every compensation event carries the row's cause.
   */
  @TestFactory
  Stream<DynamicTest> theOpeningCallAndAResume_presentTheKeysAndTheCauseOfTheRowsStamp() {
    Stream<DynamicTest> onRunningRows =
        Stream.of(0L, 1_000L)
            .flatMap(
                offset ->
                    openings().entrySet().stream()
                        .map(
                            opening ->
                                DynamicTest.dynamicTest(
                                    opening.getKey() + "_stampOffset" + offset,
                                    () -> {
                                      try (var h = new Harness(false, offset)) {
                                        assertOpeningAndResume(h, opening.getValue());
                                      }
                                    })));
    Stream<DynamicTest> bornCompensating =
        Stream.of(0L, 1_000L)
            .map(
                offset ->
                    DynamicTest.dynamicTest(
                        "startEventEvolvedToCompensating_forwardCommandRejected_stampOffset"
                            + offset,
                        () -> {
                          try (var h = new Harness(true, offset)) {
                            assertOpeningAndResume(
                                h, SagaEpisodeStampReadFromRowTest::openBornCompensating);
                          }
                        }));
    return Stream.concat(onRunningRows, bornCompensating);
  }

  private static void assertOpeningAndResume(Harness h, Consumer<Harness> opening) {
    h.refund.failure = () -> new IllegalStateException("connection reset");
    opening.accept(h);
    h.refund.failure = null;

    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.bus.compensationKeys())
        .as("the call that opens the episode keys its commands with the row's stamp")
        .isEqualTo(h.keysOfTheRowsEpisode());
    assertThat(h.stored(Released.class)).hasSize(1);

    h.bus.keys.clear();
    h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
    h.sweeper.sweepOnce();

    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.bus.compensationKeys())
        .as("a resume presents the same keys")
        .isEqualTo(h.keysOfTheRowsEpisode());
    h.assertEachCompensationStoredOnce();
    assertThat(h.compensationCauses())
        .as("every compensation event carries the cause the row records")
        .containsExactly(h.causeOfTheRowsEpisode());
  }

  /**
   * Every compensation command committed and the terminal write was lost. The resume presents the
   * keys the opening call used, the inbox answers for both, and no compensation is stored twice.
   */
  @Test
  void bornCompensating_theTerminalWriteIsLost_aResumeStoresNoCompensationTwice() {
    try (var h = new Harness(true, 0)) {
      h.sagaStore.runOnceBeforeTheTerminalWrite(
          () -> {
            throw new SimulatedCrash("nothing: the terminal write did not reach the store");
          });
      h.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = h.append(new Opened(ORDER));
      assertThatThrownBy(() -> h.deliver(opened)).isInstanceOf(SimulatedCrash.class);
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();

      h.bus.keys.clear();
      h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
      h.sweeper.sweepOnce();

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();
    }
  }

  /** The paths that can run beside the opening delivery; the value drives one pass. */
  private static Map<String, Consumer<Harness>> secondPaths(EventEnvelope[] opened) {
    Map<String, Consumer<Harness>> paths = new LinkedHashMap<>();
    paths.put(
        "aSecondDeliveryOfTheStartEvent",
        h -> h.secondReplica.asEventListener().onEvents(List.of(opened[0])));
    paths.put(
        "theSweeper",
        h -> {
          h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
          h.sweeper.sweepOnce();
        });
    paths.put(
        "theTimeoutRunner",
        h -> {
          h.clock.advance(TIMEOUT.plusSeconds(1));
          h.timeoutRunner.pollOnce();
        });
    return paths;
  }

  /**
   * Nothing fails and nothing crashes: a second path runs at one point of the opening delivery of a
   * start event born COMPENSATING whose forward command is rejected. Both paths key the episode
   * with the row's stamp, so the command inbox answers for whichever comes second and each
   * compensation is stored once.
   */
  @TestFactory
  Stream<DynamicTest> bornCompensating_aSecondPathAtEachPointOfTheOpeningDelivery() {
    Map<String, java.util.function.BiConsumer<Harness, Runnable>> points = new LinkedHashMap<>();
    points.put("afterTheClaim", (h, second) -> h.sagaStore.runOnceAfterTheClaim(second));
    points.put(
        "beforeTheFirstCompensation", (h, second) -> h.bus.runOnceBefore(Release.class, second));
    points.put(
        "betweenTheTwoCompensations", (h, second) -> h.bus.runOnceBefore(Refund.class, second));
    points.put(
        "beforeTheTerminalWrite", (h, second) -> h.sagaStore.runOnceBeforeTheTerminalWrite(second));
    EventEnvelope[] opened = new EventEnvelope[1];
    return points.entrySet().stream()
        .flatMap(
            point ->
                secondPaths(opened).entrySet().stream()
                    .map(
                        path ->
                            DynamicTest.dynamicTest(
                                point.getKey() + "_" + path.getKey(),
                                () -> {
                                  try (var h = new Harness(true, 0)) {
                                    h.reserve.failure =
                                        () -> new IllegalArgumentException("out of stock");
                                    opened[0] = h.append(new Opened(ORDER));
                                    point.getValue().accept(h, () -> path.getValue().accept(h));

                                    h.deliver(opened[0]);

                                    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                                    assertThat(h.bus.compensationKeys())
                                        .as("both paths present the keys of the row's stamp")
                                        .hasSize(4)
                                        .containsOnlyElementsOf(h.keysOfTheRowsEpisode());
                                    h.assertEachCompensationStoredOnce();
                                  }
                                })));
  }

  /**
   * A row that carries a stamp and is in a forward status — a write the store contract rules out,
   * prepared here by hand — meets a claim: the claim keeps the stamp and the cause, and the opening
   * call dispatches with what the row holds, not with the version its claim landed at nor with the
   * event in its hand. A second copy of the stamp rule inside the executor would pass every other
   * case of this class and fail this one.
   */
  @Test
  void aClaimOnARowThatAlreadyCarriesAStamp_dispatchesWithTheRowsStampAndCause() {
    try (var h = new Harness(false, 0)) {
      h.deliver(h.append(new Opened(ORDER)));
      long version = h.row().version();
      h.rows.claimCompensating(SAGA, TYPE, h.row().state(), version, EventId.of("evt-earlier"));
      h.rows.update(
          SAGA, TYPE, new State(SagaStatus.RUNNING, ORDER), SagaStatus.RUNNING, version + 1);
      long stamped = h.row().episodeVersion();
      h.ship.failure = () -> new IllegalArgumentException("address rejected");
      h.refund.failure = () -> new IllegalStateException("connection reset");

      h.deliver(h.append(new Advanced(ORDER)));
      h.refund.failure = null;

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(h.row().episodeVersion()).isEqualTo(stamped);
      assertThat(h.row().version()).as("the claim landed elsewhere").isNotEqualTo(stamped);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());

      h.bus.keys.clear();
      h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
      h.sweeper.sweepOnce();

      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();
      assertThat(h.compensationCauses())
          .as("one causation id for the episode: the one the row records")
          .containsExactly(Optional.of(CausationId.of("evt-earlier")));
    }
  }

  /**
   * The claim of the opening delivery is not written. The row is the one the insert left: stamped,
   * with no command dispatched. The redelivery resumes that episode under the insert's stamp.
   */
  @Test
  void bornCompensating_theClaimIsNotWritten_theRedeliveryResumesUnderTheInsertsStamp() {
    try (var h = new Harness(true, 0)) {
      h.sagaStore.failNextClaim = true;
      h.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = h.append(new Opened(ORDER));

      assertThatThrownBy(() -> h.deliver(opened)).isInstanceOf(SimulatedCrash.class);

      assertThat(h.bus.compensationKeys()).isEmpty();
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(h.row().version()).isEqualTo(1L);
      assertThat(h.row().episodeVersion()).isEqualTo(1L);

      h.deliver(opened);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();
    }
  }

  /**
   * The claim commits and the row cannot be read back. No compensation command is dispatched — the
   * executor has no stamp to key it with — and the failure propagates. What is left is a claimed
   * episode with nothing dispatched; the redelivery resumes it under the row's keys.
   */
  @Test
  void theRowCannotBeReadBackAfterTheClaim_nothingIsDispatched_theRedeliveryResumes() {
    try (var h = new Harness(true, 0)) {
      h.sagaStore.runOnceAfterTheClaim(() -> h.sagaStore.failNextLoad = true);
      h.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = h.append(new Opened(ORDER));

      assertThatThrownBy(() -> h.deliver(opened)).isInstanceOf(SimulatedCrash.class);

      assertThat(h.bus.compensationKeys()).isEmpty();
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);

      h.deliver(opened);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();
    }
  }

  /**
   * The row is deleted between the claim and the read. There is no stamp, so nothing is dispatched.
   * A delivery reports a stale view; the redelivered start event then finds no row and starts the
   * saga from its recorded facts.
   */
  @Test
  void theRowIsGoneAfterTheClaim_onADelivery_nothingIsDispatched_andTheStaleViewPropagates() {
    try (var h = new Harness(true, 0)) {
      h.sagaStore.runOnceAfterTheClaim(() -> h.rows.delete(SAGA, TYPE));
      h.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = h.append(new Opened(ORDER));

      assertThatThrownBy(() -> h.deliver(opened)).isInstanceOf(OptimisticLockException.class);

      assertThat(h.bus.compensationKeys()).isEmpty();
      assertThat(h.rows.load(SAGA, TYPE, State.class)).isEmpty();

      h.deliver(opened);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();
    }
  }

  /** The same on a claim with no event in hand: the pass ends as a lost claim. */
  @Test
  void theRowIsGoneAfterTheClaim_onATimeoutClaim_nothingIsDispatched() {
    try (var h = new Harness(false, 0)) {
      h.deliver(h.append(new Opened(ORDER)));
      h.sagaStore.runOnceAfterTheClaim(() -> h.rows.delete(SAGA, TYPE));
      h.clock.advance(TIMEOUT.plusSeconds(1));

      h.timeoutRunner.pollOnce();

      assertThat(h.bus.compensationKeys()).isEmpty();
      assertThat(h.rows.load(SAGA, TYPE, State.class)).isEmpty();
    }
  }

  /**
   * Another path quarantines the episode between the claim and the read. The stamp is still on the
   * row, so the opening delivery dispatches under the row's keys; its terminal write is a
   * compare-and-swap at its own claim and loses to the fault, so the row stays FAULTED. The
   * operator's resume then finds every command in the inbox.
   */
  @Test
  void theRowIsFaultedAfterTheClaim_theOpeningDeliveryKeysWithTheRowsStamp() {
    try (var h = new Harness(true, 0)) {
      h.sagaStore.runOnceAfterTheClaim(
          () -> assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED));
      h.reserve.failure = () -> new IllegalArgumentException("out of stock");

      h.deliver(h.append(new Opened(ORDER)));

      assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();

      h.bus.keys.clear();
      assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();
    }
  }
}
