package org.streamrune.test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;

/**
 * Given events, when an event, then the commands a saga dispatched and the state it keeps: a {@link
 * SagaRunner} built from the saga's own builder over in-memory stores, a bus that records every
 * command instead of executing it, and a {@link MutableClock}, so a saga test needs no
 * subscription, no event store and no command handlers.
 *
 * <pre>{@code
 * SagaFixture<OrderSaga.State> saga =
 *     SagaFixture.of(OrderSaga.State.class, builder -> builder.orchestrator(new OrderSaga()));
 * StreamId order = StreamId.of(AggregateType.of("order"), AggregateId.of("1"));
 *
 * saga.when(order, new OrderPlaced("1"))
 *     .expectCommands(new ReserveStock("1"))
 *     .expectStatus(SagaId.of("order-saga-1"), SagaStatus.RUNNING);
 * saga.replyingTo(SagaId.of("order-saga-1"), new StockReserved("1"))
 *     .expectNoCommands()
 *     .expectCompleted(SagaId.of("order-saga-1"));
 * }</pre>
 *
 * <p><b>What the fixture wires.</b> {@link #of} hands the test a {@link SagaRunner.Builder} that
 * already names an {@link InMemorySagaStore} (behind a {@link ForwardingSagaStore} whose {@link
 * #sagaStoreFaults()} the test may script), an {@link InMemorySagaDeadLetterStore}, the recording
 * bus, the fixture's clock and no leadership; the test adds the saga — {@code orchestrator(...)},
 * or {@code decider(...)} with its routing — and anything else the runner needs. The builder starts
 * with {@code forwardFence(false)}: the recording bus stores nothing and fences nothing, so a
 * runner that fences its forward commands cannot be built over it. {@link #withTimeouts} builds a
 * {@link SagaTimeoutRunner} over the same stores, bus and clock, for {@link #runTimeouts()}.
 *
 * <p><b>Steps.</b> {@link #given}, {@link #when}, {@link #replyingTo}, {@link #redeliver()} and
 * {@link #runTimeouts()} are steps: each starts a new list of dispatched commands, which {@link
 * #expectCommands} and {@link #expectNoCommands()} read. {@link #advance(Duration)} only moves the
 * clock. The events of a step are delivered through {@link SagaRunner#asEventListener()} as one
 * page, as a subscription delivers them, after the fixture stored them: an envelope at the initial
 * offset — as {@link #event}, {@link #reply} and {@link SagaEnvelopes} build them — gets the next
 * global offset and the next version of its stream; an envelope with an offset of its own keeps it,
 * so a test can deliver one again. A failure of the step — a saga that throws, a fault of {@link
 * #sagaStoreFaults()} — propagates to the test, and {@link #redeliver()} then delivers the same
 * events at the same offsets, as a subscription does after a crash before its checkpoint.
 *
 * <p><b>The commands.</b> The bus records each command a saga dispatches, in order, and executes
 * none: it stores no event, so a reply to a command is delivered by the test ({@link #replyingTo}).
 * {@link #failing} makes the dispatches of one command type throw, to test a saga whose command is
 * rejected or whose compensation cannot be dispatched. Like every shipped bus it refuses a dispatch
 * bound to a reserved correlation id that is not the bound saga origin's.
 *
 * <p>Not thread-safe: one fixture per test, driven from the test's thread.
 *
 * @param <S> the saga's state type
 */
public final class SagaFixture<S extends SagaState> {

  /** The instant the fixture's clock starts at. */
  public static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

  private static final AggregateType REPLY = AggregateType.of("reply");

  private final Class<S> stateType;
  private final SagaType sagaType;
  private final MutableClock clock = MutableClock.startingAt(START);
  private final InMemorySagaStore rows = new InMemorySagaStore(clock);
  private final FaultSchedule sagaStoreFaults = FaultSchedule.none();
  private final ForwardingSagaStore sagaStore = new ForwardingSagaStore(rows, sagaStoreFaults);
  private final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);
  private final RecordingCommandBus bus = new RecordingCommandBus();
  private final SagaRunner<S> runner;
  private SagaTimeoutRunner<S> timeoutRunner;

  private final List<EventEnvelope> delivered = new ArrayList<>();
  private final Map<StreamId, Long> streamVersions = new HashMap<>();
  private List<EventEnvelope> lastDelivery = List.of();
  private int stepStart;
  private long nextOffset = 1;

  private SagaFixture(Class<S> stateType, Consumer<SagaRunner.Builder<S>> runner) {
    this.stateType = Objects.requireNonNull(stateType, "stateType is required");
    this.sagaType = SagaType.fromClass(stateType);
    SagaRunner.Builder<S> builder =
        SagaRunner.<S>builder()
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetters)
            .clock(clock)
            .forwardFence(false);
    Objects.requireNonNull(runner, "runner is required").accept(builder);
    this.runner = builder.build();
  }

  /**
   * A fixture for the saga whose runner {@code runner} completes.
   *
   * @param stateType the saga's state class, the one its orchestrator or decider names
   * @param runner adds the saga to the builder the fixture wired: {@code orchestrator(...)}, or
   *     {@code decider(...)} with {@code startWhen}, {@code extractSagaId} and {@code
   *     correlateByEventData}
   * @param <S> the saga's state type
   * @return the fixture, with nothing delivered
   */
  public static <S extends SagaState> SagaFixture<S> of(
      Class<S> stateType, Consumer<SagaRunner.Builder<S>> runner) {
    return new SagaFixture<>(stateType, runner);
  }

  /**
   * Builds the timeout runner {@link #runTimeouts()} polls with: the fixture names the saga store,
   * the bus, the saga type, the clock and the saga runner; {@code timeoutRunner} adds the decider
   * whose {@code timeout()} the runner compares a saga's age with.
   *
   * @param timeoutRunner adds {@code decider(...)} and any other setting to the builder
   * @return this fixture
   */
  public SagaFixture<S> withTimeouts(Consumer<SagaTimeoutRunner.Builder<S>> timeoutRunner) {
    SagaTimeoutRunner.Builder<S> builder =
        SagaTimeoutRunner.<S>builder()
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaType(sagaType)
            .clock(clock)
            .sagaRunner(runner);
    Objects.requireNonNull(timeoutRunner, "timeoutRunner is required").accept(builder);
    this.timeoutRunner = builder.build();
    return this;
  }

  /**
   * Makes every later dispatch of a command of {@code commandType} throw: the command is recorded
   * and the bus throws the supplied failure, as a rejecting or failing command handler would.
   *
   * @param commandType the class of the commands that fail (a subclass matches too)
   * @param failure creates the exception the bus throws
   * @return this fixture
   */
  public SagaFixture<S> failing(
      Class<? extends Command> commandType, Supplier<? extends RuntimeException> failure) {
    bus.failures.put(
        Objects.requireNonNull(commandType, "commandType is required"),
        Objects.requireNonNull(failure, "failure is required"));
    return this;
  }

  // ==================== Steps ====================

  /**
   * A step: delivers the events as one page.
   *
   * @param storedEvents the events, in their global order
   * @return this fixture
   */
  public SagaFixture<S> given(EventEnvelope... storedEvents) {
    return deliver(List.of(storedEvents));
  }

  /**
   * A step: delivers one event.
   *
   * @param storedEvent the event
   * @return this fixture
   */
  public SagaFixture<S> when(EventEnvelope storedEvent) {
    return deliver(List.of(storedEvent));
  }

  /**
   * A step: delivers {@code event} as a request stored it on {@code streamId} ({@link #event}).
   *
   * @param streamId the stream the event belongs to
   * @param event the payload
   * @return this fixture
   */
  public SagaFixture<S> when(StreamId streamId, DomainEvent event) {
    return when(event(streamId, event));
  }

  /**
   * A step: delivers {@code event} as the reply to a command the saga {@code sagaId} dispatched
   * ({@link #reply}), on the stream {@code reply:<saga id>}.
   *
   * @param sagaId the saga whose command the event replies to
   * @param event the payload
   * @return this fixture
   */
  public SagaFixture<S> replyingTo(SagaId sagaId, DomainEvent event) {
    return replyingTo(sagaId, StreamId.of(REPLY, new AggregateId(sagaId.value())), event);
  }

  /**
   * A step: delivers {@code event} as the reply to a command the saga {@code sagaId} dispatched,
   * stored on {@code streamId}.
   *
   * @param sagaId the saga whose command the event replies to
   * @param streamId the stream the reply belongs to
   * @param event the payload
   * @return this fixture
   */
  public SagaFixture<S> replyingTo(SagaId sagaId, StreamId streamId, DomainEvent event) {
    return when(reply(sagaId, streamId, event));
  }

  /**
   * A step: delivers the events of the last step that delivered events again, at their offsets —
   * what a subscription delivers after a crash that came before its checkpoint.
   *
   * @return this fixture
   * @throws IllegalStateException when no event was delivered yet
   */
  public SagaFixture<S> redeliver() {
    if (lastDelivery.isEmpty()) {
      throw new IllegalStateException("redeliver() needs a step that delivered events first");
    }
    beginStep();
    delivered.addAll(lastDelivery);
    runner.asEventListener().onEvents(lastDelivery);
    return this;
  }

  /**
   * Moves the fixture's clock forward; no step.
   *
   * @param duration how far
   * @return this fixture
   */
  public SagaFixture<S> advance(Duration duration) {
    clock.advance(duration);
    return this;
  }

  /**
   * A step: one poll of the timeout runner at the clock's instant ({@link
   * SagaTimeoutRunner#pollOnce()}): it claims and compensates the sagas past their deadline.
   *
   * @return this fixture
   * @throws IllegalStateException when the fixture has no timeout runner ({@link #withTimeouts})
   */
  public SagaFixture<S> runTimeouts() {
    if (timeoutRunner == null) {
      throw new IllegalStateException(
          "runTimeouts() polls a timeout runner, and this fixture has none: build it with"
              + " withTimeouts(builder -> builder.decider(theDecider))");
    }
    beginStep();
    timeoutRunner.pollOnce();
    return this;
  }

  private SagaFixture<S> deliver(List<EventEnvelope> events) {
    List<EventEnvelope> stored = events.stream().map(this::stored).toList();
    lastDelivery = stored;
    beginStep();
    delivered.addAll(stored);
    runner.asEventListener().onEvents(stored);
    return this;
  }

  private void beginStep() {
    stepStart = bus.dispatched.size();
  }

  /** The envelope as stored: the next offset and stream version, unless it carries an offset. */
  private EventEnvelope stored(EventEnvelope event) {
    Objects.requireNonNull(event, "event is required");
    long offset = event.globalOffset().value();
    if (offset > 0) {
      nextOffset = Math.max(nextOffset, offset + 1);
      return event;
    }
    long version = streamVersions.merge(event.streamId(), 1L, Long::sum);
    return new EventEnvelope(
        GlobalOffset.of(nextOffset++),
        event.streamId(),
        new Version(version),
        event.eventType(),
        event.event(),
        event.metadata());
  }

  // ==================== Envelopes ====================

  /**
   * An event as a request stored it: no saga origin, no causation id, generated event, command and
   * correlation ids, the fixture clock's instant; at the initial offset, which a step replaces.
   *
   * @param streamId the stream the event belongs to
   * @param event the payload
   * @return the envelope
   */
  public EventEnvelope event(StreamId streamId, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        Version.initial(),
        EventType.fromClass(event.getClass()),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            IdGenerator.generateCorrelationId(),
            null,
            null,
            clock.instant(),
            Map.of()));
  }

  /**
   * The reply to a command the saga {@code sagaId} dispatched: {@link SagaEnvelopes#causedBy(Class,
   * SagaId, StreamId, DomainEvent)}, at the initial offset, which a step replaces.
   *
   * @param sagaId the saga whose command the event replies to
   * @param streamId the stream the reply belongs to
   * @param event the payload
   * @return the envelope
   */
  public EventEnvelope reply(SagaId sagaId, StreamId streamId, DomainEvent event) {
    return SagaEnvelopes.causedBy(stateType, sagaId, streamId, event);
  }

  // ==================== Expectations ====================

  /**
   * Fails unless the latest step dispatched exactly these commands, in this order.
   *
   * @param commands the commands, compared with {@code equals}
   * @return this fixture
   */
  public SagaFixture<S> expectCommands(Command... commands) {
    List<Command> expected = List.of(commands);
    List<Command> actual = stepCommands();
    if (!actual.equals(expected)) {
      throw new AssertionError(
          "the latest step dispatched " + actual + ", but " + expected + " was expected");
    }
    return this;
  }

  /**
   * Fails unless the latest step dispatched no command.
   *
   * @return this fixture
   */
  public SagaFixture<S> expectNoCommands() {
    List<Command> actual = stepCommands();
    if (!actual.isEmpty()) {
      throw new AssertionError("the latest step dispatched " + actual + ", but none was expected");
    }
    return this;
  }

  /**
   * Fails unless the saga exists and its state satisfies {@code condition}.
   *
   * @param sagaId the saga
   * @param condition what the state must satisfy
   * @return this fixture
   */
  public SagaFixture<S> expectState(SagaId sagaId, Predicate<S> condition) {
    S state = require(sagaId).state();
    if (!condition.test(state)) {
      throw new AssertionError(
          "the state of saga " + sagaId.value() + " does not satisfy the condition: " + state);
    }
    return this;
  }

  /**
   * Fails unless the saga exists with the given status.
   *
   * @param sagaId the saga
   * @param status the status its row must carry
   * @return this fixture
   */
  public SagaFixture<S> expectStatus(SagaId sagaId, SagaStatus status) {
    LoadedSaga<S> saga = require(sagaId);
    if (saga.status() != status) {
      throw new AssertionError(
          "saga "
              + sagaId.value()
              + " is "
              + saga.status()
              + ", but "
              + status
              + " was expected; its state is "
              + saga.state());
    }
    return this;
  }

  /**
   * Fails unless the saga is {@link SagaStatus#COMPENSATING}: its compensation episode is open.
   *
   * @param sagaId the saga
   * @return this fixture
   */
  public SagaFixture<S> expectCompensating(SagaId sagaId) {
    return expectStatus(sagaId, SagaStatus.COMPENSATING);
  }

  /**
   * Fails unless the saga is {@link SagaStatus#COMPLETED}.
   *
   * @param sagaId the saga
   * @return this fixture
   */
  public SagaFixture<S> expectCompleted(SagaId sagaId) {
    return expectStatus(sagaId, SagaStatus.COMPLETED);
  }

  /**
   * Fails unless the saga dead-letter store holds an entry of the saga: an event it quarantined.
   *
   * @param sagaId the saga
   * @return this fixture
   */
  public SagaFixture<S> expectDeadLettered(SagaId sagaId) {
    if (deadLetters.findBySaga(sagaId).isEmpty()) {
      List<SagaDeadLetterEntry> all = deadLetters.all();
      throw new AssertionError(
          "no dead-letter entry of saga "
              + sagaId.value()
              + "; the store holds "
              + (all.isEmpty() ? "none" : all));
    }
    return this;
  }

  private LoadedSaga<S> require(SagaId sagaId) {
    return saga(sagaId)
        .orElseThrow(
            () ->
                new AssertionError(
                    "no saga "
                        + sagaId.value()
                        + " of type "
                        + sagaType.value()
                        + "; the store holds "
                        + rows.all()));
  }

  private List<Command> stepCommands() {
    return List.copyOf(bus.dispatched.subList(stepStart, bus.dispatched.size()));
  }

  // ==================== The parts ====================

  /**
   * The saga's row as the store keeps it, read without passing {@link #sagaStoreFaults()}.
   *
   * @param sagaId the saga
   * @return the row, or empty when the saga has none
   */
  public Optional<LoadedSaga<S>> saga(SagaId sagaId) {
    return rows.load(sagaId, sagaType, stateType);
  }

  /**
   * Every command a saga dispatched since the fixture was built, in order, the failed ones
   * included.
   *
   * @return an immutable copy
   */
  public List<Command> dispatched() {
    return List.copyOf(bus.dispatched);
  }

  /**
   * Every envelope the steps delivered, in order, as stored: with the offset each was delivered at,
   * redeliveries included.
   *
   * @return an immutable copy
   */
  public List<EventEnvelope> delivered() {
    return List.copyOf(delivered);
  }

  /**
   * The runner the fixture built.
   *
   * @return the runner
   */
  public SagaRunner<S> runner() {
    return runner;
  }

  /**
   * The bus the runner dispatches to: it records each command and executes none.
   *
   * @return the bus
   */
  public CommandBus commandBus() {
    return bus;
  }

  /**
   * The saga rows; the runner writes them through a {@link ForwardingSagaStore} scripted by {@link
   * #sagaStoreFaults()}.
   *
   * @return the store
   */
  public InMemorySagaStore sagaStore() {
    return rows;
  }

  /**
   * The faults injected into the runner's calls of the saga store, and their count: {@code
   * crashAfter("applyEvent")} stops a step after its write, so a test can prove that {@link
   * #redeliver()} converges.
   *
   * @return the schedule
   */
  public FaultSchedule sagaStoreFaults() {
    return sagaStoreFaults;
  }

  /**
   * The saga dead-letter store the runner quarantines into.
   *
   * @return the store
   */
  public InMemorySagaDeadLetterStore deadLetterStore() {
    return deadLetters;
  }

  /**
   * The clock of the stores and the runners, at {@link #START} until {@link #advance} moves it.
   *
   * @return the clock
   */
  public MutableClock clock() {
    return clock;
  }

  /** Records each command; executes none; throws for the types {@link #failing} named. */
  private static final class RecordingCommandBus implements CommandBus {

    private final List<Command> dispatched = new ArrayList<>();
    private final Map<Class<?>, Supplier<? extends RuntimeException>> failures =
        new LinkedHashMap<>();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException(
          "a saga dispatches every command with an idempotency key: execute(command, key)");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      StreamRuneContext.requireNoForeignReservedCorrelationId();
      dispatched.add(command);
      for (Map.Entry<Class<?>, Supplier<? extends RuntimeException>> failure :
          failures.entrySet()) {
        if (failure.getKey().isInstance(command)) {
          throw failure.getValue().get();
        }
      }
      return new CommandResult(List.of(), null, Version.initial(), List.of());
    }
  }
}
