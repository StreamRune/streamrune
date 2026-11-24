package org.streamrune.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.ForwardingCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The parts of the tests of a runner that steps the sagas of a page in parallel.
 *
 * <p>The saga: {@link Started}{@code (id)} starts the saga {@code id}, {@link Replied}{@code (id)}
 * reaches it by its data, and each of the two dispatches one {@link Step} that carries the saga id
 * and the offset of its event. The state records the saga id of its last event and every offset
 * applied, so a row that received another saga's event shows it.
 *
 * <p>{@link GatedBus} executes nothing: each command can be held at a {@link Gate} and can fail by
 * its saga id. {@link World} is the real thing over the test kit: a {@link VirtualThreadCommandBus}
 * over an {@link InMemoryEventStore} with its command inbox and the saga rows, an offset store and
 * a dead-letter store. {@link EndState} is what a run of a world leaves behind.
 */
final class SagaParallelFixtures {

  /** How long a test waits for anything another thread does. */
  static final Duration AWAIT = Duration.ofSeconds(5);

  static final AggregateType SOURCE = AggregateType.of("par_source");
  static final AggregateType STEPS = AggregateType.of("par_step");
  static final SagaType SAGA_TYPE = SagaType.fromClass(ParState.class);

  /** The key of the null-saga entries in {@link EndState#deadLettersBySaga()}. */
  static final String NULL_SAGA = "<null-saga>";

  private SagaParallelFixtures() {}

  record Started(String id) implements DomainEvent {}

  record Replied(String id) implements DomainEvent {}

  /** The event a {@link Step} stores on the saga's own aggregate; it reaches the saga by origin. */
  record Stepped(String sagaId, long triggerOffset) implements DomainEvent {}

  /** The event an {@link Undo} stores; it reaches the saga by origin. */
  record Undone(String sagaId) implements DomainEvent {}

  /** The one command of a {@link Started} or {@link Replied}: its saga and its event's offset. */
  record Step(String sagaId, long triggerOffset) implements Command {}

  /** The compensation command of a saga whose {@link Step} was rejected. */
  record Undo(String sagaId) implements Command {}

  /** The saga's state: the saga id of its last event and every offset applied, in order. */
  record ParState(SagaStatus status, String sagaId, List<Long> applied) implements SagaState {}

  /** The state of a {@link Step}'s aggregate. */
  record StepAggregate() implements AggregateState {}

  static final AggregateType UNDOS = AggregateType.of("par_undo");

  /** The saga id a command of this saga carries. */
  static String sagaIdOf(Command command) {
    return switch (command) {
      case Step step -> step.sagaId();
      case Undo undo -> undo.sagaId();
      default -> throw new IllegalArgumentException("not a command of this saga: " + command);
    };
  }

  /**
   * The saga of these tests; it keeps no state of its own. {@link Started} and {@link Replied}
   * dispatch one {@link Step}; the replies to its own commands ({@link Stepped}, {@link Undone})
   * are recorded and dispatch nothing; a rejected {@link Step} is compensated by one {@link Undo}.
   */
  static SagaOrchestrator<ParState> orchestrator() {
    return new SagaOrchestrator<>() {
      @Override
      public Class<ParState> stateType() {
        return ParState.class;
      }

      @Override
      public ParState initialState(SagaId sagaId) {
        return new ParState(SagaStatus.STARTED, sagaId.value(), List.of());
      }

      @Override
      public boolean isStartEvent(EventEnvelope event) {
        return event.event() instanceof Started;
      }

      @Override
      public SagaId extractSagaId(EventEnvelope event) {
        return SagaId.of(((Started) event.event()).id());
      }

      @Override
      public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
        return event instanceof Replied replied
            ? Optional.of(SagaId.of(replied.id()))
            : Optional.empty();
      }

      @Override
      public ParState evolve(ParState state, EventEnvelope event) {
        List<Long> applied = new ArrayList<>(state.applied());
        applied.add(event.globalOffset().value());
        return new ParState(SagaStatus.RUNNING, idOf(event.event()), List.copyOf(applied));
      }

      @Override
      public List<SagaCommand> handle(ParState state, EventEnvelope event) {
        if (event.event() instanceof Stepped || event.event() instanceof Undone) {
          return List.of();
        }
        return List.of(
            SagaCommand.of(
                new Step(state.sagaId(), event.globalOffset().value()),
                aggregateOf(state.sagaId())));
      }

      @Override
      public List<SagaCommand> compensate(
          ParState state, Throwable failure, SagaCommand failedCommand) {
        return List.of(SagaCommand.of(new Undo(state.sagaId()), aggregateOf(state.sagaId())));
      }
    };
  }

  /** An aggregate id for a saga id; an aggregate id holds no control character, a test's may. */
  static AggregateId aggregateOf(String sagaId) {
    return AggregateId.of(sagaId.replaceAll("\\p{Cntrl}", "_"));
  }

  /** The saga id an event of this saga names. */
  static String idOf(DomainEvent event) {
    return switch (event) {
      case Started started -> started.id();
      case Replied replied -> replied.id();
      case Stepped stepped -> stepped.sagaId();
      case Undone undone -> undone.sagaId();
      default -> throw new IllegalArgumentException("not an event of this saga: " + event);
    };
  }

  /** Envelopes of the events at {@code firstOffset}, {@code firstOffset + 1}, … */
  static List<EventEnvelope> page(long firstOffset, DomainEvent... events) {
    List<EventEnvelope> page = new ArrayList<>(events.length);
    for (int i = 0; i < events.length; i++) {
      page.add(envelope(firstOffset + i, events[i]));
    }
    return List.copyOf(page);
  }

  private static EventEnvelope envelope(long offset, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(SOURCE, AggregateId.of("source")),
        Version.initial(),
        EventType.fromClass(event.getClass()),
        event,
        metadata());
  }

  private static EventMetadata metadata() {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        CorrelationId.of(IdGenerator.generateCommandId().value()),
        null,
        null,
        Instant.now(),
        Map.of());
  }

  /** Waits for a latch for at most {@link #AWAIT}; fails the test when it does not open. */
  static void await(CountDownLatch latch, String what) {
    try {
      if (!latch.await(AWAIT.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new AssertionError("timed out waiting for " + what);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted waiting for " + what, e);
    }
  }

  /** Holds the commands of one saga until it is released. */
  static final class Gate {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);

    /** Lets every held and every later command of the saga go on. */
    void release() {
      released.countDown();
    }

    /** Waits until a command of the saga reached the gate. */
    void awaitEntered() {
      await(entered, "a command at the gate");
    }

    /** Whether a command of the saga reached the gate. */
    boolean entered() {
      return entered.getCount() == 0;
    }

    private void pass() {
      entered.countDown();
      await(released, "the gate's release");
    }
  }

  /**
   * A bus that executes nothing, as {@code NoopBus} of {@code SagaRunnerRoutingTest}: it records
   * each command, its key and the thread that dispatched it, holds a command at its {@link Gate} —
   * the gate of that one command ({@link #gate(String, long)}) first, then the gate of its saga
   * ({@link #gate(String)}) — and then throws the failure programmed for its saga, if any.
   */
  static final class GatedBus implements CommandBus {
    private final List<Command> commands = new CopyOnWriteArrayList<>();
    private final List<IdempotencyKey> succeededKeys = new CopyOnWriteArrayList<>();
    private final List<Thread> threads = new CopyOnWriteArrayList<>();
    private final Map<String, Gate> gates = new ConcurrentHashMap<>();
    private final Map<Step, Gate> commandGates = new ConcurrentHashMap<>();
    private final Map<String, Supplier<? extends Throwable>> failures = new ConcurrentHashMap<>();
    private final AtomicInteger entered = new AtomicInteger();
    private volatile boolean gateEverySaga;

    /** The gate of every command of a saga, created closed on the first call. */
    Gate gate(String sagaId) {
      return gates.computeIfAbsent(sagaId, id -> new Gate());
    }

    /** The gate of the one {@link Step} of a saga's event at that offset, created closed. */
    Gate gate(String sagaId, long triggerOffset) {
      return commandGates.computeIfAbsent(new Step(sagaId, triggerOffset), id -> new Gate());
    }

    /** Gives every saga a closed gate on its first command. */
    GatedBus gateEverySaga() {
      gateEverySaga = true;
      return this;
    }

    /** Opens every gate there is and lets every later command pass. */
    void releaseAll() {
      gateEverySaga = false;
      gates.values().forEach(Gate::release);
      commandGates.values().forEach(Gate::release);
    }

    /** The commands of the saga throw this failure, after its gates. */
    GatedBus failing(String sagaId, Supplier<? extends Throwable> failure) {
      failures.put(sagaId, failure);
      return this;
    }

    /** The commands of the saga succeed again. */
    void clearFailure(String sagaId) {
      failures.remove(sagaId);
    }

    /** Every command dispatched, failed ones included, in order of arrival. */
    List<Command> commands() {
      return List.copyOf(commands);
    }

    /** The saga ids of the commands dispatched, in order of arrival. */
    List<String> sagas() {
      return commands.stream().map(SagaParallelFixtures::sagaIdOf).toList();
    }

    /** The commands of one saga. */
    List<Command> commandsOf(String sagaId) {
      return commands.stream().filter(c -> sagaIdOf(c).equals(sagaId)).toList();
    }

    /** The keys of the commands that returned normally, in order of return. */
    List<IdempotencyKey> succeededKeys() {
      return List.copyOf(succeededKeys);
    }

    /** The thread of each dispatch, in order of arrival. */
    List<Thread> threads() {
      return List.copyOf(threads);
    }

    /** How many commands reached the bus so far. */
    int entered() {
      return entered.get();
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("a saga dispatches with an idempotency key");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      String sagaId = sagaIdOf(command);
      commands.add(command);
      threads.add(Thread.currentThread());
      entered.incrementAndGet();
      Gate commandGate = command instanceof Step step ? commandGates.get(step) : null;
      if (commandGate != null) {
        commandGate.pass();
      }
      Gate gate = gateEverySaga ? gate(sagaId) : gates.get(sagaId);
      if (gate != null) {
        gate.pass();
      }
      Supplier<? extends Throwable> failure = failures.get(sagaId);
      if (failure != null) {
        throw GatedBus.<RuntimeException>sneaky(failure.get());
      }
      succeededKeys.add(key);
      return new CommandResult(
          List.of(), StreamId.of(STEPS, AggregateId.of("none")), Version.initial(), List.of());
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneaky(Throwable failure) throws T {
      throw (T) failure;
    }
  }

  /**
   * A real bus over the test kit's stores: a {@link VirtualThreadCommandBus} over an {@link
   * InMemoryEventStore} with its {@link InMemoryCommandInbox}, fenced against the {@link
   * InMemorySagaStore}, an {@link InMemoryOffsetStore} for a subscription's checkpoint and an
   * {@link InMemorySagaDeadLetterStore}. A {@link Step} stores one {@link Stepped} on the saga's
   * own aggregate. {@link #commandBus()} records every command before the real bus runs it.
   */
  static final class World implements AutoCloseable {
    private static final EventTypeRegistry TYPES =
        SimpleEventTypeRegistry.builder()
            .registerEvent(Started.class)
            .registerEvent(Replied.class)
            .registerEvent(Stepped.class)
            .registerEvent(Undone.class)
            .registerState("StepAggregate", StepAggregate.class)
            .build();

    final InMemorySagaStore rows = new InMemorySagaStore();
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore eventStore =
        new InMemoryEventStore(TYPES).withCommandInbox(inbox).withSagaStore(rows);
    final InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);

    /** The sagas whose {@link Step} the aggregate rejects (a business rejection: compensated). */
    final Set<String> rejecting = ConcurrentHashMap.newKeySet();

    private final List<Command> commands = new CopyOnWriteArrayList<>();
    private final List<IdempotencyKey> keys = new CopyOnWriteArrayList<>();
    private final List<VirtualThreadCommandBus> buses = new CopyOnWriteArrayList<>();

    /** Numbers the streams of {@link #append}, so two worlds that append alike name them alike. */
    private final AtomicLong sourceStreams = new AtomicLong();

    final VirtualThreadCommandBus realBus = newRealBus();
    private final CommandBus bus = recording(realBus);

    /** The bus a runner of this world dispatches to: it records each command, then runs it. */
    CommandBus commandBus() {
      return bus;
    }

    /**
     * The bus of another process over this world's stores: a {@link VirtualThreadCommandBus} of its
     * own (its own aggregate locks) over the same event store and command inbox, recording into the
     * same lists as {@link #commandBus()}.
     */
    CommandBus anotherProcessBus() {
      return recording(newRealBus());
    }

    private VirtualThreadCommandBus newRealBus() {
      VirtualThreadCommandBus created =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .locker(new LocalStripedLocker(64))
              .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
              .objectMapper(new ObjectMapper())
              .commandInbox(inbox)
              .register(STEPS, Step.class, c -> aggregateOf(c.sagaId()), new StepDecider(this))
              .register(UNDOS, Undo.class, c -> aggregateOf(c.sagaId()), new UndoDecider())
              .build();
      buses.add(created);
      return created;
    }

    private CommandBus recording(CommandBus delegate) {
      return new ForwardingCommandBus(delegate) {
        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          commands.add(command);
          keys.add(key);
          return super.execute(command, key);
        }
      };
    }

    /** A runner builder over this world's stores and bus; the fence is on. */
    SagaRunner.Builder<ParState> runner() {
      return SagaRunner.<ParState>builder()
          .orchestrator(orchestrator())
          .sagaStore(rows)
          .commandBus(bus)
          .sagaDeadLetterStore(deadLetters);
    }

    /** Stores each event on a stream of its own, as a request does, and returns them as stored. */
    List<EventEnvelope> append(DomainEvent... events) {
      List<EventEnvelope> stored = new ArrayList<>(events.length);
      for (DomainEvent event : events) {
        StreamId stream =
            StreamId.of(SOURCE, AggregateId.of("source-" + sourceStreams.incrementAndGet()));
        EventEnvelope envelope =
            new EventEnvelope(
                GlobalOffset.initial(),
                stream,
                Version.initial(),
                EventType.fromClass(event.getClass()),
                event,
                metadata());
        long offset =
            eventStore
                .append(stream, List.of(envelope), Version.initial())
                .globalOffsets()
                .getFirst()
                .value();
        stored.add(eventStore.readGlobalStream(GlobalOffset.of(offset - 1), 1).getFirst());
      }
      return List.copyOf(stored);
    }

    /**
     * A subscription over this world's event store and offsets that delivers to the runner; the
     * replies the runner's commands store come back to it and are recorded on its sagas.
     */
    PollingEventSubscription subscription(SubscriptionName name, SagaRunner<ParState> runner) {
      return PollingEventSubscription.builder()
          .subscriptionName(name)
          .eventStore(eventStore)
          .offsetStore(offsets)
          .sagaRunner(runner)
          .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ZERO))
          .build();
    }

    /** Every event in the store, in global order. */
    List<EventEnvelope> stored() {
      return eventStore.readGlobalStream(GlobalOffset.initial(), Integer.MAX_VALUE);
    }

    /** What this world holds now; {@code checkpoint} names the subscription whose offset counts. */
    EndState endState(ProjectionName checkpoint) {
      return EndState.of(this, checkpoint);
    }

    @Override
    public void close() {
      buses.forEach(VirtualThreadCommandBus::close);
    }
  }

  /** {@link Step} stores one {@link Stepped}, or is rejected for a saga the world rejects. */
  static final class StepDecider implements Decider<Step, StepAggregate, DomainEvent> {
    private final World world;

    StepDecider(World world) {
      this.world = world;
    }

    @Override
    public StepAggregate initialState() {
      return new StepAggregate();
    }

    @Override
    public List<DomainEvent> decide(Step command, StepAggregate state) {
      if (world.rejecting.contains(command.sagaId())) {
        throw new IllegalArgumentException("step rejected for " + command.sagaId());
      }
      return List.of(new Stepped(command.sagaId(), command.triggerOffset()));
    }

    @Override
    public StepAggregate evolve(StepAggregate state, DomainEvent event) {
      return state;
    }
  }

  /** {@link Undo} stores one {@link Undone}. */
  static final class UndoDecider implements Decider<Undo, StepAggregate, DomainEvent> {
    @Override
    public StepAggregate initialState() {
      return new StepAggregate();
    }

    @Override
    public List<DomainEvent> decide(Undo command, StepAggregate state) {
      return List.of(new Undone(command.sagaId()));
    }

    @Override
    public StepAggregate evolve(StepAggregate state, DomainEvent event) {
      return state;
    }
  }

  /**
   * What a run left behind, compared between runs: of a {@link World} ({@link #of(World,
   * ProjectionName)}) or of a runner over a {@link GatedBus} ({@link #of(InMemorySagaStore,
   * InMemorySagaDeadLetterStore, GatedBus)}).
   *
   * @param rowsBySaga each saga's row, by saga id
   * @param eventsByAggregate the events of each stream, in order; empty for a {@link GatedBus} run
   * @param inboxKeys every key a command was dispatched under that the inbox recorded (a {@link
   *     GatedBus} run: that the bus answered), forward and compensation keys alike
   * @param deadLettersBySaga each saga's dead-letter entries, by offset; the null-saga entries
   *     under {@link #NULL_SAGA}
   * @param checkpoint the subscription's saved offset; 0 when none or for a {@link GatedBus} run
   * @param commandsBySaga the commands dispatched for each saga, in order
   */
  record EndState(
      Map<String, Row> rowsBySaga,
      Map<StreamId, List<DomainEvent>> eventsByAggregate,
      Set<IdempotencyKey> inboxKeys,
      Map<String, List<DeadLetter>> deadLettersBySaga,
      long checkpoint,
      Map<String, List<Command>> commandsBySaga) {

    /** A dead-letter entry: the event's offset and the class name of the error it records. */
    record DeadLetter(long offset, String errorType) {}

    /** The parts of a saga row a run decides. */
    record Row(
        SagaStatus status,
        long version,
        boolean genesisApplied,
        Long lastAppliedOffset,
        ParState state) {}

    static EndState of(World world, ProjectionName checkpoint) {
      Map<StreamId, List<DomainEvent>> events = new LinkedHashMap<>();
      for (EventEnvelope e : world.stored()) {
        events.computeIfAbsent(e.streamId(), s -> new ArrayList<>()).add(e.event());
      }
      Set<IdempotencyKey> keys = new LinkedHashSet<>();
      for (IdempotencyKey key : world.keys) {
        if (world.inbox.find(key).isPresent()) {
          keys.add(key);
        }
      }
      return new EndState(
          rowsOf(world.rows),
          events,
          keys,
          deadLettersOf(world.deadLetters),
          world.offsets.getLastOffset(checkpoint).value(),
          bySaga(world.commands));
    }

    static EndState of(
        InMemorySagaStore rows, InMemorySagaDeadLetterStore deadLetters, GatedBus bus) {
      return new EndState(
          rowsOf(rows),
          Map.of(),
          new LinkedHashSet<>(bus.succeededKeys()),
          deadLettersOf(deadLetters),
          0,
          bySaga(bus.commands()));
    }

    private static Map<String, Row> rowsOf(InMemorySagaStore store) {
      Map<String, Row> rows = new TreeMap<>();
      for (SagaId id : store.all()) {
        store
            .load(id, SAGA_TYPE, ParState.class)
            .ifPresent(row -> rows.put(id.value(), rowOf(row)));
      }
      return rows;
    }

    private static Map<String, List<DeadLetter>> deadLettersOf(InMemorySagaDeadLetterStore store) {
      Map<String, List<DeadLetter>> deadLetters = new TreeMap<>();
      for (SagaDeadLetterEntry entry : store.all()) {
        String saga = entry.sagaId() == null ? NULL_SAGA : entry.sagaId().value();
        deadLetters
            .computeIfAbsent(saga, s -> new ArrayList<>())
            .add(new DeadLetter(entry.eventOffset().value(), entry.errorType()));
      }
      // the store lists its entries in no order; an entry is unique by (saga, type, offset), and
      // this fixture has one type
      deadLetters.values().forEach(l -> l.sort(Comparator.comparingLong(DeadLetter::offset)));
      return deadLetters;
    }

    private static Map<String, List<Command>> bySaga(List<Command> dispatched) {
      Map<String, List<Command>> commands = new TreeMap<>();
      for (Command command : dispatched) {
        commands.computeIfAbsent(sagaIdOf(command), s -> new ArrayList<>()).add(command);
      }
      return commands;
    }

    private static Row rowOf(LoadedSaga<ParState> row) {
      return new Row(
          row.status(), row.version(), row.genesisApplied(), row.lastAppliedOffset(), row.state());
    }
  }

  /** Delegates everything to the saga it wraps; a test overrides the method it changes. */
  static class Delegating implements SagaOrchestrator<ParState> {
    final SagaOrchestrator<ParState> delegate;

    Delegating(SagaOrchestrator<ParState> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Class<ParState> stateType() {
      return delegate.stateType();
    }

    @Override
    public ParState initialState(SagaId sagaId) {
      return delegate.initialState(sagaId);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return delegate.isStartEvent(event);
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return delegate.extractSagaId(event);
    }

    @Override
    public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
      return delegate.correlateByEventData(streamId, event);
    }

    @Override
    public ParState evolve(ParState state, EventEnvelope event) {
      return delegate.evolve(state, event);
    }

    @Override
    public List<SagaCommand> handle(ParState state, EventEnvelope event) {
      return delegate.handle(state, event);
    }

    @Override
    public List<SagaCommand> compensate(
        ParState state, Throwable failure, SagaCommand failedCommand) {
      return delegate.compensate(state, failure, failedCommand);
    }
  }
}
