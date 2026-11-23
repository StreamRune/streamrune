package org.streamrune.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import javax.sql.DataSource;
import org.streamrune.core.AggregateLocker;
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
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaDecider;
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
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.Crash;
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.CrashingBus;
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.CrashingDeadLetters;
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.CrashingOffsets;
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.CrashingSagaStore;
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.Point;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SagaTimeoutRunnerTestAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.ForwardingCommandBus;
import org.streamrune.test.MutableClock;

/**
 * Processes of an application that steps the sagas of a page in parallel, over one PostgreSQL
 * database, in one JVM: the PostgreSQL counterpart of the runtime tests' {@code
 * SagaParallelFixtures.World} and {@code CrashingStores}, which this module's tests cannot see.
 *
 * <p>The saga is that of the runtime tests: {@link Started}{@code (id)} starts the saga {@code id},
 * {@link Replied}{@code (id)} reaches it by its data, and each dispatches one {@link Step} that
 * stores a {@link Stepped} on the saga's own aggregate; the replies reach the saga by origin and
 * dispatch nothing; a {@link Step} of a saga in {@link #rejecting} is rejected and compensated by
 * {@link #compensationCommands} {@link Undo} commands. The state records the saga id and every
 * offset applied.
 *
 * <p>A {@link Node} is what one process holds: a {@link PostgresEventStore} with its {@link
 * PostgresCommandInbox}, a {@link PostgresSagaStore}, a {@link PostgresSagaDeadLetterStore}, a
 * {@link PostgresOffsetStore}, a real {@link VirtualThreadCommandBus} with its own aggregate locks,
 * a {@link SagaRunner} with the forward fence on and its own step lock, its {@link
 * PollingEventSubscription} and a {@link SagaTimeoutRunner} built from the runner; the stores and
 * the bus are wrapped in the crash decorators of {@link SagaParallelPostgresCrashPoints}. Nodes
 * share nothing but the database's rows.
 */
final class SagaParallelPostgresNodes {

  record Started(String id) implements DomainEvent {}

  record Replied(String id) implements DomainEvent {}

  /**
   * The event of a {@link Step}.
   *
   * @param mark the step's {@link Step#mark()}
   */
  record Stepped(String sagaId, long mark) implements DomainEvent {}

  record Undone(String sagaId) implements DomainEvent {}

  /**
   * One step of a saga.
   *
   * @param mark the offset of the event that dispatched it; for a saga of several steps ({@link
   *     Shape#stepsToComplete()}) the number of the step, from 1, so that the stored events of two
   *     runs do not depend on the order in which their sagas' commands committed
   */
  record Step(String sagaId, long mark) implements Command {}

  record Undo(String sagaId) implements Command {}

  record ParState(SagaStatus status, String sagaId, List<Long> applied) implements SagaState {}

  record StepAggregate() implements AggregateState {}

  static final AggregateType SOURCE = AggregateType.of("par_source");
  static final AggregateType STEPS = AggregateType.of("par_step");
  static final AggregateType UNDOS = AggregateType.of("par_undo");
  static final SagaType SAGA_TYPE = SagaType.fromClass(ParState.class);

  /** Every node's subscription: the nodes share its checkpoint. */
  static final SubscriptionName SUBSCRIPTION = SubscriptionName.of("par-crash-pg");

  static final ProjectionName CHECKPOINT = ProjectionName.of(SUBSCRIPTION.value());

  static final EventTypeRegistry REGISTRY =
      SimpleEventTypeRegistry.builder()
          .registerEvent(Started.class)
          .registerEvent(Replied.class)
          .registerEvent(Stepped.class)
          .registerEvent(Undone.class)
          .registerState("StepAggregate", StepAggregate.class)
          .build();

  /** The saga id a command of this saga carries. */
  static String sagaIdOf(Command command) {
    return switch (command) {
      case Step step -> step.sagaId();
      case Undo undo -> undo.sagaId();
      default -> throw new IllegalArgumentException("not a command of this saga: " + command);
    };
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

  /**
   * The saga's shape.
   *
   * @param compensationCommands how many {@link Undo} commands compensate a rejected step
   * @param stepsToComplete 0 for the saga of the runtime tests, which never completes: each start
   *     and each {@link Replied} dispatches one {@link Step} and the replies dispatch nothing;
   *     above 0, the start dispatches step 1, each {@link Stepped} reply the next step, and the
   *     saga is {@code COMPLETED} when the reply of that many steps is applied
   * @param stepAggregate the aggregate id every {@link Step} commands; {@code null} for the saga's
   *     own
   */
  record Shape(int compensationCommands, int stepsToComplete, String stepAggregate) {

    /** The aggregate a step of {@code sagaId} commands. */
    AggregateId stepAggregateOf(String sagaId) {
      return AggregateId.of(stepAggregate != null ? stepAggregate : sagaId);
    }
  }

  /**
   * The saga of the runtime tests' {@code SagaParallelFixtures.orchestrator()}; a rejected step is
   * compensated by {@code compensationCommands} {@link Undo} commands.
   */
  static SagaOrchestrator<ParState> orchestrator(int compensationCommands) {
    return orchestrator(new Shape(compensationCommands, 0, null));
  }

  /** The saga in the {@link Shape} {@code shape}. */
  static SagaOrchestrator<ParState> orchestrator(Shape shape) {
    int compensationCommands = shape.compensationCommands();
    int stepsToComplete = shape.stepsToComplete();
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
        boolean completed =
            stepsToComplete > 0
                && event.event() instanceof Stepped
                && applied.size() >= 1 + stepsToComplete;
        return new ParState(
            completed ? SagaStatus.COMPLETED : SagaStatus.RUNNING,
            idOf(event.event()),
            List.copyOf(applied));
      }

      @Override
      public List<SagaCommand> handle(ParState state, EventEnvelope event) {
        if (event.event() instanceof Undone
            || (event.event() instanceof Stepped && stepsToComplete == 0)) {
          return List.of();
        }
        // a saga of several steps: the start dispatches step 1, the reply of step k step k + 1
        long mark = stepsToComplete > 0 ? state.applied().size() : event.globalOffset().value();
        return List.of(
            SagaCommand.of(new Step(state.sagaId(), mark), shape.stepAggregateOf(state.sagaId())));
      }

      @Override
      public List<SagaCommand> compensate(
          ParState state, Throwable failure, SagaCommand failedCommand) {
        return Collections.nCopies(
            compensationCommands,
            SagaCommand.of(new Undo(state.sagaId()), AggregateId.of(state.sagaId())));
      }
    };
  }

  /** {@code saga} as a decider with the deadline {@code timeout}, for a timeout runner. */
  static SagaDecider<ParState> withDeadline(SagaOrchestrator<ParState> saga, Duration timeout) {
    return new SagaDecider<>() {
      @Override
      public Class<ParState> stateType() {
        return saga.stateType();
      }

      @Override
      public ParState initialState(SagaId sagaId) {
        return saga.initialState(sagaId);
      }

      @Override
      public ParState evolve(ParState state, EventEnvelope event) {
        return saga.evolve(state, event);
      }

      @Override
      public List<SagaCommand> handle(ParState state, EventEnvelope event) {
        return saga.handle(state, event);
      }

      @Override
      public List<SagaCommand> compensate(
          ParState state, Throwable failure, SagaCommand failedCommand) {
        return saga.compensate(state, failure, failedCommand);
      }

      @Override
      public Optional<Duration> timeout() {
        return Optional.of(timeout);
      }
    };
  }

  /** The deadline of the saga as the nodes' timeout runners see it. */
  static final Duration DEADLINE = Duration.ofHours(1);

  /**
   * What a run left behind, with every recorded offset given as the event it names ({@code
   * stream@version}), so two runs compare equal whatever order their sagas' commands committed in.
   *
   * @param eventsByAggregate the events of each stream, in order
   * @param rowsBySaga each saga's row, by saga id
   * @param inboxKeys every key in the command inbox
   * @param deadLettersBySaga each saga's dead-letter entries
   * @param checkpoint the subscription's saved offset
   * @param commandsBySaga the commands that reached a bus for each saga, in order, repeats removed
   */
  record EndState(
      Map<StreamId, List<DomainEvent>> eventsByAggregate,
      Map<String, Row> rowsBySaga,
      Set<String> inboxKeys,
      Map<String, List<DeadLetter>> deadLettersBySaga,
      long checkpoint,
      Map<String, List<Command>> commandsBySaga) {}

  /** The parts of a saga row a run decides, its offsets as the events they name. */
  record Row(
      SagaStatus status,
      long version,
      boolean genesisApplied,
      boolean deadLetterPending,
      SagaStatus preFaultStatus,
      Long episodeVersion,
      String lastApplied,
      SagaStatus stateStatus,
      String stateSagaId,
      List<String> applied) {}

  /** A dead-letter entry: the event it records and the error type. */
  record DeadLetter(String event, String errorType) {}

  private final DataSource source;
  private final PostgresEventStore probe;
  private final PostgresSagaStore probeRows;
  private final PostgresSagaDeadLetterStore probeDeadLetters;
  private final PostgresOffsetStore probeOffsets;
  private final AtomicLong sourceStreams = new AtomicLong();
  private final List<Node> nodes = new CopyOnWriteArrayList<>();
  private final List<Command> commands = new CopyOnWriteArrayList<>();

  /** The clock of the nodes' timeout runners; a test moves it past the deadline. */
  final MutableClock clock = MutableClock.startingAt(Instant.now());

  /** How many {@link Undo} commands compensate a rejected step. */
  volatile int compensationCommands = 1;

  /** {@link Shape#stepsToComplete()} of the nodes built from now on. */
  volatile int stepsToComplete = 0;

  /** {@link Shape#stepAggregate()} of the nodes built from now on. */
  volatile String stepAggregate = null;

  /** The sagas whose {@link Step} the aggregate rejects (a business rejection: compensated). */
  final Set<String> rejecting = ConcurrentHashMap.newKeySet();

  SagaParallelPostgresNodes(DataSource source) {
    this.source = source;
    this.probe = PostgresEventStore.builder().dataSource(source).typeRegistry(REGISTRY).build();
    this.probeRows = new PostgresSagaStore(source, SagaFenceNodes.mapper());
    this.probeDeadLetters = new PostgresSagaDeadLetterStore(source);
    this.probeOffsets = new PostgresOffsetStore(source);
  }

  /**
   * Empties the tables a run writes and starts the global offsets again at 1, so two runs that
   * append alike store their events at the same offsets.
   */
  void clean() throws SQLException {
    try (Connection conn = source.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "TRUNCATE saga_dead_letters, saga_state, command_inbox, event_stream,"
              + " subscription_checkpoint");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
    rejecting.clear();
    commands.clear();
    compensationCommands = 1;
    stepsToComplete = 0;
    stepAggregate = null;
    clock.setInstant(Instant.now());
    sourceStreams.set(0);
  }

  /**
   * How a node is built besides its crash point.
   *
   * @param source the node's connections; {@code null} for the fixture's
   * @param locker the bus's aggregate locker over {@code source}; {@code null} for a {@link
   *     LocalStripedLocker} of its own
   * @param parallelism the runner's parallelism
   * @param metrics the runner's and the timeout runner's metrics
   * @param deadline the saga's deadline as the timeout runner sees it
   * @param timeoutClock the timeout runner's clock; {@code null} for the fixture's {@link #clock}
   * @param timeoutPollInterval the pause between two polls of a started timeout runner
   * @param claimWait the timeout runner's bounded wait for a step in flight; {@code null} for its
   *     default
   */
  record Options(
      DataSource source,
      java.util.function.Function<DataSource, AggregateLocker> locker,
      int parallelism,
      StreamRuneMetrics metrics,
      Duration deadline,
      Clock timeoutClock,
      Duration timeoutPollInterval,
      Duration claimWait) {

    /** The options of the crash-point nodes. */
    static Options defaults() {
      return new Options(
          null, null, 2, StreamRuneMetrics.NOOP, DEADLINE, null, Duration.ofSeconds(1), null);
    }

    Options withSource(DataSource source) {
      return new Options(
          source,
          locker,
          parallelism,
          metrics,
          deadline,
          timeoutClock,
          timeoutPollInterval,
          claimWait);
    }

    Options withLocker(java.util.function.Function<DataSource, AggregateLocker> locker) {
      return new Options(
          source,
          locker,
          parallelism,
          metrics,
          deadline,
          timeoutClock,
          timeoutPollInterval,
          claimWait);
    }

    Options withParallelism(int parallelism) {
      return new Options(
          source,
          locker,
          parallelism,
          metrics,
          deadline,
          timeoutClock,
          timeoutPollInterval,
          claimWait);
    }

    Options withMetrics(StreamRuneMetrics metrics) {
      return new Options(
          source,
          locker,
          parallelism,
          metrics,
          deadline,
          timeoutClock,
          timeoutPollInterval,
          claimWait);
    }

    /**
     * A timeout runner on {@code clock} with the deadline {@code deadline}, polling at {@code
     * poll}.
     */
    Options withDeadline(Duration deadline, Clock clock, Duration poll) {
      return new Options(source, locker, parallelism, metrics, deadline, clock, poll, claimWait);
    }

    /** A timeout runner that waits at most {@code wait} for a step in flight before it claims. */
    Options withClaimWait(Duration wait) {
      return new Options(
          source, locker, parallelism, metrics, deadline, timeoutClock, timeoutPollInterval, wait);
    }
  }

  /** A node built with {@code options} that does not crash. */
  Node node(Options options) {
    Node node = new Node(null, null, options);
    nodes.add(node);
    return node;
  }

  /** A node that stops at {@code crashAt} on the saga {@code crashOn}. */
  Node node(Point crashAt, String crashOn) {
    Node node = new Node(crashAt, crashOn, Options.defaults());
    nodes.add(node);
    return node;
  }

  /** A node that does not crash. */
  Node node() {
    return node(null, null);
  }

  void closeAll() {
    nodes.forEach(Node::close);
    nodes.clear();
  }

  /**
   * The deadline of every saga started so far passes for one poll of {@code node}'s timeout runner:
   * the shared clock moves a minute past the deadline of a row created now, the runner polls once,
   * and the clock is set back to where it was. The nodes' timeout runners share the clock, so
   * without the reset every later {@link Node#cycle()} of any node would claim each forward saga.
   */
  void timeOut(Node node) {
    Instant before = clock.instant();
    clock.setInstant(Instant.now().plus(DEADLINE).plusSeconds(60));
    try {
      SagaTimeoutRunnerTestAccess.pollOnce(node.timeoutRunner);
    } finally {
      clock.setInstant(before);
    }
  }

  /** Stores each event on a stream of its own, as a request does, numbered per run. */
  void append(DomainEvent... events) {
    for (DomainEvent event : events) {
      StreamId stream =
          StreamId.of(SOURCE, AggregateId.of("source-" + sourceStreams.incrementAndGet()));
      probe.append(
          stream,
          List.of(
              new EventEnvelope(
                  GlobalOffset.initial(),
                  stream,
                  Version.initial(),
                  EventType.fromClass(event.getClass()),
                  event,
                  new EventMetadata(
                      IdGenerator.generateEventId(),
                      IdGenerator.generateCommandId(),
                      null,
                      CorrelationId.of(IdGenerator.generateCommandId().value()),
                      null,
                      null,
                      Instant.now(),
                      Map.of()))),
          Version.initial());
    }
  }

  /** Every event in the store, in global order. */
  List<EventEnvelope> stored() {
    return probe.readGlobalStream(GlobalOffset.initial(), 10_000);
  }

  long checkpoint() {
    return probeOffsets.getLastOffset(CHECKPOINT).value();
  }

  Optional<LoadedSaga<ParState>> row(String sagaId) {
    return probeRows.load(SagaId.of(sagaId), SAGA_TYPE, ParState.class);
  }

  /** The events that name the saga, in global order. */
  List<DomainEvent> eventsOfSaga(String sagaId) {
    return stored().stream().map(EventEnvelope::event).filter(e -> sagaId.equals(idOf(e))).toList();
  }

  /**
   * The rows of the fence's invariant ({@link SagaFenceNodes#LATE_FORWARD_COMMANDS}): a forward
   * command stored after its saga's first compensation command. Must be empty.
   */
  List<String> lateForwardCommands() throws SQLException {
    List<String> late = new ArrayList<>();
    try (Connection conn = source.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(SagaFenceNodes.LATE_FORWARD_COMMANDS)) {
      while (rs.next()) {
        late.add(rs.getString(1) + " after " + rs.getString(2));
      }
    }
    return late;
  }

  /** What the database holds now. */
  EndState endState() throws SQLException {
    List<EventEnvelope> stored = stored();
    Map<Long, String> names = new HashMap<>();
    Map<StreamId, List<DomainEvent>> events = new LinkedHashMap<>();
    for (EventEnvelope e : stored) {
      names.put(
          e.globalOffset().value(),
          e.streamId().aggregateType().value()
              + "/"
              + e.streamId().aggregateId().value()
              + "@"
              + e.version().value());
      events.computeIfAbsent(e.streamId(), s -> new ArrayList<>()).add(e.event());
    }
    LongFunction<String> named =
        offset ->
            Objects.requireNonNull(
                names.get(offset), "a recorded offset names no stored event: " + offset);
    Map<String, Row> rows = new TreeMap<>();
    Set<String> inboxKeys = new TreeSet<>();
    try (Connection conn = source.getConnection();
        Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT saga_id FROM saga_state")) {
        while (rs.next()) {
          String sagaId = rs.getString(1);
          LoadedSaga<ParState> row = row(sagaId).orElseThrow();
          rows.put(
              sagaId,
              new Row(
                  row.status(),
                  row.version(),
                  row.genesisApplied(),
                  row.deadLetterPending(),
                  row.preFaultStatus(),
                  row.episodeVersion(),
                  row.lastAppliedOffset() == null ? null : named.apply(row.lastAppliedOffset()),
                  row.state().status(),
                  row.state().sagaId(),
                  row.state().applied().stream().map(named::apply).toList()));
        }
      }
      try (ResultSet rs = stmt.executeQuery("SELECT idempotency_key FROM command_inbox")) {
        while (rs.next()) {
          inboxKeys.add(rs.getString(1));
        }
      }
    }
    Map<String, List<DeadLetter>> deadLetters = new TreeMap<>();
    for (SagaDeadLetterEntry entry : probeDeadLetters.findAll(10_000)) {
      String saga = entry.sagaId() == null ? "<null-saga>" : entry.sagaId().value();
      deadLetters
          .computeIfAbsent(saga, s -> new ArrayList<>())
          .add(new DeadLetter(named.apply(entry.eventOffset().value()), entry.errorType()));
    }
    deadLetters.values().forEach(l -> l.sort(Comparator.comparing(DeadLetter::event)));
    Map<String, List<Command>> bySaga = new TreeMap<>();
    for (Command command : commands) {
      bySaga.computeIfAbsent(sagaIdOf(command), s -> new ArrayList<>()).add(command);
    }
    bySaga.replaceAll((saga, list) -> List.copyOf(new LinkedHashSet<>(list)));
    return new EndState(events, rows, inboxKeys, deadLetters, checkpoint(), bySaga);
  }

  /** One process over the database, with its crash point. */
  final class Node implements AutoCloseable {
    final Crash crash;
    final VirtualThreadCommandBus realBus;
    final SagaRunner<ParState> runner;
    final PollingEventSubscription subscription;

    /**
     * The node's timeout runner, built from its runner, on the fixture's {@link #clock}: it claims
     * nothing until a test moves the clock past {@link #DEADLINE}.
     */
    final SagaTimeoutRunner<ParState> timeoutRunner;

    private final AggregateLocker locker;

    private Node(Point crashAt, String crashOn, Options options) {
      crash = new Crash(crashAt, crashOn);
      DataSource source =
          options.source() != null ? options.source() : SagaParallelPostgresNodes.this.source;
      locker =
          options.locker() != null ? options.locker().apply(source) : new LocalStripedLocker(64);
      Shape shape = new Shape(compensationCommands, stepsToComplete, stepAggregate);
      PostgresCommandInbox inbox = new PostgresCommandInbox(source);
      PostgresEventStore eventStore =
          PostgresEventStore.builder()
              .dataSource(source)
              .typeRegistry(REGISTRY)
              .commandInbox(inbox)
              .build();
      realBus =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .locker(locker)
              .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
              .objectMapper(new ObjectMapper())
              .commandInbox(inbox)
              .register(
                  STEPS, Step.class, c -> shape.stepAggregateOf(c.sagaId()), new StepDecider())
              .register(UNDOS, Undo.class, c -> AggregateId.of(c.sagaId()), new UndoDecider())
              .build();
      SagaOrchestrator<ParState> saga = orchestrator(shape);
      CommandBus bus =
          new CrashingBus(
              new ForwardingCommandBus(realBus) {
                @Override
                public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
                  commands.add(command);
                  return super.execute(command, key);
                }
              },
              crash,
              SagaParallelPostgresNodes::sagaIdOf);
      CrashingSagaStore sagaStore =
          new CrashingSagaStore(new PostgresSagaStore(source, SagaFenceNodes.mapper()), crash);
      runner =
          SagaRunner.<ParState>builder()
              .orchestrator(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(
                  new CrashingDeadLetters(new PostgresSagaDeadLetterStore(source), crash))
              .forwardFence(true)
              .parallelism(options.parallelism())
              .metrics(options.metrics())
              .build();
      subscription =
          PollingEventSubscription.builder()
              .subscriptionName(SUBSCRIPTION)
              .eventStore(eventStore)
              .offsetStore(new CrashingOffsets(new PostgresOffsetStore(source), crash))
              .sagaRunner(runner)
              .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ZERO))
              .build();
      SagaTimeoutRunner.Builder<ParState> timeouts =
          SagaTimeoutRunner.<ParState>builder()
              .decider(withDeadline(saga, options.deadline()))
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(SAGA_TYPE)
              .clock(options.timeoutClock() != null ? options.timeoutClock() : clock)
              .pollInterval(options.timeoutPollInterval())
              .metrics(options.metrics())
              .sagaRunner(runner);
      if (options.claimWait() != null) {
        SagaTimeoutRunnerTestAccess.claimWait(timeouts, options.claimWait());
      }
      timeoutRunner = timeouts.build();
    }

    /** One poll of the subscription on the calling thread: a drain to the head of the store. */
    int pollOnce() {
      return subscription.triggerImmediatePoll();
    }

    /** One cycle of the node: its timeout runner polls, then its subscription drains. */
    int cycle() {
      SagaTimeoutRunnerTestAccess.pollOnce(timeoutRunner);
      return pollOnce();
    }

    @Override
    public void close() {
      subscription.close();
      timeoutRunner.close();
      realBus.close();
      if (locker instanceof AutoCloseable closeable) {
        try {
          closeable.close();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  /** {@link Step} stores one {@link Stepped}, or is rejected for a saga in {@link #rejecting}. */
  private final class StepDecider implements Decider<Step, StepAggregate, DomainEvent> {
    @Override
    public StepAggregate initialState() {
      return new StepAggregate();
    }

    @Override
    public List<DomainEvent> decide(Step command, StepAggregate state) {
      if (rejecting.contains(command.sagaId())) {
        throw new IllegalArgumentException("step rejected for " + command.sagaId());
      }
      return List.of(new Stepped(command.sagaId(), command.mark()));
    }

    @Override
    public StepAggregate evolve(StepAggregate state, DomainEvent event) {
      return state;
    }
  }

  /** {@link Undo} stores one {@link Undone}. */
  private static final class UndoDecider implements Decider<Undo, StepAggregate, DomainEvent> {
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
}
