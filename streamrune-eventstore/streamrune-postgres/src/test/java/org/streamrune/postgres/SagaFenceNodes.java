package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
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
import org.streamrune.runtime.SagaCompensationRetrySweeper;
import org.streamrune.runtime.SagaCompensationRetrySweeperTestAccess;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SagaTimeoutRunnerTestAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.MutableClock;

/**
 * Processes of one application over one PostgreSQL database, in one JVM, for the forward fence's
 * partition, crash-point and pooler tests.
 *
 * <p>A {@link Node} is what one process holds: its connections (a {@link DataSource} the test
 * builds — direct, through a silent peer, through PgBouncer, with a {@link PausingDataSource} on
 * top), its event store, command inbox and saga store over them, its command bus, a {@link
 * SagaRunner} with its own step lock and a {@link SagaTimeoutRunner} built from that runner.
 * Nothing in one node knows of another; they share only the database's rows. A process that dies is
 * rebuilt: a new {@code Node} over the same database, and the trigger event delivered again, as a
 * subscription restarting from its checkpoint delivers it.
 *
 * <p>The saga: the start event {@link Opened} issues {@link Reserve} and, with {@code
 * startAlsoShips}, {@link Ship}; the compensation is a {@link Release} and a {@link Refund}. The
 * saga of order {@code n} is {@code order-saga-n}; every command stores one event that carries the
 * order id and names the saga in its metadata.
 */
final class SagaFenceNodes {

  record Opened(String orderId) implements DomainEvent {}

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

  static final SagaType TYPE = SagaType.fromClass(State.class);

  /** The saga's deadline; a test moves a node's clock past it. */
  static final Duration TIMEOUT = Duration.ofMinutes(5);

  /** The bound of every wait a test makes for another thread or another backend. */
  static final long PATIENCE_SECONDS = 30;

  static final List<String> FORWARD_EVENTS = List.of("Reserved", "Shipped");
  static final List<String> COMPENSATION_EVENTS = List.of("Released", "Refunded");

  /** Must return no row: a forward command stored after its saga's first compensation command. */
  static final String LATE_FORWARD_COMMANDS =
      """
      SELECT f.idempotency_key, c.idempotency_key
        FROM command_inbox f
        JOIN command_inbox c
          ON split_part(c.idempotency_key, ':', 2) = split_part(f.idempotency_key, ':', 2)
       WHERE c.idempotency_key LIKE 'saga:%:episode:%:comp:%'
         AND f.idempotency_key LIKE 'saga:%'
         AND f.idempotency_key NOT LIKE 'saga:%:episode:%'
         AND (SELECT max(o) FROM unnest(f.global_offsets) o)
           > (SELECT min(o) FROM unnest(c.global_offsets) o)
      """;

  private static final Map<String, Class<?>> EVENT_TYPES =
      List.of(Opened.class, Reserved.class, Shipped.class, Released.class, Refunded.class).stream()
          .collect(Collectors.toMap(type -> EventType.fromClass(type).name(), type -> type));

  static final EventTypeRegistry REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          Class<?> type = EVENT_TYPES.get(eventType.name());
          if (type == null) {
            throw new IllegalArgumentException("Unknown event type: " + eventType);
          }
          return type;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("No aggregate snapshots here: " + stateType);
        }

        @Override
        public Collection<Class<?>> registeredTypes() {
          return EVENT_TYPES.values();
        }
      };

  /** The death of a process: an {@link Error}, which no saga code catches. */
  static final class ProcessDeath extends Error {
    ProcessDeath() {
      super("the process dies here", null, false, false);
    }
  }

  static SagaId sagaOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  static ObjectMapper mapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

  /** Stores one event per command. */
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

  static final class OrderSaga implements SagaDecider<State> {
    volatile boolean startAlsoShips;

    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, sagaId.value().substring("order-saga-".length()));
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return new State(SagaStatus.RUNNING, state.orderId());
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      String order = state.orderId();
      SagaCommand reserve = SagaCommand.of(new Reserve(order), AggregateId.of(order));
      SagaCommand ship = SagaCommand.of(new Ship(order), AggregateId.of(order));
      return event.event() instanceof Opened
          ? (startAlsoShips ? List.of(reserve, ship) : List.of(reserve))
          : List.of();
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      String order = state.orderId();
      return List.of(
          SagaCommand.of(new Release(order), AggregateId.of(order)),
          SagaCommand.of(new Refund(order), AggregateId.of(order)));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(TIMEOUT);
    }
  }

  /**
   * Runs the real bus. A hook, when set, runs once on the dispatching thread right before the first
   * command of the chosen class reaches the real bus; {@code whenRefused}, when set, runs once on
   * the dispatching thread right after the real bus refused a forward command at the fence, before
   * the refusal reaches the saga (a hook that throws replaces the refusal).
   */
  static final class HookedBus implements CommandBus {
    private record Hook(Class<? extends Command> before, Runnable action) {}

    private final CommandBus delegate;
    private final AtomicReference<Hook> hook = new AtomicReference<>();
    private final AtomicReference<Runnable> whenRefused = new AtomicReference<>();

    HookedBus(CommandBus delegate) {
      this.delegate = delegate;
    }

    void runOnceBefore(Class<? extends Command> command, Runnable action) {
      hook.set(new Hook(command, action));
    }

    void runOnceWhenRefused(Runnable action) {
      whenRefused.set(action);
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return delegate.execute(command);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      Hook armed = hook.get();
      if (armed != null && armed.before().isInstance(command) && hook.compareAndSet(armed, null)) {
        armed.action().run();
      }
      try {
        return delegate.execute(command, key);
      } catch (RuntimeException failed) {
        if (refusedAtTheFence(failed)) {
          Runnable once = whenRefused.getAndSet(null);
          if (once != null) {
            once.run();
          }
        }
        throw failed;
      }
    }

    private static boolean refusedAtTheFence(Throwable failed) {
      for (Throwable t = failed; t != null; t = t.getCause()) {
        if (t instanceof SagaForwardFencedException) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return delegate.supportsIdempotentExecution();
    }

    @Override
    public Object sagaFenceStorageIdentity() {
      return delegate.sagaFenceStorageIdentity();
    }
  }

  /** One process. */
  static final class Node implements AutoCloseable {
    final String name;
    final DataSource source;
    final PostgresEventStore eventStore;
    final PostgresSagaStore rows;

    /** Runs once right after the next {@code claimCompensating} of this node returned. */
    final AtomicReference<Runnable> afterTheNextClaim = new AtomicReference<>();

    final SagaStore sagaStore;
    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
    final OrderSaga saga = new OrderSaga();

    /** The saga type of every forward command this node's steps saw refused at the commit. */
    final List<String> fencedSamples = new CopyOnWriteArrayList<>();

    final VirtualThreadCommandBus realBus;
    final HookedBus bus;
    final SagaRunner<State> runner;
    final SagaTimeoutRunner<State> timeoutRunner;
    final SagaCompensationRetrySweeper<State> sweeper;

    /**
     * @param name the node's name
     * @param source the node's connections
     * @param clock the clock of the runner and the timeout runner
     * @param statementTimeout the event store's statement bound (and the append's idle bound)
     * @param rowWriteTimeout the saga store's row-write bound
     */
    Node(
        String name,
        DataSource source,
        Clock clock,
        Duration statementTimeout,
        Duration rowWriteTimeout) {
      this.name = name;
      this.source = source;
      PostgresCommandInbox inbox = new PostgresCommandInbox(source);
      eventStore =
          PostgresEventStore.builder()
              .dataSource(source)
              .typeRegistry(REGISTRY)
              .commandInbox(inbox)
              .statementTimeout(statementTimeout)
              .build();
      rows = new PostgresSagaStore(source, mapper(), rowWriteTimeout);
      sagaStore = withAHookAfterTheClaim(rows, afterTheNextClaim);
      StreamRuneMetrics metrics =
          new StreamRuneMetrics() {
            @Override
            public void recordSagaForwardCommandFenced(String sagaType) {
              fencedSamples.add(sagaType);
            }
          };
      realBus =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .locker(new LocalStripedLocker(64))
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
                  AggregateType.of("refund"),
                  Refund.class,
                  c -> AggregateId.of(c.orderId()),
                  refund)
              .build();
      bus = new HookedBus(realBus);
      runner =
          SagaRunner.<State>builder()
              .decider(saga)
              .startWhen(e -> e.event() instanceof Opened)
              .extractSagaId(e -> sagaOf(((Opened) e.event()).orderId()))
              .correlateByEventData((streamId, e) -> Optional.empty())
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(new PostgresSagaDeadLetterStore(source))
              .clock(clock)
              .metrics(metrics)
              .forwardFence(true)
              .build();
      timeoutRunner =
          SagaTimeoutRunner.<State>builder()
              .decider(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(TYPE)
              .clock(clock)
              .sagaRunner(runner)
              .batchSize(1_000)
              .build();
      sweeper =
          SagaCompensationRetrySweeper.<State>builder()
              .sagaRunner(runner)
              .sagaStore(sagaStore)
              .leadership(SubscriptionLeadership.NOOP)
              .retryInterval(Duration.ofSeconds(30))
              .giveUpAfter(Duration.ofHours(1))
              .clock(clock)
              .build();
    }

    /** Delivers one event to this node's runner, as its subscription does. */
    void deliver(EventEnvelope event) {
      runner.asEventListener().onEvents(List.of(event));
    }

    /** This node's timeout runner polls once. */
    void pollTimeouts() {
      SagaTimeoutRunnerTestAccess.pollOnce(timeoutRunner);
    }

    /** This node's compensation-retry sweeper runs one cycle. */
    void sweep() {
      SagaCompensationRetrySweeperTestAccess.sweepOnce(sweeper);
    }

    @Override
    public void close() {
      realBus.close();
    }
  }

  /** {@code rows}, with a hook that runs once right after a claim returned. */
  private static SagaStore withAHookAfterTheClaim(
      SagaStore rows, AtomicReference<Runnable> afterTheNextClaim) {
    return (SagaStore)
        Proxy.newProxyInstance(
            SagaStore.class.getClassLoader(),
            new Class<?>[] {SagaStore.class},
            (proxy, method, args) -> {
              Object result;
              try {
                result = method.invoke(rows, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (method.getName().equals("claimCompensating")) {
                Runnable once = afterTheNextClaim.getAndSet(null);
                if (once != null) {
                  once.run();
                }
              }
              return result;
            });
  }

  // ---- the database, read and written by the test itself ----

  private final DataSource direct;
  private final PostgresEventStore probe;
  private final PostgresSagaStore probeRows;
  private final List<Node> nodes = new ArrayList<>();

  /** The clock of the nodes; a case moves it past the deadline. */
  final MutableClock clock = MutableClock.startingAt(Instant.now());

  SagaFenceNodes(DataSource direct) {
    this.direct = direct;
    this.probe = PostgresEventStore.builder().dataSource(direct).typeRegistry(REGISTRY).build();
    this.probeRows = new PostgresSagaStore(direct, mapper());
  }

  /** Empties the tables a case writes. */
  void clean() throws SQLException {
    try (Connection conn = direct.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE saga_dead_letters, saga_state, command_inbox, event_stream");
    }
  }

  /** A node with the default bounds. */
  Node node(String name, DataSource source) {
    return node(name, source, StatementTimeout.DEFAULT, PostgresSagaStore.ROW_WRITE_TIMEOUT);
  }

  Node node(String name, DataSource source, Duration statementTimeout, Duration rowWriteTimeout) {
    Node node = new Node(name, source, clock, statementTimeout, rowWriteTimeout);
    nodes.add(node);
    return node;
  }

  void closeAll() {
    nodes.forEach(Node::close);
    nodes.clear();
  }

  /**
   * The clock moves a minute past the retry interval of a compensation episode last written now,
   * and {@code node}'s compensation-retry sweeper runs one cycle.
   */
  void sweep(Node node) {
    Instant now = Instant.now();
    Instant from = clock.instant().isAfter(now) ? clock.instant() : now;
    clock.setInstant(from.plus(Duration.ofMinutes(1)));
    node.sweep();
  }

  /**
   * The deadline of every saga started so far passes — the clock moves to a second past the
   * deadline of a row created now, by the database's clock — and {@code node}'s timeout runner
   * polls once. A compensation episode idle since before now is re-picked by the same poll.
   */
  void timeOut(Node node) {
    Instant now = Instant.now();
    Instant from = clock.instant().isAfter(now) ? clock.instant() : now;
    clock.setInstant(from.plus(TIMEOUT).plusSeconds(1));
    node.pollTimeouts();
  }

  /** Appends a trigger event as a request stores it, and returns it as a subscription reads it. */
  EventEnvelope append(DomainEvent event) {
    StreamId streamId =
        StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + UUID.randomUUID()));
    var appended =
        probe.append(
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
    return probe
        .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
        .get(0);
  }

  Optional<LoadedSaga<State>> row(String orderId) {
    return probeRows.load(sagaOf(orderId), TYPE, State.class);
  }

  SagaStatus status(String orderId) {
    return row(orderId).orElseThrow().status();
  }

  /** The stored forward and compensation events of the order's saga, in global order. */
  List<String> storedOrder(String orderId) {
    List<String> stored = new ArrayList<>();
    GlobalOffset after = GlobalOffset.initial();
    while (true) {
      List<EventEnvelope> page = probe.readGlobalStream(after, 1_000);
      if (page.isEmpty()) {
        return stored;
      }
      for (EventEnvelope event : page) {
        SagaOrigin origin = event.metadata().sagaOrigin();
        if (origin != null && origin.sagaId().equals(sagaOf(orderId))) {
          stored.add(event.event().getClass().getSimpleName());
        }
      }
      after = page.getLast().globalOffset();
    }
  }

  /** The keys of the command inbox's rows of the order's saga, in key order. */
  List<String> inboxKeys(String orderId) throws SQLException {
    List<String> keys = new ArrayList<>();
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT idempotency_key FROM command_inbox WHERE idempotency_key LIKE ?"
                    + " ORDER BY idempotency_key")) {
      ps.setString(1, "saga:" + sagaOf(orderId).value() + ":%");
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          keys.add(rs.getString(1));
        }
      }
    }
    return keys;
  }

  /**
   * The invariant: no forward command of any saga is stored after its first compensation command
   * (the inbox), no forward event follows a compensation event (the stream), and each saga's
   * compensation commands ran once, in one episode.
   */
  void assertTheFenceHeld(String orderId) throws SQLException {
    try (Connection conn = direct.getConnection();
        PreparedStatement ps = conn.prepareStatement(LATE_FORWARD_COMMANDS);
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).as("a forward command stored after a compensation command").isFalse();
    }
    List<String> stored = storedOrder(orderId);
    int firstCompensation = -1;
    for (int i = 0; i < stored.size(); i++) {
      if (COMPENSATION_EVENTS.contains(stored.get(i))) {
        firstCompensation = i;
        break;
      }
    }
    if (firstCompensation >= 0) {
      assertThat(stored.subList(firstCompensation, stored.size()))
          .as("no forward event after the first compensation event: " + stored)
          .noneMatch(FORWARD_EVENTS::contains);
    }
    for (String compensation : COMPENSATION_EVENTS) {
      assertThat(stored.stream().filter(compensation::equals).count())
          .as(compensation + " stored at most once: " + stored)
          .isLessThanOrEqualTo(1);
    }
    List<String> compensationKeys =
        inboxKeys(orderId).stream().filter(key -> key.contains(":episode:")).toList();
    assertThat(compensationKeys.stream().map(key -> key.split(":")[3]).distinct())
        .as("the compensation keys of one episode: " + compensationKeys)
        .hasSizeLessThanOrEqualTo(1);
  }

  // ---- backends ----

  /** The pid of the one backend of {@code applicationName} that is idle in its transaction. */
  int idleInTransaction(String applicationName) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE application_name = ?"
                    + " AND state = 'idle in transaction'")) {
      ps.setString(1, applicationName);
      while (System.nanoTime() < deadline) {
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return rs.getInt(1);
          }
        }
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    throw new AssertionError("no backend of " + applicationName + " idle in transaction");
  }

  /** The pid of a backend of {@code applicationName} that PostgreSQL reports blocked by holder. */
  int awaitBlockedBy(int holderPid, String applicationName) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE pid <> pg_backend_pid()"
                    + " AND application_name = ? AND ? = ANY(pg_blocking_pids(pid))")) {
      ps.setString(1, applicationName);
      ps.setInt(2, holderPid);
      while (System.nanoTime() < deadline) {
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return rs.getInt(1);
          }
        }
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    throw new AssertionError(
        "no backend of " + applicationName + " blocked by pid " + holderPid + " in time");
  }

  /** What {@code pg_stat_activity} reports of one backend's wait. */
  record Wait(String eventType, String event, String query) {}

  Wait waitOf(int pid) throws SQLException {
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT wait_event_type, wait_event, query FROM pg_stat_activity WHERE pid = ?")) {
      ps.setInt(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("backend %d is alive", pid).isTrue();
        return new Wait(rs.getString(1), rs.getString(2), rs.getString(3));
      }
    }
  }

  int backendsNamed(String applicationName) throws SQLException {
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = ?")) {
      ps.setString(1, applicationName);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  void terminate(int pid) throws SQLException {
    try (Connection conn = direct.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT pg_terminate_backend(?)")) {
      ps.setInt(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next() && rs.getBoolean(1)).as("backend %d terminated", pid).isTrue();
      }
    }
  }

  /** Waits until backend {@code pid} has ended. */
  void awaitGone(int pid) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE pid = ?")) {
      ps.setInt(1, pid);
      while (System.nanoTime() < deadline) {
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          if (rs.getInt(1) == 0) {
            return;
          }
        }
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    throw new AssertionError("backend " + pid + " did not end");
  }

  /**
   * Opens a helper transaction holding the offset counter's row: every append from here on waits at
   * its offset reservation, after its inbox claim and its fence read, until it ends.
   */
  Connection freezeAppends() throws SQLException {
    Connection freezer = direct.getConnection();
    freezer.setAutoCommit(false);
    try (PreparedStatement ps =
            freezer.prepareStatement(
                "SELECT next_value FROM global_offset_sequence WHERE id = 1 FOR UPDATE");
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).isTrue();
    }
    return freezer;
  }

  static int backendPid(Connection conn) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_backend_pid()");
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getInt(1);
    }
  }

  // ---- threads ----

  /** Runs {@code body} on a new daemon thread; the future carries what it threw. */
  static CompletableFuture<Void> onAnotherThread(Runnable body) {
    CompletableFuture<Void> done = new CompletableFuture<>();
    Thread.ofPlatform()
        .daemon(true)
        .start(
            () -> {
              try {
                body.run();
                done.complete(null);
              } catch (Throwable thrown) {
                done.completeExceptionally(thrown);
              }
            });
    return done;
  }

  /** What the thread threw, or {@code null} when it returned normally. */
  static Throwable outcomeOf(CompletableFuture<Void> thread) {
    try {
      thread.get(PATIENCE_SECONDS, TimeUnit.SECONDS);
      return null;
    } catch (ExecutionException failed) {
      return failed.getCause();
    } catch (TimeoutException | InterruptedException stuck) {
      throw new AssertionError("the other thread did not finish", stuck);
    }
  }
}
