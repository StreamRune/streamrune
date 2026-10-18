package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
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
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresSagaDeadLetterStore;
import org.streamrune.postgres.PostgresSagaStore;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.SagaDeadLetterReplayer;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SagaTimeoutRunnerTestAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.MutableClock;

/**
 * Processes over one PostgreSQL database, in one JVM. A {@link Node} is what one process of an
 * application holds: its own {@link PGSimpleDataSource} (its own physical connections, named by
 * {@code application_name}; under a load, a connection pool of its own over it), its own event
 * store, command inbox, saga store and dead-letter store over it, its own command bus with its own
 * aggregate locks, a {@link SagaRunner} with its own step lock, a {@link SagaTimeoutRunner} built
 * from that runner, a {@link SagaDeadLetterReplayer} and its own metrics. Nothing in one node knows
 * of another; they share only the database's rows — so only the event store's forward fence keeps a
 * forward command of one node from being stored after a compensation another node ran.
 *
 * <p>The saga: the start event {@link Opened} issues {@link Reserve} and {@link Ship}, {@link
 * Advanced} issues {@link Ship}, {@link Completed} completes the saga, and the compensation is a
 * {@link Release} and a {@link Refund}. The saga of order {@code n} is {@code order-saga-n}; every
 * command stores one event that carries the order id.
 *
 * <p>A test places another node's work at a chosen point of a step with a hook on the stepping
 * node's bus, or freezes every append after its fence read with a helper transaction that holds the
 * global offset counter's row ({@link #freezeAppends()}).
 */
abstract class SagaForwardFenceNodes extends E2ETestBase {

  record Opened(String orderId) implements DomainEvent {}

  /** A correlated event whose forward command is {@link Ship}; the saga stays RUNNING. */
  record Advanced(String orderId) implements DomainEvent {}

  /** A correlated event that completes the saga and issues no command. */
  record Completed(String orderId) implements DomainEvent {}

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

  /** The saga's deadline in the cases that move a clock past it. */
  static final Duration TIMEOUT = Duration.ofMinutes(5);

  /** The bound of every wait a test makes for another thread or another backend. */
  static final long PATIENCE_SECONDS = 30;

  /** The connections of a pooled node: its delivering threads, its timeout runner, and spare. */
  static final int POOL_SIZE = 8;

  static final List<String> FORWARD_EVENTS = List.of("Reserved", "Shipped");
  static final List<String> COMPENSATION_EVENTS = List.of("Released", "Refunded");

  private static final Map<String, Class<?>> EVENT_TYPES =
      List.of(
              Opened.class,
              Advanced.class,
              Completed.class,
              Reserved.class,
              Shipped.class,
              Released.class,
              Refunded.class)
          .stream()
          .collect(Collectors.toMap(type -> EventType.fromClass(type).name(), type -> type));

  /** Resolves the saga's trigger events and the events its commands store. */
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

  static SagaId sagaOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  /** The key of the {@code index}-th forward command the saga dispatched for {@code trigger}. */
  static String forwardKey(String orderId, EventEnvelope trigger, int index) {
    return "saga:" + sagaOf(orderId).value() + ":" + trigger.globalOffset().value() + ":" + index;
  }

  static ObjectMapper mapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

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
   * {@link Opened} issues {@link Reserve} (and {@link Ship} with {@code startAlsoShips}), {@link
   * Advanced} issues {@link Ship}, {@link Completed} completes the saga. Compensation is a release
   * and a refund.
   */
  static final class OrderSaga implements SagaDecider<State> {
    private final Duration timeout;
    volatile boolean startAlsoShips = true;

    OrderSaga(Duration timeout) {
      this.timeout = timeout;
    }

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
      return new State(
          event.event() instanceof Completed ? SagaStatus.COMPLETED : SagaStatus.RUNNING,
          state.orderId());
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      String order = state.orderId();
      SagaCommand reserve = SagaCommand.of(new Reserve(order), AggregateId.of(order));
      SagaCommand ship = SagaCommand.of(new Ship(order), AggregateId.of(order));
      return switch (event.event()) {
        case Opened opened -> startAlsoShips ? List.of(reserve, ship) : List.of(reserve);
        case Advanced advanced -> List.of(ship);
        default -> List.of();
      };
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
      return Optional.of(timeout);
    }
  }

  /**
   * Runs the real bus. A hook, when set, runs once, on the dispatching thread, right before the
   * first command of the chosen class reaches the real bus (a hook that throws fails that command
   * once); {@code beforeEveryCommand} runs before each keyed command, and {@code failure}, when it
   * answers an exception for a command and its key, fails that command before the real bus sees it.
   */
  static final class HookedBus implements CommandBus {
    private record Hook(Class<? extends Command> before, Runnable action) {}

    private final CommandBus delegate;
    private final AtomicReference<Hook> hook = new AtomicReference<>();
    volatile Consumer<Command> beforeEveryCommand;
    volatile BiFunction<Command, IdempotencyKey, RuntimeException> failure;

    HookedBus(CommandBus delegate) {
      this.delegate = delegate;
    }

    void runOnceBefore(Class<? extends Command> command, Runnable action) {
      hook.set(new Hook(command, action));
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
      Consumer<Command> every = beforeEveryCommand;
      if (every != null) {
        every.accept(command);
      }
      BiFunction<Command, IdempotencyKey, RuntimeException> failing = failure;
      if (failing != null) {
        RuntimeException thrown = failing.apply(command, key);
        if (thrown != null) {
          throw thrown;
        }
      }
      return delegate.execute(command, key);
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

  /** One process: its connections, stores, bus, runner, timeout runner, replayer and metrics. */
  static final class Node implements AutoCloseable {
    final String name;

    /** The node's connections: a new physical connection per use, or a pool of its own. */
    final DataSource source;

    private final HikariDataSource pool;
    final PostgresCommandInbox inbox;
    final PostgresEventStore eventStore;
    final PostgresSagaStore rows;
    final PostgresSagaDeadLetterStore deadLetters;

    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
    final OrderSaga saga;

    /** The saga type of every forward command this node's steps saw refused at the commit. */
    final List<String> fencedSamples = new CopyOnWriteArrayList<>();

    /** The saga type of every compensation episode this node found opened under its step. */
    final List<String> underStepSamples = new CopyOnWriteArrayList<>();

    /** Every command that reached this node's bus, in order. */
    final List<Class<?>> dispatched = new CopyOnWriteArrayList<>();

    /** The saga type of every step this node's runner ran again after a lost compare-and-swap. */
    final List<String> deliveryRetriedSamples = new CopyOnWriteArrayList<>();

    /** The saga type of every compare-and-swap of a saga row this node lost. */
    final List<String> casConflictSamples = new CopyOnWriteArrayList<>();

    final StreamRuneMetrics metrics =
        new StreamRuneMetrics() {
          @Override
          public void recordSagaForwardCommandFenced(String sagaType) {
            fencedSamples.add(sagaType);
          }

          @Override
          public void recordSagaCompensationUnderForwardStep(String sagaType) {
            underStepSamples.add(sagaType);
          }

          @Override
          public void recordSagaDeliveryRetried(String sagaType) {
            deliveryRetriedSamples.add(sagaType);
          }

          @Override
          public void recordSagaCasConflict(String sagaType) {
            casConflictSamples.add(sagaType);
          }
        };

    final VirtualThreadCommandBus realBus;
    final HookedBus bus;
    final SagaRunner<State> runner;
    final SagaTimeoutRunner<State> timeoutRunner;
    final SagaDeadLetterReplayer replayer;

    /**
     * @param name the node's {@code application_name}
     * @param forwardFence whether the node's runner fences its forward commands at their commit
     * @param clock the clock of the runner and of the timeout runner
     * @param timeout the saga's deadline, counted from the row's creation
     * @param pooled whether the node holds a connection pool of its own, as a process under load
     *     does; without one every use opens a physical connection
     */
    Node(String name, boolean forwardFence, Clock clock, Duration timeout, boolean pooled) {
      this(name, forwardFence, clock, timeout, pooled, null, null);
    }

    /**
     * @param leadership the leadership the node's runner delivers its saga type under; {@code null}
     *     for none
     * @param timeoutPollInterval the pause between two polls of the node's timeout runner; {@code
     *     null} for the timeout runner's default
     */
    Node(
        String name,
        boolean forwardFence,
        Clock clock,
        Duration timeout,
        boolean pooled,
        SubscriptionLeadership leadership,
        Duration timeoutPollInterval) {
      this.name = name;
      PGSimpleDataSource physical = new PGSimpleDataSource();
      physical.setUrl(POSTGRES.getJdbcUrl());
      physical.setUser(POSTGRES.getUsername());
      physical.setPassword(POSTGRES.getPassword());
      physical.setApplicationName(name);
      if (pooled) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(physical);
        config.setPoolName(name);
        config.setMaximumPoolSize(POOL_SIZE);
        // A saturated pool fails a use within seconds, inside every wait of the tests.
        config.setConnectionTimeout(5_000);
        pool = new HikariDataSource(config);
        source = pool;
      } else {
        pool = null;
        source = physical;
      }
      inbox = new PostgresCommandInbox(source);
      eventStore =
          PostgresEventStore.builder()
              .dataSource(source)
              .typeRegistry(REGISTRY)
              .commandInbox(inbox)
              .build();
      rows = new PostgresSagaStore(source, mapper());
      deadLetters = new PostgresSagaDeadLetterStore(source);
      saga = new OrderSaga(timeout);
      realBus =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .locker(new LocalStripedLocker(64))
              .retryPolicy(new RetryPolicy(5, Duration.ofMillis(1), 1.0))
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
      bus.beforeEveryCommand = command -> dispatched.add(command.getClass());
      runner =
          SagaRunner.<State>builder()
              .decider(saga)
              .startWhen(e -> e.event() instanceof Opened)
              .extractSagaId(e -> sagaOf(((Opened) e.event()).orderId()))
              .correlateByEventData(
                  (streamId, e) ->
                      switch (e) {
                        case Advanced advanced -> Optional.of(sagaOf(advanced.orderId()));
                        case Completed completed -> Optional.of(sagaOf(completed.orderId()));
                        default -> Optional.empty();
                      })
              .sagaStore(rows)
              .commandBus(bus)
              .sagaDeadLetterStore(deadLetters)
              .clock(clock)
              .metrics(metrics)
              .forwardFence(forwardFence)
              .leadership(leadership)
              .build();
      SagaTimeoutRunner.Builder<State> timeouts =
          SagaTimeoutRunner.<State>builder()
              .decider(saga)
              .sagaStore(rows)
              .commandBus(bus)
              .sagaType(TYPE)
              .clock(clock)
              .sagaRunner(runner)
              .batchSize(1_000)
              .metrics(metrics);
      if (timeoutPollInterval != null) {
        timeouts.pollInterval(timeoutPollInterval);
      }
      timeoutRunner = timeouts.build();
      replayer =
          SagaDeadLetterReplayer.builder()
              .sagaDeadLetterStore(deadLetters)
              .sagaStore(rows)
              .eventStore(eventStore)
              .sagaRunner(runner)
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

    @Override
    public void close() {
      realBus.close();
      if (pool != null) {
        pool.close();
      }
    }
  }

  // ---- per test ----

  /** The clock of the nodes the deterministic cases build; a case moves it past the deadline. */
  final MutableClock clock = MutableClock.startingAt(Instant.now());

  private final List<Node> nodes = new ArrayList<>();
  private final List<Connection> helpers = new ArrayList<>();
  PostgresEventStore probe;
  PostgresSagaStore probeRows;

  @BeforeEach
  void cleanSagaTables() throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_dead_letters");
      stmt.execute("DELETE FROM saga_state");
      stmt.execute("DELETE FROM command_inbox");
    }
    probe = PostgresEventStore.builder().dataSource(dataSource).typeRegistry(REGISTRY).build();
    probeRows = new PostgresSagaStore(dataSource, mapper());
  }

  @AfterEach
  void closeNodesAndHelpers() throws SQLException {
    for (Connection helper : helpers) {
      try {
        if (!helper.getAutoCommit()) {
          helper.rollback();
        }
      } catch (SQLException ignored) {
        // closing below is what matters
      }
      helper.close();
    }
    helpers.clear();
    nodes.forEach(Node::close);
    nodes.clear();
  }

  /** A node on {@link #clock} with the five-minute deadline. */
  Node node(String name, boolean forwardFence) {
    return node(name, forwardFence, clock, TIMEOUT);
  }

  /** A node with its own connection pool, on {@code nodeClock}, with {@code timeout}. */
  Node pooledNode(String name, boolean forwardFence, Clock nodeClock, Duration timeout) {
    return track(new Node(name, forwardFence, nodeClock, timeout, true));
  }

  private Node node(String name, boolean forwardFence, Clock nodeClock, Duration timeout) {
    return track(new Node(name, forwardFence, nodeClock, timeout, false));
  }

  private Node track(Node node) {
    nodes.add(node);
    return node;
  }

  /** Closes {@code node} before the end of the test: its bus and its pool's connections. */
  void close(Node node) {
    nodes.remove(node);
    node.close();
  }

  /** The saga's deadline passes and {@code node}'s timeout runner polls once. */
  void timeOut(Node node) {
    clock.advance(TIMEOUT.plusSeconds(1));
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

  LoadedSaga<State> row(String orderId) {
    return probeRows.load(sagaOf(orderId), TYPE, State.class).orElseThrow();
  }

  /** The stored forward and compensation events of the order's saga, in global order. */
  List<String> storedOrder(String orderId) {
    return SagaForwardFenceInvariant.storedBySaga(probe)
        .getOrDefault(sagaOf(orderId).value(), List.of());
  }

  // ---- freezing appends after their fence read ----

  /**
   * Opens a helper transaction holding the offset counter's row: every append from here on waits at
   * its offset reservation, after its inbox claim and its fence read, until {@code commit()}.
   */
  Connection freezeAppends() throws SQLException {
    Connection freezer = dataSource.getConnection();
    helpers.add(freezer);
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

  /** What {@code pg_stat_activity} reports of one backend. */
  record Activity(String applicationName, String waitEventType, String waitEvent, String query) {}

  Activity activity(int pid) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT application_name, wait_event_type, wait_event, query"
                    + " FROM pg_stat_activity WHERE pid = ?")) {
      ps.setInt(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("backend %d is alive", pid).isTrue();
        return new Activity(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
      }
    }
  }

  /**
   * Returns the pid of a backend of {@code applicationName} that PostgreSQL reports blocked by
   * {@code holderPid}. The poll interval only bounds how quickly the wait is noticed; the exit
   * condition is the server's own report of the wait.
   */
  int awaitBlockedBy(int holderPid, String applicationName) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
    try (Connection conn = dataSource.getConnection();
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

  // ---- threads ----

  /**
   * Runs {@code body} on a new daemon platform thread; the future carries what it threw. A daemon:
   * a thread a failed test leaves behind does not keep the test JVM alive.
   */
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
