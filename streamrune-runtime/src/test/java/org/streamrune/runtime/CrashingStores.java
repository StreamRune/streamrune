package org.streamrune.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDeadLetterStore.HoldResult;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.saga.SagaStore.CompensatingSaga;
import org.streamrune.core.saga.SagaStore.RowFacts;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.SagaParallelFixtures.ParState;
import org.streamrune.runtime.SagaParallelFixtures.World;
import org.streamrune.test.ForwardingCommandBus;
import org.streamrune.test.ForwardingOffsetStore;
import org.streamrune.test.ForwardingSagaDeadLetterStore;

/**
 * Stops a node at a named point: the decorated call throws {@link NodeCrashed}, and every later
 * call of that node does too. A node is a {@link SagaRunner} and its {@link
 * PollingEventSubscription} over the shared stores of a {@link World}, built from subclasses of the
 * kit's decorators — {@link org.streamrune.test.ForwardingSagaStore}, {@link
 * ForwardingSagaDeadLetterStore}, {@link ForwardingOffsetStore} and {@link ForwardingCommandBus} —
 * that call {@code super} and throw {@link NodeCrashed} before or after it at their point. There is
 * no hand-written forwarding: the kit's decorators are held to the SPI by their own tests.
 *
 * <p>A crashed node is abandoned: its threads end on the same {@link Error}, nothing of it runs
 * again, and a test builds a fresh node with no crash point over the same stores. A point counts
 * the calls that concern one saga (the node's {@code crashOn}); a point of the offset store counts
 * the node's calls, whatever the saga.
 *
 * <p>Besides the crash, a call can fail once per world ({@link #failOnce}); and a test can hold or
 * observe every call through a node's {@link Node#beforeCall}, {@link Node#afterCall} and {@link
 * Node#onInjectedFailure}. A node also has a timeout runner built from its runner, as a process
 * does; its deadline is an hour away.
 */
final class CrashingStores {

  /** The decorator a point is in. */
  enum Store {
    SAGA_STORE,
    DEAD_LETTERS,
    OFFSETS,
    BUS
  }

  /** Where a node stops: a call, before or after its delegate ran, and the number of the call. */
  enum Point {
    /** A worker took the saga's group; its first statement (the row's load) is not made. */
    BEFORE_FIRST_STATEMENT(Store.SAGA_STORE, "load", false, 1),
    /** The start event's genesis-pending row is written; no forward command is. */
    AFTER_GENESIS_PENDING(Store.SAGA_STORE, "createGenesisPending", true, 1),
    /** The forward command committed (its events and its inbox key); no row write is made. */
    AFTER_FORWARD_COMMAND_COMMIT(Store.BUS, "Step", true, 1),
    /** The saga row recorded the event; the page has not ended. */
    AFTER_ROW_WRITE(Store.SAGA_STORE, "applyEvent", true, 1),
    /** Two events of the saga's group are handled; the third is not started. */
    BETWEEN_EVENTS_OF_ONE_SAGA(Store.SAGA_STORE, "load", false, 3),
    /** A rejected step's claim is written ({@code COMPENSATING}, stamped). */
    AFTER_COMPENSATION_CLAIM(Store.SAGA_STORE, "claimCompensating", true, 1),
    /** The first of two compensation commands committed, the second not. */
    AFTER_FIRST_COMPENSATION_COMMAND(Store.BUS, "Undo", true, 1),
    /** The compensation commands committed; the terminal write is not made. */
    BEFORE_TERMINAL_WRITE(Store.SAGA_STORE, "update", false, 1),
    /** A poison, before its entry (entry and shield are one write): nothing is recorded. */
    BEFORE_POISON_ENTRY(Store.DEAD_LETTERS, "publishShielded", false, 1),
    /** A poison, after its entry and shield, before the fault write. */
    AFTER_POISON_ENTRY(Store.DEAD_LETTERS, "publishShielded", true, 1),
    /** A poison, after the fault write. */
    AFTER_FAULT_WRITE(Store.SAGA_STORE, "markFaulted", true, 1),
    /** A hold, before its entry (entry and shield are one write): nothing of it is recorded. */
    BEFORE_HOLD_ENTRY(Store.DEAD_LETTERS, "publishShieldedIfAbsent", false, 1),
    /** A hold, after its entry and shield. */
    AFTER_HOLD_ENTRY(Store.DEAD_LETTERS, "publishShieldedIfAbsent", true, 1),
    /** The routing phase wrote a null-saga entry; no step has started. */
    AFTER_NULL_SAGA_ENTRY(Store.DEAD_LETTERS, "publish", true, 1),
    /**
     * A saga failed with a failure that does not stop the page, every other group ran to its end
     * and the listener threw; the node stops at the redelivery's checkpoint read.
     */
    AFTER_FAILED_PAGE_THREW(Store.OFFSETS, "getLastOffset", false, 2),
    /**
     * A connection-class failure stopped the page, the items in flight finished and the listener
     * threw; the node stops at the redelivery's checkpoint read.
     */
    AFTER_PAGE_STOP(Store.OFFSETS, "getLastOffset", false, 2),
    /**
     * The third failure of the page stopped it, the items in flight finished and the listener
     * threw; the node stops at the redelivery's checkpoint read.
     */
    AFTER_THIRD_FAILURE_STOP(Store.OFFSETS, "getLastOffset", false, 2),
    /**
     * Two sagas of the page failed, which does not stop it, and the listener threw; the node stops
     * at the redelivery's checkpoint read.
     */
    AFTER_SECOND_FAILURE(Store.OFFSETS, "getLastOffset", false, 2),
    /**
     * A saga failed and its page still runs: the node stops after the row write of another saga's
     * group, while the step of a third saga is in flight.
     */
    WHILE_FAILED_PAGE_RUNS_OTHERS(Store.SAGA_STORE, "applyEvent", true, 1),
    /** Every group ended; the checkpoint is not saved. */
    BEFORE_SAVE_OFFSET(Store.OFFSETS, "saveOffset", false, 1),
    /** The checkpoint is saved. */
    AFTER_SAVE_OFFSET(Store.OFFSETS, "saveOffset", true, 1);

    final Store store;
    final String method;
    final boolean afterTheDelegate;
    final int nthCall;

    Point(Store store, String method, boolean afterTheDelegate, int nthCall) {
      this.store = store;
      this.method = method;
      this.afterTheDelegate = afterTheDelegate;
      this.nthCall = nthCall;
    }
  }

  /** The death of a node: an {@link Error}, so that no retry loop of the node swallows it. */
  static final class NodeCrashed extends Error {
    NodeCrashed(String where) {
      super("the node crashed " + where, null, false, false);
    }
  }

  /** The name of every node's subscription: the nodes of a world share its checkpoint. */
  static final SubscriptionName SUBSCRIPTION = SubscriptionName.of("par-crash");

  /** The checkpoint of {@link #SUBSCRIPTION}. */
  static final ProjectionName CHECKPOINT = ProjectionName.of(SUBSCRIPTION.value());

  /** The deadline of the saga as the nodes' timeout runners see it. */
  static final Duration DEADLINE = Duration.ofHours(1);

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

  private final World world;
  private final SagaOrchestrator<ParState> saga;
  private final int parallelism;
  private final Map<String, Supplier<? extends Throwable>> failures = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> failureCalls = new ConcurrentHashMap<>();

  /**
   * @param world the stores every node of this world shares
   * @param saga the saga every node runs
   * @param parallelism the parallelism of every node's runner
   */
  CrashingStores(World world, SagaOrchestrator<ParState> saga, int parallelism) {
    this.world = world;
    this.saga = saga;
    this.parallelism = parallelism;
  }

  /**
   * The next call of {@code method} for {@code sagaId} by any node of this world throws {@code
   * failure} before its delegate runs; the calls after it do not. A command is named by its class's
   * simple name.
   */
  CrashingStores failOnce(String method, String sagaId, Supplier<? extends Throwable> failure) {
    return failOnce(method, sagaId, 1, failure);
  }

  /**
   * The {@code nthCall}-th call of {@code method} for {@code sagaId} by the nodes of this world,
   * counted from the first registration for that method and saga, throws {@code failure} before its
   * delegate runs.
   */
  CrashingStores failOnce(
      String method, String sagaId, int nthCall, Supplier<? extends Throwable> failure) {
    failureCalls.putIfAbsent(key(method, sagaId), new AtomicInteger());
    failures.put(key(method, sagaId) + " #" + nthCall, failure);
    return this;
  }

  /** A node that stops at {@code crashAt} on the saga {@code crashOn}. */
  Node node(Point crashAt, String crashOn) {
    return new Node(crashAt, crashOn, world.commandBus());
  }

  /** A node that does not crash. */
  Node node() {
    return new Node(null, null, world.commandBus());
  }

  /** A node of another process: a bus of its own ({@link World#anotherProcessBus()}), no crash. */
  Node nodeOfAnotherProcess() {
    return new Node(null, null, world.anotherProcessBus());
  }

  private static String key(String method, String sagaId) {
    return method + " " + sagaId;
  }

  private void injectFailureIfDue(String method, String sagaId, Node node) {
    AtomicInteger counted = failureCalls.get(key(method, sagaId));
    if (counted == null) {
      return;
    }
    Supplier<? extends Throwable> failure =
        failures.remove(key(method, sagaId) + " #" + counted.incrementAndGet());
    if (failure != null) {
      node.calls.add("failed " + key(method, sagaId));
      node.onInjectedFailure.accept(method, sagaId);
      throw CrashingStores.<RuntimeException>sneaky(failure.get());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T sneaky(Throwable failure) throws T {
    throw (T) failure;
  }

  private static String idOf(SagaId sagaId) {
    return sagaId == null ? null : sagaId.value();
  }

  /** One process: a runner and its subscription over the world's stores, and its crash point. */
  final class Node {
    private final Point crashAt;
    private final String crashOn;
    private final AtomicBoolean crashed = new AtomicBoolean();
    private final CountDownLatch crash = new CountDownLatch(1);
    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    /** Every call whose delegate returned, as {@code "<method> <saga id>"}, in order. */
    final List<String> calls = new CopyOnWriteArrayList<>();

    /** Runs first in every call, with the method and the saga id (a command: its class name). */
    volatile BiConsumer<String, String> beforeCall = (method, sagaId) -> {};

    /** Runs after every call whose delegate returned. */
    volatile BiConsumer<String, String> afterCall = (method, sagaId) -> {};

    /**
     * Runs on the failing thread when a call fails by {@link CrashingStores#failOnce}, with the
     * method and the saga id, right before the failure is thrown.
     */
    volatile BiConsumer<String, String> onInjectedFailure = (method, sagaId) -> {};

    /** Runs once, on the thread that crashes the node, right before it throws. */
    volatile Runnable onCrash = () -> {};

    final NodeSagaStore sagaStore;
    final NodeDeadLetters deadLetters;
    final NodeOffsets offsets;
    final NodeBus bus;
    final SagaRunner<ParState> runner;
    final PollingEventSubscription subscription;

    /**
     * The node's timeout runner, built from its runner. It reads the system clock, as the world's
     * saga rows do, and the saga's deadline ({@link #DEADLINE}) is an hour away: in these tests a
     * poll of it reads the timed-out sagas and finds none. A test that needs a claim builds a
     * timeout runner of its own with a clock past the deadline.
     */
    final SagaTimeoutRunner<ParState> timeoutRunner;

    private Node(Point crashAt, String crashOn, CommandBus processBus) {
      this.crashAt = crashAt;
      this.crashOn = crashOn;
      sagaStore = new NodeSagaStore(this, world.rows);
      deadLetters = new NodeDeadLetters(this, world.deadLetters);
      offsets = new NodeOffsets(this, world.offsets);
      bus = new NodeBus(this, processBus);
      runner =
          world
              .runner()
              .orchestrator(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(deadLetters)
              .parallelism(parallelism)
              .build();
      subscription =
          PollingEventSubscription.builder()
              .subscriptionName(SUBSCRIPTION)
              .eventStore(world.eventStore)
              .offsetStore(offsets)
              .sagaRunner(runner)
              .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ZERO))
              .build();
      timeoutRunner =
          SagaTimeoutRunner.<ParState>builder()
              .decider(withDeadline(saga, DEADLINE))
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(SagaParallelFixtures.SAGA_TYPE)
              .sagaRunner(runner)
              .build();
    }

    /** One poll of the subscription on the calling thread: a drain to the head of the store. */
    int pollOnce() {
      return subscription.pollOnce();
    }

    /** One cycle of the node: its timeout runner polls, then its subscription drains. */
    int cycle() {
      timeoutRunner.pollOnce();
      return subscription.pollOnce();
    }

    /** Whether this node crashed. */
    boolean crashed() {
      return crashed.get();
    }

    /** Waits until this node crashed; fails the test when it does not. */
    void awaitCrash() {
      SagaParallelFixtures.await(crash, "the node's crash at " + crashAt);
    }

    <T> T call(Store store, String method, String sagaId, Supplier<T> delegate) {
      beforeCall.accept(method, sagaId);
      if (crashed.get()) {
        throw new NodeCrashed("before " + method + " of " + sagaId + ": it crashed earlier");
      }
      boolean atThePoint = atThePoint(store, method, sagaId);
      injectFailureIfDue(method, sagaId, this);
      if (atThePoint && !crashAt.afterTheDelegate) {
        crash("before " + method + " of " + sagaId);
      }
      T result = delegate.get();
      calls.add(key(method, sagaId));
      afterCall.accept(method, sagaId);
      if (atThePoint && crashAt.afterTheDelegate) {
        crash("after " + method + " of " + sagaId);
      }
      return result;
    }

    void run(Store store, String method, String sagaId, Runnable delegate) {
      call(
          store,
          method,
          sagaId,
          () -> {
            delegate.run();
            return null;
          });
    }

    private boolean atThePoint(Store store, String method, String sagaId) {
      if (crashAt == null || crashAt.store != store || !crashAt.method.equals(method)) {
        return false;
      }
      if (store != Store.OFFSETS && !Objects.equals(crashOn, sagaId)) {
        return false;
      }
      String counted = store == Store.OFFSETS ? method : key(method, sagaId);
      return counts.computeIfAbsent(counted, k -> new AtomicInteger()).incrementAndGet()
          == crashAt.nthCall;
    }

    private void crash(String where) {
      if (!crashed.compareAndSet(false, true)) {
        throw new NodeCrashed(where + ": it crashed earlier");
      }
      crash.countDown();
      onCrash.run();
      throw new NodeCrashed("at " + crashAt + ", " + where);
    }
  }

  /** A node's view of the world's saga rows. */
  static final class NodeSagaStore extends org.streamrune.test.ForwardingSagaStore {
    private final Node node;

    NodeSagaStore(Node node, SagaStore rows) {
      super(rows);
      this.node = node;
    }

    private void run(String method, SagaId sagaId, Runnable delegate) {
      node.run(Store.SAGA_STORE, method, idOf(sagaId), delegate);
    }

    private <T> T call(String method, SagaId sagaId, Supplier<T> delegate) {
      return node.call(Store.SAGA_STORE, method, idOf(sagaId), delegate);
    }

    @Override
    public void create(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      run("create", sagaId, () -> super.create(sagaId, sagaType, state, status, deadLetterPending));
    }

    @Override
    public void create(SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status) {
      run("create", sagaId, () -> super.create(sagaId, sagaType, state, status));
    }

    @Override
    public void update(
        SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status, long expected) {
      run("update", sagaId, () -> super.update(sagaId, sagaType, state, status, expected));
    }

    @Override
    public void createGenesisPending(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending,
        EventId episodeCause) {
      run(
          "createGenesisPending",
          sagaId,
          () ->
              super.createGenesisPending(
                  sagaId, sagaType, state, status, deadLetterPending, episodeCause));
    }

    @Override
    public void createGenesisPending(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      run(
          "createGenesisPending",
          sagaId,
          () -> super.createGenesisPending(sagaId, sagaType, state, status, deadLetterPending));
    }

    @Override
    public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
      run("createFaulted", sagaId, () -> super.createFaulted(sagaId, sagaType, initialState));
    }

    @Override
    public void applyEvent(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion,
        AppliedEvent applied) {
      run(
          "applyEvent",
          sagaId,
          () -> super.applyEvent(sagaId, sagaType, state, status, expectedVersion, applied));
    }

    @Override
    public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
      return call(
          "markFaulted", sagaId, () -> super.markFaulted(sagaId, sagaType, expectedVersion));
    }

    @Override
    public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
      run(
          "setDeadLetterPending",
          sagaId,
          () -> super.setDeadLetterPending(sagaId, sagaType, pending));
    }

    @Override
    public void claimCompensating(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        long expectedVersion,
        EventId episodeCause) {
      run(
          "claimCompensating",
          sagaId,
          () -> super.claimCompensating(sagaId, sagaType, state, expectedVersion, episodeCause));
    }

    @Override
    public void claimCompensating(
        SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
      run(
          "claimCompensating",
          sagaId,
          () -> super.claimCompensating(sagaId, sagaType, state, expectedVersion));
    }

    @Override
    public <S extends SagaState> Optional<LoadedSaga<S>> load(
        SagaId sagaId, SagaType sagaType, Class<S> stateType) {
      return call("load", sagaId, () -> super.load(sagaId, sagaType, stateType));
    }

    @Override
    public Optional<RowFacts> loadRowFacts(SagaId sagaId, SagaType sagaType) {
      return call("loadRowFacts", sagaId, () -> super.loadRowFacts(sagaId, sagaType));
    }

    @Override
    public void delete(SagaId sagaId, SagaType sagaType) {
      run("delete", sagaId, () -> super.delete(sagaId, sagaType));
    }

    @Override
    public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
      return call("findTimedOut", null, () -> super.findTimedOut(sagaType, cutoff, limit));
    }

    @Override
    public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
      return call("findByStatus", null, () -> super.findByStatus(sagaType, status, limit));
    }

    @Override
    public long countByStatus(SagaType sagaType, SagaStatus status) {
      return call("countByStatus", null, () -> super.countByStatus(sagaType, status));
    }

    @Override
    public List<CompensatingSaga> findCompensating(
        SagaType sagaType, Instant updatedBefore, int limit) {
      return call(
          "findCompensating", null, () -> super.findCompensating(sagaType, updatedBefore, limit));
    }
  }

  /** A node's view of the world's dead-letter store. */
  static final class NodeDeadLetters extends ForwardingSagaDeadLetterStore {
    private final Node node;

    NodeDeadLetters(Node node, SagaDeadLetterStore deadLetters) {
      super(deadLetters);
      this.node = node;
    }

    private void run(String method, SagaId sagaId, Runnable delegate) {
      node.run(Store.DEAD_LETTERS, method, idOf(sagaId), delegate);
    }

    private <T> T call(String method, SagaId sagaId, Supplier<T> delegate) {
      return node.call(Store.DEAD_LETTERS, method, idOf(sagaId), delegate);
    }

    @Override
    public void publish(SagaDeadLetterEntry entry) {
      run("publish", entry.sagaId(), () -> super.publish(entry));
    }

    @Override
    public void publishShielded(SagaDeadLetterEntry entry) {
      run("publishShielded", entry.sagaId(), () -> super.publishShielded(entry));
    }

    @Override
    public HoldResult publishShieldedIfAbsent(SagaDeadLetterEntry entry) {
      return call(
          "publishShieldedIfAbsent", entry.sagaId(), () -> super.publishShieldedIfAbsent(entry));
    }

    @Override
    public List<SagaDeadLetterEntry> findAll(int limit) {
      return call("findAll", null, () -> super.findAll(limit));
    }

    @Override
    public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
      return call("findBySaga", sagaId, () -> super.findBySaga(sagaId));
    }

    @Override
    public Optional<SagaDeadLetterEntry> findNullSagaEntry(
        SagaType sagaType, GlobalOffset eventOffset) {
      return call("findNullSagaEntry", null, () -> super.findNullSagaEntry(sagaType, eventOffset));
    }

    @Override
    public List<SagaDeadLetterEntry> findNullSagaEntriesByResolvedTarget(
        SagaType sagaType, SagaId targetSagaId) {
      return call(
          "findNullSagaEntriesByResolvedTarget",
          targetSagaId,
          () -> super.findNullSagaEntriesByResolvedTarget(sagaType, targetSagaId));
    }

    @Override
    public boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset) {
      return call("discard", sagaId, () -> super.discard(sagaId, sagaType, eventOffset));
    }

    @Override
    public void establishFirstReplayAnchor(
        SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent) {
      run(
          "establishFirstReplayAnchor",
          sagaId,
          () -> super.establishFirstReplayAnchor(sagaId, sagaType, eventOffset, anchorIfAbsent));
    }

    @Override
    public void setResolvedTarget(
        SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId) {
      run(
          "setResolvedTarget",
          targetSagaId,
          () -> super.setResolvedTarget(sagaType, eventOffset, targetSagaId));
    }

    @Override
    public boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType) {
      return call(
          "clearShieldIfDrained", sagaId, () -> super.clearShieldIfDrained(sagaId, sagaType));
    }

    @Override
    public int deleteOlderThan(Instant cutoff) {
      return call("deleteOlderThan", null, () -> super.deleteOlderThan(cutoff));
    }

    @Override
    public long countFaultedBacklog() {
      return call("countFaultedBacklog", null, super::countFaultedBacklog);
    }
  }

  /** A node's view of the world's checkpoints. */
  static final class NodeOffsets extends ForwardingOffsetStore {
    private final Node node;

    NodeOffsets(Node node, OffsetStore offsets) {
      super(offsets);
      this.node = node;
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return node.call(
          Store.OFFSETS, "getLastOffset", null, () -> super.getLastOffset(projectionName));
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      node.run(Store.OFFSETS, "saveOffset", null, () -> super.saveOffset(projectionName, offset));
    }

    @Override
    public void reset(ProjectionName projectionName) {
      node.run(Store.OFFSETS, "reset", null, () -> super.reset(projectionName));
    }

    /** The delegate's pair with this store as its checkpoints, so its saves stay hooked. */
    @Override
    public CheckpointTarget subscriptionCheckpoint() {
      return node.call(
          Store.OFFSETS, "subscriptionCheckpoint", null, super::subscriptionCheckpoint);
    }
  }

  /** A node's bus: a command is named by its class's simple name, its saga by its data. */
  static final class NodeBus extends ForwardingCommandBus {
    private final Node node;

    NodeBus(Node node, CommandBus processBus) {
      super(processBus);
      this.node = node;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return node.call(
          Store.BUS,
          command.getClass().getSimpleName(),
          SagaParallelFixtures.sagaIdOf(command),
          () -> super.execute(command));
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      return node.call(
          Store.BUS,
          command.getClass().getSimpleName(),
          SagaParallelFixtures.sagaIdOf(command),
          () -> super.execute(command, key));
    }
  }
}
