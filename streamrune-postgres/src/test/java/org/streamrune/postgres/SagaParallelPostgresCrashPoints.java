package org.streamrune.postgres;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDeadLetterStore.HoldResult;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.saga.SagaStore.CompensatingSaga;
import org.streamrune.core.saga.SagaStore.RowFacts;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.test.ForwardingCommandBus;
import org.streamrune.test.ForwardingOffsetStore;
import org.streamrune.test.ForwardingSagaDeadLetterStore;
import org.streamrune.test.ForwardingSagaStore;

/**
 * The crash points of the runtime's parallel crash-point test ({@code CrashingStores} in the
 * runtime's tests, which this module's tests cannot see), over PostgreSQL stores: a decorated call
 * throws {@link NodeCrashed} at its point, and every later call of that node does too. The
 * decorators are subclasses of the kit's — {@link ForwardingSagaStore}, {@link
 * ForwardingSagaDeadLetterStore}, {@link ForwardingOffsetStore}, {@link ForwardingCommandBus} —
 * that call {@code super} and throw before or after it.
 */
final class SagaParallelPostgresCrashPoints {

  private SagaParallelPostgresCrashPoints() {}

  /** The decorator a point is in. */
  enum Store {
    SAGA_STORE,
    DEAD_LETTERS,
    OFFSETS,
    BUS
  }

  /** Where a node stops: a call, before or after its delegate ran, and the number of the call. */
  enum Point {
    /** The forward command committed (its events and its inbox key); no row write is made. */
    AFTER_FORWARD_COMMAND_COMMIT(Store.BUS, "Step", true, 1),
    /** The saga row recorded the event; the page has not ended. */
    AFTER_ROW_WRITE(Store.SAGA_STORE, "applyEvent", true, 1),
    /** A rejected step's claim is written ({@code COMPENSATING}, stamped). */
    AFTER_COMPENSATION_CLAIM(Store.SAGA_STORE, "claimCompensating", true, 1),
    /** The first of two compensation commands committed, the second not. */
    AFTER_FIRST_COMPENSATION_COMMAND(Store.BUS, "Undo", true, 1),
    /** The compensation commands committed; the terminal write is not made. */
    BEFORE_TERMINAL_WRITE(Store.SAGA_STORE, "update", false, 1),
    /** Every group ended; the checkpoint is not saved. */
    BEFORE_SAVE_OFFSET(Store.OFFSETS, "saveOffset", false, 1),
    /** The checkpoint is saved (the end of a deposed node's late save). */
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

  /**
   * One node's crash point and its hooks. {@code saga} of a command is read by {@code sagaOf};
   * every call is named by its method (a command: its class's simple name) and its saga id.
   */
  static final class Crash {
    private final Point crashAt;
    private final String crashOn;
    private final AtomicBoolean crashed = new AtomicBoolean();
    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    /** Every call whose delegate returned, as {@code "<method> <saga id>"}, in order. */
    final List<String> calls = new CopyOnWriteArrayList<>();

    /** Runs first in every call. */
    volatile BiConsumer<String, String> beforeCall = (method, sagaId) -> {};

    /** Runs after every call whose delegate returned. */
    volatile BiConsumer<String, String> afterCall = (method, sagaId) -> {};

    /** Runs once, on the thread that crashes the node, right before it throws. */
    volatile Runnable onCrash = () -> {};

    /**
     * @param crashAt the point, or {@code null} for a node that does not crash
     * @param crashOn the saga the point counts the calls of
     */
    Crash(Point crashAt, String crashOn) {
      this.crashAt = crashAt;
      this.crashOn = crashOn;
    }

    boolean crashed() {
      return crashed.get();
    }

    <T> T call(Store store, String method, String sagaId, Supplier<T> delegate) {
      beforeCall.accept(method, sagaId);
      if (crashed.get()) {
        throw new NodeCrashed("before " + method + " of " + sagaId + ": it crashed earlier");
      }
      boolean atThePoint = atThePoint(store, method, sagaId);
      if (atThePoint && !crashAt.afterTheDelegate) {
        crash("before " + method + " of " + sagaId);
      }
      T result = delegate.get();
      calls.add(method + " " + sagaId);
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
      String counted = store == Store.OFFSETS ? method : method + " " + sagaId;
      return counts.computeIfAbsent(counted, k -> new AtomicInteger()).incrementAndGet()
          == crashAt.nthCall;
    }

    private void crash(String where) {
      if (!crashed.compareAndSet(false, true)) {
        throw new NodeCrashed(where + ": it crashed earlier");
      }
      onCrash.run();
      throw new NodeCrashed("at " + crashAt + ", " + where);
    }
  }

  private static String idOf(SagaId sagaId) {
    return sagaId == null ? null : sagaId.value();
  }

  /** A node's view of the saga rows. */
  static final class CrashingSagaStore extends ForwardingSagaStore {
    private final Crash crash;

    CrashingSagaStore(SagaStore rows, Crash crash) {
      super(rows);
      this.crash = crash;
    }

    private void run(String method, SagaId sagaId, Runnable delegate) {
      crash.run(Store.SAGA_STORE, method, idOf(sagaId), delegate);
    }

    private <T> T call(String method, SagaId sagaId, Supplier<T> delegate) {
      return crash.call(Store.SAGA_STORE, method, idOf(sagaId), delegate);
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

  /** A node's view of the dead-letter store. */
  static final class CrashingDeadLetters extends ForwardingSagaDeadLetterStore {
    private final Crash crash;

    CrashingDeadLetters(SagaDeadLetterStore deadLetters, Crash crash) {
      super(deadLetters);
      this.crash = crash;
    }

    private void run(String method, SagaId sagaId, Runnable delegate) {
      crash.run(Store.DEAD_LETTERS, method, idOf(sagaId), delegate);
    }

    private <T> T call(String method, SagaId sagaId, Supplier<T> delegate) {
      return crash.call(Store.DEAD_LETTERS, method, idOf(sagaId), delegate);
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

  /** A node's view of the checkpoints. */
  static final class CrashingOffsets extends ForwardingOffsetStore {
    private final Crash crash;

    CrashingOffsets(OffsetStore offsets, Crash crash) {
      super(offsets);
      this.crash = crash;
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return crash.call(
          Store.OFFSETS, "getLastOffset", null, () -> super.getLastOffset(projectionName));
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      crash.run(Store.OFFSETS, "saveOffset", null, () -> super.saveOffset(projectionName, offset));
    }

    @Override
    public void reset(ProjectionName projectionName) {
      crash.run(Store.OFFSETS, "reset", null, () -> super.reset(projectionName));
    }

    /** The delegate's pair with this store as its checkpoints, so its saves stay hooked. */
    @Override
    public CheckpointTarget subscriptionCheckpoint() {
      return crash.call(
          Store.OFFSETS, "subscriptionCheckpoint", null, super::subscriptionCheckpoint);
    }
  }

  /** A node's bus: a command is named by its class's simple name, its saga by {@code sagaOf}. */
  static final class CrashingBus extends ForwardingCommandBus {
    private final Crash crash;
    private final Function<Command, String> sagaOf;

    CrashingBus(CommandBus processBus, Crash crash, Function<Command, String> sagaOf) {
      super(processBus);
      this.crash = crash;
      this.sagaOf = sagaOf;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return crash.call(
          Store.BUS,
          command.getClass().getSimpleName(),
          sagaOf.apply(command),
          () -> super.execute(command));
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      return crash.call(
          Store.BUS,
          command.getClass().getSimpleName(),
          sagaOf.apply(command),
          () -> super.execute(command, key));
    }
  }
}
