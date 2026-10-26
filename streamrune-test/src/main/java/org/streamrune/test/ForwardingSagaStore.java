package org.streamrune.test;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.SagaType;

/**
 * A {@link SagaStore} that forwards every call to a delegate and injects the faults of a {@link
 * FaultSchedule}: wrap an {@link InMemorySagaStore} to crash a saga step after one of its writes
 * ({@code crashAfter("applyEvent")}, {@code crashAfter("claimCompensating")}) and prove that the
 * redelivery converges.
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * the default methods included, so {@code countByStatus} and {@code findCompensating} reach the
 * delegate's implementation and {@link #storageIdentity()} reports the delegate's rows: a forward
 * fence built over the delegate fences this store's sagas. A subclass may override a method to
 * observe, block or fail it; {@code super} applies the schedule, {@link #delegate} does not.
 */
public class ForwardingSagaStore implements SagaStore {

  private static final Set<String> METHODS = FaultSchedule.methodsOf(SagaStore.class);

  /** The store every call is forwarded to. */
  protected final SagaStore delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the store to forward to
   */
  public ForwardingSagaStore(SagaStore delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the store to forward to
   * @param schedule the faults to inject; it learns the method names of {@link SagaStore}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link SagaStore} does not
   *     have
   */
  public ForwardingSagaStore(SagaStore delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public Object storageIdentity() {
    return schedule.call("storageIdentity", delegate::storageIdentity);
  }

  @Override
  public void create(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending) {
    schedule.run(
        "create", () -> delegate.create(sagaId, sagaType, state, status, deadLetterPending));
  }

  @Override
  public void create(SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status) {
    schedule.run("create", () -> delegate.create(sagaId, sagaType, state, status));
  }

  @Override
  public void update(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status, long expectedVersion) {
    schedule.run("update", () -> delegate.update(sagaId, sagaType, state, status, expectedVersion));
  }

  @Override
  public void createGenesisPending(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending,
      EventId episodeCause) {
    schedule.run(
        "createGenesisPending",
        () ->
            delegate.createGenesisPending(
                sagaId, sagaType, state, status, deadLetterPending, episodeCause));
  }

  @Override
  public void createGenesisPending(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending) {
    schedule.run(
        "createGenesisPending",
        () -> delegate.createGenesisPending(sagaId, sagaType, state, status, deadLetterPending));
  }

  @Override
  public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
    schedule.run("createFaulted", () -> delegate.createFaulted(sagaId, sagaType, initialState));
  }

  @Override
  public void applyEvent(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      long expectedVersion,
      AppliedEvent applied) {
    schedule.run(
        "applyEvent",
        () -> delegate.applyEvent(sagaId, sagaType, state, status, expectedVersion, applied));
  }

  @Override
  public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
    return schedule.call(
        "markFaulted", () -> delegate.markFaulted(sagaId, sagaType, expectedVersion));
  }

  @Override
  public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
    schedule.run(
        "setDeadLetterPending", () -> delegate.setDeadLetterPending(sagaId, sagaType, pending));
  }

  @Override
  public void claimCompensating(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      long expectedVersion,
      EventId episodeCause) {
    schedule.run(
        "claimCompensating",
        () -> delegate.claimCompensating(sagaId, sagaType, state, expectedVersion, episodeCause));
  }

  @Override
  public void claimCompensating(
      SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
    schedule.run(
        "claimCompensating",
        () -> delegate.claimCompensating(sagaId, sagaType, state, expectedVersion));
  }

  @Override
  public <S extends SagaState> Optional<LoadedSaga<S>> load(
      SagaId sagaId, SagaType sagaType, Class<S> stateType) {
    return schedule.call("load", () -> delegate.load(sagaId, sagaType, stateType));
  }

  @Override
  public void delete(SagaId sagaId, SagaType sagaType) {
    schedule.run("delete", () -> delegate.delete(sagaId, sagaType));
  }

  @Override
  public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
    return schedule.call("findTimedOut", () -> delegate.findTimedOut(sagaType, cutoff, limit));
  }

  @Override
  public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
    return schedule.call("findByStatus", () -> delegate.findByStatus(sagaType, status, limit));
  }

  @Override
  public long countByStatus(SagaType sagaType, SagaStatus status) {
    return schedule.call("countByStatus", () -> delegate.countByStatus(sagaType, status));
  }

  @Override
  public List<CompensatingSaga> findCompensating(
      SagaType sagaType, Instant updatedBefore, int limit) {
    return schedule.call(
        "findCompensating", () -> delegate.findCompensating(sagaType, updatedBefore, limit));
  }
}
