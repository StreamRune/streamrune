package org.streamrune.test;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

/**
 * A {@link SagaDeadLetterStore} that forwards every call to a delegate and injects the faults of a
 * {@link FaultSchedule}: wrap an {@link InMemorySagaDeadLetterStore} to make a quarantine write
 * fail ({@code failOnce("publish")}) or crash a replay between its feed and its discard ({@code
 * crashAfter("discard")}).
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * the default methods included, so {@code findNullSagaEntriesByResolvedTarget} and {@code
 * countFaultedBacklog} reach the delegate's implementation, never the throwing defaults. A subclass
 * may override a method to observe, block or fail it; {@code super} applies the schedule, {@link
 * #delegate} does not.
 */
public class ForwardingSagaDeadLetterStore implements SagaDeadLetterStore {

  private static final Set<String> METHODS = FaultSchedule.methodsOf(SagaDeadLetterStore.class);

  /** The store every call is forwarded to. */
  protected final SagaDeadLetterStore delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the store to forward to
   */
  public ForwardingSagaDeadLetterStore(SagaDeadLetterStore delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the store to forward to
   * @param schedule the faults to inject; it learns the method names of {@link SagaDeadLetterStore}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link SagaDeadLetterStore}
   *     does not have
   */
  public ForwardingSagaDeadLetterStore(SagaDeadLetterStore delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public void publish(SagaDeadLetterEntry entry) {
    schedule.run("publish", () -> delegate.publish(entry));
  }

  @Override
  public void publishShielded(SagaDeadLetterEntry entry) {
    schedule.run("publishShielded", () -> delegate.publishShielded(entry));
  }

  @Override
  public List<SagaDeadLetterEntry> findAll(int limit) {
    return schedule.call("findAll", () -> delegate.findAll(limit));
  }

  @Override
  public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
    return schedule.call("findBySaga", () -> delegate.findBySaga(sagaId));
  }

  @Override
  public Optional<SagaDeadLetterEntry> findNullSagaEntry(
      SagaType sagaType, GlobalOffset eventOffset) {
    return schedule.call(
        "findNullSagaEntry", () -> delegate.findNullSagaEntry(sagaType, eventOffset));
  }

  @Override
  public List<SagaDeadLetterEntry> findNullSagaEntriesByResolvedTarget(
      SagaType sagaType, SagaId targetSagaId) {
    return schedule.call(
        "findNullSagaEntriesByResolvedTarget",
        () -> delegate.findNullSagaEntriesByResolvedTarget(sagaType, targetSagaId));
  }

  @Override
  public boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset) {
    return schedule.call("discard", () -> delegate.discard(sagaId, sagaType, eventOffset));
  }

  @Override
  public void establishFirstReplayAnchor(
      SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent) {
    schedule.run(
        "establishFirstReplayAnchor",
        () -> delegate.establishFirstReplayAnchor(sagaId, sagaType, eventOffset, anchorIfAbsent));
  }

  @Override
  public void setResolvedTarget(SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId) {
    schedule.run(
        "setResolvedTarget", () -> delegate.setResolvedTarget(sagaType, eventOffset, targetSagaId));
  }

  @Override
  public boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType) {
    return schedule.call(
        "clearShieldIfDrained", () -> delegate.clearShieldIfDrained(sagaId, sagaType));
  }

  @Override
  public int deleteOlderThan(Instant cutoff) {
    return schedule.call("deleteOlderThan", () -> delegate.deleteOlderThan(cutoff));
  }

  @Override
  public long countFaultedBacklog() {
    return schedule.call("countFaultedBacklog", delegate::countFaultedBacklog);
  }
}
