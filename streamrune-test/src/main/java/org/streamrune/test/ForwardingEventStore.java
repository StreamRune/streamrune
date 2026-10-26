package org.streamrune.test;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * An {@link EventStore} that forwards every call to a delegate and injects the faults of a {@link
 * FaultSchedule}: wrap an {@link InMemoryEventStore} (or any store) to make an append fail before
 * it is stored, or after, and to count the calls.
 *
 * <pre>{@code
 * var faults = FaultSchedule.none().crashAfter("append");
 * var store = new ForwardingEventStore(new InMemoryEventStore(), faults);
 * // the next append stores its events, then throws InjectedFaultException
 * }</pre>
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * the default methods included: {@code appendWithKey}, {@code load(StreamId, int)}, {@code
 * lastGlobalOffset()} and {@code sagaFenceStorageIdentity()} reach the delegate's own
 * implementation, never the throwing defaults of {@link EventStore}. A subclass may override a
 * method to observe, block or fail it; {@code super} applies the schedule, {@link #delegate} does
 * not.
 */
public class ForwardingEventStore implements EventStore {

  private static final Set<String> METHODS = FaultSchedule.methodsOf(EventStore.class);

  /** The store every call is forwarded to. */
  protected final EventStore delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the store to forward to
   */
  public ForwardingEventStore(EventStore delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the store to forward to
   * @param schedule the faults to inject; it learns the method names of {@link EventStore}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link EventStore} does not
   *     have
   */
  public ForwardingEventStore(EventStore delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public AggregateHistory load(StreamId streamId) {
    return schedule.call("load", () -> delegate.load(streamId));
  }

  @Override
  public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
    return schedule.call("load", () -> delegate.load(streamId, expectedSnapshotVersion));
  }

  @Override
  public AppendResult append(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
    return schedule.call("append", () -> delegate.append(streamId, events, expectedVersion));
  }

  @Override
  public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
    schedule.run("saveSnapshot", () -> delegate.saveSnapshot(streamId, version, state));
  }

  @Override
  public void saveSnapshot(
      StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
    schedule.run(
        "saveSnapshot", () -> delegate.saveSnapshot(streamId, version, state, snapshotVersion));
  }

  @Override
  public boolean saveSnapshot(
      StreamId streamId,
      Version version,
      AggregateState state,
      int snapshotVersion,
      long snapshotPurgeGeneration) {
    return schedule.call(
        "saveSnapshot",
        () ->
            delegate.saveSnapshot(
                streamId, version, state, snapshotVersion, snapshotPurgeGeneration));
  }

  @Override
  public GlobalOffset lastGlobalOffset() {
    return schedule.call("lastGlobalOffset", delegate::lastGlobalOffset);
  }

  @Override
  public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
    return schedule.call(
        "readGlobalStream", () -> delegate.readGlobalStream(afterOffset, maxCount));
  }

  @Override
  public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
    return schedule.call("readStream", () -> delegate.readStream(streamId, afterVersion, maxCount));
  }

  @Override
  public IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType) {
    return schedule.call(
        "appendWithKey",
        () ->
            delegate.appendWithKey(streamId, events, expectedVersion, idempotencyKey, commandType));
  }

  @Override
  public IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType,
      SagaOrigin forwardOf) {
    return schedule.call(
        "appendWithKey",
        () ->
            delegate.appendWithKey(
                streamId, events, expectedVersion, idempotencyKey, commandType, forwardOf));
  }

  @Override
  public Object sagaFenceStorageIdentity() {
    return schedule.call("sagaFenceStorageIdentity", delegate::sagaFenceStorageIdentity);
  }
}
