package org.streamrune.test;

import java.util.Objects;
import java.util.Set;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * An {@link OffsetStore} that forwards every call to a delegate and injects the faults of a {@link
 * FaultSchedule}: wrap an {@link InMemoryOffsetStore} to crash a subscription between its delivery
 * and its checkpoint ({@code failOnce("saveOffset")}), or after the checkpoint is stored ({@code
 * crashAfter("saveOffset")}), and prove that the next start delivers each event at least once.
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * {@code reset} included, so a rebuild reaches the delegate's own rewind, never the throwing
 * default. A subclass may override a method to observe, block or fail it; {@code super} applies the
 * schedule, {@link #delegate} does not.
 */
public class ForwardingOffsetStore implements OffsetStore {

  private static final Set<String> METHODS = FaultSchedule.methodsOf(OffsetStore.class);

  /** The store every call is forwarded to. */
  protected final OffsetStore delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the store to forward to
   */
  public ForwardingOffsetStore(OffsetStore delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the store to forward to
   * @param schedule the faults to inject; it learns the method names of {@link OffsetStore}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link OffsetStore} does
   *     not have
   */
  public ForwardingOffsetStore(OffsetStore delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return schedule.call("getLastOffset", () -> delegate.getLastOffset(projectionName));
  }

  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    schedule.run("saveOffset", () -> delegate.saveOffset(projectionName, offset));
  }

  @Override
  public void reset(ProjectionName projectionName) {
    schedule.run("reset", () -> delegate.reset(projectionName));
  }
}
