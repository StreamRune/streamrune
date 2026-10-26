package org.streamrune.test;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * A {@link ProjectionDeadLetterStore} that forwards every call to a delegate and injects the faults
 * of a {@link FaultSchedule}: wrap an {@link InMemoryProjectionDeadLetterStore} to make a
 * projection runner's dead-letter write fail ({@code failOnce("save")}) and prove that the runner
 * does not advance past a range it could not record.
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * {@code countPending} included, so the backlog gauge reaches the delegate's count, never the
 * throwing default. A subclass may override a method to observe, block or fail it; {@code super}
 * applies the schedule, {@link #delegate} does not.
 */
public class ForwardingProjectionDeadLetterStore implements ProjectionDeadLetterStore {

  private static final Set<String> METHODS =
      FaultSchedule.methodsOf(ProjectionDeadLetterStore.class);

  /** The store every call is forwarded to. */
  protected final ProjectionDeadLetterStore delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the store to forward to
   */
  public ForwardingProjectionDeadLetterStore(ProjectionDeadLetterStore delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the store to forward to
   * @param schedule the faults to inject; it learns the method names of {@link
   *     ProjectionDeadLetterStore}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link
   *     ProjectionDeadLetterStore} does not have
   */
  public ForwardingProjectionDeadLetterStore(
      ProjectionDeadLetterStore delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public void save(ProjectionDeadLetterEntry entry) {
    schedule.run("save", () -> delegate.save(entry));
  }

  @Override
  public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
    return schedule.call("read", () -> delegate.read(projectionName, limit));
  }

  @Override
  public List<ProjectionDeadLetterEntry> readAll(int limit) {
    return schedule.call("readAll", () -> delegate.readAll(limit));
  }

  @Override
  public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
    schedule.run("discard", () -> delegate.discard(projectionName, fromOffset));
  }

  @Override
  public long countPending() {
    return schedule.call("countPending", delegate::countPending);
  }
}
