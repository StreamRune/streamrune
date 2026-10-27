package org.streamrune.test;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;

/**
 * A {@link DeadLetterQueue} that forwards every call to a delegate and injects the faults of a
 * {@link FaultSchedule}: wrap an {@link InMemoryDeadLetterQueue} to make the command bus's
 * dead-letter write fail ({@code failOnce("publish")}), or to crash a retry between its replay and
 * its bookkeeping ({@code crashAfter("updateAttempts")}).
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * the default methods included, so {@code find}, {@code findByIdempotencyKey}, {@code
 * updateAttempts}, {@code readRetryable}, {@code countPending}, {@code countAwaitingOperator} and
 * {@code deleteOlderThan} reach the delegate's implementation, never the throwing defaults. A
 * subclass may override a method to observe, block or fail it; {@code super} applies the schedule,
 * {@link #delegate} does not.
 */
public class ForwardingDeadLetterQueue implements DeadLetterQueue {

  private static final Set<String> METHODS = FaultSchedule.methodsOf(DeadLetterQueue.class);

  /** The queue every call is forwarded to. */
  protected final DeadLetterQueue delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the queue to forward to
   */
  public ForwardingDeadLetterQueue(DeadLetterQueue delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the queue to forward to
   * @param schedule the faults to inject; it learns the method names of {@link DeadLetterQueue}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link DeadLetterQueue}
   *     does not have
   */
  public ForwardingDeadLetterQueue(DeadLetterQueue delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public void publish(DeadLetterPublishRequest request) {
    schedule.run("publish", () -> delegate.publish(request));
  }

  @Override
  public List<DeadLetterEntry> read(int limit) {
    return schedule.call("read", () -> delegate.read(limit));
  }

  @Override
  public Optional<DeadLetterEntry> find(CommandId commandId) {
    return schedule.call("find", () -> delegate.find(commandId));
  }

  @Override
  public List<DeadLetterEntry> findByIdempotencyKey(IdempotencyKey key) {
    return schedule.call("findByIdempotencyKey", () -> delegate.findByIdempotencyKey(key));
  }

  @Override
  public void discard(CommandId commandId) {
    schedule.run("discard", () -> delegate.discard(commandId));
  }

  @Override
  public void updateAttempts(CommandId commandId, int dlqAttempts, Instant lastAttemptAt) {
    schedule.run(
        "updateAttempts", () -> delegate.updateAttempts(commandId, dlqAttempts, lastAttemptAt));
  }

  @Override
  public List<DeadLetterEntry> readRetryable(
      int maxRetries, int limit, boolean includeUnkeyed, Instant keyedFirstAttemptAtOrAfter) {
    return schedule.call(
        "readRetryable",
        () ->
            delegate.readRetryable(maxRetries, limit, includeUnkeyed, keyedFirstAttemptAtOrAfter));
  }

  @Override
  public long countPending() {
    return schedule.call("countPending", delegate::countPending);
  }

  @Override
  public long countAwaitingOperator(
      int maxRetries, boolean countUnkeyed, Instant keyedFirstAttemptBefore) {
    return schedule.call(
        "countAwaitingOperator",
        () -> delegate.countAwaitingOperator(maxRetries, countUnkeyed, keyedFirstAttemptBefore));
  }

  @Override
  public int deleteOlderThan(Instant cutoff) {
    return schedule.call("deleteOlderThan", () -> delegate.deleteOlderThan(cutoff));
  }
}
