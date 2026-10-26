package org.streamrune.test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;

/**
 * An {@link OutboxStore} that forwards every call to a delegate and injects the faults of a {@link
 * FaultSchedule}: wrap an {@link InMemoryOutboxStore} to crash a relay after the publish and before
 * its bookkeeping ({@code failOnce("markDeliveredAll")}), or after the bookkeeping ({@code
 * crashAfter("markDeliveredAll")}). Subclass it and override the one method a test wants to
 * observe, throw from or delay (a store without {@code sampleBlockage}, a crash after the k-th
 * write, …) while the rest keeps the delegate's real behaviour.
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * the default methods included, so {@code claimLease}, {@code orderingMode}, the batch writes and
 * the counts reach the delegate's implementation, never the defaults of {@link OutboxStore}. In a
 * subclass, {@code super} applies the schedule, {@link #delegate} does not.
 */
public class ForwardingOutboxStore implements OutboxStore {

  private static final Set<String> METHODS = FaultSchedule.methodsOf(OutboxStore.class);

  /** The store every call is forwarded to. */
  protected final OutboxStore delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the store to forward to
   */
  public ForwardingOutboxStore(OutboxStore delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the store to forward to
   * @param schedule the faults to inject; it learns the method names of {@link OutboxStore}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link OutboxStore} does
   *     not have
   */
  public ForwardingOutboxStore(OutboxStore delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public void save(OutboxEntry entry) {
    schedule.run("save", () -> delegate.save(entry));
  }

  @Override
  public List<OutboxEntry> loadPending(int limit) {
    return schedule.call("loadPending", () -> delegate.loadPending(limit));
  }

  @Override
  public String claimedBy() {
    return schedule.call("claimedBy", delegate::claimedBy);
  }

  @Override
  public Duration claimLease() {
    return schedule.call("claimLease", delegate::claimLease);
  }

  @Override
  public OutboxOrderingMode orderingMode() {
    return schedule.call("orderingMode", delegate::orderingMode);
  }

  @Override
  public boolean markDelivered(OutboxEntryId id, String claimedBy) {
    return schedule.call("markDelivered", () -> delegate.markDelivered(id, claimedBy));
  }

  @Override
  public int markDeliveredAll(Collection<OutboxEntryId> ids, String claimedBy) {
    return schedule.call("markDeliveredAll", () -> delegate.markDeliveredAll(ids, claimedBy));
  }

  @Override
  public int releaseClaims(Collection<OutboxEntry> entries, String claimedBy) {
    return schedule.call("releaseClaims", () -> delegate.releaseClaims(entries, claimedBy));
  }

  @Override
  public int deleteDelivered(Instant olderThan) {
    return schedule.call("deleteDelivered", () -> delegate.deleteDelivered(olderThan));
  }

  @Override
  public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
    return schedule.call("findByStatus", () -> delegate.findByStatus(status, limit));
  }

  @Override
  public Optional<OutboxEntry> findById(OutboxEntryId id) {
    return schedule.call("findById", () -> delegate.findById(id));
  }

  @Override
  public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
    return schedule.call("skipFailed", () -> delegate.skipFailed(id, skippedBy, reason));
  }

  @Override
  public BlockageSample sampleBlockage() {
    return schedule.call("sampleBlockage", delegate::sampleBlockage);
  }

  @Override
  public int deleteSkipped(Instant cutoff) {
    return schedule.call("deleteSkipped", () -> delegate.deleteSkipped(cutoff));
  }

  @Override
  public boolean resetFailedToPending(OutboxEntryId id) {
    return schedule.call("resetFailedToPending", () -> delegate.resetFailedToPending(id));
  }

  @Override
  public boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy) {
    return schedule.call("markFailed", () -> delegate.markFailed(id, attempts, error, claimedBy));
  }

  @Override
  public boolean markRetry(
      OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {
    return schedule.call(
        "markRetry", () -> delegate.markRetry(id, attempts, error, backoff, claimedBy));
  }

  @Override
  public long countByStatus(OutboxStatus status) {
    return schedule.call("countByStatus", () -> delegate.countByStatus(status));
  }

  @Override
  public void delete(OutboxEntryId id) {
    schedule.run("delete", () -> delegate.delete(id));
  }
}
