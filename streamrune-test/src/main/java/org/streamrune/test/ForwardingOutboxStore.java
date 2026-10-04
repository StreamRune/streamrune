package org.streamrune.test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;

/**
 * An {@link OutboxStore} that forwards every call to a delegate. Test kit: subclass and override
 * the one method a test wants to observe, throw from or delay (a store without {@code
 * sampleBlockage}, a crash after the k-th write, …) while the rest keeps the delegate's real
 * behaviour.
 */
public class ForwardingOutboxStore implements OutboxStore {

  protected final OutboxStore delegate;

  public ForwardingOutboxStore(OutboxStore delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
  }

  @Override
  public void save(OutboxEntry entry) {
    delegate.save(entry);
  }

  @Override
  public List<OutboxEntry> loadPending(int limit) {
    return delegate.loadPending(limit);
  }

  @Override
  public String claimedBy() {
    return delegate.claimedBy();
  }

  @Override
  public Duration claimLease() {
    return delegate.claimLease();
  }

  @Override
  public OutboxOrderingMode orderingMode() {
    return delegate.orderingMode();
  }

  @Override
  public boolean markDelivered(OutboxEntryId id, String claimedBy) {
    return delegate.markDelivered(id, claimedBy);
  }

  @Override
  public int markDeliveredAll(Collection<OutboxEntryId> ids, String claimedBy) {
    return delegate.markDeliveredAll(ids, claimedBy);
  }

  @Override
  public int releaseClaims(Collection<OutboxEntry> entries, String claimedBy) {
    return delegate.releaseClaims(entries, claimedBy);
  }

  @Override
  public int deleteDelivered(Instant olderThan) {
    return delegate.deleteDelivered(olderThan);
  }

  @Override
  public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
    return delegate.findByStatus(status, limit);
  }

  @Override
  public Optional<OutboxEntry> findById(OutboxEntryId id) {
    return delegate.findById(id);
  }

  @Override
  public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
    return delegate.skipFailed(id, skippedBy, reason);
  }

  @Override
  public BlockageSample sampleBlockage() {
    return delegate.sampleBlockage();
  }

  @Override
  public int deleteSkipped(Instant cutoff) {
    return delegate.deleteSkipped(cutoff);
  }

  @Override
  public boolean resetFailedToPending(OutboxEntryId id) {
    return delegate.resetFailedToPending(id);
  }

  @Override
  public boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy) {
    return delegate.markFailed(id, attempts, error, claimedBy);
  }

  @Override
  public boolean markRetry(
      OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {
    return delegate.markRetry(id, attempts, error, backoff, claimedBy);
  }

  @Override
  public long countByStatus(OutboxStatus status) {
    return delegate.countByStatus(status);
  }

  @Override
  public void delete(OutboxEntryId id) {
    delegate.delete(id);
  }
}
