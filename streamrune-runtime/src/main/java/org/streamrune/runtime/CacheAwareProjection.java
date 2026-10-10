package org.streamrune.runtime;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionRepository;

/**
 * Projection decorator that notifies a {@link CacheInvalidator} after each successful batch, once
 * the batch's read-model writes are committed (see {@link #process(List, ProjectionRepository)}).
 *
 * <p>If the delegate throws, or the read-model transaction rolls back, the invalidator is NOT
 * called — the cache remains valid with potentially stale data until TTL or the next successful
 * batch. This matches at-least-once delivery semantics: a failed projection batch means the read
 * model was not updated, so there is nothing to invalidate.
 *
 * <pre>{@code
 * Projection projection = new MyOrderProjection(repo);
 * CacheInvalidator invalidator = cachingQueryBus.cacheInvalidator();
 * Projection cacheAware = new CacheAwareProjection(projection, invalidator);
 * }</pre>
 *
 * @see CacheInvalidator
 * @see CachingQueryBus#cacheInvalidator()
 */
public final class CacheAwareProjection implements Projection {

  private final Projection delegate;
  private final CacheInvalidator invalidator;

  /**
   * Creates a new {@code CacheAwareProjection}.
   *
   * @param delegate the underlying projection to delegate processing to; must not be {@code null}
   * @param invalidator the cache invalidator to notify after a successful batch; must not be {@code
   *     null}
   */
  public CacheAwareProjection(Projection delegate, CacheInvalidator invalidator) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.invalidator = Objects.requireNonNull(invalidator, "invalidator");
  }

  /**
   * Processes {@code batch} via the delegate and, on success, notifies the invalidator.
   *
   * <p>If the delegate throws, the invalidator is not called.
   *
   * @param batch the events to process; must not be {@code null}
   */
  @Override
  public void process(List<EventEnvelope> batch) {
    delegate.process(batch);
    invalidator.onEventsProcessed(batch);
  }

  /**
   * Processes {@code batch} via the delegate, forwarding the transaction-scoped repository, and
   * notifies the invalidator once the batch's writes are committed.
   *
   * <p>Under a transactional {@link org.streamrune.core.projection.AtomicBatchProcessor} this
   * method runs INSIDE the read-model transaction, so the delegate's writes are not visible to
   * other readers when it returns. Invalidating then let a query that landed before the commit miss
   * the cache, read the pre-batch rows and cache them; nothing evicted them after the commit, so
   * the cache served the pre-batch read model until the entry's TTL ran out. The invalidation is
   * therefore registered with {@link ProjectionRepository#afterCommit}: the transaction runs it
   * once it has committed and drops it if it rolls back, which leaves the cache as it was. With a
   * {@code null} repository (an at-least-once registration, or {@code
   * AtomicBatchProcessor.nonAtomicAtLeastOnce()}) the delegate wrote through its own repository,
   * whose writes are already committed when it returns, so the invalidator is notified at once.
   *
   * <p>If the delegate throws, nothing is notified or registered.
   *
   * @param batch the events to process; must not be {@code null}
   * @param repository the transaction-scoped repository handle, or {@code null} when batches are
   *     not processed atomically
   */
  @Override
  public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
    delegate.process(batch, repository);
    if (repository == null) {
      invalidator.onEventsProcessed(batch);
    } else {
      repository.afterCommit(() -> invalidator.onEventsProcessed(batch));
    }
  }

  /**
   * Forwards to the delegate's OWN {@link Projection#processDeadLetterReplay} with the repository
   * it was handed, notifies the invalidator only when the delegate reports it APPLIED something,
   * and reports the delegate's answer. As in {@link #process(List, ProjectionRepository)}, the
   * notification waits for the replay transaction's commit when a repository is handed, and happens
   * at once when it is {@code null}. The inherited default would call THIS decorator's {@code
   * process} — reaching the delegate's {@code process}, never its override — and report {@code
   * true}, so a decorated self-fencing projection ({@code WindowedProjection}) would read as
   * "applied" on a wholly fenced replay, the replayer would discard the range's only record, and
   * the cache would be invalidated for a read model that did not change.
   *
   * @param batch the re-read dead-lettered range; must not be {@code null}
   * @param repository the replay transaction's repository, or {@code null} for an at-least-once
   *     registration
   * @return the delegate's answer
   */
  @Override
  public boolean processDeadLetterReplay(
      List<EventEnvelope> batch, ProjectionRepository repository) {
    boolean applied = delegate.processDeadLetterReplay(batch, repository);
    if (!applied) {
      return false;
    }
    if (repository == null) {
      invalidator.onEventsProcessed(batch);
    } else {
      repository.afterCommit(() -> invalidator.onEventsProcessed(batch));
    }
    return true;
  }

  /**
   * Forwards to the delegate's OWN answer — this decorator overrides {@code process(List,
   * ProjectionRepository)} only to notify the invalidator after forwarding, so the reflective
   * default (see {@link Projection#writesThroughRepository()}) would read {@code true} on this
   * class regardless of whether the WRAPPED projection actually writes through the repository.
   */
  @Override
  public boolean writesThroughRepository() {
    return delegate.writesThroughRepository();
  }

  /** Forwards the delegate's write target so the delivery policy can verify the pairing. */
  @Override
  public Optional<ProjectionRepository> writeTarget() {
    return delegate.writeTarget();
  }
}
