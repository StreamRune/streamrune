package org.streamrune.core.projection;

import java.util.List;
import java.util.Optional;
import org.streamrune.core.EventEnvelope;

/**
 * Processes batches of events to build read models.
 *
 * <p>{@link #process(List)} must be idempotent (upsert, not insert): that is the {@link
 * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} contract, and it is also what a dead-letter
 * replay ({@code ProjectionDeadLetterReplayer}) needs under every mode, because a replay applies a
 * range at least once and after later events were already applied. Idempotency covers that
 * re-application and nothing else: what keeps a replay from interleaving with a live batch is the
 * {@link AtomicBatchProcessor}'s per-projection lock, which the replayer takes through {@link
 * AtomicBatchProcessor#executeReplay}.
 */
public interface Projection {

  /**
   * Processes a batch of events. Must be idempotent — see the class javadoc for why.
   *
   * @param batch the batch of events to process, in stream order
   */
  void process(List<EventEnvelope> batch);

  /**
   * Processes a batch of events, writing through the supplied repository. Runners invoke this
   * variant when driving the projection through an {@link AtomicBatchProcessor}: transactional
   * processors pass a repository bound to their open transaction, so projection writes and the
   * offset save commit — or roll back — together. The repository is {@code null} for an {@link
   * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} registration (the runner withholds it under
   * every processor) and under {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()}.
   *
   * <p>The default implementation ignores the repository and delegates to {@link #process(List)},
   * so a projection that captures its own repository gets at-least-once semantics. Override this
   * method and write through the supplied repository (when non-null) to make batch processing
   * atomic with offset tracking.
   *
   * @param batch the batch of events to process
   * @param repository transaction-scoped repository to write through, or {@code null} for an
   *     at-least-once registration or under the nonatomic processor
   */
  default void process(List<EventEnvelope> batch, ProjectionRepository repository) {
    process(batch);
  }

  /**
   * Declares whether this projection overrides {@link #process(List, ProjectionRepository)} and
   * writes through the repository it is handed, rather than falling back to {@link #process(List)}
   * via the inherited default above. A transactional runner — one whose {@link
   * AtomicBatchProcessor#supportsFencing()} is {@code true} — consults this when a projection is
   * registered, to refuse one that would silently downgrade the runner's promised atomicity
   * (projection writes committing in the SAME transaction as the checkpoint save) to an undeclared
   * at-least-once.
   *
   * <p>The default answers {@code false}, the truth for a projection that implements only {@link
   * #process(List)}. <b>Override it to return {@code true} when you override {@link #process(List,
   * ProjectionRepository)}</b>; {@link BaseProjection} does, so its subclasses need not. The answer
   * is a declaration, not something inferred from the class: the object the container hands the
   * runner can be a generated subclass or proxy of the bean (an interceptor subclass, a CGLIB
   * proxy) whose class declares {@code process(List, ProjectionRepository)} whether or not the
   * bean's own class does, so reading the concrete class would answer {@code true} for a projection
   * that writes nowhere through the handed repository.
   *
   * <p><b>Decorators MUST override this and forward to their delegate's answer</b> — exactly like
   * {@link #processDeadLetterReplay(List, ProjectionRepository)} below — because a decorator
   * overrides {@code process(List, ProjectionRepository)} only to forward the call (as every
   * shipped decorator does), and the inherited {@code false} would then hide a delegate that does
   * write through the repository. The shipped decorators ({@code TracingProjectionDecorator},
   * {@code ValidatingProjectionDecorator}, {@code CacheAwareProjection}) forward it.
   *
   * @return {@code true} if this projection writes through the repository handed to {@link
   *     #process(List, ProjectionRepository)} or, for a decorator, if its delegate does
   */
  default boolean writesThroughRepository() {
    return false;
  }

  /**
   * The repository this projection writes to when no transaction-scoped one is handed to it — what
   * a {@code BaseProjection} captured in its constructor. The delivery policy uses it to verify
   * that a {@code TRANSACTIONAL_LOCAL} / {@code EXTERNAL_EFFECT} registration's processor IS that
   * store ({@link AtomicBatchProcessor#writesTo}); a projection that returns empty is accepted as
   * <em>declared</em> write-through, not verified, and the startup INFO line says so. Decorators
   * forward it to their delegate, like {@link #writesThroughRepository()}.
   *
   * @return the repository this projection writes to, or empty when it does not expose one
   */
  default Optional<ProjectionRepository> writeTarget() {
    return Optional.empty();
  }

  /**
   * Processes a batch that a runner dead-lettered and an operator is now replaying through {@code
   * ProjectionDeadLetterReplayer}, and reports whether this projection <b>applied anything</b> from
   * it. The replayer calls it inside {@link AtomicBatchProcessor#executeReplay}, under the lock a
   * live batch of this projection takes, and hands it what a live batch is handed: the replay
   * transaction's repository for a {@link ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} or {@link
   * ProjectionDeliveryMode#EXTERNAL_EFFECT} registration, {@code null} for an {@link
   * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} one.
   *
   * <p>The default processes the batch through {@link #process(List, ProjectionRepository)} and
   * reports {@code true}. That is the truthful answer for every projection honouring the
   * idempotent-upsert contract: it applies each event, and re-applying an already-applied range is
   * still an application. Only a <em>self-fencing</em> projection can process a batch without
   * applying it, and such a projection must override this to say so: {@code WindowedProjection}
   * fences on the highest offset it has ever accumulated, so a dead-lettered range that was never
   * accumulated — the hole the dead-letter entry records — is fenced out wholesale once later
   * batches have advanced the fence past it. Reporting {@code false} makes the replayer KEEP the
   * entry (the range's only record) with a truthful outcome instead of discarding it on a feed that
   * changed nothing.
   *
   * <p><b>Decorators MUST override this and forward to the delegate's {@code
   * processDeadLetterReplay}</b>. A wrapper that overrides only the two {@code process} overloads
   * inherits this default, which calls the WRAPPER's own {@code process} — reaching the delegate's
   * {@code process}, never the delegate's override — and reports {@code true}: a wrapped
   * self-fencing projection then reads as "applied" on a wholly fenced replay and the replayer
   * discards the range's only record. The shipped decorators ({@code TracingProjectionDecorator},
   * {@code ValidatingProjectionDecorator}, {@code CacheAwareProjection}) forward it.
   *
   * @param batch the re-read dead-lettered range, in stream order
   * @param repository the replay transaction's repository to write through, or {@code null} for an
   *     at-least-once registration
   * @return {@code true} when at least one event of the batch was applied; {@code false} when the
   *     projection fenced the whole batch out and applied nothing
   */
  default boolean processDeadLetterReplay(
      List<EventEnvelope> batch, ProjectionRepository repository) {
    process(batch, repository);
    return true;
  }
}
