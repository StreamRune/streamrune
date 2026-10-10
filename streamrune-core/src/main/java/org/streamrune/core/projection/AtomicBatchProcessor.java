package org.streamrune.core.projection;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * Coordinates a projection's read-model write and its checkpoint (the offset save).
 *
 * <p>A transactional implementation (e.g. JDBC-backed) runs both in one transaction and hands the
 * {@link ProjectionUpdater} a {@link ProjectionRepository} bound to it: every operation on that
 * repository runs on the same connection as the offset save, so the read-model writes and the
 * checkpoint commit — or roll back — together. Writes that bypass the supplied repository (e.g.
 * through a separately captured repository instance) are NOT part of the transaction. The nonatomic
 * processor ({@link #nonAtomicAtLeastOnce()}) runs them as two commits — the updater, then the
 * offset save — and hands the updater {@code null}; a save that fails after the updater returned
 * leaves the batch applied and the checkpoint where it was, so the batch is read and applied again
 * (at-least-once).
 *
 * <p>The runner decides per registration what the projection receives from the updater: a {@link
 * ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} or {@link ProjectionDeliveryMode#EXTERNAL_EFFECT}
 * registration gets the transaction-scoped repository, an {@link
 * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} one gets {@code null} under every processor.
 *
 * <p>The transaction-scoped repository must also override {@link ProjectionRepository#afterCommit}:
 * an action registered on it (such as the query-cache eviction of {@code CacheAwareProjection})
 * runs only once the transaction has committed, and is dropped if it rolls back.
 *
 * <p>A processor that keeps a lock per projection also applies dead-lettered ranges under it
 * ({@link #executeReplay}, announced by {@link #serializesReplay()}), so a dead-letter replay never
 * interleaves with a live batch of the same projection.
 */
@FunctionalInterface
public interface AtomicBatchProcessor {

  /**
   * Callback that applies a batch of events to the read model.
   *
   * <p>Transactional processors supply a {@link ProjectionRepository} bound to the processor's open
   * transaction; all writes must go through it to be atomic with the offset save. The nonatomic
   * processor ({@link #nonAtomicAtLeastOnce()}) supplies {@code null}, and the updater must fall
   * back to its own repository.
   */
  @FunctionalInterface
  interface ProjectionUpdater {

    /**
     * Performs the projection update (load → apply events → save).
     *
     * @param repository transaction-scoped repository to write through, or {@code null} when the
     *     processor is non-transactional
     */
    void update(ProjectionRepository repository);
  }

  /**
   * Executes projection update and offset save. Implementations may run these in a single database
   * transaction for atomicity.
   *
   * @param projectionName the name of the projection
   * @param batch the batch of events to process
   * @param newOffset the global offset to save after successful processing
   * @param fencingEpoch the caller's held leadership epoch (see {@link
   *     org.streamrune.core.subscription.SubscriptionLeadership.Lease#epoch()}); {@code 0} means
   *     unfenced (single-node / {@link
   *     org.streamrune.core.subscription.SubscriptionLeadership#NOOP}). Transactional
   *     implementations reject a commit whose epoch is below the epoch already stored for this
   *     projection and stamp the epoch on commit, fencing out a superseded leader. An
   *     implementation whose {@link #supportsFencing()} is {@code false} must never be handed a
   *     non-zero epoch (the leadership-aware runners reject that pairing at build time) and must
   *     refuse the commit if it is — see {@link #nonAtomicAtLeastOnce()}.
   * @param projectionUpdater code that performs the projection update; receives the
   *     transaction-scoped repository (or {@code null} from non-transactional implementations)
   * @param offsetStore the offset store to save the offset to (ignored by transactional
   *     implementations that save the offset internally)
   */
  void executeAtomically(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      OffsetStore offsetStore);

  /**
   * Whether this processor is transactional and fenced. {@code true} means all three of:
   *
   * <ol>
   *   <li>{@link #executeAtomically} runs the updater and the checkpoint write in one transaction;
   *   <li>the repository handed to the updater is bound to that transaction and is never {@code
   *       null};
   *   <li>a commit from a superseded epoch is rejected before any read-model write, and {@link
   *       #stampFencingEpoch} stamps the epoch durably.
   * </ol>
   *
   * <p>A processor that cannot say all three returns {@code false}. The delivery policy relies on
   * it: a {@link ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} or {@link
   * ProjectionDeliveryMode#EXTERNAL_EFFECT} registration is refused on a processor that returns
   * {@code false}.
   *
   * <p>This is what makes the fence fail <b>closed</b>. Before it existed, pairing a real
   * (multi-replica) {@link org.streamrune.core.subscription.SubscriptionLeadership} with a
   * processor that ignores the epoch was accepted silently: the takeover stamp was a no-op that
   * reported success, a non-transactional processor dropped the epoch on the floor, and a
   * superseded leader's batch was applied to the read model before anything could reject it. The
   * leadership-aware runners now refuse to build that pairing, so an unfenced multi-replica
   * projection is a configuration error surfaced at startup rather than a silent double-apply in
   * production.
   *
   * <p>Defaults to {@code false}: a processor that has not deliberately implemented the fence does
   * not have one. Transactional implementations override it — the framework's {@code
   * JdbcProjectionRepository} does, under the same {@code FOR UPDATE} checkpoint-row lock that
   * carries the overlap and monotonic guards.
   *
   * @return {@code true} if a commit from a superseded epoch is rejected before any read-model
   *     write
   */
  default boolean supportsFencing() {
    return false;
  }

  /**
   * Durably stamps the caller's fencing epoch on the projection's checkpoint <em>without moving the
   * offset</em>.
   *
   * <p>The epoch fence in {@link #executeAtomically} compares the caller's epoch against the epoch
   * <em>stored on the checkpoint row</em>, and a committed advance is what stamps it — so between a
   * leadership takeover and the new leader's first committed advance the row still carries the
   * previous leader's epoch and every write from that superseded leader passes the fence. Combined
   * with a superseded leader's fail-open local lease view (its staleness clock starts at the
   * client-side instant recorded <em>after</em> the renew committed, so a GC/VM pause stretches it
   * past DB lease expiry), a paused-then-resumed old leader could commit a stale SKIP/DLQ
   * forward-jump in that window, silently dropping events from the read model.
   *
   * <p>Leadership-aware runners therefore call this at takeover — after acquiring the lease,
   * <b>before reading or processing a single event</b> — so the fence starts rejecting the
   * superseded leader's writes from the moment of takeover, not only after the new leader's first
   * commit. Implementations must make the write idempotent and monotonic (never regress a stored
   * epoch — an out-of-order stamp from an older epoch is a no-op) and must never move a stored
   * offset; for a projection with no checkpoint row yet they seed the same {@code 0} floor {@link
   * #executeAtomically}'s checkpoint seeding uses. Epoch {@code 0} (unfenced / {@code
   * SubscriptionLeadership.NOOP}) callers need not stamp.
   *
   * <p>The default is a no-op only for the unfenced epoch {@code 0}; a non-zero epoch throws. The
   * previous unconditional empty body made a processor that cannot fence indistinguishable from one
   * that durably stamped — the runners' {@code stampTakeoverEpoch} treats "did not throw" as "the
   * fence is armed" and proceeds to read and process. Reaching this default with a non-zero epoch
   * now means an implementation claimed {@link #supportsFencing()} without implementing the stamp
   * (the runners reject the non-fencing pairing at build time), and that must fail loudly rather
   * than leave the takeover window open — the same fail-loudly policy as {@link
   * OffsetStore#reset}'s default. Transactional implementations override; the framework's {@code
   * JdbcProjectionRepository} does.
   *
   * @param projectionName the projection whose checkpoint row is stamped
   * @param fencingEpoch the newly acquired leadership epoch to stamp; {@code 0} means unfenced and
   *     is ignored
   * @throws UnsupportedOperationException if a non-zero epoch is stamped on a processor that has
   *     not implemented the fence
   */
  default void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
    if (fencingEpoch != 0L) {
      throw new UnsupportedOperationException(
          "AtomicBatchProcessor "
              + getClass().getName()
              + " cannot honour the leadership fencing epoch "
              + fencingEpoch
              + " for projection '"
              + (projectionName != null
                  ? LogSanitizer.sanitizeForLog(projectionName.value())
                  : "null")
              + "': it does not implement stampFencingEpoch. Silently accepting the takeover stamp"
              + " would leave the epoch fence inert and let a superseded leader re-apply its"
              + " in-flight batch to the read model. Implement stampFencingEpoch (and"
              + " executeAtomically's epoch check) or leave supportsFencing() at false.");
    }
  }

  /**
   * Registration-time check of the name a projection is registered under. The projection runners
   * call it for every projection they are given — at {@code build()}, or at {@code run()} for the
   * single-projection runners — before they read a single event, so a name this processor cannot
   * checkpoint and store read models under fails at startup instead of halting, or dead-lettering,
   * the projection on its first batch.
   *
   * <p>The default accepts every name: the nonatomic processor and in-memory processors key
   * everything by the {@link ProjectionName} value alone. The framework's {@code
   * JdbcProjectionRepository} overrides it with its table-name rule — {@code
   * [a-z_][a-z0-9_]{0,57}}, used exactly as given, so every valid name has a table of its own. As
   * long as each projection saves its read models under the name it is registered under, that also
   * makes the runners' exact-name duplicate check (per runner builder, per application context) a
   * read-model table collision check. It does not cover a projection that saves under another name
   * (a {@code BaseProjection} writes under its own save name), nor two runners in one JVM
   * registering the same name.
   *
   * @param projectionName the name the projection is registered under
   * @throws IllegalArgumentException if this processor cannot store a projection under that name
   */
  default void checkProjectionName(ProjectionName projectionName) {
    // Every name is accepted unless the implementation has a naming rule.
  }

  /**
   * Makes the read-model storage of {@code projectionName} ready before a batch writes to it
   * through the handed repository. A runner calls it before each batch of a {@link
   * ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} or {@link ProjectionDeliveryMode#EXTERNAL_EFFECT}
   * registration, and never for an {@link ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} one,
   * whose read models live wherever the projection writes them. Implementations make it cheap to
   * repeat. The framework's {@code JdbcProjectionRepository} creates the {@code <name>_view} table
   * here, outside the batch transaction.
   *
   * <p>The default does nothing: a store without a per-projection structure has nothing to prepare.
   *
   * @param projectionName the projection about to write through the handed repository
   */
  default void prepareReadModel(ProjectionName projectionName) {
    // Nothing to prepare unless the implementation keeps a structure per projection.
  }

  /**
   * Whether {@link #executeReplay} excludes {@link #executeAtomically} for the same projection:
   * while one of them runs for a projection name, the other waits. {@code true} is what lets a
   * dead-letter replay run beside a live runner.
   *
   * <p>Defaults to {@code false}: a processor that has not implemented the exclusion does not have
   * it. The framework's {@code JdbcProjectionRepository} returns {@code true}: both take the
   * projection's checkpoint row {@code FOR UPDATE}.
   *
   * @return {@code true} if a replay and a batch of one projection never run at the same time
   */
  default boolean serializesReplay() {
    return false;
  }

  /**
   * Applies a dead-lettered range to the read model beside a live runner. The range lies behind the
   * checkpoint: the runner moved past it when it dead-lettered it.
   *
   * <p>An implementation takes the same per-projection lock as {@link #executeAtomically} for the
   * whole call, so a replay and a live batch of one projection never interleave: a
   * read-modify-write projection reads rows no other writer of that projection is changing. It runs
   * {@code projectionUpdater} once, with the repository {@link #executeAtomically} would hand it —
   * a transactional implementation hands one bound to the replay's own transaction, so the range is
   * applied all-or-nothing. It never writes the checkpoint: neither the offset nor the fencing
   * epoch moves.
   *
   * <p>An exception from {@code projectionUpdater} propagates; a transactional implementation rolls
   * the range back first.
   *
   * <p>The default refuses: a processor whose {@link #serializesReplay()} is {@code false} has no
   * lock to take, and running the range unlocked beside a live batch loses one of the two writes
   * whenever both read a row before either saves it. Idempotency does not prevent that — neither
   * side applies anything twice.
   *
   * @param projectionName the projection whose dead-lettered range is replayed
   * @param batch the re-read range, in stream order
   * @param projectionUpdater applies the range; receives the transaction-scoped repository, or
   *     {@code null} from a non-transactional implementation
   * @throws UnsupportedOperationException if this processor cannot serialize a replay with a live
   *     batch
   */
  default void executeReplay(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      ProjectionUpdater projectionUpdater) {
    throw new UnsupportedOperationException(
        "AtomicBatchProcessor "
            + (this instanceof NonAtomicAtLeastOnce
                ? "nonAtomicAtLeastOnce()"
                : getClass().getName())
            + " cannot replay a dead-lettered range of projection '"
            + (projectionName != null
                ? LogSanitizer.sanitizeForLog(projectionName.value())
                : "null")
            + "' beside a live runner: it holds no per-projection lock, so the replay and a live"
            + " batch could both read a row before either saves it, and one of the two writes would"
            + " be lost. Stop the projection's runner and call"
            + " ProjectionDeadLetterReplayer.replayWithRunnerStopped, or run the projection on a"
            + " processor whose serializesReplay() is true (JdbcProjectionRepository).");
  }

  /**
   * The non-transactional processor: {@code process(batch, null)} then {@code
   * offsetStore.saveOffset(...)}, two commits. Legal only for {@link
   * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} registrations and only under {@link
   * org.streamrune.core.subscription.SubscriptionLeadership#NOOP} (it cannot fence; a non-zero
   * epoch is refused). The name is the opt-in: a reader of the call site sees what was chosen, and
   * nothing else — no field, constant or {@code null} coercion — produces it. A failure of {@code
   * saveOffset} after the updater returned is rethrown as {@link
   * ProjectionCheckpointSaveException}.
   *
   * <p>It holds no per-projection lock, so it refuses {@link #executeReplay}: a dead-lettered range
   * of a registration on this processor is replayed with its runner stopped ({@code
   * ProjectionDeadLetterReplayer.replayWithRunnerStopped}).
   *
   * @return the one nonatomic processor
   */
  static AtomicBatchProcessor nonAtomicAtLeastOnce() {
    return NonAtomicAtLeastOnce.INSTANCE;
  }

  /**
   * {@code true} when a write through the repository this processor hands to the updater lands in
   * the same store as a write through {@code target}. Compared through {@link
   * ProjectionRepository#writeTargetIdentity()}, so a bean proxy on either side is transparent: a
   * proxy forwards the method call to the real object, whereas {@code ==} and {@code instanceof} on
   * the proxy itself would fail. A processor that wraps another forwards this.
   *
   * @param target the repository a projection says it writes to ({@link Projection#writeTarget()})
   * @return whether this processor's transaction-scoped repository writes to the same store
   * @throws IllegalArgumentException if {@code target} is {@code null}
   */
  default boolean writesTo(ProjectionRepository target) {
    if (target == null) {
      throw new IllegalArgumentException("target is required");
    }
    Object mine = (this instanceof ProjectionRepository self) ? self.writeTargetIdentity() : this;
    return mine.equals(target.writeTargetIdentity());
  }

  /**
   * The processor behind {@link #nonAtomicAtLeastOnce()}. A named class, not a lambda, so the
   * runners can print its name in the startup INFO line and tests can assert on it; the singleton
   * lives here and not on the interface so the only way to reach it is the factory method.
   *
   * <p>Being non-transactional it cannot fence: {@link #supportsFencing()} stays {@code false} and
   * a non-zero {@code fencingEpoch} is <b>refused</b>, not ignored. The refusal is defence in depth
   * — the leadership-aware runners already reject this pairing at build time — and it sits at the
   * damage site: the read-model write happens inside {@code projectionUpdater.update}, so a
   * processor that shrugged off the epoch would apply a superseded leader's batch before any guard
   * could run.
   */
  final class NonAtomicAtLeastOnce implements AtomicBatchProcessor {

    private static final NonAtomicAtLeastOnce INSTANCE = new NonAtomicAtLeastOnce();

    private NonAtomicAtLeastOnce() {}

    @Override
    public void executeAtomically(
        ProjectionName projectionName,
        List<EventEnvelope> batch,
        GlobalOffset newOffset,
        long fencingEpoch,
        ProjectionUpdater projectionUpdater,
        OffsetStore offsetStore) {
      if (fencingEpoch != 0L) {
        throw new IllegalStateException(
            "AtomicBatchProcessor.nonAtomicAtLeastOnce() was handed leadership fencing epoch "
                + fencingEpoch
                + " for projection '"
                + (projectionName != null
                    ? LogSanitizer.sanitizeForLog(projectionName.value())
                    : "null")
                + "' but is non-transactional and cannot fence. Refusing the commit rather than"
                + " applying a possibly-superseded leader's batch to the read model. Pass a"
                + " transactional AtomicBatchProcessor (JdbcProjectionRepository) or run unfenced"
                + " with SubscriptionLeadership.NOOP.");
      }
      projectionUpdater.update(null);
      try {
        offsetStore.saveOffset(projectionName, newOffset);
      } catch (RuntimeException saveFailure) {
        throw new ProjectionCheckpointSaveException(projectionName, newOffset, saveFailure);
      }
    }

    @Override
    public String toString() {
      return "nonAtomicAtLeastOnce";
    }
  }
}
