package org.streamrune.core.projection;

import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Tracks the last processed global offset for each projection.
 *
 * <p><b>Single-writer requirement.</b> At most one runner should process a given {@code
 * projectionName} at a time. Deployments running multiple instances of the same projection
 * coordinate via {@link org.streamrune.core.subscription.SubscriptionLeadership single-active-
 * consumer leadership} (leader election); the offset guard below is the defense-in-depth backstop
 * for the unavoidable failover window.
 *
 * <p><b>Ordering.</b> Runners save the offset only <em>after</em> the batch has been processed
 * (save-after-process). A crash between processing and saving causes the batch to be redelivered —
 * the at-least-once contract that {@link Projection} implementations must tolerate via idempotence.
 * Offsets passed to {@link #saveOffset} are expected to be monotonically increasing per projection
 * name. The PostgreSQL/JDBC implementations <b>do</b> reject regressions via a monotonic guard: a
 * save that would move the offset backward (or sideways) is a silent no-op (0 rows updated, no
 * exception), so a stale replica racing after a failover cannot regress the checkpoint.
 *
 * <p><b>Reset caveat.</b> Because the guard blocks regressions, an intentional offset
 * <em>reset</em> or rebuild (e.g. replaying a projection from 0 after a read-model schema change)
 * must <b>not</b> go through {@link #saveOffset} — use the rebuild path (a direct {@code UPDATE
 * projection_offset SET last_offset = ?} / delete-and-reinsert invoked by an operator/rebuild
 * tool), never the runner.
 *
 * <p><b>Shared namespace.</b> The offset table is keyed by a plain string. Subscriptions that
 * checkpoint their delivery position in the same table pass a {@link ProjectionName} wrapping their
 * subscription name — the SQL column is unchanged; only the Java type at the call site is
 * strengthened.
 */
public interface OffsetStore {

  /**
   * Returns the last processed global offset for the given projection.
   *
   * <p>Note: {@link GlobalOffset#initial()} (offset 0) doubles as the "nothing stored yet"
   * sentinel; a projection that has genuinely processed up to offset 0 is indistinguishable from
   * one that never ran. This is safe because global offsets assigned by the event store start at 1.
   *
   * @param projectionName unique name of the projection
   * @return the last offset, or {@link GlobalOffset#initial()} if no offset has been stored
   */
  GlobalOffset getLastOffset(ProjectionName projectionName);

  /**
   * Saves the last processed global offset for the given projection. The PostgreSQL/JDBC
   * implementations apply a monotonic guard: a save that would move the offset backward or sideways
   * is a silent no-op. Use the rebuild/reset path — not this method — to intentionally rewind a
   * checkpoint (see class contract).
   *
   * @param projectionName unique name of the projection
   * @param offset the global offset to save
   */
  void saveOffset(ProjectionName projectionName, GlobalOffset offset);

  /**
   * Rewinds the projection's checkpoint to {@link GlobalOffset#initial()} (offset 0), the rebuild
   * path referenced in the class contract. Unlike {@link #saveOffset}, this <b>bypasses the
   * monotonic guard</b>: the whole point of a reset is to move the offset backward so the next run
   * replays from the beginning, which the guard would otherwise reject as a regression.
   *
   * <p>Default implementation throws {@link UnsupportedOperationException}. A default that
   * delegated to {@code saveOffset(name, GlobalOffset.initial())} previously sat here — harmless
   * for unguarded stores, but for a guarded implementation (PostgreSQL/JDBC/guarded in-memory) that
   * delegation is silently rejected by the very guard {@code saveOffset} applies (0 rows updated,
   * no exception, per the class contract), so a projection rebuild would appear to succeed while
   * never actually rewinding. This mirrors the repo's default-method policy of failing loudly,
   * never silently (see {@code DeadLetterQueue}'s default-method policy). Implementations <b>must
   * override</b> this with a direct unguarded write (see {@code PostgresOffsetStore}). An unguarded
   * store (whose {@code saveOffset} has no monotonic guard to begin with) may override this with
   * the one-line {@code saveOffset(projectionName, GlobalOffset.initial())} delegation explicitly —
   * that is a visible, deliberate choice, unlike inheriting a silent default.
   *
   * @param projectionName unique name of the projection to reset
   * @throws UnsupportedOperationException if not overridden
   */
  default void reset(ProjectionName projectionName) {
    throw new UnsupportedOperationException(
        "reset() is not implemented by this OffsetStore. If saveOffset(...) applies a monotonic"
            + " guard (as PostgreSQL/JDBC/guarded in-memory implementations do per the class"
            + " contract), delegating to saveOffset would silently no-op the rewind instead of"
            + " actually resetting the checkpoint — a projection rebuild would appear to succeed"
            + " while never rewinding. Override reset() with a direct unguarded write (see"
            + " PostgresOffsetStore), or — if this store has no guard — override it to explicitly"
            + " delegate to saveOffset(projectionName, GlobalOffset.initial()).");
  }
}
