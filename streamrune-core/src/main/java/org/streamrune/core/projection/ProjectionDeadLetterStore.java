package org.streamrune.core.projection;

import java.util.List;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Persistent store for failed projection batches.
 *
 * <p>Implementations are responsible for durably persisting dead-letter entries so that operators
 * can inspect, replay, or discard failed batches without losing audit information.
 */
public interface ProjectionDeadLetterStore {

  /**
   * Persists a dead-letter entry.
   *
   * @param entry the entry to save; must not be null
   */
  void save(ProjectionDeadLetterEntry entry);

  /**
   * Returns up to {@code limit} dead-letter entries for the given projection, ordered oldest first
   * — by {@code failedAt} ascending, tie-broken by {@code fromOffset} ascending — so a limited read
   * surfaces the oldest dead-letters first and a backlog larger than {@code limit} still drains
   * from the oldest range.
   *
   * @param projectionName the name of the projection; must not be null or blank
   * @param limit maximum number of entries to return; must be > 0
   * @return an immutable list of matching entries; never null
   */
  List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit);

  /**
   * Returns up to {@code limit} dead-letter entries across all projections, ordered oldest first —
   * by {@code failedAt} ascending, tie-broken by {@code fromOffset} ascending — so a limited read
   * surfaces the oldest dead-letters first and a backlog larger than {@code limit} still drains
   * from the oldest range.
   *
   * @param limit maximum number of entries to return; must be > 0
   * @return an immutable list of entries; never null
   */
  List<ProjectionDeadLetterEntry> readAll(int limit);

  /**
   * Removes the dead-letter entry identified by projection name and starting offset.
   *
   * @param projectionName the name of the projection; must not be null or blank
   * @param fromOffset the start offset of the batch to discard; must not be null
   */
  void discard(ProjectionName projectionName, GlobalOffset fromOffset);

  /**
   * Returns the current projection dead-letter backlog — the total number of dead-letter entries
   * across all projections (permanent read-model holes the runner advanced its checkpoint past).
   * Read-only: it takes no claim/lease, so it is safe to call while the runners poll. Used for
   * backlog observability — the projection runners sample it once per cycle and report it as the
   * {@code streamrune.projections.dead_letter_backlog} gauge, so un-replayed ranges stay visible
   * after a failure burst ends even though the projection reports itself health-UP.
   *
   * <p>The default throws {@link UnsupportedOperationException}: a store that does not support
   * counting is simply excluded from the depth gauge (the runners degrade gracefully after the
   * first throw, exactly like {@code DeadLetterQueue.countPending()} on the command DLQ);
   * dead-letter capture and replay are unaffected. Override it to enable the gauge. The framework's
   * {@code PostgresProjectionDeadLetterStore} and {@code InMemoryProjectionDeadLetterStore} both
   * override it.
   *
   * @return the number of dead-letter entries across all projections (never negative)
   * @throws UnsupportedOperationException if the implementation does not support counting
   */
  default long countPending() {
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support countPending() — override it to enable the projection dead-letter"
            + " depth gauge (streamrune.projections.dead_letter_backlog).");
  }
}
