package org.streamrune.postgres;

import java.util.List;
import javax.sql.DataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCheckpointSaveException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * The fencing at-least-once {@link AtomicBatchProcessor} over {@code subscription_checkpoint}, the
 * checkpoint table of the event-store series, read by {@link PostgresOffsetStore}: the processor of
 * an at-least-once projection that keeps its read models wherever it writes them, under real
 * leadership.
 *
 * <p>{@link #fencesCheckpoint()} is {@code true} and {@link #transactional()} is {@code false}: the
 * updater is handed {@code null} and writes through its own repository, outside this processor's
 * transaction, so its writes are not rolled back with it — a batch is applied at least once. What
 * the processor guarantees is the checkpoint: one transaction locks the projection's checkpoint row
 * {@code FOR UPDATE}, rejects a superseded epoch and an overlapping batch <b>before the updater
 * runs</b>, runs the updater, advances the checkpoint under the monotonic guard and commits. A
 * stale leader can therefore neither re-apply a batch past the epoch check nor move the checkpoint
 * forward, a SKIP or dead-letter advance included (see {@link PostgresCheckpointFence} for the
 * three guards). This is how an at-least-once registration behaves on {@link
 * JdbcProjectionRepository}, whose transaction shell this processor shares ({@link
 * CheckpointTransaction#run}).
 *
 * <p>{@link #serializesReplay()} is {@code true}: a dead-letter replay takes the same row lock, so
 * it never interleaves with a live batch of the same projection.
 *
 * <p>Crash and failure points of {@link #executeAtomically}, and what stays durable:
 *
 * <ul>
 *   <li>Before the lock, or the lock fails (no connection, a failed statement, a killed process):
 *       the updater never ran and the checkpoint row is as it was (a seed the lock wrote rolls
 *       back). An {@link EventStoreException}.
 *   <li>The epoch fence or the overlap guard rejects the batch: the updater never ran, the
 *       transaction rolls back, the checkpoint is as it was. A {@link
 *       ProjectionCommitFencedException}.
 *   <li>The updater throws: its exception propagates unchanged after the rollback; the checkpoint
 *       is as it was. Whatever the updater wrote before it threw is the projection's own; the rerun
 *       re-applies the batch (at-least-once).
 *   <li>After the updater returned, the monotonic guard rejects the advance: a {@link
 *       ProjectionCommitFencedException}, the checkpoint is as it was.
 *   <li>After the updater returned, the save or the commit fails: the batch is applied and the
 *       checkpoint did not move — or, when {@code commit()} itself threw, its outcome is unknown. A
 *       {@link ProjectionCheckpointSaveException}: the runners back off and re-read from the stored
 *       checkpoint, so the batch is applied again if the checkpoint did not move, and the overlap
 *       guard rejects the re-read range if it did. Never skipped, never dead-lettered. A process
 *       that dies there leaves the same state: the server rolls the open transaction back, and the
 *       next leader re-applies the batch from the stored checkpoint.
 *   <li>After the commit: the checkpoint is advanced and its epoch is {@code GREATEST(stored,
 *       fencingEpoch)}. A failure to give the connection back is reported as a {@link
 *       ProjectionCheckpointSaveException} too, and resolves the same way.
 * </ul>
 */
public final class PostgresSubscriptionCheckpointProcessor implements AtomicBatchProcessor {

  private final DataSource dataSource;

  private final PostgresCheckpointFence checkpointFence =
      new PostgresCheckpointFence(CheckpointTable.SUBSCRIPTION);

  /**
   * A processor over the {@code subscription_checkpoint} table reachable through {@code
   * dataSource}.
   *
   * @param dataSource the event-store database's pool
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   */
  public PostgresSubscriptionCheckpointProcessor(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    this.dataSource = dataSource;
  }

  /**
   * Runs one batch under the projection's checkpoint-row lock: the epoch fence and the overlap
   * guard, then {@code projectionUpdater} with a {@code null} repository, then the checkpoint
   * advance to {@code newOffset} stamped with {@code GREATEST(stored, fencingEpoch)}, then the
   * commit. See the class documentation for every failure point.
   *
   * @param projectionName the projection
   * @param batch the batch; its first offset is checked against the stored checkpoint (an empty
   *     batch skips that check)
   * @param newOffset the offset the checkpoint advances to
   * @param fencingEpoch the caller's leadership epoch; below the stored epoch is rejected before
   *     the updater runs, {@code 0} is unfenced and always passes
   * @param projectionUpdater applies the batch through the projection's own repository; handed
   *     {@code null}
   * @param offsetStore ignored: the checkpoint is saved in this processor's transaction
   * @throws ProjectionCommitFencedException if a guard rejects the batch
   * @throws ProjectionCheckpointSaveException if the save or the commit failed after the updater
   *     returned, or giving the connection back failed after the commit (the checkpoint then did
   *     advance)
   * @throws EventStoreException if the transaction failed before the updater ran
   * @throws IllegalArgumentException if {@code projectionName} is {@code null}
   */
  @Override
  public void executeAtomically(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      OffsetStore offsetStore) {
    requireName(projectionName);
    var updaterReturned = new boolean[1];
    try {
      CheckpointTransaction.BATCH.run(
          dataSource,
          conn -> {
            // Reject a fenced-out (stale-epoch) or overlapping batch before the updater runs.
            checkpointFence.lockForBatch(
                conn,
                projectionName,
                fencingEpoch,
                batch.isEmpty() ? null : batch.getFirst().globalOffset());
            projectionUpdater.update(null);
            updaterReturned[0] = true;
            checkpointFence.saveLocked(conn, projectionName, newOffset, fencingEpoch);
            return null;
          });
    } catch (EventStoreException failure) {
      if (!updaterReturned[0]) {
        throw failure;
      }
      // The updater's writes are its own and stay applied; the checkpoint did not move.
      throw new ProjectionCheckpointSaveException(projectionName, newOffset, failure);
    }
  }

  /**
   * A replay and a batch of one projection both lock its checkpoint row {@code FOR UPDATE}, so one
   * waits for the other.
   *
   * @return always {@code true}
   */
  @Override
  public boolean serializesReplay() {
    return true;
  }

  /**
   * Applies a dead-lettered range in one transaction that first locks the projection's checkpoint
   * row {@code FOR UPDATE} (seeding a {@code 0} row if none exists) — the lock {@link
   * #executeAtomically} takes — and never writes that row: neither the offset nor the epoch moves.
   * {@code projectionUpdater} is handed {@code null}; its exception propagates after the rollback.
   *
   * @param projectionName the projection whose dead-lettered range is replayed
   * @param batch the re-read range (not inspected here)
   * @param projectionUpdater applies the range through the projection's own repository
   * @throws EventStoreException if the transaction fails
   * @throws IllegalArgumentException if {@code projectionName} is {@code null}
   */
  @Override
  public void executeReplay(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      ProjectionUpdater projectionUpdater) {
    requireName(projectionName);
    CheckpointTransaction.REPLAY.run(
        dataSource,
        conn -> {
          checkpointFence.lockForReplay(conn, projectionName);
          projectionUpdater.update(null);
          return null;
        });
  }

  /**
   * The updater writes outside this processor's transaction, so it is handed no repository.
   *
   * @return always {@code false}
   */
  @Override
  public boolean transactional() {
    return false;
  }

  /**
   * {@link #executeAtomically} rejects a superseded epoch under the checkpoint row's lock before
   * the updater runs, its checkpoint write is conditional on the row it locked, and {@link
   * #stampFencingEpoch} durably arms the fence at takeover.
   *
   * @return always {@code true}
   */
  @Override
  public boolean fencesCheckpoint() {
    return true;
  }

  /**
   * Stamps {@code fencingEpoch} on the projection's checkpoint row without moving the offset — the
   * takeover stamp (see {@link AtomicBatchProcessor#stampFencingEpoch}). Seeds the row at offset
   * {@code 0} if absent; epoch {@code 0} is a no-op; a same-epoch re-stamp changes nothing.
   *
   * @param projectionName the projection whose checkpoint row is stamped
   * @param fencingEpoch the newly acquired leadership epoch; {@code 0} is a no-op
   * @throws ProjectionEpochRegressionException if the row already stores a greater epoch
   * @throws EventStoreException if the connection or a statement fails
   * @throws IllegalArgumentException if {@code projectionName} is {@code null}
   */
  @Override
  public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
    requireName(projectionName);
    checkpointFence.stampEpoch(dataSource, projectionName, fencingEpoch);
  }

  private static void requireName(ProjectionName projectionName) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName cannot be null");
    }
  }
}
