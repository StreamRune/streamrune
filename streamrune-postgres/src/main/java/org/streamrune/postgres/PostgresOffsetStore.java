package org.streamrune.postgres;

import javax.sql.DataSource;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * PostgreSQL-backed {@link OffsetStore} using upsert semantics. Stores the last processed global
 * offset for each named projection in the {@code subscription_checkpoint} table.
 */
public final class PostgresOffsetStore implements OffsetStore {

  /**
   * The fence over {@code subscription_checkpoint}: the same statements {@link
   * PostgresSubscriptionCheckpointProcessor} locks and advances the checkpoint with.
   */
  private final PostgresCheckpointFence checkpointFence =
      new PostgresCheckpointFence(CheckpointTable.SUBSCRIPTION);

  private final DataSource dataSource;

  public PostgresOffsetStore(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    this.dataSource = dataSource;
  }

  /**
   * The event store's subscription checkpoint over this offset store: this store as the checkpoints
   * and a {@link PostgresSubscriptionCheckpointProcessor} on the same {@link DataSource} as the
   * processor, under the name {@value OffsetStore#SUBSCRIPTION_CHECKPOINT}, overriding the
   * nonatomic default: the processor fences the checkpoint. The processor saves the checkpoint in
   * its own transaction and ignores the offset store it is handed, so it must reach the database
   * this store reads; building both from one DataSource is what guarantees that.
   *
   * @return the pair a runner reads and commits an at-least-once registration's position through
   */
  @Override
  public CheckpointTarget subscriptionCheckpoint() {
    return new CheckpointTarget(
        SUBSCRIPTION_CHECKPOINT, this, new PostgresSubscriptionCheckpointProcessor(dataSource));
  }

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return checkpointFence.lastOffset(dataSource, projectionName);
  }

  /**
   * Saves {@code offset} under the monotonic guard: a save at or below the stored offset is a
   * silent no-op, so a regression is impossible even in a brief split-brain window. An intentional
   * rewind is {@link #reset}, never a save (see the {@link OffsetStore} javadoc).
   */
  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    checkpointFence.saveUnlocked(dataSource, projectionName, offset);
  }

  /**
   * Rewinds the checkpoint to offset {@code 0}, bypassing the monotonic guard: {@link #saveOffset}
   * would no-op the regression to {@code 0}, silently defeating a projection reset. Seeds the row
   * at {@code 0} for a projection never seen; the epoch is left as it is.
   */
  @Override
  public void reset(ProjectionName projectionName) {
    checkpointFence.reset(dataSource, projectionName);
  }
}
