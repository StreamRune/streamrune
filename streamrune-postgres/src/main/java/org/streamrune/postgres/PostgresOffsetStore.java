package org.streamrune.postgres;

import javax.sql.DataSource;
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
