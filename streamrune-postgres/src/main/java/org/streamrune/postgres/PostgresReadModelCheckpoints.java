package org.streamrune.postgres;

import javax.sql.DataSource;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * The checkpoints of the projections routed to a {@link PostgresReadModelStore}: an {@link
 * OffsetStore} over {@code readmodel_checkpoint}, the table of the read-model series, kept in the
 * database that holds the read models. It is the read side of the checkpoint the store's {@link
 * JdbcProjectionRepository} advances when it commits a batch, through the same {@link
 * PostgresCheckpointFence} statements; {@link PostgresOffsetStore} is its twin over the event
 * store's {@code subscription_checkpoint}.
 */
public final class PostgresReadModelCheckpoints implements OffsetStore {

  /** The fence over {@code readmodel_checkpoint}: the statements the store's processor commits. */
  private final PostgresCheckpointFence checkpointFence =
      new PostgresCheckpointFence(CheckpointTable.READ_MODEL);

  private final DataSource dataSource;

  /**
   * Checkpoints over the {@code readmodel_checkpoint} table reachable through {@code dataSource}.
   *
   * @param dataSource the read-model database's pool
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   */
  public PostgresReadModelCheckpoints(DataSource dataSource) {
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
