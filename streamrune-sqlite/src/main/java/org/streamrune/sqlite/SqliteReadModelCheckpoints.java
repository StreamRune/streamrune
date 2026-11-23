package org.streamrune.sqlite;

import java.time.Clock;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * The checkpoints of the projections a SQLite read-model store holds: an {@link OffsetStore} over
 * {@code readmodel_checkpoint}, the table of the read-model series ({@link
 * SqliteSchemaSeries#READ_MODEL}), kept in the database that holds the read models. It is the read
 * side of the checkpoint the store's projection repository advances when it commits a batch,
 * through the same fence statements; {@link SqliteOffsetStore} is its twin over the event store's
 * {@code subscription_checkpoint}.
 *
 * <p><b>Guards.</b> {@link #saveOffset} applies the monotonic guard of the {@link OffsetStore}
 * contract: a save at or below the stored offset is a silent no-op. {@link #reset} rewinds to
 * offset {@code 0} past the guard and seeds the row for a name never seen. Neither touches the
 * {@code epoch} column, which only the store's fenced batch commits and the takeover stamp write.
 *
 * <p><b>Durability of a write.</b> Each write is one upsert inside its own {@code BEGIN IMMEDIATE}
 * ... {@code COMMIT} (see {@link SqliteTransactions}), so it is durable when the method returns. A
 * connection with an open transaction of the application's is refused ({@link EventStoreException},
 * nothing written, the application's transaction untouched), and a connection in manual-commit mode
 * is refused with {@link IllegalStateException}. A read is one plain query. {@code updated_at} is
 * stamped with the application clock in epoch microseconds.
 *
 * <p>The table must exist: migrate {@link SqliteSchemaSeries#READ_MODEL} on the same {@link
 * SqliteDatabase} first.
 */
public final class SqliteReadModelCheckpoints implements OffsetStore {

  private final DataSource dataSource;

  /** The fence over {@code readmodel_checkpoint}: the statements the store's processor commits. */
  private final SqliteCheckpointFence fence;

  /**
   * Checkpoints over the {@code readmodel_checkpoint} table of {@code database}, stamping {@code
   * updated_at} with {@link Clock#systemUTC()}.
   *
   * @param database the read-model database
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteReadModelCheckpoints(SqliteDatabase database) {
    this(database, Clock.systemUTC());
  }

  /**
   * Checkpoints over the {@code readmodel_checkpoint} table of {@code database}, stamping {@code
   * updated_at} with {@code clock}.
   *
   * @param database the read-model database
   * @param clock the clock that stamps {@code updated_at}
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  public SqliteReadModelCheckpoints(SqliteDatabase database, Clock clock) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock is required");
    }
    this.dataSource = database.dataSource();
    this.fence = new SqliteCheckpointFence(clock);
  }

  /**
   * {@inheritDoc}
   *
   * @throws NullPointerException if {@code projectionName} is {@code null}, before any I/O
   * @throws EventStoreException if the read fails
   */
  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return fence.lastOffset(dataSource, projectionName);
  }

  /**
   * Saves {@code offset} under the monotonic guard: a save at or below the stored offset is a
   * silent no-op, so a regression is impossible even in a brief split-brain window. An intentional
   * rewind is {@link #reset}, never a save (see the {@link OffsetStore} javadoc).
   *
   * @param projectionName the projection whose checkpoint is saved
   * @param offset the last processed global offset
   * @throws NullPointerException if an argument is {@code null}, before any I/O
   * @throws EventStoreException if the save fails, including when SQLite's write lock is not
   *     granted within {@code busy_timeout} or the connection has a transaction open
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    fence.saveUnlocked(dataSource, projectionName, offset);
  }

  /**
   * Rewinds the checkpoint to offset {@code 0}, bypassing the monotonic guard: {@link #saveOffset}
   * would no-op the regression to {@code 0}, silently defeating a projection reset. Seeds the row
   * at {@code 0} for a projection never seen; the epoch is left as it is.
   *
   * @param projectionName the projection whose checkpoint is rewound
   * @throws NullPointerException if {@code projectionName} is {@code null}, before any I/O
   * @throws EventStoreException if the reset fails, as for {@link #saveOffset}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void reset(ProjectionName projectionName) {
    fence.reset(dataSource, projectionName);
  }

  /**
   * Refused: these are a read-model store's checkpoints, never the event store's subscription
   * checkpoint. The inherited default would pair {@code readmodel_checkpoint} with {@link
   * org.streamrune.core.projection.AtomicBatchProcessor#nonAtomicAtLeastOnce()}, which does not
   * fence, under the name {@value OffsetStore#SUBSCRIPTION_CHECKPOINT}: a target that names a table
   * it does not use. The read-model store pairs these checkpoints with its own processor; the
   * subscription checkpoint is {@link SqliteOffsetStore#subscriptionCheckpoint()}.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public CheckpointTarget subscriptionCheckpoint() {
    throw new UnsupportedOperationException(
        "SqliteReadModelCheckpoints are a read-model store's checkpoints (readmodel_checkpoint),"
            + " never the event store's subscription checkpoint: use the store's"
            + " checkpointTarget() to run a projection on them, or SqliteOffsetStore's"
            + " subscriptionCheckpoint() for the event store's subscription_checkpoint");
  }

  /**
   * The fence over these checkpoints, for the read-model store's processor: it runs {@link
   * SqliteCheckpointFence#lockForBatch} and {@link SqliteCheckpointFence#saveLocked} on its own
   * write transaction, so the batch's read-model writes and the checkpoint advance commit together.
   */
  SqliteCheckpointFence fence() {
    return fence;
  }
}
