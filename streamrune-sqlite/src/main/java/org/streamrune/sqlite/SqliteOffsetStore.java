package org.streamrune.sqlite;

import java.time.Clock;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * SQLite-backed {@link OffsetStore}: the last processed global offset of each named projection in
 * the {@code subscription_checkpoint} table, as {@code PostgresOffsetStore} keeps it on Postgres.
 *
 * <p><b>Guard.</b> {@link #saveOffset} applies the monotonic guard of the {@link OffsetStore}
 * contract: a save at or below the stored offset is a silent no-op. {@link #reset} rewinds to
 * offset {@code 0} past the guard and seeds the row for a name never seen. The {@code epoch} column
 * is never written here (always {@code 0} on SQLite, which has no leadership). The statements are
 * those of the checkpoint fence over {@code subscription_checkpoint} ({@link
 * SqliteCheckpointFence}), the fence {@link SqliteReadModelCheckpoints} runs over {@code
 * readmodel_checkpoint}, so both tables keep one set of checkpoint semantics.
 *
 * <p><b>Durability of a write.</b> Each write is one upsert inside its own {@code BEGIN IMMEDIATE}
 * ... {@code COMMIT} (see {@link SqliteTransactions}), so it is durable when the method returns.
 * The explicit transaction is deliberate: a bare statement on a connection where the application
 * had left a {@code BEGIN} open would silently join that transaction and not be durable. {@code
 * BEGIN IMMEDIATE} refuses such a connection ({@link EventStoreException}, nothing written, the
 * application's transaction untouched), and a connection in manual-commit mode is refused with
 * {@link IllegalStateException}. A read is one plain query.
 *
 * <p><b>Subscription checkpoint.</b> SQLite has no leadership, and no batch runs the fence's guards
 * on {@code subscription_checkpoint}, so {@link #subscriptionCheckpoint()} is the interface
 * default: this store paired with {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()}, under the
 * name {@value OffsetStore#SUBSCRIPTION_CHECKPOINT}.
 *
 * <p>The table must exist: build the event store through {@link SqliteEventStoreFactory} on the
 * same {@link SqliteDatabase} first, which migrates the baseline that holds it.
 */
public final class SqliteOffsetStore implements OffsetStore {

  private final DataSource dataSource;

  /** The fence over {@code subscription_checkpoint}, whose unfenced statements this store runs. */
  private final SqliteCheckpointFence fence;

  /**
   * Creates the store over {@code database}, stamping {@code updated_at} with {@link
   * Clock#systemUTC()}.
   *
   * @param database the database whose {@code subscription_checkpoint} table the store keeps
   * @throws NullPointerException if {@code database} is {@code null}
   */
  public SqliteOffsetStore(SqliteDatabase database) {
    this(database, Clock.systemUTC());
  }

  /**
   * Creates the store over {@code database}, stamping {@code updated_at} with {@code clock}.
   *
   * @param database the database whose {@code subscription_checkpoint} table the store keeps
   * @param clock the clock that stamps {@code updated_at}
   * @throws NullPointerException if an argument is {@code null}
   */
  public SqliteOffsetStore(SqliteDatabase database, Clock clock) {
    this.dataSource = Objects.requireNonNull(database, "database is required").dataSource();
    this.fence =
        new SqliteCheckpointFence(
            CheckpointTable.SUBSCRIPTION, Objects.requireNonNull(clock, "clock is required"));
  }

  /**
   * {@inheritDoc}
   *
   * @throws NullPointerException if {@code projectionName} is {@code null}, before any I/O, as on
   *     Postgres
   * @throws EventStoreException if the read fails
   */
  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return fence.lastOffset(dataSource, projectionName);
  }

  /**
   * Saves {@code offset} under the monotonic guard: a save at or below the stored offset is a
   * silent no-op. An intentional rewind is {@link #reset}, never a save.
   *
   * @param projectionName the projection whose checkpoint is saved
   * @param offset the last processed global offset
   * @throws NullPointerException if an argument is {@code null}, before any I/O, as on Postgres
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
   * Rewinds the checkpoint to offset {@code 0}, bypassing the monotonic guard, and seeds the row at
   * {@code 0} for a projection never seen.
   *
   * @param projectionName the projection whose checkpoint is rewound
   * @throws NullPointerException if {@code projectionName} is {@code null}, before any I/O, as on
   *     Postgres
   * @throws EventStoreException if the reset fails, as for {@link #saveOffset}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void reset(ProjectionName projectionName) {
    fence.reset(dataSource, projectionName);
  }
}
