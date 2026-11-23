package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * SQLite-backed {@link OffsetStore}: the last processed global offset of each named projection in
 * the {@code subscription_checkpoint} table, as {@code PostgresOffsetStore} keeps it on Postgres.
 *
 * <p><b>Guard.</b> {@link #saveOffset} applies the monotonic guard of the {@link OffsetStore}
 * contract: a save at or below the stored offset is a silent no-op. {@link #reset} rewinds to
 * offset {@code 0} past the guard and seeds the row for a name never seen. The {@code epoch} column
 * is never written here (always {@code 0} on SQLite, which has no leadership).
 *
 * <p><b>Durability of a write.</b> Each write is one upsert inside its own {@code BEGIN IMMEDIATE}
 * ... {@code COMMIT} (see {@link SqliteTransactions}), so it is durable when the method returns.
 * The explicit transaction is deliberate: a bare statement on a connection where the application
 * had left a {@code BEGIN} open would silently join that transaction and not be durable. {@code
 * BEGIN IMMEDIATE} refuses such a connection ({@link EventStoreException}, nothing written, the
 * application's transaction untouched), and a connection in manual-commit mode is refused with
 * {@link IllegalStateException}. A read is one plain query.
 *
 * <p><b>Subscription checkpoint.</b> SQLite has no leadership and no checkpoint fence, so {@link
 * #subscriptionCheckpoint()} is the interface default: this store paired with {@link
 * AtomicBatchProcessor#nonAtomicAtLeastOnce()}, under the name {@value
 * OffsetStore#SUBSCRIPTION_CHECKPOINT}.
 *
 * <p>The table must exist: build the event store through {@link SqliteEventStoreFactory} on the
 * same {@link SqliteDatabase} first, which migrates the baseline that holds it.
 */
public final class SqliteOffsetStore implements OffsetStore {

  private static final String SELECT_OFFSET =
      "SELECT last_offset FROM subscription_checkpoint WHERE projection_name = ?";

  // The monotonic guard: a backward or sideways save updates no row. Parameters: name, offset,
  // updated_at.
  private static final String SAVE =
      "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at)"
          + " VALUES (?, ?, ?)"
          + " ON CONFLICT (projection_name) DO UPDATE SET"
          + " last_offset = excluded.last_offset, updated_at = excluded.updated_at"
          + " WHERE excluded.last_offset > subscription_checkpoint.last_offset";

  // The rewind, past the guard; the epoch is left as it is. Parameters: name, updated_at.
  private static final String RESET =
      "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at)"
          + " VALUES (?, 0, ?)"
          + " ON CONFLICT (projection_name) DO UPDATE SET"
          + " last_offset = 0, updated_at = excluded.updated_at";

  private final DataSource dataSource;
  private final Clock clock;

  /**
   * Creates the store over {@code database}, stamping {@code updated_at} with {@link
   * Clock#systemUTC()}.
   *
   * @throws NullPointerException if {@code database} is {@code null}
   */
  public SqliteOffsetStore(SqliteDatabase database) {
    this(database, Clock.systemUTC());
  }

  /**
   * Creates the store over {@code database}, stamping {@code updated_at} with {@code clock}.
   *
   * @throws NullPointerException if an argument is {@code null}
   */
  public SqliteOffsetStore(SqliteDatabase database, Clock clock) {
    this.dataSource = Objects.requireNonNull(database, "database is required").dataSource();
    this.clock = Objects.requireNonNull(clock, "clock is required");
  }

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_OFFSET)) {
      ps.setString(1, projectionName.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? GlobalOffset.of(rs.getLong(1)) : GlobalOffset.initial();
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to read offset for projection: " + shown(projectionName), e);
    }
  }

  /**
   * Saves {@code offset} under the monotonic guard: a save at or below the stored offset is a
   * silent no-op. An intentional rewind is {@link #reset}, never a save.
   *
   * @throws EventStoreException if the save fails, including when SQLite's write lock is not
   *     granted within {@code busy_timeout} or the connection has a transaction open
   */
  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    write(
        projectionName,
        "save",
        SAVE,
        ps -> {
          ps.setString(1, projectionName.value());
          ps.setLong(2, offset.value());
          ps.setLong(3, SqliteTimestamps.toMicros(clock.instant()));
        });
  }

  /**
   * Rewinds the checkpoint to offset {@code 0}, bypassing the monotonic guard, and seeds the row at
   * {@code 0} for a projection never seen.
   *
   * @throws EventStoreException if the reset fails, as for {@link #saveOffset}
   */
  @Override
  public void reset(ProjectionName projectionName) {
    write(
        projectionName,
        "reset",
        RESET,
        ps -> {
          ps.setString(1, projectionName.value());
          ps.setLong(2, SqliteTimestamps.toMicros(clock.instant()));
        });
  }

  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }

  private void write(ProjectionName projectionName, String what, String sql, Binder binder) {
    String failure = "Failed to " + what + " offset for projection: " + shown(projectionName);
    try (Connection conn = dataSource.getConnection()) {
      SqliteTransactions.inTransaction(
          conn,
          "projection offset " + what,
          c -> {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
              binder.bind(ps);
              return ps.executeUpdate();
            }
          });
    } catch (SQLException e) {
      EventStoreException lockFailure =
          SqliteEventStore.lockFailure(
              e, "offset " + what + " of projection " + shown(projectionName));
      throw lockFailure != null ? lockFailure : new EventStoreException(failure, e);
    }
  }

  /** A projection name rendered for an exception message: caller text, so sanitized. */
  private static String shown(ProjectionName name) {
    return LogSanitizer.sanitizeForLog(name.value());
  }
}
