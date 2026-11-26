package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * The SQLite {@link ProjectionDeadLetterStore}: failed projection batches in the {@code
 * projection_dead_letters} table of a {@link SqliteDatabase}, with the semantics of {@code
 * PostgresProjectionDeadLetterStore}. The table belongs to the event-store baseline, which {@link
 * SqliteEventStoreFactory} migrates.
 *
 * <p>{@link #save} is one upsert on the primary key {@code (projection_name, from_offset)}: a batch
 * that fails again refreshes its entry, and {@code attempts} becomes the larger of the stored count
 * plus one and the new entry's. {@code failed_at} is the entry's own instant, in epoch
 * microseconds.
 *
 * <p><b>Durable on return.</b> Every write, single statements included, runs as {@code BEGIN
 * IMMEDIATE} … {@code COMMIT} through the one SQLite write helper, so it is committed when it
 * returns. A connection on which the application left a transaction open is refused rather than
 * joined (its {@code BEGIN IMMEDIATE} fails), and a connection handed out in manual-commit mode is
 * refused with an {@link IllegalStateException} before anything is written, as every SQLite store
 * refuses it. A write waits for the write lock at most the connection's {@code busy_timeout} and
 * then fails with an {@link EventStoreException} naming {@code busy_timeout}, having written
 * nothing. Reads run in autocommit and never wait for a writer.
 *
 * <p><b>Failures.</b> A storage failure is an {@link EventStoreException}. A {@code null} argument
 * and a non-positive {@code limit} are refused with {@link IllegalArgumentException} before any
 * I/O.
 */
public final class SqliteProjectionDeadLetterStore implements ProjectionDeadLetterStore {

  private static final String COLUMNS =
      "projection_name, from_offset, to_offset, batch_size, error_type, error_message, attempts, "
          + "failed_at";

  // attempts stays monotonic: at least one more than the stored value, or the caller's count if
  // higher (Postgres: GREATEST; SQLite's two-argument max() is the scalar maximum).
  private static final String UPSERT =
      "INSERT INTO projection_dead_letters ("
          + COLUMNS
          + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
          + "ON CONFLICT (projection_name, from_offset) DO UPDATE SET "
          + "to_offset = excluded.to_offset, "
          + "batch_size = excluded.batch_size, "
          + "error_type = excluded.error_type, "
          + "error_message = excluded.error_message, "
          + "attempts = max(projection_dead_letters.attempts + 1, excluded.attempts), "
          + "failed_at = excluded.failed_at";

  private static final String SELECT_BY_NAME =
      "SELECT "
          + COLUMNS
          + " FROM projection_dead_letters WHERE projection_name = ? "
          + "ORDER BY failed_at ASC, from_offset ASC LIMIT ?";

  private static final String SELECT_ALL =
      "SELECT "
          + COLUMNS
          + " FROM projection_dead_letters ORDER BY failed_at ASC, from_offset ASC LIMIT ?";

  private static final String DELETE =
      "DELETE FROM projection_dead_letters WHERE projection_name = ? AND from_offset = ?";

  private static final String COUNT_PENDING = "SELECT count(*) FROM projection_dead_letters";

  private final DataSource dataSource;

  /**
   * Creates the store over {@code database}.
   *
   * @param database the database whose {@code projection_dead_letters} table the store keeps
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteProjectionDeadLetterStore(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    this.dataSource = database.dataSource();
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void save(ProjectionDeadLetterEntry entry) {
    if (entry == null) {
      throw new IllegalArgumentException("entry is required");
    }
    long failedAt = SqliteTimestamps.toMicros(entry.failedAt());
    write(
        "projection dead letter save",
        "Failed to save projection dead letter entry: "
            + LogSanitizer.sanitizeForLog(entry.projectionName().value()),
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            ps.setString(1, entry.projectionName().value());
            ps.setLong(2, entry.fromOffset().value());
            ps.setLong(3, entry.toOffset().value());
            ps.setInt(4, entry.batchSize());
            ps.setString(5, entry.errorType());
            ps.setString(6, entry.errorMessage());
            ps.setInt(7, entry.attempts());
            ps.setLong(8, failedAt);
            return ps.executeUpdate();
          }
        });
  }

  @Override
  public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName is required");
    }
    requirePositiveLimit(limit);
    return query(
        SELECT_BY_NAME,
        "Failed to read projection dead letters: "
            + LogSanitizer.sanitizeForLog(projectionName.value()),
        ps -> {
          ps.setString(1, projectionName.value());
          ps.setInt(2, limit);
        });
  }

  @Override
  public List<ProjectionDeadLetterEntry> readAll(int limit) {
    requirePositiveLimit(limit);
    return query(
        SELECT_ALL, "Failed to read all projection dead letters", ps -> ps.setInt(1, limit));
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName is required");
    }
    if (fromOffset == null) {
      throw new IllegalArgumentException("fromOffset is required");
    }
    write(
        "projection dead letter discard",
        "Failed to discard projection dead letter: "
            + LogSanitizer.sanitizeForLog(projectionName.value())
            + "/"
            + fromOffset.value(),
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(DELETE)) {
            ps.setString(1, projectionName.value());
            ps.setLong(2, fromOffset.value());
            return ps.executeUpdate();
          }
        });
  }

  /** {@inheritDoc} A {@code count(*)} over {@code projection_dead_letters}. */
  @Override
  public long countPending() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(COUNT_PENDING);
        ResultSet rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0L;
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException("Failed to count projection dead letter entries", e);
    }
  }

  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }

  private List<ProjectionDeadLetterEntry> query(String sql, String failureMessage, Binder binder) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      List<ProjectionDeadLetterEntry> entries = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          entries.add(fromRow(rs));
        }
      }
      return List.copyOf(entries);
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(failureMessage, e);
    }
  }

  /** Runs {@code work} as one write transaction, through {@link SqliteTransactions#write}. */
  private <T> T write(String transaction, String failureMessage, SqliteTransactions.Work<T> work) {
    return SqliteTransactions.write(dataSource, transaction, transaction, failureMessage, work);
  }

  private static void requirePositiveLimit(int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be > 0");
    }
  }

  private static ProjectionDeadLetterEntry fromRow(ResultSet rs) throws SQLException {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of(rs.getString("projection_name")),
        GlobalOffset.of(rs.getLong("from_offset")),
        GlobalOffset.of(rs.getLong("to_offset")),
        rs.getInt("batch_size"),
        rs.getString("error_type"),
        rs.getString("error_message"),
        rs.getInt("attempts"),
        SqliteTimestamps.fromMicros(rs.getLong("failed_at")));
  }
}
