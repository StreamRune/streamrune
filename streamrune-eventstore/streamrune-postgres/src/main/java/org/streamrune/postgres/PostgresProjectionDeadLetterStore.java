package org.streamrune.postgres;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
 * PostgreSQL-backed {@link ProjectionDeadLetterStore}. Stores failed projection batches in the
 * {@code projection_dead_letters} table (event-store baseline, {@code
 * V001__streamrune_baseline.sql}).
 */
public final class PostgresProjectionDeadLetterStore implements ProjectionDeadLetterStore {

  // The table's primary key is (projection_name, from_offset). A batch that fails again after a
  // replay attempt hits the same key — refresh the entry instead of violating the PK. attempts is
  // kept monotonic: at least one more than the stored value, or the caller's count if higher.
  private static final String UPSERT =
      "INSERT INTO projection_dead_letters "
          + "(projection_name, from_offset, to_offset, batch_size, error_type, error_message, attempts, failed_at) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
          + "ON CONFLICT (projection_name, from_offset) DO UPDATE SET "
          + "to_offset = EXCLUDED.to_offset, "
          + "batch_size = EXCLUDED.batch_size, "
          + "error_type = EXCLUDED.error_type, "
          + "error_message = EXCLUDED.error_message, "
          + "attempts = GREATEST(projection_dead_letters.attempts + 1, EXCLUDED.attempts), "
          + "failed_at = EXCLUDED.failed_at";

  private static final String SELECT_BY_NAME =
      "SELECT projection_name, from_offset, to_offset, batch_size, error_type, error_message, attempts, failed_at "
          + "FROM projection_dead_letters WHERE projection_name = ? "
          + "ORDER BY failed_at ASC, from_offset ASC LIMIT ?";

  private static final String SELECT_ALL =
      "SELECT projection_name, from_offset, to_offset, batch_size, error_type, error_message, attempts, failed_at "
          + "FROM projection_dead_letters ORDER BY failed_at ASC, from_offset ASC LIMIT ?";

  private static final String DELETE =
      "DELETE FROM projection_dead_letters WHERE projection_name = ? AND from_offset = ?";

  private final DataSource dataSource;

  public PostgresProjectionDeadLetterStore(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Repeat failures of the same batch (same projection name and from-offset) update the existing
   * row — refreshing the error, bumping {@code attempts}, and setting {@code failed_at} to the
   * latest failure — instead of violating the primary key.
   */
  @Override
  public void save(ProjectionDeadLetterEntry entry) {
    if (entry == null) throw new IllegalArgumentException("entry is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(UPSERT)) {
      ps.setString(1, entry.projectionName().value());
      ps.setLong(2, entry.fromOffset().value());
      ps.setLong(3, entry.toOffset().value());
      ps.setInt(4, entry.batchSize());
      ps.setString(5, entry.errorType());
      ps.setString(6, entry.errorMessage());
      ps.setInt(7, entry.attempts());
      ps.setTimestamp(8, Timestamp.from(entry.failedAt()));
      PostgresTransactions.commitIfManual(
          conn, "projection dead letter save", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to save projection dead letter entry: "
              + LogSanitizer.sanitizeForLog(entry.projectionName().value()),
          e);
    }
  }

  @Override
  public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_BY_NAME)) {
      ps.setString(1, projectionName.value());
      ps.setInt(2, limit);
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to read projection dead letters: "
              + LogSanitizer.sanitizeForLog(projectionName.value()),
          e);
    }
  }

  @Override
  public List<ProjectionDeadLetterEntry> readAll(int limit) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_ALL)) {
      ps.setInt(1, limit);
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read all projection dead letters", e);
    }
  }

  @Override
  public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
    if (projectionName == null) throw new IllegalArgumentException("projectionName is required");
    if (fromOffset == null) throw new IllegalArgumentException("fromOffset is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(DELETE)) {
      ps.setString(1, projectionName.value());
      ps.setLong(2, fromOffset.value());
      PostgresTransactions.commitIfManual(
          conn, "projection dead letter discard", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to discard projection dead letter: " + projectionName.value() + "/" + fromOffset,
          e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Fleet-wide {@code SELECT COUNT(*)} over {@code projection_dead_letters}, mirroring {@code
   * PostgresDeadLetterQueue.countPending()} on the command DLQ.
   */
  @Override
  public long countPending() {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT COUNT(*) FROM projection_dead_letters");
        var rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0L;
    } catch (SQLException e) {
      throw new EventStoreException("Failed to count projection dead letter entries", e);
    }
  }

  private List<ProjectionDeadLetterEntry> readEntries(java.sql.PreparedStatement ps)
      throws SQLException {
    var entries = new ArrayList<ProjectionDeadLetterEntry>();
    try (var rs = ps.executeQuery()) {
      while (rs.next()) {
        entries.add(fromRow(rs));
      }
    }
    return List.copyOf(entries);
  }

  private ProjectionDeadLetterEntry fromRow(ResultSet rs) throws SQLException {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of(rs.getString("projection_name")),
        GlobalOffset.of(rs.getLong("from_offset")),
        GlobalOffset.of(rs.getLong("to_offset")),
        rs.getInt("batch_size"),
        rs.getString("error_type"),
        rs.getString("error_message"),
        rs.getInt("attempts"),
        rs.getTimestamp("failed_at").toInstant());
  }
}
