package org.streamrune.postgres;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * PostgreSQL-backed {@link OffsetStore} using upsert semantics. Stores the last processed global
 * offset for each named projection in the {@code projection_offset} table.
 */
public final class PostgresOffsetStore implements OffsetStore {

  private static final String SELECT_OFFSET =
      "SELECT last_offset FROM projection_offset WHERE projection_name = ?";

  // Monotonic guard: a backward/sideways save updates 0 rows and is a silent no-op (no throw).
  // Regressions must not be possible even in a brief split-brain window; an intentional rewind
  // uses the rebuild/reset path, never saveOffset (see OffsetStore javadoc).
  private static final String UPSERT_OFFSET =
      "INSERT INTO projection_offset (projection_name, last_offset, updated_at) "
          + "VALUES (?, ?, NOW()) "
          + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = EXCLUDED.last_offset, "
          + "updated_at = NOW() "
          + "WHERE EXCLUDED.last_offset > projection_offset.last_offset";

  // Unguarded rewind for the reset/rebuild path. Deliberately omits the monotonic guard so an
  // intentional rewind to 0 is not rejected as a regression. Upserts so a not-yet-seen projection
  // name resets cleanly to 0 as well (a no-op semantically, but keeps the contract uniform).
  private static final String RESET_OFFSET =
      "INSERT INTO projection_offset (projection_name, last_offset, updated_at) "
          + "VALUES (?, 0, NOW()) "
          + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = 0, updated_at = NOW()";

  private final DataSource dataSource;

  public PostgresOffsetStore(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_OFFSET)) {
      ps.setString(1, projectionName.value());
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          return GlobalOffset.of(rs.getLong("last_offset"));
        }
        return GlobalOffset.initial();
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to read offset for projection: "
              + LogSanitizer.sanitizeForLog(projectionName.value()),
          e);
    }
  }

  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(UPSERT_OFFSET)) {
      ps.setString(1, projectionName.value());
      ps.setLong(2, offset.value());
      PostgresTransactions.commitIfManual(conn, "projection offset save", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to save offset for projection: "
              + LogSanitizer.sanitizeForLog(projectionName.value()),
          e);
    }
  }

  @Override
  public void reset(ProjectionName projectionName) {
    // Unguarded rewind (see RESET_OFFSET): saveOffset's monotonic guard would no-op the regression
    // to 0, silently defeating a projection reset — the bug this method exists to fix.
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(RESET_OFFSET)) {
      ps.setString(1, projectionName.value());
      PostgresTransactions.commitIfManual(conn, "projection offset reset", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to reset offset for projection: "
              + LogSanitizer.sanitizeForLog(projectionName.value()),
          e);
    }
  }
}
