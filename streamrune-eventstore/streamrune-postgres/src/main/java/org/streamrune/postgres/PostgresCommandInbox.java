package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * PostgreSQL {@link CommandInbox}. The {@code Connection}-taking methods are called by {@link
 * PostgresEventStore#appendWithKey} inside the append transaction (same pattern as the outbox).
 */
public final class PostgresCommandInbox implements CommandInbox {

  private static final String CLAIM =
      "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type, aggregate_id,"
          + " final_version) VALUES (?, ?, ?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING";
  private static final String BACKFILL =
      "UPDATE command_inbox SET global_offsets = ? WHERE idempotency_key = ?";
  private static final String FIND =
      "SELECT command_type, aggregate_type, aggregate_id, final_version, global_offsets,"
          + " processed_at "
          + "FROM command_inbox WHERE idempotency_key = ?";
  private static final String DELETE_BATCH =
      "DELETE FROM command_inbox WHERE idempotency_key IN ("
          + "  SELECT idempotency_key FROM command_inbox WHERE processed_at < ? "
          + "  ORDER BY processed_at LIMIT ?)";

  /** Rows deleted per chunk by {@link #deleteProcessedBefore}. */
  private static final int DELETE_BATCH_SIZE = 1000;

  private final DataSource dataSource;

  public PostgresCommandInbox(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  /**
   * Claims the key in the caller's transaction. Returns true if newly inserted, false on conflict.
   */
  public boolean tryClaim(
      IdempotencyKey key,
      String commandType,
      StreamId streamId,
      Version finalVersion,
      Connection conn)
      throws java.sql.SQLException {
    try (PreparedStatement ps = conn.prepareStatement(CLAIM)) {
      ps.setString(1, key.value());
      ps.setString(2, commandType);
      PostgresStreamKeys.bind(ps, 3, streamId);
      ps.setLong(5, finalVersion.value());
      return ps.executeUpdate() == 1;
    }
  }

  /**
   * Backfills the appended events' offsets into a freshly claimed row, in the caller's transaction.
   */
  public void backfillOffsets(IdempotencyKey key, List<GlobalOffset> offsets, Connection conn)
      throws java.sql.SQLException {
    Long[] arr = offsets.stream().map(GlobalOffset::value).toArray(Long[]::new);
    try (PreparedStatement ps = conn.prepareStatement(BACKFILL)) {
      ps.setArray(1, conn.createArrayOf("bigint", arr));
      ps.setString(2, key.value());
      ps.executeUpdate();
    }
  }

  @Override
  public Optional<InboxResult> find(IdempotencyKey key) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND)) {
      ps.setString(1, key.value());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return Optional.empty();
        List<GlobalOffset> offsets = new ArrayList<>();
        java.sql.Array sqlArr = rs.getArray("global_offsets");
        if (sqlArr != null) {
          for (Long v : (Long[]) sqlArr.getArray()) offsets.add(GlobalOffset.of(v));
        }
        return Optional.of(
            new InboxResult(
                key,
                rs.getString("command_type"),
                PostgresStreamKeys.read(rs),
                new Version(rs.getLong("final_version")),
                offsets,
                rs.getTimestamp("processed_at").toInstant()));
      }
    } catch (java.sql.SQLException e) {
      // The key is caller-supplied and this message is logged and persisted — sanitized.
      throw new EventStoreException(
          "Failed to read command inbox for key: " + LogSanitizer.sanitizeForLog(key.value()), e);
    }
  }

  @Override
  public int deleteProcessedBefore(Instant cutoff) {
    int total = 0;
    int batch;
    do {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps = conn.prepareStatement(DELETE_BATCH)) {
        ps.setObject(1, Timestamp.from(cutoff));
        ps.setInt(2, DELETE_BATCH_SIZE);
        batch =
            PostgresTransactions.commitIfManual(
                conn, "command inbox retention", c -> ps.executeUpdate());
        total += batch;
      } catch (java.sql.SQLException e) {
        throw new EventStoreException("Failed to prune command inbox", e);
      }
      // Loop while ANY row was deleted, not only on a full DELETE_BATCH_SIZE chunk (the same
      // termination as PostgresOutboxStore.deleteDelivered/deleteSkipped). A chunk deletes fewer
      // than LIMIT although older aged rows remain whenever a concurrent writer removes one of its
      // snapshot candidates inside the DELETE window — EvalPlanQual skips the gone row — and every
      // instance runs its own retention sweeper with no cluster lock, so a second sweeper is the
      // ordinary source. A `batch == DELETE_BATCH_SIZE` termination then exited with those rows
      // left behind. `batch > 0` terminates: processed_at is stamped NOW() at claim, so no new row
      // is older than the past cutoff, and each non-empty chunk strictly shrinks that finite set.
    } while (batch > 0);
    return total;
  }
}
