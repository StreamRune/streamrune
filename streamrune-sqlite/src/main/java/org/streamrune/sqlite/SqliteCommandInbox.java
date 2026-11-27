package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import javax.sql.DataSource;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.jdbc.StreamKeys;

/**
 * The SQLite {@link CommandInbox}: the {@code command_inbox} table of a {@link SqliteDatabase},
 * with the semantics of {@code PostgresCommandInbox}. A row is written by {@link
 * SqliteEventStore}'s keyed append, on the append's connection and inside its write transaction
 * (see {@link SqliteEventStoreFactory#commandInbox}): the claim, the command's events and the
 * backfill of their offsets commit together or not at all.
 *
 * <p>The claim is an insert that does nothing when the key is recorded. Every write of the database
 * holds SQLite's write lock from its {@code BEGIN IMMEDIATE} to its commit, so a claim finds a key
 * any other append recorded committed, never one in flight: of two executions of one key, the one
 * that gets the lock first records it and the other finds the record.
 *
 * <p>{@code processed_at} is the application {@link Clock}'s instant at the claim, in epoch
 * microseconds; {@code global_offsets} is a JSON array of the events' offsets ({@code BIGINT[]} on
 * Postgres). {@link #deleteProcessedBefore} deletes in chunks, each in a write transaction of its
 * own, and refuses a connection in manual-commit mode with {@link IllegalStateException}, as every
 * SQLite store does.
 */
public final class SqliteCommandInbox implements CommandInbox {

  private static final String CLAIM =
      "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type, aggregate_id,"
          + " final_version, processed_at) VALUES (?, ?, ?, ?, ?, ?)"
          + " ON CONFLICT (idempotency_key) DO NOTHING";

  private static final String BACKFILL =
      "UPDATE command_inbox SET global_offsets = ? WHERE idempotency_key = ?";

  private static final String FIND =
      "SELECT command_type, aggregate_type, aggregate_id, final_version, global_offsets,"
          + " processed_at FROM command_inbox WHERE idempotency_key = ?";

  private static final String DELETE_BATCH =
      "DELETE FROM command_inbox WHERE idempotency_key IN ("
          + "  SELECT idempotency_key FROM command_inbox WHERE processed_at < ?"
          + "  ORDER BY processed_at LIMIT ?)";

  /** Rows deleted per chunk (and per write transaction) by {@link #deleteProcessedBefore}. */
  private static final int DELETE_BATCH_SIZE = 1000;

  /** Reads {@code global_offsets}; plain JSON numbers, no modules needed. */
  private static final ObjectMapper JSON = new ObjectMapper();

  private final DataSource dataSource;
  private final Path file;
  private final Clock clock;

  /**
   * An inbox over {@code database}, stamping {@code processed_at} with {@link Clock#systemUTC()}.
   *
   * @param database the database whose {@code command_inbox} table the inbox keeps (required)
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteCommandInbox(SqliteDatabase database) {
    this(database, Clock.systemUTC());
  }

  /**
   * An inbox over {@code database}, stamping {@code processed_at} with {@code clock}.
   *
   * @param database the database whose {@code command_inbox} table the inbox keeps (required)
   * @param clock the clock of {@code processed_at}; read inside the append's write transaction, so
   *     it must return at once (required)
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  public SqliteCommandInbox(SqliteDatabase database, Clock clock) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock is required");
    }
    this.dataSource = database.dataSource();
    this.file = database.file();
    this.clock = clock;
  }

  /** The database file the inbox keeps its rows in. */
  Path file() {
    return file;
  }

  /**
   * Claims {@code key} on the caller's connection, inside the caller's open write transaction:
   * neither begins, commits nor rolls back.
   *
   * @return {@code true} if the key was recorded now; {@code false} if it was recorded before
   * @throws SQLException if the write fails; the caller rolls its transaction back
   */
  boolean tryClaim(
      IdempotencyKey key,
      String commandType,
      StreamId streamId,
      Version finalVersion,
      Connection conn)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(CLAIM)) {
      ps.setString(1, key.value());
      ps.setString(2, commandType);
      StreamKeys.bind(ps, 3, streamId);
      ps.setLong(5, finalVersion.value());
      ps.setLong(6, SqliteTimestamps.toMicros(clock.instant()));
      return ps.executeUpdate() == 1;
    }
  }

  /**
   * Records the appended events' offsets in the row just claimed, on the caller's connection,
   * inside the caller's open write transaction.
   *
   * @throws SQLException if the write fails or finds no row of {@code key}; the caller rolls its
   *     transaction back
   */
  void backfillOffsets(IdempotencyKey key, List<GlobalOffset> offsets, Connection conn)
      throws SQLException {
    StringJoiner array = new StringJoiner(",", "[", "]");
    for (GlobalOffset offset : offsets) {
      array.add(Long.toString(offset.value()));
    }
    try (PreparedStatement ps = conn.prepareStatement(BACKFILL)) {
      ps.setString(1, array.toString());
      ps.setString(2, key.value());
      if (ps.executeUpdate() != 1) {
        // Only a claim of this transaction precedes the backfill; without its row the append would
        // commit a claim without its offsets, so it fails and rolls back instead.
        throw new SQLException(
            "No command inbox row to record the offsets of key "
                + LogSanitizer.sanitizeForLog(key.value())
                + " in: the backfill must follow the key's claim in the same transaction");
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws NullPointerException if {@code key} is {@code null}, as on Postgres
   */
  @Override
  public Optional<InboxResult> find(IdempotencyKey key) {
    Objects.requireNonNull(key, "key is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND)) {
      ps.setString(1, key.value());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        List<GlobalOffset> offsets = new ArrayList<>();
        for (long offset : JSON.readValue(rs.getString("global_offsets"), long[].class)) {
          offsets.add(GlobalOffset.of(offset));
        }
        return Optional.of(
            new InboxResult(
                key,
                rs.getString("command_type"),
                StreamKeys.read(rs),
                new Version(rs.getLong("final_version")),
                offsets,
                SqliteTimestamps.fromMicros(rs.getLong("processed_at"))));
      }
    } catch (Exception e) {
      // The key is caller-supplied and this message is logged and persisted: sanitized.
      throw new EventStoreException(
          "Failed to read command inbox for key: " + LogSanitizer.sanitizeForLog(key.value()), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Deletes in chunks of {@value #DELETE_BATCH_SIZE} rows, each in a write transaction of its
   * own, while a chunk deletes a row: no row claimed later is older than a past cutoff, so the set
   * shrinks with every chunk and the loop ends.
   *
   * @throws NullPointerException if {@code cutoff} is {@code null}, as on Postgres
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode
   * @throws EventStoreException if a chunk fails
   */
  @Override
  public int deleteProcessedBefore(Instant cutoff) {
    long cutoffMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(cutoff, "cutoff"));
    int total = 0;
    int batch;
    do {
      batch =
          SqliteTransactions.write(
              dataSource,
              "command inbox retention",
              "command inbox retention",
              "Failed to prune command inbox",
              conn -> {
                try (PreparedStatement ps = conn.prepareStatement(DELETE_BATCH)) {
                  ps.setLong(1, cutoffMicros);
                  ps.setInt(2, DELETE_BATCH_SIZE);
                  return ps.executeUpdate();
                }
              });
      total += batch;
    } while (batch > 0);
    return total;
  }
}
