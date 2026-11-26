package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Objects;
import java.util.OptionalLong;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * The checkpoint-row fence over one {@link CheckpointTable}: every read and write of a projection's
 * checkpoint row ({@code projection_name}, {@code last_offset}, {@code epoch}) that guards a commit
 * against split-brain double-apply and lost events. It is the SQLite port of the Postgres fence
 * over the same tables, with the same guards, outcomes and exceptions. {@link SqliteOffsetStore}
 * reads and writes {@code subscription_checkpoint} through it, and {@link
 * SqliteReadModelCheckpoints} and the read-model store's processor {@code readmodel_checkpoint}.
 *
 * <p><b>Three guards under the write lock.</b> A batch transaction calls {@link #lockForBatch}
 * first. Where Postgres locks the checkpoint row with {@code SELECT ... FOR UPDATE}, the caller's
 * {@code BEGIN IMMEDIATE} already holds SQLite's database-wide write lock, so every writer of the
 * file, and with it every writer of one projection, is serialized until the caller's transaction
 * ends. {@link #lockForBatch} seeds a {@code 0} row if none exists, reads it, and applies, in this
 * order:
 *
 * <ol>
 *   <li>the <b>epoch fence</b>: a caller epoch below the stored epoch is a superseded leader whose
 *       lease a newer one took over; rejected before the caller writes anything. Epoch {@code 0} is
 *       unfenced (single-node / {@link
 *       org.streamrune.core.subscription.SubscriptionLeadership#NOOP}) and always passes;
 *   <li>the <b>first-offset overlap guard</b>: a batch whose first offset is at or before the
 *       stored offset overlaps events another writer already applied (a stale leader that read an
 *       older checkpoint and fetched a range past a newer leader's advance); rejected, because the
 *       monotonic guard alone would let it commit and re-apply its prefix. An empty batch has no
 *       first offset and skips it;
 *   <li>the <b>monotonic offset guard</b>, in {@link #saveLocked} after the caller's writes: an
 *       advance that does not move the offset forward updates no row and is rejected. The advance
 *       stamps {@code max(stored, caller)} as the epoch under the same {@code WHERE}, so the stored
 *       epoch never regresses and a rejected advance stamps nothing.
 * </ol>
 *
 * Each rejection is a {@link ProjectionCommitFencedException} naming its {@link
 * ProjectionCommitFencedException.Guard}; the caller rolls its transaction back, so neither its
 * writes nor the checkpoint move. A {@link #lockForReplay} seeds the row and checks nothing, under
 * the same write lock, so a dead-letter replay and a live batch of one projection never interleave.
 *
 * <p>SQLite has no leadership: the runners always pass epoch {@code 0}, and the epoch fence never
 * fires. It is kept whole so that the fencing contract applies unchanged and the store stays
 * correct if an epoch ever reaches it.
 *
 * <p><b>Connection discipline.</b> The {@link Connection} methods ({@link #lockForBatch}, {@link
 * #saveLocked}, {@link #lockForReplay}) run inside the caller's open write transaction: a
 * connection in driver autocommit on which the caller ran {@code BEGIN IMMEDIATE}, as {@link
 * SqliteTransactions#inTransaction} does. Run outside one, their statements would commit one by one
 * and the guards would check a row another writer can change in between, so each of them first
 * checks that a transaction is open ({@link SqliteTransactions#requireOpenTransaction}) and refuses
 * a connection without one with {@link IllegalStateException}, before it writes anything. The check
 * cannot see {@link #lockForBatch} and {@link #saveLocked} split across two transactions: running
 * the lock, the caller's writes and the advance in one transaction is the caller's contract. A
 * deferred {@code BEGIN} is enough: the seed takes the write lock before the read. The methods
 * leave committing and rolling back to the caller, and their {@link SQLException}s propagate
 * unwrapped for the caller to report. The {@link DataSource} methods borrow one connection for one
 * operation; a write runs in its own {@code BEGIN IMMEDIATE} ... {@code COMMIT} through {@link
 * SqliteTransactions#write}, so it is durable when the method returns, refuses a connection with an
 * open transaction of the application's, and reports a failure as an {@link EventStoreException}
 * naming the operation and the (sanitized) projection; a connection in manual-commit mode is
 * refused with {@link IllegalStateException}.
 *
 * <p><b>Timestamps.</b> {@code updated_at} is the application {@link Clock}'s instant in epoch
 * microseconds ({@link SqliteTimestamps}), bound where Postgres calls {@code NOW()}; a write the
 * guards refuse stamps nothing.
 *
 * <p>The table name comes only from {@link CheckpointTable}; the statements are fixed when the
 * fence is built, and the projection name is always a bind parameter.
 */
final class SqliteCheckpointFence {

  // The statement templates; %1$s is the checkpoint table's name, and nothing else is spliced in.

  // Seeds the 0 row the batch reads and the monotonic guard measures against. Parameters: name,
  // updated_at.
  private static final String SEED =
      "INSERT INTO %1$s (projection_name, last_offset, updated_at)"
          + " VALUES (?, 0, ?) ON CONFLICT (projection_name) DO NOTHING";

  // The read under the caller's write lock; Postgres locks the row here with FOR UPDATE.
  private static final String READ_FOR_BATCH =
      "SELECT last_offset, epoch FROM %1$s WHERE projection_name = ?";

  // Monotonic guard: a backward or sideways save updates 0 rows. Intentional rewinds use the reset
  // path, never a save (see the OffsetStore javadoc). The advance also stamps the caller's fencing
  // epoch with max() so the stored epoch is monotonic and never regresses below a newer leader's
  // takeover; an unfenced epoch-0 caller leaves the stored epoch unchanged. The stamp rides the
  // SAME monotonic WHERE guard, so a rejected (0-row) advance stamps nothing. Parameters: name,
  // offset, epoch, updated_at.
  private static final String SAVE_LOCKED =
      "INSERT INTO %1$s (projection_name, last_offset, epoch, updated_at)"
          + " VALUES (?, ?, ?, ?)"
          + " ON CONFLICT (projection_name) DO UPDATE SET last_offset = excluded.last_offset,"
          + " epoch = max(%1$s.epoch, excluded.epoch),"
          + " updated_at = excluded.updated_at"
          + " WHERE excluded.last_offset > %1$s.last_offset";

  // The takeover stamp: seeds last_offset 0 for a projection that never committed and never
  // touches last_offset on conflict. The WHERE makes a same-epoch re-stamp a 0-row no-op and never
  // lets the epoch regress. Parameters: name, epoch, updated_at.
  private static final String STAMP =
      "INSERT INTO %1$s (projection_name, last_offset, epoch, updated_at)"
          + " VALUES (?, 0, ?, ?)"
          + " ON CONFLICT (projection_name) DO UPDATE SET"
          + " epoch = max(%1$s.epoch, excluded.epoch),"
          + " updated_at = excluded.updated_at"
          + " WHERE excluded.epoch > %1$s.epoch";

  private static final String SELECT_EPOCH = "SELECT epoch FROM %1$s WHERE projection_name = ?";

  // Unguarded rewind for the reset/rebuild path. Deliberately omits the monotonic guard so an
  // intentional rewind to 0 is not rejected as a regression. Upserts so a not-yet-seen projection
  // name resets cleanly to 0 as well. Parameters: name, updated_at.
  private static final String RESET =
      "INSERT INTO %1$s (projection_name, last_offset, updated_at)"
          + " VALUES (?, 0, ?)"
          + " ON CONFLICT (projection_name) DO UPDATE SET last_offset = 0,"
          + " updated_at = excluded.updated_at";

  private static final String SELECT_OFFSET =
      "SELECT last_offset FROM %1$s WHERE projection_name = ?";

  // The OffsetStore.saveOffset path: the same monotonic guard as SAVE_LOCKED, without the epoch
  // stamp. A backward or sideways save is a silent no-op (no throw): there is no transaction of the
  // caller's to roll back. Parameters: name, offset, updated_at.
  private static final String SAVE_UNLOCKED =
      "INSERT INTO %1$s (projection_name, last_offset, updated_at)"
          + " VALUES (?, ?, ?)"
          + " ON CONFLICT (projection_name) DO UPDATE SET last_offset = excluded.last_offset,"
          + " updated_at = excluded.updated_at"
          + " WHERE excluded.last_offset > %1$s.last_offset";

  // The statements over this fence's table (package-private so a test can pin them).
  final String seedSql;
  final String readForBatchSql;
  final String saveLockedSql;
  final String stampSql;
  final String epochSql;
  final String resetSql;
  final String lastOffsetSql;
  final String saveUnlockedSql;

  private final CheckpointTable table;
  private final Clock clock;

  /**
   * A fence over {@code table} that stamps {@code updated_at} with {@code clock}.
   *
   * @param table the checkpoint table
   * @param clock the application clock
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  SqliteCheckpointFence(CheckpointTable table, Clock clock) {
    if (table == null) {
      throw new IllegalArgumentException("table is required");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock is required");
    }
    this.table = table;
    this.clock = clock;
    String name = table.tableName();
    this.seedSql = SEED.formatted(name);
    this.readForBatchSql = READ_FOR_BATCH.formatted(name);
    this.saveLockedSql = SAVE_LOCKED.formatted(name);
    this.stampSql = STAMP.formatted(name);
    this.epochSql = SELECT_EPOCH.formatted(name);
    this.resetSql = RESET.formatted(name);
    this.lastOffsetSql = SELECT_OFFSET.formatted(name);
    this.saveUnlockedSql = SAVE_UNLOCKED.formatted(name);
  }

  /** The table this fence guards. */
  CheckpointTable table() {
    return table;
  }

  /** The clock that stamps {@code updated_at}. */
  Clock clock() {
    return clock;
  }

  /**
   * Seeds and reads the projection's checkpoint row for a batch and applies the epoch fence and the
   * first-offset overlap guard to it, in that order, before the caller writes anything. The seed
   * gives even a first-ever batch a row; it is part of the caller's transaction and rolls back with
   * it. The caller's {@code BEGIN IMMEDIATE} holds the write lock until its transaction ends, so no
   * other writer changes the row between this read and the caller's {@link #saveLocked}.
   *
   * @param c the caller's open write transaction (see the class Javadoc)
   * @param name the projection
   * @param fencingEpoch the caller's leadership epoch; below the stored epoch is rejected, {@code
   *     0} is unfenced and always passes
   * @param firstOffsetOfBatch the global offset of the batch's first event, at or before the stored
   *     offset is rejected; {@code null} for an empty batch, which skips the overlap guard
   * @return the row's {@code last_offset}
   * @throws ProjectionCommitFencedException with {@link
   *     ProjectionCommitFencedException.Guard#EPOCH_FENCE} on a superseded epoch, or {@link
   *     ProjectionCommitFencedException.Guard#OVERLAP} on an overlapping batch
   * @throws IllegalStateException if {@code c} has no transaction open (nothing is written)
   * @throws SQLException if a statement fails
   */
  long lockForBatch(
      Connection c, ProjectionName name, long fencingEpoch, GlobalOffset firstOffsetOfBatch)
      throws SQLException {
    SqliteTransactions.requireOpenTransaction(c);
    Checkpoint checkpoint = seedAndRead(c, name);
    rejectStaleEpoch(name, fencingEpoch, checkpoint.epoch());
    rejectOverlappingBatch(name, firstOffsetOfBatch, checkpoint.offset());
    return checkpoint.offset();
  }

  /**
   * Seeds the projection's checkpoint row for a dead-letter replay if none exists, with no check,
   * and writes nothing beyond the seed: what {@link #lockForBatch} does before its guards. The
   * caller's write lock keeps a live batch of the same projection out until the replay's
   * transaction ends, and keeps the replay out while a batch is open.
   *
   * @param c the caller's open write transaction (see the class Javadoc)
   * @param name the projection
   * @throws IllegalStateException if {@code c} has no transaction open (nothing is written)
   * @throws SQLException if a statement fails
   */
  void lockForReplay(Connection c, ProjectionName name) throws SQLException {
    SqliteTransactions.requireOpenTransaction(c);
    seedAndRead(c, name);
  }

  /**
   * Advances the checkpoint row to {@code newOffset} and stamps {@code max(stored, fencingEpoch)}
   * as its epoch, under the monotonic guard.
   *
   * <p>The caller's writes and this advance must be all-or-nothing. A {@code 0}-row result means
   * the stored offset is already at or past {@code newOffset} (a fully-behind redelivery or an
   * offset regression), so committing would apply the batch a second time; this throws and the
   * caller rolls back. An overlapping batch whose endpoint still advances passes this guard: it is
   * rejected earlier by the overlap guard of {@link #lockForBatch}, under the same lock. In
   * single-leader forward progress the guard always updates exactly one row and never fires.
   *
   * @param c the caller's open write transaction, in which {@link #lockForBatch} ran
   * @param name the projection
   * @param newOffset the offset the batch advances to
   * @param fencingEpoch the caller's leadership epoch, stamped with {@code max}
   * @throws ProjectionCommitFencedException with {@link
   *     ProjectionCommitFencedException.Guard#MONOTONIC} if the advance does not move the offset
   *     forward
   * @throws IllegalStateException if {@code c} has no transaction open (nothing is written)
   * @throws SQLException if the statement fails
   */
  void saveLocked(Connection c, ProjectionName name, GlobalOffset newOffset, long fencingEpoch)
      throws SQLException {
    SqliteTransactions.requireOpenTransaction(c);
    try (PreparedStatement ps = c.prepareStatement(saveLockedSql)) {
      ps.setString(1, name.value());
      ps.setLong(2, newOffset.value());
      ps.setLong(3, fencingEpoch);
      ps.setLong(4, now());
      int updated = ps.executeUpdate();
      if (updated == 0) {
        throw new ProjectionCommitFencedException(
            ProjectionCommitFencedException.Guard.MONOTONIC,
            "Projection '"
                + shown(name)
                + "' offset advance to "
                + newOffset.value()
                + " was rejected by the monotonic guard (stored offset already at/past it): the"
                + " batch is already applied. Rolling back so the read-model write is not committed"
                + " a second time.");
      }
    }
  }

  /**
   * Stamps {@code fencingEpoch} on the projection's checkpoint row without moving the offset: the
   * takeover stamp a new leader commits before it reads anything, so every transaction the old
   * leader starts afterwards fails the epoch fence. Seeds the row at offset {@code 0} if absent.
   * Epoch {@code 0} (unfenced) is a no-op: there is nothing to fence and stamping would only churn
   * the row.
   *
   * <p>Idempotent and monotonic: a same-epoch re-stamp updates no row, and the stored epoch never
   * regresses. A {@code 0}-row outcome reads the stored epoch in the same transaction and throws
   * when it is strictly greater: the caller's epoch is not authoritative, and running under it
   * would have every commit fenced out. An equal stored epoch (the ordinary re-stamp) returns
   * quietly.
   *
   * @param ds the data source to borrow the connection from
   * @param name the projection
   * @param fencingEpoch the newly acquired leadership epoch; {@code 0} is a no-op
   * @throws NullPointerException if {@code name} is {@code null}, before any I/O
   * @throws ProjectionEpochRegressionException if the row already stores a greater epoch
   * @throws EventStoreException if the connection or a statement fails, including when SQLite's
   *     write lock is not granted within {@code busy_timeout} or the connection has a transaction
   *     open
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  void stampEpoch(DataSource ds, ProjectionName name, long fencingEpoch) {
    requireName(name);
    if (fencingEpoch == 0L) {
      return;
    }
    // Empty when the stamp was written; otherwise the epoch the row already stores.
    OptionalLong unstamped =
        SqliteTransactions.write(
            ds,
            "projection fencing stamp",
            "fencing stamp of projection " + shown(name),
            "Failed to stamp fencing epoch " + fencingEpoch + " for projection: " + shown(name),
            c -> {
              int stamped;
              try (PreparedStatement ps = c.prepareStatement(stampSql)) {
                ps.setString(1, name.value());
                ps.setLong(2, fencingEpoch);
                ps.setLong(3, now());
                stamped = ps.executeUpdate();
              }
              return stamped == 0
                  ? OptionalLong.of(readStoredEpoch(c, name))
                  : OptionalLong.empty();
            });
    if (unstamped.isPresent() && unstamped.getAsLong() > fencingEpoch) {
      throw new ProjectionEpochRegressionException(name, fencingEpoch, unstamped.getAsLong());
    }
  }

  /**
   * The epoch stored on the projection's checkpoint row, or {@code 0} when there is no row.
   *
   * @param ds the data source to borrow the connection from
   * @param name the projection
   * @return the stored epoch
   * @throws NullPointerException if {@code name} is {@code null}, before any I/O
   * @throws EventStoreException if the connection or the statement fails
   */
  long readEpoch(DataSource ds, ProjectionName name) {
    requireName(name);
    try (Connection conn = ds.getConnection()) {
      return readStoredEpoch(conn, name);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to read fencing epoch for projection: " + shown(name), e);
    }
  }

  /**
   * The projection's stored {@code last_offset}, or {@link GlobalOffset#initial()} when there is no
   * row.
   *
   * @param ds the data source to borrow the connection from
   * @param name the projection
   * @return the stored offset
   * @throws NullPointerException if {@code name} is {@code null}, before any I/O
   * @throws EventStoreException if the connection or the statement fails
   */
  GlobalOffset lastOffset(DataSource ds, ProjectionName name) {
    requireName(name);
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(lastOffsetSql)) {
      ps.setString(1, name.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? GlobalOffset.of(rs.getLong(1)) : GlobalOffset.initial();
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read offset for projection: " + shown(name), e);
    }
  }

  /**
   * Rewinds the projection's checkpoint to offset {@code 0}, bypassing the monotonic guard: the
   * reset/rebuild path, which a save cannot express. Seeds the row at {@code 0} if absent; the
   * epoch is left as it is.
   *
   * @param ds the data source to borrow the connection from
   * @param name the projection
   * @throws NullPointerException if {@code name} is {@code null}, before any I/O
   * @throws EventStoreException if the connection or the statement fails, as for {@link
   *     #stampEpoch}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  void reset(DataSource ds, ProjectionName name) {
    requireName(name);
    write(
        ds,
        name,
        "reset",
        c -> {
          try (PreparedStatement ps = c.prepareStatement(resetSql)) {
            ps.setString(1, name.value());
            ps.setLong(2, now());
            return ps.executeUpdate();
          }
        });
  }

  /**
   * Saves {@code offset} outside any batch transaction (the {@code OffsetStore.saveOffset} path)
   * under the monotonic guard: a save at or below the stored offset is a silent no-op. Leaves the
   * epoch as it is.
   *
   * @param ds the data source to borrow the connection from
   * @param name the projection
   * @param offset the offset to save
   * @throws NullPointerException if an argument is {@code null}, before any I/O
   * @throws EventStoreException if the connection or the statement fails, as for {@link
   *     #stampEpoch}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  void saveUnlocked(DataSource ds, ProjectionName name, GlobalOffset offset) {
    requireName(name);
    Objects.requireNonNull(offset, "offset is required");
    write(
        ds,
        name,
        "save",
        c -> {
          try (PreparedStatement ps = c.prepareStatement(saveUnlockedSql)) {
            ps.setString(1, name.value());
            ps.setLong(2, offset.value());
            ps.setLong(3, now());
            return ps.executeUpdate();
          }
        });
  }

  /** One standalone write of an offset: {@code what} is "save" or "reset". */
  private static void write(
      DataSource ds, ProjectionName name, String what, SqliteTransactions.Work<Integer> work) {
    SqliteTransactions.write(
        ds,
        "projection offset " + what,
        "offset " + what + " of projection " + shown(name),
        "Failed to " + what + " offset for projection: " + shown(name),
        work);
  }

  /** The committed checkpoint of a projection: its last applied offset and stamped epoch. */
  private record Checkpoint(long offset, long epoch) {}

  /**
   * Seeds a {@code 0} checkpoint if none exists, then reads the row's stored offset and epoch, on
   * the caller's write transaction.
   */
  private Checkpoint seedAndRead(Connection conn, ProjectionName name) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(seedSql)) {
      ps.setString(1, name.value());
      ps.setLong(2, now());
      ps.executeUpdate();
    }
    try (PreparedStatement ps = conn.prepareStatement(readForBatchSql)) {
      ps.setString(1, name.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? new Checkpoint(rs.getLong(1), rs.getLong(2)) : new Checkpoint(0L, 0L);
      }
    }
  }

  /**
   * Fences out a commit whose caller epoch is below the epoch stored on the checkpoint row: a
   * superseded leader whose lease a newer one took over (bumping the epoch). A {@code fencingEpoch}
   * of {@code 0} is unfenced and always passes, so a single-node deployment commits unfenced.
   */
  private static void rejectStaleEpoch(ProjectionName name, long fencingEpoch, long storedEpoch) {
    if (fencingEpoch != 0L && fencingEpoch < storedEpoch) {
      throw new ProjectionCommitFencedException(
          ProjectionCommitFencedException.Guard.EPOCH_FENCE,
          "Projection '"
              + shown(name)
              + "' commit fenced out: caller epoch "
              + fencingEpoch
              + " is below the stored epoch "
              + storedEpoch
              + " — a newer leader has taken over the lease. Rolling back the stale leader's write"
              + " (epoch fencing).");
    }
  }

  /**
   * Rejects a batch whose first event offset is at or before the committed checkpoint: it overlaps
   * events already applied by another writer (a concurrent or failed-over stale leader, or a
   * redelivery). A forward batch always starts strictly after the checkpoint (the runners read the
   * global stream from the stored offset); an overlapping start means part of the batch would be
   * applied a second time. An empty batch ({@code null} first offset) is governed solely by the
   * monotonic guard on the advance.
   */
  private static void rejectOverlappingBatch(
      ProjectionName name, GlobalOffset firstOffsetOfBatch, long storedOffset) {
    if (firstOffsetOfBatch == null) {
      return;
    }
    long firstOffset = firstOffsetOfBatch.value();
    if (firstOffset <= storedOffset) {
      throw new ProjectionCommitFencedException(
          ProjectionCommitFencedException.Guard.OVERLAP,
          "Projection '"
              + shown(name)
              + "' batch starting at offset "
              + firstOffset
              + " overlaps the committed checkpoint ("
              + storedOffset
              + "): those events were already applied by another writer (a concurrent or"
              + " failed-over stale leader, or a redelivery). Rolling back so the read-model write is"
              + " not applied a second time.");
    }
  }

  /** The epoch stored on the row, or {@code 0} when there is none, on {@code conn}. */
  private long readStoredEpoch(Connection conn, ProjectionName name) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(epochSql)) {
      ps.setString(1, name.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  private long now() {
    return SqliteTimestamps.toMicros(clock.instant());
  }

  private static void requireName(ProjectionName name) {
    Objects.requireNonNull(name, "projectionName is required");
  }

  /** A projection name rendered for an exception message: caller text, so sanitized. */
  private static String shown(ProjectionName name) {
    return LogSanitizer.sanitizeForLog(name.value());
  }
}
