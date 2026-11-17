package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.SQLException;
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
 * against split-brain double-apply and lost events.
 *
 * <p><b>Three guards under one row lock.</b> A batch transaction calls {@link #lockForBatch} first:
 * it seeds a {@code 0} row if none exists and {@code SELECT ... FOR UPDATE}-locks it, so every
 * writer of one projection serializes on that row, then applies, in this order:
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
 *       stamps {@code GREATEST(stored, caller)} as the epoch under the same {@code WHERE}, so the
 *       stored epoch never regresses and a rejected advance stamps nothing.
 * </ol>
 *
 * Each rejection is a {@link ProjectionCommitFencedException} naming its {@link
 * ProjectionCommitFencedException.Guard}; the caller rolls its transaction back, so neither its
 * writes nor the checkpoint move. A {@link #lockForReplay} takes the same lock and checks nothing,
 * so a dead-letter replay and a live batch of one projection never interleave.
 *
 * <p><b>Connection discipline.</b> The {@link Connection} methods run on the caller's open
 * transaction (autocommit off) and leave committing, rolling back and restoring autocommit to the
 * caller; their {@link SQLException}s propagate unwrapped for the caller to report. The {@link
 * DataSource} methods borrow one pooled connection for one operation, make a write durable whatever
 * autocommit mode the pool hands out ({@link PostgresTransactions#commitIfManual}), and report a
 * failure as an {@link EventStoreException} naming the operation and the (sanitized) projection.
 *
 * <p>The table name comes only from {@link CheckpointTable}; the statements are fixed when the
 * fence is built, and the projection name is always a bind parameter.
 */
final class PostgresCheckpointFence {

  // The statement templates; %1$s is the checkpoint table's name, and nothing else is spliced in.

  private static final String SEED =
      "INSERT INTO %1$s (projection_name, last_offset, updated_at) "
          + "VALUES (?, 0, NOW()) ON CONFLICT (projection_name) DO NOTHING";

  private static final String LOCK =
      "SELECT last_offset, epoch FROM %1$s WHERE projection_name = ? FOR UPDATE";

  // Monotonic guard: a backward/sideways save updates 0 rows. Intentional rewinds use the reset
  // path, never a save (see the OffsetStore javadoc). The advance also stamps the caller's fencing
  // epoch with GREATEST so the stored epoch is monotonic and never regresses below a newer
  // leader's takeover; an unfenced epoch-0 caller leaves the stored epoch unchanged. The stamp
  // rides the SAME monotonic WHERE guard, so a rejected (0-row) advance stamps nothing.
  private static final String SAVE_LOCKED =
      "INSERT INTO %1$s (projection_name, last_offset, epoch, updated_at) "
          + "VALUES (?, ?, ?, NOW()) "
          + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = EXCLUDED.last_offset, "
          + "epoch = GREATEST(%1$s.epoch, EXCLUDED.epoch), "
          + "updated_at = NOW() "
          + "WHERE EXCLUDED.last_offset > %1$s.last_offset";

  // The takeover stamp: seeds last_offset 0 for a projection that never committed (the floor the
  // seed and the monotonic guard measure against) and never touches last_offset on conflict. The
  // WHERE makes a same-epoch re-stamp a 0-row no-op and never lets the epoch regress.
  private static final String STAMP =
      "INSERT INTO %1$s (projection_name, last_offset, epoch, updated_at) "
          + "VALUES (?, 0, ?, NOW()) "
          + "ON CONFLICT (projection_name) DO UPDATE SET "
          + "epoch = GREATEST(%1$s.epoch, EXCLUDED.epoch), updated_at = NOW() "
          + "WHERE EXCLUDED.epoch > %1$s.epoch";

  private static final String SELECT_EPOCH = "SELECT epoch FROM %1$s WHERE projection_name = ?";

  // Unguarded rewind for the reset/rebuild path. Deliberately omits the monotonic guard so an
  // intentional rewind to 0 is not rejected as a regression. Upserts so a not-yet-seen projection
  // name resets cleanly to 0 as well (a no-op semantically, but keeps the contract uniform).
  private static final String RESET =
      "INSERT INTO %1$s (projection_name, last_offset, updated_at) "
          + "VALUES (?, 0, NOW()) "
          + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = 0, updated_at = NOW()";

  private static final String SELECT_OFFSET =
      "SELECT last_offset FROM %1$s WHERE projection_name = ?";

  // The OffsetStore.saveOffset path: the same monotonic guard as SAVE_LOCKED, without the epoch
  // stamp. A backward/sideways save is a silent no-op (no throw): there is no transaction of the
  // caller's to roll back.
  private static final String SAVE_UNLOCKED =
      "INSERT INTO %1$s (projection_name, last_offset, updated_at) "
          + "VALUES (?, ?, NOW()) "
          + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = EXCLUDED.last_offset, "
          + "updated_at = NOW() "
          + "WHERE EXCLUDED.last_offset > %1$s.last_offset";

  // The statements over this fence's table (package-private so a test can pin them).
  final String seedSql;
  final String lockSql;
  final String saveLockedSql;
  final String stampSql;
  final String epochSql;
  final String resetSql;
  final String lastOffsetSql;
  final String saveUnlockedSql;

  /**
   * A fence over {@code table}.
   *
   * @throws IllegalArgumentException if {@code table} is {@code null}
   */
  PostgresCheckpointFence(CheckpointTable table) {
    if (table == null) {
      throw new IllegalArgumentException("table is required");
    }
    String name = table.tableName();
    this.seedSql = SEED.formatted(name);
    this.lockSql = LOCK.formatted(name);
    this.saveLockedSql = SAVE_LOCKED.formatted(name);
    this.stampSql = STAMP.formatted(name);
    this.epochSql = SELECT_EPOCH.formatted(name);
    this.resetSql = RESET.formatted(name);
    this.lastOffsetSql = SELECT_OFFSET.formatted(name);
    this.saveUnlockedSql = SAVE_UNLOCKED.formatted(name);
  }

  /**
   * Locks the projection's checkpoint row for a batch and applies the epoch fence and the
   * first-offset overlap guard to it, in that order, before the caller writes anything. Seeds a
   * {@code 0} row first if none exists, so even a first-ever batch has a row to lock and two
   * concurrent writers serialize on it rather than both proceeding past a missing row. The lock is
   * held until the caller's transaction ends.
   *
   * @param c the caller's open transaction (autocommit off)
   * @param name the projection
   * @param fencingEpoch the caller's leadership epoch; below the stored epoch is rejected, {@code
   *     0} is unfenced and always passes
   * @param firstOffsetOfBatch the global offset of the batch's first event, at or before the stored
   *     offset is rejected; {@code null} for an empty batch, which skips the overlap guard
   * @return the locked row's {@code last_offset}
   * @throws ProjectionCommitFencedException with {@link
   *     ProjectionCommitFencedException.Guard#EPOCH_FENCE} on a superseded epoch, or {@link
   *     ProjectionCommitFencedException.Guard#OVERLAP} on an overlapping batch
   * @throws SQLException if a statement fails
   */
  long lockForBatch(
      Connection c, ProjectionName name, long fencingEpoch, GlobalOffset firstOffsetOfBatch)
      throws SQLException {
    Checkpoint checkpoint = lock(c, name);
    rejectStaleEpoch(name, fencingEpoch, checkpoint.epoch());
    rejectOverlappingBatch(name, firstOffsetOfBatch, checkpoint.offset());
    return checkpoint.offset();
  }

  /**
   * Locks the projection's checkpoint row for a dead-letter replay, seeding a {@code 0} row if none
   * exists: the lock {@link #lockForBatch} takes, with no check, and the row is not written beyond
   * the seed. A live batch of the same projection waits for the replay's transaction, and the
   * replay waits for an open batch.
   *
   * @param c the caller's open transaction (autocommit off)
   * @param name the projection
   * @throws SQLException if a statement fails
   */
  void lockForReplay(Connection c, ProjectionName name) throws SQLException {
    lock(c, name);
  }

  /**
   * Advances the locked checkpoint row to {@code newOffset} and stamps {@code GREATEST(stored,
   * fencingEpoch)} as its epoch, under the monotonic guard.
   *
   * <p>The caller's writes and this advance must be all-or-nothing. A {@code 0}-row result means
   * the stored offset is already at or past {@code newOffset} (a fully-behind redelivery or an
   * offset regression), so committing would apply the batch a second time; this throws and the
   * caller rolls back. An overlapping batch whose endpoint still advances passes this guard — it is
   * rejected earlier by the overlap guard of {@link #lockForBatch}, under the same lock. In
   * single-leader forward progress the guard always updates exactly one row and never fires.
   *
   * @param c the caller's open transaction, which holds the row lock from {@link #lockForBatch}
   * @param name the projection
   * @param newOffset the offset the batch advances to
   * @param fencingEpoch the caller's leadership epoch, stamped with {@code GREATEST}
   * @throws ProjectionCommitFencedException with {@link
   *     ProjectionCommitFencedException.Guard#MONOTONIC} if the advance does not move the offset
   *     forward
   * @throws SQLException if the statement fails
   */
  void saveLocked(Connection c, ProjectionName name, GlobalOffset newOffset, long fencingEpoch)
      throws SQLException {
    try (var ps = c.prepareStatement(saveLockedSql)) {
      ps.setString(1, name.value());
      ps.setLong(2, newOffset.value());
      ps.setLong(3, fencingEpoch);
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
   * Stamps {@code fencingEpoch} on the projection's checkpoint row without moving the offset — the
   * takeover stamp a new leader commits before it reads anything, so every transaction the old
   * leader starts afterwards fails the epoch fence. Seeds the row at offset {@code 0} if absent.
   * Epoch {@code 0} (unfenced) is a no-op: there is nothing to fence and stamping would only churn
   * the row.
   *
   * <p>Idempotent and monotonic: a same-epoch re-stamp updates no row, and the stored epoch never
   * regresses. A {@code 0}-row outcome re-reads the stored epoch on the same connection and throws
   * when it is strictly greater: the caller's epoch is not authoritative, and running under it
   * would have every commit fenced out. An equal stored epoch (the ordinary re-stamp) and a row
   * deleted between the two statements return quietly.
   *
   * <p>One statement on its own pooled connection, committed whatever autocommit mode the pool
   * hands out. A concurrent batch holding the row lock makes it wait until that batch resolves.
   *
   * @param ds the pool to borrow the connection from
   * @param name the projection
   * @param fencingEpoch the newly acquired leadership epoch; {@code 0} is a no-op
   * @throws ProjectionEpochRegressionException if the row already stores a greater epoch
   * @throws EventStoreException if the connection or a statement fails
   */
  void stampEpoch(DataSource ds, ProjectionName name, long fencingEpoch) {
    if (fencingEpoch == 0L) {
      return;
    }
    try (var conn = ds.getConnection()) {
      int stamped =
          PostgresTransactions.commitIfManual(
              conn,
              "projection fencing stamp",
              c -> {
                try (var ps = c.prepareStatement(stampSql)) {
                  ps.setString(1, name.value());
                  ps.setLong(2, fencingEpoch);
                  return ps.executeUpdate();
                }
              });
      if (stamped == 0) {
        long stored = readStoredEpoch(conn, name);
        if (stored > fencingEpoch) {
          throw new ProjectionEpochRegressionException(name, fencingEpoch, stored);
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to stamp fencing epoch " + fencingEpoch + " for projection: " + shown(name), e);
    }
  }

  /**
   * The epoch stored on the projection's checkpoint row, or {@code 0} when there is no row.
   *
   * @param ds the pool to borrow the connection from
   * @param name the projection
   * @throws EventStoreException if the connection or the statement fails
   */
  long readEpoch(DataSource ds, ProjectionName name) {
    try (var conn = ds.getConnection()) {
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
   * @param ds the pool to borrow the connection from
   * @param name the projection
   * @throws EventStoreException if the connection or the statement fails
   */
  GlobalOffset lastOffset(DataSource ds, ProjectionName name) {
    try (var conn = ds.getConnection();
        var ps = conn.prepareStatement(lastOffsetSql)) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          return GlobalOffset.of(rs.getLong("last_offset"));
        }
        return GlobalOffset.initial();
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read offset for projection: " + shown(name), e);
    }
  }

  /**
   * Rewinds the projection's checkpoint to offset {@code 0}, bypassing the monotonic guard — the
   * reset/rebuild path, which a save cannot express. Seeds the row at {@code 0} if absent; the
   * epoch is left as it is.
   *
   * @param ds the pool to borrow the connection from
   * @param name the projection
   * @throws EventStoreException if the connection or the statement fails
   */
  void reset(DataSource ds, ProjectionName name) {
    try (var conn = ds.getConnection();
        var ps = conn.prepareStatement(resetSql)) {
      ps.setString(1, name.value());
      PostgresTransactions.commitIfManual(conn, "projection offset reset", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to reset offset for projection: " + shown(name), e);
    }
  }

  /**
   * Saves {@code offset} outside any batch transaction — the {@code OffsetStore.saveOffset} path —
   * under the monotonic guard: a save at or below the stored offset is a silent no-op. Leaves the
   * epoch as it is.
   *
   * @param ds the pool to borrow the connection from
   * @param name the projection
   * @param offset the offset to save
   * @throws EventStoreException if the connection or the statement fails
   */
  void saveUnlocked(DataSource ds, ProjectionName name, GlobalOffset offset) {
    try (var conn = ds.getConnection();
        var ps = conn.prepareStatement(saveUnlockedSql)) {
      ps.setString(1, name.value());
      ps.setLong(2, offset.value());
      PostgresTransactions.commitIfManual(conn, "projection offset save", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to save offset for projection: " + shown(name), e);
    }
  }

  /** The committed checkpoint of a projection: its last applied offset and stamped epoch. */
  private record Checkpoint(long offset, long epoch) {}

  /**
   * Seeds a {@code 0} checkpoint if none exists, then {@code SELECT ... FOR UPDATE}-locks the row
   * and returns its stored offset and epoch. Runs on the caller's transaction, so the lock is held
   * until it commits or rolls back.
   */
  private Checkpoint lock(Connection conn, ProjectionName name) throws SQLException {
    try (var ps = conn.prepareStatement(seedSql)) {
      ps.setString(1, name.value());
      ps.executeUpdate();
    }
    try (var ps = conn.prepareStatement(lockSql)) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? new Checkpoint(rs.getLong(1), rs.getLong(2)) : new Checkpoint(0L, 0L);
      }
    }
  }

  /**
   * Fences out a commit whose caller epoch is below the epoch stored on the checkpoint row — a
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
   * Rejects a batch whose first event offset is at or before the committed checkpoint — it overlaps
   * events already applied by another writer (a concurrent/failed-over stale leader or a
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
    try (var ps = conn.prepareStatement(epochSql)) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong("epoch") : 0L;
      }
    }
  }

  /** A projection name rendered for an exception message: caller text, so sanitized. */
  private static String shown(ProjectionName name) {
    return LogSanitizer.sanitizeForLog(name.value());
  }
}
