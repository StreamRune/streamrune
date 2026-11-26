package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaOrigin;

/**
 * The fence read of a saga's forward command on SQLite: one primary-key read of the saga's {@code
 * saga_state} row, inside the append's own write transaction.
 *
 * <p><b>The write lock is the fence lock.</b> On Postgres the forward append takes a shared
 * advisory lock on the saga and every write that can take the saga out of its forward phase takes
 * the exclusive form ({@code SagaFenceLock}). SQLite needs neither: the append runs from {@code
 * BEGIN IMMEDIATE} to {@code COMMIT} holding the database-wide write lock, and so does every write
 * of {@link SqliteSagaStore} (see its class javadoc), on every connection and pool of the file. A
 * write that stamps a compensation episode, ends the saga or deletes its row therefore commits
 * wholly before the append's transaction begins, and the read finds what it wrote, or begins after
 * the append committed, and every forward command it admitted is stored before the episode. An
 * extra in-process lock would add a lock-order hazard against the write lock and nothing else.
 *
 * <p>The read refuses the append with {@link SagaForwardFencedException} when the row is absent, of
 * another saga type, carries an episode stamp, or is terminal ({@code COMPLETED}, {@code
 * COMPENSATED}, {@code FAILED}). A stamp is never cleared and a terminal status never left, so the
 * verdict only ever turns from "forward" to "refused". The append rolls back on the refusal: no
 * event, no inbox claim, no outbox row.
 */
final class SagaFence {

  // The fence read. No row, or forward = 0, refuses the append. SQLite's boolean is 0 or 1.
  private static final String FENCE_SAGA_FORWARD =
      "SELECT episode_version IS NULL AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')"
          + " AS forward FROM saga_state WHERE saga_id = ? AND saga_type = ?";

  private SagaFence() {}

  /**
   * Reads the row of the saga {@code forwardOf} names on {@code conn}, inside the caller's open
   * write transaction, and throws unless the saga is in its forward phase.
   *
   * @throws SagaForwardFencedException if the saga has left its forward phase, is of another type
   *     or has no row
   * @throws SQLException if the read fails
   */
  static void requireForward(Connection conn, SagaOrigin forwardOf) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(FENCE_SAGA_FORWARD)) {
      ps.setString(1, forwardOf.sagaId().value());
      ps.setString(2, forwardOf.sagaType().value());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next() || !rs.getBoolean(1)) {
          throw new SagaForwardFencedException(forwardOf);
        }
      }
    }
  }
}
