package org.streamrune.postgres;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.CRC32C;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.SagaType;

/**
 * The transaction-scoped advisory lock that orders a saga's forward commands against the writes
 * that take the saga out of its forward phase.
 *
 * <p>A fenced append ({@code PostgresEventStore.appendWithKey} with a saga origin) takes the
 * <em>shared</em> lock on the saga's key and then reads the saga's row; every {@code saga_state}
 * write that can stamp a compensation episode, end the saga or delete its row takes the
 * <em>exclusive</em> lock on the same key before its statement, in the same transaction ({@code
 * PostgresSagaStore}'s one fence-changing write path). Both are {@code pg_advisory_xact_lock}
 * locks: released at commit or rollback, so they hold no session state and work behind a
 * transaction-pooling PgBouncer. PostgreSQL queues advisory lock requests in arrival order: a
 * shared request that arrives after a waiting exclusive one waits behind it, so a stream of forward
 * appends cannot hold a claim off.
 *
 * <p><b>The key.</b> The two-key form {@code (classid, objid)}: {@link #CLASS_ID} is reserved for
 * this lock, and {@code objid} is {@link #objectId(SagaType, SagaId)}, a CRC-32C of the saga type
 * and the saga id. PostgreSQL keeps the one-key ({@code bigint}) and the two-key advisory lock
 * spaces apart, so no one-key lock — the aggregate locks of {@code PgAdvisoryLocker}, the crypto
 * engine's key locks, Flyway's migration lock — can meet this one; no other code in StreamRune
 * takes a two-key advisory lock. Advisory locks are per database: stores of two schemas in one
 * database share the key space.
 *
 * <p><b>A collision is safe.</b> Two sagas whose keys collide (a 32-bit hash, so possible) share
 * one lock: a claim on one waits for an in-flight forward append of the other, and an append waits
 * behind the other's claim. That is a spurious wait of one transaction, never a wrong answer: the
 * fence's verdict comes from the saga's own row, read after the lock is granted, and the lock is
 * held by short transactions only (an append, one row write), none of which waits for another saga
 * lock while it holds one.
 *
 * <p>The hash is part of the protocol between the event store and the saga store, also across two
 * versions of an application during a rolling deployment; it must not change.
 */
final class SagaFenceLock {

  /** The first key of every saga fence lock ("SRGF"); no other two-key advisory lock uses it. */
  static final int CLASS_ID = 0x53524746;

  private static final String LOCK_SHARED = "SELECT pg_advisory_xact_lock_shared(?, ?)";
  private static final String LOCK_EXCLUSIVE = "SELECT pg_advisory_xact_lock(?, ?)";

  private SagaFenceLock() {}

  /** The second key of the saga's fence lock: CRC-32C of the type, a line feed, and the id. */
  static int objectId(SagaType sagaType, SagaId sagaId) {
    CRC32C crc = new CRC32C();
    crc.update((sagaType.value() + "\n" + sagaId.value()).getBytes(StandardCharsets.UTF_8));
    return (int) crc.getValue();
  }

  /**
   * Takes the shared lock in {@code conn}'s open transaction; waits behind an exclusive holder and
   * behind an exclusive request queued before it.
   */
  static void lockShared(Connection conn, SagaType sagaType, SagaId sagaId) throws SQLException {
    lock(conn, LOCK_SHARED, sagaType, sagaId);
  }

  /** Takes the exclusive lock in {@code conn}'s open transaction; waits for every holder. */
  static void lockExclusive(Connection conn, SagaType sagaType, SagaId sagaId) throws SQLException {
    lock(conn, LOCK_EXCLUSIVE, sagaType, sagaId);
  }

  private static void lock(Connection conn, String sql, SagaType sagaType, SagaId sagaId)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setInt(1, CLASS_ID);
      ps.setInt(2, objectId(sagaType, sagaId));
      // executeQuery: the lock function returns one (void) row
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    }
  }
}
