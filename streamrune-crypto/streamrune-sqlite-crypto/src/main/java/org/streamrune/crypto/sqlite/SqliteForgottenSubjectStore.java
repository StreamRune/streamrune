package org.streamrune.crypto.sqlite;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteSchemaSeries;
import org.streamrune.sqlite.SqliteTimestamps;

/**
 * Durable {@link ForgottenSubjectStore} in a SQLite database: the store that makes the AWS KMS and
 * HashiCorp Vault engines' GDPR erasure survive a restart when the application runs on SQLite, the
 * counterpart of {@code JdbcForgottenSubjectStore}.
 *
 * <p><b>Schema.</b> Rows live in the {@code forgotten_subjects} table of {@link
 * SqliteSchemaSeries#CRYPTO}, the table {@link SqliteCryptoEngine} tombstones into, so both mark a
 * subject in the same row. {@code forgotten_at} (the first erasure) and {@code key_created_at} (the
 * erased key's creation instant, which the Vault engine records) are microseconds since the epoch;
 * {@code forgotten_at} is stamped with the store's clock, where PostgreSQL takes {@code NOW()}.
 *
 * <p><b>Subject ids are stored hashed, never verbatim:</b> the SHA-256 hex of the subject id, as in
 * every crypto store, so a subject id that is itself PII never persists in the permanent tombstone.
 * Every failure message names the subject only by that hash.
 *
 * <p><b>Writes.</b> {@link #forget} and {@link #reinstate} are one statement each, in a write
 * transaction of their own ({@code BEGIN IMMEDIATE} ... {@code COMMIT}), committed before they
 * return. Inside a projection batch on the same file they are refused at once with {@link
 * IllegalStateException}: joined to the batch, a tombstone would commit or roll back with it, and
 * the Vault engine destroys its transit key once {@code forget} returns. The reads join the batch's
 * transaction there, as the engine's do (see {@link SqliteCryptoEngine}). Crash points: before the
 * {@code COMMIT} nothing is written and the rerun writes it; after it the tombstone (or its
 * removal) is durable, and a rerun changes nothing: a repeated forget keeps the first instant and
 * evidence.
 *
 * <p>Safe for concurrent use: every method borrows a connection from the database's data source and
 * holds no mutable state, so the store keeps working after the {@link SqliteDatabase} handle it was
 * built from is closed, and holds nothing that needs releasing.
 */
public final class SqliteForgottenSubjectStore implements ForgottenSubjectStore {

  private final SqliteCryptoStatements statements;
  private final Clock clock;

  /**
   * A store over {@code database}, stamping {@code forgotten_at} with {@link Clock#systemUTC()}.
   *
   * @param database the database; its crypto series must be applied
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteForgottenSubjectStore(SqliteDatabase database) {
    this(database, Clock.systemUTC());
  }

  /**
   * A store over {@code database}, stamping {@code forgotten_at} with {@code clock}.
   *
   * @param database the database; its crypto series must be applied
   * @param clock the clock of {@code forgotten_at}
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  public SqliteForgottenSubjectStore(SqliteDatabase database, Clock clock) {
    if (database == null) {
      throw new IllegalArgumentException("database must not be null");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock must not be null");
    }
    this.statements =
        new SqliteCryptoStatements(
            database.dataSource(), database.file(), "forgotten-subject write");
    this.clock = clock;
  }

  /**
   * {@inheritDoc}
   *
   * <p>As {@link #forget(SubjectId, Instant)} without evidence.
   *
   * @throws IllegalStateException inside a projection batch on this store's database file, before
   *     anything runs: a tombstone write there would commit or roll back with the batch
   */
  @Override
  public void forget(SubjectId subjectId) {
    forget(subjectId, null);
  }

  /**
   * Writes the tombstone and the erased key's creation instant in one statement, so the evidence
   * never outlives or precedes its tombstone. A repeated forget changes nothing: the first
   * erasure's instant and evidence stay, and a tombstone recorded without evidence never gains
   * some. Never inside a projection batch on the same file: the tombstone is committed when this
   * returns, which the Vault engine relies on before it destroys its transit key.
   *
   * @throws IllegalStateException inside a projection batch on this store's database file, before
   *     anything runs: a tombstone write there would commit or roll back with the batch
   */
  @Override
  public void forget(SubjectId subjectId, Instant keyCreatedAt) {
    requireSubject(subjectId);
    statements.refuseInsideABatch();
    try {
      statements.writeAlone(
          conn -> {
            try (PreparedStatement ps =
                conn.prepareStatement(
                    "INSERT INTO forgotten_subjects (subject_id, forgotten_at, key_created_at)"
                        + " VALUES (?, ?, ?) ON CONFLICT (subject_id) DO NOTHING")) {
              ps.setString(1, SqliteCryptoEngine.storedSubjectId(subjectId));
              ps.setLong(2, SqliteTimestamps.toMicros(clock.instant()));
              if (keyCreatedAt == null) {
                ps.setNull(3, java.sql.Types.INTEGER);
              } else {
                ps.setLong(3, SqliteTimestamps.toMicros(keyCreatedAt));
              }
              return ps.executeUpdate();
            }
          });
    } catch (SQLException | RuntimeException e) {
      throw failure("record forgotten-subject tombstone for", subjectId, e);
    }
  }

  @Override
  public boolean isForgotten(SubjectId subjectId) {
    requireSubject(subjectId);
    try {
      return statements.read(
          conn -> {
            try (PreparedStatement ps =
                conn.prepareStatement("SELECT 1 FROM forgotten_subjects WHERE subject_id = ?")) {
              ps.setString(1, SqliteCryptoEngine.storedSubjectId(subjectId));
              try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
              }
            }
          });
    } catch (SQLException | RuntimeException e) {
      throw failure("check forgotten-subject tombstone for", subjectId, e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Committed when this returns; never inside a projection batch on the same file.
   *
   * @throws IllegalStateException inside a projection batch on this store's database file, before
   *     anything runs: a tombstone write there would commit or roll back with the batch
   */
  @Override
  public void reinstate(SubjectId subjectId) {
    requireSubject(subjectId);
    statements.refuseInsideABatch();
    try {
      statements.writeAlone(
          conn -> {
            try (PreparedStatement ps =
                conn.prepareStatement("DELETE FROM forgotten_subjects WHERE subject_id = ?")) {
              ps.setString(1, SqliteCryptoEngine.storedSubjectId(subjectId));
              return ps.executeUpdate();
            }
          });
    } catch (SQLException | RuntimeException e) {
      throw failure("remove forgotten-subject tombstone for", subjectId, e);
    }
  }

  @Override
  public boolean requiresDatabaseSchema() {
    return true;
  }

  /**
   * The tombstone's first-forget instant; empty when the subject is not tombstoned. A read failure
   * throws.
   */
  @Override
  public Optional<Instant> forgottenAt(SubjectId subjectId) {
    requireSubject(subjectId);
    try {
      return instant(subjectId, "forgotten_at");
    } catch (SQLException | RuntimeException e) {
      throw failure("read forgotten-at timestamp for", subjectId, e);
    }
  }

  /**
   * The erased key's creation instant recorded with the tombstone; empty when the subject is not
   * tombstoned or the tombstone carries no evidence. A read failure THROWS: a reinstate must never
   * take a storage failure for "no evidence".
   */
  @Override
  public Optional<Instant> keyCreatedAt(SubjectId subjectId) {
    requireSubject(subjectId);
    try {
      return instant(subjectId, "key_created_at");
    } catch (SQLException | RuntimeException e) {
      throw failure("read erased-key creation evidence for", subjectId, e);
    }
  }

  /** The instant in {@code column} of the subject's tombstone, or empty (no row, or NULL). */
  private Optional<Instant> instant(SubjectId subjectId, String column) throws SQLException {
    return statements.read(
        conn -> {
          try (PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT " + column + " FROM forgotten_subjects WHERE subject_id = ?")) {
            ps.setString(1, SqliteCryptoEngine.storedSubjectId(subjectId));
            try (ResultSet rs = ps.executeQuery()) {
              if (!rs.next()) {
                return Optional.empty();
              }
              long micros = rs.getLong(1);
              return rs.wasNull()
                  ? Optional.empty()
                  : Optional.of(SqliteTimestamps.fromMicros(micros));
            }
          }
        });
  }

  /** A failure naming the subject by its hash only: the message reaches the GDPR audit log. */
  private static CryptoOperationException failure(
      String action, SubjectId subjectId, Exception cause) {
    return new CryptoOperationException(
        "Failed to "
            + action
            + " subject-hash="
            + subjectId.redacted()
            + SqliteCryptoStatements.lockAdvice(cause),
        cause);
  }

  private static void requireSubject(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
  }
}
