package org.streamrune.crypto.postgres;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HexFormat;
import javax.sql.DataSource;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;

/**
 * Durable, database-backed {@link ForgottenSubjectStore} — the persistent store that makes the AWS
 * KMS and HashiCorp Vault backends' GDPR erasure survive a restart.
 *
 * <p>Those backends have no per-subject key to hard-delete for good (KMS shares one Customer
 * Managed Key across every subject; Vault's transit engine re-creates a deleted key on the next
 * {@code encrypt}), so their {@code deleteKey(subjectId)} records a tombstone here instead. Unlike
 * {@link org.streamrune.crypto.InMemoryForgottenSubjectStore}, that tombstone is a database row, so
 * an erased subject stays forgotten across process restarts and shared across replicas: after a
 * restart the engine still refuses to decrypt/encrypt for the subject.
 *
 * <p><b>Schema.</b> Rows live in the shared {@code forgotten_subjects} table — the same table
 * {@code PostgresCryptoEngine} tombstones into, including its nullable {@code key_created_at}
 * column (both created by the crypto Flyway baseline shipped at {@code
 * classpath:db/crypto-migration}). Point this store at a {@link DataSource} whose database has that
 * schema applied. The upsert uses PostgreSQL {@code ON CONFLICT}.
 *
 * <p><b>Subject ids are stored hashed, never verbatim.</b> The {@code subject_id} column holds the
 * SHA-256 hex of the subject id — matching {@code PostgresCryptoEngine}'s convention — so a subject
 * id that is itself PII (an email or username) never persists in cleartext in the permanent
 * tombstone. All lookups hash first, so callers still pass the natural {@link SubjectId}.
 *
 * <p>Safe for concurrent use: every method borrows a short-lived {@link Connection} from the pool
 * and holds no mutable state.
 */
public final class JdbcForgottenSubjectStore implements ForgottenSubjectStore {

  private static final String FORGET_SQL =
      "INSERT INTO forgotten_subjects (subject_id) VALUES (?) ON CONFLICT (subject_id) DO NOTHING";
  private static final String IS_FORGOTTEN_SQL =
      "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?";
  private static final String REINSTATE_SQL = "DELETE FROM forgotten_subjects WHERE subject_id = ?";
  private static final String FORGOTTEN_AT_SQL =
      "SELECT forgotten_at FROM forgotten_subjects WHERE subject_id = ?";
  // The erased key's BACKEND-sourced creation instant,
  // recorded with the tombstone so a later reinstate can prove key identity without comparing two
  // hosts' clocks. Nullable (NULL = no evidence); ON CONFLICT DO NOTHING keeps the FIRST
  // erasure's evidence, matching forgotten_at's first-write-wins.
  private static final String FORGET_WITH_KEY_EVIDENCE_SQL =
      "INSERT INTO forgotten_subjects (subject_id, key_created_at) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO NOTHING";
  private static final String KEY_CREATED_AT_SQL =
      "SELECT key_created_at FROM forgotten_subjects WHERE subject_id = ?";

  private final DataSource dataSource;

  /**
   * @param dataSource a pooled {@link DataSource} whose database has the {@code forgotten_subjects}
   *     table (created by the crypto Flyway baseline); must not be null
   */
  public JdbcForgottenSubjectStore(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource must not be null");
    }
    this.dataSource = dataSource;
  }

  @Override
  public void forget(SubjectId subjectId) {
    requireNonNull(subjectId);
    execute(FORGET_SQL, subjectId, "record forgotten-subject tombstone for");
  }

  /**
   * Writes the tombstone AND the erased key's backend-sourced creation instant into the same row —
   * one statement, so the evidence can never outlive or precede its tombstone. {@code ON CONFLICT
   * DO NOTHING} keeps the FIRST erasure's evidence on a repeated {@code deleteKey}: the key
   * readable the second time may already be post-forget residue, and recording that would prove the
   * wrong key.
   *
   * <p>A {@code null} {@code keyCreatedAt} (the backend could not supply provable evidence) is
   * stored as SQL NULL, which is indistinguishable from any other tombstone without evidence — both
   * make {@link #keyCreatedAt(SubjectId)} answer empty and the Vault revival fail closed.
   */
  @Override
  public void forget(SubjectId subjectId, java.time.Instant keyCreatedAt) {
    requireNonNull(subjectId);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FORGET_WITH_KEY_EVIDENCE_SQL)) {
      ps.setString(1, storedSubjectId(subjectId));
      ps.setTimestamp(2, keyCreatedAt == null ? null : java.sql.Timestamp.from(keyCreatedAt));
      PostgresCryptoTransactions.commitIfManual(conn, c -> ps.executeUpdate());
    } catch (Exception e) {
      // Redact the (possibly-PII) subject id to its hash — this message reaches the
      // GDPR audit_log FAILURE row via ForgetSubjectService and any log/DLQ error handler.
      throw new CryptoOperationException(
          "Failed to record forgotten-subject tombstone for subject-hash=" + subjectId.redacted(),
          e);
    }
  }

  @Override
  public boolean isForgotten(SubjectId subjectId) {
    requireNonNull(subjectId);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(IS_FORGOTTEN_SQL)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (Exception e) {
      // The subject id may itself be PII (email/username). Redact it to its SHA-256
      // hash — a raw id here would leak into logs and, via any DLQ/error handler, into durable
      // error_message columns (matching the convention on every crypto-engine message).
      throw new CryptoOperationException(
          "Failed to check forgotten-subject tombstone for subject-hash=" + subjectId.redacted(),
          e);
    }
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    requireNonNull(subjectId);
    execute(REINSTATE_SQL, subjectId, "remove forgotten-subject tombstone for");
  }

  @Override
  public boolean requiresDatabaseSchema() {
    // Tombstones are rows in forgotten_subjects (crypto Flyway baseline), so a
    // KMS/Vault engine wired with this store needs that table provisioned/validated at startup.
    return true;
  }

  /**
   * The tombstone's durable {@code forgotten_at} (crypto Flyway baseline; first-forget instant —
   * the conflict-DO-NOTHING upsert never overwrites it). It is the erasure's audit record and the
   * context {@code VaultCryptoEngine.reinstate} quotes in its refusal — <b>not</b> a term of the
   * identity proof, which compares {@link #keyCreatedAt(SubjectId)} against the live key's creation
   * time so that no clock but the key backend's is involved. Empty when the subject is not
   * tombstoned. A read failure throws — reinstate must fail closed on it, never treat it as "no
   * timestamp".
   */
  @Override
  public java.util.Optional<java.time.Instant> forgottenAt(SubjectId subjectId) {
    requireNonNull(subjectId);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FORGOTTEN_AT_SQL)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return java.util.Optional.empty();
        }
        java.sql.Timestamp ts = rs.getTimestamp("forgotten_at");
        return java.util.Optional.ofNullable(ts).map(java.sql.Timestamp::toInstant);
      }
    } catch (Exception e) {
      // Redact the (possibly-PII) subject id to its hash.
      throw new CryptoOperationException(
          "Failed to read forgotten-at timestamp for subject-hash=" + subjectId.redacted(), e);
    }
  }

  /**
   * The erased key's backend-sourced creation instant recorded with this tombstone ({@code
   * key_created_at}), read by {@code VaultCryptoEngine.reinstate} to prove that the live transit
   * key IS the key the erasure targeted — a Vault-to-Vault comparison with no cross-clock term.
   * Empty when the subject is not tombstoned, or when the tombstone carries no evidence (a row
   * written without it, or an erasure whose key read failed): the revival then fails closed. A read
   * failure THROWS — reinstate must never mistake a database outage for "no evidence".
   */
  @Override
  public java.util.Optional<java.time.Instant> keyCreatedAt(SubjectId subjectId) {
    requireNonNull(subjectId);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(KEY_CREATED_AT_SQL)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return java.util.Optional.empty();
        }
        java.sql.Timestamp ts = rs.getTimestamp("key_created_at");
        return java.util.Optional.ofNullable(ts).map(java.sql.Timestamp::toInstant);
      }
    } catch (Exception e) {
      // Redact the (possibly-PII) subject id to its hash.
      throw new CryptoOperationException(
          "Failed to read erased-key creation evidence for subject-hash=" + subjectId.redacted(),
          e);
    }
  }

  private void execute(String sql, SubjectId subjectId, String action) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      PostgresCryptoTransactions.commitIfManual(conn, c -> ps.executeUpdate());
    } catch (Exception e) {
      // Redact the (possibly-PII) subject id to its hash — this message reaches the
      // GDPR audit_log FAILURE row via ForgetSubjectService and any log/DLQ error handler.
      throw new CryptoOperationException(
          "Failed to " + action + " subject-hash=" + subjectId.redacted(), e);
    }
  }

  private static void requireNonNull(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
  }

  /**
   * The opaque value written to / matched against the {@code subject_id} column: the SHA-256 hex of
   * the subject id's UTF-8 bytes. Mirrors {@code PostgresCryptoEngine.storedSubjectId} so both
   * backends tombstone the same subject to the same row, and a caller's subject id — which may
   * itself be PII — never lands verbatim in the permanent tombstone. UTF-8 is fixed so the hash is
   * stable across JVMs and platform default charsets.
   */
  private static String storedSubjectId(SubjectId subjectId) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(subjectId.value().getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      // The hash computation itself failed, so we cannot (and must not) emit the
      // subject's hash or its raw value — omit the identifier entirely, mirroring
      // VaultCryptoEngine.
      throw new CryptoOperationException("SHA-256 hashing failed for a subject", e);
    }
  }
}
