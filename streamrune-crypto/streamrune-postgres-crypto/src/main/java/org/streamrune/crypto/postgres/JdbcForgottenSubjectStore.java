package org.streamrune.crypto.postgres;

import static org.streamrune.crypto.JdbcSubjectKeyStore.storedSubjectId;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.crypto.JdbcForgottenSubjects;

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

  // The forgotten_subjects SQL, shared with the SQLite store; the insert and the timestamp read
  // are PostgreSQL's (ON CONFLICT DO NOTHING keeps the FIRST forget's row, evidence included).
  private static final JdbcForgottenSubjects FORGOTTEN =
      new JdbcForgottenSubjects(PostgresCryptoDialect.INSTANCE);

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
    write(
        subjectId,
        "record forgotten-subject tombstone for",
        (c, stored) -> FORGOTTEN.forget(c, stored, null));
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
    write(
        subjectId,
        "record forgotten-subject tombstone for",
        (c, stored) -> FORGOTTEN.forget(c, stored, keyCreatedAt));
  }

  @Override
  public boolean isForgotten(SubjectId subjectId) {
    requireNonNull(subjectId);
    try (Connection conn = dataSource.getConnection()) {
      return FORGOTTEN.isForgotten(conn, storedSubjectId(subjectId));
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
    write(subjectId, "remove forgotten-subject tombstone for", FORGOTTEN::reinstate);
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
    try (Connection conn = dataSource.getConnection()) {
      return FORGOTTEN.forgottenAt(conn, storedSubjectId(subjectId));
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
    try (Connection conn = dataSource.getConnection()) {
      return FORGOTTEN.keyCreatedAt(conn, storedSubjectId(subjectId));
    } catch (Exception e) {
      // Redact the (possibly-PII) subject id to its hash.
      throw new CryptoOperationException(
          "Failed to read erased-key creation evidence for subject-hash=" + subjectId.redacted(),
          e);
    }
  }

  /** One tombstone write on a borrowed connection, given the hashed subject id. */
  @FunctionalInterface
  private interface TombstoneWrite {
    void run(Connection conn, String storedSubjectId) throws SQLException;
  }

  /**
   * Runs {@code work} on a borrowed connection and makes it durable whatever the pool's autoCommit
   * mode ({@link PostgresCryptoTransactions#commitIfManual}).
   */
  private void write(SubjectId subjectId, String action, TombstoneWrite work) {
    try (Connection conn = dataSource.getConnection()) {
      String stored = storedSubjectId(subjectId);
      PostgresCryptoTransactions.commitIfManual(
          conn,
          c -> {
            work.run(c, stored);
            return null;
          });
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
}
