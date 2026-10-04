package org.streamrune.core.crypto;

import org.streamrune.core.types.SubjectId;

/**
 * Encryption engine for crypto-shredding support. Encrypts and decrypts data using per-subject
 * keys. Deleting a subject's key renders all their encrypted data unreadable (GDPR "right to be
 * forgotten").
 *
 * <p><b>Terminal erasure.</b> {@link #deleteKey(SubjectId)} is the GDPR Article 17 operation and is
 * <em>terminal</em>: once a subject is forgotten, a subsequent {@link #encrypt(SubjectId, byte[])}
 * for that subject must <b>fail loudly</b> with {@link SubjectForgottenException} rather than
 * silently minting a fresh key. Silently re-creating a key would write new, recoverable PII for a
 * subject who exercised right-to-erasure (a retried command, a re-imported event, any path that did
 * not stop carrying the subject's PII) and would un-do the terminal intent of the erasure. Backends
 * that can persist a tombstone enforce this; backends that cannot define their own recreation
 * behavior (see below).
 *
 * <p><b>Enforcement is backend-specific:</b>
 *
 * <ul>
 *   <li><em>Filesystem, PostgreSQL</em> — persist a tombstone alongside/instead of the key, so
 *       erasure survives restarts and {@code encrypt} after {@code deleteKey} throws {@link
 *       SubjectForgottenException}; {@link #reinstate(SubjectId)} lifts it for legitimate
 *       re-registration.
 *   <li><em>AWS KMS</em> — a single shared CMK encrypts every subject, so there is no per-subject
 *       key to hard-delete; {@code deleteKey} instead records a per-subject tombstone (in a
 *       pluggable store, mandatory — the engine's builder fails closed without one; there is no
 *       in-memory default) so {@code decrypt} yields {@link KeyNotFoundException} and {@code
 *       encrypt} throws {@link SubjectForgottenException}, and {@link #reinstate(SubjectId)} lifts
 *       it. Erasure durability equals the tombstone store's durability — see the engine's javadoc
 *       and module README.
 *   <li><em>Vault</em> — Vault's transit engine re-creates a deleted key on the next {@code
 *       encrypt}, so {@code deleteKey} deletes the Vault key <em>and</em> records a per-subject
 *       tombstone (in a pluggable store, mandatory — the engine fails closed with no default) so
 *       {@code encrypt} throws {@link SubjectForgottenException} and {@code decrypt} yields {@link
 *       KeyNotFoundException}, and {@link #reinstate(SubjectId)} lifts it. Erasure durability
 *       equals the tombstone store's durability — see the engine's javadoc and module README.
 * </ul>
 *
 * <p>On every shipped backend, {@link #reinstate(SubjectId)} is the only way to re-register a
 * forgotten subject: it is explicit and per subject, and no backend offers an engine-wide opt-out
 * of terminal erasure.
 *
 * <p>{@link #isKeyAvailable(SubjectId)} remains advisory and cannot be used to gate {@code
 * encrypt}: it is stale the moment it returns and races with concurrent calls. Terminal-erasure
 * enforcement lives inside {@code encrypt} itself, not in a check-then-act dance.
 *
 * <p><b>Common contract</b> (all StreamRune implementations):
 *
 * <ul>
 *   <li>{@code subjectId} must be non-null; violations throw {@link IllegalArgumentException}.
 *   <li>{@code plaintext}/{@code ciphertext} must be non-null; violations throw {@link
 *       IllegalArgumentException}.
 *   <li>Implementations must be safe for concurrent use from multiple threads.
 *   <li>Infrastructure failures (I/O, database, remote KMS) are wrapped in {@link
 *       CryptoOperationException}.
 * </ul>
 */
public interface CryptoEngine {

  /**
   * Encrypts plaintext using the key for the given subject. Creates a new key if none exists for a
   * subject that was never forgotten.
   *
   * <p><b>Terminal erasure:</b> if the subject was crypto-shredded via {@link
   * #deleteKey(SubjectId)} and the backend enforces tombstones (every backend shipped with
   * StreamRune: filesystem, PostgreSQL, Vault, AWS KMS), this throws {@link
   * SubjectForgottenException} instead of minting a fresh key — see the class contract. Lift the
   * tombstone via {@link #reinstate(SubjectId)} to allow legitimate re-registration.
   *
   * @param subjectId the data subject the key belongs to; must not be null
   * @param plaintext the bytes to encrypt; must not be null
   * @return the ciphertext, in an implementation-defined format
   * @throws IllegalArgumentException if {@code subjectId} or {@code plaintext} is null
   * @throws SubjectForgottenException if the subject was crypto-shredded and the backend enforces
   *     terminal erasure (every shipped backend) and the subject has not been reinstated since
   * @throws CryptoOperationException if encryption or key creation fails
   */
  byte[] encrypt(SubjectId subjectId, byte[] plaintext);

  /**
   * Decrypts ciphertext using the key for the given subject.
   *
   * @param subjectId the data subject the key belongs to; must not be null
   * @param ciphertext the bytes to decrypt; must not be null
   * @return the plaintext
   * @throws IllegalArgumentException if {@code subjectId} or {@code ciphertext} is null
   * @throws KeyNotFoundException if no key exists for the subject — either never created or
   *     crypto-shredded — or (PostgreSQL/filesystem) if the ciphertext was written under an earlier
   *     key generation that a GDPR erasure destroyed, even though the subject was later reinstated
   *     and re-keyed. Decrypting pre-forget ciphertext after a forget always throws this (the
   *     {@code CryptoShreddingModule} maps it to the {@code [REDACTED]} tombstone); it is never
   *     reported as {@link SubjectForgottenException}, which is reserved for the encrypt path.
   * @throws CryptoOperationException if decryption fails (corrupted or foreign ciphertext,
   *     infrastructure failure)
   */
  byte[] decrypt(SubjectId subjectId, byte[] ciphertext);

  /**
   * Deletes the encryption key for the given subject (crypto-shredding).
   *
   * <p>Idempotent: deleting a key that is already deleted changes nothing. A subject that never had
   * a key is still tombstoned by every backend shipped with StreamRune, so a later {@link
   * #encrypt(SubjectId, byte[])} for that id is refused until {@link #reinstate(SubjectId)}: erase
   * only subject ids known to exist. Terminal-erasure enforcement is backend-specific (see the
   * class documentation): filesystem and PostgreSQL delete the per-subject key and record a
   * persistent tombstone directly; Vault and AWS KMS record a persistent tombstone in a pluggable
   * {@code ForgottenSubjectStore} (Vault also deletes its transit key; AWS KMS has no per-subject
   * key to delete, since one shared CMK serves every subject). On every backend shipped with
   * StreamRune, once this method returns, a later {@link #encrypt(SubjectId, byte[])} for the
   * subject throws {@link SubjectForgottenException} instead of resurrecting the key, and
   * decrypting the subject's old ciphertext throws {@link KeyNotFoundException}.
   *
   * @param subjectId the data subject whose key to delete; must not be null
   * @throws IllegalArgumentException if {@code subjectId} is null
   * @throws UnsupportedOperationException if the backend can neither delete a per-subject key nor
   *     record a durable tombstone for one, so erasure must be handled by a separate compliance
   *     workflow outside this SPI. No backend shipped with StreamRune throws this — it remains
   *     available for third-party {@code CryptoEngine} implementations with no terminal-erasure
   *     mechanism of their own.
   * @throws CryptoOperationException if the deletion fails
   */
  void deleteKey(SubjectId subjectId);

  /**
   * Lifts the terminal-erasure tombstone for a previously forgotten subject, allowing {@link
   * #encrypt(SubjectId, byte[])} to mint a fresh key again for legitimate re-registration.
   *
   * <p>This is the explicit escape hatch from terminal erasure. After a completed erasure on
   * PostgreSQL, the filesystem or Vault it does <b>not</b> recover the old key or make pre-erasure
   * ciphertext readable again — those are gone forever; it only stops a future {@code encrypt} from
   * throwing {@link SubjectForgottenException}. On AWS KMS it does make pre-erasure ciphertext
   * readable again (see below). Idempotent: reinstating a subject that was never forgotten is a
   * no-op.
   *
   * <p><b>The engine writes no audit record.</b> A reinstate reverses part of an erasure, so it
   * must be a recorded, authorized decision: reinstate through {@code
   * ForgetSubjectService.reinstate(SubjectId, UserId)} (in {@code streamrune-runtime}), which
   * records a {@code GDPR_REINSTATE} SUCCESS or FAILURE row naming the requester beside the
   * erasure's {@code GDPR_FORGET} rows. Calling this method directly bypasses the GDPR audit trail.
   *
   * <p><b>What pre-erasure ciphertext reads as after reinstate + re-encrypt is
   * backend-specific:</b>
   *
   * <ul>
   *   <li><em>PostgreSQL, filesystem</em> — the re-minted key gets a HIGHER key generation
   *       (recorded durably across erasures), so decrypting pre-erasure ciphertext keeps yielding
   *       {@link KeyNotFoundException} → {@code [REDACTED]}: aggregate loads and projection
   *       rebuilds stay working, exactly as before the reinstate. Tampering with current-generation
   *       ciphertext still fails loudly.
   *   <li><em>Vault</em> — transit owns the ciphertext format, so generations cannot be
   *       discriminated; {@code reinstate} therefore <b>refuses</b> (throws {@link
   *       CryptoOperationException}) after a COMPLETED erasure — re-register under a new subject id
   *       — and succeeds only for the documented crashed-erasure residue (transit key still live),
   *       which revives the original key and its ciphertext.
   *   <li><em>AWS KMS</em> — the shared CMK is never destroyed, so reinstate makes pre-erasure
   *       ciphertext readable again (the tombstone gate is the erasure boundary — separately
   *       documented).
   * </ul>
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}; backends that do
   * not record tombstones have nothing to reinstate.
   *
   * @param subjectId the data subject to un-forget; must not be null
   * @throws IllegalArgumentException if {@code subjectId} is null
   * @throws UnsupportedOperationException if the backend does not record tombstones
   * @throws CryptoOperationException if clearing the tombstone fails, or if the backend refuses the
   *     reinstate because it cannot keep pre-erasure ciphertext replay-tolerant (Vault after a
   *     completed erasure)
   */
  default void reinstate(SubjectId subjectId) {
    throw new UnsupportedOperationException(
        "This CryptoEngine does not record crypto-shredding tombstones, so there is nothing to"
            + " reinstate");
  }

  /**
   * Returns true if an encryption key exists for the given subject.
   *
   * <p>Advisory only: the result is stale the moment it returns. It cannot distinguish "never had a
   * key" from "crypto-shredded", and checking it before {@link #encrypt(SubjectId, byte[])} does
   * not prevent key re-creation (TOCTOU race with concurrent encrypt calls) nor substitute for
   * terminal-erasure enforcement inside {@code encrypt}.
   *
   * @param subjectId the data subject to check; must not be null
   * @throws IllegalArgumentException if {@code subjectId} is null
   * @throws CryptoOperationException if the lookup fails
   */
  boolean isKeyAvailable(SubjectId subjectId);

  /**
   * The names of the tables in the shared crypto schema ({@code db/crypto-migration}) this engine
   * requires — e.g. {@code encryption_keys} and/or {@code forgotten_subjects}.
   *
   * <p>Those tables live in the opt-in {@code db/crypto-migration} Flyway location, which the
   * default schema auto-initialization does not apply and {@code SchemaValidator} does not
   * otherwise check. A Postgres-backed engine (or a KMS/Vault engine wired with a durable {@code
   * JdbcForgottenSubjectStore}) that reports its tables here lets the event-store factory (1)
   * auto-provision the crypto migration when schema auto-initialization is on, so durable erasure
   * works out of the box, and (2) fail fast at startup — with an actionable "apply
   * classpath:db/crypto-migration" message — instead of only throwing on the first
   * {@code @Encrypted} operation, when the operator manages the schema themselves.
   *
   * <p>The default is an empty set: backends that need no database table (filesystem, in-memory, or
   * a KMS/Vault engine with an in-memory tombstone store) require no crypto schema. Decorators
   * (e.g. a caching wrapper) must delegate to the wrapped engine.
   *
   * @return the required crypto-schema table names; never null, empty when none are required
   */
  default java.util.Set<String> requiredCryptoTables() {
    return java.util.Set.of();
  }
}
