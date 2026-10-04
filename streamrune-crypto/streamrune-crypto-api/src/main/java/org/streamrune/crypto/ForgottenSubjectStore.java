package org.streamrune.crypto;

import org.streamrune.core.types.SubjectId;

/**
 * Per-subject crypto-shredding tombstone shared by the crypto backends whose erasure is not, by
 * itself, terminal — the AWS KMS backend (one shared Customer Managed Key, so there is no
 * per-subject key to hard-delete) and the HashiCorp Vault backend (whose transit engine re-creates
 * a deleted key on the next {@code encrypt}).
 *
 * <p>Those engines record a tombstone here on {@code deleteKey(subjectId)}; a tombstoned subject
 * then fails {@code decrypt} with {@code KeyNotFoundException} (mapped to {@code [REDACTED]}) and
 * {@code encrypt} with {@code SubjectForgottenException} — the same terminal-erasure behavior the
 * filesystem and PostgreSQL backends enforce natively, so the auto-wired {@code
 * ForgetSubjectService} completes (crypto-shred step + read-model purge) on those backends too.
 *
 * <p><b>Durability.</b> Erasure is only as durable as this store, so the engines that consume it
 * have <b>no default</b> and fail closed unless one is supplied. Use the durable {@code
 * JdbcForgottenSubjectStore} (in {@code streamrune-postgres-crypto}) in production — a {@code
 * forgotten_subjects} database row that survives restarts; {@link InMemoryForgottenSubjectStore} is
 * process-local — tombstones are lost on restart — so it is for tests / single-process ephemeral
 * use only. Supply the chosen store via the engine builder's {@code forgottenSubjectStore(...)}
 * method. Implementations must be safe for concurrent use.
 */
public interface ForgottenSubjectStore {

  /** Records a durable tombstone for {@code subjectId}. Idempotent. */
  void forget(SubjectId subjectId);

  /**
   * Records a durable tombstone for {@code subjectId} <em>together with the erased key's
   * backend-sourced creation instant</em> — the evidence {@code VaultCryptoEngine.reinstate} later
   * uses to prove key IDENTITY. Idempotent, and <b>first-write-wins on the evidence too</b>: a
   * repeated {@code deleteKey} must never overwrite the first erasure's record, because the key it
   * can read the second time may already be post-forget residue.
   *
   * <p>Why it exists: comparing the transit key's age against the tombstone's age would span TWO
   * hosts' clocks (this process's {@code now} and the store's {@code forgotten_at} — Postgres
   * {@code NOW()} for the JDBC store). A store clock running ahead of this process's under-measures
   * that age in the direction that LOOSENS the check, so a post-forget residue key could pass it
   * and be revived. Recording the key's creation time at erasure time makes the later comparison
   * Vault-to-Vault, with no cross-clock term at all.
   *
   * <p><b>Default and its degradation:</b> a store that does not track key-creation evidence may
   * rely on this default; it drops the evidence and delegates to {@link #forget(SubjectId)}, and
   * stays SAFE — {@link #keyCreatedAt(SubjectId)} then reports nothing and the Vault engine
   * <em>fails closed</em> (refuses the reinstate, keeping the tombstone). Only the crashed-erasure
   * revival convenience is lost; erasure safety is never weakened. Override BOTH methods to support
   * revival.
   *
   * @param subjectId the subject to tombstone; must not be null
   * @param keyCreatedAt the erased key's creation instant as reported by the key backend itself, or
   *     {@code null} when the backend could not supply provable evidence (the caller must then
   *     accept that the revival will be refused)
   */
  default void forget(SubjectId subjectId, java.time.Instant keyCreatedAt) {
    forget(subjectId);
  }

  /**
   * The key-creation evidence recorded with {@code subjectId}'s current tombstone by {@link
   * #forget(SubjectId, java.time.Instant)}, or empty when the subject is not tombstoned, the
   * tombstone was recorded without evidence, the erasure could not read it, <em>or this store does
   * not track it</em>.
   *
   * <p>Callers must treat empty as <b>unprovable identity and fail closed</b> — never as "any live
   * key will do". A read failure must THROW rather than answer empty, so a store outage cannot be
   * mistaken for "no evidence".
   *
   * @param subjectId the tombstoned subject; must not be null
   * @return the recorded key-creation instant, or empty when unknown or not tombstoned
   */
  default java.util.Optional<java.time.Instant> keyCreatedAt(SubjectId subjectId) {
    return java.util.Optional.empty();
  }

  /** Returns {@code true} if {@code subjectId} has been tombstoned (crypto-shredded). */
  boolean isForgotten(SubjectId subjectId);

  /** Removes the tombstone for {@code subjectId}, re-enabling encryption. Idempotent. */
  void reinstate(SubjectId subjectId);

  /**
   * The instant {@code subjectId}'s current tombstone was first recorded, or empty when the subject
   * is not currently tombstoned <em>or this store does not track the timestamp</em> (callers must
   * use {@link #isForgotten(SubjectId)} to tell those apart).
   *
   * <p><b>Diagnostic, not a term of any proof</b>. {@code VaultCryptoEngine.reinstate}
   * discriminates the ORIGINAL transit key from encrypt-race residue by comparing {@link
   * #keyCreatedAt(SubjectId)} against the live key's creation time — both sourced from the key
   * backend — never against this tombstone's age, which would span two hosts' clocks (this
   * process's and the store's): a store clock running ahead of the caller's under-measures the
   * tombstone's age in the direction that makes a post-forget residue key pass. This timestamp
   * serves as the tombstone's audit record and as the context the refusal message quotes. The
   * shipped stores supply it: {@code JdbcForgottenSubjectStore} reads the {@code forgotten_at}
   * column of the crypto baseline (V001); {@link InMemoryForgottenSubjectStore} records the first
   * {@code forget} instant.
   *
   * <p><b>Default and its degradation:</b> a store that does not track the timestamp may rely on
   * this default, which returns empty. It costs nothing but diagnostic detail in the Vault engine's
   * refusal message.
   *
   * @param subjectId the tombstoned subject; must not be null
   * @return the first-forget instant, or empty when unknown or not tombstoned
   */
  default java.util.Optional<java.time.Instant> forgottenAt(SubjectId subjectId) {
    return java.util.Optional.empty();
  }

  /**
   * Whether this store persists tombstones in the shared {@code forgotten_subjects} database table
   * (the crypto baseline, {@code V001}) and therefore needs that table provisioned. Lets a
   * KMS/Vault engine report the {@code forgotten_subjects} table from {@code
   * requiredCryptoTables()} so the event-store factory can auto-provision and validate it. The
   * default is {@code false} (an in-memory store needs no schema); the JDBC-backed store overrides
   * it to {@code true}.
   */
  default boolean requiresDatabaseSchema() {
    return false;
  }
}
