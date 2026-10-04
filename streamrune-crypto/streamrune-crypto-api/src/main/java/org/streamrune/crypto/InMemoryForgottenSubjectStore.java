package org.streamrune.crypto;

import java.util.concurrent.ConcurrentHashMap;
import org.streamrune.core.types.SubjectId;

/**
 * Process-local {@link ForgottenSubjectStore} for tests and single-process/ephemeral use.
 *
 * <p><b>Not durable, and not a production default.</b> Tombstones live only in this JVM's heap and
 * are lost on restart, after which a subject that was crypto-shredded can be re-encrypted (and, on
 * the AWS KMS backend, its pre-erasure ciphertext becomes decryptable again under the intact shared
 * CMK). The engines that consume a {@link ForgottenSubjectStore} therefore do <b>not</b> default to
 * this store — they fail closed unless a store is supplied — so this must be opted into explicitly
 * and only where erasure need not survive a restart. For production GDPR erasure use the durable
 * {@code JdbcForgottenSubjectStore} (in {@code streamrune-postgres-crypto}).
 */
public final class InMemoryForgottenSubjectStore implements ForgottenSubjectStore {

  /**
   * Tombstoned subject → the instant of its FIRST {@code forget} (first-write-wins, mirroring the
   * durable store's {@code forgotten_at DEFAULT NOW()} + conflict-DO-NOTHING semantics), so {@link
   * #forgottenAt(SubjectId)} answers the Vault engine's crashed-vs-residue discrimination the same
   * way in tests as {@code JdbcForgottenSubjectStore} does in production.
   */
  private final ConcurrentHashMap<SubjectId, java.time.Instant> forgotten =
      new ConcurrentHashMap<>();

  /**
   * Tombstoned subject → the erased key's backend-sourced creation instant recorded by {@link
   * #forget(SubjectId, java.time.Instant)}, mirroring the crypto baseline's nullable {@code
   * key_created_at} column including its first-write-wins semantics. A subject tombstoned through
   * the one-argument {@link #forget(SubjectId)} has no entry here, so {@link
   * #keyCreatedAt(SubjectId)} answers empty exactly as a row written without evidence does — the
   * in-memory double must predict the Vault engine's fail-closed refusal, not hide it.
   */
  private final ConcurrentHashMap<SubjectId, java.time.Instant> keyCreatedAt =
      new ConcurrentHashMap<>();

  @Override
  public void forget(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    forgotten.putIfAbsent(subjectId, java.time.Instant.now());
  }

  @Override
  public void forget(SubjectId subjectId, java.time.Instant erasedKeyCreatedAt) {
    forget(subjectId);
    if (erasedKeyCreatedAt != null) {
      // First-write-wins, like the durable store's ON CONFLICT DO NOTHING: a repeated deleteKey
      // must never overwrite the FIRST erasure's evidence, because the key it reads the second
      // time may already be post-forget residue.
      keyCreatedAt.putIfAbsent(subjectId, erasedKeyCreatedAt);
    }
  }

  @Override
  public boolean isForgotten(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    return forgotten.containsKey(subjectId);
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    forgotten.remove(subjectId);
    // The evidence belongs to the tombstone (the durable store holds both in ONE row, so the
    // tombstone DELETE takes the evidence with it). Leaving it behind would let a LATER erasure's
    // reinstate be proven against a PREVIOUS erasure's key.
    keyCreatedAt.remove(subjectId);
  }

  @Override
  public java.util.Optional<java.time.Instant> forgottenAt(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    return java.util.Optional.ofNullable(forgotten.get(subjectId));
  }

  @Override
  public java.util.Optional<java.time.Instant> keyCreatedAt(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    return java.util.Optional.ofNullable(keyCreatedAt.get(subjectId));
  }
}
