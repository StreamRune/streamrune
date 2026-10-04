package org.streamrune.core.crypto;

/**
 * Thrown by {@link CryptoEngine#encrypt(org.streamrune.core.types.SubjectId, byte[]) encrypt} when
 * a subject has been crypto-shredded via {@link
 * CryptoEngine#deleteKey(org.streamrune.core.types.SubjectId) deleteKey} and the engine refuses to
 * mint a fresh key for the erased subject.
 *
 * <p><b>Why this exists.</b> Crypto-shredding implements GDPR Article 17 (right to erasure):
 * deleting a subject's key renders all their ciphertext permanently unreadable. An engine that
 * silently re-creates a key on the next {@code encrypt(subjectId, ...)} — for a retried command, a
 * re-imported event, or any code path that did not stop carrying the subject's PII — un-does that
 * terminal intent: new, recoverable PII is written under a fresh key for a subject who exercised
 * right-to-erasure. Every backend shipped with StreamRune (filesystem, PostgreSQL, Vault, AWS KMS)
 * records a tombstone, makes erasure terminal and surfaces this exception instead of resurrecting
 * the key.
 *
 * <p>This is deliberately <em>distinct</em> from {@link KeyNotFoundException}: a forgotten subject
 * is not the same as a subject who never had a key. {@code KeyNotFoundException} on a
 * <em>decrypt</em> of old ciphertext still means "key gone, return the tombstone"; {@code
 * SubjectForgottenException} on an <em>encrypt</em> means "this write must not happen — the subject
 * was erased".
 *
 * <p>To legitimately re-register a previously forgotten subject, lift its tombstone explicitly via
 * {@link CryptoEngine#reinstate(org.streamrune.core.types.SubjectId) reinstate} — the only
 * re-registration path; no backend offers an engine-wide opt-out. Call it through {@code
 * ForgetSubjectService.reinstate(SubjectId, UserId)} so the reinstate is recorded in the GDPR audit
 * trail; the engine method writes no audit record.
 */
public class SubjectForgottenException extends RuntimeException {

  public SubjectForgottenException(String message) {
    super(message);
  }

  public SubjectForgottenException(String message, Throwable cause) {
    super(message, cause);
  }
}
