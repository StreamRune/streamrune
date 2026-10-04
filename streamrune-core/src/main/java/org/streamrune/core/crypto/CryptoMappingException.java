package org.streamrune.core.crypto;

/**
 * A <b>deterministic</b> crypto failure — raised by {@code CryptoShreddingModule} for a mapping it
 * can never perform, and by a {@link CryptoEngine} for a deterministic verdict on the blob it was
 * handed — as opposed to the plain {@link CryptoOperationException} an engine raises for a
 * transient, backend or key-level failure such as an unavailable key store or a disabled key.
 *
 * <p><b>Why the distinction is load-bearing.</b> {@code CryptoOperationException} is used by the
 * crypto layer for two failure classes that demand opposite handling:
 *
 * <ul>
 *   <li><b>Deterministic</b> — the mapper can never produce (or consume) the stored form for this
 *       type or this value: an {@code @Encrypted} component whose {@code subjectId} field is {@code
 *       null} (the natural shape of a partially-populated saga state mid-flow), a shape/annotation
 *       combination the encrypting writer cannot honour without leaking plaintext, a record whose
 *       constructor rejects the post-forget {@code [REDACTED]} tombstone. Retrying re-fails
 *       identically, forever. <b>This type.</b>
 *   <li><b>Transient</b> — a Vault/AWS-KMS outage, a pool timeout, an unreadable key volume. The
 *       backends deliberately fail closed with {@code CryptoOperationException} precisely so a
 *       replay <em>blocks and retries</em> instead of writing {@code [REDACTED]} tombstones over
 *       recoverable data. <b>Never this type.</b>
 * </ul>
 *
 * <p>Without a type split, a caller that must choose between "quarantine" and "retry" — {@code
 * SagaRunner} classifying a {@code SagaStateSerializationException}, for one — has only two wrong
 * options: quarantine everything (dead-letters events and FAULTs sagas during a routine KMS outage)
 * or retry everything (a deterministic failure wedges the subscription forever, no event
 * quarantined, no saga FAULTED, health still RUNNING). This type gives the deterministic half a
 * name so only it is quarantined.
 *
 * <p>The split is sound in one direction only. A <b>bare</b> {@code CryptoOperationException} is
 * never raised by {@code CryptoShreddingModule} itself — it <b>never wraps engine exceptions</b>,
 * so {@code CryptoEngine.encrypt}/{@code decrypt} failures propagate with the engine's own type —
 * meaning every bare occurrence in a cause chain came from an engine reporting a transient,
 * backend, or key-level failure (a Vault/AWS-KMS outage, a policy refusal an operator can reverse,
 * a disabled or wrong key) and calls for <em>retry</em>. {@code CryptoMappingException} itself has
 * two sources, not one: the module raises it for the mapping/binding failures above, and every
 * shipped {@link CryptoEngine} also raises it directly for a deterministic verdict on the blob it
 * was handed — too short, an unsupported format or key/envelope version, a local AEAD tag failure,
 * AWS KMS's {@code InvalidCiphertextException}, or a Vault HTTP 400 body that positively identifies
 * an invalid-ciphertext verdict. So, unlike the bare type, seeing this type in a chain does not by
 * itself say which side raised it.
 *
 * <p>Extends {@link CryptoOperationException} deliberately: every {@code catch
 * (CryptoOperationException)} and every fail-closed check that tests for that type (e.g. {@code
 * PostgresEventStore.hasCryptoCause}, which must refuse to discard an undecryptable snapshot) also
 * covers this subtype. Only callers that need the split test for it.
 */
public class CryptoMappingException extends CryptoOperationException {

  private static final long serialVersionUID = 1L;

  public CryptoMappingException(String message) {
    super(message);
  }

  public CryptoMappingException(String message, Throwable cause) {
    super(message, cause);
  }
}
