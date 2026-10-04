package org.streamrune.core.saga;

/**
 * Thrown when saga state cannot be converted to or from its stored form — as opposed to the {@link
 * org.streamrune.core.EventStoreException} a {@link SagaStore} throws when the <em>storage
 * backend</em> itself fails.
 *
 * <p><b>Why the distinction is load-bearing.</b> {@code SagaRunner} deliberately treats {@code
 * SagaStore} failures as infrastructure: they propagate out of {@code onEvents}, the subscription
 * does not advance its offset, and the batch is retried. That is correct for a lost connection or a
 * deadlocked transaction. It is catastrophic for a failure that is <em>deterministic</em>: the same
 * state re-serializes to the same failure on every retry, so the poll loop retries forever with
 * capped backoff, no event is quarantined, no saga is marked {@link SagaStatus#FAULTED}, no
 * dead-letter row is written, and the subscription still reports itself RUNNING. Every saga of
 * every type on that subscription stops advancing until a human notices the log storm.
 *
 * <p>Before this type existed, both failure classes were wrapped in the same {@code
 * EventStoreException} by a blanket {@code catch (Exception)} spanning serialization <em>and</em>
 * the JDBC call, so no caller could tell them apart. Serialization now gets its own narrow catch
 * and this distinct type; the blanket wrap covers only the storage operation.
 *
 * <p><b>This type carries no verdict.</b> It says "the conversion failed", not "retrying is futile"
 * — the cause chain is what distinguishes the two, and classifying it is the caller's job (the same
 * job {@code VirtualThreadCommandBus} already does for commands via its DLQ-eligibility rules). The
 * taxonomy a caller must expect:
 *
 * <ul>
 *   <li><b>Deterministic — retrying can never succeed.</b> A Jackson mapping failure (an unmappable
 *       shape, a self-referential structure, an unknown enum constant); a {@code
 *       CryptoOperationException} raised by {@code CryptoShreddingModule} itself because an
 *       {@code @Encrypted} component's {@code subjectId} field is {@code null} — it refuses to fall
 *       back to plaintext, which is the natural shape of a partially-populated saga state mid-flow;
 *       a {@code SubjectForgottenException} for a subject whose erasure completed. These must be
 *       quarantined (dead-letter the event, FAULT the saga) rather than retried.
 *   <li><b>Transient — retrying is correct.</b> A {@code CryptoOperationException} raised by the
 *       {@code CryptoEngine} itself: a Vault/AWS-KMS outage, a pool timeout, an unreadable key
 *       volume. The crypto backends deliberately fail closed with this type precisely so replay
 *       blocks and retries instead of writing {@code [REDACTED]} tombstones.
 * </ul>
 *
 * <p>Note that {@code CryptoOperationException} appears in both rows: the crypto layer uses one
 * type for both, so a caller that needs the split must inspect the cause chain rather than the
 * top-level type alone.
 *
 * <p>Modelled on {@link SagaTypeCollisionException}: a store-thrown, runtime-classified signal
 * whose whole purpose is that it is <em>not</em> the generic type, so no caller can mis-handle it
 * as something it isn't.
 */
public class SagaStateSerializationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final transient SagaId sagaId;

  /**
   * @param message what failed and for which saga
   * @param sagaId the saga whose state could not be converted; may be null if unknown
   * @param cause the underlying Jackson / crypto failure (required — it is the only thing that
   *     distinguishes a deterministic failure from a transient one)
   */
  public SagaStateSerializationException(String message, SagaId sagaId, Throwable cause) {
    super(message, cause);
    this.sagaId = sagaId;
  }

  /** The saga whose state could not be converted, or {@code null} when unknown. */
  public SagaId sagaId() {
    return sagaId;
  }
}
