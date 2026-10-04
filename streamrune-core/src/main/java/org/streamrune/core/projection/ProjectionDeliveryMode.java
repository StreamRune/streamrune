package org.streamrune.core.projection;

/**
 * What a projection promises about the relationship between its read-model writes and its
 * checkpoint. Required on every registration ({@code register(...)}, {@code @ProjectionConfig},
 * {@code ProjectionRunner.run}); there is no default. Which value is right is decided by where the
 * projection writes, never by which beans exist.
 *
 * <p>No mode makes an effect outside the database exactly-once: an HTTP call, a Kafka produce or an
 * e-mail inside {@code process} is at-least-once under every mode and belongs behind an outbox.
 */
public enum ProjectionDeliveryMode {
  /**
   * Read-model write and checkpoint commit in ONE transaction. Needs a transactional {@link
   * AtomicBatchProcessor} ({@link AtomicBatchProcessor#supportsFencing()} {@code true}) and a
   * projection that writes through the repository handed to {@link
   * Projection#process(java.util.List, ProjectionRepository)}; when the projection exposes its
   * {@link Projection#writeTarget()}, the processor must be that store ({@link
   * AtomicBatchProcessor#writesTo}). A redelivered or split-brain batch is rejected before any
   * write.
   */
  TRANSACTIONAL_LOCAL,
  /**
   * Read-model write and checkpoint commit separately. The runner hands this projection NO
   * transaction-scoped repository ({@code process(batch, null)}) under every processor: its writes
   * go where it sends them, outside the checkpoint transaction. A crash or a rejected checkpoint
   * redelivers the batch from the last committed checkpoint and the projection applies it again, so
   * it dedups, version-guards or upserts idempotently. This value IS the acknowledgement of
   * at-least-once delivery.
   */
  AT_LEAST_ONCE_IDEMPOTENT,
  /**
   * The projection's job is a side effect outside this database. It never calls the external system
   * from {@code process()}; it writes an outbox row through the handed repository, in the
   * checkpoint's transaction, and a relay you provide delivers it at-least-once to an idempotent
   * receiver. Mechanically identical to {@link #TRANSACTIONAL_LOCAL}; the contract is what differs.
   */
  EXTERNAL_EFFECT;

  /**
   * {@code true} for {@link #TRANSACTIONAL_LOCAL} and {@link #EXTERNAL_EFFECT}: the runner passes
   * the processor's transaction-scoped repository to {@code process(batch, repository)}. {@code
   * false} for {@link #AT_LEAST_ONCE_IDEMPOTENT}: the runner passes {@code null}.
   *
   * @return whether this mode's read-model writes go through the checkpoint's transaction
   */
  public boolean writesInCheckpointTransaction() {
    return this != AT_LEAST_ONCE_IDEMPOTENT;
  }
}
