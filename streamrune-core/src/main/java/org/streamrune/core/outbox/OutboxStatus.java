package org.streamrune.core.outbox;

/** Delivery status of an {@link OutboxEntry}. */
public enum OutboxStatus {
  /** Entry is awaiting delivery. */
  PENDING,
  /**
   * Entry has been claimed by a relay instance for delivery (competing-consumer lease). Stale
   * claims — whose lease has expired without a delivery outcome — are reset to {@link #PENDING}.
   */
  IN_PROGRESS,
  /** Entry was delivered successfully. */
  DELIVERED,
  /**
   * The counted retry ladder is exhausted. Unresolved until an operator acts: {@code
   * OutboxFailedReplayer.replay} returns it to {@link #PENDING}, {@code skip} moves it to {@link
   * #SKIPPED}. On a {@code STRICT_PER_AGGREGATE} channel a FAILED entry is still the head of its
   * aggregate and blocks every later entry of that aggregate.
   */
  FAILED,
  /**
   * Terminal. An operator decided the entry will never be delivered; the row carries {@code
   * skipped_at}, {@code skipped_by} and {@code skip_reason}. Releases the aggregate on a strict
   * channel. A skip is not a delivery: the downstream did not receive this change.
   */
  SKIPPED
}
