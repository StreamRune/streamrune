package org.streamrune.core.backend;

import java.time.Duration;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStore;

/**
 * What {@link BackendStores#outboxStore(OutboxOptions)} builds the outbox with.
 *
 * @param claimLease how long an {@code IN_PROGRESS} claim is honoured before another instance may
 *     reclaim the entry ({@link OutboxStore#claimLease()}); positive
 * @param orderingMode the ordering the outbox enforces ({@link OutboxStore#orderingMode()}); not
 *     {@code null}
 */
public record OutboxOptions(Duration claimLease, OutboxOrderingMode orderingMode) {

  /**
   * The claim lease an application uses when it sets none: four minutes, strictly longer than the
   * longest in-flight horizon of the shipped publishers (Kafka's {@code max.block.ms} 60s plus
   * {@code delivery.timeout.ms} 120s).
   */
  public static final Duration DEFAULT_CLAIM_LEASE = Duration.ofMinutes(4);

  /**
   * Validates the options.
   *
   * @throws IllegalArgumentException if {@code claimLease} is {@code null}, zero or negative, or
   *     {@code orderingMode} is {@code null}; the message names the component
   */
  public OutboxOptions {
    if (claimLease == null) {
      throw new IllegalArgumentException("claimLease is required");
    }
    if (claimLease.isZero() || claimLease.isNegative()) {
      throw new IllegalArgumentException("claimLease must be positive, got: " + claimLease);
    }
    if (orderingMode == null) {
      throw new IllegalArgumentException("orderingMode is required");
    }
  }
}
