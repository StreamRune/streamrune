package org.streamrune.runtime;

import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.LogSanitizer;

/**
 * Single place where the three leadership-aware projection runner builders refuse to assemble a
 * multi-replica projection whose {@link AtomicBatchProcessor} cannot honour the leadership fencing
 * epoch.
 *
 * <p>Leadership issues a strictly-increasing epoch per acquisition and the runners thread it into
 * every commit, but only a processor that fences its checkpoint ({@link
 * AtomicBatchProcessor#fencesCheckpoint()}) turns that token into a guarantee: it rejects a commit
 * from a superseded epoch before the updater writes a single read-model row, and moves the
 * checkpoint only under the current epoch. Paired with a processor that ignores the epoch, the
 * whole protocol is theatre — the takeover stamp reported success from an empty method body, the
 * nonatomic processor dropped the epoch, and the read-model write and the checkpoint became two
 * independent auto-commit statements. A stale leader resuming inside its local lease-staleness
 * window then double-applied its in-flight batch to a non-idempotent read model, and the offset
 * store's monotonic guard swallowed the trailing checkpoint save so nothing logged.
 *
 * <p>That is a configuration error, not a degraded mode, so it is rejected at build time: every
 * replica fails to start with the same actionable message, before any traffic, instead of booting
 * and corrupting a read model on the first failover.
 */
final class ProjectionFencingPolicy {

  private ProjectionFencingPolicy() {}

  /**
   * Rejects a real (multi-replica) leadership paired with a processor whose {@link
   * AtomicBatchProcessor#fencesCheckpoint()} is {@code false}.
   *
   * @param leadership the configured leadership; {@link SubscriptionLeadership#NOOP} is the
   *     unfenced single-instance contract (epoch {@code 0}) and is always accepted
   * @param processor the configured batch processor
   * @param runnerName the runner type, for the error message
   * @throws IllegalArgumentException if leadership is real and {@code processor} cannot fence
   */
  static void requireFencingCapable(
      SubscriptionLeadership leadership, AtomicBatchProcessor processor, String runnerName) {
    require(
        leadership,
        processor,
        runnerName,
        processor == null ? "null" : processor.getClass().getName());
  }

  /**
   * {@link #requireFencingCapable(SubscriptionLeadership, AtomicBatchProcessor, String)} for one
   * registration of a runner that commits each registration through its own pair: {@code processor}
   * is that registration's processor, and a refusal names the registration and the checkpoint
   * target its processor came from, so a runner whose other registrations fence is refused for this
   * one alone.
   *
   * @param leadership the configured leadership; {@link SubscriptionLeadership#NOOP} is always
   *     accepted
   * @param processor the registration's batch processor
   * @param runnerName the runner type, for the error message
   * @param projectionName the registration's projection name, for the message (sanitized here)
   * @param targetName the registration's checkpoint target name, or {@code null} for the runner's
   *     default pair
   * @throws IllegalArgumentException if leadership is real and {@code processor} cannot fence
   */
  static void requireFencingCapable(
      SubscriptionLeadership leadership,
      AtomicBatchProcessor processor,
      String runnerName,
      String projectionName,
      String targetName) {
    require(
        leadership,
        processor,
        runnerName + " projection '" + LogSanitizer.sanitizeForLog(projectionName) + "'",
        processor == null
            ? "null"
            : processor.getClass().getName()
                + (targetName == null
                    ? ""
                    : " (checkpoint target '" + LogSanitizer.sanitizeForLog(targetName) + "')"));
  }

  private static void require(
      SubscriptionLeadership leadership,
      AtomicBatchProcessor processor,
      String subject,
      String processorDescription) {
    if (leadership == SubscriptionLeadership.NOOP) {
      return;
    }
    if (processor != null && processor.fencesCheckpoint()) {
      return;
    }
    throw new IllegalArgumentException(
        subject
            + " was configured with single-active-consumer leadership ("
            + (leadership.getClass().getName())
            + ") but its AtomicBatchProcessor cannot honour the leadership fencing epoch ("
            + processorDescription
            + "). The epoch fence and the first-offset overlap guard would be inert, and the"
            + " checkpoint write would not be conditional on the epoch: a superseded leader"
            + " resuming after a GC/VM pause would re-apply its in-flight batch to the read model"
            + " with nothing to reject it, and the offset store's monotonic guard would swallow the"
            + " trailing checkpoint save so nothing would even log. Route the projection to a store whose processor fences its checkpoint"
            + " (AtomicBatchProcessor.fencesCheckpoint() — org.streamrune.postgres"
            + ".JdbcProjectionRepository does) and keep leadership on, or — if exactly one instance"
            + " runs this projection — disable leadership with"
            + " streamrune.subscription.single-active-consumer.enabled=false so it falls back to"
            + " SubscriptionLeadership.NOOP (epoch 0, unfenced).");
  }
}
