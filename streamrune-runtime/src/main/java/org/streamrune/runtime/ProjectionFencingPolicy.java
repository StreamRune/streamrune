package org.streamrune.runtime;

import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * Single place where the three leadership-aware projection runner builders refuse to assemble a
 * multi-replica projection whose {@link AtomicBatchProcessor} cannot honour the leadership fencing
 * epoch.
 *
 * <p>Leadership issues a strictly-increasing epoch per acquisition and the runners thread it into
 * every commit, but only a <em>transactional</em> processor turns that token into a guarantee: it
 * rejects a commit from a superseded epoch under the checkpoint row's lock, before the updater
 * writes a single read-model row. Paired with a processor that ignores the epoch, the whole
 * protocol is theatre — the takeover stamp reported success from an empty method body, the
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
   * Rejects a real (multi-replica) leadership paired with a processor that cannot fence.
   *
   * @param leadership the configured leadership; {@link SubscriptionLeadership#NOOP} is the
   *     unfenced single-instance contract (epoch {@code 0}) and is always accepted
   * @param processor the configured batch processor
   * @param runnerName the runner type, for the error message
   * @throws IllegalArgumentException if leadership is real and {@code processor} cannot fence
   */
  static void requireFencingCapable(
      SubscriptionLeadership leadership, AtomicBatchProcessor processor, String runnerName) {
    if (leadership == null || leadership == SubscriptionLeadership.NOOP) {
      return;
    }
    if (processor != null && processor.supportsFencing()) {
      return;
    }
    throw new IllegalArgumentException(
        runnerName
            + " was configured with single-active-consumer leadership ("
            + (leadership.getClass().getName())
            + ") but its AtomicBatchProcessor cannot honour the leadership fencing epoch ("
            + (processor == null ? "null" : processor.getClass().getName())
            + "). The epoch fence, the first-offset overlap guard and the atomic checkpoint would"
            + " all be inert: a superseded leader resuming after a GC/VM pause would re-apply its"
            + " in-flight batch to the read model with nothing to reject it, and the offset store's"
            + " monotonic guard would swallow the trailing checkpoint save so nothing would even"
            + " log. Define an AtomicBatchProcessor bean — org.streamrune.postgres"
            + ".JdbcProjectionRepository implements it — and keep leadership on, or — if exactly"
            + " one instance runs this projection — disable leadership with"
            + " streamrune.subscription.single-active-consumer.enabled=false so it falls back to"
            + " SubscriptionLeadership.NOOP (epoch 0, unfenced).");
  }
}
