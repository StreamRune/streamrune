package org.streamrune.core.outbox;

/**
 * How one outbox channel orders delivery within an aggregate once an entry has terminally failed.
 */
public enum OutboxOrderingMode {
  /**
   * Default. Delivery within an aggregate is in strict seq order and a terminal FAILED head BLOCKS
   * every later entry of its aggregate until an operator replays or skips it. streamId is
   * mandatory: an entry without a stream is rejected at save time and the append fails.
   */
  STRICT_PER_AGGREGATE,
  /**
   * Explicit opt-in for channels whose consumers tolerate a gap and a reorder after a terminal
   * failure (telemetry, notifications, best-effort fan-out). A FAILED head does not hold its
   * aggregate; a later replay is delivered after already-delivered successors. streamId may be
   * null. Never the right choice for cross-context state changes, inventory, payments or cache
   * invalidation.
   */
  AVAILABILITY_FIRST
}
