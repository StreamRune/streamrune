package org.streamrune.runtime;

import java.util.List;

/**
 * A saga runner's parallelism as of one moment: the value, where it came from, the bound
 * subscription whose property set it (when that is its source) and the names bound, in bind order.
 * Immutable and published once per change.
 *
 * @param value the parallelism the next page uses
 * @param source where the value came from
 * @param winningSubscription the bound name whose property set the value; {@code null} otherwise
 * @param boundNames the bound subscription names, in bind order (unmodifiable)
 */
record SagaParallelismSnapshot(
    int value, SagaParallelismSource source, String winningSubscription, List<String> boundNames) {

  SagaParallelismSnapshot {
    boundNames = List.copyOf(boundNames);
  }

  /** How many pages of this runner can be in flight at once: one per bound subscription. */
  int concurrentPages() {
    return Math.max(1, boundNames.size());
  }
}
