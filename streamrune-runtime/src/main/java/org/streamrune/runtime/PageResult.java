package org.streamrune.runtime;

/**
 * What a page of keyed items came to.
 *
 * @param items the items of the page
 * @param groups the distinct keys of the page
 * @param failed how many items failed, each counted once; a failure of the observer or of the stop
 *     rule at an item counts as that item's, also when its step succeeded or never ran (that item
 *     is then in {@code notStarted} as well)
 * @param notStarted how many items were never handed to the step
 * @param failure the failure to throw: the failed item with the lowest position, the others
 *     suppressed (at most 8); an Error wins over any exception; null when no item failed, the
 *     caller was not interrupted and every worker's task returned
 * @param failurePosition the position in the page of the item whose failure is thrown; -1 when no
 *     item failed or when the failure belongs to no item: the interrupt, or a worker whose task
 *     never returned
 * @param stoppedBy the failure on which the page stopped starting items; null when it did not stop
 *     (always null on the inline paths, which never ask the rule)
 * @param interrupted whether the calling thread was interrupted while it waited for the workers
 *     (failure is then the IllegalStateException at position -1, unless an Error won)
 * @param stepNanos the time spent in steps, summed over all items
 * @param wallNanos the time the page took
 */
record PageResult(
    int items,
    int groups,
    int failed,
    int notStarted,
    Throwable failure,
    int failurePosition,
    Throwable stoppedBy,
    boolean interrupted,
    long stepNanos,
    long wallNanos) {

  /**
   * Throws {@link #failure} as it was thrown when there is one — an {@link Error}, a {@link
   * RuntimeException}, or a checked exception a step threw past a signature that does not declare
   * it, which is not wrapped.
   */
  void rethrow() {
    if (failure != null) {
      throw PageResult.<RuntimeException>unchanged(failure);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T unchanged(Throwable failure) throws T {
    throw (T) failure;
  }
}
