package org.streamrune.runtime;

/**
 * Decides whether a page of keyed items starts no further item after a failure.
 *
 * <p>The executor calls it on the worker thread that ran the failed item, so several calls can run
 * at the same time; an implementation is thread-safe. It is not asked for an {@link Error}, which
 * always stops the page, nor on the inline paths (one worker, or one key), which end at the first
 * failure. An exception from the rule fails and stops the page.
 */
@FunctionalInterface
interface PageStopRule {

  /**
   * Whether the page starts no further item after this failure, the n-th of the page.
   *
   * @param failure the failure of the item
   * @param failuresInPage how many items of the page have failed so far, this one included (n ≥ 1)
   * @return {@code true} to start no further item
   */
  boolean stopsPage(Throwable failure, int failuresInPage);
}
