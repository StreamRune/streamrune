package org.streamrune.runtime;

/**
 * Observes how many steps of a page run and how many have not started yet.
 *
 * <p>The executor calls it on the thread that runs the step, while it holds its counting lock: the
 * calls are ordered and never concurrent, and a slow observer holds up every worker. The call for
 * the end of a step that failed comes after the executor has decided whether the failure stops the
 * page. An exception from the observer fails the page. After a page that stopped, the last call
 * still reports the items that were never started; the caller resets what it publishes.
 */
@FunctionalInterface
interface PageObserver {

  /** Ignores every change. */
  PageObserver NONE = (inFlight, notYetStarted) -> {};

  /**
   * Called when a step starts and when it ends.
   *
   * @param inFlight the steps running now
   * @param notYetStarted the items not handed to a step yet
   */
  void stepsChanged(int inFlight, int notYetStarted);
}
