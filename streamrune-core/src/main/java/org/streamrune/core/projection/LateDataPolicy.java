package org.streamrune.core.projection;

/**
 * Policy for handling events whose event-time falls in a window that has already closed past its
 * grace period.
 */
public enum LateDataPolicy {
  /** Drop late events; do not re-open the window or re-emit. */
  DROP,
  /**
   * Re-open the window state, accumulate the late event, and re-emit with {@code isFinal=false}.
   */
  REOPEN
}
