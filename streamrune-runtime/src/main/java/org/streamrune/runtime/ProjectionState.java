package org.streamrune.runtime;

/** State of a projection managed by {@link MultiProjectionRunner}. */
public enum ProjectionState {
  PENDING,
  CATCHING_UP,
  LIVE,
  /**
   * The runner is a standby: another instance holds single-active-consumer leadership for this
   * projection name, so this runner does not read, process, or advance the offset. It periodically
   * retries to become leader and takes over when the current leader dies.
   */
  STANDBY,
  ERROR,
  STOPPED
}
