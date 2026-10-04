package org.streamrune.runtime;

/** State of a projection managed by {@link ScheduledProjectionRunner}. */
public enum ScheduledProjectionState {
  PENDING,
  SLEEPING,
  RUNNING,
  STOPPED,
  ERROR
}
