package org.streamrune.core;

/** Lock mode for projection repository reads. */
public enum LockMode {
  /** Read version for optimistic concurrency check on subsequent save. */
  OPTIMISTIC,
  /** Acquire row-level exclusive lock (SELECT FOR UPDATE). Requires active transaction. */
  PESSIMISTIC
}
