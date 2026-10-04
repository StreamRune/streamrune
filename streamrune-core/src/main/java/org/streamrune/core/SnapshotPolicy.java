package org.streamrune.core;

/**
 * Determines when the framework should persist an aggregate snapshot.
 *
 * <p>This interface is deliberately sealed: the command bus and event store must understand every
 * policy variant to act on it (e.g. they pattern-match on {@link EveryNEvents} to decide when to
 * snapshot and which snapshot schema version to expect), so user-defined implementations could not
 * work. New strategies (time-based, state-size-based, predicate-based) require a new variant here
 * plus matching runtime support, and ship with a framework release.
 */
public sealed interface SnapshotPolicy permits SnapshotPolicy.EveryNEvents, SnapshotPolicy.Never {

  /**
   * Snapshot after every {@code n} events since the last snapshot.
   *
   * @param n the event count threshold (must be >= 1)
   * @param snapshotVersion the schema version of the snapshot state (must be >= 1). When the
   *     aggregate state class structure changes, increment this value so that snapshots saved with
   *     the old structure are automatically ignored and the aggregate is rebuilt from events.
   */
  record EveryNEvents(int n, int snapshotVersion) implements SnapshotPolicy {
    public EveryNEvents {
      if (n < 1) {
        throw new IllegalArgumentException("n must be >= 1");
      }
      if (snapshotVersion < 1) {
        throw new IllegalArgumentException("snapshotVersion must be >= 1");
      }
    }
  }

  /** Never create snapshots. */
  record Never() implements SnapshotPolicy {}

  /** Creates a policy that snapshots every {@code n} events using snapshot schema version 1. */
  static SnapshotPolicy everyNEvents(int n) {
    return new EveryNEvents(n, 1);
  }

  /**
   * Creates a policy that snapshots every {@code n} events using the given snapshot schema version.
   * Increment {@code snapshotVersion} whenever the aggregate state class structure changes.
   */
  static SnapshotPolicy everyNEvents(int n, int snapshotVersion) {
    return new EveryNEvents(n, snapshotVersion);
  }

  /** Creates a policy that never snapshots. */
  static SnapshotPolicy never() {
    return new Never();
  }
}
