package org.streamrune.core;

import java.util.List;
import org.streamrune.core.types.Version;

/**
 * Result of loading an aggregate stream from the event store.
 *
 * @param snapshotState the deserialized snapshot state, or {@code null} if no snapshot exists
 * @param events events after the snapshot (or all events if no snapshot); never null
 * @param version the latest version of the stream
 * @param lastSnapshotVersion the version at which the snapshot was taken, or {@link
 *     Version#initial()} if no snapshot exists
 * @param snapshotPurgeGeneration how many times the stream's snapshot had been purged when it was
 *     loaded (see {@link org.streamrune.core.gdpr.SnapshotPurger}); {@code 0} for a stream never
 *     purged and for a store that keeps no purge record. A snapshot taken from this history is
 *     saved with it, and the store refuses the save if a purge ran since the load (see {@link
 *     EventStore#saveSnapshot(org.streamrune.core.types.StreamId, Version, AggregateState, int,
 *     long)}).
 */
public record AggregateHistory(
    AggregateState snapshotState,
    List<EventEnvelope> events,
    Version version,
    Version lastSnapshotVersion,
    long snapshotPurgeGeneration) {

  public AggregateHistory {
    if (events == null) {
      throw new IllegalArgumentException("events is required");
    }
    events = List.copyOf(events);
    if (version == null) {
      throw new IllegalArgumentException("version is required");
    }
    if (lastSnapshotVersion == null) {
      throw new IllegalArgumentException("lastSnapshotVersion is required");
    }
    if (snapshotPurgeGeneration < 0) {
      throw new IllegalArgumentException(
          "snapshotPurgeGeneration must not be negative, got " + snapshotPurgeGeneration);
    }
  }

  /**
   * A history of a stream whose snapshot was never purged: {@link #snapshotPurgeGeneration()} is
   * {@code 0}.
   */
  public AggregateHistory(
      AggregateState snapshotState,
      List<EventEnvelope> events,
      Version version,
      Version lastSnapshotVersion) {
    this(snapshotState, events, version, lastSnapshotVersion, 0);
  }

  /** Returns an empty history with no snapshot, no events, version 0, and lastSnapshotVersion 0. */
  public static AggregateHistory empty() {
    return new AggregateHistory(null, List.of(), Version.initial(), Version.initial());
  }
}
