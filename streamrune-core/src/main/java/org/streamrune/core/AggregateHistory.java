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
 */
public record AggregateHistory(
    AggregateState snapshotState,
    List<EventEnvelope> events,
    Version version,
    Version lastSnapshotVersion) {

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
  }

  /** Returns an empty history with no snapshot, no events, version 0, and lastSnapshotVersion 0. */
  public static AggregateHistory empty() {
    return new AggregateHistory(null, List.of(), Version.initial(), Version.initial());
  }
}
