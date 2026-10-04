package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.List;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Minimal read-only {@link EventStore} for unit tests. Takes a list of {@link EventEnvelope} and
 * assigns sequential {@link GlobalOffset} values starting from 1. Only implements {@link
 * #readGlobalStream(GlobalOffset, int)}; all other methods throw {@link
 * UnsupportedOperationException}.
 */
final class SimpleTestEventStore implements EventStore {

  private final List<EventEnvelope> events;

  private SimpleTestEventStore(List<EventEnvelope> events) {
    this.events = List.copyOf(events);
  }

  /**
   * Creates a store pre-populated with the given events. Each event is rewrapped with a sequential
   * {@link GlobalOffset} starting at 1, preserving all other envelope fields.
   */
  static SimpleTestEventStore of(List<EventEnvelope> source) {
    List<EventEnvelope> reindexed = new ArrayList<>(source.size());
    long offset = 1L;
    for (EventEnvelope e : source) {
      reindexed.add(
          new EventEnvelope(
              GlobalOffset.of(offset++),
              e.streamId(),
              e.version(),
              e.eventType(),
              e.event(),
              e.metadata()));
    }
    return new SimpleTestEventStore(reindexed);
  }

  @Override
  public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
    return events.stream()
        .filter(e -> e.globalOffset().value() > afterOffset.value())
        .limit(maxCount)
        .toList();
  }

  @Override
  public AggregateHistory load(StreamId streamId) {
    throw new UnsupportedOperationException("SimpleTestEventStore does not support load");
  }

  @Override
  public AppendResult append(StreamId streamId, List<EventEnvelope> evts, Version expectedVersion) {
    throw new UnsupportedOperationException("SimpleTestEventStore does not support append");
  }

  @Override
  public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
    throw new UnsupportedOperationException("SimpleTestEventStore does not support saveSnapshot");
  }

  @Override
  public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
    throw new UnsupportedOperationException("SimpleTestEventStore does not support readStream");
  }
}
