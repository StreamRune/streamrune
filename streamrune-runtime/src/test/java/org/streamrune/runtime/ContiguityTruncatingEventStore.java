package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Read-only {@link EventStore} test double that reproduces {@code
 * PostgresEventStore.readGlobalStream}'s contiguity guard over a stream containing permanent offset
 * holes: from {@code afterOffset} it returns only the contiguous prefix, stopping at the first
 * internal gap. Events are supplied already carrying their (possibly non-contiguous) global
 * offsets.
 */
final class ContiguityTruncatingEventStore implements EventStore {

  private final List<EventEnvelope> events;

  ContiguityTruncatingEventStore(List<EventEnvelope> events) {
    this.events =
        events.stream().sorted(Comparator.comparingLong(e -> e.globalOffset().value())).toList();
  }

  @Override
  public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
    List<EventEnvelope> out = new ArrayList<>();
    long expectedNext = -1;
    for (EventEnvelope e : events) {
      long offset = e.globalOffset().value();
      if (offset <= afterOffset.value()) {
        continue;
      }
      if (out.size() >= maxCount) {
        break;
      }
      if (expectedNext == -1) {
        expectedNext = offset; // seed from the first row past the checkpoint (skips a leading gap)
      } else if (offset != expectedNext) {
        break; // internal hole → return the contiguous prefix only
      }
      out.add(e);
      expectedNext++;
    }
    return out;
  }

  @Override
  public AggregateHistory load(StreamId streamId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AppendResult append(StreamId streamId, List<EventEnvelope> evts, Version expectedVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
    throw new UnsupportedOperationException();
  }
}
