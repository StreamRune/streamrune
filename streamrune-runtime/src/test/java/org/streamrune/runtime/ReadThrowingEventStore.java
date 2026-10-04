package org.streamrune.runtime;

import java.util.List;
import java.util.function.Supplier;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Read-only {@link EventStore} test double whose {@link #readGlobalStream} always throws a supplied
 * exception, counting the attempts. Used to drive the read-poison path — e.g. a store that throws
 * {@code UnknownEventTypeException} on every read.
 */
final class ReadThrowingEventStore implements EventStore {

  private final Supplier<? extends RuntimeException> error;
  private final java.util.concurrent.atomic.AtomicInteger reads =
      new java.util.concurrent.atomic.AtomicInteger();

  ReadThrowingEventStore(Supplier<? extends RuntimeException> error) {
    this.error = error;
  }

  int readAttempts() {
    return reads.get();
  }

  @Override
  public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
    reads.incrementAndGet();
    throw error.get();
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
