package org.streamrune.test;

import java.util.List;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The event-store contract against a store that implements only the abstract methods of {@link
 * EventStore} and keeps no snapshots: every capability the javadoc lets a store leave out is left
 * out, so the contract's capability hooks skip those cases and check the refusals instead.
 */
class MinimalEventStoreContractTest extends EventStoreContract {

  @Override
  protected EventStore newStore(Setup setup) {
    return new MinimalEventStore(
        InMemoryEventStoreContractTest.configured(
            new InMemoryEventStore(setup.registry(), setup.crypto()), setup));
  }

  /** A minimal store over the events of {@code store}'s in-memory store. */
  @Override
  protected EventStore reopen(EventStore store, Setup setup) {
    var events = ((MinimalEventStore) store).events();
    return new MinimalEventStore(
        InMemoryEventStoreContractTest.configured(
            InMemoryEventStore.sharingStorageOf(events, setup.registry(), setup.crypto()), setup));
  }

  @Override
  protected boolean supportsIdempotentAppend() {
    return false;
  }

  @Override
  protected boolean supportsSnapshots() {
    return false;
  }

  @Override
  protected boolean supportsSnapshotSchemaVersions() {
    return false;
  }

  @Override
  protected boolean supportsLastGlobalOffset() {
    return false;
  }

  /** The in-memory storage belongs to the process that holds it: there is no second instance. */
  @Override
  protected boolean supportsMultipleInstances() {
    return false;
  }

  /** Events through an in-memory store; every default method of the interface left in place. */
  private record MinimalEventStore(InMemoryEventStore events) implements EventStore {

    @Override
    public AggregateHistory load(StreamId streamId) {
      return events.load(streamId);
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> envelopes, Version expectedVersion) {
      return events.append(streamId, envelopes, expectedVersion);
    }

    @Override
    public boolean saveSnapshot(
        StreamId streamId,
        Version version,
        AggregateState state,
        int snapshotVersion,
        long snapshotPurgeGeneration) {
      throw new UnsupportedOperationException("this store keeps no snapshots");
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return events.readGlobalStream(afterOffset, maxCount);
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return events.readStream(streamId, afterVersion, maxCount);
    }
  }
}
