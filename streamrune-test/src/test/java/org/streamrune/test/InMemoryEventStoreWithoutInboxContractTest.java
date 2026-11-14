package org.streamrune.test;

import org.streamrune.core.EventStore;

/**
 * The event-store contract against {@link InMemoryEventStore} with no command inbox wired: the
 * keyed append is a capability it leaves out, refused with {@code UnsupportedOperationException}.
 */
class InMemoryEventStoreWithoutInboxContractTest extends EventStoreContract {

  @Override
  protected EventStore newStore(Setup setup) {
    return InMemoryEventStoreContractTest.configured(
        new InMemoryEventStore(setup.registry(), setup.crypto()), setup);
  }

  @Override
  protected EventStore reopen(EventStore store, Setup setup) {
    return InMemoryEventStoreContractTest.configured(
        InMemoryEventStore.sharingStorageOf(
            (InMemoryEventStore) store, setup.registry(), setup.crypto()),
        setup);
  }

  @Override
  protected boolean supportsIdempotentAppend() {
    return false;
  }

  /** The in-memory storage belongs to the process that holds it: there is no second instance. */
  @Override
  protected boolean supportsMultipleInstances() {
    return false;
  }
}
