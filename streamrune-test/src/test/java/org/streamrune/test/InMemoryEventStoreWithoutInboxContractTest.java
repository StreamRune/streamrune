package org.streamrune.test;

import org.streamrune.core.EventStore;

/**
 * The event-store contract against {@link InMemoryEventStore} in serializing mode with no command
 * inbox wired: the keyed append is a capability it leaves out, refused with {@code
 * UnsupportedOperationException}.
 */
class InMemoryEventStoreWithoutInboxContractTest extends EventStoreContract {

  @Override
  protected EventStore newStore(Setup setup) {
    return InMemoryEventStoreContractTest.configured(
        InMemoryEventStore.serializing(setup.registry(), setup.crypto()), setup);
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
}
