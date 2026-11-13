package org.streamrune.test;

import org.streamrune.core.EventStore;

/**
 * The event-store contract against {@link InMemoryEventStore} in serializing mode, with a command
 * inbox and no saga store: it cannot fence a saga's forward command, which {@code
 * InMemorySagaForwardFenceTest} covers with one.
 */
class InMemoryEventStoreContractTest extends EventStoreContract {

  @Override
  protected EventStore newStore(Setup setup) {
    return configured(InMemoryEventStore.serializing(setup.registry(), setup.crypto()), setup)
        .withCommandInbox(new InMemoryCommandInbox());
  }

  /** A store over the same events, snapshots and inbox rows; the inbox comes along with them. */
  @Override
  protected EventStore reopen(EventStore store, Setup setup) {
    return configured(
        InMemoryEventStore.sharingStorageOf(
            (InMemoryEventStore) store, setup.registry(), setup.crypto()),
        setup);
  }

  static InMemoryEventStore configured(InMemoryEventStore store, Setup setup) {
    return store.withUpcasters(setup.upcasters()).withMigrations(setup.migrations());
  }
}
