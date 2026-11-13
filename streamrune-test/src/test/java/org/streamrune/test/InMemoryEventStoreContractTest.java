package org.streamrune.test;

import org.streamrune.core.EventStore;

/**
 * The event-store contract against {@link InMemoryEventStore} with a command inbox and no saga
 * store: it cannot fence a saga's forward command, which {@code InMemorySagaForwardFenceTest}
 * covers with one.
 */
class InMemoryEventStoreContractTest extends EventStoreContract {
  @Override
  protected EventStore newStore() {
    return new InMemoryEventStore().withCommandInbox(new InMemoryCommandInbox());
  }
}
