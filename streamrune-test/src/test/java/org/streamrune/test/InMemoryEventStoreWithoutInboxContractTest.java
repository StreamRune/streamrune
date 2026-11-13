package org.streamrune.test;

import org.streamrune.core.EventStore;

/**
 * The event-store contract against {@link InMemoryEventStore} with no command inbox wired: the
 * keyed append is a capability it leaves out, refused with {@code UnsupportedOperationException}.
 */
class InMemoryEventStoreWithoutInboxContractTest extends EventStoreContract {
  @Override
  protected EventStore newStore() {
    return new InMemoryEventStore();
  }

  @Override
  protected boolean supportsIdempotentAppend() {
    return false;
  }
}
