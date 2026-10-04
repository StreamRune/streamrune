package org.streamrune.test;

import java.time.Clock;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStore;

class InMemoryOutboxStoreContractTest extends OutboxStoreContract {
  @Override
  protected OutboxStore newStore(OutboxOrderingMode mode) {
    return new InMemoryOutboxStore(
        Clock.systemUTC(), InMemoryOutboxStore.DEFAULT_CLAIM_LEASE, mode);
  }

  @Override
  protected OutboxStore sameDataWithMode(OutboxStore store, OutboxOrderingMode mode) {
    return ((InMemoryOutboxStore) store).withOrderingMode(mode);
  }
}
