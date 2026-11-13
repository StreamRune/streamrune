package org.streamrune.test;

import org.streamrune.core.projection.OffsetStore;

class InMemoryOffsetStoreContractTest extends OffsetStoreContract {
  @Override
  protected OffsetStore newStore() {
    return new InMemoryOffsetStore();
  }
}
