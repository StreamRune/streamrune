package org.streamrune.test;

import org.streamrune.core.saga.SagaStore;

class InMemorySagaStoreContractTest extends SagaStoreContract {
  @Override
  protected SagaStore newStore() {
    return new InMemorySagaStore();
  }
}
