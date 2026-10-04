package org.streamrune.test;

import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;

class InMemorySagaDeadLetterStoreContractTest extends SagaDeadLetterStoreContract {
  @Override
  protected SagaStore newSagaStore() {
    return new InMemorySagaStore();
  }

  @Override
  protected SagaDeadLetterStore newStore(SagaStore sagaStore) {
    return new InMemorySagaDeadLetterStore(sagaStore);
  }
}
