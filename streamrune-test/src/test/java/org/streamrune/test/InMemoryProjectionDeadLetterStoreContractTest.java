package org.streamrune.test;

import org.streamrune.core.projection.ProjectionDeadLetterStore;

class InMemoryProjectionDeadLetterStoreContractTest extends ProjectionDeadLetterStoreContract {
  @Override
  protected ProjectionDeadLetterStore newStore() {
    return new InMemoryProjectionDeadLetterStore();
  }
}
