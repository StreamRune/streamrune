package org.streamrune.test;

import org.streamrune.core.DeadLetterQueue;

class InMemoryDeadLetterQueueContractTest extends DeadLetterQueueContract {
  @Override
  protected DeadLetterQueue newQueue() {
    return new InMemoryDeadLetterQueue();
  }
}
