package org.streamrune.test;

import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;

/** Factory for creating in-memory EventStore instances for testing. */
public class InMemoryEventStoreFactory implements EventStoreFactory {

  private InMemoryEventStoreFactory() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    public InMemoryEventStoreFactory build() {
      return new InMemoryEventStoreFactory();
    }
  }

  @Override
  public EventStore create() {
    return new InMemoryEventStore();
  }
}
