package org.streamrune.core;

/**
 * Factory for creating EventStore instances based on configuration.
 *
 * <p>Each backend has its own: {@code PostgresEventStoreFactory} in {@code streamrune-postgres} and
 * {@code SqliteEventStoreFactory} in {@code streamrune-sqlite}. A {@link
 * org.streamrune.core.backend.BackendStores} bundle hands out the factory of its backend.
 */
public interface EventStoreFactory {

  /**
   * Creates an EventStore instance.
   *
   * @return configured EventStore
   */
  EventStore create();
}
