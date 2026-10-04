package org.streamrune.core;

/**
 * Factory for creating EventStore instances based on configuration.
 *
 * <p>Supports different backend implementations (PostgreSQL, MongoDB, etc.)
 */
public interface EventStoreFactory {

  /**
   * Creates an EventStore instance.
   *
   * @return configured EventStore
   */
  EventStore create();
}
