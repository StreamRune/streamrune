package org.streamrune.core;

import java.util.Objects;

/**
 * Wraps a read model with its current version for optimistic locking.
 *
 * @param data the read model
 * @param version the current version (incremented on each save)
 * @param <T> the read model type
 */
public record Versioned<T>(T data, long version) {

  public Versioned {
    Objects.requireNonNull(data, "data");
    if (version < 0) throw new IllegalArgumentException("version must be >= 0");
  }
}
