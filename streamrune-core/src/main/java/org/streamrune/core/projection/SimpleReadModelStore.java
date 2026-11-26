package org.streamrune.core.projection;

import java.util.Objects;

/** The store behind {@link ReadModelStore#of}. */
record SimpleReadModelStore(
    String name,
    ProjectionRepository repository,
    OffsetStore checkpoints,
    AtomicBatchProcessor processor)
    implements ReadModelStore {

  SimpleReadModelStore {
    Objects.requireNonNull(name, "name is required");
    Objects.requireNonNull(repository, "repository is required");
    Objects.requireNonNull(checkpoints, "checkpoints is required");
    Objects.requireNonNull(processor, "processor is required");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (ReadModelStore.DEFAULT.equals(name)) {
      throw new IllegalArgumentException(
          "'default' is reserved: it names the backend's default read-model store");
    }
  }
}
