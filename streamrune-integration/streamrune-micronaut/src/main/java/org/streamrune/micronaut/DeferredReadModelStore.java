package org.streamrune.micronaut;

import java.util.Objects;
import java.util.function.Supplier;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;

/**
 * The default read-model store as a Micronaut singleton that does not touch the database until it
 * is used: its {@link #name()} is known at once, and the {@code PostgresReadModelStore} behind it —
 * whose creation checks the server version, migrates and validates — is created by the first call
 * that needs a part of it ({@link #repository()}, {@link #checkpoints()}, {@link #processor()}).
 *
 * <p>Micronaut has no lazy singleton: a context started with {@code eagerInitSingletons(true)}
 * creates every {@code @Singleton}, the default store's too. Replacing the default store is by
 * name, so a replaced default store must still be creatable as a bean without touching its
 * database; deferring the creation behind this bean keeps that promise in every start mode.
 * Creation runs once; a failure is thrown to every caller and retried by the next.
 */
final class DeferredReadModelStore implements ReadModelStore {

  private final String name;
  private final Supplier<? extends ReadModelStore> create;
  private volatile ReadModelStore created;

  DeferredReadModelStore(String name, Supplier<? extends ReadModelStore> create) {
    this.name = Objects.requireNonNull(name, "name is required");
    this.create = Objects.requireNonNull(create, "create is required");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public ProjectionRepository repository() {
    return store().repository();
  }

  @Override
  public OffsetStore checkpoints() {
    return store().checkpoints();
  }

  @Override
  public AtomicBatchProcessor processor() {
    return store().processor();
  }

  /** The store behind this one, created on the first call. */
  ReadModelStore store() {
    ReadModelStore store = created;
    if (store == null) {
      synchronized (this) {
        store = created;
        if (store == null) {
          store = Objects.requireNonNull(create.get(), "the default store was not created");
          created = store;
        }
      }
    }
    return store;
  }

  @Override
  public String toString() {
    return "DeferredReadModelStore[" + name + "]";
  }
}
