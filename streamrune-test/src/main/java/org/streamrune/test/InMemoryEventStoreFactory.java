package org.streamrune.test;

import java.util.Objects;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;

/**
 * Factory for creating {@link InMemoryEventStore} instances, for wiring that asks for an {@link
 * EventStoreFactory}. Each store serializes with the given {@link EventTypeRegistry} and optional
 * {@link CryptoEngine}, as {@code PostgresEventStoreFactory} configures its stores.
 */
public final class InMemoryEventStoreFactory implements EventStoreFactory {

  private final EventTypeRegistry typeRegistry;
  private final CryptoEngine cryptoEngine;

  private InMemoryEventStoreFactory(EventTypeRegistry typeRegistry, CryptoEngine cryptoEngine) {
    this.typeRegistry = typeRegistry;
    this.cryptoEngine = cryptoEngine;
  }

  /**
   * Starts a factory whose stores resolve event and state types through {@code typeRegistry}.
   *
   * @param typeRegistry resolves event and state classes on read (required)
   * @return the builder
   */
  public static Builder builder(EventTypeRegistry typeRegistry) {
    return new Builder(Objects.requireNonNull(typeRegistry, "typeRegistry is required"));
  }

  /** Builder for {@link InMemoryEventStoreFactory}. */
  public static final class Builder {

    private final EventTypeRegistry typeRegistry;
    private CryptoEngine cryptoEngine;

    private Builder(EventTypeRegistry typeRegistry) {
      this.typeRegistry = typeRegistry;
    }

    /**
     * Sets the crypto engine for {@code @Encrypted} field handling.
     *
     * @param cryptoEngine the engine, or {@code null} for none
     * @return this builder
     */
    public Builder cryptoEngine(CryptoEngine cryptoEngine) {
      this.cryptoEngine = cryptoEngine;
      return this;
    }

    /** Builds the factory. */
    public InMemoryEventStoreFactory build() {
      return new InMemoryEventStoreFactory(typeRegistry, cryptoEngine);
    }
  }

  @Override
  public EventStore create() {
    return new InMemoryEventStore(typeRegistry, cryptoEngine);
  }
}
