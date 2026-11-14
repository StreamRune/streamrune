package org.streamrune.crypto;

import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;

/**
 * The check an {@code EventStore} built over an {@link EventTypeRegistry} runs before it stores
 * anything: a registry with an {@code @Encrypted} type and no {@link CryptoEngine} is refused,
 * because the store would write the field as plaintext, with no key ever minted for a later erasure
 * to delete. {@code PostgresEventStore} and {@code InMemoryEventStore} both run it when they are
 * built; a store of your own that serializes events through a {@link CryptoShreddingModule} calls
 * {@link #check} too, so every store refuses the same configuration with the same message.
 *
 * <p>Without an engine, every event and state type the registry lists is scanned (nested records,
 * collection elements and sealed hierarchies included) and an {@code @Encrypted} component throws
 * {@link IllegalStateException} naming every such field; a registry that cannot list its types
 * cannot be cleared, so the {@link UnsupportedOperationException} of {@link
 * EventTypeRegistry#registeredTypes()} propagates. With an engine the components are encrypted, and
 * the scan only warns about a property-level Jackson serializer override that would bypass
 * encryption; it is skipped for a registry that cannot list its types.
 */
public final class EncryptedTypeGuard {

  private static final Logger log = LoggerFactory.getLogger(EncryptedTypeGuard.class);

  private EncryptedTypeGuard() {}

  /**
   * Refuses a configuration that would persist {@code @Encrypted} components as plaintext.
   *
   * @param typeRegistry the registry the store resolves types with (required)
   * @param cryptoEngine the configured engine, or {@code null} for none
   * @throws NullPointerException if {@code typeRegistry} is {@code null}
   * @throws IllegalStateException if {@code cryptoEngine} is {@code null} and a registered type
   *     carries {@code @Encrypted}
   * @throws CryptoMappingException if {@code cryptoEngine} is {@code null} and a registered type
   *     declares {@code @Encrypted} wrongly: a component that is not a {@code String}, or a {@code
   *     subjectId} that names no {@code String} component of the record (with an engine the same
   *     declaration fails at the first append instead)
   * @throws IllegalStateException also, with or without an engine, if a sealed type reachable from
   *     the registered types reports no readable permitted subclasses (a GraalVM native image
   *     without reflection metadata for it), which the scan cannot treat as a leaf
   * @throws UnsupportedOperationException if {@code cryptoEngine} is {@code null} and {@code
   *     typeRegistry} does not implement {@link EventTypeRegistry#registeredTypes()}
   */
  public static void check(EventTypeRegistry typeRegistry, CryptoEngine cryptoEngine) {
    Objects.requireNonNull(typeRegistry, "typeRegistry is required");
    if (cryptoEngine == null) {
      CryptoConfigValidator.validate(typeRegistry.registeredTypes(), null);
      return;
    }
    Collection<Class<?>> registered;
    try {
      registered = typeRegistry.registeredTypes();
    } catch (UnsupportedOperationException e) {
      log.debug(
          "EventTypeRegistry does not list its registered types, so the check for Jackson"
              + " serializer overrides over @Encrypted components is skipped for event and state"
              + " types. Override registeredTypes() to enable it.",
          e);
      return;
    }
    CryptoConfigValidator.validate(registered, cryptoEngine);
  }
}
