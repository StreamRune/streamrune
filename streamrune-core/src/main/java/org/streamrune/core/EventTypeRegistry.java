package org.streamrune.core;

import org.streamrune.core.types.EventType;

/**
 * Registry that maps string event-type names and state-type names to their Java {@code Class<?>}
 * counterparts. Required by {@code PostgresEventStore} for deserialization. Populate via {@link
 * SimpleEventTypeRegistry.Builder} and pass to {@code PostgresEventStoreFactory}.
 *
 * <p>The registry is read-side only: nothing validates at append time that the {@link EventType}
 * written into an envelope is registered here. A mismatch (e.g. events written with {@code
 * EventType.fromClass} but registered under a different name, or a new event type that was never
 * registered) surfaces as an {@link UnknownEventTypeException} from {@link #resolveEventType} when
 * the event is first read back. To keep write and read names aligned, register classes with {@code
 * SimpleEventTypeRegistry.Builder#registerEvent(Class)} whenever the write path derives names via
 * {@code EventType.fromClass}.
 */
public interface EventTypeRegistry {

  /**
   * Resolves the Java class for the given event type.
   *
   * @throws UnknownEventTypeException if no class is registered under {@code eventType}
   */
  Class<?> resolveEventType(EventType eventType);

  /**
   * Resolves the Java class for the given state type name.
   *
   * @throws UnknownEventTypeException if no class is registered under {@code stateType}
   */
  Class<?> resolveStateType(String stateType);

  /**
   * Returns every event and state class registered here, for startup-time validation (e.g. checking
   * that types with {@code @Encrypted} fields have a {@link
   * org.streamrune.core.crypto.CryptoEngine} configured). The registry remains read-side only: this
   * does not change resolution behavior.
   *
   * <p>Default implementation throws {@link UnsupportedOperationException} rather than returning an
   * empty collection: a silent empty default would let the {@code @Encrypted} plaintext-PII startup
   * guard ({@code CryptoConfigValidator}, run when a {@code PostgresEventStore} is built without a
   * {@code CryptoEngine}) skip validation for any registry that did not override it, exactly the
   * "fail open on a privacy control" this framework's SPI policy forbids (see {@code
   * DeadLetterQueue}'s default-method policy: an optional capability's default fails loudly, never
   * silently). Implementations that back a real registry (see {@link SimpleEventTypeRegistry}) must
   * override this to expose their registered classes. A registry with genuinely nothing to
   * enumerate may explicitly override it to return {@code List.of()} — that is a visible,
   * deliberate choice, unlike inheriting a silent default.
   *
   * @return an unmodifiable collection of registered event and state classes; never {@code null}
   * @throws UnsupportedOperationException if not overridden
   */
  default java.util.Collection<Class<?>> registeredTypes() {
    throw new UnsupportedOperationException(
        "registeredTypes() is not implemented by this EventTypeRegistry, so the @Encrypted"
            + " plaintext-PII startup guard (CryptoConfigValidator, run when a PostgresEventStore"
            + " is built without a CryptoEngine) cannot scan your registered event/state types and"
            + " would otherwise silently skip validation. Override registeredTypes() to return"
            + " every event and state class this registry can resolve (see"
            + " SimpleEventTypeRegistry), or — if this registry genuinely has no types to"
            + " enumerate — override it to explicitly return List.of().");
  }
}
