package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.EventType;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.UnregisteredSealedTypes;

/** What an event store learns from {@link EncryptedTypeGuard#check} when it is built. */
class EncryptedTypeGuardTest {

  record Registered(String userId, @Encrypted(subjectId = "userId") String email)
      implements DomainEvent {}

  record Plain(String id) implements DomainEvent {}

  record NoSuchSubject(String id, @Encrypted(subjectId = "missing") String email)
      implements DomainEvent {}

  private static EventTypeRegistry registryOf(Class<? extends DomainEvent> type) {
    return SimpleEventTypeRegistry.builder().registerEvent(type).build();
  }

  /** A registry that resolves but does not list its types. */
  private static final EventTypeRegistry UNLISTED =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          return Plain.class;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          return Plain.class;
        }
      };

  @Test
  void aNullRegistryIsRefused() {
    var refused =
        assertThrows(
            NullPointerException.class,
            () -> EncryptedTypeGuard.check(null, new InMemoryCryptoEngine()));

    assertTrue(refused.getMessage().contains("typeRegistry"), refused.getMessage());
  }

  @Test
  void withoutAnEngineAnEncryptedTypeIsRefusedNamingTheField() {
    var refused =
        assertThrows(
            IllegalStateException.class,
            () -> EncryptedTypeGuard.check(registryOf(Registered.class), null));

    assertTrue(refused.getMessage().contains("Registered.email"), refused.getMessage());
  }

  @Test
  void withoutAnEngineAPlainRegistryIsAccepted() {
    assertDoesNotThrow(() -> EncryptedTypeGuard.check(registryOf(Plain.class), null));
  }

  @Test
  void withAnEngineAnEncryptedTypeIsAccepted() {
    assertDoesNotThrow(
        () -> EncryptedTypeGuard.check(registryOf(Registered.class), new InMemoryCryptoEngine()));
  }

  @Test
  void withoutAnEngineARegistryThatDoesNotListItsTypesIsRefused() {
    var refused =
        assertThrows(
            UnsupportedOperationException.class, () -> EncryptedTypeGuard.check(UNLISTED, null));

    assertTrue(refused.getMessage().contains("registeredTypes()"), refused.getMessage());
  }

  @Test
  void withAnEngineARegistryThatDoesNotListItsTypesIsAccepted() {
    assertDoesNotThrow(() -> EncryptedTypeGuard.check(UNLISTED, new InMemoryCryptoEngine()));
  }

  @Test
  void withoutAnEngineAMalformedEncryptedDeclarationIsRefused() {
    assertThrows(
        CryptoMappingException.class,
        () -> EncryptedTypeGuard.check(registryOf(NoSuchSubject.class), null));
  }

  @Test
  void aSealedTypeWhosePermittedSubclassesCannotBeReadIsRefusedWithAndWithoutAnEngine() {
    Class<?> unreadable = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());
    EventTypeRegistry registry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return unreadable;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return unreadable;
          }

          @Override
          public Collection<Class<?>> registeredTypes() {
            return java.util.List.of(unreadable);
          }
        };

    assertThrows(IllegalStateException.class, () -> EncryptedTypeGuard.check(registry, null));
    assertThrows(
        IllegalStateException.class,
        () -> EncryptedTypeGuard.check(registry, new InMemoryCryptoEngine()));
  }
}
