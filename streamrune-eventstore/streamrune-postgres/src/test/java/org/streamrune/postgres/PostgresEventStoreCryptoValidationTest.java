package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.SubjectId;

/**
 * The plain-Java construction paths of {@link PostgresEventStore} — the static {@code create(...)}
 * the QUICKSTART uses and {@link PostgresEventStore#builder()} — refuse an event or state type that
 * carries {@code @Encrypted} when no {@link CryptoEngine} is configured, exactly as {@link
 * PostgresEventStoreFactory#create()} does. Without the check the store is built with a mapper that
 * has no crypto module, and the first append writes the personal data as plaintext into the
 * append-only log with no key ever minted, so no later erasure can reach it.
 *
 * <p>The refusals use a {@link DataSource} mock with no stubbing and assert it was never touched:
 * the check runs before the server-version probe, so a misconfigured application fails without a
 * database.
 */
class PostgresEventStoreCryptoValidationTest {

  record UserRegistered(String userId, @Encrypted(subjectId = "userId") String email) {}

  record OrderCreated(String orderId) {}

  record Address(String ownerId, @Encrypted(subjectId = "ownerId") String street) {}

  record CustomerMoved(String customerId, Address address) {}

  record CustomerState(String customerId, @Encrypted(subjectId = "customerId") String phone) {}

  record CustomerWithPii(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  /** Writes the nested record's fields itself, so the encrypting writers never run. */
  static final class PlaintextCustomerSerializer
      extends com.fasterxml.jackson.databind.JsonSerializer<CustomerWithPii> {
    @Override
    public void serialize(
        CustomerWithPii value,
        com.fasterxml.jackson.core.JsonGenerator gen,
        com.fasterxml.jackson.databind.SerializerProvider provider)
        throws java.io.IOException {
      gen.writeStartObject();
      gen.writeStringField("email", value.email());
      gen.writeEndObject();
    }
  }

  record OrderPlacedWithOverride(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          CustomerWithPii customer) {}

  /** A non-null engine; these tests never encrypt anything. */
  private static final class NoOpCryptoEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return plaintext;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      return ciphertext;
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  /** A mock whose connection reports a supported server, so {@code create(...)} can finish. */
  private static DataSource supportedServer() {
    try {
      DataSource dataSource = mock(DataSource.class);
      Connection connection = mock(Connection.class);
      DatabaseMetaData meta = mock(DatabaseMetaData.class);
      when(dataSource.getConnection()).thenReturn(connection);
      when(connection.getMetaData()).thenReturn(meta);
      when(meta.getDatabaseMajorVersion()).thenReturn(PostgresServerVersion.MINIMUM_MAJOR_VERSION);
      when(meta.getDatabaseProductVersion()).thenReturn("17.0");
      return dataSource;
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
  }

  private static EventTypeRegistry registryOf(Class<?>... eventTypes) {
    var builder = SimpleEventTypeRegistry.builder();
    for (Class<?> type : eventTypes) {
      builder.registerEvent(type);
    }
    return builder.build();
  }

  /** A hand-written registry that does not override {@code registeredTypes()}. */
  private static EventTypeRegistry registryThatCannotListItsTypes() {
    return new EventTypeRegistry() {
      @Override
      public Class<?> resolveEventType(EventType eventType) {
        return UserRegistered.class;
      }

      @Override
      public Class<?> resolveStateType(String stateType) {
        throw new UnsupportedOperationException("no state types registered");
      }
    };
  }

  private static void assertNamesTheField(IllegalStateException ex, String type, String field) {
    assertTrue(ex.getMessage().contains(type), ex.getMessage());
    assertTrue(ex.getMessage().contains(field), ex.getMessage());
    assertTrue(ex.getMessage().contains("CryptoEngine"), ex.getMessage());
  }

  private static List<String> validatorWarningsWhile(Runnable body) {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger("org.streamrune.crypto.CryptoConfigValidator");
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }

  // ── static create(...) ──────────────────────────────────────────────────────────────────────

  @Test
  void create_refusesAnEncryptedEventWithoutAnEngine_beforeTouchingTheDatabase() {
    DataSource dataSource = mock(DataSource.class);

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                PostgresEventStore.create(
                    dataSource, registryOf(UserRegistered.class), null, List.of()));

    assertNamesTheField(ex, "UserRegistered", "email");
    verifyNoInteractions(dataSource);
  }

  @Test
  void createWithMetrics_refusesAnEncryptedEventWithoutAnEngine_beforeTouchingTheDatabase() {
    DataSource dataSource = mock(DataSource.class);

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                PostgresEventStore.create(
                    dataSource,
                    registryOf(UserRegistered.class),
                    null,
                    List.of(),
                    StreamRuneMetrics.NOOP));

    assertNamesTheField(ex, "UserRegistered", "email");
    verifyNoInteractions(dataSource);
  }

  @Test
  void create_refusesAnEncryptedComponentNestedInAnEvent() {
    DataSource dataSource = mock(DataSource.class);

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                PostgresEventStore.create(
                    dataSource, registryOf(CustomerMoved.class), null, List.of()));

    assertNamesTheField(ex, "Address", "street");
    verifyNoInteractions(dataSource);
  }

  @Test
  void create_refusesAnEncryptedStateTypeWithoutAnEngine() {
    DataSource dataSource = mock(DataSource.class);
    EventTypeRegistry registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent(OrderCreated.class)
            .registerState("CustomerState", CustomerState.class)
            .build();

    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> PostgresEventStore.create(dataSource, registry, null, List.of()));

    assertNamesTheField(ex, "CustomerState", "phone");
    verifyNoInteractions(dataSource);
  }

  @Test
  void create_refusesARegistryThatCannotListItsTypes_whenNoEngineIsConfigured() {
    DataSource dataSource = mock(DataSource.class);

    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                PostgresEventStore.create(
                    dataSource, registryThatCannotListItsTypes(), null, List.of()));

    assertTrue(ex.getMessage().contains("registeredTypes"), ex.getMessage());
    verifyNoInteractions(dataSource);
  }

  @Test
  void create_buildsTheStoreForAnEncryptedEventWhenAnEngineIsConfigured() {
    var store =
        PostgresEventStore.create(
            supportedServer(), registryOf(UserRegistered.class), new NoOpCryptoEngine(), List.of());

    assertNotNull(store);
  }

  @Test
  void create_buildsTheStoreForARegistryThatCannotListItsTypes_whenAnEngineIsConfigured() {
    var store =
        PostgresEventStore.create(
            supportedServer(), registryThatCannotListItsTypes(), new NoOpCryptoEngine(), List.of());

    assertNotNull(store);
  }

  @Test
  void create_buildsTheStoreWithoutAnEngineWhenNoTypeCarriesEncrypted() {
    var store =
        PostgresEventStore.create(supportedServer(), registryOf(OrderCreated.class), null, null);

    assertNotNull(store);
  }

  @Test
  void create_warnsAboutASerializerOverrideOverAnEncryptedComponent_whenAnEngineIsConfigured() {
    var warnings =
        validatorWarningsWhile(
            () ->
                PostgresEventStore.create(
                    supportedServer(),
                    registryOf(OrderPlacedWithOverride.class),
                    new NoOpCryptoEngine(),
                    List.of()));

    assertEquals(1, warnings.size(), warnings.toString());
    assertTrue(warnings.getFirst().contains("OrderPlacedWithOverride"), warnings.toString());
  }

  // ── builder() ───────────────────────────────────────────────────────────────────────────────

  @Test
  void builder_refusesAnEncryptedEventWithoutAnEngine_beforeTouchingTheDatabase() {
    DataSource dataSource = mock(DataSource.class);
    var builder =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(registryOf(UserRegistered.class));

    var ex = assertThrows(IllegalStateException.class, builder::build);

    assertNamesTheField(ex, "UserRegistered", "email");
    verifyNoInteractions(dataSource);
  }

  @Test
  void builder_refusesARegistryThatCannotListItsTypes_whenNoEngineIsConfigured() {
    var builder =
        PostgresEventStore.builder()
            .dataSource(mock(DataSource.class))
            .typeRegistry(registryThatCannotListItsTypes());

    var ex = assertThrows(UnsupportedOperationException.class, builder::build);

    assertTrue(ex.getMessage().contains("registeredTypes"), ex.getMessage());
  }

  @Test
  void builder_buildsTheStoreForAnEncryptedEventWhenAnEngineIsConfigured() {
    var store =
        PostgresEventStore.builder()
            .dataSource(mock(DataSource.class))
            .typeRegistry(registryOf(UserRegistered.class))
            .cryptoEngine(new NoOpCryptoEngine())
            .build();

    assertNotNull(store);
  }

  @Test
  void builder_warnsAboutASerializerOverrideOverAnEncryptedComponent_whenAnEngineIsConfigured() {
    var warnings =
        validatorWarningsWhile(
            () ->
                PostgresEventStore.builder()
                    .dataSource(mock(DataSource.class))
                    .typeRegistry(registryOf(OrderPlacedWithOverride.class))
                    .cryptoEngine(new NoOpCryptoEngine())
                    .build());

    assertEquals(1, warnings.size(), warnings.toString());
    assertTrue(warnings.getFirst().contains("OrderPlacedWithOverride"), warnings.toString());
  }

  @Test
  void builder_stillRequiresATypeRegistry() {
    var builder = PostgresEventStore.builder().dataSource(mock(DataSource.class));

    var ex = assertThrows(IllegalArgumentException.class, builder::build);

    assertTrue(ex.getMessage().contains("typeRegistry"), ex.getMessage());
  }
}
