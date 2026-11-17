package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.SubjectId;

/**
 * Verifies that {@link PostgresEventStoreFactory#create()} fails fast — before ever touching the
 * database — when a registered event/state type has an {@code @Encrypted} field but no {@code
 * CryptoEngine} was configured. This is the startup choke point shared by all three framework
 * integrations (Spring, Quarkus, Micronaut): each builds its event store through this factory, so
 * wiring the {@link org.streamrune.crypto.CryptoConfigValidator} call here covers all of them.
 *
 * <p>Uses a mock {@link DataSource} with no stubbing so any attempt to open a connection (schema
 * init/validation) throws immediately — proving the validation genuinely runs first, not just that
 * it eventually runs.
 */
class PostgresEventStoreFactoryCryptoValidationTest {

  record UserRegistered(String userId, @Encrypted(subjectId = "userId") String email) {}

  /**
   * A mock {@link DataSource} whose connection reports a supported PostgreSQL server. {@code
   * create()} checks the server version right after the crypto validation these tests are about; an
   * unstubbed mock would hand it a {@code null} connection and every test that wants {@code
   * create()} to complete would instead die in the version check. With the schema flags off,
   * nothing after the version check touches the DataSource, so {@code create()} completes.
   */
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

  record OrderCreated(String orderId) {}

  /**
   * Minimal no-op {@link CryptoEngine} double — avoids adding a test dependency on streamrune-test
   * just for InMemoryCryptoEngine; this test only needs a non-null engine.
   */
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

  @Test
  void createThrowsBeforeTouchingDatabaseWhenEncryptedTypeHasNoCryptoEngine() {
    DataSource dataSource = mock(DataSource.class);
    EventTypeRegistry registry =
        SimpleEventTypeRegistry.builder().registerEvent(UserRegistered.class).build();
    var factory = new PostgresEventStoreFactory(dataSource, registry);

    var ex = assertThrows(IllegalStateException.class, factory::create);

    assertTrue(ex.getMessage().contains("UserRegistered"), ex.getMessage());
    assertTrue(ex.getMessage().contains("email"), ex.getMessage());
    assertTrue(ex.getMessage().contains("CryptoEngine"), ex.getMessage());
    // The failure must happen before any database interaction (schema init/validation).
    verifyNoInteractions(dataSource);
  }

  @Test
  void createDoesNotValidateCryptoWhenNoEncryptedFieldsArePresent() {
    DataSource dataSource = supportedServer();
    EventTypeRegistry registry =
        SimpleEventTypeRegistry.builder().registerEvent(OrderCreated.class).build();
    var factory =
        new PostgresEventStoreFactory(dataSource, registry)
            .autoInitializeSchema(false)
            .validateSchema(false);

    // No @Encrypted fields anywhere and no CryptoEngine: the crypto validator must not block
    // startup, so create() completes.
    assertNotNull(assertDoesNotThrow(factory::create));
  }

  @Test
  void createThrowsWhenCustomRegistryDoesNotOverrideRegisteredTypes() {
    // A hand-rolled EventTypeRegistry that never overrides registeredTypes(). A silent
    // List.of() default would disable the @Encrypted guard below (the interface javadoc says why
    // the default throws instead); the default itself must fail loudly.
    DataSource dataSource = mock(DataSource.class);
    EventTypeRegistry registry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(org.streamrune.core.types.EventType eventType) {
            return UserRegistered.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            throw new UnsupportedOperationException("no state types registered");
          }
        };
    var factory = new PostgresEventStoreFactory(dataSource, registry);

    var ex = assertThrows(UnsupportedOperationException.class, factory::create);

    assertTrue(ex.getMessage().contains("registeredTypes"), ex.getMessage());
    // The failure must happen before any database interaction (schema init/validation).
    verifyNoInteractions(dataSource);
  }

  @Test
  void createDoesNotThrowFromCryptoValidationWhenCryptoEngineIsConfigured() {
    DataSource dataSource = supportedServer();
    EventTypeRegistry registry =
        SimpleEventTypeRegistry.builder().registerEvent(UserRegistered.class).build();
    var factory =
        new PostgresEventStoreFactory(dataSource, registry)
            .cryptoEngine(new NoOpCryptoEngine())
            .autoInitializeSchema(false)
            .validateSchema(false);

    // A CryptoEngine is configured, so the crypto validator must not block startup even though
    // UserRegistered has an @Encrypted field: create() completes.
    assertNotNull(assertDoesNotThrow(factory::create));
  }

  // ── The override detector must reach the engine-PRESENT boot path ─────────────────────────
  //
  // The property-level-override detector exists for exactly one configuration: a CryptoEngine IS
  // wired, and a @JsonSerialize(using = ...) on a component whose type carries @Encrypted replaces
  // the encrypting serializer, so the PII is written to the append-only log as plaintext with no
  // key minted (no later forget(subjectId) can reach it). CryptoConfigValidator runs the scan
  // before its engine != null short-circuit — but create()'s only call passing registeredTypes()
  // sat behind `if (cryptoEngine == null)`, so for event and state types the detector never ran in
  // the one configuration it was written for. The in-validator ordering test pins the validator,
  // not the boot path, so it stayed green.

  record CustomerWithPii(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  /** The bypass shape: writes the nested record's fields itself, so no encrypting writer runs. */
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

  private static java.util.List<String> warningsWhile(Runnable body) {
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

  @Test
  void createRunsTheJacksonOverrideScanWhenACryptoEngineIsConfigured() {
    DataSource dataSource = supportedServer();
    EventTypeRegistry registry =
        SimpleEventTypeRegistry.builder().registerEvent(OrderPlacedWithOverride.class).build();
    var factory =
        new PostgresEventStoreFactory(dataSource, registry)
            .cryptoEngine(new NoOpCryptoEngine())
            .autoInitializeSchema(false)
            .validateSchema(false);

    var warnings =
        warningsWhile(
            () -> {
              // The scan only warns: create() completes.
              assertDoesNotThrow(factory::create);
            });

    assertTrue(
        warnings.stream().anyMatch(w -> w.contains("OrderPlacedWithOverride")),
        "the override-detector WARN must name the offending component on the engine-present boot path — "
            + "the only configuration in which the plaintext leak is possible. Was: "
            + warnings);
  }

  @Test
  void createToleratesAThrowingRegisteredTypesWhenACryptoEngineIsConfigured() {
    // Preserved: a custom registry that never overrides
    // registeredTypes() must still boot when crypto is fully wired. The override scan now WANTS
    // those types, so create() asks for them and skips the scan when the registry refuses —
    // it must not turn the refusal into a boot failure.
    DataSource dataSource = supportedServer();
    EventTypeRegistry registry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(org.streamrune.core.types.EventType eventType) {
            return UserRegistered.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            throw new UnsupportedOperationException("no state types registered");
          }
        };
    var factory =
        new PostgresEventStoreFactory(dataSource, registry)
            .cryptoEngine(new NoOpCryptoEngine())
            .autoInitializeSchema(false)
            .validateSchema(false);

    assertDoesNotThrow(
        factory::create,
        "a registry that cannot enumerate its types must not fail boot when an engine is present");
  }
}
