package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;

/**
 * StreamRune declares PostgreSQL 17 or newer, and the runtime refuses an older server at startup
 * instead of letting it fail later on whichever statement first needs something the older server
 * lacks.
 *
 * <p>Not a durable flow: the check is a read of the connection's server-version metadata that runs
 * before the first migration and writes nothing, so there is no crash point to pin and a rerun
 * simply repeats the same read. What is pinned here is WHEN it runs (before any Flyway connection),
 * WHETHER it can be switched off (it cannot — the schema flags do not gate it), and what it says.
 */
class PostgresServerVersionTest {

  private static DataSource serverReporting(int major, String productVersion) throws SQLException {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData meta = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(meta);
    when(meta.getDatabaseMajorVersion()).thenReturn(major);
    when(meta.getDatabaseProductVersion()).thenReturn(productVersion);
    return dataSource;
  }

  @Test
  void theMinimumIsPostgreSql17() {
    assertEquals(17, PostgresServerVersion.MINIMUM_MAJOR_VERSION);
  }

  @ParameterizedTest
  @ValueSource(ints = {17, 18, 19, 25})
  void aSupportedServerIsAccepted(int major) throws SQLException {
    DataSource dataSource = serverReporting(major, major + ".0");

    PostgresServerVersion.requireSupported(dataSource);

    // The probe's connection is returned to the caller's pool, not leaked.
    verify(dataSource.getConnection(), times(1)).close();
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 10, 12, 14, 15, 16})
  void anOlderServerIsRefused(int major) throws SQLException {
    DataSource dataSource = serverReporting(major, major + ".4");

    var ex =
        assertThrows(
            UnsupportedServerVersionException.class,
            () -> PostgresServerVersion.requireSupported(dataSource));

    assertEquals(major + ".4", ex.serverVersion());
    assertEquals(17, ex.minimumMajorVersion());
    // The operator's first question is "what does it want, and what did you find?".
    assertTrue(ex.getMessage().contains("PostgreSQL " + major + ".4"), ex.getMessage());
    assertTrue(ex.getMessage().contains("17 or newer"), ex.getMessage());
    verify(dataSource.getConnection(), times(1)).close();
  }

  @Test
  void aServerVersionTheServerControlsCannotForgeALogLine() throws SQLException {
    DataSource dataSource = serverReporting(16, "16.4\r\n2026-01-01 ERROR forged line");

    var ex =
        assertThrows(
            UnsupportedServerVersionException.class,
            () -> PostgresServerVersion.requireSupported(dataSource));

    assertFalse(ex.getMessage().contains("\n"), ex.getMessage());
    assertFalse(ex.getMessage().contains("\r"), ex.getMessage());
  }

  @Test
  void anUnreadableServerVersionIsAStoreFailureNotASilentPass() throws SQLException {
    DataSource dataSource = mock(DataSource.class);
    SQLException down = new SQLException("connection refused");
    when(dataSource.getConnection()).thenThrow(down);

    var ex =
        assertThrows(
            EventStoreException.class, () -> PostgresServerVersion.requireSupported(dataSource));

    assertSame(down, ex.getCause());
    assertTrue(ex.getMessage().contains("server version"), ex.getMessage());
  }

  // ── The factory runs it, first, whatever the schema flags say ────────────────────────────────

  private static EventTypeRegistry registry() {
    return SimpleEventTypeRegistry.builder().build();
  }

  @Test
  void createRefusesAnOldServerBeforeAnyMigrationConnection() throws SQLException {
    DataSource dataSource = serverReporting(16, "16.4");

    var ex =
        assertThrows(
            UnsupportedServerVersionException.class,
            () -> new PostgresEventStoreFactory(dataSource, registry()).create());

    assertTrue(ex.getMessage().contains("17 or newer"), ex.getMessage());
    // Exactly one connection — the version probe. Flyway (initializeSchema), the schema validator
    // and the pool each open their own; none of them may have run against the unsupported server.
    verify(dataSource, times(1)).getConnection();
  }

  @Test
  void theSchemaFlagsDoNotSwitchTheRefusalOff() throws SQLException {
    DataSource dataSource = serverReporting(15, "15.8");

    assertThrows(
        UnsupportedServerVersionException.class,
        () ->
            new PostgresEventStoreFactory(dataSource, registry())
                .autoInitializeSchema(false)
                .validateSchema(false)
                .create());

    verify(dataSource, times(1)).getConnection();
  }

  @Test
  void initializeSchemaRefusesAnOldServerBeforeAnyMigrationConnection() throws SQLException {
    // The plain-Java path the QUICKSTART documents provisions the schema with a bare
    // initializeSchema(), never reaching create(): it must refuse on its own, before Flyway.
    DataSource dataSource = serverReporting(16, "16.4");

    assertThrows(
        UnsupportedServerVersionException.class,
        () -> new PostgresEventStoreFactory(dataSource, registry()).initializeSchema());

    verify(dataSource, times(1)).getConnection();
  }

  @Test
  void theStaticEventStoreFactoryRefusesAnOldServer() throws SQLException {
    // The QUICKSTART's plain-Java store: PostgresEventStore.create(dataSource, registry, ...).
    DataSource dataSource = serverReporting(16, "16.4");

    assertThrows(
        UnsupportedServerVersionException.class,
        () -> PostgresEventStore.create(dataSource, registry(), null, java.util.List.of()));

    verify(dataSource, times(1)).getConnection();
  }

  @Test
  void theStaticEventStoreFactoryBuildsTheStoreOnASupportedServer() throws SQLException {
    DataSource dataSource = serverReporting(17, "17.2");

    var store = PostgresEventStore.create(dataSource, registry(), null, java.util.List.of());

    assertNotNull(store);
    verify(dataSource, times(1)).getConnection();
  }

  @Test
  void aSupportedServerLetsCreateContinuePastTheCheck() throws SQLException {
    DataSource dataSource = serverReporting(17, "17.2");

    // The mock cannot complete a schema migration, so create() fails further down — what matters
    // is that it is NOT the version refusal.
    var ex =
        assertThrows(
            Exception.class, () -> new PostgresEventStoreFactory(dataSource, registry()).create());

    assertFalse(ex instanceof UnsupportedServerVersionException, ex.toString());
  }
}
