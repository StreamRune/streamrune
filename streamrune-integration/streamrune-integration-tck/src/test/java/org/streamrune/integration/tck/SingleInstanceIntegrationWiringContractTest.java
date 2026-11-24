package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.streamrune.integration.BackendSelection;
import org.streamrune.integration.tck.IntegrationWiringContractTest.StubDataSource;

/**
 * The contract as a subclass on the SQLite backend runs it — {@code streamrune.backend=sqlite} on
 * every start with a database, {@link #supportsMultiInstance()} {@code false}, the backend name
 * {@code sqlite} — against {@link ReferenceApplication}, which reads the selection: each inherited
 * test passes. The tests declared here show that the single-instance cases still fail an
 * application that does not run on the selected backend.
 */
class SingleInstanceIntegrationWiringContractTest extends IntegrationWiringContract {

  private boolean selectsSqlite = true;

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    Map<String, String> configuration =
        selectsSqlite ? SqliteTestDatabases.onSqlite(dataSource, properties) : properties;
    return ReferenceApplication.start(dataSource, configuration, extraBeans, Set.of());
  }

  @Override
  protected DataSource database() {
    return new StubDataSource();
  }

  @Override
  protected String healthCheckName() {
    return "streamrune";
  }

  @Override
  protected Map<String, String> configurationDefaults(Application application) {
    return ((ReferenceApplication) application).configurationDefaults();
  }

  @Override
  protected Duration patience() {
    return Duration.ofSeconds(2);
  }

  @Override
  protected boolean supportsMultiInstance() {
    return false;
  }

  @Override
  protected String backendName() {
    return "sqlite";
  }

  @Test
  void aStartOnADatabaseSelectsSqliteAndOneWithoutIsLeftAlone() {
    Map<String, String> properties = Map.of("streamrune.polling-interval", "50ms");
    assertEquals(
        Map.of("streamrune.polling-interval", "50ms", BackendSelection.PROPERTY, "sqlite"),
        SqliteTestDatabases.onSqlite(new StubDataSource(), properties));
    assertSame(properties, SqliteTestDatabases.onSqlite(null, properties));
  }

  @Test
  void aRunnerLeadingUnderALeaseFailsTheSingleInstanceHealthTest() {
    selectsSqlite = false;
    var failure =
        assertThrows(AssertionFailedError.class, this::healthCarriesTheDocumentedNameAndKeys);
    assertTrue(failure.getMessage().contains("UNGATED"), failure.getMessage());
  }

  @Test
  void aBundleOfAnotherBackendFailsTheBackendStoresTest() {
    selectsSqlite = false;
    var failure = assertThrows(AssertionFailedError.class, this::backendStoresBeanIsExposed);
    assertTrue(failure.getMessage().contains("sqlite"), failure.getMessage());
  }
}
