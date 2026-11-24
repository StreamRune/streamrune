package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.integration.BackendSelection;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.postgres.PostgresBackend;

/**
 * {@code streamrune.backend=sqlite} in the Quarkus integration: the SQLite backend is selected and
 * verified against the {@code DataSource}, leadership defaults to off, and the fail-fast checks of
 * the spec refuse what SQLite cannot honour. Booted in a real Arc container; the backend class is
 * loaded by {@code BackendSelection}'s own class loader, the application class loader that also
 * defines the Arc beans' types.
 */
class QuarkusSqliteSelectionTest {

  /** The DataSource, the properties and the configuration every boot below takes. */
  @ApplicationScoped
  public static class Infrastructure {

    static volatile DataSource dataSource;
    static volatile Map<String, String> values = Map.of();

    @Produces
    @Singleton
    public DataSource dataSource() {
      return dataSource;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return TestProperties.of(values);
    }

    @Produces
    @Singleton
    public Config config() {
      return TestProperties.config(values);
    }
  }

  private Path dir;

  @BeforeEach
  void sqliteFile() throws Exception {
    dir = Files.createTempDirectory("quarkus-sqlite");
    Infrastructure.dataSource = SqliteDataSources.file(dir.resolve("app.db"));
    Infrastructure.values = Map.of("streamrune.backend", "sqlite");
  }

  @AfterEach
  void reset() {
    Infrastructure.values = Map.of();
  }

  private static RealArcTestContainer boot(Class<?>... application) throws Exception {
    List<Class<?>> classes =
        new ArrayList<>(List.of(StreamRuneProducers.class, Infrastructure.class));
    classes.addAll(List.of(application));
    return RealArcTestContainer.boot(classes);
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }

  @Test
  void aPostgresDataSourceIsRefusedNamingBothTheBackendAndTheDatabase() throws Exception {
    DataSource postgres = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData meta = mock(DatabaseMetaData.class);
    when(postgres.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(meta);
    when(meta.getDatabaseProductName()).thenReturn("PostgreSQL");
    when(meta.getDatabaseProductVersion()).thenReturn("17.2");
    Infrastructure.dataSource = postgres;

    try (var arc = boot()) {
      assertThatThrownBy(() -> arc.container().instance(BackendStores.class).get())
          .satisfies(
              failure ->
                  assertThat(rootCause(failure))
                      .isInstanceOf(IllegalStateException.class)
                      .hasMessageContaining("streamrune.backend=sqlite")
                      .hasMessageContaining("needs a SQLite database")
                      .hasMessageContaining("PostgreSQL 17.2"));
    }
  }

  @Test
  void aSqliteDataSourceWithDefaultsRunsOnTheSqliteBackendWithoutLeadership() throws Exception {
    try (var arc = boot()) {
      assertThat(arc.container().instance(StreamRuneBackend.class).get().name())
          .isEqualTo("sqlite");
      assertThat(arc.container().instance(BackendStores.class).get().backendName())
          .isEqualTo("sqlite");
      assertThat(arc.container().instance(SubscriptionLeadership.class).get())
          .isSameAs(SubscriptionLeadership.NOOP);
      assertThat(arc.container().instance(ReadModelStores.class).get().defaultStoreName())
          .isEqualTo("sqlite");
      assertThat(arc.container().instance(EventStore.class).get().getClass().getName())
          .isEqualTo("org.streamrune.sqlite.SqliteEventStore");
    }
  }

  @Test
  void anExplicitSingleActiveConsumerIsRefused() throws Exception {
    Infrastructure.values =
        Map.of(
            "streamrune.backend",
            "sqlite",
            "streamrune.subscription.single-active-consumer.enabled",
            "true");
    try (var arc = boot()) {
      assertThatThrownBy(() -> arc.container().instance(SubscriptionLeadership.class).get())
          .satisfies(
              failure ->
                  assertThat(rootCause(failure))
                      .isInstanceOf(IllegalStateException.class)
                      .hasMessageContaining(
                          "SQLite is single-instance; set"
                              + " streamrune.subscription.single-active-consumer.enabled=false"));
    }
  }

  @Test
  void theStartupValidatorRefusesAnExplicitSingleActiveConsumerToo() {
    var properties =
        TestProperties.of(
            Map.of(
                "streamrune.backend",
                "sqlite",
                "streamrune.subscription.single-active-consumer.enabled",
                "on"));
    assertThatThrownBy(
            () ->
                org.streamrune.integration.SingleActiveConsumerSetting.enabled(
                    properties.subscription().singleActiveConsumer().enabled().orElse(null),
                    BackendSelection.parse(properties.backend())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SQLite is single-instance");
  }

  @Test
  void thePostgresKeyStoreEngineIsRefusedOnSqlite() {
    StreamRuneBackend sqlite = BackendSelection.resolve("sqlite", PostgresBackend::new);
    assertThatThrownBy(
            () ->
                StreamRuneCryptoProducers.cryptoBackendOf(
                    sqlite,
                    TestProperties.crypto(Map.of("streamrune.crypto.postgres.enabled", "true"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "streamrune.crypto.postgres.enabled=true enables the PostgreSQL key-store engine, but"
                + " streamrune.backend=sqlite");
  }

  @Test
  void theSqliteKeyStoreEngineIsRefusedOnPostgres() {
    assertThatThrownBy(
            () ->
                StreamRuneCryptoProducers.cryptoBackendOf(
                    new PostgresBackend(),
                    TestProperties.crypto(Map.of("streamrune.crypto.sqlite.enabled", "true"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "streamrune.crypto.sqlite.enabled=true enables the SQLite key-store engine, but"
                + " streamrune.backend=postgres");
  }

  @Test
  void theCryptoBackendIsSelectedByTheBackendsName() {
    StreamRuneBackend sqlite = BackendSelection.resolve("sqlite", PostgresBackend::new);
    CryptoBackend crypto =
        StreamRuneCryptoProducers.cryptoBackendOf(
            sqlite, TestProperties.crypto(Map.of("streamrune.crypto.sqlite.enabled", "true")));
    assertThat(crypto.name()).isEqualTo("sqlite");
    assertThat(crypto.getClass().getName()).isEqualTo(BackendSelection.SQLITE_CRYPTO_BACKEND_CLASS);
  }

  /** The SQLite crypto producer, gated on its build property, builds the cached SQLite engine. */
  @Test
  void theSqliteKeyStoreEngineRunsOnSqlite() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                Infrastructure.class,
                CryptoProperties.class,
                StreamRuneCryptoProducers.CryptoBackendProducer.class,
                StreamRuneCryptoProducers.SqliteCryptoProducer.class,
                StreamRuneCryptoProducers.PostgresCryptoProducer.class),
            Map.of("streamrune.crypto.sqlite.enabled", "true"))) {
      assertThat(arc.container().instance(CryptoBackend.class).get().name()).isEqualTo("sqlite");
      assertThat(arc.container().instance(CryptoEngine.class).get())
          .isInstanceOf(CachedCryptoEngine.class);
      assertThat(arc.container().instance(BackendStores.class).get().backendName())
          .isEqualTo("sqlite");
    }
  }

  /** The crypto properties of the boot above. */
  @ApplicationScoped
  public static class CryptoProperties {
    @Produces
    @Singleton
    public StreamRuneQuarkusCryptoProperties cryptoProperties() {
      return TestProperties.crypto(Map.of("streamrune.crypto.sqlite.enabled", "true"));
    }
  }

  @Test
  void aConfiguredStatementTimeoutStartsAndWarnsOnce() throws Exception {
    Infrastructure.values =
        Map.of("streamrune.backend", "sqlite", "streamrune.event-store.statement-timeout", "5s");
    List<String> warnings = captureBackendStartupWarnings();
    try (var arc = boot()) {
      assertThat(arc.container().instance(EventStore.class).get()).isNotNull();
    } finally {
      releaseCapture();
    }
    assertThat(warnings.stream().filter(w -> w.contains("ignored on SQLite")).count()).isEqualTo(1);
    assertThat(warnings)
        .anyMatch(w -> w.startsWith("streamrune.event-store.statement-timeout=5s is ignored"));
  }

  @Test
  void anUnsetStatementTimeoutWarnsNothing() throws Exception {
    List<String> warnings = captureBackendStartupWarnings();
    try (var arc = boot()) {
      assertThat(arc.container().instance(EventStore.class).get()).isNotNull();
    } finally {
      releaseCapture();
    }
    assertThat(warnings).noneMatch(w -> w.contains("ignored on SQLite"));
  }

  private java.util.logging.Handler handler;

  private List<String> captureBackendStartupWarnings() {
    List<String> captured = new CopyOnWriteArrayList<>();
    var logger =
        java.util.logging.Logger.getLogger(
            org.streamrune.integration.BackendStartup.class.getName());
    handler =
        new java.util.logging.Handler() {
          @Override
          public void publish(java.util.logging.LogRecord record) {
            captured.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    logger.setLevel(java.util.logging.Level.ALL);
    return captured;
  }

  private void releaseCapture() {
    java.util.logging.Logger.getLogger(org.streamrune.integration.BackendStartup.class.getName())
        .removeHandler(handler);
  }

  // ── shutdown order ────────────────────────────────────────────────────────────────────────

  /** What was shut down, in order, and how many connections were out at the bundle's close. */
  static final List<String> SHUTDOWN = new CopyOnWriteArrayList<>();

  static final AtomicInteger OPEN_CONNECTIONS = new AtomicInteger();

  /**
   * An application bean of the usual Quarkus scope that uses a framework store and records its own
   * destruction.
   */
  @ApplicationScoped
  public static class SagaStoreReader {
    @Inject public SagaStore store;

    public SagaStore store() {
      return store;
    }

    @PreDestroy
    public void destroy() {
      assertThat(store).isNotNull();
      SHUTDOWN.add("reader");
    }
  }

  /** The SQLite backend, its bundle recording its close. */
  @ApplicationScoped
  public static class RecordingBackend {
    @Produces
    @Singleton
    @Alternative
    @Priority(1)
    public StreamRuneBackend recordingBackend() {
      StreamRuneBackend real = BackendSelection.resolve("sqlite", PostgresBackend::new);
      return new StreamRuneBackend() {
        @Override
        public String name() {
          return real.name();
        }

        @Override
        public void verify(DataSource dataSource) {
          real.verify(dataSource);
        }

        @Override
        public BackendStores open(BackendContext context) {
          BackendStores stores = org.mockito.Mockito.spy(real.open(context));
          org.mockito.Mockito.doAnswer(
                  invocation -> {
                    SHUTDOWN.add("bundle with " + OPEN_CONNECTIONS.get() + " connections out");
                    return invocation.callRealMethod();
                  })
              .when(stores)
              .close();
          return stores;
        }
      };
    }
  }

  /**
   * The bundle is closed only after the application's {@code @ApplicationScoped} beans that use its
   * stores were destroyed: a SQLite store keeps writing the file after its bundle released the
   * instance guard. Arc destroys the application context before the singleton context, where the
   * bundle lives; within the singleton context it destroys producer products (the bundle and every
   * framework store) before class beans, so an application {@code @Singleton} bean must not write
   * through a store in its {@code @PreDestroy} (see the bundle producer's Javadoc).
   */
  @Test
  void theBundleClosesAfterEveryUserOfItsStores() throws Exception {
    SHUTDOWN.clear();
    Infrastructure.dataSource = SqliteDataSources.counting(dir.resolve("app.db"), OPEN_CONNECTIONS);
    try (var arc = boot(SagaStoreReader.class, RecordingBackend.class)) {
      assertThat(arc.container().instance(SagaStoreReader.class).get().store()).isNotNull();
    }
    assertThat(SHUTDOWN).containsExactly("reader", "bundle with 0 connections out");
  }
}
