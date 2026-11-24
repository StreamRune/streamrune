package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
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
import org.streamrune.integration.BackendStartup;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.postgres.PostgresBackend;

/**
 * {@code streamrune.backend=sqlite} in the Micronaut integration: the SQLite backend is selected
 * and verified against the {@code DataSource}, leadership defaults to off, and the fail-fast checks
 * of the spec refuse what SQLite cannot honour. Booted in a real Micronaut context.
 */
class MicronautSqliteSelectionTest {

  private static final String SPEC = "spec.name";
  private static final String SELECTION = "sqlite-selection";

  /** The DataSource of the next boot. */
  @Factory
  @Requires(property = SPEC, pattern = "sqlite-selection.*")
  static class Infrastructure {
    static volatile DataSource dataSource;

    @Singleton
    DataSource dataSource() {
      return dataSource;
    }
  }

  private Path dir;

  @BeforeEach
  void sqliteFile() throws Exception {
    dir = Files.createTempDirectory("micronaut-sqlite");
    Infrastructure.dataSource = SqliteDataSources.file(dir.resolve("app.db"));
  }

  private static Map<String, Object> properties(String spec, Object... more) {
    Map<String, Object> values = new HashMap<>();
    values.put(SPEC, spec);
    values.put("streamrune.backend", "sqlite");
    // A crypto engine brings the forget service, which needs a snapshot purger or this setting.
    values.put("streamrune.gdpr.snapshot-purge", "none");
    for (int i = 0; i < more.length; i += 2) {
      values.put((String) more[i], more[i + 1]);
    }
    return values;
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

    assertThatThrownBy(
            () -> {
              try (var ctx = ApplicationContext.run(properties(SELECTION))) {
                ctx.getBean(BackendStores.class);
              }
            })
        .satisfies(
            failure ->
                assertThat(rootCause(failure))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("streamrune.backend=sqlite")
                    .hasMessageContaining("needs a SQLite database")
                    .hasMessageContaining("PostgreSQL 17.2"));
  }

  @Test
  void aSqliteDataSourceWithDefaultsRunsOnTheSqliteBackendWithoutLeadership() {
    try (var ctx = ApplicationContext.run(properties(SELECTION))) {
      assertThat(ctx.getBean(StreamRuneBackend.class).name()).isEqualTo("sqlite");
      assertThat(ctx.getBean(BackendStores.class).backendName()).isEqualTo("sqlite");
      assertThat(ctx.findBean(SubscriptionLeadership.class))
          .as("no leadership bean: runners lead with SubscriptionLeadership.NOOP")
          .isEmpty();
      assertThat(ctx.getBean(ReadModelStores.class).defaultStoreName()).isEqualTo("sqlite");
      assertThat(ctx.getBean(EventStore.class).getClass().getName())
          .isEqualTo("org.streamrune.sqlite.SqliteEventStore");
    }
  }

  /** An unknown backend fails the start even in a context without a DataSource. */
  @Test
  void anUnknownBackendFailsTheStartWithoutADataSource() {
    assertThatThrownBy(() -> ApplicationContext.run(Map.of("streamrune.backend", "oracle")).close())
        .satisfies(
            failure ->
                assertThat(rootCause(failure))
                    .hasMessage(
                        "streamrune.backend='oracle' is not a backend; valid values: postgres,"
                            + " sqlite"));
  }

  @Test
  void anExplicitSingleActiveConsumerIsRefused() {
    assertThatThrownBy(
            () -> {
              try (var ctx =
                  ApplicationContext.run(
                      properties(
                          SELECTION,
                          "streamrune.subscription.single-active-consumer.enabled",
                          "true"))) {
                ctx.findBean(SubscriptionLeadership.class);
              }
            })
        .satisfies(
            failure ->
                assertThat(rootCause(failure))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                        "SQLite is single-instance; set"
                            + " streamrune.subscription.single-active-consumer.enabled=false"));
  }

  @Test
  void thePostgresKeyStoreEngineIsRefusedOnSqlite() {
    assertThatThrownBy(
            () -> {
              try (var ctx =
                  ApplicationContext.run(
                      properties(SELECTION, "streamrune.crypto.postgres.enabled", "true"))) {
                ctx.getBean(CryptoEngine.class);
              }
            })
        .satisfies(
            failure ->
                assertThat(rootCause(failure))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith(
                        "streamrune.crypto.postgres.enabled=true enables the PostgreSQL key-store"
                            + " engine, but streamrune.backend=sqlite"));
  }

  @Test
  void theSqliteKeyStoreEngineIsRefusedOnPostgres() {
    assertThatThrownBy(
            () -> StreamRuneCryptoFactory.cryptoBackendOf(new PostgresBackend(), false, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "streamrune.crypto.sqlite.enabled=true enables the SQLite key-store engine, but"
                + " streamrune.backend=postgres");
  }

  @Test
  void theSqliteKeyStoreEngineRunsOnSqlite() {
    try (var ctx =
        ApplicationContext.run(properties(SELECTION, "streamrune.crypto.sqlite.enabled", "true"))) {
      assertThat(ctx.getBean(CryptoBackend.class).name()).isEqualTo("sqlite");
      assertThat(ctx.getBean(CryptoEngine.class)).isInstanceOf(CachedCryptoEngine.class);
    }
  }

  @Test
  void theUncachedSqliteKeyStoreEngineRunsOnSqlite() {
    try (var ctx =
        ApplicationContext.run(
            properties(
                SELECTION,
                "streamrune.crypto.sqlite.enabled",
                "true",
                "streamrune.crypto.cache.enabled",
                "false"))) {
      assertThat(ctx.getBean(CryptoEngine.class)).isNotInstanceOf(CachedCryptoEngine.class);
    }
  }

  @Test
  void aConfiguredStatementTimeoutStartsAndWarnsOnce() {
    ListAppender<ILoggingEvent> appender = capture();
    try (var ctx =
        ApplicationContext.run(
            properties(SELECTION, "streamrune.event-store.statement-timeout", "5s"))) {
      assertThat(ctx.getBean(EventStore.class)).isNotNull();
    } finally {
      release(appender);
    }
    List<String> warnings = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    assertThat(warnings.stream().filter(w -> w.contains("ignored on SQLite")).count()).isEqualTo(1);
    assertThat(warnings)
        .anyMatch(w -> w.startsWith("streamrune.event-store.statement-timeout=5s is ignored"));
  }

  @Test
  void anUnsetStatementTimeoutWarnsNothing() {
    ListAppender<ILoggingEvent> appender = capture();
    try (var ctx = ApplicationContext.run(properties(SELECTION))) {
      assertThat(ctx.getBean(EventStore.class)).isNotNull();
    } finally {
      release(appender);
    }
    assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().contains("ignored on SQLite"));
  }

  private static ListAppender<ILoggingEvent> capture() {
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    ((Logger) LoggerFactory.getLogger(BackendStartup.class)).addAppender(appender);
    return appender;
  }

  private static void release(ListAppender<ILoggingEvent> appender) {
    ((Logger) LoggerFactory.getLogger(BackendStartup.class)).detachAppender(appender);
  }

  // ── shutdown order ────────────────────────────────────────────────────────────────────────

  static final List<String> SHUTDOWN = new CopyOnWriteArrayList<>();
  static final AtomicInteger OPEN_CONNECTIONS = new AtomicInteger();

  /** An application bean that uses a framework store and records its own destruction. */
  @Singleton
  @Requires(property = SPEC, value = "sqlite-selection-shutdown")
  static class SagaStoreReader {
    final SagaStore store;

    SagaStoreReader(SagaStore store) {
      this.store = store;
    }

    @PreDestroy
    void destroy() {
      assertThat(store).isNotNull();
      SHUTDOWN.add("reader");
    }
  }

  /** The SQLite backend, its bundle recording its close. */
  @Factory
  @Requires(property = SPEC, value = "sqlite-selection-shutdown")
  static class RecordingBackend {
    @Singleton
    @Replaces(StreamRuneBackend.class)
    StreamRuneBackend recordingBackend() {
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
   * The bundle is closed only after every bean that uses one of its stores was destroyed: a SQLite
   * store keeps writing the file after its bundle released the instance guard. Micronaut destroys a
   * bean's dependents before it.
   */
  @Test
  void theBundleClosesAfterEveryUserOfItsStores() {
    SHUTDOWN.clear();
    Infrastructure.dataSource = SqliteDataSources.counting(dir.resolve("app.db"), OPEN_CONNECTIONS);
    try (var ctx = ApplicationContext.run(properties("sqlite-selection-shutdown"))) {
      assertThat(ctx.getBean(SagaStoreReader.class).store).isNotNull();
    }
    assertThat(SHUTDOWN).containsExactly("reader", "bundle with 0 connections out");
  }
}
