package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.streamrune.core.EventStore;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.integration.ReadModelStores;

/**
 * {@code streamrune.backend=sqlite} in the Spring integration: the SQLite backend is selected and
 * verified against the {@code DataSource}, leadership defaults to off, and the fail-fast checks of
 * the spec refuse what SQLite cannot honour.
 */
@ExtendWith(OutputCaptureExtension.class)
class SpringSqliteSelectionTest {

  @TempDir Path dir;

  private ApplicationContextRunner runner(DataSource dataSource) {
    return new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS, "streamrune.backend=sqlite")
        .withConfiguration(
            AutoConfigurations.of(
                StreamRuneAutoConfiguration.class,
                CryptoPropertiesConfiguration.class,
                PostgresCryptoEngineConfiguration.class,
                SqliteCryptoEngineConfiguration.class))
        .withBean(DataSource.class, () -> dataSource);
  }

  private ApplicationContextRunner sqlite() {
    return runner(SqliteDataSources.file(dir.resolve("app.db")));
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

    runner(postgres)
        .run(
            ctx ->
                assertThat(rootCause(ctx.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("streamrune.backend=sqlite")
                    .hasMessageContaining("needs a SQLite database")
                    .hasMessageContaining("PostgreSQL 17.2"));
  }

  @Test
  void aSqliteDataSourceWithDefaultsRunsOnTheSqliteBackendWithoutLeadership() {
    sqlite()
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(StreamRuneBackend.class).name()).isEqualTo("sqlite");
              assertThat(ctx.getBean(BackendStores.class).backendName()).isEqualTo("sqlite");
              assertThat(ctx.getBean(ReadModelStores.class).defaultStoreName()).isEqualTo("sqlite");
              assertThat(ctx).doesNotHaveBean("streamRuneSubscriptionLeadership");
              assertThat(LeadershipBeans.resolve(ctx.getBeanProvider(SubscriptionLeadership.class)))
                  .isSameAs(SubscriptionLeadership.NOOP);
              assertThat(ctx.getBean(EventStore.class).getClass().getName())
                  .isEqualTo("org.streamrune.sqlite.SqliteEventStore");
            });
  }

  @Test
  void anExplicitSingleActiveConsumerIsRefused() {
    sqlite()
        .withPropertyValues("streamrune.subscription.single-active-consumer.enabled=true")
        .run(
            ctx ->
                assertThat(rootCause(ctx.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                        "SQLite is single-instance; set"
                            + " streamrune.subscription.single-active-consumer.enabled=false"));
  }

  @Test
  void anExplicitlyDisabledSingleActiveConsumerStarts() {
    sqlite()
        .withPropertyValues("streamrune.subscription.single-active-consumer.enabled=false")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void thePostgresKeyStoreEngineIsRefusedOnSqlite() {
    sqlite()
        .withPropertyValues("streamrune.crypto.postgres.enabled=true")
        .run(
            ctx ->
                assertThat(rootCause(ctx.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith(
                        "streamrune.crypto.postgres.enabled=true enables the PostgreSQL key-store"
                            + " engine, but streamrune.backend=sqlite"));
  }

  @Test
  void theSqliteKeyStoreEngineIsRefusedOnPostgres() {
    sqlite()
        .withPropertyValues("streamrune.backend=postgres", "streamrune.crypto.sqlite.enabled=true")
        .run(
            ctx ->
                assertThat(rootCause(ctx.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith(
                        "streamrune.crypto.sqlite.enabled=true enables the SQLite key-store"
                            + " engine, but streamrune.backend=postgres"));
  }

  @Test
  void theSqliteKeyStoreEngineRunsOnSqlite() {
    sqlite()
        .withPropertyValues("streamrune.crypto.sqlite.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(CryptoBackend.class).name()).isEqualTo("sqlite");
              assertThat(ctx.getBean(CryptoEngine.class)).isInstanceOf(CachedCryptoEngine.class);
              assertThat(ctx).hasBean("sqliteCryptoEngine");
            });
  }

  @Test
  void aConfiguredStatementTimeoutStartsAndWarnsOnce(CapturedOutput output) {
    sqlite()
        .withPropertyValues("streamrune.event-store.statement-timeout=5s")
        .run(ctx -> assertThat(ctx).hasNotFailed());

    assertThat(occurrences(output.getAll(), "ignored on SQLite")).isEqualTo(1);
    assertThat(output.getAll())
        .contains("streamrune.event-store.statement-timeout=5s is ignored on SQLite");
  }

  @Test
  void anUnsetStatementTimeoutWarnsNothing(CapturedOutput output) {
    sqlite().run(ctx -> assertThat(ctx).hasNotFailed());

    assertThat(output.getAll()).doesNotContain("ignored on SQLite");
  }

  /**
   * The bundle is closed only after every bean that uses one of its stores was destroyed: a SQLite
   * store keeps writing the file after its bundle released the instance guard.
   */
  @Test
  void theBundleClosesAfterEveryUserOfItsStores() {
    ShutdownLog shutdown = new ShutdownLog();
    runner(SqliteDataSources.counting(dir.resolve("app.db"), shutdown.openConnections))
        .withBean("shutdownLog", ShutdownLog.class, () -> shutdown)
        .withBean(
            "recordBundleClose", BeanPostProcessor.class, () -> new RecordBundleClose(shutdown))
        .withBean(SagaStoreReader.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              ctx.getBean(SagaStoreReader.class);
            });

    // The retention sweepers sweep right after start: their close() waits for that sweep, so no
    // connection is out when the bundle releases the instance guard.
    assertThat(shutdown.events).containsExactly("reader", "bundle with 0 connections out");
  }

  /** What was shut down, in order. */
  static final class ShutdownLog {
    final List<String> events = new CopyOnWriteArrayList<>();
    final java.util.concurrent.atomic.AtomicInteger openConnections =
        new java.util.concurrent.atomic.AtomicInteger();
  }

  /** An application bean that injects a framework store and records its own destruction. */
  static final class SagaStoreReader implements DisposableBean {
    private final SagaStore store;
    private final ShutdownLog shutdown;

    SagaStoreReader(SagaStore store, ShutdownLog shutdown) {
      this.store = store;
      this.shutdown = shutdown;
    }

    @Override
    public void destroy() {
      assertThat(store).isNotNull();
      shutdown.events.add("reader");
    }
  }

  /** Wraps the backend so the bundle it opens records its close. */
  static final class RecordBundleClose implements BeanPostProcessor {
    private final ShutdownLog shutdown;

    RecordBundleClose(ShutdownLog shutdown) {
      this.shutdown = shutdown;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
      if (!"streamRuneBackend".equals(beanName)) {
        return bean;
      }
      StreamRuneBackend real = (StreamRuneBackend) bean;
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
          BackendStores stores = spy(real.open(context));
          doAnswer(
                  invocation -> {
                    shutdown.events.add(
                        "bundle with " + shutdown.openConnections.get() + " connections out");
                    return invocation.callRealMethod();
                  })
              .when(stores)
              .close();
          return stores;
        }
      };
    }
  }

  private static Throwable rootCause(Throwable failure) {
    assertThat(failure).as("the start fails").isNotNull();
    Throwable root = failure;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }

  private static int occurrences(String text, String part) {
    int count = 0;
    for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
      count++;
    }
    return count;
  }
}
