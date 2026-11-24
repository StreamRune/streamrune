package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.tracing.StreamRuneTracing;

class BackendStartupTest {

  private static final DataSource DATA_SOURCE =
      (DataSource)
          Proxy.newProxyInstance(
              BackendStartupTest.class.getClassLoader(),
              new Class<?>[] {DataSource.class},
              (proxy, method, args) -> {
                throw new AssertionError("the data source is not touched: " + method.getName());
              });

  private static final BackendContext CONTEXT =
      new BackendContext(
          DATA_SOURCE,
          null,
          Clock.systemUTC(),
          StreamRuneMetrics.NOOP,
          StreamRuneTracing.NOOP,
          true,
          true,
          true);

  /** Records the calls the integration makes on the backend and on its stores. */
  private static class RecordingBackend implements StreamRuneBackend {
    final List<String> calls = new ArrayList<>();
    private final String name;
    private final String storesName;

    RecordingBackend(String name, String storesName) {
      this.name = name;
      this.storesName = storesName;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public void verify(DataSource dataSource) {
      assertThat(dataSource).isSameAs(DATA_SOURCE);
      calls.add("verify");
    }

    @Override
    public BackendStores open(BackendContext context) {
      calls.add("open");
      return (BackendStores)
          Proxy.newProxyInstance(
              BackendStartupTest.class.getClassLoader(),
              new Class<?>[] {BackendStores.class},
              (proxy, method, args) ->
                  switch (method.getName()) {
                    case "backendName" -> storesName;
                    case "close" -> {
                      calls.add("close");
                      yield null;
                    }
                    case "toString" -> "stores";
                    default -> throw new AssertionError("unexpected " + method.getName());
                  });
    }
  }

  @Test
  void aBackendOtherThanPostgresIsVerifiedBeforeItIsOpened() {
    var backend = new RecordingBackend("sqlite", "sqlite");

    BackendStores stores = BackendStartup.open(backend, CONTEXT);

    assertThat(stores.backendName()).isEqualTo("sqlite");
    assertThat(backend.calls).containsExactly("verify", "open");
  }

  @Test
  void postgresIsOpenedWithoutVerification_itsStoresCheckTheServerOnFirstUse() {
    var backend = new RecordingBackend("postgres", "postgres");

    BackendStartup.open(backend, CONTEXT);

    assertThat(backend.calls).containsExactly("open");
  }

  @Test
  void aVerificationFailureOpensNothing() {
    var backend =
        new RecordingBackend("sqlite", "sqlite") {
          @Override
          public void verify(DataSource dataSource) {
            throw new IllegalStateException("not a SQLite database");
          }
        };

    assertThatThrownBy(() -> BackendStartup.open(backend, CONTEXT))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("not a SQLite database");
    assertThat(backend.calls).isEmpty();
  }

  @Test
  void storesReportingAnotherNameAreClosedAndRefused() {
    var backend = new RecordingBackend("sqlite", "postgres");

    assertThatThrownBy(() -> BackendStartup.open(backend, CONTEXT))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The 'sqlite' backend opened stores that report the backend name 'postgres'; the"
                + " default read-model store must have one name");
    assertThat(backend.calls).containsExactly("verify", "open", "close");
  }

  @Test
  void openRefusesNulls() {
    assertThatThrownBy(() -> BackendStartup.open(null, CONTEXT))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("backend is required");
    assertThatThrownBy(() -> BackendStartup.open(new RecordingBackend("sqlite", "sqlite"), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("context is required");
  }

  @Test
  void aConfiguredStatementTimeoutIsIgnoredOnSqliteWithOneWarning() {
    assertThat(BackendStartup.statementTimeoutIgnored("sqlite", "5s"))
        .hasValue(
            "streamrune.event-store.statement-timeout=5s is ignored on SQLite: SQLite has no"
                + " statement timeout. Waiting for the write lock stays bounded by the"
                + " connection's busy_timeout; remove the property.");
  }

  @Test
  void anUnsetStatementTimeoutOrPostgresWarnsNothing() {
    assertThat(BackendStartup.statementTimeoutIgnored("sqlite", null)).isEmpty();
    assertThat(BackendStartup.statementTimeoutIgnored("postgres", "5s")).isEmpty();
  }

  @Test
  void theKeyStoreEngineMustBeTheSelectedBackends() {
    BackendStartup.requireKeyStoreEngineOfBackend("postgres", true, false);
    BackendStartup.requireKeyStoreEngineOfBackend("sqlite", false, true);
    BackendStartup.requireKeyStoreEngineOfBackend("sqlite", false, false);
    BackendStartup.requireKeyStoreEngineOfBackend("postgres", false, false);

    assertThatThrownBy(() -> BackendStartup.requireKeyStoreEngineOfBackend("sqlite", true, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.crypto.postgres.enabled=true enables the PostgreSQL key-store engine, but"
                + " streamrune.backend=sqlite: the key-store engine keeps its keys in the selected"
                + " backend's database. Set streamrune.crypto.sqlite.enabled=true instead, or"
                + " select streamrune.backend=postgres.");
    assertThatThrownBy(() -> BackendStartup.requireKeyStoreEngineOfBackend("postgres", false, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.crypto.sqlite.enabled=true enables the SQLite key-store engine, but"
                + " streamrune.backend=postgres: the key-store engine keeps its keys in the"
                + " selected backend's database. Set streamrune.crypto.postgres.enabled=true"
                + " instead, or select streamrune.backend=sqlite.");
  }
}
