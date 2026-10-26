package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.ContractBeans;
import org.streamrune.integration.tck.ContractBeans.ContractSagaState;
import org.streamrune.integration.tck.ContractBeans.UngatedSagaRunner;
import org.streamrune.integration.tck.ContractHttp;
import org.streamrune.integration.tck.HttpResponse;
import org.streamrune.integration.tck.IntegrationWiringContract;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The integration wiring contract on Micronaut. Each start is an {@code ApplicationContext} with
 * its embedded Netty server on a free port and the management health endpoint, Micronaut Security
 * switched off. The application's own beans are registered as singletons before the context starts,
 * so every bean condition sees them: without a database the test kit's in-memory event store and
 * offset store, and the contract's beans. Two are declared at compile time in {@link
 * ApplicationBeans} instead, as an application declares them: a {@code SagaRunner}, because
 * Micronaut does not reliably collect a runtime singleton into an injected {@code List}, and the
 * {@code DataSource}, because the context closes a bean through its {@code preDestroy} method. A
 * contract bean of another type that Micronaut collects into an injected {@code List} needs a
 * method there too.
 *
 * <p>{@link ApplicationBeans} hands the context the current start's {@code DataSource} through a
 * static field: the tests run one start at a time, in one thread.
 */
@Execution(ExecutionMode.SAME_THREAD)
class MicronautIntegrationWiringContractTest extends IntegrationWiringContract {

  /** The property that declares the {@link ApplicationBeans#dataSource()} of a start. */
  static final String DATA_SOURCE = "contract.data-source";

  /** The property that declares the {@link ApplicationBeans#ungatedSagaRunner()} of a start. */
  static final String UNGATED_SAGA_RUNNER = "contract.ungated-saga-runner";

  /**
   * The application's beans that Micronaut must know at compile time, each declared only for a
   * start that sets its property.
   */
  @Factory
  static class ApplicationBeans {

    /** The DataSource of the current start; one start runs at a time. */
    static volatile DataSource current;

    @Singleton
    @Bean(preDestroy = "close")
    @Requires(property = DATA_SOURCE, value = "true")
    ApplicationDataSource dataSource() {
      return new ApplicationDataSource(current);
    }

    @Singleton
    @Requires(property = UNGATED_SAGA_RUNNER, value = "true")
    SagaRunner<ContractSagaState> ungatedSagaRunner() {
      return new UngatedSagaRunner().get();
    }
  }

  /**
   * The application's DataSource as a bean the context closes: every call goes to the given one,
   * and closing it closes that one when it is closeable, as closing a connection pool does.
   */
  static final class ApplicationDataSource implements DataSource, AutoCloseable {

    private final DataSource delegate;

    ApplicationDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
      return delegate.getConnection();
    }

    @Override
    public java.sql.Connection getConnection(String username, String password) throws SQLException {
      return delegate.getConnection(username, password);
    }

    @Override
    public java.io.PrintWriter getLogWriter() throws SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) throws SQLException {
      delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger()
        throws java.sql.SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
      return type.isInstance(this) ? type.cast(this) : delegate.unwrap(type);
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
      return type.isInstance(this) || delegate.isWrapperFor(type);
    }

    @Override
    public void close() throws Exception {
      if (delegate instanceof AutoCloseable closeable) {
        closeable.close();
      }
    }
  }

  /** Started by the first behaviour that needs PostgreSQL, stopped after the class. */
  private static PostgreSQLContainer<?> postgres;

  private static final AtomicInteger DATABASES = new AtomicInteger();

  private static final ObjectMapper JSON = new ObjectMapper();

  @AfterAll
  static void stopPostgres() {
    if (postgres != null) {
      postgres.stop();
    }
  }

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    Map<String, Object> configuration = new HashMap<>(properties);
    configuration.put("micronaut.server.port", "-1");
    configuration.put("micronaut.security.enabled", "false");
    configuration.put("endpoints.health.details-visible", "ANONYMOUS");
    List<Object> singletons = new ArrayList<>();
    if (dataSource == null) {
      singletons.add(new InMemoryEventStore());
      singletons.add(new InMemoryOffsetStore());
    } else {
      ApplicationBeans.current = dataSource;
      configuration.put(DATA_SOURCE, "true");
    }
    for (Class<?> beanClass : extraBeans) {
      if (beanClass == UngatedSagaRunner.class) {
        configuration.put(UNGATED_SAGA_RUNNER, "true");
      } else {
        singletons.add(ContractBeans.create(beanClass));
      }
    }
    Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    ListAppender<ILoggingEvent> log = new ListAppender<>();
    log.start();
    root.addAppender(log);
    ApplicationContext context = null;
    try {
      context =
          ApplicationContext.builder()
              .properties(configuration)
              .singletons(singletons.toArray())
              .start();
      EmbeddedServer server = context.getBean(EmbeddedServer.class);
      server.start();
      return new MicronautApplication(context, server, null, lines(log));
    } catch (RuntimeException e) {
      if (context != null && context.isRunning()) {
        context.close();
      }
      return new MicronautApplication(null, null, messages(e), lines(log));
    } finally {
      root.detachAppender(log);
    }
  }

  @Override
  protected DataSource postgresql() {
    synchronized (MicronautIntegrationWiringContractTest.class) {
      if (postgres == null) {
        postgres = new PostgreSQLContainer<>(PostgresTestImage.NAME);
        postgres.start();
      }
    }
    String database = "contract_" + DATABASES.incrementAndGet();
    try (var admin =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        var statement = admin.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
    } catch (SQLException e) {
      throw new IllegalStateException("cannot create the database " + database, e);
    }
    var dataSource = new PGSimpleDataSource();
    dataSource.setServerNames(new String[] {postgres.getHost()});
    dataSource.setPortNumbers(
        new int[] {postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)});
    dataSource.setDatabaseName(database);
    dataSource.setUser(postgres.getUsername());
    dataSource.setPassword(postgres.getPassword());
    return dataSource;
  }

  @Override
  protected String healthCheckName() {
    return "streamrune";
  }

  @Override
  protected Map<String, String> configurationDefaults(Application application) {
    try {
      var oracle =
          ConfigurationDocLintTest.oracleOf(
              application.bean(StreamRuneMicronautProperties.class),
              application.bean(StreamRuneMicronautCryptoProperties.class));
      assertEquals(List.of(), oracle.conflicts());
      return oracle.keys();
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private static List<String> lines(ListAppender<ILoggingEvent> log) {
    List<String> lines = new ArrayList<>();
    for (ILoggingEvent event : List.copyOf(log.list)) {
      if (event.getLevel().isGreaterOrEqual(Level.WARN)) {
        lines.add(
            event.getLevel() + " " + event.getLoggerName() + ": " + event.getFormattedMessage());
      }
    }
    return lines;
  }

  private static String messages(Throwable failure) {
    List<String> messages = new ArrayList<>();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      String message = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
      if (!messages.contains(message)) {
        messages.add(message);
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return String.join("\n", messages);
  }

  /** A started Micronaut application, or a refused start. */
  private final class MicronautApplication implements Application {

    private final ApplicationContext context;
    private final EmbeddedServer server;
    private final String failure;
    private final List<String> logLines;

    MicronautApplication(
        ApplicationContext context, EmbeddedServer server, String failure, List<String> logLines) {
      this.context = context;
      this.server = server;
      this.failure = failure;
      this.logLines = logLines;
    }

    @Override
    public boolean started() {
      return context != null;
    }

    @Override
    public String startupFailureMessage() {
      return failure;
    }

    @Override
    public <T> T bean(Class<T> type) {
      return context.getBean(type);
    }

    @Override
    public HttpResponse http(String method, String path, Map<String, String> headers, String body) {
      return ContractHttp.send(server.getPort(), method, path, headers, body);
    }

    @Override
    public Map<String, Object> health() {
      HttpResponse response = http("GET", "/health", Map.of("Accept", "application/json"), null);
      Map<String, Object> endpoint;
      try {
        endpoint = JSON.readValue(response.body(), new TypeReference<Map<String, Object>>() {});
      } catch (IOException e) {
        throw new UncheckedIOException("the health endpoint answered " + response, e);
      }
      if (endpoint.get("details") instanceof Map<?, ?> checks
          && checks.get(healthCheckName()) instanceof Map<?, ?> check) {
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("status", check.get("status"));
        health.put("details", check.get("details"));
        return health;
      }
      return Map.of();
    }

    @Override
    public List<String> startupLogLines() {
      return logLines;
    }

    @Override
    public void close() {
      if (context != null) {
        server.stop();
        context.close();
      }
    }
  }
}
