package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.jackson.JacksonEndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementContextAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.context.LifecycleAutoConfiguration;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.health.autoconfigure.actuate.endpoint.HealthEndpointAutoConfiguration;
import org.springframework.boot.health.autoconfigure.contributor.HealthContributorAutoConfiguration;
import org.springframework.boot.health.autoconfigure.registry.HealthContributorRegistryAutoConfiguration;
import org.springframework.boot.http.converter.autoconfigure.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.servlet.autoconfigure.actuate.web.ServletManagementContextAutoConfiguration;
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.webmvc.autoconfigure.DispatcherServletAutoConfiguration;
import org.springframework.boot.webmvc.autoconfigure.WebMvcAutoConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.ResolvableType;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.ConfigurationOracle;
import org.streamrune.integration.tck.ContractBeans;
import org.streamrune.integration.tck.ContractHttp;
import org.streamrune.integration.tck.HttpResponse;
import org.streamrune.integration.tck.IntegrationWiringContract;
import org.streamrune.test.InMemoryEventStoreFactory;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The integration wiring contract on Spring Boot. Each start is a {@code SpringApplication} with an
 * embedded Tomcat on a free port: the servlet web stack, the actuator health endpoint, and every
 * StreamRune auto-configuration the module registers. A contract bean is registered the way an
 * application declares a bean, under its bean type; without a database the application declares the
 * test kit's in-memory event store factory and offset store, with one the {@code DataSource}, which
 * the context closes as it closes a connection pool.
 *
 * <p>spring-security-core is on this module's test classpath and the application imports no
 * security filter chain, so its request identity runs in the {@code AUTHENTICATED_PRINCIPAL} mode:
 * a request without a principal is anonymous.
 */
class SpringIntegrationWiringContractTest extends IntegrationWiringContract {

  /** Started by the first behaviour that needs PostgreSQL, stopped after the class. */
  private static PostgreSQLContainer<?> postgres;

  private static final AtomicInteger DATABASES = new AtomicInteger();

  private static final ObjectMapper JSON = new ObjectMapper();

  /** The servlet web stack, the actuator health endpoint and the StreamRune auto-configuration. */
  @Configuration(proxyBeanMethods = false)
  @ImportAutoConfiguration({
    PropertyPlaceholderAutoConfiguration.class,
    LifecycleAutoConfiguration.class,
    TomcatServletWebServerAutoConfiguration.class,
    DispatcherServletAutoConfiguration.class,
    WebMvcAutoConfiguration.class,
    HttpMessageConvertersAutoConfiguration.class,
    JacksonAutoConfiguration.class,
    EndpointAutoConfiguration.class,
    WebEndpointAutoConfiguration.class,
    JacksonEndpointAutoConfiguration.class,
    HealthContributorAutoConfiguration.class,
    HealthContributorRegistryAutoConfiguration.class,
    HealthEndpointAutoConfiguration.class,
    ManagementContextAutoConfiguration.class,
    ServletManagementContextAutoConfiguration.class,
    StreamRuneAutoConfiguration.class,
    ProjectionAutoConfig.class,
    CryptoPropertiesConfiguration.class,
    FileSystemCryptoEngineConfiguration.class,
    PostgresCryptoEngineConfiguration.class,
    VaultCryptoEngineConfiguration.class,
    AwsKmsCryptoEngineConfiguration.class
  })
  static class ContractApplication {}

  /** The StreamRune auto-configurations of {@link ContractApplication}, in the module's order. */
  private static final List<String> STREAMRUNE_AUTO_CONFIGURATIONS =
      List.of(
          StreamRuneAutoConfiguration.class.getName(),
          ProjectionAutoConfig.class.getName(),
          CryptoPropertiesConfiguration.class.getName(),
          FileSystemCryptoEngineConfiguration.class.getName(),
          PostgresCryptoEngineConfiguration.class.getName(),
          VaultCryptoEngineConfiguration.class.getName(),
          AwsKmsCryptoEngineConfiguration.class.getName());

  @AfterAll
  static void stopPostgres() {
    if (postgres != null) {
      postgres.stop();
    }
  }

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    ListAppender<ILoggingEvent> log = new ListAppender<>();
    log.start();
    List<String> configuration = new ArrayList<>();
    configuration.add("server.port=0");
    configuration.add("spring.main.banner-mode=off");
    configuration.add("management.endpoints.web.exposure.include=health");
    configuration.add("management.endpoint.health.show-details=always");
    properties.forEach((key, value) -> configuration.add(key + "=" + value));
    try {
      ConfigurableApplicationContext context =
          new SpringApplicationBuilder(ContractApplication.class)
              .web(WebApplicationType.SERVLET)
              .registerShutdownHook(false)
              // Spring Boot initialises the logging system before the context is prepared, and
              // that drops an appender attached earlier: the capture starts once it has.
              .listeners((ApplicationListener<ApplicationPreparedEvent>) _ -> root.addAppender(log))
              .initializers(
                  applicationContext ->
                      declare(
                          (GenericApplicationContext) applicationContext, dataSource, extraBeans))
              .properties(configuration.toArray(String[]::new))
              .run();
      return new SpringApplicationUnderTest(context, null, lines(log));
    } catch (RuntimeException e) {
      return new SpringApplicationUnderTest(null, messages(e), lines(log));
    } finally {
      root.detachAppender(log);
    }
  }

  /** Declares the application's own beans: its store, or its DataSource, and the contract's. */
  private static void declare(
      GenericApplicationContext context, DataSource dataSource, Set<Class<?>> extraBeans) {
    if (dataSource == null) {
      context.registerBean(
          EventStoreFactory.class, () -> InMemoryEventStoreFactory.builder().build());
      context.registerBean(OffsetStore.class, InMemoryOffsetStore::new);
    } else {
      context.registerBean("dataSource", DataSource.class, () -> dataSource);
    }
    for (Class<?> beanClass : extraBeans) {
      @SuppressWarnings("unchecked")
      Class<Object> type = (Class<Object>) ContractBeans.beanClassOf(beanClass);
      context.registerBean(
          beanClass.getName(),
          type,
          () -> ContractBeans.create(beanClass),
          definition ->
              ((RootBeanDefinition) definition)
                  .setTargetType(ResolvableType.forType(ContractBeans.beanType(beanClass))));
    }
  }

  @Override
  protected DataSource postgresql() {
    synchronized (SpringIntegrationWiringContractTest.class) {
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
    return "streamRune";
  }

  @Override
  protected Map<String, String> configurationDefaults(Application application) {
    try {
      ConfigurationOracle.Merged oracle =
          ConfigurationDocLintTest.oracleOf(
              application.bean(StreamRuneProperties.class),
              application.bean(StreamRuneCryptoProperties.class));
      assertEquals(List.of(), oracle.conflicts());
      return oracle.keys();
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The application imports every auto-configuration the module registers, so a new one does not
   * stay out of the contract.
   */
  @Test
  void theApplicationImportsEveryStreamRuneAutoConfiguration() throws IOException {
    String imports;
    try (var in =
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")) {
      imports = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    assertEquals(
        STREAMRUNE_AUTO_CONFIGURATIONS, imports.lines().filter(l -> !l.isBlank()).toList());
    List<Class<?>> imported =
        List.of(ContractApplication.class.getAnnotation(ImportAutoConfiguration.class).value());
    assertEquals(
        STREAMRUNE_AUTO_CONFIGURATIONS,
        imported.stream()
            .map(Class::getName)
            .filter(name -> name.startsWith("org.streamrune"))
            .toList());
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
      if (t.getMessage() != null && !messages.contains(t.getMessage())) {
        messages.add(t.getMessage());
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return String.join("\n", messages);
  }

  /** A started Spring application, or a refused start. */
  private final class SpringApplicationUnderTest implements Application {

    private final ConfigurableApplicationContext context;
    private final String failure;
    private final List<String> logLines;

    SpringApplicationUnderTest(
        ConfigurableApplicationContext context, String failure, List<String> logLines) {
      this.context = context;
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
      int port = ((WebServerApplicationContext) context).getWebServer().getPort();
      return ContractHttp.send(port, method, path, headers, body);
    }

    @Override
    public Map<String, Object> health() {
      HttpResponse response =
          http("GET", "/actuator/health", Map.of("Accept", "application/json"), null);
      Map<String, Object> endpoint;
      try {
        endpoint = JSON.readValue(response.body(), new TypeReference<Map<String, Object>>() {});
      } catch (IOException e) {
        throw new UncheckedIOException("the health endpoint answered " + response, e);
      }
      if (endpoint.get("components") instanceof Map<?, ?> components
          && components.get(healthCheckName()) instanceof Map<?, ?> check) {
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
        context.close();
      }
    }
  }
}
