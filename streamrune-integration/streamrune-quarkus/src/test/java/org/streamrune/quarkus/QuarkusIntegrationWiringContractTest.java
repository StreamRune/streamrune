package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.arc.BeanCreator;
import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.configuration.DurationConverter;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.ext.Provider;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStore;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.ConfigurationDocs;
import org.streamrune.integration.tck.ContractBeans;
import org.streamrune.integration.tck.ContractHttp;
import org.streamrune.integration.tck.HttpResponse;
import org.streamrune.integration.tck.IntegrationWiringContract;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The integration wiring contract on Quarkus. Each start boots a real Arc container ({@link
 * RealArcTestContainer}) over every bean class of the module, with the build-time conditions of the
 * application's configuration evaluated, then fires the startup event a Quarkus application fires;
 * closing fires the shutdown event and shuts the container down. What a Quarkus application has and
 * raw Arc does not is supplied the way the Quarkus extensions supply it: the SmallRye configuration
 * (with Quarkus' duration converter) and its two mappings, and the {@code DataSource}, which the
 * container disposes of when it shuts down, as quarkus-agroal closes its pool. An application
 * without a database declares the test kit's in-memory event store and offset store, and its {@code
 * DataSource} refuses every connection. A plain contract bean is a {@code Singleton} class bean, a
 * supplied one a synthetic bean of the type it supplies.
 *
 * <p>The SSE endpoint is served by Quarkus REST over HTTP ({@link QuarkusRestTestServer}): the
 * {@link SseController} and the {@link StreamRuneRequestFilter} are built with the container's
 * beans, the request-context holder a plain instance, as {@code SseWireFormatTest} builds them. The
 * health check is the container's {@link StreamRuneHealthCheck} bean.
 */
class QuarkusIntegrationWiringContractTest extends IntegrationWiringContract {

  /**
   * Every class of the module's main sources that Quarkus makes a bean, other than the two JAX-RS
   * classes the adapter builds itself ({@link SseController}, {@link StreamRuneRequestFilter}).
   */
  static final List<Class<?>> FRAMEWORK_BEANS =
      List.of(
          StreamRuneProducers.class,
          ProjectionProducer.class,
          StreamRuneCryptoProducers.FileSystemCryptoProducer.class,
          StreamRuneCryptoProducers.PostgresCryptoProducer.class,
          StreamRuneCryptoProducers.VaultCryptoProducer.class,
          StreamRuneCryptoProducers.AwsKmsCryptoProducer.class,
          StreamRuneLifecycle.class,
          StreamRuneConfigValidator.class,
          StreamRuneAuthorizationValidator.class,
          StreamRuneRequestIdentityValidator.class,
          SagaStateCryptoValidator.class,
          SagaEventPathRetentionConfigurer.class,
          SagaCompensationRetryLifecycle.class,
          SagaTimeoutRunnerHealthRegistrar.class,
          CryptoForgetListenHealthRegistrar.class,
          SseEventFeedLifecycle.class,
          StreamRuneHealthCheck.class,
          StreamRuneStartupLogger.class,
          StreamRuneDevServicesConfig.class,
          StreamRuneRequestContextHolder.class);

  /** Started by the first behaviour that needs PostgreSQL, stopped after the class. */
  private static PostgreSQLContainer<?> postgres;

  private static final AtomicInteger DATABASES = new AtomicInteger();

  @AfterAll
  static void stopPostgres() {
    if (postgres != null) {
      postgres.stop();
    }
  }

  /**
   * What a Quarkus application's extensions supply and raw Arc does not: the configuration, its
   * mappings and the {@code DataSource}. The adapter sets the fields before each boot.
   */
  @ApplicationScoped
  public static class ApplicationInfrastructure {

    static volatile SmallRyeConfig config;
    static volatile DataSource dataSource;

    @Produces
    @Singleton
    public org.eclipse.microprofile.config.Config config() {
      return config;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return config.getConfigMapping(StreamRuneQuarkusProperties.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusCryptoProperties cryptoProperties() {
      return config.getConfigMapping(StreamRuneQuarkusCryptoProperties.class);
    }

    @Produces
    @Singleton
    public DataSource dataSource() {
      return dataSource;
    }

    /** Closes the DataSource as quarkus-agroal closes its pool when the container shuts down. */
    public void closeDataSource(@Disposes DataSource dataSource) throws Exception {
      if (dataSource instanceof AutoCloseable closeable) {
        closeable.close();
      }
    }
  }

  /** The stores of an application without a database: the test kit's in-memory ones. */
  @ApplicationScoped
  public static class InMemoryStores {

    @Produces
    @Singleton
    public EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Produces
    @Singleton
    public OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }
  }

  /** Makes a supplied contract bean: the class named by the {@code bean} parameter supplies it. */
  public static class SuppliedBean implements BeanCreator<Object> {
    @Override
    public Object create(SyntheticCreationalContext<Object> context) {
      try {
        return ContractBeans.create(Class.forName((String) context.getParams().get("bean")));
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    ApplicationInfrastructure.config = configuration(properties);
    ApplicationInfrastructure.dataSource = dataSource != null ? dataSource : new NoDatabase();
    List<Class<?>> beans = new ArrayList<>(FRAMEWORK_BEANS);
    // The qualifier SmallRye Health registers: indexed, Arc reads it as one.
    beans.add(Readiness.class);
    beans.add(ApplicationInfrastructure.class);
    if (dataSource == null) {
      beans.add(InMemoryStores.class);
    }
    List<Class<?>> classes = new ArrayList<>();
    List<RealArcTestContainer.SyntheticBean> supplied = new ArrayList<>();
    for (Class<?> beanClass : extraBeans) {
      if (Supplier.class.isAssignableFrom(beanClass)) {
        supplied.add(
            new RealArcTestContainer.SyntheticBean(
                ContractBeans.beanType(beanClass),
                ContractBeans.beanClassOf(beanClass),
                SuppliedBean.class,
                Map.of("bean", beanClass.getName())));
      } else {
        classes.add(beanClass);
      }
    }
    CapturedLog log = new CapturedLog();
    RealArcTestContainer arc = null;
    try {
      arc = RealArcTestContainer.bootApplication(beans, properties, classes, supplied);
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
      QuarkusRestTestServer server = null;
      SseController controller = null;
      if (Boolean.parseBoolean(properties.get("streamrune.sse.enabled"))) {
        var holder = new StreamRuneRequestContextHolder();
        RequestIdentityPolicy policy = arc.container().instance(RequestIdentityPolicy.class).get();
        StreamRuneQuarkusProperties mapping =
            arc.container().instance(StreamRuneQuarkusProperties.class).get();
        controller =
            new SseController(
                arc.container().instance(SseEventPublisher.class).get(),
                arc.container().instance(SseAuthorizer.class).get(),
                policy,
                holder,
                () -> Boolean.TRUE,
                mapping);
        server =
            QuarkusRestTestServer.start(
                controller,
                List.of(
                    new StreamRuneRequestFilter(
                        holder, mapping, policy, arc.container().select(UserRoleResolver.class))));
      }
      return new QuarkusApplication(arc, server, controller, null, log.close());
    } catch (Throwable failure) {
      List<String> lines = log.close();
      if (arc != null) {
        arc.close();
      }
      return new QuarkusApplication(null, null, null, messages(failure), lines);
    }
  }

  /** The application's configuration as a Quarkus application binds it. */
  private static SmallRyeConfig configuration(Map<String, String> properties) {
    return new SmallRyeConfigBuilder()
        .withMapping(StreamRuneQuarkusProperties.class)
        .withMapping(StreamRuneQuarkusCryptoProperties.class)
        .withConverter(Duration.class, 100, new DurationConverter())
        .withSources(new PropertiesConfigSource(properties, "application.properties", 250))
        .build();
  }

  @Override
  protected DataSource postgresql() {
    synchronized (QuarkusIntegrationWiringContractTest.class) {
      if (postgres == null) {
        postgres = new PostgreSQLContainer<>(PostgresTestImage.NAME);
        postgres.start();
      }
    }
    String database = "contract_" + DATABASES.incrementAndGet();
    try (Connection admin =
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
    var oracle =
        ConfigurationDocLintTest.oracleOf(
            application.bean(StreamRuneQuarkusProperties.class),
            application.bean(StreamRuneQuarkusCryptoProperties.class));
    assertEquals(List.of(), oracle.conflicts());
    return oracle.keys();
  }

  /**
   * Every class of the module's main sources that Quarkus makes a bean is in {@link
   * #FRAMEWORK_BEANS}, or is one of the two JAX-RS classes the adapter builds: a new bean does not
   * stay out of the contract.
   */
  @Test
  void theContainerHoldsEveryBeanClassOfTheModule() throws ClassNotFoundException {
    Set<String> beanClasses = new TreeSet<>();
    moduleBeanClasses().forEach(c -> beanClasses.add(c.getName()));
    Set<String> covered = new TreeSet<>();
    FRAMEWORK_BEANS.forEach(c -> covered.add(c.getName()));
    covered.add(SseController.class.getName());
    covered.add(StreamRuneRequestFilter.class.getName());
    assertEquals(beanClasses, covered);
  }

  /**
   * Every class of the module's main sources, nested ones included, that Quarkus makes a bean: a
   * scope annotation, a JAX-RS provider or a JAX-RS resource.
   */
  private static List<Class<?>> moduleBeanClasses() throws ClassNotFoundException {
    List<Class<?>> beanClasses = new ArrayList<>();
    for (String type : ConfigurationDocs.typesDeclaredIn(java.nio.file.Path.of("src/main/java"))) {
      collectBeanClasses(
          Class.forName(type, false, QuarkusIntegrationWiringContractTest.class.getClassLoader()),
          beanClasses);
    }
    return beanClasses;
  }

  /**
   * Every bean class of the module is proxyable and observable as Arc meets it here, where the
   * classes Arc generates live in another classloader and no bytecode transformation makes a class
   * proxyable (a Quarkus application can switch that transformation off too): a normal-scoped one
   * is not final, has no final method a client proxy would have to override, has a non-private
   * no-argument constructor and marks its injecting constructor {@code Inject}, and every observer
   * and disposer method is public.
   */
  @Test
  void everyBeanClassIsProxyableAndObservableAsWritten() throws ClassNotFoundException {
    List<String> problems = new ArrayList<>();
    for (Class<?> bean : moduleBeanClasses()) {
      boolean normalScoped =
          bean.isAnnotationPresent(ApplicationScoped.class)
              || bean.isAnnotationPresent(RequestScoped.class);
      if (normalScoped && Modifier.isFinal(bean.getModifiers())) {
        problems.add(bean.getName() + " is final");
      }
      var constructors = List.of(bean.getDeclaredConstructors());
      if (normalScoped
          && constructors.stream()
              .noneMatch(
                  c -> c.getParameterCount() == 0 && !Modifier.isPrivate(c.getModifiers()))) {
        problems.add(bean.getName() + " has no non-private no-argument constructor");
      }
      if (normalScoped
          && constructors.stream().anyMatch(c -> c.getParameterCount() > 0)
          && constructors.stream()
              .noneMatch(c -> c.isAnnotationPresent(jakarta.inject.Inject.class))) {
        problems.add(bean.getName() + " marks no constructor @Inject");
      }
      for (Method method : bean.getDeclaredMethods()) {
        boolean observesOrDisposes =
            Arrays.stream(method.getParameters())
                .anyMatch(
                    p ->
                        p.isAnnotationPresent(jakarta.enterprise.event.Observes.class)
                            || p.isAnnotationPresent(Disposes.class));
        if (observesOrDisposes && !Modifier.isPublic(method.getModifiers())) {
          problems.add(bean.getName() + "#" + method.getName() + " is not public");
        }
        int modifiers = method.getModifiers();
        if (normalScoped
            && Modifier.isFinal(modifiers)
            && !Modifier.isPrivate(modifiers)
            && !Modifier.isStatic(modifiers)) {
          problems.add(bean.getName() + "#" + method.getName() + " is final");
        }
      }
    }
    assertEquals(List.of(), problems);
  }

  private static void collectBeanClasses(Class<?> type, List<Class<?>> into) {
    if (!type.isAnnotation()
        && (type.isAnnotationPresent(ApplicationScoped.class)
            || type.isAnnotationPresent(Singleton.class)
            || type.isAnnotationPresent(Dependent.class)
            || type.isAnnotationPresent(RequestScoped.class)
            || type.isAnnotationPresent(Provider.class)
            || type.isAnnotationPresent(Path.class))) {
      into.add(type);
    }
    for (Class<?> nested : type.getDeclaredClasses()) {
      collectBeanClasses(nested, into);
    }
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

  /**
   * The log records of level WARNING and above while it is open, through a java.util.logging
   * handler on the root logger: this module's tests bind SLF4J to JBoss LogManager, a JUL provider.
   */
  private static final class CapturedLog {
    private final Logger root = Logger.getLogger("");
    private final List<String> lines = new CopyOnWriteArrayList<>();
    private final Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord logRecord) {
            int level = logRecord.getLevel().intValue();
            if (level >= Level.WARNING.intValue()) {
              lines.add(
                  (level >= Level.SEVERE.intValue() ? "ERROR" : "WARN")
                      + " "
                      + logRecord.getLoggerName()
                      + ": "
                      + logRecord.getMessage());
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    CapturedLog() {
      root.addHandler(handler);
    }

    List<String> close() {
      root.removeHandler(handler);
      return List.copyOf(lines);
    }
  }

  /** The DataSource of an application without a database: it grants no connection. */
  static final class NoDatabase implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException("this application has no database");
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return java.util.logging.Logger.getGlobal();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
      throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
      return false;
    }
  }

  /** A started Quarkus application, or a refused start. */
  private final class QuarkusApplication implements Application {

    private final RealArcTestContainer arc;
    private final QuarkusRestTestServer server;
    private final SseController controller;
    private final String failure;
    private final List<String> logLines;

    QuarkusApplication(
        RealArcTestContainer arc,
        QuarkusRestTestServer server,
        SseController controller,
        String failure,
        List<String> logLines) {
      this.arc = arc;
      this.server = server;
      this.controller = controller;
      this.failure = failure;
      this.logLines = logLines;
    }

    @Override
    public boolean started() {
      return arc != null;
    }

    @Override
    public String startupFailureMessage() {
      return failure;
    }

    @Override
    public <T> T bean(Class<T> type) {
      return arc.container().instance(type).get();
    }

    @Override
    public HttpResponse http(String method, String path, Map<String, String> headers, String body) {
      return ContractHttp.send(server.port(), method, path, headers, body);
    }

    @Override
    public Map<String, Object> health() {
      HealthCheckResponse response =
          arc.container()
              .select(StreamRuneHealthCheck.class, Readiness.Literal.INSTANCE)
              .get()
              .call();
      if (!healthCheckName().equals(response.getName())) {
        return Map.of();
      }
      Map<String, Object> health = new LinkedHashMap<>();
      health.put("status", response.getStatus().name());
      health.put("details", response.getData().orElse(Map.of()));
      return health;
    }

    @Override
    public List<String> startupLogLines() {
      return logLines;
    }

    @Override
    public void close() {
      if (arc == null) {
        return;
      }
      try {
        if (server != null) {
          server.close();
        }
        if (controller != null) {
          controller.shutdown();
        }
        arc.container()
            .beanManager()
            .getEvent()
            .select(ShutdownEvent.class)
            .fire(new ShutdownEvent());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      } finally {
        arc.close();
      }
    }
  }
}
