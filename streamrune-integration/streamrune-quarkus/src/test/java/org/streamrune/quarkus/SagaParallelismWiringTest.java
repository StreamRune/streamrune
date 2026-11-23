package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.DotEnvConfigSourceProvider;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import javax.sql.DataSource;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.postgres.PgAdvisoryLocker;
import org.streamrune.quarkus.SagaEventPathRetentionContainerWiringTest.FakeOrchestrator;
import org.streamrune.quarkus.SagaEventPathRetentionContainerWiringTest.FakeState;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaParallelismDefault;
import org.streamrune.runtime.SagaParallelismSource;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Saga parallelism through a real Arc container: the global property, the per-subscription
 * property, the startup check (unknown names, one line per runner, the refusal, the pool warning).
 * The container reads one configuration, as a Quarkus application does: the mapping bound on it,
 * with unknown keys under {@code streamrune} refused, and the same object as its {@code Config}
 * bean.
 */
class SagaParallelismWiringTest {

  private static final String SAGA_TYPE = SagaType.fromClass(FakeState.class).value();

  /** The marker {@link #start} records in the log when the start fails. */
  private static final String START_FAILED = "<the start failed>";

  // ---- what the container under test is handed ------------------------------------------------

  static volatile SmallRyeConfig applicationConfig;
  static volatile SagaRunner<FakeState> sagaRunner;
  static volatile String subscriptionName;
  static volatile DataSource dataSource;
  static volatile AggregateLocker locker;

  @BeforeEach
  void theDefaultLockerAndNoPool() {
    // What the producers supply when the application defines no locker.
    locker = new LocalStripedLocker(64);
    dataSource = null;
  }

  /** The bound configuration of the application: the mapping of {@link #applicationConfig}. */
  public static class Infrastructure {
    @Produces
    @Singleton
    public org.eclipse.microprofile.config.Config config() {
      return applicationConfig;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return applicationConfig.getConfigMapping(StreamRuneQuarkusProperties.class);
    }
  }

  /** The application's saga runner. */
  public static class RunnerBean {
    @Produces
    @Singleton
    @Named("orderSagaRunner")
    public SagaRunner<FakeState> runner() {
      return sagaRunner;
    }
  }

  /** A subscription built over the runner when the startup event fires, before the check. */
  public static class SubscriptionBuilder {
    PollingEventSubscription subscription;

    public void onStart(@Observes StartupEvent event) {
      subscription = subscription(subscriptionName, sagaRunner);
    }
  }

  public static class DataSourceBean {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return dataSource;
    }
  }

  public static class LockerBean {
    @Produces
    @Singleton
    public AggregateLocker locker() {
      return locker;
    }
  }

  private static SagaRunner<FakeState> newRunner(Integer builderParallelism) {
    var store = new InMemorySagaStore();
    var builder =
        SagaRunner.<FakeState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(store)
            .commandBus(SagaEventPathRetentionContainerWiringTest.NOOP_BUS)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
            .forwardFence(false);
    if (builderParallelism != null) {
      builder.parallelism(builderParallelism);
    }
    return builder.build();
  }

  private static PollingEventSubscription subscription(String name, SagaRunner<?> runner) {
    return PollingEventSubscription.builder()
        .subscriptionName(SubscriptionName.of(name))
        .eventStore(new InMemoryEventStore(SimpleEventTypeRegistry.builder().build()))
        .offsetStore(new InMemoryOffsetStore())
        .config(
            org.streamrune.core.subscription.SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
        .sagaRunner(runner)
        .build();
  }

  // ---- the configuration ----------------------------------------------------------------------

  /**
   * The application's configuration over {@code sources}: the mapping registered on it as Quarkus
   * registers it, so that a key under {@code streamrune} the mapping does not declare is refused.
   */
  private static SmallRyeConfig config(ConfigSource... sources) {
    return config(null, sources);
  }

  /** The same with {@code profile} active, when it is not {@code null}. */
  private static SmallRyeConfig config(String profile, ConfigSource... sources) {
    var builder =
        new SmallRyeConfigBuilder()
            .addDefaultInterceptors()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withSources(sources);
    if (profile != null) {
      builder.withProfile(profile);
    }
    return builder.build();
  }

  /** An {@code application.properties} of {@code values}. */
  private static ConfigSource file(Map<String, String> values) {
    return new PropertiesConfigSource(values, "application.properties", 250);
  }

  /** The environment variables {@code values}. */
  private static ConfigSource environment(Map<String, String> values) {
    return new EnvConfigSource(values, EnvConfigSource.ORDINAL);
  }

  /** A {@code .env} file of {@code lines} in {@code directory}, as the configuration loads it. */
  private static ConfigSource dotEnv(Path directory, String... lines) throws java.io.IOException {
    Path file = Files.write(directory.resolve(".env"), List.of(lines));
    // The provider also hands back a source for profile-specific files beside it (none here).
    List<ConfigSource> sources =
        DotEnvConfigSourceProvider.dotEnvSources(
                file.toUri().toString(), SagaParallelismWiringTest.class.getClassLoader())
            .stream()
            .filter(source -> source.getName().startsWith(EnvConfigSource.NAME))
            .toList();
    assertThat(sources).hasSize(1);
    assertThat(sources.getFirst().getOrdinal()).isEqualTo(295);
    return sources.getFirst();
  }

  // ---- booting --------------------------------------------------------------------------------

  /** The log records of the startup check while it is open. */
  private static final class CapturedLog {
    private final java.util.logging.Logger logger =
        java.util.logging.Logger.getLogger(SagaParallelismStartupCheck.class.getName());
    private final Level originalLevel = logger.getLevel();
    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    private final Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord logRecord) {
            records.add(logRecord);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    CapturedLog() {
      logger.addHandler(handler);
      logger.setLevel(Level.ALL);
    }

    List<String> messages(Level level) {
      return records.stream()
          .filter(r -> r.getLevel().equals(level))
          .map(LogRecord::getMessage)
          .toList();
    }

    void close() {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
    }
  }

  /** What one start did: the log it wrote, and the failure it ended with. */
  record Started(CapturedLog log, Throwable failure) {
    List<String> info() {
      return log.messages(Level.INFO);
    }

    List<String> warn() {
      return log.messages(Level.WARNING);
    }

    /** Every record in the order it was written, {@link #START_FAILED} where the start failed. */
    List<String> sequence() {
      return log.records.stream().map(r -> r.getLevel() + " " + r.getMessage()).toList();
    }
  }

  /** The text of every message in the chain of {@code failure}. */
  private static String chain(Throwable failure) {
    StringBuilder text = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      text.append(t.getMessage()).append('\n');
      if (t.getCause() == t) {
        break;
      }
    }
    return text.toString();
  }

  /**
   * Boots a container with the configurer, the check, the locker and the given beans over {@code
   * config}, fires the startup event {@code events} times and returns what happened.
   */
  private static Started start(
      SmallRyeConfig config, int events, boolean withRunner, List<Class<?>> extra)
      throws Exception {
    applicationConfig = config;
    var log = new CapturedLog();
    Throwable failure = null;
    try {
      var beans = new ArrayList<Class<?>>();
      beans.add(SagaEventPathRetentionConfigurer.class);
      beans.add(SagaParallelismStartupCheck.class);
      beans.add(Infrastructure.class);
      beans.add(LockerBean.class);
      if (withRunner) {
        beans.add(RunnerBean.class);
      }
      beans.addAll(extra);
      try (var arc = RealArcTestContainer.boot(beans)) {
        for (int i = 0; i < events; i++) {
          arc.container()
              .beanManager()
              .getEvent()
              .select(StartupEvent.class)
              .fire(new StartupEvent());
        }
      }
    } catch (Exception e) {
      failure = e;
      log.records.add(new LogRecord(Level.OFF, START_FAILED));
    } finally {
      log.close();
    }
    return new Started(log, failure);
  }

  /** One start over {@code config} with the runner bound to {@code subscription}. */
  private static Started start(SmallRyeConfig config, String subscription, Class<?>... extra)
      throws Exception {
    subscriptionName = subscription;
    var beans = new ArrayList<Class<?>>(List.of(extra));
    beans.add(SubscriptionBuilder.class);
    return start(config, 1, true, beans);
  }

  /** One start over an {@code application.properties} of {@code properties}. */
  private static Started start(
      Map<String, String> properties, String subscription, Class<?>... extra) throws Exception {
    return start(config(file(properties)), subscription, extra);
  }

  private static int effective(SagaRunner<?> saga) {
    return SagaParallelismDefault.effectiveOf(saga);
  }

  private static void hasNotFailed(Started started) {
    assertThat(started.failure()).as("the start").isNull();
  }

  private static void failedWith(Started started, String... texts) {
    assertThat(started.failure()).as("the start must fail").isNotNull();
    for (String text : texts) {
      assertThat(chain(started.failure())).contains(text);
    }
  }

  private static String notAProperty(String key) {
    return key + " is not a property; set streamrune.saga.subscriptions.<name>.parallelism";
  }

  // ---- the properties -------------------------------------------------------------------------

  @Test
  void theGlobalPropertyReachesARunnerThatNamedNothing() throws Exception {
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(start(Map.of("streamrune.saga.parallelism", "8"), "order-saga"));
    assertThat(effective(saga)).isEqualTo(8);
    assertThat(SagaParallelismDefault.sourceOf(saga))
        .isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
  }

  @Test
  void aRunnerThatNamedItsParallelismKeepsItAgainstTheGlobalProperty() throws Exception {
    var saga = sagaRunner = newRunner(2);
    hasNotFailed(start(Map.of("streamrune.saga.parallelism", "8"), "order-saga"));
    assertThat(effective(saga)).isEqualTo(2);
  }

  @Test
  void aSubscriptionPropertyWinsOverTheBuilder() throws Exception {
    var saga = sagaRunner = newRunner(2);
    hasNotFailed(
        start(Map.of("streamrune.saga.subscriptions.order-saga.parallelism", "16"), "order-saga"));
    assertThat(effective(saga)).isEqualTo(16);
    assertThat(SagaParallelismDefault.sourceOf(saga))
        .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
  }

  @Test
  void theConfigurationStillRefusesAMisspeltKeyBesideTheSubscriptions() {
    assertThatThrownBy(() -> config(file(Map.of("streamrune.saga.paralelism", "3"))))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining("SRCFG00050")
        .hasMessageContaining("streamrune.saga.paralelism");
  }

  @Test
  void aSubscriptionNameWithADotIsMatchedExactly() throws Exception {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.\"orders.v2\".parallelism",
            "streamrune.saga.subscriptions.orders.v2.parallelism")) {
      var saga = sagaRunner = newRunner(null);
      hasNotFailed(start(Map.of(key, "3"), "orders.v2"));
      assertThat(effective(saga)).as(key).isEqualTo(3);
      assertThat(SagaParallelismDefault.sourceOf(saga))
          .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
    }
  }

  @Test
  void aSubscriptionNameWithUpperCaseIsMatchedExactly() throws Exception {
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(start(Map.of("streamrune.saga.subscriptions.Orders.parallelism", "3"), "Orders"));
    assertThat(effective(saga)).isEqualTo(3);
    assertThat(SagaParallelismDefault.sourceOf(saga))
        .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
  }

  @Test
  void aKeyUnderSubscriptionsThatIsNotParallelismRefusesTheStart() throws Exception {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.order-saga.paralelism",
            "streamrune.saga.subscriptions.\"orders.v2\".paralelism",
            "streamrune.saga.subscriptions.parallelism",
            "streamrune.saga.subscriptions.orders",
            "streamrune.saga.subscriptions")) {
      sagaRunner = newRunner(null);
      failedWith(start(Map.of(key, "3"), "order-saga"), notAProperty(key));
    }
    sagaRunner = newRunner(null);
    failedWith(
        start(
            config(environment(Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALELISM", "3"))),
            "order-saga"),
        notAProperty("streamrune.saga.subscriptions.orders.paralelism"));
  }

  @Test
  void aKeyThatIsNotAPropertyIsNamedSanitizedForTheLog() throws Exception {
    sagaRunner = newRunner(null);
    failedWith(
        start(Map.of("streamrune.saga.subscriptions.order\nsaga.paralelism", "3"), "order-saga"),
        notAProperty("streamrune.saga.subscriptions.ordersaga.paralelism"));
  }

  @Test
  void anEnvironmentVariableReadsUnderscoresAsDotsAndRefusesTheUnknownName() throws Exception {
    sagaRunner = newRunner(null);
    var values = Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDER_SAGA_PARALLELISM", "8");
    failedWith(
        start(config(environment(values)), "order-saga"),
        "bound to: [order.saga]",
        "Subscriptions bound to a saga runner: [order-saga]");
  }

  @Test
  void anEnvironmentVariableNamesASubscriptionWithoutHyphenOrDot() throws Exception {
    var saga = sagaRunner = newRunner(null);
    var values = Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8");
    hasNotFailed(start(config(environment(values)), "orders"));
    assertThat(effective(saga)).isEqualTo(8);
  }

  @Test
  void anEnvironmentVariableWithDoubleUnderscoresNamesOneQuotedElement() throws Exception {
    var saga = sagaRunner = newRunner(null);
    var values = Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS__ORDERS_V2__PARALLELISM", "8");
    hasNotFailed(start(config(environment(values)), "orders.v2"));
    assertThat(effective(saga)).isEqualTo(8);
  }

  @Test
  void anEnvironmentVariableOfAnotherSpellingNamesASubscriptionOfItsOwn() throws Exception {
    // Each key takes the value the configuration resolves for it; the variable's own spelling is a
    // second name, which must belong to a subscription.
    var file = Map.of("streamrune.saga.subscriptions.order-saga.parallelism", "4");
    var variable = Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDER_SAGA_PARALLELISM", "16");
    assertThat(SagaParallelismProperties.bySubscription(config(file(file), environment(variable))))
        .containsExactlyInAnyOrderEntriesOf(Map.of("order-saga", "16", "order.saga", "16"));
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    file(Map.of("streamrune.saga.subscriptions.Orders.parallelism", "4")),
                    environment(Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "16")))))
        .containsExactlyInAnyOrderEntriesOf(Map.of("Orders", "16", "orders", "16"));

    sagaRunner = newRunner(null);
    failedWith(
        start(config(file(file), environment(variable)), "order-saga"),
        "bound to: [order.saga]",
        "Subscriptions bound to a saga runner: [order-saga]");
  }

  @Test
  void anEnvironmentVariableOfTheSameSpellingOverridesTheFile() throws Exception {
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            config(
                file(Map.of("streamrune.saga.subscriptions.orders.parallelism", "4")),
                environment(Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "16"))),
            "orders"));
    assertThat(effective(saga)).isEqualTo(16);
  }

  @Test
  void anEnvironmentVariableWhoseNameStartsWithANumberIsRead() {
    for (String variable :
        List.of(
            "STREAMRUNE_SAGA_SUBSCRIPTIONS_2024_ORDERS_PARALLELISM",
            "streamrune_saga_subscriptions_2024_orders_parallelism")) {
      assertThat(
              SagaParallelismProperties.bySubscription(config(environment(Map.of(variable, "8")))))
          .as(variable)
          .containsExactlyEntriesOf(Map.of("2024.orders", "8"));
    }
  }

  @Test
  void aValueSetInTwoSourcesTakesTheOneOfHigherPrecedence() throws Exception {
    var saga = sagaRunner = newRunner(null);
    var config =
        config(
            new PropertiesConfigSource(
                Map.of("streamrune.saga.subscriptions.orders.v2.parallelism", "16"), "high", 1000),
            new PropertiesConfigSource(
                Map.of("streamrune.saga.subscriptions.\"orders.v2\".parallelism", "3"),
                "low",
                100));
    hasNotFailed(start(config, "orders.v2"));
    assertThat(effective(saga)).isEqualTo(16);
  }

  @Test
  void twoKeysOfOneNameAtTheSamePrecedenceRefuseTheStartNamingBoth() throws Exception {
    sagaRunner = newRunner(null);
    var values = new LinkedHashMap<String, String>();
    values.put("streamrune.saga.subscriptions.orders.v2.parallelism", "3");
    values.put("streamrune.saga.subscriptions.\"orders.v2\".parallelism", "5");
    failedWith(
        start(values, "orders.v2"),
        "streamrune.saga.subscriptions.\"orders.v2\".parallelism and"
            + " streamrune.saga.subscriptions.orders.v2.parallelism both set the parallelism of"
            + " the saga subscription 'orders.v2' with the same precedence; keep one of them");
  }

  @Test
  void variablesOfOneKeyInDifferentCaseTakeTheConfigurationsValue(@TempDir Path directory)
      throws Exception {
    // The process environment (ordinal 300) over a .env file (295): the configuration's value.
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            config(
                environment(Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "16")),
                dotEnv(directory, "streamrune_saga_subscriptions_orders_parallelism=8")),
            "orders"));
    assertThat(effective(saga)).isEqualTo(16);

    // Both spellings in one source: still one key, and the configuration's value.
    var config =
        config(
            environment(
                Map.of(
                    "STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "3",
                    "streamrune_saga_subscriptions_orders_parallelism", "5")));
    assertThat(SagaParallelismProperties.bySubscription(config))
        .containsExactlyEntriesOf(
            Map.of(
                "orders",
                config.getValue("streamrune.saga.subscriptions.orders.parallelism", String.class)));
  }

  // ---- keys with brackets ---------------------------------------------------------------------

  /** A system property source of {@code values}. */
  private static ConfigSource systemProperties(Map<String, String> values) {
    return new PropertiesConfigSource(values, "SysPropConfigSource", 400);
  }

  @Test
  void aKeyWithBracketsIsReadAsWritten() throws Exception {
    var key = "streamrune.saga.subscriptions.shard[0].parallelism";
    assertThat(SagaParallelismProperties.bySubscription(config(file(Map.of(key, "8")))))
        .containsExactlyEntriesOf(Map.of("shard[0]", "8"));
    assertThat(SagaParallelismProperties.bySubscription(config(systemProperties(Map.of(key, "8")))))
        .containsExactlyEntriesOf(Map.of("shard[0]", "8"));
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    file(Map.of("streamrune.saga.subscriptions.\"orders[eu]\".parallelism", "8")))))
        .containsExactlyEntriesOf(Map.of("orders[eu]", "8"));

    var saga = sagaRunner = newRunner(null);
    hasNotFailed(start(Map.of(key, "8"), "shard[0]"));
    assertThat(effective(saga)).isEqualTo(8);
    assertThat(SagaParallelismDefault.sourceOf(saga))
        .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
  }

  @Test
  void aListItemUnderASubscriptionRefusesTheStartAsTheNameItWrites() throws Exception {
    // A YAML file lists "orders:" with the item "- parallelism: 8" under this key.
    sagaRunner = newRunner(null);
    failedWith(
        start(Map.of("streamrune.saga.subscriptions.orders[0].parallelism", "8"), "orders"),
        "bound to: [orders[0]]",
        "Subscriptions bound to a saga runner: [orders]");
  }

  @Test
  void aKeyWithBracketsBesideItsDottedTwinKeepsBoth() {
    var values = new LinkedHashMap<String, String>();
    values.put("streamrune.saga.subscriptions.orders[0].parallelism", "8");
    values.put("streamrune.saga.subscriptions.orders.0.parallelism", "5");
    assertThat(SagaParallelismProperties.bySubscription(config(file(values))))
        .containsExactlyInAnyOrderEntriesOf(Map.of("orders[0]", "8", "orders.0", "5"));

    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    systemProperties(
                        Map.of("streamrune.saga.subscriptions.shard[0].parallelism", "8")),
                    environment(
                        Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_SHARD_0_PARALLELISM", "16")))))
        .containsExactlyInAnyOrderEntriesOf(Map.of("shard[0]", "8", "shard.0", "16"));
  }

  @Test
  void aKeyWithAnIndexAfterParallelismRefusesTheStartNamingTheKeyAsWritten() throws Exception {
    var key = "streamrune.saga.subscriptions.orders.parallelism[0]";
    sagaRunner = newRunner(null);
    failedWith(start(Map.of(key, "8"), "orders"), notAProperty(key));
  }

  @Test
  void aVariableWithANumberBeforeADoubleUnderscoreIsRead() throws Exception {
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    environment(
                        Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_7__PARALLELISM", "8")))))
        .containsExactlyEntriesOf(Map.of("orders.7", "8"));

    // Beside a file key of the name it spells, the variable's higher precedence wins.
    var file = Map.of("streamrune.saga.subscriptions.orders.2024.parallelism", "3");
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    file(file),
                    environment(
                        Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_2024__PARALLELISM", "8")))))
        .containsExactlyEntriesOf(Map.of("orders.2024", "8"));
    // The variable that is the file key's own spelling sets that key: one setting, not two.
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    file(file),
                    environment(
                        Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_2024_PARALLELISM", "8")))))
        .containsExactlyEntriesOf(Map.of("orders.2024", "8"));

    var saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            config(
                file(file),
                environment(Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_2024__PARALLELISM", "8"))),
            "orders.2024"));
    assertThat(effective(saga)).isEqualTo(8);
  }

  // ---- profiles -------------------------------------------------------------------------------

  @Test
  void aVariableOfTheActiveProfileIsRead() throws Exception {
    var profiled = Map.of("_PROD_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8");
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(start(config("prod", environment(profiled)), "orders"));
    assertThat(effective(saga)).isEqualTo(8);
    assertThat(SagaParallelismDefault.sourceOf(saga))
        .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);

    // Beside the plain key in a file, and beside the plain variable: the profile's value.
    saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            config(
                "prod",
                file(Map.of("streamrune.saga.subscriptions.orders.parallelism", "3")),
                environment(profiled)),
            "orders"));
    assertThat(effective(saga)).isEqualTo(8);
    var both = new LinkedHashMap<String, String>(profiled);
    both.put("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "5");
    assertThat(SagaParallelismProperties.bySubscription(config("prod", environment(both))))
        .containsExactlyEntriesOf(Map.of("orders", "8"));
  }

  @Test
  void aDotEnvLineOfTheActiveProfileIsRead(@TempDir Path directory) throws Exception {
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            config(
                "dev",
                dotEnv(directory, "_DEV_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM=2")),
            "orders"));
    assertThat(effective(saga)).isEqualTo(2);

    saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            config(
                "dev",
                file(Map.of("streamrune.saga.subscriptions.orders.parallelism", "6")),
                dotEnv(directory, "_DEV_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM=2")),
            "orders"));
    assertThat(effective(saga)).isEqualTo(2);
  }

  @Test
  void aProfileKeyBesideThePlainKeyWinsAsTheConfigurationResolvesIt() throws Exception {
    var values = new LinkedHashMap<String, String>();
    values.put("%prod.streamrune.saga.subscriptions.orders.parallelism", "8");
    values.put("streamrune.saga.subscriptions.orders.parallelism", "3");
    var config = config("prod", file(values));
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(start(config, "orders"));
    assertThat(effective(saga)).isEqualTo(8);
    assertThat(SagaParallelismProperties.bySubscription(config))
        .containsExactlyEntriesOf(
            Map.of(
                "orders",
                config.getValue("streamrune.saga.subscriptions.orders.parallelism", String.class)));

    // A key of several profiles counts while one of them is active.
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    "staging",
                    file(
                        Map.of(
                            "%prod,staging.streamrune.saga.subscriptions.\"orders.v2\".parallelism",
                            "4")))))
        .containsExactlyEntriesOf(Map.of("orders.v2", "4"));
  }

  @Test
  void aKeyOfAnInactiveProfileIsNotApplied() throws Exception {
    for (String profile : new String[] {null, "dev"}) {
      var saga = sagaRunner = newRunner(null);
      var started =
          start(
              config(
                  profile,
                  file(
                      Map.of(
                          "%prod.streamrune.saga.subscriptions.orders.parallelism", "8",
                          "%prod.streamrune.saga.subscriptions.nope.parallelism", "8",
                          "%prod.streamrune.saga.subscriptions.orders.paralelism", "8")),
                  environment(
                      Map.of("_PROD_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8"))),
              "orders");
      hasNotFailed(started);
      assertThat(effective(saga)).as("profile %s", profile).isEqualTo(4);
      assertThat(SagaParallelismDefault.sourceOf(saga))
          .isNotEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
    }
  }

  @Test
  void aMisspeltKeyOfTheActiveProfileRefusesTheStart() throws Exception {
    sagaRunner = newRunner(null);
    failedWith(
        start(
            config(
                "prod", file(Map.of("%prod.streamrune.saga.subscriptions.orders.paralelism", "8"))),
            "orders"),
        notAProperty("streamrune.saga.subscriptions.orders.paralelism"));
    sagaRunner = newRunner(null);
    failedWith(
        start(
            config(
                "prod",
                environment(Map.of("_PROD_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALELISM", "8"))),
            "orders"),
        notAProperty("streamrune.saga.subscriptions.orders.paralelism"));
  }

  @Test
  void aVariableOfAProfileWhoseNameAVariableCannotSpellIsRead() throws Exception {
    for (String profile : List.of("prod-eu", "prod_eu", "PROD.EU")) {
      var config =
          config(
              profile,
              environment(
                  Map.of("_PROD_EU_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8")));
      assertThat(config.getValue("streamrune.saga.subscriptions.orders.parallelism", String.class))
          .as(profile)
          .isEqualTo("8");
      assertThat(SagaParallelismProperties.bySubscription(config))
          .as(profile)
          .containsExactlyEntriesOf(Map.of("orders", "8"));
    }
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            config(
                "prod-eu",
                environment(
                    Map.of("_PROD_EU_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8"))),
            "orders"));
    assertThat(effective(saga)).isEqualTo(8);

    // Another active profile that only starts the variable's name: the key is not one of ours.
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    "prod",
                    environment(
                        Map.of("_PROD_EU_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8")))))
        .isEmpty();
  }

  @Test
  void aVariableThatOnlyStartsWithThePrefixIsNotClaimed() throws Exception {
    var variables =
        Map.of(
            "STREAMRUNE_SAGA_SUBSCRIPTIONSX_FOO", "8",
            "STREAMRUNE_SAGA_SUBSCRIPTIONSX_ORDERS_PARALLELISM", "8");
    assertThat(SagaParallelismProperties.bySubscription(config(environment(variables)))).isEmpty();
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(start(config(environment(variables)), "orders"));
    assertThat(effective(saga)).isEqualTo(4);
  }

  @Test
  void aPlaceholderBesideAVariableOfAnotherSpellingRefusesTheUnknownName() throws Exception {
    sagaRunner = newRunner(null);
    failedWith(
        start(
            config(
                file(Map.of("streamrune.saga.subscriptions.order-saga.parallelism", "${par:4}")),
                environment(Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDER_SAGA_PARALLELISM", "16"))),
            "order-saga"),
        "bound to: [order.saga]",
        "Subscriptions bound to a saga runner: [order-saga]");
  }

  @Test
  void aPropertyWithoutAValueRefusesTheStart() throws Exception {
    sagaRunner = newRunner(null);
    failedWith(
        start(Map.of("streamrune.saga.subscriptions.order-saga.parallelism", ""), "order-saga"),
        "streamrune.saga.subscriptions.order-saga.parallelism must be a whole number");
  }

  @Test
  void aPlaceholderInTheValueIsResolved() throws Exception {
    var saga = sagaRunner = newRunner(null);
    var values = new LinkedHashMap<String, String>();
    values.put("saga.width", "6");
    values.put("streamrune.saga.subscriptions.order-saga.parallelism", "${saga.width}");
    hasNotFailed(start(values, "order-saga"));
    assertThat(effective(saga)).isEqualTo(6);
  }

  @Test
  void anUnknownSubscriptionNameRefusesTheStartListingTheKnownNames() throws Exception {
    sagaRunner = newRunner(null);
    failedWith(
        start(Map.of("streamrune.saga.subscriptions.nope.parallelism", "3"), "order-saga"),
        "[nope]",
        "[order-saga]");
  }

  @Test
  void withoutASagaRunnerASubscriptionPropertyRefusesTheStart() throws Exception {
    var started =
        start(
            config(file(Map.of("streamrune.saga.subscriptions.nope.parallelism", "3"))),
            1,
            false,
            List.of());
    failedWith(started, "bound to: [nope]", "Subscriptions bound to a saga runner: []");
  }

  @Test
  void theCheckRunsOncePerStart() throws Exception {
    sagaRunner = newRunner(2);
    subscriptionName = "order-saga";
    var started = start(config(), 2, true, List.of(SubscriptionBuilder.class));
    hasNotFailed(started);
    assertThat(started.info()).filteredOn(m -> m.startsWith("StreamRune: saga type ")).hasSize(1);
  }

  @Test
  void aSubscriptionBuiltInAStartupObserverIsKnownToTheCheck() throws Exception {
    var saga = sagaRunner = newRunner(null);
    hasNotFailed(
        start(
            Map.of("streamrune.saga.subscriptions.observer-saga.parallelism", "5"),
            "observer-saga"));
    assertThat(effective(saga)).isEqualTo(5);
  }

  @Test
  void anUnreadableValueRefusesTheStartNamingThePropertyAndTheValue() throws Exception {
    for (String bad : List.of("abc", "0")) {
      sagaRunner = newRunner(null);
      failedWith(
          start(Map.of("streamrune.saga.subscriptions.order-saga.parallelism", bad), "order-saga"),
          "streamrune.saga.subscriptions.order-saga.parallelism",
          "'" + bad + "'");
      sagaRunner = newRunner(null);
      failedWith(
          start(Map.of("streamrune.saga.parallelism", bad), "order-saga"),
          "streamrune.saga.parallelism",
          "'" + bad + "'");
    }
  }

  // ---- the startup lines, the refusal and the pool warning ------------------------------------

  @Test
  void theStartupLineNamesValueSourceAndBudget() throws Exception {
    sagaRunner = newRunner(2);
    var started = start(Map.of(), "order-saga");
    hasNotFailed(started);
    assertThat(started.info())
        .contains(
            "StreamRune: saga type "
                + SAGA_TYPE
                + " (subscription 'order-saga'): parallelism 2 (SagaRunner.Builder.parallelism);"
                + " its steps hold up to 2 connections at once");
  }

  private static AgroalDataSource agroal(int max) throws java.sql.SQLException {
    return AgroalDataSource.from(
        new AgroalDataSourceConfigurationSupplier()
            .connectionPoolConfiguration(
                pool ->
                    pool.maxSize(max)
                        .connectionFactoryConfiguration(
                            factory -> factory.jdbcUrl("jdbc:postgresql://localhost:1/none")))
            .get());
  }

  @Test
  void aSmallSharedPoolLogsTheWarn_aDedicatedLockPoolDoesNot() throws Exception {
    try (var shared = agroal(10);
        var dedicated = agroal(10)) {
      dataSource = shared;
      locker = new PgAdvisoryLocker(shared);
      sagaRunner = newRunner(null);
      var sharedStart = start(Map.of(), "order-saga", DataSourceBean.class);
      hasNotFailed(sharedStart);
      assertThat(sharedStart.warn())
          .singleElement()
          .asString()
          .contains("up to 8 connections")
          .contains("maximum of 10");

      locker = PgAdvisoryLocker.withDedicatedPool(dedicated);
      sagaRunner = newRunner(null);
      var dedicatedStart = start(Map.of(), "order-saga", DataSourceBean.class);
      hasNotFailed(dedicatedStart);
      assertThat(dedicatedStart.warn()).isEmpty();
    }
  }

  @Test
  void aLockerThatKeepsTheSpiDefaultCountsTwoConnectionsPerStep() throws Exception {
    try (var pool = agroal(10)) {
      dataSource = pool;
      locker = (StreamId streamId, Duration timeout) -> () -> {};
      sagaRunner = newRunner(null);
      var started = start(Map.of(), "order-saga", DataSourceBean.class);
      hasNotFailed(started);
      assertThat(started.info()).anyMatch(m -> m.contains("up to 8 connections"));
      assertThat(started.warn()).hasSize(1);
    }
  }

  @Test
  void aPoolTheIntegrationCannotReadLogsNoWarn_andTheLineStillHasTheBudget() throws Exception {
    dataSource = new PGSimpleDataSource();
    locker = new PgAdvisoryLocker(new PGSimpleDataSource());
    sagaRunner = newRunner(null);
    var unreadable = start(Map.of(), "order-saga", DataSourceBean.class);
    hasNotFailed(unreadable);
    assertThat(unreadable.warn()).isEmpty();
    assertThat(unreadable.info()).anyMatch(m -> m.contains("up to 8 connections"));

    locker = new LocalStripedLocker(64);
    sagaRunner = newRunner(null);
    var defaultLocker = start(Map.of(), "order-saga", DataSourceBean.class);
    hasNotFailed(defaultLocker);
    assertThat(defaultLocker.warn()).isEmpty();
    assertThat(defaultLocker.info()).anyMatch(m -> m.contains("up to 4 connections"));
  }

  @Test
  void aLockSharingThePoolThatCanTakeAllOfItRefusesTheStart() throws Exception {
    try (var shared = agroal(4);
        var lockPool = agroal(4)) {
      dataSource = shared;
      locker = new PgAdvisoryLocker(shared);
      sagaRunner = newRunner(null);
      String refusal =
          "StreamRune: saga steps can hold up to 4 connections of the connection pool as aggregate"
              + " locks at once (4 steps, 1 per lock), at least the pool's maximum of 4: every"
              + " connection can be taken by a lock while its step waits for a second one. Give the"
              + " aggregate locker a pool of its own (PgAdvisoryLocker.withDedicatedPool), raise"
              + " the pool, or lower streamrune.saga.parallelism.";
      var refused =
          start(Map.of("streamrune.saga.parallelism", "4"), "order-saga", DataSourceBean.class);
      failedWith(refused, refusal);
      // The INFO line first, then the refusal, and no WARN.
      assertThat(refused.sequence())
          .containsExactly(
              "INFO StreamRune: saga type "
                  + SAGA_TYPE
                  + " (subscription 'order-saga'): parallelism 4 (streamrune.saga.parallelism);"
                  + " its steps hold up to 8 connections at once",
              "OFF " + START_FAILED);

      locker = PgAdvisoryLocker.withDedicatedPool(lockPool);
      sagaRunner = newRunner(null);
      hasNotFailed(
          start(Map.of("streamrune.saga.parallelism", "4"), "order-saga", DataSourceBean.class));
    }
  }

  @Test
  void aPoolTheIntegrationCannotReadIsNeverRefused() throws Exception {
    dataSource = new PGSimpleDataSource();
    locker = new PgAdvisoryLocker(new PGSimpleDataSource());
    sagaRunner = newRunner(null);
    hasNotFailed(
        start(Map.of("streamrune.saga.parallelism", "64"), "order-saga", DataSourceBean.class));
  }

  @Test
  void aStartWithTheDefaultLockerAndNoPoolStillLogsTheLine() throws Exception {
    sagaRunner = newRunner(null);
    var started = start(Map.of(), "order-saga");
    hasNotFailed(started);
    assertThat(started.info()).anyMatch(m -> m.contains("up to 4 connections"));
    assertThat(started.warn()).isEmpty();
  }

  // ---- names the configuration lists that no source writes as they are ---------------------

  private static final String ORDERS_KEY = "streamrune.saga.subscriptions.orders.parallelism";

  @Test
  void aNameAnInterceptorAddsIsRead() {
    var config =
        new SmallRyeConfigBuilder()
            .addDefaultInterceptors()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withInterceptors(
                new io.smallrye.config.RelocateConfigSourceInterceptor(
                    n ->
                        n.startsWith("acme.saga.")
                            ? "streamrune.saga.subscriptions." + n.substring(10)
                            : n),
                new io.smallrye.config.FallbackConfigSourceInterceptor(
                    n ->
                        n.startsWith("streamrune.saga.subscriptions.")
                            ? "acme.saga." + n.substring(30)
                            : n))
            .withSources(file(Map.of("acme.saga.orders.parallelism", "8")))
            .build();
    assertThat(SagaParallelismProperties.bySubscription(config))
        .containsExactlyEntriesOf(Map.of("orders", "8"));
  }

  @Test
  void aDottedVariableNotWrittenAsTheKeyIsRead() {
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(environment(Map.of(ORDERS_KEY.toUpperCase(), "8")))))
        .containsExactlyEntriesOf(Map.of("orders", "8"));
    var misspelt = config(environment(Map.of(ORDERS_KEY.toUpperCase().replace("ALL", "AL"), "8")));
    assertThatThrownBy(() -> SagaParallelismProperties.bySubscription(misspelt))
        .hasMessageContaining(
            "streamrune.saga.subscriptions.orders.paralelism is not a property; set ");
  }

  @Test
  void aSourceWithoutPropertyNamesListsNothing() {
    ConfigSource silent =
        new ConfigSource() {
          @Override
          public Set<String> getPropertyNames() {
            return null;
          }

          @Override
          public String getValue(String propertyName) {
            return null;
          }

          @Override
          public String getName() {
            return "silent";
          }
        };
    assertThat(
            SagaParallelismProperties.bySubscription(config(silent, file(Map.of(ORDERS_KEY, "3")))))
        .containsExactlyEntriesOf(Map.of("orders", "3"));
  }

  @Test
  void dottedEntriesOfAnEnvironmentSourceAreReadAsWritten(@TempDir Path directory)
      throws Exception {
    assertThat(
            SagaParallelismProperties.bySubscription(
                config(
                    dotEnv(
                        directory,
                        "streamrune.saga.subscriptions.Orders.parallelism=8",
                        "streamrune.saga.subscriptions.order-saga.parallelism=5",
                        "streamrune.saga.subscriptions.shard[0].parallelism=4"))))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("Orders", "8", "order-saga", "5", "shard[0]", "4"));

    var misspelt =
        config(environment(Map.of("streamrune.saga.subscriptions.orders.paralelism", "8")));
    assertThatThrownBy(() -> SagaParallelismProperties.bySubscription(misspelt))
        .hasMessageContaining(
            "streamrune.saga.subscriptions.orders.paralelism is not a property; set ");

    var profiled = Map.of("%prod." + ORDERS_KEY, "8");
    assertThat(SagaParallelismProperties.bySubscription(config("prod", environment(profiled))))
        .containsExactlyEntriesOf(Map.of("orders", "8"));
    assertThat(SagaParallelismProperties.bySubscription(config("dev", environment(profiled))))
        .isEmpty();
  }
}
