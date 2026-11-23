package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.zaxxer.hikari.HikariDataSource;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.ApplicationContextBuilder;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.AbstractPropertySourceLoader;
import io.micronaut.context.env.EnvironmentPropertySource;
import io.micronaut.context.env.MapPropertySource;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.SystemPropertiesPropertySource;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.EventStore;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.postgres.PgAdvisoryLocker;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaParallelismDefault;
import org.streamrune.runtime.SagaParallelismSource;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Saga parallelism through the real container: the global property, the per-subscription property,
 * the startup check (unknown names, one line per runner, the refusal, the pool warning).
 */
class SagaParallelismWiringTest {

  private static final String SAGA_TYPE =
      SagaType.fromClass(SagaEventPathRetentionWiringTest.FakeState.class).value();

  private static final String SPEC = "saga-parallelism-wiring";

  /** What the next application's factory beans build; set by the test before it starts. */
  private static volatile Integer builderParallelism;

  private static volatile String subscriptionName = "order-saga";
  private static volatile boolean subscribed = true;
  private static volatile Supplier<AggregateLocker> locker;
  private static volatile Supplier<DataSource> dataSource;
  private static volatile String otherSubscriptionName = "other-saga";

  /** A saga runner bean and, unless the test says otherwise, a subscription over it. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class Sagas {
    @Singleton
    @Named("orderSagaRunner")
    SagaRunner<SagaEventPathRetentionWiringTest.FakeState> runner() {
      var store = new InMemorySagaStore();
      var builder =
          SagaRunner.<SagaEventPathRetentionWiringTest.FakeState>builder()
              .orchestrator(new SagaEventPathRetentionWiringTest.FakeOrchestrator())
              .sagaStore(store)
              .commandBus(SagaEventPathRetentionWiringTest.NOOP_BUS)
              .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
              .forwardFence(false);
      if (builderParallelism != null) {
        builder.parallelism(builderParallelism);
      }
      return builder.build();
    }

    @Singleton
    @Named("otherSagaRunner")
    @Requires(property = "test.other-saga", value = "true")
    SagaRunner<OtherState> otherRunner() {
      var store = new InMemorySagaStore();
      return SagaRunner.<OtherState>builder()
          .orchestrator(new OtherOrchestrator())
          .sagaStore(store)
          .commandBus(SagaEventPathRetentionWiringTest.NOOP_BUS)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
          .forwardFence(false)
          .build();
    }

    @Singleton
    @Context
    @Named("otherSubscription")
    @Requires(property = "test.other-saga", value = "true")
    PollingEventSubscription otherSubscriptionBean(
        @Named("otherSagaRunner") SagaRunner<OtherState> runner) {
      return SagaParallelismWiringTest.subscription(otherSubscriptionName, runner);
    }

    @Singleton
    @Context
    @Requires(property = "test.subscribed", value = "true")
    PollingEventSubscription subscriptionBean(
        @Named("orderSagaRunner") SagaRunner<SagaEventPathRetentionWiringTest.FakeState> runner) {
      return SagaParallelismWiringTest.subscription(subscriptionName, runner);
    }

    @Singleton
    @Requires(property = "test.pool")
    DataSource dataSource() {
      return dataSource.get();
    }

    @Singleton
    @Requires(property = "test.locker")
    AggregateLocker locker() {
      return locker.get();
    }

    @Singleton
    EventStore eventStore() {
      return org.mockito.Mockito.mock(EventStore.class);
    }
  }

  /** The state of a second saga type, so two runners can be bound side by side. */
  record OtherState(SagaStatus status, String id) implements org.streamrune.core.saga.SagaState {}

  /** The orchestrator of the second saga type; it reacts to nothing. */
  static class OtherOrchestrator implements org.streamrune.core.saga.SagaOrchestrator<OtherState> {
    @Override
    public Class<OtherState> stateType() {
      return OtherState.class;
    }

    @Override
    public OtherState initialState(org.streamrune.core.saga.SagaId sagaId) {
      return new OtherState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(org.streamrune.core.EventEnvelope event) {
      return false;
    }

    @Override
    public org.streamrune.core.saga.SagaId extractSagaId(org.streamrune.core.EventEnvelope event) {
      return org.streamrune.core.saga.SagaId.of("x");
    }

    @Override
    public OtherState evolve(OtherState state, org.streamrune.core.EventEnvelope event) {
      return state;
    }

    @Override
    public List<org.streamrune.core.saga.SagaCommand> handle(
        OtherState state, org.streamrune.core.EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<org.streamrune.core.saga.SagaCommand> compensate(
        OtherState state, Throwable failure, org.streamrune.core.saga.SagaCommand failedCommand) {
      return List.of();
    }
  }

  /** Builds a subscription in a startup listener that runs just before the check. */
  @Singleton
  @Requires(property = "test.listener-built", value = "true")
  static class ListenerBuiltSubscription
      implements ApplicationEventListener<StartupEvent>, Ordered {
    private final SagaRunner<SagaEventPathRetentionWiringTest.FakeState> runner;
    private PollingEventSubscription subscription;

    ListenerBuiltSubscription(SagaRunner<SagaEventPathRetentionWiringTest.FakeState> runner) {
      this.runner = runner;
    }

    @Override
    public int getOrder() {
      return Ordered.LOWEST_PRECEDENCE - 100;
    }

    @Override
    public void onApplicationEvent(StartupEvent event) {
      subscription = subscription("lifecycle-saga", runner);
    }
  }

  /** Builds a subscription in a startup listener that sets no order: no Ordered, no @Order. */
  @Singleton
  @Requires(property = "test.unordered-listener-built", value = "true")
  static class UnorderedListenerBuiltSubscription
      implements ApplicationEventListener<StartupEvent> {
    private final SagaRunner<SagaEventPathRetentionWiringTest.FakeState> runner;
    private PollingEventSubscription subscription;

    UnorderedListenerBuiltSubscription(
        SagaRunner<SagaEventPathRetentionWiringTest.FakeState> runner) {
      this.runner = runner;
    }

    @Override
    public void onApplicationEvent(StartupEvent event) {
      subscription = subscription("unordered-saga", runner);
    }
  }

  private static PollingEventSubscription subscription(String name, SagaRunner<?> runner) {
    return PollingEventSubscription.builder()
        .subscriptionName(SubscriptionName.of(name))
        .eventStore(new InMemoryEventStore(SimpleEventTypeRegistry.builder().build()))
        .offsetStore(new InMemoryOffsetStore())
        .config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
        .sagaRunner(runner)
        .build();
  }

  private static Map<String, Object> app(Map<String, Object> more) {
    Map<String, Object> properties = app();
    properties.putAll(more);
    return properties;
  }

  private static Map<String, Object> app(Object... pairs) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("spec.name", SPEC);
    properties.put("test.subscribed", String.valueOf(subscribed));
    properties.put("streamrune.saga.compensation-retry-enabled", "false");
    for (int i = 0; i < pairs.length; i += 2) {
      properties.put((String) pairs[i], pairs[i + 1]);
    }
    return properties;
  }

  /** Starts the application and hands the runner it built to {@code check}. */
  private static void run(
      Integer builder,
      String subscription,
      Map<String, Object> properties,
      java.util.function.Consumer<SagaRunner<?>> check) {
    builderParallelism = builder;
    subscriptionName = subscription;
    subscribed = subscription != null;
    properties.put("test.subscribed", String.valueOf(subscribed));
    try (var ctx = ApplicationContext.run(properties)) {
      check.accept(ctx.getBean(SagaRunner.class));
    } finally {
      builderParallelism = null;
      subscriptionName = "order-saga";
      subscribed = true;
    }
  }

  private static int effective(SagaRunner<?> saga) {
    return SagaParallelismDefault.effectiveOf(saga);
  }

  private static String failureOf(Map<String, Object> properties) {
    return failureOf(properties, "order-saga");
  }

  private static String failureOf(Map<String, Object> properties, String subscription) {
    builderParallelism = null;
    subscriptionName = subscription;
    subscribed = subscription != null;
    properties.put("test.subscribed", String.valueOf(subscribed));
    RuntimeException thrown;
    try {
      thrown =
          org.junit.jupiter.api.Assertions.assertThrows(
              RuntimeException.class, () -> ApplicationContext.run(properties).close());
    } finally {
      subscriptionName = "order-saga";
      subscribed = true;
    }
    var text = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
      text.append(t.getMessage()).append('\n');
    }
    return text.toString();
  }

  @Test
  void theGlobalPropertyReachesARunnerThatNamedNothing() {
    run(
        null,
        "order-saga",
        app("streamrune.saga.parallelism", "8"),
        saga -> {
          assertThat(effective(saga)).isEqualTo(8);
          assertThat(SagaParallelismDefault.sourceOf(saga))
              .isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
        });
  }

  @Test
  void aRunnerThatNamedItsParallelismKeepsItAgainstTheGlobalProperty() {
    run(
        2,
        "order-saga",
        app("streamrune.saga.parallelism", "8"),
        saga -> assertThat(effective(saga)).isEqualTo(2));
  }

  @Test
  void aSubscriptionPropertyWinsOverTheBuilder() {
    run(
        2,
        "order-saga",
        app("streamrune.saga.subscriptions.order-saga.parallelism", "16"),
        saga -> assertThat(effective(saga)).isEqualTo(16));
  }

  @Test
  void aSubscriptionNameWithADotIsMatchedExactly() {
    run(
        null,
        "orders.v2",
        app("streamrune.saga.subscriptions.orders.v2.parallelism", "3"),
        saga -> {
          assertThat(effective(saga)).isEqualTo(3);
          assertThat(SagaParallelismDefault.sourceOf(saga))
              .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
        });
  }

  /** The keys of a YAML file, as the framework's own loader flattens them. */
  private static Map<String, Object> yaml(String text) {
    try {
      return new io.micronaut.context.env.yaml.YamlPropertySourceLoader()
          .read(
              "test-yaml",
              new java.io.ByteArrayInputStream(
                  text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  @Test
  void aQuotedYamlNameWithADotIsMatchedExactly() {
    var quoted =
        yaml(
            "streamrune:\n  saga:\n    subscriptions:\n      \"orders.v2\":\n        parallelism: 3\n");
    run(null, "orders.v2", app(quoted), saga -> assertThat(effective(saga)).isEqualTo(3));
    var nested =
        yaml(
            "streamrune:\n  saga:\n    subscriptions:\n      order-saga:\n        parallelism: 5\n");
    run(null, "order-saga", app(nested), saga -> assertThat(effective(saga)).isEqualTo(5));
  }

  @Test
  void aNestedMapValueIsReadLikeTheFlatKey() {
    run(
        null,
        "orderSaga",
        app("streamrune.saga.subscriptions", Map.of("orderSaga", Map.of("parallelism", 6))),
        saga -> assertThat(effective(saga)).isEqualTo(6));
  }

  @Test
  void aSubscriptionNameWithUpperCaseIsMatchedExactly() {
    run(
        null,
        "Orders",
        app("streamrune.saga.subscriptions.Orders.parallelism", "3"),
        saga -> {
          assertThat(effective(saga)).isEqualTo(3);
          assertThat(SagaParallelismDefault.sourceOf(saga))
              .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
        });
  }

  @Test
  void aSubscriptionNameWithInnerCapitalsIsMatchedExactly() {
    for (String name : List.of("orderSaga", "OrderFulfillmentSaga", "ordersV2")) {
      String key = "streamrune.saga.subscriptions." + name + ".parallelism";
      run(
          null,
          name,
          app(key, "3"),
          saga -> {
            assertThat(effective(saga)).isEqualTo(3);
            assertThat(SagaParallelismDefault.sourceOf(saga))
                .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
          });
      subscriptionName = name;
      try (var ctx =
          ApplicationContext.builder()
              .properties(app())
              .propertySources(
                  source("test-system", SystemPropertiesPropertySource.POSITION, key, "5"))
              .start()) {
        assertThat(SagaParallelismProperties.bySubscription(ctx.getEnvironment()))
            .containsExactly(Map.entry(name, "5"));
        assertThat(effective(ctx.getBean(SagaRunner.class))).isEqualTo(5);
      } finally {
        subscriptionName = "order-saga";
      }
    }
  }

  @Test
  void twoNamesThatDifferOnlyInCaseEachTakeTheirOwnValue() {
    subscriptionName = "orderSaga";
    otherSubscriptionName = "OrderSaga";
    try (var ctx =
        ApplicationContext.run(
            app(
                "test.other-saga",
                "true",
                "streamrune.saga.subscriptions.orderSaga.parallelism",
                "5",
                "streamrune.saga.subscriptions.OrderSaga.parallelism",
                "3"))) {
      assertValue(ctx, "orderSagaRunner", "orderSaga", 5);
      assertValue(ctx, "otherSagaRunner", "OrderSaga", 3);
    } finally {
      subscriptionName = "order-saga";
      otherSubscriptionName = "other-saga";
    }
  }

  @Test
  void twoNamesMicronautReadsAsOnePropertyRefuseTheStart() {
    for (String[] pair :
        List.of(new String[] {"orders", "Orders"}, new String[] {"order-saga", "orderSaga"})) {
      String canonical = "streamrune.saga.subscriptions." + pair[0] + ".parallelism";
      String other = "streamrune.saga.subscriptions." + pair[1] + ".parallelism";
      subscriptionName = pair[0];
      otherSubscriptionName = pair[1];
      try {
        assertThat(failureOf(app("test.other-saga", "true", canonical, "5", other, "3"), pair[0]))
            .contains(
                canonical
                    + " and "
                    + other
                    + " are one property to Micronaut, which can give either the value of the"
                    + " other; set only one of them");
      } finally {
        otherSubscriptionName = "other-saga";
      }
    }
  }

  /** The runner's parallelism, and Micronaut's own value for the subscription's property. */
  private static void assertValue(
      ApplicationContext ctx, String runner, String subscription, int expected) {
    assertThat(effective(ctx.getBean(SagaRunner.class, Qualifiers.byName(runner))))
        .isEqualTo(expected);
    assertThat(
            ctx.getEnvironment()
                .getProperty(
                    "streamrune.saga.subscriptions." + subscription + ".parallelism", String.class))
        .contains(String.valueOf(expected));
  }

  private static String notAProperty(String key) {
    return key + " is not a property; set streamrune.saga.subscriptions.<name>.parallelism";
  }

  @Test
  void aMisspeltParallelismRefusesTheStartNamingTheKey() {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.order-saga.paralelism",
            "streamrune.saga.subscriptions.orders.v2.paralelism")) {
      assertThat(failureOf(app(key, "3"))).contains(notAProperty(key));
    }
  }

  @Test
  void aKeyThatIsNotAPropertyIsNamedSanitizedForTheLog() {
    assertThat(failureOf(app("streamrune.saga.subscriptions.order\nsaga.paralelism", "3")))
        .contains(notAProperty("streamrune.saga.subscriptions.ordersaga.paralelism"));
  }

  @Test
  void aKeyWithoutASubscriptionNameRefusesTheStart() {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.parallelism", "streamrune.saga.subscriptions.orders")) {
      assertThat(failureOf(app(key, "3"))).contains(notAProperty(key));
    }
  }

  @Test
  void aKeyUnderSubscriptionsThatIsNotParallelismRefusesTheStart() {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.order-saga.parallelism.max",
            "streamrune.saga.subscriptions.order-saga.x.paralelism")) {
      assertThat(failureOf(app(key, "3"))).contains(notAProperty(key));
    }
  }

  @Test
  void anIndexedSpellingRefusesTheStartNamingTheKeyAsWritten() {
    String bracketed = "streamrune.saga.subscriptions[order-saga].parallelism";
    assertThat(failureOf(app(bracketed, "8"))).contains(notAProperty(bracketed));

    var list =
        yaml(
            "streamrune:\n  saga:\n    subscriptions:\n      - order-saga:\n"
                + "          parallelism: 8\n");
    assertThat(failureOf(app(list)))
        .contains(notAProperty("streamrune.saga.subscriptions[0].order-saga.parallelism"));

    var leafList =
        yaml(
            "streamrune:\n  saga:\n    subscriptions:\n      order-saga:\n"
                + "        parallelism: [8]\n");
    assertThat(failureOf(app(leafList)))
        .contains(notAProperty("streamrune.saga.subscriptions.order-saga.parallelism[0]"));
  }

  /** A property source of the given order and convention, as the environment's own sources are. */
  private static PropertySource source(
      String name, int order, PropertySource.PropertyConvention convention, Map<String, ?> map) {
    return new MapPropertySource(name, map) {
      @Override
      public int getOrder() {
        return order;
      }

      @Override
      public PropertyConvention getConvention() {
        return convention;
      }
    };
  }

  private static PropertySource source(String name, int order, String key, Object value) {
    Map<String, Object> map = new HashMap<>();
    map.put(key, value);
    return source(name, order, PropertySource.PropertyConvention.JAVA_PROPERTIES, map);
  }

  /** Environment variables at the order of Micronaut's environment source. */
  private static PropertySource environmentVariables(String name, String value) {
    return source(
        "test-env",
        EnvironmentPropertySource.POSITION,
        PropertySource.PropertyConvention.ENVIRONMENT_VARIABLE,
        Map.of(name, value));
  }

  /** The messages of the failure that ends a start with these property sources. */
  private static String failureWith(PropertySource... sources) {
    var thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            RuntimeException.class,
            () ->
                ApplicationContext.builder()
                    .properties(app())
                    .propertySources(sources)
                    .start()
                    .close());
    var text = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
      text.append(t.getMessage()).append('\n');
    }
    return text.toString();
  }

  @Test
  void anEnvironmentVariableRefusesTheUnknownNameListingTheKnownOnes() {
    assertThat(
            failureWith(
                environmentVariables("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDER_SAGA_PARALLELISM", "8")))
        .contains("bound to: [order.saga]")
        .contains("Subscriptions bound to a saga runner: [order-saga]");
  }

  @Test
  void aVariableMicronautReadsAsAnIndexRefusesTheStartNamingItAsWritten() {
    // Micronaut's environment source holds STREAMRUNE_SAGA_SUBSCRIPTIONS_2_PARALLELISM as
    // STREAMRUNE_SAGA_SUBSCRIPTIONS[2]PARALLELISM, and STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_2 as
    // STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS[2]: an index, not a subscription's property.
    Map<String, String> writtenAs =
        Map.of(
            "STREAMRUNE_SAGA_SUBSCRIPTIONS[2]PARALLELISM",
            "STREAMRUNE_SAGA_SUBSCRIPTIONS_2_PARALLELISM",
            "STREAMRUNE_SAGA_SUBSCRIPTIONS_SHARD[1]PARALLELISM",
            "STREAMRUNE_SAGA_SUBSCRIPTIONS_SHARD_1_PARALLELISM",
            "STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS[2]",
            "STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_2");
    writtenAs.forEach(
        (held, written) -> {
          String failure = failureWith(environmentVariables(held, "8"));
          assertThat(failure)
              .contains(written + " holds an element of one or two digits")
              .contains("set streamrune.saga.subscriptions.<name>.parallelism in a file or as a")
              .doesNotContain("[");
        });
  }

  @Test
  void aMapHeldAboveThePrefixIsReadAndItsMisspeltLeafRefused() {
    for (String above : List.of("streamrune", "streamrune.saga")) {
      Object map =
          above.equals("streamrune")
              ? Map.of(
                  "saga", Map.of("subscriptions", Map.of("order-saga", Map.of("parallelism", 8))))
              : Map.of("subscriptions", Map.of("order-saga", Map.of("parallelism", 8)));
      run(null, "order-saga", app(above, map), saga -> assertThat(effective(saga)).isEqualTo(8));
      Object misspelt =
          above.equals("streamrune")
              ? Map.of(
                  "saga", Map.of("subscriptions", Map.of("order-saga", Map.of("paralelism", 8))))
              : Map.of("subscriptions", Map.of("order-saga", Map.of("paralelism", 8)));
      assertThat(failureOf(app(above, misspelt)))
          .contains(notAProperty("streamrune.saga.subscriptions.order-saga.paralelism"));
    }
  }

  @Test
  void anUnresolvablePlaceholderBesideAMapKeepsTheMapRead() {
    // A map at the bare prefix is still read.
    run(
        null,
        "order-saga",
        app(
            "streamrune.saga.note",
            "${no.such.note}",
            "streamrune.saga.subscriptions",
            Map.of("order-saga", Map.of("parallelism", 8))),
        saga -> assertThat(effective(saga)).isEqualTo(8));
    // A list under the prefix is still refused as an indexed spelling.
    var list =
        yaml(
            "streamrune:\n  saga:\n    subscriptions:\n      - order-saga:\n"
                + "          parallelism: 8\n");
    list.put("streamrune.saga.note", "${no.such.note}");
    assertThat(failureOf(app(list)))
        .contains(notAProperty("streamrune.saga.subscriptions[0].order-saga.parallelism"));
    // The property that holds the placeholder is blamed, not a valid key beside it.
    String failure =
        failureOf(
            app(
                "streamrune.saga.subscriptions.other-saga.parallelism",
                "${no.such.width}",
                "streamrune.saga.subscriptions.order-saga",
                Map.of("parallelism", 8)));
    assertThat(failure)
        .contains("streamrune.saga.subscriptions.other-saga.parallelism holds a placeholder")
        .doesNotContain("order-saga is not a property");
  }

  @Test
  void everyCasingMicronautReadsAsTheRootIsReadAndTheOnesItDoesNotAreLeftAlone() {
    for (String root : List.of("STreamrune", "STREAMRUNe", "STREamrune")) {
      for (String leaf : List.of("parallelism", "paralelism")) {
        String key = root + ".saga.subscriptions.order-saga." + leaf;
        assertThat(failureOf(app(key, "8"))).contains(notAProperty(key));
      }
    }
    // Micronaut spells these stream-rune, so they are not the root of any StreamRune property.
    for (String root : List.of("StreamRune", "sTreamrune", "streamRune")) {
      String key = root + ".saga.subscriptions.order-saga.paralelism";
      run(null, "order-saga", app(key, "8"), saga -> assertThat(effective(saga)).isEqualTo(4));
    }
  }

  @Test
  void anEnvironmentVariableNamedWithDotsIsReadAsWritten() {
    String key = "streamrune.saga.subscriptions.order-saga.parallelism";
    try (var ctx =
        ApplicationContext.builder()
            .properties(app())
            .propertySources(
                source(
                    "test-env",
                    EnvironmentPropertySource.POSITION,
                    PropertySource.PropertyConvention.ENVIRONMENT_VARIABLE,
                    Map.of(key, "8")))
            .start()) {
      assertValue(ctx, "orderSagaRunner", "order-saga", 8);
    }
    String misspelt = "streamrune.saga.subscriptions.order-saga.paralelism";
    assertThat(
            failureWith(
                source(
                    "test-env",
                    EnvironmentPropertySource.POSITION,
                    PropertySource.PropertyConvention.ENVIRONMENT_VARIABLE,
                    Map.of(misspelt, "8"))))
        .contains(notAProperty(misspelt));
  }

  @Test
  void aPrefixInAnotherLetterCaseRefusesTheStart() {
    for (String key :
        List.of(
            "streamrune.saga.Subscriptions.order-saga.parallelism",
            "streamrune.Saga.subscriptions.order-saga.parallelism",
            "STREAMRUNE.SAGA.SUBSCRIPTIONS.order-saga.parallelism")) {
      assertThat(failureOf(app(key, "3"))).contains(notAProperty(key));
    }
  }

  @Test
  void anEnvironmentVariableSetsASubscriptionNamedWithDots() {
    subscriptionName = "order.v2";
    try (var ctx =
        ApplicationContext.builder()
            .properties(app())
            .propertySources(
                environmentVariables("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDER_V2_PARALLELISM", "3"))
            .start()) {
      assertThat(effective(ctx.getBean(SagaRunner.class))).isEqualTo(3);
    } finally {
      subscriptionName = "order-saga";
    }
  }

  @Test
  void anEnvironmentVariableBesideAnUpperCaseFileNameNamesItsOwnLowerCaseSubscription() {
    subscriptionName = "Orders";
    try {
      assertThat(
              failureWith(
                  source(
                      "test-file",
                      AbstractPropertySourceLoader.DEFAULT_POSITION,
                      "streamrune.saga.subscriptions.Orders.parallelism",
                      "3"),
                  environmentVariables("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8")))
          .contains("bound to: [orders]")
          .contains("Subscriptions bound to a saga runner: [Orders]");
      // Both bound: each takes the value Micronaut resolves for its own property.
      otherSubscriptionName = "orders";
      try (var ctx =
          ApplicationContext.builder()
              .properties(app("test.other-saga", "true"))
              .propertySources(
                  source(
                      "test-file",
                      AbstractPropertySourceLoader.DEFAULT_POSITION,
                      "streamrune.saga.subscriptions.Orders.parallelism",
                      "3"),
                  environmentVariables("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM", "8"))
              .start()) {
        assertValue(ctx, "orderSagaRunner", "Orders", 3);
        assertValue(ctx, "otherSagaRunner", "orders", 8);
      }
    } finally {
      subscriptionName = "order-saga";
      otherSubscriptionName = "other-saga";
    }
  }

  @Test
  void aMisspeltEnvironmentVariableRefusesTheStart() {
    assertThat(
            failureWith(
                environmentVariables("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALELISM", "8")))
        .contains(notAProperty("streamrune.saga.subscriptions.orders.paralelism"));
  }

  @Test
  void aValueSetInTwoSourcesTakesTheOneOfHigherPrecedence() {
    String key = "streamrune.saga.subscriptions.orders.v2.parallelism";
    var file = source("test-file", AbstractPropertySourceLoader.DEFAULT_POSITION, key, "3");
    var env = environmentVariables("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_V2_PARALLELISM", "8");
    var system = source("test-system", SystemPropertiesPropertySource.POSITION, key, "16");
    subscriptionName = "orders.v2";
    try {
      // Registered from the highest precedence down, so that registration order would pick wrong.
      try (var ctx =
          ApplicationContext.builder()
              .properties(app())
              .propertySources(system, env, file)
              .start()) {
        assertThat(effective(ctx.getBean(SagaRunner.class))).isEqualTo(16);
        assertThat(ctx.getEnvironment().getProperty(key, String.class)).contains("16");
      }
      try (var ctx =
          ApplicationContext.builder().properties(app()).propertySources(env, file).start()) {
        assertThat(effective(ctx.getBean(SagaRunner.class))).isEqualTo(8);
        assertThat(ctx.getEnvironment().getProperty(key, String.class)).contains("8");
      }
      subscriptionName = "orderSaga";
      String camel = "streamrune.saga.subscriptions.orderSaga.parallelism";
      try (var ctx =
          ApplicationContext.builder()
              .properties(app())
              .propertySources(
                  source("test-system", SystemPropertiesPropertySource.POSITION, camel, "5"),
                  source("test-file", AbstractPropertySourceLoader.DEFAULT_POSITION, camel, "3"))
              .start()) {
        assertThat(effective(ctx.getBean(SagaRunner.class))).isEqualTo(5);
        assertThat(ctx.getEnvironment().getProperty(camel, String.class)).contains("5");
      }
    } finally {
      subscriptionName = "order-saga";
    }
  }

  @Test
  void aPropertyWithoutAValueRefusesTheStart() {
    String key = "streamrune.saga.subscriptions.order-saga.parallelism";
    assertThat(failureWith(source("nothing", 0, key, null)))
        .contains(key + " must be a whole number between 1 and 64; got: 'null'");
    var empty = properties(key + "=\n");
    assertThat(failureOf(app(empty)))
        .contains(key + " must be a whole number between 1 and 64; got: ''");
  }

  /** The keys of a properties file, as the framework's own loader reads them. */
  private static Map<String, Object> properties(String text) {
    try {
      return new io.micronaut.context.env.PropertiesPropertySourceLoader()
          .read(
              "test-properties",
              new java.io.ByteArrayInputStream(
                  text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  @Test
  void aPlaceholderInTheValueIsResolved() {
    run(
        null,
        "order-saga",
        app(
            "saga.width",
            "6",
            "streamrune.saga.subscriptions.order-saga.parallelism",
            "${saga.width}"),
        saga -> assertThat(effective(saga)).isEqualTo(6));
  }

  @Test
  void anUnresolvablePlaceholderRefusesTheStartNamingTheProperty() {
    assertThat(
            failureOf(
                app("streamrune.saga.subscriptions.order-saga.parallelism", "${no.such.width}")))
        .contains(
            "streamrune.saga.subscriptions.order-saga.parallelism holds a placeholder Micronaut"
                + " cannot resolve")
        .contains("${no.such.width}");
  }

  private static final String ORDER_SAGA_YML =
      "streamrune:\n  saga:\n    subscriptions:\n      order-saga:\n        parallelism: 8\n";

  private static Path write(Path dir, String file, String text) throws IOException {
    Files.createDirectories(dir);
    return Files.writeString(dir.resolve(file), text);
  }

  /** Starts the application the builder describes, with the second saga runner bound too. */
  private static void start(
      ApplicationContextBuilder builder, java.util.function.Consumer<ApplicationContext> check) {
    try (var ctx = builder.properties(app("test.other-saga", "true")).start()) {
      check.accept(ctx);
    }
  }

  @Test
  void anApplicationYmlBesideAnApplicationPropertiesIsRead(@TempDir Path dir) throws IOException {
    write(dir, "application.properties", "unrelated.key=1\n");
    write(dir, "application.yml", ORDER_SAGA_YML);
    start(
        ApplicationContext.builder().overrideConfigLocations("file:" + dir),
        ctx -> assertValue(ctx, "orderSagaRunner", "order-saga", 8));

    write(
        dir,
        "application.yml",
        "streamrune:\n  saga:\n    subscriptions:\n      order-saga:\n        paralelism: 8\n");
    var thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            RuntimeException.class,
            () ->
                start(
                    ApplicationContext.builder().overrideConfigLocations("file:" + dir),
                    ctx -> {}));
    assertThat(messagesOf(thrown))
        .contains(notAProperty("streamrune.saga.subscriptions.order-saga.paralelism"));
  }

  @Test
  void aClasspathApplicationYmlAndAConfigDirectoryApplicationYmlAreBothRead(@TempDir Path dir)
      throws IOException {
    write(dir.resolve("config"), "application.yml", ORDER_SAGA_YML);
    start(
        ApplicationContext.builder()
            .overrideConfigLocations(
                "classpath:saga-parallelism-classpath/", "file:" + dir.resolve("config")),
        ctx -> {
          assertValue(ctx, "orderSagaRunner", "order-saga", 8);
          assertValue(ctx, "otherSagaRunner", "other-saga", 2);
        });
  }

  @Test
  void anImportedFileIsRead(@TempDir Path dir) throws IOException {
    Path imported =
        write(
            dir,
            "saga.yml",
            "streamrune:\n  saga:\n    subscriptions:\n      other-saga:\n        parallelism: 7\n"
                + "      order-saga:\n        parallelism: 8\n");
    write(
        dir,
        "application.yml",
        "micronaut:\n  config:\n    import: \"file://"
            + imported
            + "\"\nstreamrune:\n  saga:\n    subscriptions:\n      other-saga:\n"
            + "        parallelism: 2\n");
    start(
        ApplicationContext.builder().overrideConfigLocations("file:" + dir),
        ctx -> {
          assertValue(ctx, "orderSagaRunner", "order-saga", 8);
          assertValue(ctx, "otherSagaRunner", "other-saga", 7);
        });
  }

  @Test
  void anUnnamedPropertySourceIsRead(@TempDir Path dir) throws IOException {
    write(
        dir,
        "application.yml",
        "streamrune:\n  saga:\n    subscriptions:\n      other-saga:\n        parallelism: 2\n");
    Map<String, Object> map = new HashMap<>();
    map.put("streamrune.saga.subscriptions.order-saga.parallelism", "8");
    start(
        ApplicationContext.builder()
            .overrideConfigLocations("file:" + dir)
            .propertySources(PropertySource.of(map)),
        ctx -> {
          assertValue(ctx, "orderSagaRunner", "order-saga", 8);
          assertValue(ctx, "otherSagaRunner", "other-saga", 2);
        });
  }

  @Test
  void equalOrdersAndASourceAddedAtRuntimeTakeMicronautsValue(@TempDir Path dir)
      throws IOException {
    write(dir, "application.yml", ORDER_SAGA_YML);
    String key = "streamrune.saga.subscriptions.order-saga.parallelism";
    start(
        ApplicationContext.builder()
            .overrideConfigLocations("file:" + dir)
            .propertySources(
                source("test-file", AbstractPropertySourceLoader.DEFAULT_POSITION, key, "3")),
        ctx -> {
          String micronauts = ctx.getEnvironment().getProperty(key, String.class).orElseThrow();
          // The source registered at the order of application.yml wins in Micronaut.
          assertThat(micronauts).isEqualTo("3");
          assertThat(effective(ctx.getBean(SagaRunner.class, Qualifiers.byName("orderSagaRunner"))))
              .isEqualTo(Integer.parseInt(micronauts));
          assertThat(SagaParallelismProperties.bySubscription(ctx.getEnvironment()))
              .containsEntry("order-saga", micronauts);
        });
    start(
        ApplicationContext.builder().overrideConfigLocations("file:" + dir),
        ctx -> {
          var environment = ctx.getEnvironment();
          environment.addPropertySource(source("test-late", -1000, key, "3"));
          assertThat(environment.getProperty(key, String.class)).contains("3");
          assertThat(SagaParallelismProperties.bySubscription(environment))
              .containsEntry("order-saga", "3");
        });
  }

  private static String messagesOf(Throwable thrown) {
    var text = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
      text.append(t.getMessage()).append('\n');
    }
    return text.toString();
  }

  @Test
  void anUnknownSubscriptionNameRefusesTheStartListingTheKnownNames() {
    assertThat(failureOf(app("streamrune.saga.subscriptions.nope.parallelism", "3")))
        .contains("[nope]")
        .contains("[order-saga]");
  }

  @Test
  void withoutASagaRunnerASubscriptionPropertyRefusesTheStart() {
    var thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            RuntimeException.class,
            () ->
                ApplicationContext.run(
                        Map.of(
                            "streamrune.saga.subscriptions.nope.parallelism",
                            "3",
                            "streamrune.saga.compensation-retry-enabled",
                            "false"))
                    .close());
    var text = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
      text.append(t.getMessage()).append('\n');
    }
    assertThat(text.toString())
        .contains("bound to: [nope]")
        .contains("Subscriptions bound to a saga runner: []");
  }

  @Test
  void theCheckRunsOnceEvenWhenAnotherStartupEventIsPublished() {
    var log = capture();
    builderParallelism = 2;
    try (var ctx = ApplicationContext.run(app())) {
      ctx.publishEvent(new StartupEvent(ctx));
    } finally {
      log.detach();
      builderParallelism = null;
    }
    assertThat(messages(log.list(), Level.INFO))
        .filteredOn(m -> m.startsWith("StreamRune: saga type "))
        .hasSize(1);
  }

  @Test
  void aSubscriptionBuiltInAStartupListenerIsKnownToTheCheck() {
    run(
        null,
        null,
        app(
            "test.listener-built",
            "true",
            "streamrune.saga.subscriptions.lifecycle-saga.parallelism",
            "5"),
        saga -> assertThat(effective(saga)).isEqualTo(5));
  }

  @Test
  void aSubscriptionBuiltInAStartupListenerThatSetsNoOrderIsKnownToTheCheck() {
    run(
        null,
        null,
        app(
            "test.unordered-listener-built",
            "true",
            "streamrune.saga.subscriptions.unordered-saga.parallelism",
            "5"),
        saga -> {
          assertThat(effective(saga)).isEqualTo(5);
          assertThat(SagaParallelismDefault.sourceOf(saga))
              .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
        });
  }

  @Test
  void aValueAtThePrefixItselfNamesNoSubscriptionAndIsNotRead() {
    run(
        null,
        "order-saga",
        app("streamrune.saga.subscriptions", "8"),
        saga -> {
          assertThat(effective(saga)).isEqualTo(4);
          assertThat(SagaParallelismDefault.sourceOf(saga))
              .isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
        });
  }

  @Test
  void anUnreadableValueRefusesTheStartNamingThePropertyAndTheValue() {
    for (String bad : List.of("abc", "0")) {
      assertThat(failureOf(app("streamrune.saga.subscriptions.order-saga.parallelism", bad)))
          .contains("streamrune.saga.subscriptions.order-saga.parallelism")
          .contains("'" + bad + "'");
      assertThat(failureOf(app("streamrune.saga.parallelism", bad)))
          .contains("streamrune.saga.parallelism")
          .contains("'" + bad + "'");
    }
  }

  /** Captures what the startup check logs until {@link Capture#detach()}. */
  private record Capture(Logger log, ListAppender<ILoggingEvent> list, Level before) {
    void detach() {
      log.detachAppender(list);
      log.setLevel(before);
    }
  }

  private static Capture capture() {
    var log = (Logger) LoggerFactory.getLogger(SagaParallelismStartupCheck.class);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    log.addAppender(appender);
    var before = log.getLevel();
    log.setLevel(Level.INFO);
    return new Capture(log, appender, before);
  }

  private static List<String> messages(ListAppender<ILoggingEvent> events, Level level) {
    return new ArrayList<>(events.list)
        .stream()
            .filter(e -> e.getLevel() == level)
            .map(ILoggingEvent::getFormattedMessage)
            .toList();
  }

  /**
   * The properties of an application with the test's pool and, with {@code withLocker}, the test's
   * locker (else the integration's default). The retention sweepers are off: they would query a
   * pool that reaches no database.
   */
  private static Map<String, Object> pooled(boolean withLocker, Object... pairs) {
    Map<String, Object> properties = app(pairs);
    properties.put("test.pool", "true");
    if (withLocker) {
      properties.put("test.locker", "true");
    }
    properties.put("streamrune.inbox.retention-max-age", "0s");
    properties.put("streamrune.saga.dead-letter-retention-max-age", "0s");
    return properties;
  }

  /** Starts an application with a pool and locker, returning what the check logged. */
  private static Capture started(
      Integer builder,
      Map<String, Object> properties,
      Supplier<DataSource> pool,
      Supplier<AggregateLocker> lock) {
    dataSource = pool;
    locker = lock;
    var captured = capture();
    try {
      run(builder, "order-saga", properties, saga -> {});
    } finally {
      captured.detach();
      dataSource = null;
      locker = null;
    }
    return captured;
  }

  @Test
  void theStartupLineNamesValueSourceAndBudget() {
    var captured = capture();
    run(2, "order-saga", app(), saga -> {});
    captured.detach();
    assertThat(messages(captured.list(), Level.INFO))
        .contains(
            "StreamRune: saga type "
                + SAGA_TYPE
                + " (subscription 'order-saga'): parallelism 2 (SagaRunner.Builder.parallelism);"
                + " its steps hold up to 2 connections at once");
  }

  private static HikariDataSource hikari(int max) {
    var pool = new HikariDataSource();
    pool.setMaximumPoolSize(max);
    return pool;
  }

  @Test
  void aSmallSharedPoolLogsTheWarn_aDedicatedLockPoolDoesNot() {
    var shared = hikari(10);
    var sharedLog = started(null, pooled(true), () -> shared, () -> new PgAdvisoryLocker(shared));
    assertThat(messages(sharedLog.list(), Level.WARN))
        .singleElement()
        .asString()
        .contains("up to 8 connections")
        .contains("maximum of 10");

    var pool = hikari(10);
    var lockPool = hikari(10);
    var dedicatedLog =
        started(null, pooled(true), () -> pool, () -> PgAdvisoryLocker.withDedicatedPool(lockPool));
    assertThat(messages(dedicatedLog.list(), Level.WARN)).isEmpty();
  }

  /** A locker that keeps the SPI default of one pool connection per lock. */
  private static AggregateLocker spiDefaultLocker() {
    return (StreamId streamId, Duration timeout) -> () -> {};
  }

  @Test
  void aLockerThatKeepsTheSpiDefaultCountsTwoConnectionsPerStep() {
    var pool = hikari(10);
    var events =
        started(null, pooled(true), () -> pool, SagaParallelismWiringTest::spiDefaultLocker);
    assertThat(messages(events.list(), Level.INFO))
        .anyMatch(m -> m.contains("up to 8 connections"));
    assertThat(messages(events.list(), Level.WARN)).hasSize(1);
  }

  @Test
  void aPoolTheIntegrationCannotReadLogsNoWarn_andTheLineStillHasTheBudget() {
    var unreadable =
        started(
            null,
            pooled(true),
            PGSimpleDataSource::new,
            () -> new PgAdvisoryLocker(new PGSimpleDataSource()));
    assertThat(messages(unreadable.list(), Level.WARN)).isEmpty();
    assertThat(messages(unreadable.list(), Level.INFO))
        .anyMatch(m -> m.contains("up to 8 connections"));

    var noLocker = started(null, pooled(false), PGSimpleDataSource::new, () -> null);
    assertThat(messages(noLocker.list(), Level.WARN)).isEmpty();
    assertThat(messages(noLocker.list(), Level.INFO))
        .anyMatch(m -> m.contains("up to 4 connections"));
  }

  @Test
  void aLockSharingThePoolThatCanTakeAllOfItRefusesTheStart() {
    var shared = hikari(4);
    dataSource = () -> shared;
    locker = () -> new PgAdvisoryLocker(shared);
    try {
      assertThat(failureOf(pooled(true, "streamrune.saga.parallelism", "4")))
          .contains(
              "StreamRune: saga steps can hold up to 4 connections of the connection pool"
                  + " as aggregate locks at once (4 steps, 1 per lock), at least the"
                  + " pool's maximum of 4: every connection can be taken by a lock while"
                  + " its step waits for a second one. Give the aggregate locker a pool"
                  + " of its own (PgAdvisoryLocker.withDedicatedPool), raise the pool,"
                  + " or lower streamrune.saga.parallelism.");

      var pool = hikari(4);
      var lockPool = hikari(4);
      dataSource = () -> pool;
      locker = () -> PgAdvisoryLocker.withDedicatedPool(lockPool);
      run(
          null,
          "order-saga",
          pooled(true, "streamrune.saga.parallelism", "4"),
          saga -> assertThat(effective(saga)).isEqualTo(4));
    } finally {
      dataSource = null;
      locker = null;
    }
  }

  @Test
  void aPoolTheIntegrationCannotReadIsNeverRefused() {
    dataSource = PGSimpleDataSource::new;
    locker = () -> new PgAdvisoryLocker(new PGSimpleDataSource());
    try {
      run(
          null,
          "order-saga",
          pooled(true, "streamrune.saga.parallelism", "64"),
          saga -> assertThat(effective(saga)).isEqualTo(64));
    } finally {
      dataSource = null;
      locker = null;
    }
  }
}
