package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.postgres.PgAdvisoryLocker;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaParallelismDefault;
import org.streamrune.runtime.SagaParallelismSource;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.spring.SagaEventPathRetentionWiringTest.FakeOrchestrator;
import org.streamrune.spring.SagaEventPathRetentionWiringTest.FakeState;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Saga parallelism through the real auto-configuration: the global property, the per-subscription
 * property, the startup check (unknown names, one line per runner, the refusal, the pool warning).
 */
class SagaParallelismWiringTest {

  private static final String SAGA_TYPE = SagaType.fromClass(FakeState.class).value();

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
          .withBean(SagaStore.class, InMemorySagaStore::new);

  private static SagaRunner<FakeState> sagaRunner(Integer builderParallelism) {
    var store = new InMemorySagaStore();
    var builder =
        SagaRunner.<FakeState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(store)
            .commandBus(SagaEventPathRetentionWiringTest.NOOP_BUS)
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
        .config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
        .sagaRunner(runner)
        .build();
  }

  /** A runner bean and a subscription bean built over it with the given name. */
  private ApplicationContextRunner withSaga(SagaRunner<FakeState> saga, String subscriptionName) {
    return runner
        .withBean("orderSagaRunner", SagaRunner.class, () -> saga)
        .withBean(
            "orderSubscription",
            PollingEventSubscription.class,
            () -> subscription(subscriptionName, saga));
  }

  private static int effective(SagaRunner<?> saga) {
    return SagaParallelismDefault.effectiveOf(saga);
  }

  @Test
  void theGlobalPropertyReachesARunnerThatNamedNothing() {
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withPropertyValues("streamrune.saga.parallelism=8")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(8);
              assertThat(SagaParallelismDefault.sourceOf(saga))
                  .isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
            });
  }

  @Test
  void aRunnerThatNamedItsParallelismKeepsItAgainstTheGlobalProperty() {
    var saga = sagaRunner(2);
    withSaga(saga, "order-saga")
        .withPropertyValues("streamrune.saga.parallelism=8")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(2);
            });
  }

  @Test
  void aSubscriptionPropertyWinsOverTheBuilder() {
    var saga = sagaRunner(2);
    withSaga(saga, "order-saga")
        .withPropertyValues("streamrune.saga.subscriptions.order-saga.parallelism=16")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(16);
            });
  }

  @Test
  void aSubscriptionNameWithADotIsMatchedExactly() {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.[orders.v2].parallelism",
            "streamrune.saga.subscriptions.orders.v2.parallelism")) {
      var saga = sagaRunner(null);
      withSaga(saga, "orders.v2")
          .withPropertyValues(key + "=3")
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(effective(saga)).as(key).isEqualTo(3);
                assertThat(SagaParallelismDefault.sourceOf(saga))
                    .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
              });
    }
  }

  @Test
  void aSubscriptionNameWithUpperCaseIsMatchedExactly() {
    var saga = sagaRunner(null);
    withSaga(saga, "Orders")
        .withPropertyValues("streamrune.saga.subscriptions.[Orders].parallelism=3")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(3);
            });
    var unbracketed = sagaRunner(null);
    withSaga(unbracketed, "Orders")
        .withPropertyValues("streamrune.saga.subscriptions.Orders.parallelism=3")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(unbracketed)).isEqualTo(3);
              assertThat(SagaParallelismDefault.sourceOf(unbracketed))
                  .isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
            });
  }

  /** Two runner beans, each with a subscription bean of the given name built over it. */
  private ApplicationContextRunner withTwoSagas(
      SagaRunner<FakeState> first,
      String firstName,
      SagaRunner<FakeState> second,
      String secondName) {
    return withSaga(first, firstName)
        .withBean("otherSagaRunner", SagaRunner.class, () -> second)
        .withBean(
            "otherSubscription",
            PollingEventSubscription.class,
            () -> subscription(secondName, second));
  }

  @Test
  void twoUnbracketedNamesSpringReadsAsOnePropertyRefuseTheStart() {
    for (String[] pair :
        List.of(
            new String[] {"Orders", "orders"},
            new String[] {"order-saga", "orderSaga"},
            new String[] {"order_saga", "ordersaga"})) {
      String one = "streamrune.saga.subscriptions." + pair[0] + ".parallelism";
      String other = "streamrune.saga.subscriptions." + pair[1] + ".parallelism";
      withTwoSagas(sagaRunner(null), pair[0], sagaRunner(null), pair[1])
          .withPropertyValues(one + "=5", other + "=3")
          .run(
              ctx ->
                  assertThat(ctx)
                      .hasFailed()
                      .getFailure()
                      .hasStackTraceContaining(
                          one
                              + " and "
                              + other
                              + " are one property to Spring, which can give either the value of"
                              + " the other; write each name in brackets or set only one of"
                              + " them"));
    }
  }

  @Test
  void twoBracketedNamesThatDifferOnlyInCaseOrDashEachTakeTheirOwnValue() {
    for (String[] pair :
        List.of(new String[] {"Orders", "orders"}, new String[] {"order-saga", "orderSaga"})) {
      var first = sagaRunner(null);
      var second = sagaRunner(null);
      withTwoSagas(first, pair[0], second, pair[1])
          .withPropertyValues(
              "streamrune.saga.subscriptions.[" + pair[0] + "].parallelism=5",
              "streamrune.saga.subscriptions.[" + pair[1] + "].parallelism=3")
          .run(
              ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(effective(first)).as(pair[0]).isEqualTo(5);
                assertThat(effective(second)).as(pair[1]).isEqualTo(3);
              });
    }
  }

  @Test
  void twoNamesSpringReadsAsOnePropertyInTwoSourcesEachTakeTheirOwnValue() {
    var first = sagaRunner(null);
    var second = sagaRunner(null);
    withTwoSagas(first, "Orders", second, "orders")
        .withInitializer(
            sources(
                new MapPropertySource(
                    "one", Map.of("streamrune.saga.subscriptions.Orders.parallelism", "5")),
                new MapPropertySource(
                    "other", Map.of("streamrune.saga.subscriptions.orders.parallelism", "3"))))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(first)).isEqualTo(5);
              assertThat(effective(second)).isEqualTo(3);
            });
  }

  @Test
  void aValueAtThePrefixItselfNamesNoSubscriptionAndIsNotRead() {
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withPropertyValues("streamrune.saga.subscriptions=8")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(4);
              assertThat(SagaParallelismDefault.sourceOf(saga))
                  .isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
            });
  }

  private static String notAProperty(String key) {
    return key + " is not a property; set streamrune.saga.subscriptions.<name>.parallelism";
  }

  @Test
  void aMisspeltParallelismRefusesTheStartNamingTheKey() {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.order-saga.paralelism",
            "streamrune.saga.subscriptions.[orders.v2].paralelism")) {
      var saga = sagaRunner(null);
      withSaga(saga, "order-saga")
          .withPropertyValues(key + "=3")
          .run(
              ctx ->
                  assertThat(ctx)
                      .hasFailed()
                      .getFailure()
                      .hasStackTraceContaining(notAProperty(key)));
    }
  }

  @Test
  void aKeyThatIsNotAPropertyIsNamedSanitizedForTheLog() {
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withInitializer(
            sources(
                new MapPropertySource(
                    "forged",
                    Map.of("streamrune.saga.subscriptions.[order\nsaga].paralelism", "3"))))
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining(
                        notAProperty("streamrune.saga.subscriptions.ordersaga.paralelism")));
  }

  @Test
  void aKeyWithoutASubscriptionNameRefusesTheStart() {
    for (String key :
        List.of(
            "streamrune.saga.subscriptions.parallelism", "streamrune.saga.subscriptions.orders")) {
      var saga = sagaRunner(null);
      withSaga(saga, "order-saga")
          .withPropertyValues(key + "=3")
          .run(
              ctx ->
                  assertThat(ctx)
                      .hasFailed()
                      .getFailure()
                      .hasStackTraceContaining(notAProperty(key)));
    }
  }

  /** Puts the sources first, the first of them with the highest precedence. */
  private static ApplicationContextInitializer<ConfigurableApplicationContext> sources(
      PropertySource<?>... sources) {
    return context -> {
      for (int i = sources.length - 1; i >= 0; i--) {
        context.getEnvironment().getPropertySources().addFirst(sources[i]);
      }
    };
  }

  @Test
  void anEnvironmentVariableReadsUnderscoresAsDotsAndRefusesTheUnknownName() {
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withInitializer(
            sources(
                new SystemEnvironmentPropertySource(
                    "test-systemEnvironment",
                    Map.of("STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDER_SAGA_PARALLELISM", "8"))))
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining("bound to: [order.saga]")
                    .hasStackTraceContaining("Subscriptions bound to a saga runner: [order-saga]"));
  }

  @Test
  void aValueSetInTwoSourcesTakesTheOneOfHigherPrecedence() {
    var saga = sagaRunner(null);
    withSaga(saga, "orders.v2")
        .withInitializer(
            sources(
                new MapPropertySource(
                    "high", Map.of("streamrune.saga.subscriptions.orders.v2.parallelism", "16")),
                new MapPropertySource(
                    "low", Map.of("streamrune.saga.subscriptions.[orders.v2].parallelism", "3"))))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(16);
            });
  }

  @Test
  void aPropertyWithoutAValueRefusesTheStart() {
    Map<String, Object> nothing = new HashMap<>();
    nothing.put("streamrune.saga.subscriptions.order-saga.parallelism", null);
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withInitializer(sources(new MapPropertySource("nothing", nothing)))
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining(
                        "streamrune.saga.subscriptions.order-saga.parallelism must be a whole"
                            + " number"));
  }

  @Test
  void aPlaceholderInTheValueIsResolved() {
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withPropertyValues(
            "saga.width=6", "streamrune.saga.subscriptions.order-saga.parallelism=${saga.width}")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(6);
            });
  }

  @Test
  void anUnknownSubscriptionNameRefusesTheStartListingTheKnownNames() {
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withPropertyValues("streamrune.saga.subscriptions.nope.parallelism=3")
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining("[nope]")
                    .hasStackTraceContaining("[order-saga]"));
  }

  @Test
  void withoutASagaRunnerASubscriptionPropertyRefusesTheStart() {
    runner
        .withPropertyValues("streamrune.saga.subscriptions.nope.parallelism=3")
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining("bound to: [nope]")
                    .hasStackTraceContaining("Subscriptions bound to a saga runner: []"));
  }

  @Test
  void theCheckRunsOnceForItsOwnContextOnly() {
    var saga = sagaRunner(2);
    List<ILoggingEvent> events =
        capture(
            () ->
                withSaga(saga, "order-saga")
                    .run(
                        ctx -> {
                          assertThat(ctx).hasNotFailed();
                          try (var child = new GenericApplicationContext()) {
                            child.setParent(ctx.getSourceApplicationContext());
                            child.refresh();
                          }
                          ctx.publishEvent(
                              new ContextRefreshedEvent(ctx.getSourceApplicationContext()));
                        }));
    assertThat(messages(events, Level.INFO))
        .filteredOn(m -> m.startsWith("StreamRune: saga type "))
        .hasSize(1);
  }

  /** Starts a subscription in {@code start()}; it is not a bean. */
  static final class LifecycleBuiltSubscription implements SmartLifecycle {
    private final SagaRunner<?> saga;
    private PollingEventSubscription subscription;

    LifecycleBuiltSubscription(SagaRunner<?> saga) {
      this.saga = saga;
    }

    @Override
    public void start() {
      subscription = subscription("lifecycle-saga", saga);
    }

    @Override
    public void stop() {
      if (subscription != null) {
        subscription.close();
        subscription = null;
      }
    }

    @Override
    public boolean isRunning() {
      return subscription != null;
    }
  }

  @Test
  void aSubscriptionBuiltInASmartLifecycleIsKnownToTheCheck() {
    var saga = sagaRunner(null);
    runner
        .withBean("orderSagaRunner", SagaRunner.class, () -> saga)
        .withBean(
            "builder", LifecycleBuiltSubscription.class, () -> new LifecycleBuiltSubscription(saga))
        .withPropertyValues("streamrune.saga.subscriptions.lifecycle-saga.parallelism=5")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(5);
            });
  }

  /** Refreshes a child context of its own context while the parent is still starting. */
  static final class EarlyChildContext implements SmartLifecycle, ApplicationContextAware {
    private ApplicationContext parent;
    private boolean running;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
      this.parent = applicationContext;
    }

    @Override
    public int getPhase() {
      return -1000;
    }

    @Override
    public void start() {
      try (var child = new GenericApplicationContext()) {
        child.setParent(parent);
        child.refresh();
      }
      running = true;
    }

    @Override
    public void stop() {
      running = false;
    }

    @Override
    public boolean isRunning() {
      return running;
    }
  }

  @Test
  void aChildContextRefreshedWhileTheParentStartsDoesNotRunTheCheckEarly() {
    var saga = sagaRunner(null);
    runner
        .withBean("orderSagaRunner", SagaRunner.class, () -> saga)
        .withBean("early", EarlyChildContext.class, EarlyChildContext::new)
        .withBean(
            "builder", LifecycleBuiltSubscription.class, () -> new LifecycleBuiltSubscription(saga))
        .withPropertyValues("streamrune.saga.subscriptions.lifecycle-saga.parallelism=5")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(saga)).isEqualTo(5);
            });
  }

  @Test
  void aSubscriptionNameWithACharacterOutsideTheElementSetNeedsBrackets() {
    var bracketed = sagaRunner(null);
    withSaga(bracketed, "orders/v2")
        .withPropertyValues("streamrune.saga.subscriptions.[orders/v2].parallelism=3")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(bracketed)).isEqualTo(3);
            });
    var unbracketed = sagaRunner(null);
    withSaga(unbracketed, "ordersv2")
        .withPropertyValues("streamrune.saga.subscriptions.orders/v2.parallelism=3")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(effective(unbracketed)).isEqualTo(3);
            });
  }

  @Test
  void anUnreadableValueRefusesTheStartNamingThePropertyAndTheValue() {
    for (String bad : List.of("abc", "0")) {
      var saga = sagaRunner(null);
      withSaga(saga, "order-saga")
          .withPropertyValues("streamrune.saga.subscriptions.order-saga.parallelism=" + bad)
          .run(
              ctx ->
                  assertThat(ctx)
                      .hasFailed()
                      .getFailure()
                      .hasStackTraceContaining(
                          "streamrune.saga.subscriptions.order-saga.parallelism")
                      .hasStackTraceContaining("'" + bad + "'"));
      var global = sagaRunner(null);
      withSaga(global, "order-saga")
          .withPropertyValues("streamrune.saga.parallelism=" + bad)
          .run(
              ctx ->
                  assertThat(ctx)
                      .hasFailed()
                      .getFailure()
                      .hasStackTraceContaining("streamrune.saga.parallelism")
                      .hasStackTraceContaining("'" + bad + "'"));
    }
  }

  private static List<ILoggingEvent> capture(Runnable body) {
    var log = (Logger) LoggerFactory.getLogger(SagaParallelismStartupCheck.class);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    log.addAppender(appender);
    try {
      body.run();
      return new ArrayList<>(appender.list);
    } finally {
      log.detachAppender(appender);
    }
  }

  private static List<String> messages(List<ILoggingEvent> events, Level level) {
    return events.stream()
        .filter(e -> e.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void theStartupLineNamesValueSourceAndBudget() {
    var saga = sagaRunner(2);
    List<ILoggingEvent> events =
        capture(() -> withSaga(saga, "order-saga").run(ctx -> assertThat(ctx).hasNotFailed()));
    assertThat(messages(events, Level.INFO))
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
    var saga = sagaRunner(null);
    List<ILoggingEvent> sharedEvents =
        capture(
            () ->
                withSaga(saga, "order-saga")
                    .withBean(DataSource.class, () -> shared)
                    .withBean(AggregateLocker.class, () -> new PgAdvisoryLocker(shared))
                    .run(ctx -> assertThat(ctx).hasNotFailed()));
    assertThat(messages(sharedEvents, Level.WARN))
        .singleElement()
        .asString()
        .contains("up to 8 connections")
        .contains("maximum of 10");

    var pool = hikari(10);
    var dedicated = hikari(10);
    var other = sagaRunner(null);
    List<ILoggingEvent> dedicatedEvents =
        capture(
            () ->
                withSaga(other, "order-saga")
                    .withBean(DataSource.class, () -> pool)
                    .withBean(
                        AggregateLocker.class, () -> PgAdvisoryLocker.withDedicatedPool(dedicated))
                    .run(ctx -> assertThat(ctx).hasNotFailed()));
    assertThat(messages(dedicatedEvents, Level.WARN)).isEmpty();
  }

  /** A locker that keeps the SPI default of one pool connection per lock. */
  private static AggregateLocker spiDefaultLocker() {
    return (StreamId streamId, Duration timeout) -> () -> {};
  }

  @Test
  void aLockerThatKeepsTheSpiDefaultCountsTwoConnectionsPerStep() {
    var pool = hikari(10);
    var saga = sagaRunner(null);
    List<ILoggingEvent> events =
        capture(
            () ->
                withSaga(saga, "order-saga")
                    .withBean(DataSource.class, () -> pool)
                    .withBean(AggregateLocker.class, SagaParallelismWiringTest::spiDefaultLocker)
                    .run(ctx -> assertThat(ctx).hasNotFailed()));
    assertThat(messages(events, Level.INFO)).anyMatch(m -> m.contains("up to 8 connections"));
    assertThat(messages(events, Level.WARN)).hasSize(1);
  }

  @Test
  void aPoolTheIntegrationCannotReadLogsNoWarn_andTheLineStillHasTheBudget() {
    var saga = sagaRunner(null);
    List<ILoggingEvent> unreadable =
        capture(
            () ->
                withSaga(saga, "order-saga")
                    .withBean(DataSource.class, PGSimpleDataSource::new)
                    .withBean(
                        AggregateLocker.class, () -> new PgAdvisoryLocker(new PGSimpleDataSource()))
                    .run(ctx -> assertThat(ctx).hasNotFailed()));
    assertThat(messages(unreadable, Level.WARN)).isEmpty();
    assertThat(messages(unreadable, Level.INFO)).anyMatch(m -> m.contains("up to 8 connections"));

    var other = sagaRunner(null);
    List<ILoggingEvent> noLocker =
        capture(
            () ->
                withSaga(other, "order-saga")
                    .withBean(DataSource.class, PGSimpleDataSource::new)
                    .run(ctx -> assertThat(ctx).hasNotFailed()));
    assertThat(messages(noLocker, Level.WARN)).isEmpty();
    assertThat(messages(noLocker, Level.INFO)).anyMatch(m -> m.contains("up to 4 connections"));
  }

  @Test
  void aLockSharingThePoolThatCanTakeAllOfItRefusesTheStart() {
    var shared = hikari(4);
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withBean(DataSource.class, () -> shared)
        .withBean(AggregateLocker.class, () -> new PgAdvisoryLocker(shared))
        .withPropertyValues("streamrune.saga.parallelism=4")
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining(
                        "StreamRune: saga steps can hold up to 4 connections of the connection pool"
                            + " as aggregate locks at once (4 steps, 1 per lock), at least the"
                            + " pool's maximum of 4: every connection can be taken by a lock while"
                            + " its step waits for a second one. Give the aggregate locker a pool"
                            + " of its own (PgAdvisoryLocker.withDedicatedPool), raise the pool,"
                            + " or lower streamrune.saga.parallelism."));

    var pool = hikari(4);
    var lockPool = hikari(4);
    var other = sagaRunner(null);
    withSaga(other, "order-saga")
        .withBean(DataSource.class, () -> pool)
        .withBean(AggregateLocker.class, () -> PgAdvisoryLocker.withDedicatedPool(lockPool))
        .withPropertyValues("streamrune.saga.parallelism=4")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void aPoolTheIntegrationCannotReadIsNeverRefused() {
    var saga = sagaRunner(null);
    withSaga(saga, "order-saga")
        .withBean(DataSource.class, PGSimpleDataSource::new)
        .withBean(AggregateLocker.class, () -> new PgAdvisoryLocker(new PGSimpleDataSource()))
        .withPropertyValues("streamrune.saga.parallelism=64")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }
}
