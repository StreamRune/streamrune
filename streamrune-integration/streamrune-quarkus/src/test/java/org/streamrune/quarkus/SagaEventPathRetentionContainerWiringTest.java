package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaForwardFenceDefault;
import org.streamrune.runtime.SagaLeaseWiring;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The CONTAINER half of the Quarkus event-path retention wiring. {@link
 * SagaEventPathRetentionWiringTest} pins the configurer's logic by constructing it with {@code new}
 * and calling {@code apply(null)} — which exercises none of the three container obligations the
 * bean actually relies on: (1) Arc discovers the {@code @ApplicationScoped} configurer at all, (2)
 * its {@code @All List<SagaRunner<?>>} injection point collects the APPLICATION's runner beans, and
 * (3) its {@code @Observes StartupEvent} observer fires on startup. This test boots the real Arc
 * container through {@link RealArcTestContainer} and pins all three: an application-produced runner
 * with no explicit window takes the configured default the moment the startup event fires, and one
 * with an explicit window keeps it.
 */
class SagaEventPathRetentionContainerWiringTest {

  record FakeState(SagaStatus status, String id) implements SagaState {}

  static final class FakeOrchestrator implements SagaOrchestrator<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("x");
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final CommandBus NOOP_BUS =
      new CommandBus() {
        @Override
        public boolean supportsIdempotentExecution() {
          return true;
        }

        @Override
        public <C extends Command> CommandResult execute(C command) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          throw new UnsupportedOperationException();
        }
      };

  /**
   * The application's runner beans — static so the test can inspect the very instances Arc
   * injected.
   */
  static final InMemorySagaStore APP_SAGA_STORE = new InMemorySagaStore();

  static final SagaRunner<FakeState> APP_RUNNER =
      SagaRunner.<FakeState>builder()
          .orchestrator(new FakeOrchestrator())
          .sagaStore(APP_SAGA_STORE)
          .commandBus(NOOP_BUS)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(APP_SAGA_STORE))
          // The bus is a test double that stores nothing; the fence is tested in
          // SagaForwardFence*Test.
          .forwardFence(false)
          .build();

  static final InMemorySagaStore EXPLICIT_SAGA_STORE = new InMemorySagaStore();

  static final SagaRunner<FakeState> EXPLICIT_RUNNER =
      SagaRunner.<FakeState>builder()
          .orchestrator(new FakeOrchestrator())
          .sagaStore(EXPLICIT_SAGA_STORE)
          .commandBus(NOOP_BUS)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(EXPLICIT_SAGA_STORE))
          .inboxRetentionMaxAge(Duration.ofHours(2))
          // The bus is a test double that stores nothing; the fence is tested in
          // SagaForwardFence*Test.
          .forwardFence(false)
          .build();

  /**
   * Application-side beans: the real {@code @ConfigMapping} binding with a configured window, and
   * two {@code SagaRunner} producers the configurer must discover through {@code @All}.
   */
  public static class ApplicationBeans {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      SmallRyeConfig config =
          new SmallRyeConfigBuilder()
              .withMapping(StreamRuneQuarkusProperties.class)
              .withConverter(
                  Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
              .withDefaultValues(Map.of("streamrune.inbox.retention-max-age", "3d"))
              .build();
      return config.getConfigMapping(StreamRuneQuarkusProperties.class);
    }

    @Produces
    @Singleton
    @Named("appRunner")
    public SagaRunner<FakeState> appRunner() {
      return APP_RUNNER;
    }

    @Produces
    @Singleton
    @Named("explicitRunner")
    public SagaRunner<FakeState> explicitRunner() {
      return EXPLICIT_RUNNER;
    }
  }

  @Test
  void realContainer_discoversTheConfigurer_collectsAllRunners_andAppliesOnStartupEvent()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(SagaEventPathRetentionConfigurer.class, ApplicationBeans.class))) {
      // Obligation 1: Arc bean discovery of the framework's @ApplicationScoped configurer.
      assertTrue(
          arc.container().instance(SagaEventPathRetentionConfigurer.class).isAvailable(),
          "the @ApplicationScoped configurer must be a resolvable Arc bean");
      assertNull(
          SagaEventPathRetention.effectiveWindowOf(APP_RUNNER),
          "precondition: nothing has applied the default before the startup event");

      // Obligation 3: the @Observes StartupEvent observer fires when the container starts up.
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());

      // Obligation 2: the @All List<SagaRunner<?>> injection point collected BOTH application
      // runner beans — the default landed on the one without a window, and the explicit one kept
      // its own value.
      assertEquals(
          Duration.ofDays(3),
          SagaEventPathRetention.effectiveWindowOf(APP_RUNNER),
          "the configured window must reach the application runner's event-path guard");
      assertEquals(
          Duration.ofHours(2),
          SagaEventPathRetention.effectiveWindowOf(EXPLICIT_RUNNER),
          "an application-supplied window is never overwritten");
    }
  }

  /**
   * Producers of two runners built for the startup report alone: the runners of {@link
   * ApplicationBeans} are shared by the test that watches their window before and after startup.
   */
  public static class UngatedRunnerBeans {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      SmallRyeConfig config =
          new SmallRyeConfigBuilder()
              .withMapping(StreamRuneQuarkusProperties.class)
              .withConverter(
                  Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
              .build();
      return config.getConfigMapping(StreamRuneQuarkusProperties.class);
    }

    @Produces
    @Singleton
    @Named("firstRunner")
    public SagaRunner<FakeState> firstRunner() {
      return fencedRunner(null);
    }

    @Produces
    @Singleton
    @Named("secondRunner")
    public SagaRunner<FakeState> secondRunner() {
      return fencedRunner(null);
    }
  }

  /** A leadership bean that is not {@link SubscriptionLeadership#NOOP}. */
  public static class LeadershipBeans {
    @Produces
    @Singleton
    public SubscriptionLeadership leadership() {
      return new SubscriptionLeadership() {
        @Override
        public Optional<Lease> tryAcquire(String consumerName) {
          return Optional.empty();
        }

        @Override
        public Optional<Lease> current(String consumerName) {
          return Optional.empty();
        }

        @Override
        public void resign(String consumerName) {}

        @Override
        public void close() {}
      };
    }
  }

  /**
   * This module binds SLF4J to JBoss Logging, so the startup report is observed at the call that
   * writes it: how many runners {@link SagaLeaseWiring#warnUngatedUnderLeadership} reported.
   */
  private static List<Object> reportsWhileStarting(List<Class<?>> beans) throws Exception {
    List<Object> reports = new java.util.ArrayList<>();
    try (var arc = RealArcTestContainer.boot(beans);
        var mocked =
            mockStatic(
                SagaLeaseWiring.class,
                invocation -> {
                  Object reported = invocation.callRealMethod();
                  reports.add(reported);
                  return reported;
                })) {
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
    }
    return reports;
  }

  @Test
  void realContainer_anUngatedRunnerUnderRealLeadershipIsReported() throws Exception {
    var reports =
        reportsWhileStarting(
            List.of(
                SagaEventPathRetentionConfigurer.class,
                UngatedRunnerBeans.class,
                LeadershipBeans.class));

    assertEquals(
        List.of(2), reports, "the two application runners carry no lease: both are reported");
  }

  @Test
  void realContainer_nothingIsReportedWithoutALeadershipBean() throws Exception {
    var reports =
        reportsWhileStarting(
            List.of(SagaEventPathRetentionConfigurer.class, UngatedRunnerBeans.class));

    assertEquals(List.of(0), reports);
  }

  // ---- streamrune.saga.forward-fence --------------------------------------------------------

  /** The property values and runners the {@link FenceBeans} producers hand to the container. */
  static volatile Map<String, String> fenceProperties = Map.of();

  static volatile SagaRunner<FakeState> namesNothing;
  static volatile SagaRunner<FakeState> namesItsFence;

  /**
   * A bus that, like the one over a fencing event store, answers the saga rows its forward commands
   * are fenced against.
   */
  private static CommandBus fencingBus(InMemoryEventStore eventStore) {
    return new CommandBus() {
      @Override
      public boolean supportsIdempotentExecution() {
        return true;
      }

      @Override
      public Object sagaFenceStorageIdentity() {
        return eventStore.sagaFenceStorageIdentity();
      }

      @Override
      public <C extends Command> CommandResult execute(C command) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** A runner over a store the event store fences against; {@code named} null names no fence. */
  private static SagaRunner<FakeState> fencedRunner(Boolean named) {
    var sagaStore = new InMemorySagaStore();
    var eventStore =
        new InMemoryEventStore()
            .withCommandInbox(new InMemoryCommandInbox())
            .withSagaStore(sagaStore);
    var builder =
        SagaRunner.<FakeState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(fencingBus(eventStore))
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore));
    if (named != null) {
      builder.forwardFence(named);
    }
    return builder.build();
  }

  /** Producers for the fence tests: the real mapping bound to {@link #fenceProperties}. */
  public static class FenceBeans {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      SmallRyeConfig config =
          new SmallRyeConfigBuilder()
              .withMapping(StreamRuneQuarkusProperties.class)
              .withConverter(
                  Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
              .withDefaultValues(fenceProperties)
              .build();
      return config.getConfigMapping(StreamRuneQuarkusProperties.class);
    }

    @Produces
    @Singleton
    @Named("namesNothing")
    public SagaRunner<FakeState> namesNothing() {
      return namesNothing;
    }

    @Produces
    @Singleton
    @Named("namesItsFence")
    public SagaRunner<FakeState> namesItsFence() {
      return namesItsFence;
    }
  }

  /** Boots the container with {@code properties}, fires the startup event, returns the runners. */
  private static void startWith(
      Map<String, String> properties, Boolean theOtherRunnerNames, Runnable afterStartup)
      throws Exception {
    fenceProperties = properties;
    namesNothing = fencedRunner(null);
    namesItsFence = fencedRunner(theOtherRunnerNames);
    try (var arc =
        RealArcTestContainer.boot(
            List.of(SagaEventPathRetentionConfigurer.class, FenceBeans.class))) {
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
      afterStartup.run();
    }
  }

  @Test
  void auto_withSingleActiveConsumer_fencesARunnerThatNamedNothing() throws Exception {
    startWith(Map.of(), false, () -> assertTrue(SagaForwardFenceDefault.effectiveOf(namesNothing)));
  }

  @Test
  void auto_withoutSingleActiveConsumer_switchesItOff() throws Exception {
    startWith(
        Map.of("streamrune.subscription.single-active-consumer.enabled", "false"),
        false,
        () -> assertFalse(SagaForwardFenceDefault.effectiveOf(namesNothing)));
  }

  @Test
  void on_fences() throws Exception {
    startWith(
        Map.of(
            "streamrune.subscription.single-active-consumer.enabled", "false",
            "streamrune.saga.forward-fence", "on"),
        false,
        () -> assertTrue(SagaForwardFenceDefault.effectiveOf(namesNothing)));
  }

  @Test
  void off_switchesItOff() throws Exception {
    startWith(
        Map.of("streamrune.saga.forward-fence", "off"),
        false,
        () -> assertFalse(SagaForwardFenceDefault.effectiveOf(namesNothing)));
  }

  @Test
  void aRunnerThatNamedItsFenceKeepsIt() throws Exception {
    startWith(
        Map.of("streamrune.saga.forward-fence", "off"),
        true,
        () -> assertTrue(SagaForwardFenceDefault.effectiveOf(namesItsFence)));
    startWith(
        Map.of("streamrune.saga.forward-fence", "on"),
        false,
        () -> assertFalse(SagaForwardFenceDefault.effectiveOf(namesItsFence)));
  }

  /**
   * {@code streamrune.subscription.single-active-consumer.enabled} has one meaning: the leadership
   * producer hands out a real lease exactly when the forward fence's {@code auto} switches the
   * fence on. Both read one parser, the same one Spring and Micronaut use.
   */
  @ParameterizedTest
  @CsvSource({
    "true,true", "on,true", "yes,true", "TRUE,true", "1,true",
    "false,false", "off,false", "no,false", "Off,false", "0,false"
  })
  void singleActiveConsumer_meansTheSameForTheLeadershipProducerAndTheFence(
      String value, boolean on) throws Exception {
    Map<String, String> properties =
        Map.of("streamrune.subscription.single-active-consumer.enabled", value);
    @SuppressWarnings("unchecked")
    Instance<DataSource> dataSource = mock(Instance.class);
    when(dataSource.isUnsatisfied()).thenReturn(false);
    when(dataSource.isResolvable()).thenReturn(true);
    when(dataSource.get()).thenReturn(mock(DataSource.class));
    SubscriptionLeadership leadership =
        new StreamRuneProducers().subscriptionLeadership(dataSource, TestProperties.of(properties));
    try {
      assertEquals(
          on,
          leadership instanceof org.streamrune.postgres.LeaseBasedLeadership,
          () -> "leadership for enabled=" + value);
    } finally {
      leadership.close();
    }
    startWith(
        properties,
        false,
        () ->
            assertEquals(
                on,
                SagaForwardFenceDefault.effectiveOf(namesNothing),
                () -> "forward fence for enabled=" + value));
  }

  @Test
  void singleActiveConsumer_anUnknownSpellingFailsStartupNamingTheProperty() {
    var failure =
        assertThrows(
            Exception.class,
            () ->
                startWith(
                    Map.of("streamrune.subscription.single-active-consumer.enabled", "maybe"),
                    false,
                    () -> {}));
    Throwable cause = failure;
    boolean found = false;
    while (cause != null && !found) {
      found =
          cause.getMessage() != null
              && cause
                  .getMessage()
                  .contains(
                      "streamrune.subscription.single-active-consumer.enabled must be true or"
                          + " false")
              && cause.getMessage().contains("got: 'maybe'");
      cause = cause.getCause();
    }
    assertTrue(found, "the startup failure names the property and the value: " + failure);
  }

  /** A leadership that holds nothing and grants nothing. */
  @Singleton
  public static class LeadershipA implements SubscriptionLeadership {
    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return Optional.empty();
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return Optional.empty();
    }

    @Override
    public void resign(String consumerName) {}

    @Override
    public void close() {}
  }

  /** A second leadership bean, so the refusal can name both. */
  @Singleton
  public static class LeadershipB extends LeadershipA {}

  /** Two leadership producers in one configuration class. */
  public static class TwoLeadershipProducers {
    @Produces
    @Singleton
    @Named("leadershipA")
    public SubscriptionLeadership leadershipA() {
      return new LeadershipA();
    }

    @Produces
    @Singleton
    @Named("leadershipB")
    public SubscriptionLeadership leadershipB() {
      return new LeadershipB();
    }
  }

  /**
   * Two producers of one class are told apart in the refusal: each is named by its producer method,
   * not by the declaring class both share.
   */
  @Test
  void twoLeadershipProducersOfOneClassAreNamedApart() {
    fenceProperties = Map.of();
    namesNothing = fencedRunner(null);
    namesItsFence = fencedRunner(false);
    var failure =
        assertThrows(
            Exception.class,
            () -> {
              try (var arc =
                  RealArcTestContainer.boot(
                      List.of(
                          SagaEventPathRetentionConfigurer.class,
                          FenceBeans.class,
                          TwoLeadershipProducers.class))) {
                arc.container()
                    .beanManager()
                    .getEvent()
                    .select(StartupEvent.class)
                    .fire(new StartupEvent());
              }
            });
    String message = null;
    for (Throwable t = failure; t != null && message == null; t = t.getCause()) {
      if (t.getMessage() != null
          && t.getMessage().contains("SubscriptionLeadership beans and none is the default")) {
        message = t.getMessage();
      }
    }
    assertTrue(message != null, "the startup failure explains the two leaderships: " + failure);
    assertTrue(message.contains("leadershipA") && message.contains("leadershipB"), message);
  }

  /**
   * Two leadership beans and none the default: startup is refused with a message naming both, the
   * way Spring and Micronaut refuse them. The startup hook used to read the ambiguous leadership as
   * absent and report nothing.
   */
  @Test
  void twoLeadershipBeansWithoutADefaultFailStartupNamingBoth() {
    fenceProperties = Map.of();
    namesNothing = fencedRunner(null);
    namesItsFence = fencedRunner(false);
    var failure =
        assertThrows(
            Exception.class,
            () -> {
              try (var arc =
                  RealArcTestContainer.boot(
                      List.of(
                          SagaEventPathRetentionConfigurer.class,
                          FenceBeans.class,
                          LeadershipA.class,
                          LeadershipB.class))) {
                arc.container()
                    .beanManager()
                    .getEvent()
                    .select(StartupEvent.class)
                    .fire(new StartupEvent());
              }
            });
    Throwable cause = failure;
    String message = null;
    while (cause != null && message == null) {
      if (cause.getMessage() != null
          && cause.getMessage().contains("SubscriptionLeadership beans and none is the default")) {
        message = cause.getMessage();
      }
      cause = cause.getCause();
    }
    assertTrue(message != null, "the startup failure explains the two leaderships: " + failure);
    assertTrue(message.contains(LeadershipA.class.getName()), message);
    assertTrue(message.contains(LeadershipB.class.getName()), message);
  }

  @Test
  void anUnknownValueFailsStartup() {
    var failure =
        assertThrows(
            Exception.class,
            () -> startWith(Map.of("streamrune.saga.forward-fence", "maybe"), false, () -> {}));
    Throwable cause = failure;
    boolean found = false;
    while (cause != null && !found) {
      found =
          cause.getMessage() != null
              && cause
                  .getMessage()
                  .contains("streamrune.saga.forward-fence must be auto, on or off")
              && cause.getMessage().contains("got: 'maybe'");
      cause = cause.getCause();
    }
    assertTrue(found, "the startup failure names the property and the value: " + failure);
  }
}
