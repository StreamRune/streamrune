package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.interceptor.Interceptor;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.EventStore;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.quarkus.SagaEventPathRetentionContainerWiringTest.FakeOrchestrator;
import org.streamrune.quarkus.SagaEventPathRetentionContainerWiringTest.FakeState;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SseEventFeed;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.runtime.health.RelayComponentNames;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A start that a startup observer refuses stops every thread the framework started.
 *
 * <p>Quarkus fires no {@link ShutdownEvent} after a failed start, but it still shuts the container
 * down, which destroys the application's beans. In production the process exits; in dev mode and in
 * a test JVM it does not, so whatever the framework started before the refusal must stop when the
 * container goes. Each test boots a real Arc container with the three beans that start the
 * framework's threads ({@link StreamRuneLifecycle}, {@link SagaCompensationRetryLifecycle}, {@link
 * SseEventFeedLifecycle}), fires the startup event, and then shuts the container down as Quarkus
 * does after the failure, without a shutdown event. The threads must end, and must have ended
 * before the container destroys an application-scoped resource they would use (the application's
 * {@code DataSource}, in a real application). The threads are read from a JVM thread dump, which
 * lists virtual threads too.
 */
class QuarkusRefusedStartTest {

  private static final Duration SETTLE = Duration.ofSeconds(10);

  static volatile SmallRyeConfig applicationConfig;

  /** The configuration, the runners the framework would build, and what they read. */
  public static class Infrastructure {
    private final SagaStore sagaStore = new InMemorySagaStore();

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

    @Produces
    @Singleton
    public SagaStore sagaStore() {
      return sagaStore;
    }

    @Produces
    @Singleton
    @Named("orderSagaRunner")
    public SagaRunner<FakeState> sagaRunner() {
      return SagaRunner.<FakeState>builder()
          .orchestrator(new FakeOrchestrator())
          .sagaStore(sagaStore)
          .commandBus(SagaEventPathRetentionContainerWiringTest.NOOP_BUS)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
          .forwardFence(false)
          .build();
    }

    @Produces
    @Singleton
    @Named(StreamRuneProducers.FRAMEWORK_INBOX_RETENTION_SWEEPER)
    public InboxRetentionSweeper inboxRetentionSweeper() {
      return new InboxRetentionSweeper(
          new InMemoryCommandInbox(),
          Duration.ofDays(7),
          Duration.ofHours(1),
          Clock.systemUTC(),
          StreamRuneMetrics.NOOP);
    }

    @Produces
    @Singleton
    @Named(StreamRuneProducers.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER)
    public SagaDeadLetterRetentionSweeper sagaDeadLetterRetentionSweeper() {
      return new SagaDeadLetterRetentionSweeper(
          new InMemorySagaDeadLetterStore(sagaStore),
          Duration.ofDays(3),
          Duration.ofHours(1),
          Clock.systemUTC(),
          StreamRuneMetrics.NOOP);
    }

    @Produces
    @Singleton
    public EventStore eventStore() {
      return new InMemoryEventStore(SimpleEventTypeRegistry.builder().build());
    }

    @Produces
    @Singleton
    public SseEventPublisher sseEventPublisher() {
      return new SseEventPublisher();
    }

    @Produces
    @Singleton
    public AggregateLocker locker() {
      return new LocalStripedLocker(64);
    }
  }

  /** An application observer at the default priority that refuses the start. */
  public static class ApplicationRefusal {
    public void onStart(@Observes StartupEvent event) {
      throw new IllegalStateException("the application refuses the start");
    }
  }

  /** A framework runner whose producer fails, after the lifecycle started the runners before it. */
  public static class FailingDeadLetterRetryRunner {
    @Produces
    @Singleton
    @Named(StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER)
    public DeadLetterRetryRunner deadLetterRetryRunner() {
      throw new IllegalStateException("the dead-letter retry runner cannot be built");
    }
  }

  /**
   * An application-scoped resource the runners would use, as the application's Agroal {@code
   * DataSource} is: it records the virtual threads still alive when the container destroys it.
   */
  @ApplicationScoped
  public static class ApplicationResource {
    static volatile Map<Long, String> aliveWhenDestroyed;

    public void onStart(
        @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event) {
      // Observing the event creates the bean, as a resource the application uses is created.
    }

    @PreDestroy
    public void destroy() throws IOException {
      aliveWhenDestroyed = virtualThreads();
    }
  }

  private static SmallRyeConfig config(Map<String, String> values) {
    var all = new HashMap<>(values);
    all.put("streamrune.sse.enabled", "true");
    return new SmallRyeConfigBuilder()
        .addDefaultInterceptors()
        .withMapping(StreamRuneQuarkusProperties.class)
        .withConverter(
            Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
        .withDefaultValues(all)
        .build();
  }

  private static List<Class<?>> beans(Class<?>... extra) {
    var beans =
        new ArrayList<Class<?>>(
            List.of(
                StreamRuneLifecycle.class,
                SagaCompensationRetryLifecycle.class,
                SseEventFeedLifecycle.class,
                SagaParallelismStartupCheck.class,
                Infrastructure.class,
                ApplicationResource.class));
    beans.addAll(List.of(extra));
    return beans;
  }

  @Test
  void aStartTheSagaParallelismCheckRefusesStopsEveryFrameworkThread() throws Exception {
    // A per-subscription property naming a subscription no saga runner is bound to: the check,
    // which observes after the runners started, refuses the start.
    applicationConfig = config(Map.of("streamrune.saga.subscriptions.nobody.parallelism", "2"));

    assertTheRefusedStartLeavesNoThread(beans(), "nobody", ALL_FRAMEWORK_THREADS);
  }

  @Test
  void aStartAnApplicationObserverRefusesStopsEveryFrameworkThread() throws Exception {
    applicationConfig = config(Map.of());

    assertTheRefusedStartLeavesNoThread(
        beans(ApplicationRefusal.class),
        "the application refuses the start",
        ALL_FRAMEWORK_THREADS);
  }

  @Test
  void aRunnerThatFailsMidStartLeavesNoneOfTheRunnersStartedBeforeIt() throws Exception {
    applicationConfig = config(Map.of());

    // The lifecycle starts the retention sweepers before the dead-letter retry runner, whose
    // producer fails. Whether the compensation-retry sweepers and the SSE feed started depends on
    // the order CDI notifies observers of one priority in, so only the sweepers are required.
    assertTheRefusedStartLeavesNoThread(
        beans(FailingDeadLetterRetryRunner.class),
        "the dead-letter retry runner cannot be built",
        List.of(
            RelayComponentNames.INBOX_RETENTION_SWEEPER,
            RelayComponentNames.SAGA_DEAD_LETTER_RETENTION_SWEEPER));
  }

  @Test
  void theShutdownEventStopsTheThreadsAndTheContainerStopsNothingTwice() throws Exception {
    applicationConfig = config(Map.of());
    var log = new CapturedLog();
    try {
      Map<Long, String> before = virtualThreads();
      Map<Long, String> started;
      try (var arc = RealArcTestContainer.boot(beans())) {
        var events = arc.container().beanManager().getEvent();
        events.select(StartupEvent.class).fire(new StartupEvent());
        started = frameworkThreadsSince(before, ALL_FRAMEWORK_THREADS);

        events.select(ShutdownEvent.class).fire(new ShutdownEvent());
        awaitEnded(started);
      }
      assertThat(alive(started)).isEmpty();
      assertThat(log.messages())
          .containsOnlyOnce(
              "StreamRune: stopped InboxRetentionSweeper",
              "StreamRune: stopped SagaDeadLetterRetentionSweeper",
              "StreamRune: stopped SseEventFeed");
    } finally {
      log.close();
    }
  }

  /** The framework threads the fixture starts, by name. */
  private static final List<String> ALL_FRAMEWORK_THREADS =
      List.of(
          RelayComponentNames.INBOX_RETENTION_SWEEPER,
          RelayComponentNames.SAGA_DEAD_LETTER_RETENTION_SWEEPER,
          "saga-compensation-retry-" + SagaType.fromClass(FakeState.class).value(),
          "streamrune-poll-" + SseEventFeed.SUBSCRIPTION_NAME.value());

  /**
   * Boots {@code beans}, fires the startup event, which must fail with {@code refusal}, then shuts
   * the container down without a shutdown event, as Quarkus does after a failed start: every
   * framework thread the start left running must end, and must have ended before the container
   * destroys an application-scoped resource the threads would use.
   */
  private static void assertTheRefusedStartLeavesNoThread(
      List<Class<?>> beans, String refusal, List<String> required) throws Exception {
    ApplicationResource.aliveWhenDestroyed = null;
    Map<Long, String> before = virtualThreads();
    Map<Long, String> started;
    try (var arc = RealArcTestContainer.boot(beans)) {
      assertThatThrownBy(
              () ->
                  arc.container()
                      .beanManager()
                      .getEvent()
                      .select(StartupEvent.class)
                      .fire(new StartupEvent()))
          .hasStackTraceContaining(refusal);
      started = frameworkThreadsSince(before, ALL_FRAMEWORK_THREADS);
      // The refusal came after the framework started these threads: the test sees them.
      assertThat(started.values()).containsAll(required);
    }
    awaitEnded(started);
    assertThat(ApplicationResource.aliveWhenDestroyed)
        .as("the virtual threads alive when the application's resource was destroyed")
        .isNotNull()
        .doesNotContainKeys(started.keySet().toArray(Long[]::new));
  }

  /** Waits until none of {@code threads} is alive, and fails naming those that still are. */
  private static void awaitEnded(Map<Long, String> threads) throws Exception {
    long deadline = System.nanoTime() + SETTLE.toNanos();
    Map<Long, String> alive = alive(threads);
    while (!alive.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(50);
      alive = alive(threads);
    }
    assertThat(alive.values()).as("framework threads still running").isEmpty();
  }

  private static Map<Long, String> alive(Map<Long, String> threads) throws IOException {
    Map<Long, String> now = virtualThreads();
    Map<Long, String> alive = new HashMap<>();
    threads.forEach(
        (tid, name) -> {
          if (now.containsKey(tid)) {
            alive.put(tid, name);
          }
        });
    return alive;
  }

  /**
   * The virtual threads alive now that were not alive {@code before} and carry one of {@code
   * names}: the threads this test's container started, whatever else the test JVM runs.
   */
  private static Map<Long, String> frameworkThreadsSince(
      Map<Long, String> before, List<String> names) throws IOException {
    Map<Long, String> started = new HashMap<>(virtualThreads());
    started.keySet().removeAll(before.keySet());
    started.values().retainAll(names);
    return started;
  }

  /** The messages the module's lifecycle beans log while it is open. */
  private static final class CapturedLog {
    private final java.util.logging.Logger logger =
        java.util.logging.Logger.getLogger("org.streamrune.quarkus");
    private final List<String> messages = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.logging.Handler handler =
        new java.util.logging.Handler() {
          @Override
          public void publish(java.util.logging.LogRecord logRecord) {
            messages.add(new java.util.logging.SimpleFormatter().formatMessage(logRecord));
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    CapturedLog() {
      logger.addHandler(handler);
    }

    List<String> messages() {
      return List.copyOf(messages);
    }

    void close() {
      logger.removeHandler(handler);
    }
  }

  /** The virtual threads alive now, by thread id, from a JVM thread dump. */
  private static Map<Long, String> virtualThreads() throws IOException {
    Path file = Files.createTempFile("streamrune-threads", ".json");
    JsonNode dump;
    try {
      // The dump refuses to overwrite a file.
      Files.delete(file);
      ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
          .dumpThreads(file.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.JSON);
      dump = new ObjectMapper().readTree(file.toFile());
    } finally {
      Files.deleteIfExists(file);
    }
    Map<Long, String> threads = new HashMap<>();
    for (JsonNode container : dump.path("threadDump").path("threadContainers")) {
      for (JsonNode thread : container.path("threads")) {
        if (thread.path("virtual").asBoolean(false)) {
          threads.put(thread.path("tid").asLong(), thread.path("name").asText());
        }
      }
    }
    return threads;
  }
}
