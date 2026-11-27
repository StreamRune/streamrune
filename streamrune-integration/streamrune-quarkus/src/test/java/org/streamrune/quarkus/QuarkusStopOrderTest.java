package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.interceptor.Interceptor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.health.RelayComponentNames;
import org.streamrune.test.ScriptedSubscriptionLeadership;

/**
 * The framework stops around the application: an application bean that stops a subscription on the
 * shutdown event, or on the before-destroyed event after a refused start, still holds its lease and
 * still has an open command bus while it stops, and the compensation-retry sweeper, which follows
 * the lease of the saga's subscription, and the SSE feed have stopped before it.
 *
 * <p>Each test boots a real Arc container with the framework's three runner owners, a leadership
 * bean and an application bean that takes a lease at startup. The application bean observes each
 * stop event three times: one step after the framework's first step, at the default priority and
 * one step before the framework's second step, so the order is proved at both ends of the range the
 * framework leaves the application, whatever CDI does with observers of one priority. Each time it
 * records whether its lease is still held, whether the bus is still open and whether the threads of
 * the sweeper and of the feed have ended.
 */
class QuarkusStopOrderTest {

  private static final String EXPECTED_STATE = "lease held, bus open, sweeper and feed stopped";

  /** What the application bean saw each time it stopped, in order. */
  static final List<String> APPLICATION_STOPS = new CopyOnWriteArrayList<>();

  /** The threads of the compensation-retry sweeper and of the SSE feed the start left running. */
  static volatile Map<Long, String> sweeperAndFeedThreads = Map.of();

  /**
   * The application's subscription, reduced to what the order is about: it takes a lease of the
   * shared leadership at startup and uses the command bus.
   */
  @ApplicationScoped
  public static class ApplicationSubscription {
    static final SubscriptionName NAME = SubscriptionName.of("application-subscription");

    private final SubscriptionLeadership leadership;
    private final VirtualThreadCommandBus bus;

    @Inject
    public ApplicationSubscription(SubscriptionLeadership leadership, VirtualThreadCommandBus bus) {
      this.leadership = leadership;
      this.bus = bus;
    }

    /** Client-proxy constructor: never used for a real instance. */
    protected ApplicationSubscription() {
      this.leadership = null;
      this.bus = null;
    }

    public void start(@Observes @Priority(Interceptor.Priority.APPLICATION) StartupEvent event) {
      assertThat(leadership.tryAcquire(NAME)).isPresent();
    }

    public void stopEarly(
        @Observes @Priority(StreamRuneLifecycle.STOP_BEFORE_APPLICATION_PRIORITY + 1)
            ShutdownEvent event) {
      record("shutdown, early");
    }

    public void stop(@Observes ShutdownEvent event) {
      record("shutdown");
    }

    public void stopLate(
        @Observes @Priority(StreamRuneLifecycle.STOP_AFTER_APPLICATION_PRIORITY - 1)
            ShutdownEvent event) {
      record("shutdown, late");
    }

    public void stopBeforeDestroyEarly(
        @Observes(notifyObserver = Reception.IF_EXISTS)
            @Priority(StreamRuneLifecycle.STOP_BEFORE_APPLICATION_PRIORITY + 1)
            @BeforeDestroyed(ApplicationScoped.class)
            Object event) {
      record("before-destroyed, early");
    }

    public void stopBeforeDestroy(
        @Observes(notifyObserver = Reception.IF_EXISTS) @BeforeDestroyed(ApplicationScoped.class)
            Object event) {
      record("before-destroyed");
    }

    public void stopBeforeDestroyLate(
        @Observes(notifyObserver = Reception.IF_EXISTS)
            @Priority(StreamRuneLifecycle.STOP_AFTER_APPLICATION_PRIORITY - 1)
            @BeforeDestroyed(ApplicationScoped.class)
            Object event) {
      record("before-destroyed, late");
    }

    private void record(String event) {
      APPLICATION_STOPS.add(
          event
              + ": lease "
              + (leadership.current(NAME).isPresent() ? "held" : "lost")
              + ", bus "
              + (bus.isClosed() ? "closed" : "open")
              + ", sweeper and feed "
              + (alive(sweeperAndFeedThreads).isEmpty() ? "stopped" : "running"));
    }
  }

  /** An application observer of the shutdown event that throws, which ends the notification. */
  public static class ThrowingShutdownObserver {
    public void onShutdown(@Observes ShutdownEvent event) {
      throw new IllegalStateException("the application's shutdown observer fails");
    }
  }

  /** The shared leadership, which grants every lease; closing it releases them. */
  public static class Leadership {
    @Produces
    @Singleton
    public SubscriptionLeadership leadership() {
      return new ScriptedSubscriptionLeadership().grant();
    }
  }

  @BeforeEach
  void reset() {
    APPLICATION_STOPS.clear();
    sweeperAndFeedThreads = Map.of();
    QuarkusRefusedStartTest.ApplicationResource.aliveWhenDestroyed = null;
  }

  @Test
  void onTheShutdownEventTheApplicationStopsBetweenTheFrameworksTwoSteps() throws Exception {
    QuarkusRefusedStartTest.applicationConfig = QuarkusRefusedStartTest.config(Map.of());
    Map<Long, String> before = QuarkusRefusedStartTest.virtualThreads();
    try (var arc = RealArcTestContainer.boot(beans())) {
      var events = arc.container().beanManager().getEvent();
      events.select(StartupEvent.class).fire(new StartupEvent());
      watchTheSweeperAndTheFeedSince(before);

      events.select(ShutdownEvent.class).fire(new ShutdownEvent());

      assertThat(APPLICATION_STOPS)
          .containsExactly(
              "shutdown, early: " + EXPECTED_STATE,
              "shutdown: " + EXPECTED_STATE,
              "shutdown, late: " + EXPECTED_STATE);
    }
  }

  @Test
  void afterARefusedStartTheApplicationStopsBetweenTheFrameworksTwoSteps() throws Exception {
    // The saga parallelism check refuses a property naming a subscription no saga runner is bound
    // to, after every runner started.
    QuarkusRefusedStartTest.applicationConfig =
        QuarkusRefusedStartTest.config(
            Map.of("streamrune.saga.subscriptions.nobody.parallelism", "2"));
    Map<Long, String> before = QuarkusRefusedStartTest.virtualThreads();
    try (var arc = RealArcTestContainer.boot(beans())) {
      assertThatThrownBy(
              () ->
                  arc.container()
                      .beanManager()
                      .getEvent()
                      .select(StartupEvent.class)
                      .fire(new StartupEvent()))
          .hasStackTraceContaining("nobody");
      watchTheSweeperAndTheFeedSince(before);
    }

    assertThat(APPLICATION_STOPS)
        .containsExactly(
            "before-destroyed, early: " + EXPECTED_STATE,
            "before-destroyed: " + EXPECTED_STATE,
            "before-destroyed, late: " + EXPECTED_STATE);
  }

  @Test
  void anApplicationShutdownObserverThatThrowsLeavesTheStopToTheContainersShutdown()
      throws Exception {
    QuarkusRefusedStartTest.applicationConfig = QuarkusRefusedStartTest.config(Map.of());
    Map<Long, String> before = QuarkusRefusedStartTest.virtualThreads();
    Map<Long, String> started;
    try (var arc = RealArcTestContainer.boot(beans(ThrowingShutdownObserver.class))) {
      var events = arc.container().beanManager().getEvent();
      events.select(StartupEvent.class).fire(new StartupEvent());
      started =
          QuarkusRefusedStartTest.frameworkThreadsSince(
              before, QuarkusRefusedStartTest.ALL_FRAMEWORK_THREADS);
      watchTheSweeperAndTheFeedSince(before);

      // The observer that throws ends the notification: neither the application's later observers
      // nor StreamRuneLifecycle hear the event.
      assertThatThrownBy(() -> events.select(ShutdownEvent.class).fire(new ShutdownEvent()))
          .hasStackTraceContaining("the application's shutdown observer fails");
    }

    // The container's shutdown stops what the shutdown event left running, in the same order.
    assertThat(APPLICATION_STOPS.stream().filter(stop -> stop.startsWith("before-destroyed")))
        .containsExactly(
            "before-destroyed, early: " + EXPECTED_STATE,
            "before-destroyed: " + EXPECTED_STATE,
            "before-destroyed, late: " + EXPECTED_STATE);
    assertThat(QuarkusRefusedStartTest.ApplicationResource.aliveWhenDestroyed)
        .as("the virtual threads alive when the application's resource was destroyed")
        .isNotNull()
        .doesNotContainKeys(started.keySet().toArray(Long[]::new));
  }

  /**
   * Records the threads of the compensation-retry sweeper and of the SSE feed the start left
   * running, which must have ended when the application's first stop observer runs.
   */
  private static void watchTheSweeperAndTheFeedSince(Map<Long, String> before) throws IOException {
    Map<Long, String> started =
        QuarkusRefusedStartTest.frameworkThreadsSince(
            before, QuarkusRefusedStartTest.ALL_FRAMEWORK_THREADS);
    started
        .values()
        .removeAll(
            List.of(
                RelayComponentNames.INBOX_RETENTION_SWEEPER,
                RelayComponentNames.SAGA_DEAD_LETTER_RETENTION_SWEEPER));
    // Both owners started a thread: the check has something to see.
    assertThat(started).hasSize(2);
    sweeperAndFeedThreads = started;
  }

  private static Map<Long, String> alive(Map<Long, String> threads) {
    try {
      return QuarkusRefusedStartTest.alive(threads);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<Class<?>> beans(Class<?>... extra) {
    var beans = new java.util.ArrayList<Class<?>>(List.of(extra));
    beans.add(ApplicationSubscription.class);
    beans.add(Leadership.class);
    return QuarkusRefusedStartTest.beans(beans.toArray(Class<?>[]::new));
  }
}
