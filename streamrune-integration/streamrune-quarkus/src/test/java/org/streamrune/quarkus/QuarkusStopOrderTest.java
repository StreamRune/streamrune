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
 * The framework stops after the application: an application bean that stops a subscription on the
 * shutdown event, or on the before-destroyed event after a refused start, still holds its lease and
 * still has an open command bus while it stops, and the framework's other runner owners stop before
 * {@link StreamRuneLifecycle} closes the leadership they use.
 *
 * <p>Each test boots a real Arc container with the framework's three runner owners, a leadership
 * bean and an application bean that takes a lease at startup. The application bean observes each
 * stop event twice: at the default priority, and at {@code Interceptor.Priority.LIBRARY_AFTER +
 * 300}, later than the default, which proves the order whatever CDI does with observers of one
 * priority. It records whether its lease is still held and the bus still open each time.
 */
class QuarkusStopOrderTest {

  private static final String HELD_AND_OPEN = "lease held, bus open";

  /** What the application bean saw each time it stopped, in order. */
  static final List<String> APPLICATION_STOPS = new CopyOnWriteArrayList<>();

  /** What was still running when the framework closed the leadership. */
  static final List<String> RUNNING_AT_LEADERSHIP_CLOSE = new CopyOnWriteArrayList<>();

  /** The framework threads, other than {@link StreamRuneLifecycle}'s, that must stop before it. */
  static volatile Map<Long, String> otherOwnersThreads = Map.of();

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

    public void stop(@Observes ShutdownEvent event) {
      record("shutdown");
    }

    public void stopLate(
        @Observes @Priority(Interceptor.Priority.LIBRARY_AFTER + 300) ShutdownEvent event) {
      record("shutdown, late");
    }

    public void stopBeforeDestroy(
        @Observes(notifyObserver = Reception.IF_EXISTS) @BeforeDestroyed(ApplicationScoped.class)
            Object event) {
      record("before-destroyed");
    }

    public void stopBeforeDestroyLate(
        @Observes(notifyObserver = Reception.IF_EXISTS)
            @Priority(Interceptor.Priority.LIBRARY_AFTER + 300)
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
              + (bus.isClosed() ? "closed" : "open"));
    }
  }

  /**
   * The shared leadership, which grants every lease. Closing it releases them; it first records
   * which threads of the framework's other runner owners are still alive.
   */
  public static class Leadership {
    @Produces
    @Singleton
    public SubscriptionLeadership leadership() {
      return new ScriptedSubscriptionLeadership() {
        {
          grant();
        }

        @Override
        public void close() {
          RUNNING_AT_LEADERSHIP_CLOSE.addAll(alive(otherOwnersThreads).values());
          super.close();
        }
      };
    }
  }

  @BeforeEach
  void reset() {
    APPLICATION_STOPS.clear();
    RUNNING_AT_LEADERSHIP_CLOSE.clear();
    otherOwnersThreads = Map.of();
  }

  @Test
  void onTheShutdownEventTheApplicationStopsWhileItsLeaseAndTheBusAreOpen() throws Exception {
    QuarkusRefusedStartTest.applicationConfig = QuarkusRefusedStartTest.config(Map.of());
    Map<Long, String> before = QuarkusRefusedStartTest.virtualThreads();
    try (var arc = RealArcTestContainer.boot(beans())) {
      var events = arc.container().beanManager().getEvent();
      events.select(StartupEvent.class).fire(new StartupEvent());
      watchTheOtherOwnersThreadsSince(before);

      events.select(ShutdownEvent.class).fire(new ShutdownEvent());

      assertThat(APPLICATION_STOPS)
          .containsExactly("shutdown: " + HELD_AND_OPEN, "shutdown, late: " + HELD_AND_OPEN);
    }
    assertThat(RUNNING_AT_LEADERSHIP_CLOSE)
        .as("threads of the framework's other runner owners when the leadership closed")
        .isEmpty();
  }

  @Test
  void afterARefusedStartTheApplicationStopsWhileItsLeaseAndTheBusAreOpen() throws Exception {
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
      watchTheOtherOwnersThreadsSince(before);
    }

    assertThat(APPLICATION_STOPS)
        .containsExactly(
            "before-destroyed: " + HELD_AND_OPEN, "before-destroyed, late: " + HELD_AND_OPEN);
    assertThat(RUNNING_AT_LEADERSHIP_CLOSE)
        .as("threads of the framework's other runner owners when the leadership closed")
        .isEmpty();
  }

  /**
   * Records the threads of the compensation-retry sweeper and of the SSE feed the start left
   * running, which must have ended when the leadership closes.
   */
  private static void watchTheOtherOwnersThreadsSince(Map<Long, String> before) throws IOException {
    Map<Long, String> started =
        QuarkusRefusedStartTest.frameworkThreadsSince(
            before, QuarkusRefusedStartTest.ALL_FRAMEWORK_THREADS);
    started
        .values()
        .removeAll(
            List.of(
                RelayComponentNames.INBOX_RETENTION_SWEEPER,
                RelayComponentNames.SAGA_DEAD_LETTER_RETENTION_SWEEPER));
    // Both owners started a thread: the check below has something to see.
    assertThat(started).hasSize(2);
    otherOwnersThreads = started;
  }

  private static Map<Long, String> alive(Map<Long, String> threads) {
    try {
      return QuarkusRefusedStartTest.alive(threads);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<Class<?>> beans() {
    return QuarkusRefusedStartTest.beans(ApplicationSubscription.class, Leadership.class);
  }
}
