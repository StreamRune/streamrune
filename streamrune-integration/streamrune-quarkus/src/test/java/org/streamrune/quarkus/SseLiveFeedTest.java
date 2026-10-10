package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.ws.rs.sse.OutboundSseEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.SseEventFeed;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;

/**
 * With {@code streamrune.sse.enabled=true} the Server-Sent Events endpoint is fed by the
 * integration: a command executed through the bus reaches a subscriber of the event's stream with
 * no application code publishing it.
 *
 * <p>Boots a real Arc container with the producers and the feed lifecycle, fires the Quarkus
 * startup event, opens {@code /api/sse/order/o-1} through the resource method over the container's
 * publisher, and executes commands through the container's command bus.
 */
class SseLiveFeedTest {

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final SseAuthorizer ALLOW_ALL = (principal, streamId) -> true;
  private static final Map<String, String> SSE_BUILT_IN = Map.of("streamrune.sse.enabled", "true");

  @Test
  void aCommandExecutedThroughTheBusReachesTheSubscriberOfItsStreamAndNoOtherStreamsEventDoes()
      throws Exception {
    try (var arc = RealArcTestContainer.boot(beans(SseEnabledInfrastructure.class), SSE_BUILT_IN)) {
      startUp(arc);
      SseEventFeedLifecycle feed = arc.container().instance(SseEventFeedLifecycle.class).get();
      assertThat(feed.isRunning()).as("the feed starts with the application").isTrue();

      SseEventPublisher publisher = arc.container().instance(SseEventPublisher.class).get();
      RequestIdentityPolicy identity = arc.container().instance(RequestIdentityPolicy.class).get();
      var controller = new SseController(publisher, ALLOW_ALL, identity);
      var client = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
      controller.stream("order", "o-1", List.of()).subscribe(client);
      assertThat(controller.openStreamCount()).isEqualTo(1);

      CommandBus bus = arc.container().instance(VirtualThreadCommandBus.class).get();
      // The other stream's event is stored first: were it routed to this subscriber, it would be
      // emitted before the frame the test waits for.
      bus.execute(new OrderCommand.Place("o-2", "placed-on-o-2"));
      bus.execute(new OrderCommand.Place("o-1", "placed-on-o-1"));

      // The opening comment frame, then the event.
      client.awaitItems(2, Duration.ofSeconds(10));
      assertThat(client.getItems())
          .as("only the events of order/o-1 stored after the start are emitted to its subscriber")
          .filteredOn(frame -> frame.getData() != null)
          .extracting(OutboundSseEvent::getData)
          .containsExactly(new OrderEvent.Placed("placed-on-o-1"));

      shutDown(arc);
      assertThat(feed.isRunning()).as("the feed stops with the application").isFalse();
      controller.completeOpenStreams(mock(ShutdownEvent.class));
      client.awaitCompletion(Duration.ofSeconds(5));
      assertThat(controller.openStreamCount()).as("no stream is left open").isZero();
    }
  }

  @Test
  void theFeedDoesNotStartWhenTheEndpointIsSwitchedOffAtRuntime() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(beans(SseDisabledAtRuntimeInfrastructure.class), SSE_BUILT_IN)) {
      startUp(arc);

      assertThat(arc.container().instance(SseEventFeedLifecycle.class).get().isRunning())
          .as("a disabled endpoint answers 404, so nothing reads the global stream for it")
          .isFalse();
    }
  }

  @Test
  void theFeedIsNotPartOfAnApplicationBuiltWithoutTheEndpoint() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            beans(SseEnabledInfrastructure.class), Map.of("streamrune.sse.enabled", "false"))) {
      assertThat(arc.container().instance(SseEventFeedLifecycle.class).isAvailable()).isFalse();
    }
  }

  /** The feed's polling thread is watched like the framework's other pollers. */
  @Test
  void theStartedFeedIsReportedByTheRelayHealthContributor() throws Exception {
    try (var arc = RealArcTestContainer.boot(beans(SseEnabledInfrastructure.class), SSE_BUILT_IN)) {
      BackgroundRelayHealthContributor relayHealth =
          arc.container().instance(BackgroundRelayHealthContributor.class).get();
      assertThat(relayHealth.components())
          .as("nothing is registered before the feed is started")
          .noneMatch(component -> component.name().equals("sse-event-feed"));

      startUp(arc);

      assertThat(relayHealth.components())
          .filteredOn(component -> component.name().equals("sse-event-feed"))
          .singleElement()
          .satisfies(
              feed -> {
                assertThat(feed.started()).isTrue();
                assertThat(feed.alive()).isTrue();
                assertThat(feed.status()).isEqualTo(BackgroundRelayHealthContributor.Status.UP);
              });
      shutDown(arc);
    }
  }

  /**
   * A polling interval below one millisecond would be no pause between two reads of the global
   * stream. The start-up event's observer throws, which fails the start of a Quarkus application,
   * and the failure names the property.
   */
  @Test
  void aPollingIntervalBelowOneMillisecondFailsTheStartAndNamesTheProperty() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            beans(SubMillisecondIntervalInfrastructure.class), SSE_BUILT_IN)) {
      assertThatThrownBy(() -> startUp(arc))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("streamrune.sse.polling-interval")
          .hasMessageContaining("at least 1ms");
      assertThat(arc.container().instance(SseEventFeedLifecycle.class).get().isRunning()).isFalse();
    }
  }

  /**
   * An application that declares its own feed owns it: the framework creates and starts none, and
   * does not start the application's.
   */
  @Test
  void anApplicationsOwnFeedIsLeftToTheApplication() throws Exception {
    List<Class<?>> beans = new java.util.ArrayList<>(beans(SseEnabledInfrastructure.class));
    beans.add(ApplicationFeedConfig.class);
    try (var arc = RealArcTestContainer.boot(beans, SSE_BUILT_IN)) {
      startUp(arc);

      assertThat(arc.container().instance(SseEventFeedLifecycle.class).get().isRunning())
          .as("the framework starts no feed beside the application's")
          .isFalse();
      SseEventFeed applicationFeed = arc.container().instance(SseEventFeed.class).get();
      assertThat(applicationFeed.isStarted())
          .as("the framework does not start a feed it did not create")
          .isFalse();
      assertThat(
              arc.container().instance(BackgroundRelayHealthContributor.class).get().components())
          .noneMatch(component -> component.name().equals("sse-event-feed"));
      shutDown(arc);
    }
  }

  // ---- harness ----------------------------------------------------------------------------------

  private static List<Class<?>> beans(Class<?> infrastructure) {
    return List.of(
        StreamRuneProducers.class,
        SseEventFeedLifecycle.class,
        infrastructure,
        EventStoreConfig.class,
        AuditStoreConfig.class,
        OrderRegistrationConfig.class);
  }

  /** A real, synchronous {@code Event#fire} of the Quarkus startup event. */
  private static void startUp(RealArcTestContainer arc) {
    arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
  }

  /** A real, synchronous {@code Event#fire} of the Quarkus shutdown event. */
  private static void shutDown(RealArcTestContainer arc) {
    arc.container()
        .beanManager()
        .getEvent()
        .select(ShutdownEvent.class)
        .fire(mock(ShutdownEvent.class));
  }

  // ---- the domain -------------------------------------------------------------------------------

  sealed interface OrderCommand extends Command permits OrderCommand.Place {
    record Place(String orderId, String note) implements OrderCommand {}
  }

  record OrderState() implements AggregateState {}

  sealed interface OrderEvent extends DomainEvent permits OrderEvent.Placed {
    record Placed(String note) implements OrderEvent {}
  }

  static final class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {
    @Override
    public OrderState initialState() {
      return new OrderState();
    }

    @Override
    public List<OrderEvent> decide(OrderCommand command, OrderState state) {
      return switch (command) {
        case OrderCommand.Place place -> List.of(new OrderEvent.Placed(place.note()));
      };
    }

    @Override
    public OrderState evolve(OrderState state, OrderEvent event) {
      return state;
    }
  }

  // ---- beans ------------------------------------------------------------------------------------

  private static StreamRuneQuarkusProperties properties(boolean sseEnabledAtRuntime) {
    return properties(sseEnabledAtRuntime, "PT0.05S");
  }

  private static StreamRuneQuarkusProperties properties(
      boolean sseEnabledAtRuntime, String pollingInterval) {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("streamrune.sse.enabled", String.valueOf(sseEnabledAtRuntime));
    overrides.put("streamrune.sse.polling-interval", pollingInterval);
    return TestProperties.of(overrides);
  }

  @ApplicationScoped
  public static class SubMillisecondIntervalInfrastructure {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return SseLiveFeedTest.properties(true, "PT0.0005S");
    }
  }

  /** The application's own feed, which the application would start and stop. */
  @ApplicationScoped
  public static class ApplicationFeedConfig {
    @Produces
    @Singleton
    public SseEventFeed applicationFeed(EventStore eventStore, SseEventPublisher publisher) {
      return new SseEventFeed(eventStore, publisher, Duration.ofSeconds(1));
    }
  }

  @ApplicationScoped
  public static class SseEnabledInfrastructure {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return SseLiveFeedTest.properties(true);
    }
  }

  @ApplicationScoped
  public static class SseDisabledAtRuntimeInfrastructure {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return SseLiveFeedTest.properties(false);
    }
  }

  /** An in-memory store that holds one event of {@code order/o-1} before the application starts. */
  @ApplicationScoped
  public static class EventStoreConfig {
    @Produces
    @Singleton
    public EventStore eventStore() {
      InMemoryEventStore store = new InMemoryEventStore();
      StreamId stream = StreamId.of(ORDER, AggregateId.of("o-1"));
      store.append(
          stream,
          List.of(
              new EventEnvelope(
                  GlobalOffset.of(1),
                  stream,
                  new Version(1),
                  new EventType("Placed"),
                  new OrderEvent.Placed("stored-before-the-start"),
                  new EventMetadata(
                      EventId.of("evt-history"),
                      CommandId.of("cmd-history"),
                      null,
                      null,
                      CorrelationId.of("corr-history"),
                      null,
                      null,
                      Instant.now()))),
          Version.initial());
      return store;
    }
  }

  /** Keeps the audit interceptor off the mocked {@code DataSource}. */
  @ApplicationScoped
  public static class AuditStoreConfig {
    @Produces
    @Singleton
    public AuditStore auditStore() {
      return entry -> {};
    }
  }

  @ApplicationScoped
  public static class OrderRegistrationConfig {
    @Produces
    @Singleton
    public DeciderRegistration<OrderCommand, OrderState, OrderEvent> orderRegistration() {
      return new DeciderRegistration<>(
          ORDER,
          OrderCommand.class,
          command ->
              switch (command) {
                case OrderCommand.Place place -> AggregateId.of(place.orderId());
              },
          new OrderDecider());
    }
  }
}
