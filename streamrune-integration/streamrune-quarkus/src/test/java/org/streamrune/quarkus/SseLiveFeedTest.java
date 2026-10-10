package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.streamrune.runtime.DeciderRegistration;
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

      client.awaitItems(1, Duration.ofSeconds(10));
      assertThat(client.getItems())
          .as("only the events of order/o-1 stored after the start are emitted to its subscriber")
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
    Map<String, String> overrides = new HashMap<>();
    overrides.put("streamrune.sse.enabled", String.valueOf(sseEnabledAtRuntime));
    overrides.put("streamrune.sse.polling-interval", "PT0.05S");
    return TestProperties.of(overrides);
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
