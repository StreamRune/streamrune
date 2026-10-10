package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.NoDeciderException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.filesystem.FileSystemCryptoEngine;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.runtime.ContinuousProjectionRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.StreamRune;
import org.streamrune.test.InMemoryEventStore;

/**
 * Compiles and runs the code samples of {@code docs/QUICKSTART.md}, sections 3 to 10, so that a
 * sample which stops compiling or stops working fails the build. The code below is the code of the
 * guide; when one changes, change the other.
 *
 * <p>Two samples need a PostgreSQL server to run to the end (the event store of section 5 and the
 * crypto-aware store of section 9). They are compiled here and run as far as they go without one.
 */
class QuickstartSamplesTest {

  // ---- Section 3: domain events ----

  sealed interface OrderEvent extends DomainEvent
      permits OrderPlaced, OrderConfirmed, OrderCancelled {
    String orderId();
  }

  record OrderPlaced(String orderId, String productId, int quantity) implements OrderEvent {}

  record OrderConfirmed(String orderId) implements OrderEvent {}

  record OrderCancelled(String orderId, String reason) implements OrderEvent {}

  // ---- Section 4: commands, state and decider ----

  sealed interface OrderCommand extends Command permits PlaceOrder, ConfirmOrder, CancelOrder {
    String orderId();
  }

  record PlaceOrder(String orderId, String productId, int quantity) implements OrderCommand {}

  record ConfirmOrder(String orderId) implements OrderCommand {}

  record CancelOrder(String orderId, String reason) implements OrderCommand {}

  record OrderState(String orderId, boolean placed, boolean confirmed, boolean cancelled)
      implements AggregateState {
    static final OrderState EMPTY = new OrderState(null, false, false, false);
  }

  static final class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {

    @Override
    public OrderState initialState() {
      return OrderState.EMPTY;
    }

    @Override
    public List<OrderEvent> decide(OrderCommand command, OrderState state) {
      return switch (command) {
        case PlaceOrder cmd when !state.placed() ->
            List.of(new OrderPlaced(cmd.orderId(), cmd.productId(), cmd.quantity()));

        case ConfirmOrder cmd when state.placed() && !state.confirmed() ->
            List.of(new OrderConfirmed(cmd.orderId()));

        case CancelOrder cmd when state.placed() && !state.cancelled() ->
            List.of(new OrderCancelled(cmd.orderId(), cmd.reason()));

        default -> List.of(); // valid no-op
      };
    }

    @Override
    public OrderState evolve(OrderState state, OrderEvent event) {
      return switch (event) {
        case OrderPlaced e -> new OrderState(e.orderId(), true, false, false);
        case OrderConfirmed e -> new OrderState(state.orderId(), true, true, state.cancelled());
        case OrderCancelled e -> new OrderState(state.orderId(), true, state.confirmed(), true);
      };
    }
  }

  static final AggregateType ORDER = AggregateType.of("order");

  // ---- Section 5: the event type registry ----

  private static EventTypeRegistry section5TypeRegistry() {
    EventTypeRegistry typeRegistry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("OrderPlaced", OrderPlaced.class)
            .registerEvent("OrderConfirmed", OrderConfirmed.class)
            .registerEvent("OrderCancelled", OrderCancelled.class)
            .build();
    return typeRegistry;
  }

  /** Compiled only: {@code create(...)} and {@code initializeSchema()} open a connection. */
  @SuppressWarnings("unused")
  private static EventStore section5PostgresStore(DataSource dataSource) {
    EventTypeRegistry typeRegistry = section5TypeRegistry();
    new PostgresEventStoreFactory(dataSource, typeRegistry).initializeSchema();
    return org.streamrune.postgres.PostgresEventStore.create(
        dataSource, typeRegistry, /* cryptoEngine */ null, /* upcasters */ List.of());
  }

  // ---- Section 6: build and execute ----

  @Test
  void section6_oneRegistrationOfTheSealedRootHandlesEveryCommandItPermits() {
    InMemoryEventStore eventStore = new InMemoryEventStore();

    StreamRune streamRune =
        StreamRune.builder()
            .eventStore(eventStore)
            .register(
                ORDER, OrderCommand.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
            .build();

    CommandBus.CommandResult result =
        streamRune.execute(new PlaceOrder("order-42", "product-7", 3));
    StreamId stream = result.streamId();
    boolean noop = result.events().isEmpty();

    AggregateHistory history = eventStore.load(StreamId.of(ORDER, AggregateId.of("order-42")));

    assertThat(stream).hasToString("order:order-42");
    assertThat(noop).isFalse();
    assertThat(history.events()).hasSize(1);
    assertThat(history.version().value()).isEqualTo(1);

    // The same registration dispatches the other two variants.
    assertThat(List.<DomainEvent>copyOf(streamRune.execute(new ConfirmOrder("order-42")).events()))
        .containsExactly(new OrderConfirmed("order-42"));
    assertThat(
            List.<DomainEvent>copyOf(
                streamRune.execute(new CancelOrder("order-42", "changed my mind")).events()))
        .containsExactly(new OrderCancelled("order-42", "changed my mind"));
    // A second PlaceOrder for an order that exists is the decider's valid no-op.
    assertThat(streamRune.execute(new PlaceOrder("order-42", "product-9", 1)).events()).isEmpty();

    streamRune.close();
  }

  // ---- Section 7: a projection ----

  /** The guide's own read-model write target. */
  interface OrderSummaryRepository {
    void upsert(String orderId, String productId, int quantity);
  }

  private static StreamRune.Builder section7Builder(
      EventStore eventStore, DataSource dataSource, OrderSummaryRepository orderSummaryRepository) {
    Projection orderSummary =
        batch -> {
          for (EventEnvelope envelope : batch) {
            if (envelope.event() instanceof OrderPlaced e) {
              orderSummaryRepository.upsert(e.orderId(), e.productId(), e.quantity());
            }
          }
        };

    var offsetStore = new PostgresOffsetStore(dataSource); // tracks each projection's progress

    return StreamRune.builder()
        .eventStore(eventStore)
        .register(
            ORDER, OrderCommand.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
        .registerProjection(
            "order_summary", orderSummary, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
        .projectionRunnerFactory(
            (store, subscriptionConfig) ->
                ContinuousProjectionRunner.builder()
                    .eventStore(store)
                    .offsetStore(offsetStore)
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .subscriptionConfig(subscriptionConfig)
                    .build());
  }

  @Test
  void section7_aRegisteredProjectionBuildsWithItsRunnerFactory() {
    // Built, not started: the offset store of the sample reads PostgreSQL once the runner runs.
    StreamRune streamRune =
        section7Builder(
                new InMemoryEventStore(), mock(DataSource.class), (orderId, productId, qty) -> {})
            .build();

    streamRune.close();
  }

  @Test
  void section7_aRegisteredProjectionWithoutARunnerIsRefusedByBuild() {
    Projection orderSummary = batch -> {};

    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .register(
                ORDER, OrderCommand.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
            .registerProjection(
                "order_summary", orderSummary, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no projection runner is configured");
  }

  // ---- Section 8: Spring Boot ----

  @Configuration
  static class OrderConfiguration {

    @Bean
    EventTypeRegistry eventTypeRegistry() {
      return SimpleEventTypeRegistry.builder()
          .registerEvent("OrderPlaced", OrderPlaced.class)
          .registerEvent("OrderConfirmed", OrderConfirmed.class)
          .registerEvent("OrderCancelled", OrderCancelled.class)
          .registerState("OrderState", OrderState.class)
          .build();
    }

    @Bean
    DeciderRegistration<OrderCommand, OrderState, OrderEvent> orderRegistration() {
      return new DeciderRegistration<>(
          ORDER, OrderCommand.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider());
    }
  }

  /** The guide's controller, without the web annotations: the calls it makes on the bus. */
  static final class OrderController {
    private final CommandBus commandBus;

    OrderController(CommandBus commandBus) {
      this.commandBus = commandBus;
    }

    String placeOrder(PlaceOrder command) {
      CommandBus.CommandResult result = commandBus.execute(command);
      return result.streamId().toString();
    }
  }

  /**
   * The auto-configuration over a {@code DataSource} bean. The event store factory is replaced by
   * an in-memory one so the context starts without a PostgreSQL server; everything else is what the
   * guide's application gets.
   */
  private static ApplicationContextRunner springApplication() {
    return new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(EventStoreFactory.class, () -> InMemoryEventStore::new);
  }

  @Test
  void section8_theThreeBeansOfTheGuideMakeTheControllerWork() {
    springApplication()
        .withUserConfiguration(OrderConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var controller = new OrderController(ctx.getBean(CommandBus.class));

              assertThat(controller.placeOrder(new PlaceOrder("order-42", "product-7", 3)))
                  .isEqualTo("order:order-42");
              assertThat(ctx.getBean(EventTypeRegistry.class).registeredTypes())
                  .contains(
                      OrderPlaced.class,
                      OrderConfirmed.class,
                      OrderCancelled.class,
                      OrderState.class);
            });
  }

  @Test
  void section8_withoutADeciderRegistrationBeanTheFirstCommandFindsNoDecider() {
    springApplication()
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var controller = new OrderController(ctx.getBean(CommandBus.class));

              assertThatThrownBy(
                      () -> controller.placeOrder(new PlaceOrder("order-42", "product-7", 3)))
                  .isInstanceOf(NoDeciderException.class);
            });
  }

  @Test
  void section8_withoutADataSourceBeanTheContextDoesNotStart() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(OrderConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              // Nothing built the EventStore the command bus needs.
              assertThat(ctx.getStartupFailure())
                  .hasMessageContaining(
                      "No qualifying bean of type 'org.streamrune.core.EventStore'");
            });
  }

  // ---- Section 9: encryption at rest ----

  record CustomerRegistered(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements DomainEvent {}

  record RegisterCustomer(String customerId, String email) implements Command {}

  record CustomerState(String customerId) implements AggregateState {}

  static final class CustomerDecider
      implements Decider<RegisterCustomer, CustomerState, CustomerRegistered> {
    @Override
    public CustomerState initialState() {
      return new CustomerState(null);
    }

    @Override
    public List<CustomerRegistered> decide(RegisterCustomer command, CustomerState state) {
      return state.customerId() == null
          ? List.of(new CustomerRegistered(command.customerId(), command.email()))
          : List.of();
    }

    @Override
    public CustomerState evolve(CustomerState state, CustomerRegistered event) {
      return new CustomerState(event.customerId());
    }
  }

  static final AggregateType CUSTOMER = AggregateType.of("customer");

  @Test
  void section9_theCryptoSamplesConfigureTheBuilder(@TempDir Path keyDir) {
    DataSource dataSource = mock(DataSource.class);

    // "Usage"
    CryptoEngine crypto = FileSystemCryptoEngine.builder().keyDirectory(keyDir).build();

    StreamRune.Builder usage =
        StreamRune.builder()
            .eventStoreFactoryProvider(
                settings ->
                    new PostgresEventStoreFactory(dataSource, settings.typeRegistry())
                        .cryptoEngine(settings.cryptoEngine()))
            .cryptoEngine(crypto)
            .registerEventType("OrderPlaced", OrderPlaced.class)
            .register(
                ORDER,
                OrderCommand.class,
                cmd -> AggregateId.of(cmd.orderId()),
                new OrderDecider());

    // "Quick Start with Encryption"
    CryptoEngine cached =
        new CachedCryptoEngine.Builder()
            .delegate(FileSystemCryptoEngine.builder().keyDirectory(keyDir).build())
            .maximumSize(5_000)
            .expireAfterWrite(Duration.ofMinutes(10))
            .build();

    StreamRune.Builder quickStart =
        StreamRune.builder()
            .eventStoreFactoryProvider(
                settings ->
                    new PostgresEventStoreFactory(dataSource, settings.typeRegistry())
                        .cryptoEngine(settings.cryptoEngine()))
            .cryptoEngine(cached)
            .registerEventType("CustomerRegistered", CustomerRegistered.class)
            .register(
                CUSTOMER,
                RegisterCustomer.class,
                cmd -> AggregateId.of(cmd.customerId()),
                new CustomerDecider());

    // build() would open a connection to create the store, so the samples stop here.
    assertThat(usage).isNotNull();
    assertThat(quickStart).isNotNull();
  }

  @Test
  void section9_aPreBuiltStoreCombinedWithACryptoEngineIsRefusedByBuild(@TempDir Path keyDir) {
    CryptoEngine crypto = FileSystemCryptoEngine.builder().keyDirectory(keyDir).build();

    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .cryptoEngine(crypto)
            .register(
                ORDER,
                OrderCommand.class,
                cmd -> AggregateId.of(cmd.orderId()),
                new OrderDecider());

    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
  }

  // ---- Section 10: native image hints ----

  static final class MyAppNativeHints implements RuntimeHintsRegistrar {
    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
      // Option A — scan your domain packages:
      StreamRuneRuntimeHints.registerDomainPackages(hints, classLoader, "com.example.shop.domain");

      // Option B — list the types explicitly:
      StreamRuneRuntimeHints.registerSerializableTypes(hints, List.of(OrderPlaced.class));
    }
  }

  @Test
  void section10_theHintsRegistrarRegistersTheListedType() {
    RuntimeHints hints = new RuntimeHints();

    new MyAppNativeHints().registerHints(hints, getClass().getClassLoader());

    assertThat(hints.reflection().getTypeHint(OrderPlaced.class)).isNotNull();
  }
}
