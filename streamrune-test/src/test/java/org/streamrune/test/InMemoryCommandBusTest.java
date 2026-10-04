package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.NoDeciderException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

class InMemoryCommandBusTest {

  private static final AggregateType CART = AggregateType.of("cart");

  // Test types
  record CartId(String value) implements DomainEvent {}

  record CreateCart(String cartId) implements DomainEvent {}

  record ItemAdded(String cartId, String sku, int qty) implements DomainEvent {}

  record CartState(int itemCount, boolean created) implements AggregateState {}

  record CreateCartCmd(String cartId) implements Command {}

  record AddItemCmd(String cartId, String sku, int qty) implements Command {}

  record UnknownCmd(String id) implements Command {}

  record CreateWithItemCmd(String cartId, String sku) implements Command {}

  // Test deciders
  Decider<CreateCartCmd, CartState, DomainEvent> createCartDecider =
      new Decider<>() {
        @Override
        public CartState initialState() {
          return new CartState(0, false);
        }

        @Override
        public List<DomainEvent> decide(CreateCartCmd cmd, CartState state) {
          return List.of(new CreateCart(cmd.cartId()));
        }

        @Override
        public CartState evolve(CartState state, DomainEvent event) {
          if (event instanceof CreateCart) {
            return new CartState(0, true);
          }
          return state;
        }
      };

  Decider<AddItemCmd, CartState, DomainEvent> addItemDecider =
      new Decider<>() {
        @Override
        public CartState initialState() {
          return new CartState(0, false);
        }

        @Override
        public List<DomainEvent> decide(AddItemCmd cmd, CartState state) {
          return List.of(new ItemAdded(cmd.cartId(), cmd.sku(), cmd.qty()));
        }

        @Override
        public CartState evolve(CartState state, DomainEvent event) {
          if (event instanceof ItemAdded added) {
            return new CartState(state.itemCount() + added.qty(), true);
          } else if (event instanceof CreateCart) {
            return new CartState(0, true);
          }
          return state;
        }
      };

  // Idempotent decider that returns empty when already created
  Decider<CreateCartCmd, CartState, DomainEvent> idempotentCreateDecider =
      new Decider<>() {
        @Override
        public CartState initialState() {
          return new CartState(0, false);
        }

        @Override
        public List<DomainEvent> decide(CreateCartCmd cmd, CartState state) {
          if (state.created()) {
            return List.of(); // Idempotent: already created
          }
          return List.of(new CreateCart(cmd.cartId()));
        }

        @Override
        public CartState evolve(CartState state, DomainEvent event) {
          if (event instanceof CreateCart) {
            return new CartState(0, true);
          }
          return state;
        }
      };

  // Decider producing TWO events from one command
  Decider<CreateWithItemCmd, CartState, DomainEvent> createWithItemDecider =
      new Decider<>() {
        @Override
        public CartState initialState() {
          return new CartState(0, false);
        }

        @Override
        public List<DomainEvent> decide(CreateWithItemCmd cmd, CartState state) {
          return List.of(new CreateCart(cmd.cartId()), new ItemAdded(cmd.cartId(), cmd.sku(), 1));
        }

        @Override
        public CartState evolve(CartState state, DomainEvent event) {
          if (event instanceof ItemAdded added) {
            return new CartState(state.itemCount() + added.qty(), true);
          } else if (event instanceof CreateCart) {
            return new CartState(0, true);
          }
          return state;
        }
      };

  @Test
  void shouldExecuteCommandAndReturnEvents() {
    var eventStore = new InMemoryEventStore();
    var commandBus =
        InMemoryCommandBus.builder()
            .eventStore(eventStore)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    CART,
                    CreateCartCmd.class,
                    cmd -> AggregateId.of(cmd.cartId()),
                    createCartDecider))
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    CART, AddItemCmd.class, cmd -> AggregateId.of(cmd.cartId()), addItemDecider))
            .build();

    // Execute first command
    CommandBus.CommandResult result = commandBus.execute(new CreateCartCmd("cart-1"));

    assertFalse(result.events().isEmpty());
    assertEquals(1, result.events().size());
    assertInstanceOf(CreateCart.class, result.events().getFirst());
    assertEquals("cart-1", ((CreateCart) result.events().getFirst()).cartId());
    assertFalse(result.globalOffsets().isEmpty());
    assertEquals(1, result.finalVersion().value());

    // Execute second command on same aggregate
    CommandBus.CommandResult result2 = commandBus.execute(new AddItemCmd("cart-1", "SKU-42", 2));

    assertEquals(1, result2.events().size());
    assertInstanceOf(ItemAdded.class, result2.events().getFirst());
    assertEquals("SKU-42", ((ItemAdded) result2.events().getFirst()).sku());
    assertEquals(2, result2.finalVersion().value());
  }

  @Test
  void shouldReturnEmptyEventsForIdempotentCommand() {
    var eventStore = new InMemoryEventStore();
    var commandBus =
        InMemoryCommandBus.builder()
            .eventStore(eventStore)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    CART,
                    CreateCartCmd.class,
                    cmd -> AggregateId.of(cmd.cartId()),
                    idempotentCreateDecider))
            .build();

    // First call creates the cart
    CommandBus.CommandResult first = commandBus.execute(new CreateCartCmd("cart-1"));
    assertEquals(1, first.events().size());
    assertEquals(1, first.finalVersion().value());

    // Second call is idempotent — no new events
    CommandBus.CommandResult second = commandBus.execute(new CreateCartCmd("cart-1"));
    assertTrue(second.events().isEmpty());
    assertEquals(1, second.finalVersion().value());
  }

  @Test
  void shouldThrowNoDeciderExceptionForUnknownCommand() {
    var eventStore = new InMemoryEventStore();
    var commandBus =
        InMemoryCommandBus.builder()
            .eventStore(eventStore)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    CART,
                    CreateCartCmd.class,
                    cmd -> AggregateId.of(cmd.cartId()),
                    createCartDecider))
            .build();

    var exception =
        assertThrows(NoDeciderException.class, () -> commandBus.execute(new UnknownCmd("id-1")));

    assertEquals(UnknownCmd.class, exception.commandType());
    assertEquals(1, exception.availableCommands().size());
    assertTrue(exception.availableCommands().contains(CreateCartCmd.class));
  }

  @Test
  void deciderRegistrationAcceptsNonNullValues() {
    // Covers the non-throw branches of DeciderRegistration compact constructor
    var reg =
        new InMemoryCommandBus.DeciderRegistration<>(
            CART, CreateCartCmd.class, cmd -> AggregateId.of(cmd.cartId()), createCartDecider);
    assertEquals(CART, reg.aggregateType());
    assertEquals(CreateCartCmd.class, reg.commandType());
    assertNotNull(reg.idExtractor());
    assertNotNull(reg.decider());
    // Invoke the id extractor to exercise it
    assertEquals(
        AggregateId.of("test-cart"), reg.idExtractor().apply(new CreateCartCmd("test-cart")));
  }

  @Test
  void builderRejectsNullEventStore() {
    assertThrows(IllegalArgumentException.class, () -> InMemoryCommandBus.builder().build());
  }

  @Test
  void shouldBeCloseable() {
    var eventStore = new InMemoryEventStore();
    var commandBus =
        InMemoryCommandBus.builder()
            .eventStore(eventStore)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    CART,
                    CreateCartCmd.class,
                    cmd -> AggregateId.of(cmd.cartId()),
                    createCartDecider))
            .build();

    commandBus.close();

    assertThrows(
        IllegalStateException.class, () -> commandBus.execute(new CreateCartCmd("cart-1")));
  }

  // --- Envelope parity with VirtualThreadCommandBus / the persisted stream ---

  private InMemoryCommandBus busWithTwoEventDecider(InMemoryEventStore eventStore) {
    return InMemoryCommandBus.builder()
        .eventStore(eventStore)
        .register(
            new InMemoryCommandBus.DeciderRegistration<>(
                CART,
                CreateWithItemCmd.class,
                cmd -> AggregateId.of(cmd.cartId()),
                createWithItemDecider))
        .register(
            new InMemoryCommandBus.DeciderRegistration<>(
                CART, AddItemCmd.class, cmd -> AggregateId.of(cmd.cartId()), addItemDecider))
        .build();
  }

  @Test
  void resultEnvelopesCarryStoreAssignedVersionsAndOffsets() {
    var eventStore = new InMemoryEventStore();
    var commandBus = busWithTwoEventDecider(eventStore);

    var first = commandBus.execute(new CreateWithItemCmd("cart-1", "SKU-1"));
    assertEquals(2, first.envelopes().size());
    assertEquals(1, first.envelopes().get(0).version().value());
    assertEquals(2, first.envelopes().get(1).version().value());

    // A follow-up command continues from the stream head, not from zero
    var second = commandBus.execute(new AddItemCmd("cart-1", "SKU-2", 1));
    assertEquals(1, second.envelopes().size());
    assertEquals(3, second.envelopes().getFirst().version().value());

    // Versions and offsets match what the store actually persisted
    var persisted = eventStore.load(StreamId.of(CART, AggregateId.of("cart-1")), 0);
    assertEquals(3, persisted.events().size());
    for (int i = 0; i < 2; i++) {
      assertEquals(persisted.events().get(i).version(), first.envelopes().get(i).version());
      assertEquals(
          persisted.events().get(i).globalOffset(), first.envelopes().get(i).globalOffset());
    }
  }

  @Test
  void eventsOfOneCommandShareCorrelationIdAndCommandId() {
    var commandBus = busWithTwoEventDecider(new InMemoryEventStore());

    var result = commandBus.execute(new CreateWithItemCmd("cart-1", "SKU-1"));

    var firstMeta = result.envelopes().get(0).metadata();
    var secondMeta = result.envelopes().get(1).metadata();
    assertEquals(firstMeta.correlationId(), secondMeta.correlationId());
    assertEquals(firstMeta.commandId(), secondMeta.commandId());
    assertNotEquals(firstMeta.eventId(), secondMeta.eventId());
    // Without a bound context the command is a causation root
    assertNull(firstMeta.causationId());
    assertNull(secondMeta.causationId());

    // A separate command gets its own correlation
    var other = commandBus.execute(new AddItemCmd("cart-1", "SKU-2", 1));
    assertNotEquals(
        firstMeta.correlationId(), other.envelopes().getFirst().metadata().correlationId());
  }

  @Test
  void boundStreamRuneContextPropagatesIntoMetadata() {
    var commandBus = busWithTwoEventDecider(new InMemoryEventStore());

    var correlationId = CorrelationId.of("corr-42");
    var userId = UserId.of("user-7");
    var timestamp = Instant.parse("2026-06-12T10:00:00Z");
    var ctx =
        new StreamRuneContext.RequestContext(
            null,
            userId,
            correlationId,
            timestamp,
            Map.of(InMemoryCommandBus.CAUSATION_ID_BAGGAGE_KEY, "evt-99"));

    var result =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .call(() -> commandBus.execute(new CreateWithItemCmd("cart-1", "SKU-1")));

    for (var envelope : result.envelopes()) {
      var metadata = envelope.metadata();
      assertEquals(correlationId, metadata.correlationId());
      assertEquals(userId, metadata.userId());
      assertEquals(timestamp, metadata.timestamp());
      assertEquals("evt-99", metadata.causationId().value());
      assertEquals("evt-99", metadata.baggage().get(InMemoryCommandBus.CAUSATION_ID_BAGGAGE_KEY));
    }
  }

  // --- The authorization hook runs exactly as on the production bus ---

  @Test
  void guardRunsBeforeDecide_aRejectingGuardPropagates_andAppendsNothing() {
    var eventStore = new InMemoryEventStore();
    var guardCalls = new java.util.concurrent.atomic.AtomicInteger();
    Decider<AddItemCmd, CartState, DomainEvent> guardedDecider =
        new Decider<>() {
          @Override
          public CartState initialState() {
            return new CartState(0, false);
          }

          @Override
          public void guard(AddItemCmd cmd, CartState state) {
            guardCalls.incrementAndGet();
            throw new SecurityException("not the owner of " + cmd.cartId());
          }

          @Override
          public List<DomainEvent> decide(AddItemCmd cmd, CartState state) {
            return List.of(new ItemAdded(cmd.cartId(), cmd.sku(), cmd.qty()));
          }

          @Override
          public CartState evolve(CartState state, DomainEvent event) {
            return state;
          }
        };
    var bus =
        InMemoryCommandBus.builder()
            .eventStore(eventStore)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    CART, AddItemCmd.class, c -> AggregateId.of(c.cartId()), guardedDecider))
            .build();

    var thrown =
        assertThrows(
            SecurityException.class, () -> bus.execute(new AddItemCmd("cart-1", "sku", 1)));

    assertEquals("not the owner of cart-1", thrown.getMessage());
    assertEquals(1, guardCalls.get(), "the guard must run — the pre-fix bus skipped it entirely");
    assertTrue(
        eventStore
            .readStream(StreamId.of(CART, AggregateId.of("cart-1")), Version.initial(), 10)
            .isEmpty(),
        "a rejected command decides nothing and appends nothing");
  }

  // --- the aggregate type: two types sharing an id value are two streams ---

  private static final AggregateType PRODUCT = AggregateType.of("product");
  private static final AggregateType INVENTORY = AggregateType.of("inventory");

  record ProductCmd(String id) implements Command {}

  record StockCmd(String id) implements Command {}

  record ProductCreated(String id) implements DomainEvent {}

  record StockReceived(String id) implements DomainEvent {}

  record Marker() implements AggregateState {}

  private static final class ProductDecider implements Decider<ProductCmd, Marker, DomainEvent> {
    @Override
    public Marker initialState() {
      return new Marker();
    }

    @Override
    public List<DomainEvent> decide(ProductCmd cmd, Marker state) {
      return List.of(new ProductCreated(cmd.id()));
    }

    @Override
    public Marker evolve(Marker state, DomainEvent event) {
      if (event instanceof ProductCreated) {
        return state;
      }
      throw new IllegalStateException("foreign event " + event.getClass().getSimpleName());
    }
  }

  private static final class StockDecider implements Decider<StockCmd, Marker, DomainEvent> {
    @Override
    public Marker initialState() {
      return new Marker();
    }

    @Override
    public List<DomainEvent> decide(StockCmd cmd, Marker state) {
      return List.of(new StockReceived(cmd.id()));
    }

    @Override
    public Marker evolve(Marker state, DomainEvent event) {
      if (event instanceof StockReceived) {
        return state;
      }
      throw new IllegalStateException("foreign event " + event.getClass().getSimpleName());
    }
  }

  @Test
  void twoAggregateTypesSharingAnIdValue_writeTwoStreams() {
    var store = new InMemoryEventStore();
    var bus =
        InMemoryCommandBus.builder()
            .eventStore(store)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    PRODUCT, ProductCmd.class, c -> AggregateId.of(c.id()), new ProductDecider()))
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    INVENTORY, StockCmd.class, c -> AggregateId.of(c.id()), new StockDecider()))
            .build();

    var product = bus.execute(new ProductCmd("sku-1"));
    var stock = bus.execute(new StockCmd("sku-1"));

    assertEquals(StreamId.of(PRODUCT, AggregateId.of("sku-1")), product.streamId());
    assertEquals(StreamId.of(INVENTORY, AggregateId.of("sku-1")), stock.streamId());
    assertEquals(1, store.load(product.streamId()).events().size());
    assertEquals(1, store.load(stock.streamId()).events().size());
    assertEquals(new Version(1), stock.finalVersion());
  }

  @Test
  void registrationFollowsTheSameRuleAsTheRealBus_andFailsAtTheCall() {
    var builder =
        InMemoryCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    PRODUCT, ProductCmd.class, c -> AggregateId.of(c.id()), new ProductDecider()));
    // The builder refuses at the offending register(...) call, as VirtualThreadCommandBus does.
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.register(
                    new InMemoryCommandBus.DeciderRegistration<>(
                        PRODUCT,
                        ProductCmd.class,
                        c -> AggregateId.of(c.id()),
                        new ProductDecider())));
    assertTrue(
        ex.getMessage()
            .startsWith("command type " + ProductCmd.class.getName() + " is already registered"),
        ex.getMessage());
    // The bus's own public register(...) applies the same rule.
    var bus = builder.build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            bus.register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    INVENTORY,
                    ProductCmd.class,
                    c -> AggregateId.of(c.id()),
                    new ProductDecider())));
  }

  @Test
  void registrationRefusesATypeOutsideTheSyntax_atTheCall() {
    var lenient = new AggregateType("Product");
    var builder = InMemoryCommandBus.builder().eventStore(new InMemoryEventStore());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder.register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    lenient, ProductCmd.class, c -> AggregateId.of(c.id()), new ProductDecider())));
  }

  @Test
  void aDeciderRegistrationRequiresItsAggregateType() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new InMemoryCommandBus.DeciderRegistration<>(
                    null, ProductCmd.class, c -> AggregateId.of(c.id()), new ProductDecider()));
    assertEquals("aggregateType is required", ex.getMessage());
  }

  @Test
  void anExtractorThatReturnsNullFailsBeforeAnythingIsLoaded() {
    var store = new InMemoryEventStore();
    var bus =
        InMemoryCommandBus.builder()
            .eventStore(store)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    PRODUCT, ProductCmd.class, c -> null, new ProductDecider()))
            .build();

    var ex = assertThrows(IllegalArgumentException.class, () -> bus.execute(new ProductCmd("x")));

    assertEquals("idExtractor returned null for command: ProductCmd", ex.getMessage());
  }
}
