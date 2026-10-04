package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Authorization;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;

/**
 * An ownership check in {@link Decider#guard} against commands a saga dispatches. A saga runs as
 * the system, with no end user bound, so {@link Authorization#requireOwner} can never pass for it;
 * {@link Authorization#requireOwnerOrSaga} admits it. The saga-dispatch marker is bound by {@link
 * SagaCommandDispatch#executeCorrelated} and read in domain code through {@link
 * StreamRuneContext#isSagaDispatch()}, so a domain module depending only on {@code streamrune-core}
 * can make the choice.
 */
class SagaDispatchOwnershipGuardTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  private static final UserId ALICE = UserId.of("alice");

  sealed interface OrderCommand extends Command {
    String orderId();

    record PlaceOrder(String orderId, UserId ownerId) implements OrderCommand {}

    /** A saga confirms the order: the guard admits the owner or a saga. */
    record ConfirmOrder(String orderId) implements OrderCommand {}

    /** Only the owner may ever change the address: the guard is strict. */
    record ChangeAddress(String orderId) implements OrderCommand {}
  }

  sealed interface OrderEvent extends DomainEvent {
    record OrderPlaced(String orderId, UserId ownerId) implements OrderEvent {}

    record OrderConfirmed(String orderId) implements OrderEvent {}

    record AddressChanged(String orderId) implements OrderEvent {}
  }

  record OrderState(UserId ownerId) implements AggregateState {}

  /** The guard shape the {@link Decider#guard} javadoc documents, plus one strict command. */
  static final class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {
    @Override
    public OrderState initialState() {
      return new OrderState(null);
    }

    @Override
    public void guard(OrderCommand command, OrderState state) {
      switch (command) {
        case OrderCommand.PlaceOrder _ -> {
          // aggregate does not exist yet, nobody owns it
        }
        case OrderCommand.ConfirmOrder _ -> Authorization.requireOwnerOrSaga(state.ownerId());
        case OrderCommand.ChangeAddress _ -> Authorization.requireOwner(state.ownerId());
      }
    }

    @Override
    public List<OrderEvent> decide(OrderCommand command, OrderState state) {
      return switch (command) {
        case OrderCommand.PlaceOrder p ->
            List.of(new OrderEvent.OrderPlaced(p.orderId(), p.ownerId()));
        case OrderCommand.ConfirmOrder c -> List.of(new OrderEvent.OrderConfirmed(c.orderId()));
        case OrderCommand.ChangeAddress c -> List.of(new OrderEvent.AddressChanged(c.orderId()));
      };
    }

    @Override
    public OrderState evolve(OrderState state, OrderEvent event) {
      return event instanceof OrderEvent.OrderPlaced placed
          ? new OrderState(placed.ownerId())
          : state;
    }
  }

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final InMemoryEventStore store = new InMemoryEventStore().withCommandInbox(inbox);
  private final VirtualThreadCommandBus bus =
      VirtualThreadCommandBus.builder()
          .eventStore(store)
          .locker(new LocalStripedLocker(16))
          .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
          .commandInbox(inbox)
          .register(
              TYPE, OrderCommand.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
          .build();

  private static StreamRuneContext.RequestContext contextFor(UserId user) {
    return new StreamRuneContext.RequestContext(
        null, user, CorrelationId.of("request-1"), Instant.now(), Map.of());
  }

  private void placeOrderAsAlice(String orderId) {
    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(ALICE))
        .run(() -> bus.execute(new OrderCommand.PlaceOrder(orderId, ALICE)));
  }

  private List<Class<?>> eventTypes(String orderId) {
    return store.load(StreamId.of(TYPE, AggregateId.of(orderId))).events().stream()
        .map(EventEnvelope::event)
        .<Class<?>>map(Object::getClass)
        .toList();
  }

  @Test
  void sagaDispatchedCommand_passesRequireOwnerOrSagaGuard_withNoBoundUser() {
    placeOrderAsAlice("order-1");

    // No user is bound: exactly how a subscription poll, timeout or sweeper thread dispatches.
    assertDoesNotThrow(
        () ->
            SagaCommandDispatch.executeCorrelated(
                bus,
                CorrelationId.of("saga-order-1"),
                new OrderCommand.ConfirmOrder("order-1"),
                IdempotencyKey.of("saga:saga-order-1:0:0")));

    assertEquals(
        List.of(OrderEvent.OrderPlaced.class, OrderEvent.OrderConfirmed.class),
        eventTypes("order-1"));
  }

  @Test
  void sagaDispatchedCommand_deniedByStrictRequireOwnerGuard_namesTheSagaCause() {
    placeOrderAsAlice("order-2");

    var denied =
        assertThrows(
            AuthorizationException.class,
            () ->
                SagaCommandDispatch.executeCorrelated(
                    bus,
                    CorrelationId.of("saga-order-2"),
                    new OrderCommand.ChangeAddress("order-2"),
                    IdempotencyKey.of("saga:saga-order-2:0:0")));

    assertTrue(
        denied.getMessage().contains("requireOwnerOrSaga"),
        "a saga denied by requireOwner must be told why: " + denied.getMessage());
    assertEquals(List.of(OrderEvent.OrderPlaced.class), eventTypes("order-2"));
  }

  @Test
  void unauthenticatedDirectCommand_isStillDeniedByRequireOwnerOrSagaGuard() {
    placeOrderAsAlice("order-3");

    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(null))
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class,
                    () -> bus.execute(new OrderCommand.ConfirmOrder("order-3"))));
    assertThrows(
        AuthorizationException.class, () -> bus.execute(new OrderCommand.ConfirmOrder("order-3")));

    assertEquals(List.of(OrderEvent.OrderPlaced.class), eventTypes("order-3"));
  }

  @Test
  void nonOwnerDirectCommand_isDeniedByRequireOwnerOrSagaGuard() {
    placeOrderAsAlice("order-4");

    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(UserId.of("mallory")))
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class,
                    () -> bus.execute(new OrderCommand.ConfirmOrder("order-4"))));

    assertEquals(List.of(OrderEvent.OrderPlaced.class), eventTypes("order-4"));
  }

  @Test
  void asyncDispatchFromASagaScope_keepsTheSagaDispatchMarkerOnTheNewThread() {
    placeOrderAsAlice("order-5");

    assertDoesNotThrow(
        () ->
            ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
                .call(() -> bus.executeAsync(new OrderCommand.ConfirmOrder("order-5")).join()));
    assertEquals(
        List.of(OrderEvent.OrderPlaced.class, OrderEvent.OrderConfirmed.class),
        eventTypes("order-5"));

    // Non-vacuity: the same async dispatch outside a saga scope is denied on the new thread.
    var failure =
        assertThrows(
            CompletionException.class,
            () -> bus.executeAsync(new OrderCommand.ConfirmOrder("order-5")).join());
    assertInstanceOf(AuthorizationException.class, failure.getCause());
  }
}
