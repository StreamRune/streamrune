package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeciderRegistrations.Registration;
import org.streamrune.core.types.AggregateType;

class DeciderRegistrationsTest {

  interface OrderCommand extends Command {}

  record PlaceOrder(String id) implements OrderCommand {}

  // Sibling records with no common registered root: the per-command registration style.
  record ConfirmOrder(String id) implements Command {}

  record CancelOrder(String id) implements Command {}

  interface PolyA extends Command {}

  interface PolyB extends Command {}

  record State() implements AggregateState {}

  record Happened() implements DomainEvent {}

  static final class AnyDecider<C extends Command> implements Decider<C, State, Happened> {
    @Override
    public State initialState() {
      return new State();
    }

    @Override
    public List<Happened> decide(C command, State state) {
      return List.of(new Happened());
    }

    @Override
    public State evolve(State state, Happened event) {
      return state;
    }
  }

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final AggregateType PAYMENT = AggregateType.of("payment");

  /** Registers in order, as a bus builder does; returns what was accepted. */
  private static List<Registration> registerAll(Registration... registrations) {
    var accepted = new ArrayList<Registration>();
    for (var r : registrations) {
      DeciderRegistrations.requireCompatible(accepted, r);
      accepted.add(r);
    }
    return accepted;
  }

  @Test
  void siblingRootsUnderOneType_areOneAggregate_andAccepted() {
    assertDoesNotThrow(
        () ->
            registerAll(
                new Registration(ORDER, PlaceOrder.class, new AnyDecider<PlaceOrder>()),
                new Registration(ORDER, ConfirmOrder.class, new AnyDecider<ConfirmOrder>()),
                new Registration(ORDER, CancelOrder.class, new AnyDecider<CancelOrder>())));
  }

  @Test
  void overlappingRootsWithTheSameType_areAccepted() {
    assertDoesNotThrow(
        () ->
            registerAll(
                new Registration(ORDER, OrderCommand.class, new AnyDecider<OrderCommand>()),
                new Registration(ORDER, PlaceOrder.class, new AnyDecider<PlaceOrder>())));
  }

  @Test
  void unrelatedRootsWithDifferentTypes_areAccepted() {
    assertDoesNotThrow(
        () ->
            registerAll(
                new Registration(AggregateType.of("a"), PolyA.class, new AnyDecider<PolyA>()),
                new Registration(AggregateType.of("b"), PolyB.class, new AnyDecider<PolyB>())));
  }

  @Test
  void oneDeciderInstanceReusedUnderOneType_isAccepted() {
    var shared = new AnyDecider<Command>();
    assertDoesNotThrow(
        () ->
            registerAll(
                new Registration(ORDER, ConfirmOrder.class, shared),
                new Registration(ORDER, CancelOrder.class, shared)));
  }

  @Test
  void aDuplicateCommandType_isRefused() {
    var accepted =
        registerAll(new Registration(ORDER, PlaceOrder.class, new AnyDecider<PlaceOrder>()));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeciderRegistrations.requireCompatible(
                    accepted,
                    new Registration(ORDER, PlaceOrder.class, new AnyDecider<PlaceOrder>())));
    assertEquals(
        "command type "
            + PlaceOrder.class.getName()
            + " is already registered (aggregate type 'order'); a command type routes to exactly"
            + " one decider",
        ex.getMessage());
  }

  @Test
  void overlappingRootsWithDifferentTypes_areRefused_inBothDirections() {
    var rootFirst =
        registerAll(new Registration(ORDER, OrderCommand.class, new AnyDecider<OrderCommand>()));
    var ex1 =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeciderRegistrations.requireCompatible(
                    rootFirst,
                    new Registration(PAYMENT, PlaceOrder.class, new AnyDecider<PlaceOrder>())));
    assertEquals(
        "command types "
            + OrderCommand.class.getName()
            + " and "
            + PlaceOrder.class.getName()
            + " overlap (one is assignable to the other) but declare aggregate types 'order' and"
            + " 'payment'; commands of both route to the same streams, so they must declare the"
            + " same aggregate type",
        ex1.getMessage());

    var leafFirst =
        registerAll(new Registration(ORDER, PlaceOrder.class, new AnyDecider<PlaceOrder>()));
    var ex2 =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeciderRegistrations.requireCompatible(
                    leafFirst,
                    new Registration(PAYMENT, OrderCommand.class, new AnyDecider<OrderCommand>())));
    assertEquals(
        "command types "
            + PlaceOrder.class.getName()
            + " and "
            + OrderCommand.class.getName()
            + " overlap (one is assignable to the other) but declare aggregate types 'order' and"
            + " 'payment'; commands of both route to the same streams, so they must declare the"
            + " same aggregate type",
        ex2.getMessage());
  }

  @Test
  void oneDeciderInstanceUnderTwoTypes_isRefused() {
    var shared = new AnyDecider<Command>();
    var accepted = registerAll(new Registration(ORDER, ConfirmOrder.class, shared));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeciderRegistrations.requireCompatible(
                    accepted, new Registration(PAYMENT, CancelOrder.class, shared)));
    assertEquals(
        "this decider instance is already registered under aggregate type 'order' (command type "
            + ConfirmOrder.class.getName()
            + "); registering it under 'payment' for "
            + CancelOrder.class.getName()
            + " would split one aggregate across two stream namespaces",
        ex.getMessage());
  }

  @Test
  void aTypeBuiltThroughTheLenientDoor_isRefusedAtRegistration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeciderRegistrations.requireCompatible(
                List.of(),
                new Registration(
                    new AggregateType("Order"), PlaceOrder.class, new AnyDecider<PlaceOrder>())));
  }

  @Test
  void registrationComponentsAreRequired() {
    var decider = new AnyDecider<PlaceOrder>();
    assertThrows(
        NullPointerException.class, () -> new Registration(null, PlaceOrder.class, decider));
    assertThrows(NullPointerException.class, () -> new Registration(ORDER, null, decider));
    assertThrows(NullPointerException.class, () -> new Registration(ORDER, PlaceOrder.class, null));
  }
}
