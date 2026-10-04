package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.RequirePermission;
import org.streamrune.core.RequireRole;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.test.InMemoryEventStore;

/**
 * A command bus checks its own registrations against its own interceptor chain when it is built: a
 * registered command type that declares {@link RequireRole}/{@link RequirePermission} (on itself,
 * its supertypes, or a permitted subtype of a sealed registered type) is refused unless an {@link
 * AnnotationAuthorizationInterceptor} is among the interceptors handed to that same builder. The
 * check therefore covers the bus an application builds by hand exactly like the bus a framework
 * integration assembles — nothing outside the bus has to know which deciders and interceptors it
 * was given.
 */
class VirtualThreadCommandBusAuthorizationEnforcementTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  @RequireRole("ADMIN")
  record ShipOrder(String id) implements Command {}

  record PlaceOrder(String id) implements Command {}

  /** A sealed command family whose only guarded member is a permitted subtype. */
  sealed interface InventoryCommand extends Command permits Restock, Purge {
    String id();
  }

  record Restock(String id) implements InventoryCommand {}

  @RequirePermission("inventory:purge")
  record Purge(String id) implements InventoryCommand {}

  record Done() implements DomainEvent {}

  record NoState() implements AggregateState {}

  static final class NoOpDecider<C extends Command> implements Decider<C, NoState, Done> {
    @Override
    public NoState initialState() {
      return new NoState();
    }

    @Override
    public List<Done> decide(C command, NoState state) {
      return List.of(new Done());
    }

    @Override
    public NoState evolve(NoState state, Done event) {
      return state;
    }
  }

  private static final AnnotationAuthorizationInterceptor ENFORCEMENT =
      new AnnotationAuthorizationInterceptor(userId -> UserAuthority.EMPTY);

  private static VirtualThreadCommandBus.Builder busWith(Class<? extends Command> commandType) {
    return VirtualThreadCommandBus.builder()
        .eventStore(new InMemoryEventStore())
        .register(TYPE, commandType, cmd -> AggregateId.of("agg-1"), new NoOpDecider<>());
  }

  @Test
  void anAnnotatedCommandRegisteredWithoutTheEnforcingInterceptor_isRefusedAtBuild() {
    var builder = busWith(ShipOrder.class).interceptors(CommandInterceptor.noop());

    IllegalStateException refused = assertThrows(IllegalStateException.class, builder::build);

    assertTrue(refused.getMessage().contains("UNGUARDED"), refused.getMessage());
    assertTrue(refused.getMessage().contains(ShipOrder.class.getName()), refused.getMessage());
  }

  @Test
  void anAnnotatedPermittedSubtypeOfARegisteredSealedCommand_isRefusedAtBuild() {
    var builder = busWith(InventoryCommand.class);

    IllegalStateException refused = assertThrows(IllegalStateException.class, builder::build);

    assertTrue(refused.getMessage().contains(Purge.class.getName()), refused.getMessage());
  }

  @Test
  void theEnforcingInterceptorAnywhereInTheChain_satisfiesTheCheck() {
    assertDoesNotThrow(
        () ->
            busWith(ShipOrder.class)
                .interceptors(CommandInterceptor.noop())
                .interceptors(ENFORCEMENT)
                .build());
  }

  @Test
  void anUnannotatedCommand_needsNoEnforcement() {
    assertDoesNotThrow(() -> busWith(PlaceOrder.class).build());
  }

  @Test
  void interceptors_isTheChainInTheOrderItWasAdded_andCannotBeChanged() {
    CommandInterceptor first = CommandInterceptor.noop();
    CommandInterceptor last = CommandInterceptor.noop();
    var bus =
        busWith(ShipOrder.class)
            .interceptors(first)
            .interceptors(List.of(ENFORCEMENT, last))
            .build();

    assertEquals(List.of(first, ENFORCEMENT, last), bus.interceptors());
    assertThrows(UnsupportedOperationException.class, () -> bus.interceptors().add(ENFORCEMENT));
  }

  @Test
  void registeredCommandTypes_isTheRegistrationsInOrder_andCannotBeChanged() {
    var bus =
        busWith(PlaceOrder.class)
            .register(
                TYPE, InventoryCommand.class, cmd -> AggregateId.of(cmd.id()), new NoOpDecider<>())
            .interceptors(ENFORCEMENT)
            .build();

    assertEquals(
        List.of(PlaceOrder.class, InventoryCommand.class),
        List.copyOf(bus.registeredCommandTypes()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> bus.registeredCommandTypes().add(ShipOrder.class));
  }

  @Test
  void theStreamRuneFacade_refusesTheSameConfiguration_beforeItCreatesTheEventStore() {
    var storesCreated = new AtomicInteger();
    var builder =
        StreamRune.builder()
            .eventStoreFactory(
                settings -> {
                  storesCreated.incrementAndGet();
                  return new InMemoryEventStore();
                })
            .register(TYPE, ShipOrder.class, cmd -> AggregateId.of(cmd.id()), new NoOpDecider<>());

    IllegalStateException refused = assertThrows(IllegalStateException.class, builder::build);

    assertTrue(refused.getMessage().contains(ShipOrder.class.getName()), refused.getMessage());
    assertEquals(0, storesCreated.get(), "a refused configuration must not create an event store");
  }
}
