package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class ForwardingCommandBusTest {

  record OpenCart(String cartId) implements Command {}

  record CartOpened(String cartId) implements DomainEvent {}

  record Cart() implements AggregateState {}

  private static final AggregateType CART = AggregateType.of("cart");

  private static final Decider<OpenCart, Cart, CartOpened> OPEN =
      new Decider<>() {
        @Override
        public Cart initialState() {
          return new Cart();
        }

        @Override
        public List<CartOpened> decide(OpenCart command, Cart state) {
          return List.of(new CartOpened(command.cartId()));
        }

        @Override
        public Cart evolve(Cart state, CartOpened event) {
          return state;
        }
      };

  private static InMemoryCommandBus bus(InMemoryEventStore store) {
    return InMemoryCommandBus.builder()
        .eventStore(store)
        .register(
            new InMemoryCommandBus.DeciderRegistration<>(
                CART, OpenCart.class, c -> AggregateId.of(c.cartId()), OPEN))
        .build();
  }

  @Test
  void everyMethodOfTheInterfaceIsOverridden_theProbesIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        CommandBus.class, ForwardingCommandBus.class, 4);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        CommandBus.class, ForwardingCommandBus::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        CommandBus.class, ForwardingCommandBus::new);
  }

  @Test
  void failOnce_failsTheFirstCommandBeforeItRuns_andTheSecondSucceeds() {
    var store = new InMemoryEventStore();
    var faults = FaultSchedule.none().failOnce("execute");
    var forwarding = new ForwardingCommandBus(bus(store), faults);
    StreamId cart = StreamId.of(CART, AggregateId.of("cart-1"));

    assertThrows(InjectedFaultException.class, () -> forwarding.execute(new OpenCart("cart-1")));
    assertTrue(store.readStream(cart, Version.initial(), 10).isEmpty(), "nothing was stored");

    var result = forwarding.execute(new OpenCart("cart-1"));

    assertEquals(new Version(1), result.finalVersion());
    assertEquals(2, faults.calls("execute"));
  }

  @Test
  void theProbes_reportTheDelegatesAnswers_notTheDefaults() {
    var fenceIdentity = new Object();
    var keyed =
        new CommandBus() {
          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public Object sagaFenceStorageIdentity() {
            return fenceIdentity;
          }
        };
    var forwarding = new ForwardingCommandBus(keyed);

    assertTrue(forwarding.supportsIdempotentExecution());
    assertSame(fenceIdentity, forwarding.sagaFenceStorageIdentity());
    assertFalse(
        new ForwardingCommandBus(bus(new InMemoryEventStore())).supportsIdempotentExecution());
  }

  @Test
  void aSubclass_handsTheDelegateTheCommandInstanceAndTheKeyItReceived() {
    var received = new ArrayList<Object>();
    var recording =
        new CommandBus() {
          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException();
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            received.add(command);
            received.add(key);
            received.add(Thread.currentThread());
            return null;
          }
        };
    var counted = new ArrayList<Command>();
    var decorator =
        new ForwardingCommandBus(recording) {
          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            counted.add(command);
            return super.execute(command, key);
          }
        };
    var command = new OpenCart("cart-2");
    var key = IdempotencyKey.of("key-2");

    decorator.execute(command, key);

    assertEquals(List.of(command), counted);
    assertSame(command, received.get(0));
    assertSame(key, received.get(1));
    assertSame(Thread.currentThread(), received.get(2), "forwarded on the calling thread");
  }

  @Test
  void aScheduleNamingAMethodTheBusLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().failOnce("excute");

    assertThrows(
        IllegalArgumentException.class,
        () -> new ForwardingCommandBus(bus(new InMemoryEventStore()), misspelled));
  }
}
