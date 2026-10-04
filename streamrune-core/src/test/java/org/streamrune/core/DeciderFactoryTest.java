package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class DeciderFactoryTest {

  private static final Supplier<CartState> INITIAL = CartState::new;
  private static final BiFunction<CartCommand.AddItem, CartState, List<CartEvent>> DECIDE =
      (cmd, state) -> List.of(new CartEvent.ItemAdded(cmd.sku(), cmd.qty()));
  private static final BiFunction<CartState, CartEvent, CartState> EVOLVE =
      (state, evt) ->
          switch (evt) {
            case CartEvent.ItemAdded e -> new CartState(List.of(new CartItem(e.sku(), e.qty())));
          };

  @Test
  void deciderOfCreatesInlineDecider() {
    var decider =
        DeciderFactory.of(
            CartState::new,
            (CartCommand.AddItem cmd, CartState state) ->
                List.of(new CartEvent.ItemAdded(cmd.sku(), cmd.qty())),
            (CartState state, CartEvent evt) ->
                switch (evt) {
                  case CartEvent.ItemAdded e ->
                      new CartState(List.of(new CartItem(e.sku(), e.qty())));
                });

    var state = decider.initialState();
    var events = decider.decide(new CartCommand.AddItem("cart-1", "SKU-1", 2), state);
    assertEquals(1, events.size());
  }

  @Test
  void deciderOfWithoutGuardPermitsAllCommands() {
    var decider = DeciderFactory.of(INITIAL, DECIDE, EVOLVE);

    assertDoesNotThrow(
        () -> decider.guard(new CartCommand.AddItem("cart-1", "SKU-1", 2), new CartState()));
  }

  @Test
  void deciderOfWithGuardInvokesGuardWithCommandAndState() {
    var seen = new AtomicReference<CartCommand.AddItem>();
    var decider =
        DeciderFactory.of(
            INITIAL,
            DECIDE,
            EVOLVE,
            (command, state) -> {
              seen.set(command);
              if (command.qty() > 10) {
                throw new IllegalStateException("denied");
              }
            });

    var allowed = new CartCommand.AddItem("cart-1", "SKU-1", 2);
    assertDoesNotThrow(() -> decider.guard(allowed, decider.initialState()));
    assertSame(allowed, seen.get());

    var denied = new CartCommand.AddItem("cart-1", "SKU-1", 99);
    var ex =
        assertThrows(
            IllegalStateException.class, () -> decider.guard(denied, decider.initialState()));
    assertEquals("denied", ex.getMessage());
  }

  @Test
  void deciderOfWithGuardStillDelegatesDecideAndEvolve() {
    var decider = DeciderFactory.of(INITIAL, DECIDE, EVOLVE, (command, state) -> {});

    var events = decider.decide(new CartCommand.AddItem("cart-1", "SKU-1", 2), new CartState());
    assertEquals(List.of(new CartEvent.ItemAdded("SKU-1", 2)), events);

    var evolved = decider.evolve(new CartState(), new CartEvent.ItemAdded("SKU-1", 2));
    assertEquals(new CartState(List.of(new CartItem("SKU-1", 2))), evolved);
  }

  @Test
  void deciderOfRejectsNullArguments() {
    BiConsumer<CartCommand.AddItem, CartState> guard = (command, state) -> {};

    var e1 =
        assertThrows(NullPointerException.class, () -> DeciderFactory.of(null, DECIDE, EVOLVE));
    assertEquals("initialState is required", e1.getMessage());

    var e2 =
        assertThrows(NullPointerException.class, () -> DeciderFactory.of(INITIAL, null, EVOLVE));
    assertEquals("decide is required", e2.getMessage());

    var e3 =
        assertThrows(NullPointerException.class, () -> DeciderFactory.of(INITIAL, DECIDE, null));
    assertEquals("evolve is required", e3.getMessage());

    var e4 =
        assertThrows(
            NullPointerException.class, () -> DeciderFactory.of(INITIAL, DECIDE, EVOLVE, null));
    assertEquals("guard is required", e4.getMessage());

    assertThrows(NullPointerException.class, () -> DeciderFactory.of(null, DECIDE, EVOLVE, guard));
  }

  // Mini domain
  sealed interface CartCommand extends Command {
    record AddItem(String cartId, String sku, int qty) implements CartCommand {}
  }

  sealed interface CartEvent extends DomainEvent {
    record ItemAdded(String sku, int qty) implements CartEvent {}
  }

  record CartState(List<CartItem> items) implements AggregateState {
    CartState() {
      this(List.of());
    }
  }

  record CartItem(String sku, int qty) {}
}
