package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

class DeciderTest {

  // --- Minimal test domain ---

  sealed interface Cmd extends Command {
    record Add(String item, int qty) implements Cmd {
      public Add {
        if (item == null || item.isBlank()) throw new IllegalArgumentException("item is required");
        if (qty <= 0) throw new IllegalArgumentException("qty must be > 0");
      }
    }

    record Clear() implements Cmd {}
  }

  sealed interface Evt extends DomainEvent {
    record Added(String item, int qty) implements Evt {}

    record Cleared() implements Evt {}
  }

  record State(Map<String, Integer> items) implements AggregateState {
    public State() {
      this(Map.of());
    }
  }

  static class TestDecider implements Decider<Cmd, State, Evt> {
    @Override
    public State initialState() {
      return new State();
    }

    @Override
    public List<Evt> decide(Cmd command, State state) {
      return switch (command) {
        case Cmd.Add c -> List.of(new Evt.Added(c.item(), c.qty()));
        case Cmd.Clear _ -> List.of(new Evt.Cleared());
      };
    }

    @Override
    public State evolve(State state, Evt event) {
      return switch (event) {
        case Evt.Added e -> {
          var items = new HashMap<>(state.items());
          items.merge(e.item(), e.qty(), Integer::sum);
          yield new State(Map.copyOf(items));
        }
        case Evt.Cleared _ -> new State();
      };
    }
  }

  private final TestDecider decider = new TestDecider();

  @Test
  void initialStateShouldBeEmpty() {
    var state = decider.initialState();
    assertTrue(state.items().isEmpty());
  }

  @Test
  void decideShouldProduceAddedEvent() {
    var events = decider.decide(new Cmd.Add("SKU-1", 3), decider.initialState());
    assertEquals(1, events.size());
    assertInstanceOf(Evt.Added.class, events.getFirst());
    var added = (Evt.Added) events.getFirst();
    assertEquals("SKU-1", added.item());
    assertEquals(3, added.qty());
  }

  @Test
  void evolveShouldUpdateState() {
    var state = decider.evolve(decider.initialState(), new Evt.Added("SKU-1", 2));
    assertEquals(Map.of("SKU-1", 2), state.items());
  }

  @Test
  void evolveShouldMergeQuantities() {
    var s1 = decider.evolve(decider.initialState(), new Evt.Added("SKU-1", 2));
    var s2 = decider.evolve(s1, new Evt.Added("SKU-1", 3));
    assertEquals(Map.of("SKU-1", 5), s2.items());
  }

  @Test
  void clearShouldResetState() {
    var s1 = decider.evolve(decider.initialState(), new Evt.Added("SKU-1", 2));
    var s2 = decider.evolve(s1, new Evt.Cleared());
    assertTrue(s2.items().isEmpty());
  }

  @Test
  void fullCycleDecideThenEvolve() {
    var state = decider.initialState();
    var events = decider.decide(new Cmd.Add("A", 1), state);
    for (var e : events) {
      state = decider.evolve(state, e);
    }
    assertEquals(Map.of("A", 1), state.items());

    events = decider.decide(new Cmd.Add("B", 5), state);
    for (var e : events) {
      state = decider.evolve(state, e);
    }
    assertEquals(Map.of("A", 1, "B", 5), state.items());
  }

  @Test
  void commandValidationShouldFailFast() {
    assertThrows(IllegalArgumentException.class, () -> new Cmd.Add(null, 1));
    assertThrows(IllegalArgumentException.class, () -> new Cmd.Add("", 1));
    assertThrows(IllegalArgumentException.class, () -> new Cmd.Add("X", 0));
    assertThrows(IllegalArgumentException.class, () -> new Cmd.Add("X", -1));
  }

  // --- guard() ---

  sealed interface OwnedCmd extends Command {
    record Create(String name) implements OwnedCmd {}

    record Rename(String name) implements OwnedCmd {}
  }

  record OwnedState(UserId ownerId) implements AggregateState {}

  /** Implements exactly the guard pattern documented on {@link Decider#guard}. */
  static class GuardedDecider implements Decider<OwnedCmd, OwnedState, Evt> {
    @Override
    public OwnedState initialState() {
      return new OwnedState(null);
    }

    @Override
    public List<Evt> decide(OwnedCmd command, OwnedState state) {
      return List.of();
    }

    @Override
    public OwnedState evolve(OwnedState state, Evt event) {
      return state;
    }

    @Override
    public void guard(OwnedCmd command, OwnedState state) {
      if (command instanceof OwnedCmd.Create) {
        return; // aggregate does not exist yet, nobody owns it
      }
      Authorization.requireOwnerOrSaga(state.ownerId());
    }
  }

  private final GuardedDecider guarded = new GuardedDecider();

  private static void runAs(UserId user, Runnable body) {
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, user, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(body);
  }

  @Test
  void guardDefaultImplementationPermitsAllCommands() {
    assertDoesNotThrow(() -> decider.guard(new Cmd.Add("SKU-1", 1), decider.initialState()));
  }

  @Test
  void documentedGuardPatternPermitsCreationCommandOnInitialState() {
    runAs(
        UserId.of("user-1"),
        () ->
            assertDoesNotThrow(
                () -> guarded.guard(new OwnedCmd.Create("first"), guarded.initialState())));
  }

  @Test
  void documentedGuardPatternPermitsOwnerOnExistingAggregate() {
    UserId owner = UserId.of("user-1");
    runAs(
        owner,
        () ->
            assertDoesNotThrow(
                () -> guarded.guard(new OwnedCmd.Rename("renamed"), new OwnedState(owner))));
  }

  @Test
  void documentedGuardPatternRejectsNonOwnerOnExistingAggregate() {
    runAs(
        UserId.of("intruder"),
        () ->
            assertThrows(
                AuthorizationException.class,
                () ->
                    guarded.guard(
                        new OwnedCmd.Rename("hijack"), new OwnedState(UserId.of("user-1")))));
  }

  @Test
  void documentedGuardPatternPermitsSagaDispatchedCommandWithNoUser() {
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .run(
            () ->
                assertDoesNotThrow(
                    () ->
                        guarded.guard(
                            new OwnedCmd.Rename("by-saga"), new OwnedState(UserId.of("user-1")))));
  }

  @Test
  void documentedGuardPatternRejectsUnauthenticatedCommandOutsideASaga() {
    assertThrows(
        AuthorizationException.class,
        () -> guarded.guard(new OwnedCmd.Rename("anon"), new OwnedState(UserId.of("user-1"))));
  }
}
