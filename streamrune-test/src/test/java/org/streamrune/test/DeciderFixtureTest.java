package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;

class DeciderFixtureTest {

  // --- Mini domain ---

  sealed interface Cmd extends Command {
    record Add(String item) implements Cmd {}

    record Close() implements Cmd {}
  }

  sealed interface Evt extends DomainEvent {
    record Added(String item) implements Evt {}

    record Closed() implements Evt {}
  }

  record State(List<String> items, boolean closed) implements AggregateState {
    State() {
      this(List.of(), false);
    }
  }

  static final class TestDecider implements Decider<Cmd, State, Evt> {

    @Override
    public State initialState() {
      return new State();
    }

    @Override
    public List<Evt> decide(Cmd command, State state) {
      return switch (command) {
        case Cmd.Add add -> {
          if (state.closed()) {
            throw new DomainException("Cannot add to closed");
          }
          yield List.of(new Evt.Added(add.item()));
        }
        case Cmd.Close _ -> List.of(new Evt.Closed());
      };
    }

    @Override
    public State evolve(State state, Evt event) {
      return switch (event) {
        case Evt.Added e -> {
          var items = new java.util.ArrayList<>(state.items());
          items.add(e.item());
          yield new State(List.copyOf(items), state.closed());
        }
        case Evt.Closed _ -> new State(state.items(), true);
      };
    }
  }

  // --- Tests ---

  private final DeciderFixture<Cmd, State, Evt> fixture = DeciderFixture.of(new TestDecider());

  @Test
  void givenNothingWhenAddThenExpectEvents() {
    fixture.given().when(new Cmd.Add("x")).expectEvents(new Evt.Added("x"));
  }

  @Test
  void givenClosedWhenAddThenExpectException() {
    fixture.given(new Evt.Closed()).when(new Cmd.Add("x")).expectException(DomainException.class);
  }

  @Test
  void expectFailedWithValidatesMessage() {
    fixture
        .given(new Evt.Closed())
        .when(new Cmd.Add("x"))
        .expectFailedWith(DomainException.class, "Cannot add");
  }

  @Test
  void givenEventsWhenAddThenExpectEventsAndState() {
    fixture
        .given(new Evt.Added("a"))
        .when(new Cmd.Add("b"))
        .expectEvents(new Evt.Added("b"))
        .expectState(
            state -> {
              assertFalse(state.closed());
              assertEquals(List.of("a", "b"), state.items());
            });
  }

  @Test
  void expectStateMatchesComparesStates() {
    var expected = new State(List.of("a"), true);
    fixture.given(new Evt.Added("a")).when(new Cmd.Close()).expectStateMatches(expected);
  }

  @Test
  void expectProducedCombinesAssertions() {
    fixture
        .givenEvents(new Evt.Added("a"))
        .whenCommand(new Cmd.Close())
        .expectProduced(
            events -> assertEquals(1, events.size()), state -> assertTrue(state.closed()));
  }
}
