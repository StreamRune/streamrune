package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;

/**
 * Fluent BDD-style testing DSL for {@link Decider} implementations.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * DeciderFixture.of(new CartDecider())
 *     .given(new CartEvent.ItemAdded("SKU-1", 2))
 *     .when(new CartCommand.Checkout("cart-1"))
 *     .expectEvents(new CartEvent.CheckedOut())
 *     .expectState(state -> assertTrue(state.isClosed()));
 * }</pre>
 *
 * @param <C> Command type
 * @param <S> State type (must implement AggregateState)
 * @param <E> Event type (must implement DomainEvent)
 */
public final class DeciderFixture<
    C extends Command, S extends AggregateState, E extends DomainEvent> {

  private final Decider<C, S, E> decider;

  private DeciderFixture(Decider<C, S, E> decider) {
    this.decider = decider;
  }

  /** Creates a new fixture for the given decider. */
  public static <C extends Command, S extends AggregateState, E extends DomainEvent>
      DeciderFixture<C, S, E> of(Decider<C, S, E> decider) {
    return new DeciderFixture<>(decider);
  }

  /** Sets up the initial state by applying the given events to the decider's initial state. */
  @SafeVarargs
  public final GivenStage given(E... events) {
    S state = decider.initialState();
    for (E event : events) {
      state = decider.evolve(state, event);
    }
    return new GivenStage(state);
  }

  /** Alias for {@link #given(DomainEvent...)} for a more expressive DSL. */
  @SafeVarargs
  public final GivenStage givenEvents(E... events) {
    return given(events);
  }

  /** Intermediate stage after setting up preconditions. */
  public final class GivenStage {

    private final S state;

    GivenStage(S state) {
      this.state = state;
    }

    /** Executes the command against the current state, capturing events or any thrown exception. */
    public WhenStage when(C command) {
      try {
        List<E> events = decider.decide(command, state);
        return new WhenStage(state, events, null);
      } catch (Exception e) {
        return new WhenStage(state, null, e);
      }
    }

    /** Alias for {@link #when(Command)} for a more expressive DSL. */
    public WhenStage whenCommand(C command) {
      return when(command);
    }
  }

  /** Terminal stage for asserting expectations on produced events, state, or exceptions. */
  public final class WhenStage {

    private final S stateBefore;
    private final List<E> producedEvents;
    private final Exception exception;

    WhenStage(S stateBefore, List<E> producedEvents, Exception exception) {
      this.stateBefore = stateBefore;
      this.producedEvents = producedEvents;
      this.exception = exception;
    }

    /** Asserts that exactly the given events were produced. */
    @SafeVarargs
    public final WhenStage expectEvents(E... expected) {
      assertNull(exception, "Expected events but got exception: " + exception);
      assertEquals(Arrays.asList(expected), producedEvents);
      return this;
    }

    /** Asserts that no events were produced. */
    public WhenStage expectNoEvents() {
      assertNull(exception, "Expected no events but got exception: " + exception);
      assertTrue(producedEvents.isEmpty(), "Expected no events but got: " + producedEvents);
      return this;
    }

    /** Asserts that an exception of the given type was thrown. */
    public <X extends Throwable> WhenStage expectException(Class<X> exceptionType) {
      assertNotNull(
          exception, "Expected " + exceptionType.getSimpleName() + " but none was thrown");
      assertInstanceOf(exceptionType, exception);
      return this;
    }

    /**
     * Asserts that an exception of the given type was thrown with a message containing the given
     * text.
     */
    @SuppressWarnings("unchecked")
    public <X extends Throwable> WhenStage expectFailedWith(
        Class<X> exceptionType, String messageContains) {
      assertNotNull(messageContains, "messageContains must not be null");
      X ex = (X) expectException(exceptionType).exception;
      assertTrue(
          ex.getMessage() != null && ex.getMessage().contains(messageContains),
          "Exception message should contain '" + messageContains + "' but was: " + ex.getMessage());
      return this;
    }

    /**
     * Evolves the state with produced events and passes the resulting state to the given assertion.
     */
    public WhenStage expectState(Consumer<S> assertion) {
      assertNull(exception, "Expected state check but got exception: " + exception);
      S finalState = stateBefore;
      for (E event : producedEvents) {
        finalState = decider.evolve(finalState, event);
      }
      assertion.accept(finalState);
      return this;
    }

    /** Asserts that the final state equals the expected state using equals(). */
    public WhenStage expectStateMatches(S expected) {
      assertNull(exception, "Expected state check but got exception: " + exception);
      S finalState = stateBefore;
      for (E event : producedEvents) {
        finalState = decider.evolve(finalState, event);
      }
      assertEquals(expected, finalState);
      return this;
    }

    /** Combines event and state assertions in a single call. */
    public WhenStage expectProduced(Consumer<List<E>> eventsAssertion, Consumer<S> stateAssertion) {
      assertNull(exception, "Expected produced events but got exception: " + exception);
      eventsAssertion.accept(producedEvents);
      S finalState = stateBefore;
      for (E event : producedEvents) {
        finalState = decider.evolve(finalState, event);
      }
      stateAssertion.accept(finalState);
      return this;
    }
  }
}
