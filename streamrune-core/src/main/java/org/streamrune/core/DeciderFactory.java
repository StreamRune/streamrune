package org.streamrune.core;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Factory for creating deciders inline with lambdas. */
public final class DeciderFactory {

  private DeciderFactory() {}

  /**
   * Creates a decider with inline lambda functions and the default permit-all {@link
   * Decider#guard(Command, AggregateState) guard}.
   *
   * @param initialState supplier for the initial aggregate state
   * @param decide function that produces events from command and state; must honor the {@link
   *     Decider#decide(Command, AggregateState)} contract and never return {@code null}
   * @param evolve function that evolves state given an event
   * @throws NullPointerException if any argument is {@code null}
   */
  public static <C extends Command, S extends AggregateState, E extends DomainEvent>
      Decider<C, S, E> of(
          Supplier<S> initialState, BiFunction<C, S, List<E>> decide, BiFunction<S, E, S> evolve) {
    return of(initialState, decide, evolve, (command, state) -> {});
  }

  /**
   * Creates a decider with inline lambda functions and an authorization guard.
   *
   * <p>{@code guard} runs after state is loaded and before {@code decide}, exactly like an
   * overridden {@link Decider#guard(Command, AggregateState)} — including for commands that create
   * the aggregate, where {@code state} is the initial state. It rejects a command by throwing.
   *
   * @param initialState supplier for the initial aggregate state
   * @param decide function that produces events from command and state; must honor the {@link
   *     Decider#decide(Command, AggregateState)} contract and never return {@code null}
   * @param evolve function that evolves state given an event
   * @param guard authorization check invoked with the command and the loaded state
   * @throws NullPointerException if any argument is {@code null}
   */
  public static <C extends Command, S extends AggregateState, E extends DomainEvent>
      Decider<C, S, E> of(
          Supplier<S> initialState,
          BiFunction<C, S, List<E>> decide,
          BiFunction<S, E, S> evolve,
          BiConsumer<C, S> guard) {
    Objects.requireNonNull(initialState, "initialState is required");
    Objects.requireNonNull(decide, "decide is required");
    Objects.requireNonNull(evolve, "evolve is required");
    Objects.requireNonNull(guard, "guard is required");
    return new Decider<>() {
      @Override
      public S initialState() {
        return initialState.get();
      }

      @Override
      public List<E> decide(C command, S state) {
        return decide.apply(command, state);
      }

      @Override
      public S evolve(S state, E event) {
        return evolve.apply(state, event);
      }

      @Override
      public void guard(C command, S state) {
        guard.accept(command, state);
      }
    };
  }
}
