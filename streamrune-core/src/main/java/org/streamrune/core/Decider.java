package org.streamrune.core;

import java.util.List;

/**
 * Pure domain logic. No side effects, no I/O.
 *
 * <p>The Decider pattern replaces traditional stateful aggregates with pure functions: {@code
 * decide(command, state) → events} and {@code evolve(state, event) → state}.
 *
 * @param <C> Command type (sealed interface recommended)
 * @param <S> State type (immutable record, must implement AggregateState)
 * @param <E> Event type (sealed interface recommended, must implement DomainEvent)
 */
public interface Decider<C extends Command, S extends AggregateState, E extends DomainEvent> {

  /** Returns the initial state before any events have been applied. */
  S initialState();

  /**
   * Evaluates a command against the current state, producing zero or more events.
   *
   * <p>Must never return {@code null}: return an empty list when the command is valid but produces
   * no events. The runtime treats an empty list as a successful no-op and persists nothing; a
   * {@code null} return is a contract violation.
   */
  List<E> decide(C command, S state);

  /** Applies a single event to the current state, returning the new state. */
  S evolve(S state, E event);

  /**
   * Authorization hook called after state is loaded from the event store and before {@link
   * #decide(Command, AggregateState)}. Override to add ownership checks or custom authorization
   * logic. Default implementation permits all commands.
   *
   * <p>{@code guard} also runs for commands that create the aggregate: the stream does not exist
   * yet, so {@code state} is {@link #initialState()} and owner fields are not populated. An
   * unconditional ownership check would therefore reject every creation command (a {@code null}
   * owner always fails it) — branch on the creation command type instead.
   *
   * <p>{@code guard} also runs for commands a saga dispatches. Those carry no end user, so {@code
   * Authorization.requireOwner} denies them; use {@code Authorization.requireOwnerOrSaga} for any
   * command a saga sends, forward or compensation, or a guard rejection compensates the saga's flow
   * and a rejected compensation leaves the saga {@code FAILED} with its undo never run:
   *
   * <pre>{@code
   * @Override
   * public void guard(OrderCommand command, OrderState state) {
   *     if (command instanceof CreateOrder) {
   *         return; // aggregate does not exist yet, nobody owns it
   *     }
   *     Authorization.requireOwnerOrSaga(state.ownerId()); // the owner, or a saga
   * }
   * }</pre>
   *
   * @param command the command about to be decided
   * @param state the current aggregate state (already loaded; {@link #initialState()} when the
   *     stream does not exist yet)
   */
  default void guard(C command, S state) {}
}
