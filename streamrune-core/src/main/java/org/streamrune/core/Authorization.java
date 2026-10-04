package org.streamrune.core;

import org.streamrune.core.types.UserId;

/**
 * Static utility for authorization checks inside {@link Decider#guard(Command, AggregateState)}.
 *
 * <p>Reads the current caller's identity from {@link StreamRuneContext#CURRENT}. Throws {@link
 * AuthorizationException} when a check fails.
 *
 * <p>A command a saga dispatches carries no end user: it runs as the system, on a thread where no
 * user is bound (see {@link StreamRuneContext#SAGA_OWNED}). So there are two ownership checks, and
 * the guard says which one applies to which command:
 *
 * <ul>
 *   <li>{@link #requireOwnerOrSaga(UserId)} admits the owner or a saga. Use it for every command a
 *       saga may send to the aggregate, forward or compensation, or the saga fails on it: a forward
 *       command rejected by the guard is compensated, and a rejected compensation leaves the saga
 *       {@code FAILED} with its undo never run.
 *   <li>{@link #requireOwner(UserId)} admits only the owner and denies every saga-dispatched
 *       command. Use it for a command no saga may ever issue.
 * </ul>
 *
 * <p>Example: skip the check for creation commands, where the aggregate (and its owner) does not
 * exist yet and {@code state.ownerId()} is still {@code null}; admit the owner or a saga otherwise:
 *
 * <pre>{@code
 * public void guard(OrderCommand command, OrderState state) {
 *     if (command instanceof CreateOrder) {
 *         return; // aggregate does not exist yet, nobody owns it
 *     }
 *     Authorization.requireOwnerOrSaga(state.ownerId());
 * }
 * }</pre>
 *
 * <p>A saga acts with the system's authority, so authorize the user command that starts the saga's
 * flow: a saga that takes the target aggregate from an event payload acts on whichever aggregate
 * that payload names.
 */
public final class Authorization {

  private Authorization() {}

  /**
   * Returns the current caller's {@link UserId} from {@link StreamRuneContext}, or {@code null} if
   * no context is bound or no user is set.
   */
  public static UserId currentUserId() {
    if (!StreamRuneContext.CURRENT.isBound()) return null;
    var ctx = StreamRuneContext.CURRENT.get();
    return ctx != null ? ctx.userId() : null;
  }

  /**
   * Throws {@link AuthorizationException} if the current caller is not the given owner.
   *
   * <p>A {@code null} {@code ownerId} always fails the check: no caller can equal a missing owner.
   * In particular, the owner field of {@link Decider#initialState()} is not populated yet, so call
   * this only for commands that target an existing aggregate (see the class example).
   *
   * <p>A saga-dispatched command always fails it too: it carries no end user, and a user bound on
   * the dispatching thread (an operator replaying a saga) is not the actor. Use {@link
   * #requireOwnerOrSaga(UserId)} for a command a saga sends.
   *
   * @param ownerId the aggregate owner's ID; {@code null} always fails
   * @throws AuthorizationException if the caller is unauthenticated, the owner is {@code null}, the
   *     caller is not the owner, or the command was dispatched by a saga
   */
  public static void requireOwner(UserId ownerId) {
    if (StreamRuneContext.isSagaDispatch()) {
      throw new AuthorizationException(
          "A saga-dispatched command has no end user and cannot pass requireOwner; use"
              + " Authorization.requireOwnerOrSaga for a command a saga sends");
    }
    UserId caller = currentUserId();
    if (caller == null || !caller.equals(ownerId)) {
      throw new AuthorizationException("Caller does not own this aggregate");
    }
  }

  /**
   * Passes when the command was dispatched by a saga ({@link StreamRuneContext#isSagaDispatch()});
   * otherwise behaves exactly like {@link #requireOwner(UserId)}.
   *
   * <p>A saga dispatch passes whatever {@code ownerId} is, including {@code null}. Outside a saga,
   * an unauthenticated caller, a {@code null} owner and a caller who is not the owner are denied.
   *
   * @param ownerId the aggregate owner's ID; {@code null} fails unless a saga dispatched the
   *     command
   * @throws AuthorizationException if the command was not dispatched by a saga and the caller is
   *     unauthenticated, the owner is {@code null}, or the caller is not the owner
   */
  public static void requireOwnerOrSaga(UserId ownerId) {
    if (StreamRuneContext.isSagaDispatch()) {
      return;
    }
    requireOwner(ownerId);
  }
}
