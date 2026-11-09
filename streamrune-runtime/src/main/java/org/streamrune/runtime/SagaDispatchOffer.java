package org.streamrune.runtime;

import org.streamrune.core.Command;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.IdempotencyKey;

/**
 * The saga a dispatch is made for, handed from {@link SagaCommandDispatch} to {@link
 * VirtualThreadCommandBus} in a field of the dispatching thread, beside the scoped values the
 * dispatch binds.
 *
 * <p>The bus takes the offer at the start of the keyed dispatch it was made for, which empties the
 * field, so a command dispatched inside that command is not compared with it. It compares the offer
 * with the bindings it reads, and the events of the command then carry the offer's origin,
 * causation id and correlation id, and a fenced forward command is fenced against the offer's saga
 * ({@code VirtualThreadCommandBus.EventProvenance}, {@code VirtualThreadCommandBus.fenceOrigin}).
 * Saga dispatch withdraws an offer the bus did not take when the call returns (a bus of another
 * kind, or one that failed before it reached the dispatch), so no later dispatch of the thread
 * meets it.
 *
 * <p><b>Only the saga's own command takes it.</b> A keyed dispatch of another command on the same
 * thread before the saga's — a bus decorator in front of the bus that dispatches an audit or
 * metering command of its own and then delegates — leaves the offer in place: the dispatch takes it
 * only when it carries the saga's command (the same instance) or the saga's key. That other command
 * is dispatched as any command under the saga's bindings is: its events carry the bound saga
 * provenance and it is not fenced, since the forward marker names the saga's key, not its own. The
 * saga's command still takes the offer, is compared with it and is fenced against it, also when a
 * decorator hands it to the bus under another key. A decorator that hands the bus another command
 * object under another key makes the saga's command neither compared nor fenced, as a bus of
 * another kind does: a decorator forwards either the command instance or its key.
 *
 * @param origin the saga the dispatch is made for
 * @param causationId the causation id of the dispatch's cause: the event that caused it, or {@code
 *     null}
 * @param command the saga's command the dispatch is made for
 * @param key the idempotency key of the command
 * @param forwardFenced whether the command is a forward command fenced at its commit, so the
 *     forward marker of {@code key} is bound
 */
record SagaDispatchOffer(
    SagaOrigin origin,
    CausationId causationId,
    Command command,
    IdempotencyKey key,
    boolean forwardFenced) {

  private static final ThreadLocal<SagaDispatchOffer> OFFERED = new ThreadLocal<>();

  /** Offers this dispatch to the next keyed dispatch of the command bus on this thread. */
  void offer() {
    OFFERED.set(this);
  }

  /**
   * Takes the offer of this thread for the keyed dispatch of {@code command} under {@code key}:
   * when the offer was made for that command (the same instance) or for that key. An offer made for
   * another command and another key stays in place for the dispatch it was made for.
   *
   * @return the offer, which no later call returns; {@code null} when there is none for this
   *     dispatch
   */
  static SagaDispatchOffer take(Command command, IdempotencyKey key) {
    SagaDispatchOffer offer = OFFERED.get();
    if (offer == null || (offer.command() != command && !offer.key().equals(key))) {
      return null;
    }
    OFFERED.remove();
    return offer;
  }

  /** Withdraws an offer of this thread that was not taken. */
  static void withdraw() {
    OFFERED.remove();
  }

  /** Whether an offer of this thread is waiting; for tests. */
  static boolean pending() {
    return OFFERED.get() != null;
  }
}
