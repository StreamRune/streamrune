package org.streamrune.runtime;

import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;

/**
 * A correlated event delivered to a KNOWN saga that cannot consume it yet, recorded in the saga
 * dead-letter store instead of processed. The FAULTED hold keeps its own type, {@link
 * SagaSkippedWhileFaultedException}. {@code SagaDeadLetterReplayer.replayAll(sagaId)} feeds held
 * events in offset order once the blocker is gone.
 */
final class SagaEventHeldException extends RuntimeException {

  enum HoldReason {
    /** The start event's dispatch loop has not committed ({@code genesis_applied = false}). */
    GENESIS_PENDING,
    /**
     * The saga owns older quarantined entries; consuming a newer live event first would reorder its
     * history.
     */
    BACKLOG_PENDING,
    /**
     * No row exists, but the saga owns a dead-letter record: its start could not produce an initial
     * state.
     */
    BEFORE_START
  }

  private final HoldReason reason;

  SagaEventHeldException(SagaId sagaId, GlobalOffset eventOffset, HoldReason reason) {
    super(message(sagaId, eventOffset, reason));
    this.reason = reason;
  }

  HoldReason reason() {
    return reason;
  }

  private static String message(SagaId sagaId, GlobalOffset eventOffset, HoldReason reason) {
    String why =
        switch (reason) {
          case GENESIS_PENDING ->
              "its start event's dispatch loop has not committed yet (genesis pending)";
          case BACKLOG_PENDING ->
              "it owns older quarantined entries that must be applied first (backlog pending)";
          case BEFORE_START ->
              "it has no row but owns a dead-letter record — its start could not produce an"
                  + " initial state";
        };
    return "Correlated event at offset "
        + eventOffset.value()
        + " was delivered to saga "
        + LogSanitizer.sanitizeForLog(sagaId.value())
        + " and HELD ["
        + reason
        + "] because "
        + why
        + ". This entry is the event's only surviving record. Once the blocker is gone, drain the"
        + " saga with SagaDeadLetterReplayer.replayAll(sagaId), which feeds it in offset order.";
  }
}
