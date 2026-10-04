package org.streamrune.runtime;

import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;

/**
 * Recorded as the cause of a saga dead-letter entry when a correlated event was delivered to a saga
 * that is {@link org.streamrune.core.saga.SagaStatus#FAULTED} and was therefore <em>not</em>
 * processed.
 *
 * <p>This is not a failure of the event or of the orchestrator: it is the durable record that the
 * event ARRIVED and was skipped. A FAULTED saga is quarantined pending an operator fix, not
 * finished, so the events that arrive during the FAULTED window are exactly the ones the saga still
 * needs. Before this entry existed they were dropped silently while the subscription checkpoint
 * advanced past them, so the eventual dead-letter replay of the original poison event re-drove the
 * saga from that one event and could never learn about the rest — a saga waiting forever for a
 * payment it had already received, until a timeout compensated a genuinely captured payment.
 *
 * <p>With the entry present, {@code SagaDeadLetterReplayer.replayAll(sagaId)} — which drains a
 * saga's entries oldest-offset-first, deferring row-less-skipped entries until the saga's genesis
 * entry has replayed and created the row (the genesis is saga-causally first even when a
 * crash/redelivery quarantined a skipped event at a LOWER global offset) — re-delivers the skipped
 * events after the original poison event, so the operator's existing recovery workflow converges
 * without any new step.
 *
 * <p>Deliberately NOT recorded for a {@linkplain org.streamrune.core.saga.SagaStatus#isTerminal()
 * terminal} saga: that saga is done by design and can never consume the event, so keeping a record
 * would strand it forever.
 *
 * <p>Package-private: only {@code SagaRunner} constructs it, and it is never thrown — it is a cause
 * object stamped onto the dead-letter entry.
 */
final class SagaSkippedWhileFaultedException extends RuntimeException {

  SagaSkippedWhileFaultedException(SagaId sagaId, GlobalOffset eventOffset) {
    super(
        "Correlated event at offset "
            + eventOffset.value()
            + " was delivered while saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + " is FAULTED, so it was NOT processed. The saga is quarantined pending an operator"
            + " fix, not finished — this entry is the event's only surviving record. Fix the"
            + " orchestrator, then drain the saga with SagaDeadLetterReplayer.replayAll(sagaId),"
            + " which replays the original poison event and then this one (the genesis entry"
            + " first even when this entry's offset is lower, then the rest oldest-first).");
  }
}
