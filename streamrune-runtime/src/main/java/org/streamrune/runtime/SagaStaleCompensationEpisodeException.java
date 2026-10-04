package org.streamrune.runtime;

import java.time.Duration;
import java.time.Instant;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.LogSanitizer;

/**
 * Signals that the event path refused to resume a {@code COMPENSATING} episode whose command-inbox
 * dedup keys cannot be proven younger than the configured inbox retention window.
 *
 * <p>Effectively-once compensation across a resume relies on a succeeded compensation command's
 * inbox row surviving until the episode terminalizes: the durable {@code episode_version}
 * deliberately re-derives the ORIGINAL {@code episodeCompensationKey}, so a resume re-dispatches
 * under that key and the inbox swallows it. But the inbox never refreshes {@code processed_at} on a
 * dedup hit, so once {@code InboxRetentionSweeper} prunes the row — after an outage longer than the
 * window, for instance — the re-dispatch MISSES and the compensation runs a second time. A real
 * double refund.
 *
 * <p>The three out-of-band re-drivers already refuse this dispatch on the same durable {@code
 * episode_claimed_at} anchor: {@code SagaCompensationRetrySweeper} and {@code
 * SagaTimeoutRunner.faultStaleEpisode} CAS-FAULT without dispatching, and {@code
 * SagaDeadLetterReplayer} returns {@code STALE_COMPENSATION_BLOCKED}. The event-path resume is the
 * fourth member of that set — and the only one that runs on EVERY replica with no leadership gate,
 * so it was the likeliest to fire.
 *
 * <p>It is the one member with a triggering event, so instead of a bare CAS-FAULT it is raised as a
 * poison-class fault: {@code SagaRunner} quarantines the event (preserving it — see {@code
 * SagaSkippedWhileFaultedException} for why a dropped event is unrecoverable) and marks the saga
 * {@code FAULTED}, which is exactly the siblings' end state plus a durable record. The entry
 * carries {@code preFaultStatus = COMPENSATING}, so a later plain replay is refused by the
 * replayer's own {@code STALE_COMPENSATION_BLOCKED} guard until the operator reconciles the partial
 * compensation and forces. The forced feed carries that acknowledgement to the executor, which then
 * resumes the episode at-least-once instead of raising this again.
 *
 * <p>The operator's {@code SagaDeadLetterReplayer.resumeFaulted} (a give-up FAULTED episode, no
 * triggering event) meets the same guard: there is nothing to quarantine and the row is already
 * FAULTED, so the executor throws this bare — no dispatch, no write — and the replayer reports
 * {@code STALE_COMPENSATION_BLOCKED}; a forced resume passes it exactly like a forced replay.
 *
 * <p>The saga id in the message goes through {@link LogSanitizer#sanitizeForLog}: the message is
 * stored on the dead-letter entry and reaches log records, and a saga id is a business id the
 * application chose.
 *
 * <p>Package-private: only {@link SagaStepExecutor} constructs it.
 */
final class SagaStaleCompensationEpisodeException extends RuntimeException {

  SagaStaleCompensationEpisodeException(
      SagaId sagaId, Instant claimInstant, Duration age, Duration inboxRetentionMaxAge) {
    super(
        "Saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + " has dwelled COMPENSATING for "
            + age
            + " (claimed at "
            + claimInstant
            + ") — past the command-inbox retention window ("
            + inboxRetentionMaxAge
            + ") — so its episode's succeeded-compensation dedup keys may already be pruned and a"
            + " resume could RE-EXECUTE an already-succeeded compensation (double refund). The"
            + " triggering event was quarantined and the saga marked FAULTED WITHOUT dispatching"
            + " anything. Reconcile the partial compensation manually, then replay the quarantined"
            + " event with force=true if it is safe to resume.");
  }
}
