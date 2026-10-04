package org.streamrune.runtime;

import org.streamrune.core.EventEnvelope;

/**
 * The reason a saga step is being executed: a forward-path event delivery, a resumed compensation
 * after a prior failure, an operator's resume of a faulted compensation episode, an operator's
 * compensation of a forward-faulted saga, or a claim-timeout reclaim.
 */
sealed interface SagaTrigger {

  /**
   * A forward-path event delivery.
   *
   * @param event the triggering event envelope
   * @param startPath {@code true} if this event starts the saga
   * @param replayRedrive {@code true} if this delivery is a replay redrive rather than a live
   *     delivery
   * @param sagaOwnsDeadLetterRecord {@code true} when the saga is known to own a dead-letter record
   *     of this type at trigger time, so a row the executor CREATES for it is born shielded. {@code
   *     true} by construction on the replay channel (the entry being fed exists); the runner's
   *     own-record lookup on a live start over an absent row; irrelevant otherwise — only the two
   *     genesis-path creates read it
   * @param operatorForced {@code true} only on a replay feed an operator FORCED ({@code
   *     SagaDeadLetterReplayer.replay(sagaId, offset, true)} / {@code replayAll(sagaId, true)}):
   *     the operator's explicit, per-saga acknowledgement of at-least-once, given after reconciling
   *     what actually executed downstream. The executor reads it at exactly one place — its
   *     event-path key-age guard on a {@code COMPENSATING} resume ({@code
   *     SagaStepExecutor.requireFreshEpisodeKeys}), which then resumes the stale episode loudly
   *     instead of refusing it. A live delivery can never carry it: the compact constructor rejects
   *     {@code operatorForced} without {@code replayRedrive}, the constructors the live paths use
   *     leave it off, and the only caller that sets it is {@code SagaRunner.feedReplay}
   */
  record ForwardStep(
      EventEnvelope event,
      boolean startPath,
      boolean replayRedrive,
      boolean sagaOwnsDeadLetterRecord,
      boolean operatorForced)
      implements SagaTrigger {

    // public: a record nested in an interface is implicitly public, and its canonical constructor
    // may not be less accessible than the record. SagaTrigger itself is package-private.
    public ForwardStep {
      if (operatorForced && !replayRedrive) {
        throw new IllegalArgumentException(
            "operatorForced is an operator's replay acknowledgement (SagaDeadLetterReplayer"
                + " force); a live delivery (replayRedrive = false) can never carry it");
      }
    }

    /** A step no operator forced — the live start path passes its own-record lookup through it. */
    ForwardStep(
        EventEnvelope event,
        boolean startPath,
        boolean replayRedrive,
        boolean sagaOwnsDeadLetterRecord) {
      this(event, startPath, replayRedrive, sagaOwnsDeadLetterRecord, false);
    }

    /**
     * The three-argument form: a replay feed owns its record by construction; a live delivery is
     * assumed not to. {@code SagaRunner} passes its own lookup explicitly for a live start.
     */
    ForwardStep(EventEnvelope event, boolean startPath, boolean replayRedrive) {
      this(event, startPath, replayRedrive, replayRedrive);
    }
  }

  /**
   * A resumed compensation after a prior failure.
   *
   * @param resumeCause the cause that triggered the compensation resume
   */
  record ResumeCompensation(Throwable resumeCause) implements SagaTrigger {}

  /**
   * The operator's resume of a compensation episode the compensation-retry sweeper or the timeout
   * runner gave up on ({@code SagaDeadLetterReplayer.resumeFaulted}): the row is {@code FAULTED}
   * with {@code pre_fault_status = COMPENSATING} and there is no triggering event. The executor
   * resumes the episode exactly as {@link ResumeCompensation} does — at the row's current version,
   * under the durable episode keys, {@code catchPoison = true} — after the same key-age guard a
   * {@link ForwardStep} resume meets.
   *
   * @param operatorForced the operator's {@code force}: the explicit at-least-once acknowledgement
   *     that lets the executor's key-age guard resume a stale episode instead of refusing it. Set
   *     only by {@code SagaRunner.resumeFaultedEpisode}, which only the replayer calls
   */
  record ResumeFaultedEpisode(boolean operatorForced) implements SagaTrigger {}

  /**
   * The operator's compensation of a saga a forward step faulted ({@code
   * SagaDeadLetterReplayer.compensateFaulted}): the row is {@code FAULTED} with a forward {@code
   * pre_fault_status} ({@code NULL}, {@code STARTED} or {@code RUNNING}) and has no dead-letter
   * entry left to drain, so nothing else can ever advance it. The executor claims a fresh
   * compensation episode exactly as {@link ClaimTimeout} does — a compare-and-swap to {@code
   * COMPENSATING} at the faulted row's version, which stamps the episode and clears {@code
   * pre_fault_status} — and compensates from the persisted state with {@code catchPoison = true}.
   * Set only by {@code SagaRunner.compensateForwardFault}, which only the replayer calls.
   */
  record CompensateForwardFault() implements SagaTrigger {}

  /**
   * A claim-timeout reclaim.
   *
   * @param timeoutCause the cause associated with the claim timeout
   */
  record ClaimTimeout(Throwable timeoutCause) implements SagaTrigger {}
}
