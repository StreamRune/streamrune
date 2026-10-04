package org.streamrune.core.saga;

import org.streamrune.core.types.LogSanitizer;

/**
 * Thrown when the saga runtime meets a compensation episode whose row carries no durable episode
 * stamp — a {@code COMPENSATING} row, or a {@code FAULTED} row whose {@code pre_fault_status} is
 * {@code COMPENSATING}, with {@code episode_version} or {@code episode_claimed_at} {@code null}.
 *
 * <p>Every write that lands a row at {@code COMPENSATING} stamps the episode identity in the same
 * statement — {@link SagaStore#claimCompensating} and, for an evolved {@code SagaState.status()} of
 * {@code COMPENSATING}, {@link SagaStore#applyEvent}, {@link SagaStore#createGenesisPending},
 * {@link SagaStore#create} and {@link SagaStore#update} — and no later write touches it (the
 * episode stamp rule, see {@link SagaStore#claimCompensating}). A conforming store therefore never
 * produces such a row: {@code SagaStoreContract} pins the rule for every write of every
 * implementation, and {@code PostgresSagaStore} additionally refuses the row at the schema (a CHECK
 * constraint on {@code saga_state}). The stamp is what makes a resume safe — the compensation
 * idempotency keys are scoped by {@code episode_version}, and the key-age guards measure from
 * {@code episode_claimed_at}. Without it the runtime would have to guess both: re-derive the keys
 * from a row version that a fault→replay cycle has moved (re-executing a committed compensation — a
 * double refund) and measure key age from a last-write instant the fault itself refreshed. It no
 * longer guesses; the episode is refused.
 *
 * <p>Where it surfaces, nothing is dispatched: the event-path resume raises it as poison (the
 * triggering event is quarantined and the saga marked {@code FAULTED}); {@code
 * SagaDeadLetterReplayer.replay}, {@code replayAll} and {@code resumeFaulted} throw it before any
 * anchor, feed or write; the compensation-retry sweeper and the timeout runner log it and leave the
 * row for the next cycle. The remedy is the store: fix the implementation that left the row
 * unstamped (or repair the row by hand with the version the episode was entered at), then resume.
 *
 * <p>It extends {@link IllegalStateException}, like {@link SagaTypeCollisionException}: a store
 * that breaks its contract is a programming error, not a recoverable conflict. The saga id in the
 * message goes through {@link LogSanitizer#sanitizeForLog(String)} — the message reaches log
 * records and dead-letter entries, and a saga id is a business id the application chose; {@link
 * #sagaId()} keeps the raw value.
 */
public final class SagaUnstampedCompensationEpisodeException extends IllegalStateException {

  private final transient SagaId sagaId;

  /**
   * @param sagaId the saga whose compensation episode is unstamped
   * @param detail what the row carries, e.g. {@code "episode_version=null,
   *     episode_claimed_at=null"}
   */
  public SagaUnstampedCompensationEpisodeException(SagaId sagaId, String detail) {
    super(
        "Saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + " is in a compensation episode but its row carries no durable episode stamp ("
            + detail
            + "): the SagaStore violated its contract — every write that lands a row at COMPENSATING"
            + " must stamp episode_version and episode_claimed_at in the same statement (see"
            + " SagaStore.claimCompensating). Nothing was dispatched: the compensation idempotency"
            + " keys and the key-age anchor cannot be derived from a row version or a last-write"
            + " instant without re-executing a committed compensation. Fix the store (or repair the"
            + " row), then resume the episode.");
    this.sagaId = sagaId;
  }

  /** For a loaded row: names both halves of the stamp as the row carries them. */
  public static SagaUnstampedCompensationEpisodeException ofRow(SagaId sagaId, LoadedSaga<?> row) {
    return new SagaUnstampedCompensationEpisodeException(
        sagaId,
        "episode_version="
            + row.episodeVersion()
            + ", episode_claimed_at="
            + row.episodeClaimedAt());
  }

  /**
   * For a {@link SagaStore.CompensatingSaga} the sweeper enumerated without a claim instant (the
   * enumeration surfaces only {@code episode_claimed_at}).
   */
  public static SagaUnstampedCompensationEpisodeException ofEnumeration(SagaId sagaId) {
    return new SagaUnstampedCompensationEpisodeException(
        sagaId, "episode_claimed_at=null, as surfaced by SagaStore.findCompensating");
  }

  /** The saga whose compensation episode is unstamped (raw, unsanitized). */
  public SagaId sagaId() {
    return sagaId;
  }
}
