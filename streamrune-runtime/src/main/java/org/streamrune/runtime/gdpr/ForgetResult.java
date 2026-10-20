package org.streamrune.runtime.gdpr;

import java.util.List;
import java.util.Objects;
import org.streamrune.core.types.SubjectId;

/**
 * The programmatic outcome of a GDPR {@linkplain ForgetSubjectService#forget(SubjectId,
 * org.streamrune.core.types.UserId) forget}. It exists so a caller (an HTTP endpoint answering the
 * data subject) can tell a COMPLETE erasure from a PARTIAL one instead of assuming success from a
 * void return.
 *
 * <p>The subject's key is crypto-shredded first and irreversibly; if that step throws, {@code
 * forget} propagates and no {@code ForgetResult} is produced. Once a result is returned {@link
 * #keyDeleted()} is therefore always {@code true}. Each registered read-model purger then runs and
 * contributes one {@link PurgeOutcome}. A purger failure does NOT abort the others and does NOT
 * propagate (the key is already gone) — it is reported here instead, so the caller can retry the
 * failed purgers and, crucially, must NOT report a completed erasure to the data subject while
 * {@link #fullyErased()} is {@code false} (unencrypted derived PII may still be queryable — an
 * Article-17 gap). The one exception is an {@link Error} thrown after the key was deleted: every
 * purger still runs, then the Error propagates and no result is returned; a suppressed note on it
 * carries the same outcomes (see {@link ForgetSubjectService#forget(SubjectId,
 * org.streamrune.core.types.UserId) forget}).
 *
 * @param subjectId the data subject that was forgotten
 * @param keyDeleted whether the encryption key was crypto-shredded (always {@code true} for a
 *     returned result — a key-deletion failure propagates instead)
 * @param purgeOutcomes one entry per registered {@code SubjectDataPurger}, in invocation order
 * @param queryCacheEvicted whether the query cache was evicted after the last purger: {@code true}
 *     when the eviction succeeded or no query cache is set, {@code false} when the eviction threw
 *     (cached answers taken before the erasure may then be served until their TTL runs out)
 */
public record ForgetResult(
    SubjectId subjectId,
    boolean keyDeleted,
    List<PurgeOutcome> purgeOutcomes,
    boolean queryCacheEvicted) {

  public ForgetResult {
    Objects.requireNonNull(subjectId, "subjectId");
    purgeOutcomes = List.copyOf(purgeOutcomes);
  }

  /** The outcome of one read-model purge. */
  public record PurgeOutcome(String purgerName, boolean succeeded, String detail) {
    public PurgeOutcome {
      Objects.requireNonNull(purgerName, "purgerName");
    }
  }

  /**
   * {@code true} only when the key was deleted, every purger succeeded AND the query cache was
   * evicted — i.e. no derived PII is known to remain. When this is {@code false}, the caller must
   * not tell the data subject the erasure completed.
   *
   * <p>It describes the moment {@code forget} returned. It does not cover a projection page that
   * was decrypted before the key was deleted and is written after the purgers ran: neither the key
   * deletion nor a purger reaches data a projection holds in memory. A projection closes that
   * window by deleting the subject's rows when it handles an event that says the subject is
   * forgotten, provided no later event carries the subject's data (the subject's decider refuses
   * commands once it is forgotten; for the subject's data on other streams, append that event after
   * {@code forget} returns, or let the projection treat the subject as forgotten from that event
   * on), or the application calls {@code forget} again once every such projection's checkpoint has
   * passed the head of the global stream read after the forget (see the GDPR guide). It does not
   * cover a peer replica whose decrypt cache still holds the subject's plaintext until the forget
   * signal evicts it: a snapshot or projection page built there from that cache holds the subject's
   * data. Request identities ({@code userId} in event metadata, the audit logs and the dead-letter
   * queue) are outside erasure by design.
   */
  public boolean fullyErased() {
    return keyDeleted
        && queryCacheEvicted
        && purgeOutcomes.stream().allMatch(PurgeOutcome::succeeded);
  }

  /** Names of the purgers that failed (read models that may still hold the subject's PII). */
  public List<String> failedPurgers() {
    return purgeOutcomes.stream()
        .filter(o -> !o.succeeded())
        .map(PurgeOutcome::purgerName)
        .toList();
  }
}
