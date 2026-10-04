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
