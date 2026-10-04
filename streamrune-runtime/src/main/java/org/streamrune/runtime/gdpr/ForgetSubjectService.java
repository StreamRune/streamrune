package org.streamrune.runtime.gdpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.GdprAction;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.gdpr.ForgetResult.PurgeOutcome;

/**
 * Orchestrates GDPR Article 17 (right to erasure) in three ordered steps:
 *
 * <ol>
 *   <li><b>Crypto-shred</b> the subject's encryption key, after which every {@code @Encrypted}
 *       field tied to this subject deserializes as {@code "[REDACTED]"} via {@code
 *       CryptoShreddingModule}. Domain events stay in the event store (immutability preserved);
 *       only the key disappears.
 *   <li><b>Purge read models.</b> Crypto-shredding alone leaves the (now-undecryptable) ciphertext
 *       rows — and any unencrypted derived data — sitting in read models and projections. Each
 *       registered {@link SubjectDataPurger} deletes or redacts the subject's rows from one such
 *       source. Purgers run <em>after</em> the key deletion so they remove rows that are already
 *       cryptographically dead.
 *   <li><b>Evict the query cache</b>, when one is set ({@link Builder#queryCache}). A cached
 *       {@code @Cacheable} answer taken before the erasure may hold the subject's decrypted fields
 *       or a row a purger has just deleted; the eviction runs after the last purger, so neither is
 *       served afterwards.
 * </ol>
 *
 * <p><b>Completing an interrupted erasure: call {@code forget} again.</b> There is no durable job
 * behind a forget. If the process dies, or a step fails, anywhere between the engine's tombstone,
 * the key deletion, the audit rows and the purges, the remedy is to call {@code forget} again for
 * the same subject. The re-call repeats every step, and every step is safe on a state a partial run
 * left behind: {@code deleteKey} re-records the tombstone and deletes a key that is still there (a
 * no-op on one already gone), the audit log is append-only (a repeated row is one more attempt on
 * record), every purger is idempotent, and the eviction clears whatever is cached. A partial run
 * never makes the subject readable again: every shipped engine records the tombstone before, or in
 * the same transaction as, the key deletion. An erasure is incomplete while the audit log lacks,
 * for the subject's hash, a {@code GDPR_FORGET} SUCCESS row with no summary (the shred row)
 * followed by a {@code SUCCESS} row {@code purged read model '<name>'} for every registered purger,
 * or holds a {@code FAILURE} row {@code query-cache eviction failed: ...} newer than its last shred
 * row, or while a {@code forget} call ended in an exception, a {@link ForgetResult#fullyErased()}
 * of {@code false} or an Error carrying the crypto-shred-completed note. The convergence from every
 * crash point is pinned in {@code ForgetSubjectServiceCrashRecoveryTest}.
 *
 * <p>Stateless and thread-safe. Construct via {@link #builder()} and register as a singleton bean.
 * The purger list defaults to empty, so a service configured without purgers crypto-shreds only —
 * note this leaves ciphertext rows in read models; register a purger per read model holding subject
 * data for a complete erasure.
 *
 * <p><b>Audit failure semantics:</b> Unlike {@code AuditCommandInterceptor}, audit failures
 * encountered <em>after</em> a successful crypto-shred are <b>not</b> propagated — the operation is
 * irreversible and throwing would mislead callers into thinking it rolled back. The one exception
 * is an {@link Error}; see <i>After the shred</i> below. Audit failures during the FAILURE-path
 * write, of any kind, never mask the original crypto exception: they are attached to it as
 * suppressed exceptions.
 *
 * <p><b>Purge failure semantics:</b> A purger that throws does <b>not</b> abort the forget: the key
 * is already shredded (irreversible), so each purger's outcome is audited independently — SUCCESS
 * or FAILURE with the purger's name — and the remaining purgers still run. A failed purge leaves
 * that read model's rows behind; besides the FAILURE audit entry it increments {@code
 * streamrune.gdpr.purge_failed} (alert on it) and is reported in the returned {@link ForgetResult}.
 *
 * <p><b>After the shred, nothing stops the erasure.</b> Once {@code deleteKey} has returned, every
 * remaining step is attempted: the SUCCESS audit row, each purger's {@code purge}, its audit row,
 * the metric and the query-cache eviction. An exception from any of them (checked or unchecked) is
 * handled as described above. An {@link Error} is never swallowed, but it does not cut the erasure
 * short either: it is kept while the remaining steps run, and once every purger has been attempted
 * the first Error propagates. It carries a suppressed note saying the crypto-shred completed,
 * whether the SUCCESS audit row is missing, which purgers purged and which failed, and that a
 * re-run finishes the erasure; any later Error is attached to it as a suppressed exception. The
 * {@link ForgetResult} is lost with the throw, which is why the note carries the purge outcomes.
 *
 * <p><b>Purger names are fixed when the service is built.</b> {@link Builder#build()} reads every
 * purger's {@link SubjectDataPurger#name()} once, before any key can be deleted, and every forget
 * reports that purger's outcome, audit rows and metric tag under it; {@code forget} never calls
 * {@code name()}. The name must be non-blank, but it is the purger's own code: a {@code name()}
 * that throws an exception, or returns {@code null} or a blank string, does not stop the build. The
 * purger is named by its class instead (the simple name, or for an anonymous class its binary name
 * without the package), and a WARN names the class so the purger can be fixed. An {@link Error}
 * from {@code name()} propagates from {@code build()}. Names must be unique: {@code build()} throws
 * {@link IllegalArgumentException} naming every name two or more purgers share, including a class
 * name standing in for a missing one, since two purges under one name could not be told apart in
 * the {@link ForgetResult}, the audit trail or the metric. Every purger name the service renders
 * into a log line or an audit summary goes through {@link LogSanitizer#sanitizeForLog}; the {@link
 * ForgetResult} keeps the name as the purger gave it.
 *
 * <p><b>Programmatic outcome (do not report a completed erasure blindly):</b> {@link
 * #forget(SubjectId, UserId)} returns a {@link ForgetResult}. A caller (e.g. an HTTP endpoint
 * answering the data subject) MUST branch on {@link ForgetResult#fullyErased()} and must not report
 * a completed Article-17 erasure while it is {@code false} — unencrypted derived PII may still be
 * queryable in the read models named by {@link ForgetResult#failedPurgers()}, which the operator
 * should re-purge, or a cached query answer may still hold it when {@link
 * ForgetResult#queryCacheEvicted()} is {@code false}; a re-run of {@code forget} does both.
 *
 * <p><b>Reinstating a forgotten subject.</b> {@link #reinstate(SubjectId, UserId)} lifts the
 * terminal-erasure tombstone through {@link CryptoEngine#reinstate(SubjectId)} and records the
 * decision in the audit log as {@code GDPR_REINSTATE}, beside the {@code GDPR_FORGET} rows it
 * reverses. Reinstate through this service, not through the engine: the engine writes no audit
 * record, and on AWS KMS a reinstate makes the subject's pre-erasure ciphertext readable again.
 */
public final class ForgetSubjectService {

  /**
   * The {@code streamrune.gdpr.purge_failed} tag a failed query-cache eviction is counted under,
   * beside the purgers' own names.
   */
  public static final String QUERY_CACHE_METRIC_NAME = "query-cache";

  /** A registered purger and the name it was given when the service was built. */
  private record NamedPurger(String name, SubjectDataPurger purger) {}

  private final CryptoEngine cryptoEngine;
  private final GdprAuditWriter auditWriter;
  private final List<NamedPurger> purgers;
  private final StreamRuneMetrics metrics;
  private final CachingQueryBus queryCache;

  private ForgetSubjectService(
      CryptoEngine cryptoEngine,
      AuditStore auditStore,
      List<NamedPurger> purgers,
      StreamRuneMetrics metrics,
      CachingQueryBus queryCache) {
    this.cryptoEngine = cryptoEngine;
    this.auditWriter = new GdprAuditWriter(auditStore);
    this.purgers = List.copyOf(purgers);
    this.metrics = metrics == null ? StreamRuneMetrics.NOOP : metrics;
    this.queryCache = queryCache;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Crypto-shreds the subject's encryption key, then invokes every registered {@link
   * SubjectDataPurger} to remove the subject's rows from read models and projections. Idempotent
   * (per the {@link CryptoEngine#deleteKey(SubjectId)} and {@link
   * SubjectDataPurger#purge(SubjectId)} contracts). A subject that never had a key is still
   * tombstoned by every shipped engine, so a later {@code encrypt} for that id is refused until
   * {@link #reinstate(SubjectId, UserId)}: issue a forget only for subject ids known to exist.
   *
   * @param subjectId the data subject identifier; must not be null
   * @param requesterUserId who requested the forget (for audit); may be {@code null} for
   *     system-initiated operations (scheduled retention jobs, admin tools)
   * @return the erasure outcome: the key was deleted plus one outcome per registered purger. Check
   *     {@link ForgetResult#fullyErased()} before reporting a completed erasure — a {@code false}
   *     value means some read model may still hold the subject's PII.
   * @throws NullPointerException if {@code subjectId} is null
   * @throws RuntimeException re-thrown from {@link CryptoEngine#deleteKey(SubjectId)} on failure;
   *     audit FAILURE is written before re-throwing, and no purger runs (the key was not deleted).
   *     An {@link Error} from {@code deleteKey} is handled the same way: audited as FAILURE, then
   *     re-thrown unchanged. If the FAILURE audit write itself throws (anything, an Error
   *     included), the original failure is still the one re-thrown, carrying the audit failure as a
   *     suppressed exception.
   * @throws Error the first Error thrown after the key was deleted, by the SUCCESS audit write, a
   *     purger's {@code purge}, a per-purge audit write, the metrics backend or the query-cache
   *     eviction, re-thrown once every purger has been attempted and the cache evicted. The
   *     crypto-shred is then complete and irreversible; the Error carries a suppressed note saying
   *     so, naming the purgers that purged and those that failed, and saying whether the SUCCESS
   *     audit row is missing. Re-run {@code forget} (idempotent) to finish. An exception from those
   *     steps is not thrown (see the class javadoc).
   */
  public ForgetResult forget(SubjectId subjectId, UserId requesterUserId) {
    Objects.requireNonNull(subjectId, "subjectId must not be null");
    try {
      cryptoEngine.deleteKey(subjectId);
    } catch (Throwable e) {
      // Throwable, not RuntimeException : an Error from deleteKey (an
      // OutOfMemoryError, a LinkageError) is a failed forget too, and must leave a FAILURE row.
      // The rethrow is precise: deleteKey declares no checked exception, so e is unchecked.
      auditEngineFailure(GdprAction.FORGET, subjectId, requesterUserId, e);
      throw e;
    }
    // The key is gone for good. From here on no failure stops the erasure (see the class javadoc):
    // a persistently broken audit store throws at this SUCCESS row first, and if its Error skipped
    // the purgers, no read model would be purged until the store was fixed. Every Error is held in
    // postShredErrors and the first one is thrown below, after the last purger.
    //
    // Crash points, all after deleteKey returned, pinned in ForgetSubjectServiceTest: (1) this
    // SUCCESS write throws an Error (no SUCCESS row; the purgers still run); (2) a purger throws
    // one (FAILURE row, metric and failed outcome for it; the purgers after it still run); (3) a
    // per-purge audit row throws one (the purge itself is unaffected); (4) the purge_failed
    // metric throws one (the FAILURE row after it is still written). A re-run converges from each:
    // deleteKey is a no-op on a deleted key, the audit rows are written again (the audit log is
    // append-only, so a repeated SUCCESS row is just one more attempt on record) and every purger
    // runs again, which is a no-op on a read model it already purged. No purger's name() runs from
    // here on: every name was read when the service was built.
    PostShredErrors postShredErrors = new PostShredErrors();
    boolean successRowWritten =
        auditAfterShred(
            AuditOutcome.SUCCESS, subjectId, requesterUserId, null, "SUCCESS", postShredErrors);
    List<PurgeOutcome> purgeOutcomes = purgeReadModels(subjectId, requesterUserId, postShredErrors);
    // After the last purger, never before it: a query answered while a purge was running may have
    // cached the row that purge then deleted, and only an eviction after it removes that answer
    // (a load still in flight drops its own answer, see CachingQueryBus#evictAll).
    boolean queryCacheEvicted = evictQueryCache(subjectId, requesterUserId, postShredErrors);
    // After the last post-shred step, never before it (see PostShredErrors#record).
    postShredErrors.restoreInterrupt();
    if (postShredErrors.first != null) {
      Error error = postShredErrors.first;
      error.addSuppressed(CryptoShredCompletedNote.of(subjectId, successRowWritten, purgeOutcomes));
      throw error;
    }
    return new ForgetResult(subjectId, true, purgeOutcomes, queryCacheEvicted);
  }

  /**
   * Lifts the terminal-erasure tombstone of a forgotten subject through {@link
   * CryptoEngine#reinstate(SubjectId)}, so that {@code encrypt} may mint a key for it again, and
   * records the decision in the audit log as {@code GDPR_REINSTATE}: the hashed subject id, the
   * requester and the outcome, in the same {@code audit_log} as the {@code GDPR_FORGET} rows.
   *
   * <p>A reinstate reverses part of an erasure, so it must be a recorded decision. On PostgreSQL
   * and the filesystem the subject can re-register under the same id while its pre-erasure data
   * stays {@code [REDACTED]}; on AWS KMS the subject's pre-erasure ciphertext becomes readable
   * again, because the shared key was never destroyed; on Vault a reinstate after a completed
   * erasure is refused (see {@link CryptoEngine#reinstate(SubjectId)}). This method records who
   * asked; it does not decide whether they may, so authorize the caller first. Calling {@link
   * CryptoEngine#reinstate(SubjectId)} directly lifts the tombstone without any audit record.
   *
   * <p>The SUCCESS row is written after the engine has lifted the tombstone. If that write throws,
   * the failure propagates, carrying a suppressed note that says the tombstone is already lifted
   * and the SUCCESS row is missing; a WARN says the same. This differs from {@link #forget}, which
   * swallows an audit failure after its irreversible crypto-shred: a reinstate can be undone, and
   * its caller must not carry on as if it were on record. Re-run {@code reinstate} once the audit
   * store works (the engine's reinstate is a no-op on a subject that is no longer forgotten, so the
   * re-run only writes the missing row), or {@code forget} the subject again to restore the
   * erasure. A process that dies between the two steps leaves the same state, and the same re-run
   * records it.
   *
   * @param subjectId the forgotten data subject; must not be null
   * @param requesterUserId who decided the reinstate (for audit); may be {@code null} for
   *     system-initiated operations
   * @throws NullPointerException if {@code subjectId} is null; nothing is called or written
   * @throws RuntimeException re-thrown unchanged from {@link CryptoEngine#reinstate(SubjectId)} —
   *     for example the Vault engine's refusal after a completed erasure, or {@link
   *     UnsupportedOperationException} from an engine that records no tombstones — once a FAILURE
   *     row carrying its message is written. An {@link Error} from the engine is handled the same
   *     way. If the FAILURE row cannot be written, the engine's failure is still the one re-thrown,
   *     carrying the audit failure as a suppressed exception. Also thrown: the audit store's own
   *     failure (an Error included) when the SUCCESS row cannot be written after the tombstone was
   *     lifted, as described above.
   */
  public void reinstate(SubjectId subjectId, UserId requesterUserId) {
    Objects.requireNonNull(subjectId, "subjectId must not be null");
    try {
      cryptoEngine.reinstate(subjectId);
    } catch (Throwable e) {
      // Throwable for the same reason as forget: an Error from the engine is a failed reinstate
      // too. The rethrow is precise: reinstate declares no checked exception.
      auditEngineFailure(GdprAction.REINSTATE, subjectId, requesterUserId, e);
      throw e;
    }
    // The tombstone is lifted. Crash point: the process dies, or the audit store fails, before the
    // SUCCESS row lands, so the subject is reinstated but not on record. The audit failure
    // propagates (the caller must not carry on as if the reinstate were recorded), and a re-run
    // converges: the engine's reinstate is a no-op on a subject that is no longer forgotten, so
    // the re-run writes only the SUCCESS row. Pinned in ForgetSubjectServiceReinstateTest.
    try {
      auditWriter.write(
          GdprAction.REINSTATE, AuditOutcome.SUCCESS, subjectId, requesterUserId, null);
    } catch (Throwable auditFailure) {
      GdprAuditWriter.logger()
          .warn(
              "GDPR reinstate SUCCESS audit write failed for subject-hash={} (the tombstone is"
                  + " already lifted; re-run reinstate to record it): {}",
              subjectId.redacted(),
              auditFailure.toString());
      auditFailure.addSuppressed(ReinstateCompletedNote.of(subjectId));
      throw auditFailure;
    }
  }

  /**
   * Writes the FAILURE row for an engine call that threw, and never replaces {@code failure}: an
   * audit failure of any kind rides along on it as a suppressed exception and is logged.
   */
  private void auditEngineFailure(
      GdprAction action, SubjectId subjectId, UserId requesterUserId, Throwable failure) {
    try {
      auditWriter.write(
          action, AuditOutcome.FAILURE, subjectId, requesterUserId, failure.getMessage());
    } catch (Throwable auditFailure) {
      // Throwable here too: an Error from the audit store (an AssertionError, a LinkageError)
      // used to escape this catch and REPLACE the engine's failure, so the caller saw an
      // audit-store failure and lost the reason the operation failed. The audit failure rides
      // along on it instead. A store that rethrows the very instance it was handed must not turn
      // this into an IllegalArgumentException ("Self-suppression not permitted").
      if (auditFailure != failure) {
        failure.addSuppressed(auditFailure);
      }
      GdprAuditWriter.logger()
          .warn(
              "GDPR {} audit FAILURE write failed for subject-hash={}: {}",
              action.name().toLowerCase(Locale.ROOT),
              subjectId.redacted(),
              auditFailure.getMessage());
    }
  }

  /**
   * Invokes every registered purger and audits each outcome independently. A purger failure is
   * logged, audited, counted ({@code streamrune.gdpr.purge_failed}), and recorded in the returned
   * outcome list — never propagated from here (the key is already shredded) and never a reason to
   * skip the remaining purgers. That holds for an {@link Error} too, from the purger, its audit row
   * or the metric: it goes to {@code postShredErrors} for {@code forget} to throw after the loop.
   * The returned outcomes let {@code forget} report a partial erasure to the caller.
   */
  private List<PurgeOutcome> purgeReadModels(
      SubjectId subjectId, UserId requesterUserId, PostShredErrors postShredErrors) {
    List<PurgeOutcome> outcomes = new ArrayList<>(purgers.size());
    for (NamedPurger purger : purgers) {
      outcomes.add(purgeOne(purger, subjectId, requesterUserId, postShredErrors));
    }
    return outcomes;
  }

  /**
   * Runs one purger and audits its outcome; nothing it throws escapes (see purgeReadModels).
   *
   * <p>The name is the one read when the service was built. The outcome and the metric carry it as
   * the purger gave it (so a caller can match it to the purger it registered); the log lines and
   * audit summaries carry it through {@link LogSanitizer#sanitizeForLog}, the sink-side rule for an
   * identifier a purger chose, and the sink is the only place it is rewritten.
   */
  private PurgeOutcome purgeOne(
      NamedPurger purger,
      SubjectId subjectId,
      UserId requesterUserId,
      PostShredErrors postShredErrors) {
    String purgerName = purger.name();
    String loggedName = LogSanitizer.sanitizeForLog(purgerName);
    try {
      purger.purger().purge(subjectId);
    } catch (Throwable purgeFailure) {
      // Any Throwable is a failed purge, recorded like one. A checked exception thrown without
      // being
      // declared (a sneaky SQLException) is an ordinary failure, as a RuntimeException is. An
      // Error is that too, and is also held to be thrown once the loop is done.
      postShredErrors.record(purgeFailure);
      GdprAuditWriter.logger()
          .warn(
              "GDPR forget purge of read model '{}' failed for subject-hash={} "
                  + "(key already shredded; row data may remain): {}",
              loggedName,
              subjectId.redacted(),
              purgeFailure.toString());
      // Alert-worthy: a data subject may have been told "erased" while plaintext PII remains.
      // Guarded exactly like the audit row below. This is NOT a cheap increment —
      // MicrometerStreamRuneMetrics does live registry work here, which genuinely throws in
      // production (a registry rejecting a colliding meter id, a user MeterFilter, a registry
      // mid-shutdown). Unguarded, that escaped the loop after the key was already irreversibly
      // shredded, so purgers 2..n never ran and the caller could not tell this from "the shred
      // itself failed" — the one outcome this class's javadoc forbids.
      recordPurgeFailedMetric(purgerName, loggedName, postShredErrors);
      auditAfterShred(
          AuditOutcome.FAILURE,
          subjectId,
          requesterUserId,
          "purge of read model '" + loggedName + "' failed: " + purgeFailure.getMessage(),
          "purge FAILURE",
          postShredErrors);
      return new PurgeOutcome(purgerName, false, purgeFailure.getMessage());
    }
    auditAfterShred(
        AuditOutcome.SUCCESS,
        subjectId,
        requesterUserId,
        "purged read model '" + loggedName + "'",
        "purge SUCCESS",
        postShredErrors);
    return new PurgeOutcome(purgerName, true, null);
  }

  /**
   * Evicts every cached query answer, so none taken before this erasure is served after it (see
   * {@link Builder#queryCache}), and reports whether nothing cached can remain: {@code true} after
   * a successful eviction or when no query cache is set. Like every post-shred step, a failure
   * never escapes. It is handled like a failed purge: logged at WARN, counted in {@code
   * streamrune.gdpr.purge_failed} under {@link #QUERY_CACHE_METRIC_NAME}, recorded as a FAILURE
   * audit row {@code query-cache eviction failed: ...}, and reported as {@link
   * ForgetResult#queryCacheEvicted()} {@code false}; an Error is also held in {@code
   * postShredErrors}.
   */
  private boolean evictQueryCache(
      SubjectId subjectId, UserId requesterUserId, PostShredErrors postShredErrors) {
    if (queryCache == null) {
      return true;
    }
    try {
      queryCache.evictAll();
      return true;
    } catch (Throwable evictionFailure) {
      postShredErrors.record(evictionFailure);
      GdprAuditWriter.logger()
          .warn(
              "GDPR forget query-cache eviction failed for subject-hash={} (crypto-shred already"
                  + " done; cached query answers may still hold the subject's data until their"
                  + " TTL expires, re-run forget to evict them): {}",
              subjectId.redacted(),
              evictionFailure.toString());
      recordPurgeFailedMetric(QUERY_CACHE_METRIC_NAME, QUERY_CACHE_METRIC_NAME, postShredErrors);
      auditAfterShred(
          AuditOutcome.FAILURE,
          subjectId,
          requesterUserId,
          "query-cache eviction failed: " + evictionFailure.getMessage(),
          "query-cache eviction FAILURE",
          postShredErrors);
      return false;
    }
  }

  /**
   * Reads a purger's name once, for {@link Builder#build()}, without letting {@code name()} stop
   * the build. A name that is {@code null} or blank, or a {@code name()} that throws an exception,
   * breaks the contract of {@link SubjectDataPurger#name()}: the purger is named by its class name
   * instead, and a WARN says which purger lacks a name. An {@link Error} is not caught here; it
   * propagates from {@code build()}, before any key can be deleted.
   */
  private static String resolveName(SubjectDataPurger purger) {
    String name;
    try {
      name = purger.name();
    } catch (Exception nameFailure) {
      String fallback = GdprNames.classNameOf(purger);
      GdprAuditWriter.logger()
          .warn(
              "GDPR forget: SubjectDataPurger {} threw from name() ({}); it is still run, named by"
                  + " its class — give it a stable name",
              LogSanitizer.sanitizeForLog(fallback),
              nameFailure.toString());
      return fallback;
    }
    if (name == null || name.isBlank()) {
      String fallback = GdprNames.classNameOf(purger);
      GdprAuditWriter.logger()
          .warn(
              "GDPR forget: SubjectDataPurger {} returned a null or blank name(); it is still run,"
                  + " named by its class — give it a stable name",
              LogSanitizer.sanitizeForLog(fallback));
      return fallback;
    }
    return name;
  }

  /**
   * Bumps {@code streamrune.gdpr.purge_failed}; a metrics-backend failure never escapes. The
   * counter is the paging signal for an Article-17 gap, but losing it must never cost the remaining
   * purgers: the durable record of the same failure is the FAILURE audit row and the {@link
   * PurgeOutcome}, both written regardless. An exception is logged and swallowed; an Error is held
   * in {@code postShredErrors}.
   */
  private void recordPurgeFailedMetric(
      String purgerName, String loggedName, PostShredErrors postShredErrors) {
    try {
      metrics.recordGdprPurgeFailed(purgerName);
    } catch (Throwable metricFailure) {
      postShredErrors.record(metricFailure);
      GdprAuditWriter.logger()
          .warn(
              "Metrics recordGdprPurgeFailed failed for purger '{}' — the purge failure is still"
                  + " audited and reported, but streamrune.gdpr.purge_failed did not move for it",
              loggedName,
              metricFailure);
    }
  }

  /**
   * Writes one audit row after the crypto-shred and reports whether the write went through. A
   * failed write never escapes: an exception is logged and swallowed (the shred is irreversible,
   * and a throw would read as an erasure that rolled back), an Error is held in {@code
   * postShredErrors} for {@code forget} to throw once every purger has run.
   *
   * @param row names the row in the warning, e.g. {@code "SUCCESS"} or {@code "purge FAILURE"}
   */
  private boolean auditAfterShred(
      AuditOutcome outcome,
      SubjectId subjectId,
      UserId requesterUserId,
      String summary,
      String row,
      PostShredErrors postShredErrors) {
    try {
      auditWriter.write(GdprAction.FORGET, outcome, subjectId, requesterUserId, summary);
      return true;
    } catch (Throwable auditFailure) {
      postShredErrors.record(auditFailure);
      GdprAuditWriter.logger()
          .warn(
              "GDPR forget {} audit write failed for subject-hash={} (crypto-shred already"
                  + " done): {}",
              row,
              subjectId.redacted(),
              auditFailure.toString());
      return false;
    }
  }

  /**
   * The {@link Error}s thrown after the crypto-shred of one {@code forget}. None of them stops the
   * erasure: each is held here while the remaining steps run, and {@code forget} throws the first
   * once every purger has been attempted. Any later one is attached to it as a suppressed
   * exception. Every other Throwable is an exception (JLS 11.1.1), logged where it is caught.
   */
  private static final class PostShredErrors {

    private Error first;
    private boolean interrupted;

    /**
     * Called with every Throwable a post-shred step threw. Holds an Error; for an undeclared
     * InterruptedException, which is swallowed like any other exception here, it remembers the
     * interrupt so {@link #restoreInterrupt()} can set the flag again once every post-shred step
     * has run. Not at once: on a virtual thread, blocking socket I/O with the flag set closes the
     * socket, so every later JDBC-backed purger and audit write would fail and the erasure would be
     * cut short after all.
     */
    void record(Throwable failure) {
      if (failure instanceof InterruptedException) {
        interrupted = true;
        return;
      }
      if (!(failure instanceof Error error)) {
        return;
      }
      if (first == null) {
        first = error;
      } else if (error != first && !List.of(first.getSuppressed()).contains(error)) {
        // The same instance can come back: an audit store failing every write with one Error, or
        // the JVM's preallocated OutOfMemoryError. Suppressing it on itself would throw an
        // IllegalArgumentException that replaces it, and carrying it twice says nothing new.
        first.addSuppressed(error);
      }
    }

    /** Sets the interrupt flag again if a post-shred step swallowed an InterruptedException. */
    void restoreInterrupt() {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Attached as a suppressed exception to the {@link Error} that {@code forget} throws after {@link
   * CryptoEngine#deleteKey(SubjectId)} returned. The Error propagates (an Error is never
   * swallowed), but the erasure it interrupted is not a failed one: the key is gone and cannot come
   * back, and every purger has been attempted. This note says so wherever the Error is printed,
   * says what is left (a missing SUCCESS audit row, the purgers that failed), and says how to
   * finish the erasure. Its message names the subject by its hash only, never the raw id. A note,
   * not a failure, so it carries no stack trace.
   */
  private static final class CryptoShredCompletedNote extends RuntimeException {

    private CryptoShredCompletedNote(String message) {
      super(message, null, false, false);
    }

    static CryptoShredCompletedNote of(
        SubjectId subjectId, boolean successRowWritten, List<PurgeOutcome> purgeOutcomes) {
      StringBuilder message =
          new StringBuilder("GDPR forget for subject-hash=")
              .append(subjectId.redacted())
              .append(
                  ": the crypto-shred completed before this failure and is irreversible (the key"
                      + " is deleted).");
      if (!successRowWritten) {
        message.append(" Its SUCCESS audit row was not written.");
      }
      if (purgeOutcomes.isEmpty()) {
        message.append(" No read-model purger is registered.");
      } else {
        List<String> purged = names(purgeOutcomes, true);
        List<String> failed = names(purgeOutcomes, false);
        if (failed.isEmpty()) {
          message
              .append(" Every read-model purger was still attempted and succeeded: ")
              .append(purged)
              .append('.');
        } else {
          message
              .append(" Every read-model purger was still attempted: purged ")
              .append(purged)
              .append("; failed ")
              .append(failed)
              .append(", whose read models may still hold derived PII.");
        }
        message.append(
            " Any per-purge audit row or purge_failed increment whose write threw is missing.");
      }
      message.append(
          " To finish the erasure, re-run forget for this subject once the cause is fixed: it is"
              + " idempotent (deleteKey is a no-op on a deleted key, and every purger is"
              + " idempotent), writes the audit rows and runs every purger again.");
      return new CryptoShredCompletedNote(message.toString());
    }

    private static List<String> names(List<PurgeOutcome> outcomes, boolean succeeded) {
      return outcomes.stream()
          .filter(o -> o.succeeded() == succeeded)
          .map(o -> LogSanitizer.sanitizeForLog(o.purgerName()))
          .toList();
    }
  }

  /**
   * Attached as a suppressed exception to the audit failure that {@code reinstate} throws when the
   * SUCCESS row could not be written after {@link CryptoEngine#reinstate(SubjectId)} returned. The
   * failure propagates, but the reinstate it follows took effect: the tombstone is lifted. This
   * note says so wherever the failure is printed, and says how to put the reinstate on record or
   * undo it. Its message names the subject by its hash only, never the raw id. A note, not a
   * failure, so it carries no stack trace.
   */
  private static final class ReinstateCompletedNote extends RuntimeException {

    private ReinstateCompletedNote(String message) {
      super(message, null, false, false);
    }

    static ReinstateCompletedNote of(SubjectId subjectId) {
      return new ReinstateCompletedNote(
          "GDPR reinstate for subject-hash="
              + subjectId.redacted()
              + ": the terminal-erasure tombstone was lifted before this failure, so the reinstate"
              + " took effect (encrypt may mint a key for this subject again), but its SUCCESS"
              + " audit row was not written. To put it on record, re-run reinstate for this"
              + " subject once the audit store works: the engine's reinstate is a no-op on a"
              + " subject that is no longer forgotten, so the re-run writes only the missing row."
              + " To undo it instead, forget the subject again.");
    }
  }

  /** Builder for {@link ForgetSubjectService}. */
  public static final class Builder {
    private CryptoEngine cryptoEngine;
    private AuditStore auditStore;
    private List<SubjectDataPurger> purgers = List.of();
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private CachingQueryBus queryCache;

    private Builder() {}

    /** Required. */
    public Builder cryptoEngine(CryptoEngine cryptoEngine) {
      this.cryptoEngine = cryptoEngine;
      return this;
    }

    /** Optional. {@code null} disables audit writes. */
    public Builder auditStore(AuditStore auditStore) {
      this.auditStore = auditStore;
      return this;
    }

    /**
     * Sets the read-model purgers invoked after the key is deleted, one per read model holding the
     * subject's data. Defaults to empty (crypto-shred only, which leaves ciphertext rows in read
     * models). The list is defensively copied when the service is built. Each purger needs a name
     * of its own (see {@link #build()}).
     *
     * @param purgers the purgers to invoke; must not be {@code null} (use an empty list to disable)
     */
    public Builder purgers(List<SubjectDataPurger> purgers) {
      this.purgers = Objects.requireNonNull(purgers, "purgers must not be null");
      return this;
    }

    /**
     * Sets the query cache that every forget evicts once its last purger has run. Optional; {@code
     * null} (the default) evicts nothing.
     *
     * <p>A {@code @Cacheable} query answered before the erasure holds what the subject's data
     * looked like then: a decrypted {@code @Encrypted} field, or a read-model row a purger is about
     * to delete. Without this eviction the cache serves that answer until the entry's TTL runs out,
     * after the key is shredded and the read model purged. The forget evicts every cached query
     * type, in every caller's partition, because the service cannot know which answers hold the
     * subject. The integrations wire their auto-configured {@link CachingQueryBus} here.
     *
     * <p>The cache lives in this process only: other instances of the application keep their
     * answers until their TTL runs out (see the GDPR guide).
     *
     * @param queryCache the caching query bus to evict, or {@code null} for none
     */
    public Builder queryCache(CachingQueryBus queryCache) {
      this.queryCache = queryCache;
      return this;
    }

    /**
     * Sets the metrics collector so a failed read-model purge increments {@code
     * streamrune.gdpr.purge_failed}. Optional; defaults to {@link StreamRuneMetrics#NOOP} (null →
     * NOOP).
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    /**
     * Builds the service. Reads every purger's {@code name()} once; each forget reports that purger
     * under it (see the class javadoc). No key is deleted, nothing is purged or audited here.
     *
     * @throws NullPointerException if no crypto engine was set, or the purger list holds a {@code
     *     null}
     * @throws IllegalArgumentException if two or more purgers go by one name, a class name standing
     *     in for a missing one included; the message names each shared name and the classes sharing
     *     it
     * @throws Error an {@link Error} from a purger's {@code name()}, unchanged
     */
    public ForgetSubjectService build() {
      Objects.requireNonNull(cryptoEngine, "cryptoEngine is required");
      List<SubjectDataPurger> registered = List.copyOf(purgers);
      List<String> names = registered.stream().map(ForgetSubjectService::resolveName).toList();
      GdprNames.requireUnique(
          "SubjectDataPurger",
          "each one names its purge in the ForgetResult, the GDPR audit trail and the purge_failed"
              + " metric, where two purgers under one name could not be told apart",
          registered,
          names);
      List<NamedPurger> named = new ArrayList<>(registered.size());
      for (int i = 0; i < registered.size(); i++) {
        named.add(new NamedPurger(names.get(i), registered.get(i)));
      }
      return new ForgetSubjectService(cryptoEngine, auditStore, named, metrics, queryCache);
    }
  }
}
