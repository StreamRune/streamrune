package org.streamrune.runtime;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.Auditable;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;

/**
 * A {@link QueryBus} decorator that logs dispatches of {@link Auditable}-annotated queries to an
 * {@link AuditStore}.
 *
 * <p>Queries without {@code @Auditable} pass through without auditing. Audit entries reuse the
 * existing {@code audit_log} table with {@code commandId=null} (a query is not a command), {@code
 * aggregateId=null}, and {@code eventCount=0}. The correlation id of the row is the one the
 * dispatch runs under, tying the row to the originating business flow: the bound {@link
 * StreamRuneContext}'s, or — for a query dispatched with a saga origin bound, inside a saga's
 * dispatch or a continuation — that origin's reserved one, as the command audit records it. It is
 * {@code null} when neither is bound.
 *
 * <p><strong>A reserved correlation id.</strong> A query dispatched under a correlation id with the
 * reserved {@code streamrune:} prefix that is not the bound saga origin's is refused with a {@link
 * org.streamrune.core.ReservedCorrelationIdException} before anything is recorded — no audit row,
 * no metric, no delegate call — as the command bus refuses such a command ({@link
 * StreamRuneContext#requireNoForeignReservedCorrelationId()}). So a row this bus writes carries a
 * reserved correlation id only for a query that ran with that saga's origin bound. The refusal is
 * logged once at {@code WARN}, without the bound value.
 *
 * <p>When a {@link StreamRuneMetrics} is supplied, every dispatch records {@code
 * queries.dispatched} and {@code queries.duration} tagged with the query type (auditable or not).
 * Wire metrics into exactly one bus in a decorator chain to avoid double-counting.
 *
 * <p><strong>Audit-store failure semantics (fail-open, matching the command path).</strong> Each
 * {@code auditStore.save} is isolated in its own try/catch so an audit-store outage never changes
 * the query outcome. On the <em>failure</em> path, a failing FAILURE-row save is attached to the
 * business exception via {@link Throwable#addSuppressed} and the ORIGINAL query exception still
 * propagates — the audit error can never mask the real failure. On the <em>success</em> path, a
 * failing SUCCESS-row save is logged and swallowed and the result is returned; a successful query
 * is never misrouted into a false {@code FAILURE} audit row. This mirrors {@code
 * VirtualThreadCommandBus.notifyAfter/notifyOnError}, where post-outcome audit failures are
 * logged-and-swallowed with the original command outcome preserved.
 */
public final class AuditingQueryBus implements QueryBus {

  private static final Logger LOG = LoggerFactory.getLogger(AuditingQueryBus.class);

  private final QueryBus delegate;
  private final AuditStore auditStore;
  private final StreamRuneMetrics metrics;
  private final Map<Class<?>, Boolean> auditableCache = new ConcurrentHashMap<>();

  public AuditingQueryBus(QueryBus delegate, AuditStore auditStore) {
    this(delegate, auditStore, null);
  }

  /**
   * Creates an auditing bus with an optional metrics collector.
   *
   * @param metrics the metrics collector; {@code null} disables metrics (behavior unchanged)
   */
  public AuditingQueryBus(QueryBus delegate, AuditStore auditStore, StreamRuneMetrics metrics) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.auditStore = Objects.requireNonNull(auditStore, "auditStore");
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
  }

  @Override
  public <R> R dispatch(Query<R> query) {
    // Before anything is recorded: an audit row written here carries the bound correlation id, and
    // a reserved one that is not a bound saga origin's would mark the row as a saga's work.
    BoundCorrelationId.refuseForeignReserved(LOG, "query", query.getClass());
    String queryType = query.getClass().getSimpleName();
    recordMetric(() -> metrics.recordQueryDispatched(queryType), "recordQueryDispatched");
    long start = System.nanoTime();
    try {
      return dispatchAudited(query, queryType);
    } finally {
      long duration = System.nanoTime() - start;
      recordMetric(() -> metrics.recordQueryDuration(queryType, duration), "recordQueryDuration");
    }
  }

  private <R> R dispatchAudited(Query<R> query, String queryType) {
    if (!isAuditable(query.getClass())) {
      return delegate.dispatch(query);
    }
    UserId userId = currentUserId();
    CorrelationId correlationId = currentCorrelationId();
    Instant now = Instant.now();
    R result;
    try {
      result = delegate.dispatch(query);
    } catch (RuntimeException ex) {
      // Failure path. Persisting the FAILURE audit row must never MASK the real business exception:
      // if the audit store is also down (typically the same outage), attach its error as suppressed
      // and rethrow the ORIGINAL query failure — the analog of the command path, where
      // VirtualThreadCommandBus.notifyOnError logs-and-swallows so "original failure still
      // propagates".
      try {
        auditStore.save(
            new AuditEntry(
                null,
                queryType,
                null,
                null,
                userId,
                now,
                AuditOutcome.FAILURE,
                // Persisted free text, sanitized at the audit_log sink; the caller still
                // receives ex itself, untouched.
                LogSanitizer.sanitizeFreeText(ex.getMessage()),
                0,
                correlationId));
      } catch (RuntimeException saveEx) {
        ex.addSuppressed(saveEx);
        LOG.warn(
            "Audit FAILURE save for query {} failed; original query failure still propagates",
            queryType,
            saveEx);
      }
      throw ex;
    }
    // Success path. The query already succeeded, so a failing SUCCESS-audit save must NOT be
    // misrouted through the failure branch (which would fabricate a false FAILURE row AND surface a
    // 500 for a successful query). Fail-open — log and return the result — matching the command
    // path (VirtualThreadCommandBus.notifyAfter: "command result is unaffected").
    try {
      auditStore.save(
          new AuditEntry(
              null,
              queryType,
              null,
              null,
              userId,
              now,
              AuditOutcome.SUCCESS,
              null,
              0,
              correlationId));
    } catch (RuntimeException saveEx) {
      LOG.warn(
          "Audit SUCCESS save for query {} failed; query result is unaffected", queryType, saveEx);
    }
    return result;
  }

  /** Runs a metric-recording action, logging and swallowing any failure. */
  private void recordMetric(Runnable recorder, String meter) {
    try {
      recorder.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics {} failed", meter, e);
    }
  }

  @Override
  public <Q extends Query<R>, R> void register(Class<Q> queryType, QueryHandler<Q, R> handler) {
    delegate.register(queryType, handler);
  }

  private boolean isAuditable(Class<?> queryClass) {
    return auditableCache.computeIfAbsent(queryClass, c -> c.isAnnotationPresent(Auditable.class));
  }

  private static UserId currentUserId() {
    if (StreamRuneContext.CURRENT.isBound()) {
      StreamRuneContext.RequestContext ctx = StreamRuneContext.CURRENT.get();
      if (ctx != null) {
        return ctx.userId();
      }
    }
    return null;
  }

  /**
   * The correlation id the row records, as the command audit records it: the bound saga origin's
   * when a query runs with an origin bound (inside a saga's dispatch or a continuation), otherwise
   * the bound context's, otherwise none.
   */
  private static CorrelationId currentCorrelationId() {
    return BoundCorrelationId.inEffect();
  }
}
