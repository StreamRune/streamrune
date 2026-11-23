package org.streamrune.runtime;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * What a saga runner records about the pages it stepped at a parallelism above 1: the gauges of the
 * steps in flight and of the items not started, across the runner's pages, the effective
 * parallelism, and — for a page that is not acknowledged, also one whose routing phase failed — the
 * {@code page.failed} sample, every time, and the page WARN: the first, then at most once per
 * {@value FailureLogLimiter#INTERVAL_MINUTES} minutes with the number of pages since.
 *
 * <p>A metrics backend or classifier that throws an exception is logged and dropped, so it never
 * replaces the page's failure; an {@link Error} passes. The gauge is reported at every step start
 * and end, under the page executor's lock: its failures are only counted there, and logged by
 * {@link #endPage} on the delivering thread — the first, then at most once per {@value
 * FailureLogLimiter#INTERVAL_MINUTES} minutes ({@link FailureLogLimiter}).
 *
 * <p>Logs through the logger of {@link SagaRunner}: these are the runner's lines.
 */
final class SagaPageAccounting {

  private static final Logger LOG = LoggerFactory.getLogger(SagaRunner.class);

  private final SagaType sagaType;
  private final StreamRuneMetrics metrics;
  private final SagaPageStopRule stopRule;
  private final FailureLogLimiter gaugeFailures = new FailureLogLimiter();

  /**
   * The pages that were not acknowledged: the page WARN is written for the first, then at most once
   * per interval with the number of pages since; {@code page.failed} counts every one.
   */
  private final FailureLogLimiter pageWarnings;

  private final AtomicReference<RuntimeException> lastGaugeFailure = new AtomicReference<>();

  SagaPageAccounting(SagaType sagaType, StreamRuneMetrics metrics, SagaPageStopRule stopRule) {
    this(sagaType, metrics, stopRule, System::nanoTime);
  }

  /** Accounting on a clock of the test's, which decides when the page WARN is written again. */
  SagaPageAccounting(
      SagaType sagaType,
      StreamRuneMetrics metrics,
      SagaPageStopRule stopRule,
      LongSupplier nanoTime) {
    this.sagaType = Objects.requireNonNull(sagaType, "sagaType is required");
    this.metrics = Objects.requireNonNull(metrics, "metrics is required");
    this.stopRule = Objects.requireNonNull(stopRule, "stopRule is required");
    this.pageWarnings = new FailureLogLimiter(Objects.requireNonNull(nanoTime, "nanoTime"));
  }

  /** Runner-wide steps in flight over every page in flight; guarded by {@link #gaugeLock}. */
  private int inFlightTotal;

  /** Runner-wide items not started over every page in flight; guarded by {@link #gaugeLock}. */
  private int pendingTotal;

  private final Object gaugeLock = new Object();

  /**
   * Starts the accounting of one fanned-out page. A runner bound to several subscriptions has
   * several pages in flight, so the gauges count across the runner's pages: each page adds its own
   * changes, and ends by removing only what it still holds ({@link #endPage}).
   */
  Page startPage() {
    return new Page();
  }

  /** One page's share of the runner-wide gauges; also the observer the page executor tells. */
  final class Page implements PageObserver {
    private int inFlight;
    private int pending;

    @Override
    public void stepsChanged(int inFlightNow, int notYetStarted) {
      synchronized (gaugeLock) {
        inFlightTotal += inFlightNow - inFlight;
        pendingTotal += notYetStarted - pending;
        inFlight = inFlightNow;
        pending = notYetStarted;
        reportGauges();
      }
    }
  }

  /** Reports the totals; a failing backend is counted. The caller holds the gauge lock. */
  private void reportGauges() {
    try {
      metrics.recordSagaStepsInFlight(sagaType.value(), inFlightTotal, pendingTotal);
    } catch (RuntimeException e) {
      gaugeFailures.count();
      lastGaugeFailure.set(e);
    }
  }

  /**
   * After a page (the page executor reports no end of page), also after a page that stopped or
   * could not start its workers: removes what {@code page} still holds from the runner-wide gauges,
   * leaving the other pages' counts as they are; then the gauge failures, if any, are logged as the
   * limiter allows.
   */
  void endPage(Page page) {
    synchronized (gaugeLock) {
      inFlightTotal -= page.inFlight;
      pendingTotal -= page.pending;
      page.inFlight = 0;
      page.pending = 0;
      reportGauges();
    }
    long unlogged = gaugeFailures.claim();
    if (unlogged > 0) {
      LOG.warn(
          "Saga type {}: recording the saga steps in flight failed {} time(s) since the last"
              + " report; the steps go on without it. Further failures are reported at most once"
              + " per {} minutes.",
          sagaType.value(),
          unlogged,
          FailureLogLimiter.INTERVAL_MINUTES,
          lastGaugeFailure.getAndSet(null));
    }
  }

  /**
   * After a page: the effective parallelism of a page of two sagas or more, and — for a page that
   * is not acknowledged and did not end with an {@link Error} or an interrupt — the {@code
   * page.failed} sample and its WARN, the WARN as {@link #pageWarnings} allows.
   *
   * @param result the page as it ran, its failure filled in when it failed closed
   * @param items the work items of the page, in the executor's positions
   */
  void afterPage(PageResult result, List<SagaWorkItem> items) {
    if (result.groups() >= 2 && result.wallNanos() > 0) {
      double effective = (double) result.stepNanos() / result.wallNanos();
      record(
          () -> metrics.recordSagaPageEffectiveParallelism(sagaType.value(), effective),
          "recordSagaPageEffectiveParallelism");
    }
    Optional<String> stop = stopTagOf(result);
    if (stop.isPresent()) {
      long since = claimPageWarning();
      if (since > 0) {
        warnNotAcknowledged(result, items, stop.get(), since);
      }
      recordPageFailed(stop.get());
    }
  }

  /**
   * After a page whose routing phase threw an exception, before any of its steps started (a
   * quarantine write that failed): the {@code page.failed} sample, tagged as the stop rule tags a
   * page that never asked it, and the page WARN as {@link #pageWarnings} allows.
   *
   * @param failure what routing threw; the listener throws it
   * @param events the events of the page
   */
  void afterRoutingFailure(RuntimeException failure, int events) {
    PageResult routingFailed = new PageResult(0, 0, 0, 0, failure, -1, null, false, 0, 0);
    Optional<String> stop = stopTagOf(routingFailed);
    if (stop.isEmpty()) {
      return;
    }
    long since = claimPageWarning();
    if (since > 0) {
      LOG.warn(
          "Saga type {}: a page of {} event(s) was not acknowledged: routing it failed before any"
              + " step started, {}: {}. The subscription delivers the page again.{}",
          sagaType.value(),
          events,
          MetricNames.STOP_CONNECTION.equals(stop.get())
              ? "on a connection-class failure"
              : "on a failure that is not connection-class",
          LogSanitizer.sanitizeForLog(String.valueOf(failure.getMessage())),
          limitPhrase(since));
    }
    recordPageFailed(stop.get());
  }

  /** Counts one page that is not acknowledged; the pages since the last WARN when one is due. */
  private long claimPageWarning() {
    pageWarnings.count();
    return pageWarnings.claim();
  }

  private void recordPageFailed(String stop) {
    record(() -> metrics.recordSagaPageFailed(sagaType.value(), stop), "recordSagaPageFailed");
  }

  /** What the page WARN says about the pages it does not report. */
  private static String limitPhrase(long since) {
    String earlier =
        since > 1
            ? " " + (since - 1) + " more page(s) were not acknowledged since the last report."
            : "";
    return earlier
        + " This line is written at most once per "
        + FailureLogLimiter.INTERVAL_MINUTES
        + " minutes; "
        + MetricNames.SAGA_PAGE_FAILED
        + " counts every page.";
  }

  /** The stop tag of the page, or empty when its classifier throws. */
  private Optional<String> stopTagOf(PageResult result) {
    try {
      return stopRule.stopTag(result);
    } catch (RuntimeException classifierFailed) {
      LOG.warn(
          "Saga type {}: the connection-failure classifier failed while the outcome of a page"
              + " that was not acknowledged was decided; no page.failed is recorded for it",
          sagaType.value(),
          classifierFailed);
      return Optional.empty();
    }
  }

  private void warnNotAcknowledged(
      PageResult result, List<SagaWorkItem> items, String stop, long since) {
    int at = result.failurePosition();
    if (at < 0) {
      // no item failed: the page's own failure says what happened
      LOG.warn(
          "Saga type {}: a page of {} step(s) was not acknowledged although none of its steps"
              + " failed: {}; {} not started. The subscription delivers the page again; finished"
              + " steps are answered from their saga rows.{}",
          sagaType.value(),
          result.items(),
          result.failure().getMessage(),
          result.notStarted(),
          limitPhrase(since));
      return;
    }
    SagaWorkItem first = items.get(at);
    LOG.warn(
        "Saga type {}: a page of {} step(s) was not acknowledged: {} failed, first for saga {} at"
            + " offset {}; {}; {} not started. The subscription delivers the page again; finished"
            + " steps are answered from their saga rows.{}",
        sagaType.value(),
        result.items(),
        result.failed(),
        LogSanitizer.sanitizeForLog(first.sagaId().value()),
        first.event().globalOffset().value(),
        stopPhrase(stop, result),
        result.notStarted(),
        limitPhrase(since));
  }

  /** What the page WARN says about the rest of the page. */
  static String stopPhrase(String stop, PageResult result) {
    return switch (stop) {
      case MetricNames.STOP_CONNECTION -> "the page stopped on a connection-class failure";
      case MetricNames.STOP_THIRD_FAILURE -> "the page stopped on its third failure";
      default ->
          result.stoppedBy() != null
              ? "the page stopped when its stop rule or its step observer failed"
              : "the other sagas of the page ran to their end";
    };
  }

  /** Runs a metrics call; an exception of the backend is logged and dropped, an Error passes. */
  private static void record(Runnable metricCall, String metricName) {
    try {
      metricCall.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for {}", metricName, e);
    }
  }
}
