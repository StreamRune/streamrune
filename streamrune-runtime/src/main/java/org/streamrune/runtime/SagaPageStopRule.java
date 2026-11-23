package org.streamrune.runtime;

import java.util.Objects;
import java.util.Optional;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.metrics.MetricNames;

/**
 * The stop rule of a page of saga steps: the page starts no further saga after a connection-class
 * failure, which says the database is gone and not that one saga failed, nor after the third failed
 * saga of the page. Any other failure ends only the group of its saga. The rule keeps no state and
 * is asked by several workers at once; it is as thread-safe as its classifier.
 */
final class SagaPageStopRule implements PageStopRule {

  /** The failed items of a page after which no further item is started. */
  static final int MAX_FAILURES_PER_PAGE = 3;

  private final ConnectionFailureClassifier classifier;

  SagaPageStopRule(ConnectionFailureClassifier classifier) {
    this.classifier = Objects.requireNonNull(classifier, "classifier is required");
  }

  /** True for a connection-class failure and for the third failure of the page. */
  @Override
  public boolean stopsPage(Throwable failure, int failuresInPage) {
    return classifier.isConnectionFailure(failure) || failuresInPage >= MAX_FAILURES_PER_PAGE;
  }

  /**
   * The {@code stop} tag of a failed page, decided from its failures and not only from the failure
   * it stopped on.
   *
   * <p>Empty when the page has no failure, when the failure is an {@link Error} (it stops the
   * subscription anyway) or when the caller was interrupted: no page.failed is recorded for those.
   * Otherwise, in this order:
   *
   * <ul>
   *   <li>{@code "connection"} when the failure the page stopped on is connection-class; a page
   *       that never asked the rule (one saga) has no such failure, so its own thrown failure
   *       decides. A lost connection wins over the count: the items in flight fail the same way, so
   *       an outage often has three or more failures.
   *   <li>{@code "third_failure"} when three or more items failed.
   *   <li>{@code "none"} otherwise: every other saga ran to its end. A page stopped by a throwing
   *       rule or observer, with fewer than three failures, is {@code "none"} too, and so is a page
   *       on which no item failed and which failed all the same ({@link
   *       PageResult#failurePosition()} −1: its workers ended without running their items, or items
   *       were left unrun) — the page WARN then says so instead of naming a saga.
   * </ul>
   *
   * <p>Of two reasons to stop, the first one reported decides.
   */
  Optional<String> stopTag(PageResult result) {
    Throwable failure = result.failure();
    if (failure == null || failure instanceof Error || result.interrupted()) {
      return Optional.empty();
    }
    Throwable decisive = result.stoppedBy() != null ? result.stoppedBy() : failure;
    if (classifier.isConnectionFailure(decisive)) {
      return Optional.of(MetricNames.STOP_CONNECTION);
    }
    if (result.failed() >= MAX_FAILURES_PER_PAGE) {
      return Optional.of(MetricNames.STOP_THIRD_FAILURE);
    }
    return Optional.of(MetricNames.STOP_NONE);
  }
}
