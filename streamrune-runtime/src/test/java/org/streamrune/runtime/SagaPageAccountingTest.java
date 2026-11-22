package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.SagaParallelFixtures.SAGA_TYPE;
import static org.streamrune.runtime.SagaParallelFixtures.page;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.saga.SagaId;
import org.streamrune.runtime.SagaParallelFixtures.Started;
import org.streamrune.test.RecordingStreamRuneMetrics;

/** The meters and the WARN of a page at a parallelism above 1, without a runner. */
class SagaPageAccountingTest {

  private final RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();
  private final Logger runnerLog = (Logger) LoggerFactory.getLogger(SagaRunner.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  private final List<EventEnvelope> page = page(10, new Started("a"), new Started("b"));
  private final List<SagaWorkItem> items =
      page.stream()
          .map(e -> new SagaWorkItem(SagaId.of(((Started) e.event()).id()), true, e))
          .toList();

  @BeforeEach
  void capture() {
    appender.start();
    runnerLog.addAppender(appender);
  }

  @AfterEach
  void release() {
    runnerLog.detachAppender(appender);
  }

  private SagaPageAccounting accounting(ConnectionFailureClassifier classifier) {
    return new SagaPageAccounting(SAGA_TYPE, metrics, new SagaPageStopRule(classifier));
  }

  private static PageResult result(
      int groups,
      int failed,
      int notStarted,
      Throwable failure,
      int position,
      Throwable stoppedBy) {
    return new PageResult(
        2, groups, failed, notStarted, failure, position, stoppedBy, false, 10, 5);
  }

  private List<String> warns() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void aPageThatRanRecordsItsEffectiveParallelism_andNoFailure() {
    accounting(ConnectionFailureClassifier.GENERIC)
        .afterPage(result(2, 0, 0, null, -1, null), items);

    assertThat(metrics.sagaPageEffectiveParallelism()).containsExactly(2.0);
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
    assertThat(warns()).isEmpty();
  }

  @Test
  void aPageOfOneSagaRecordsNoEffectiveParallelism() {
    accounting(ConnectionFailureClassifier.GENERIC)
        .afterPage(result(1, 0, 0, null, -1, null), items);

    assertThat(metrics.sagaPageEffectiveParallelism()).isEmpty();
  }

  @Test
  void aFailedItemIsNamedInTheWarn() {
    accounting(ConnectionFailureClassifier.GENERIC)
        .afterPage(result(2, 1, 0, new IllegalStateException("x"), 1, null), items);

    assertThat(warns())
        .singleElement()
        .asString()
        .contains("1 failed, first for saga b at offset 11; the other sagas of the page ran to");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
  }

  @Test
  void aPageStoppedByItsRuleOrObserverSaysSo() {
    var ruleDown = new IllegalStateException("rule down");
    accounting(ConnectionFailureClassifier.GENERIC)
        .afterPage(result(2, 1, 0, new IllegalStateException("x"), 0, ruleDown), items);

    assertThat(warns())
        .singleElement()
        .asString()
        .contains("the page stopped when its stop rule or its step observer failed");
  }

  @Test
  void aPageOnWhichNoItemFailedSaysWhatHappened() {
    var shortfall = new IllegalStateException("2 of 2 worker(s) of the page ended without running");
    accounting(ConnectionFailureClassifier.GENERIC)
        .afterPage(result(2, 0, 2, shortfall, -1, null), items);

    assertThat(warns())
        .singleElement()
        .asString()
        .contains(
            "was not acknowledged although none of its steps failed: 2 of 2 worker(s) of the page"
                + " ended without running; 2 not started.")
        .doesNotContain("<none>");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
  }

  @Test
  void thePageWarnIsWrittenFirstThenAtMostOncePerInterval_andEveryFailedPageIsCounted() {
    AtomicLong now = new AtomicLong();
    var accounting =
        new SagaPageAccounting(
            SAGA_TYPE,
            metrics,
            new SagaPageStopRule(ConnectionFailureClassifier.GENERIC),
            now::get);
    var failed = result(2, 1, 0, new IllegalStateException("x"), 1, null);

    accounting.afterPage(failed, items);
    now.addAndGet(Duration.ofMinutes(1).toNanos());
    accounting.afterPage(failed, items);
    accounting.afterRoutingFailure(new IllegalStateException("dead letters down"), 3);
    assertThat(warns()).as("the first page only, within the interval").hasSize(1);
    now.addAndGet(Duration.ofMinutes(FailureLogLimiter.INTERVAL_MINUTES).toNanos());
    accounting.afterPage(failed, items);

    assertThat(warns()).hasSize(2);
    assertThat(warns().get(0))
        .contains("1 failed, first for saga b")
        .contains("at most once per 5 minutes; streamrune.saga.page.failed counts every page")
        .doesNotContain("more page(s)");
    assertThat(warns().get(1))
        .contains("2 more page(s) were not acknowledged since the last report");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(4);
  }

  @Test
  void aRoutingFailureIsTaggedByTheClassifier_andItsMessageIsSanitized() {
    var accounting = accounting(ConnectionFailureClassifier.GENERIC);

    accounting.afterRoutingFailure(new IllegalStateException("forged\nline"), 3);
    accounting.afterRoutingFailure(new RuntimeException(new SQLException("down", "08006")), 2);

    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION)).isEqualTo(1);
    assertThat(warns())
        .singleElement()
        .asString()
        .contains("a page of 3 event(s) was not acknowledged: routing it failed before any step")
        .doesNotContain("\n");
  }

  @Test
  void aConnectionFailureIsTaggedAndPhrased() {
    var down = new SQLException("down", "08006");
    accounting(ConnectionFailureClassifier.GENERIC)
        .afterPage(
            result(2, 1, 1, new RuntimeException(down), 0, new RuntimeException(down)), items);

    assertThat(warns()).singleElement().asString().contains("connection-class failure");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION)).isEqualTo(1);
  }

  @Test
  void aThrowingClassifierIsLogged_andNoPageFailedIsRecorded() {
    accounting(
            f -> {
              throw new IllegalStateException("classifier down");
            })
        .afterPage(result(2, 1, 0, new IllegalStateException("x"), 0, null), items);

    assertThat(warns()).singleElement().asString().contains("classifier failed");
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
  }

  @Test
  void aThrowingBackendIsLoggedAndDropped_anErrorPasses() {
    var throwing =
        new RecordingStreamRuneMetrics() {
          @Override
          public void recordSagaPageEffectiveParallelism(String sagaType, double value) {
            throw new IllegalStateException("down");
          }

          @Override
          public void recordSagaPageFailed(String sagaType, String stop) {
            throw new AssertionError("error");
          }
        };
    var accounting =
        new SagaPageAccounting(
            SAGA_TYPE, throwing, new SagaPageStopRule(ConnectionFailureClassifier.GENERIC));

    accounting.afterPage(result(2, 0, 0, null, -1, null), items);
    assertThat(warns()).singleElement().asString().contains("Metrics recording failed");

    assertThatThrownBy(
            () ->
                accounting.afterPage(
                    result(2, 1, 0, new IllegalStateException("x"), 0, null), items))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void gaugeFailuresAreCountedByTheObserver_andLoggedOnceByZeroGauges() {
    var throwing =
        new RecordingStreamRuneMetrics() {
          @Override
          public void recordSagaStepsInFlight(String sagaType, int inFlight, int pending) {
            throw new IllegalStateException("gauge down");
          }
        };
    var accounting =
        new SagaPageAccounting(
            SAGA_TYPE, throwing, new SagaPageStopRule(ConnectionFailureClassifier.GENERIC));

    var page = accounting.startPage();
    page.stepsChanged(1, 1);
    page.stepsChanged(0, 1);
    assertThat(warns()).as("nothing logged under the executor's lock").isEmpty();
    accounting.endPage(page);
    var next = accounting.startPage();
    next.stepsChanged(1, 0);
    accounting.endPage(next);

    assertThat(warns()).singleElement().asString().contains("failed 3 time(s)");
  }

  @Test
  void zeroGaugesSetsBothGaugesToZero() {
    var accounting = accounting(ConnectionFailureClassifier.GENERIC);
    var page = accounting.startPage();
    page.stepsChanged(2, 5);
    accounting.endPage(page);

    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(0);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(0);
  }

  @Test
  void twoPagesAddUp_andEachEndsByRemovingOnlyItsOwnShare() {
    var accounting = accounting(ConnectionFailureClassifier.GENERIC);
    var first = accounting.startPage();
    var second = accounting.startPage();

    first.stepsChanged(2, 3);
    second.stepsChanged(1, 4);
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(3);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(7);

    accounting.endPage(first);
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(1);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(4);

    accounting.endPage(first);
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT))
        .as("ending a page twice removes nothing more")
        .hasValue(1);

    accounting.endPage(second);
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(0);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(0);
  }
}
