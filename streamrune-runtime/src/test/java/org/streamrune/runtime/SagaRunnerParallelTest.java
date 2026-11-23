package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.SagaParallelFixtures.AWAIT;
import static org.streamrune.runtime.SagaParallelFixtures.SAGA_TYPE;
import static org.streamrune.runtime.SagaParallelFixtures.STEPS;
import static org.streamrune.runtime.SagaParallelFixtures.orchestrator;
import static org.streamrune.runtime.SagaParallelFixtures.page;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.SagaParallelFixtures.Delegating;
import org.streamrune.runtime.SagaParallelFixtures.EndState;
import org.streamrune.runtime.SagaParallelFixtures.GatedBus;
import org.streamrune.runtime.SagaParallelFixtures.ParState;
import org.streamrune.runtime.SagaParallelFixtures.Replied;
import org.streamrune.runtime.SagaParallelFixtures.Started;
import org.streamrune.runtime.SagaParallelFixtures.Step;
import org.streamrune.runtime.SagaParallelFixtures.Stepped;
import org.streamrune.runtime.SagaParallelFixtures.World;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A runner steps the sagas of a page in parallel: different sagas at the same time, the events of
 * one saga in offset order, at most {@code parallelism} steps at once. A failed step ends its
 * saga's part of the page; a connection-class failure or the third failure ends the page; the page
 * is not acknowledged either way. Parallelism 1 is the serial path of before, with nothing new
 * recorded.
 */
class SagaRunnerParallelTest {

  private static final String NOT_ACKNOWLEDGED = "was not acknowledged";

  private final InMemorySagaStore rows = new InMemorySagaStore();
  private final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);
  private final GatedBus bus = new GatedBus();
  private final org.streamrune.test.RecordingStreamRuneMetrics metrics =
      new org.streamrune.test.RecordingStreamRuneMetrics();

  private final Logger runnerLog = (Logger) LoggerFactory.getLogger(SagaRunner.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  @BeforeEach
  void captureTheRunnersLog() {
    appender.start();
    runnerLog.addAppender(appender);
  }

  @AfterEach
  void releaseEverything() {
    bus.releaseAll();
    runnerLog.detachAppender(appender);
  }

  private SagaRunner.Builder<ParState> builder() {
    return builder(rows);
  }

  private SagaRunner.Builder<ParState> builder(SagaStore sagaStore) {
    return SagaRunner.<ParState>builder()
        .orchestrator(orchestrator())
        .sagaStore(sagaStore)
        .commandBus(bus)
        .sagaDeadLetterStore(deadLetters)
        .metrics(metrics)
        // the bus is a test double that stores nothing; the fence is tested elsewhere
        .forwardFence(false);
  }

  private SagaRunner<ParState> runner(int parallelism) {
    return builder().parallelism(parallelism).build();
  }

  private Optional<LoadedSaga<ParState>> row(String sagaId) {
    return rows.load(SagaId.of(sagaId), SAGA_TYPE, ParState.class);
  }

  private List<String> pageWarns() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(m -> m.contains(NOT_ACKNOWLEDGED))
        .toList();
  }

  private static DomainEvent[] started(String... ids) {
    DomainEvent[] events = new DomainEvent[ids.length];
    for (int i = 0; i < ids.length; i++) {
      events[i] = new Started(ids[i]);
    }
    return events;
  }

  private static String[] sagas(String prefix, int count) {
    String[] ids = new String[count];
    for (int i = 0; i < count; i++) {
      ids[i] = prefix + (i + 1);
    }
    return ids;
  }

  /** Delivers the page on a thread of its own; completes with the failure, or null. */
  private static CompletableFuture<Throwable> deliverInBackground(
      SagaRunner<ParState> runner, List<EventEnvelope> page) {
    CompletableFuture<Throwable> done = new CompletableFuture<>();
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                runner.asEventListener().onEvents(page);
                done.complete(null);
              } catch (Throwable t) {
                done.complete(t);
              }
            });
    return done;
  }

  private static Throwable outcome(CompletableFuture<Throwable> done) throws Exception {
    return done.get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static void awaitUntil(BooleanSupplier condition, String what) {
    long deadline = System.nanoTime() + AWAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      LockSupport.parkNanos(1_000_000);
    }
  }

  /** The in-flight samples so far: the start and end of each step, in order. */
  private int inFlightSamples() {
    return metrics.samples(MetricNames.SAGA_STEPS_IN_FLIGHT).size();
  }

  // ==================== Parallel, and in order per saga ====================

  @Test
  void twoSagasOfOnePageStepAtTheSameTime() throws Exception {
    var runner = runner(4);
    bus.gate("a");
    bus.gate("b");

    var done = deliverInBackground(runner, page(1, started("a", "b")));

    bus.gate("a").awaitEntered();
    bus.gate("b").awaitEntered();
    assertThat(done).as("both commands are held").isNotDone();
    bus.releaseAll();
    assertThat(outcome(done)).isNull();
    assertThat(row("a").orElseThrow().genesisApplied()).isTrue();
    assertThat(row("b").orElseThrow().genesisApplied()).isTrue();
  }

  @Test
  void theEventsOfOneSagaAreAppliedInOffsetOrder_whileAnotherSagaIsInFlight() throws Exception {
    var runner = runner(4);
    bus.gate("b");

    var done =
        deliverInBackground(runner, page(1, new Started("a"), new Started("b"), new Replied("a")));

    bus.gate("b").awaitEntered();
    awaitUntil(
        () -> row("a").map(r -> Long.valueOf(3).equals(r.lastAppliedOffset())).orElse(false),
        "saga a to apply its reply while saga b is held");
    assertThat(row("a").orElseThrow().state().applied()).containsExactly(1L, 3L);
    assertThat(bus.commandsOf("a")).containsExactly(new Step("a", 1), new Step("a", 3));
    assertThat(done).isNotDone();

    bus.releaseAll();
    assertThat(outcome(done)).isNull();
  }

  @Test
  void aValueThatArrivesDuringAPageAppliesFromTheNextPage() throws Exception {
    var runner = builder().build();
    bus.gate("a");
    bus.gate("b");
    var first = deliverInBackground(runner, page(1, started("a", "b")));
    bus.gate("a").awaitEntered();
    bus.gate("b").awaitEntered();

    // the page in flight keeps its four-wide read
    runner.applyParallelismSettings(new SagaParallelismSettings(1, java.util.Map.of()));
    assertThat(runner.parallelism()).isEqualTo(1);
    bus.releaseAll();
    assertThat(outcome(first)).isNull();
    assertThat(bus.threads()).hasSize(2).doesNotContain(Thread.currentThread());

    // the next page reads one: its steps run on the calling thread
    runner.asEventListener().onEvents(page(3, started("c", "d")));

    assertThat(bus.threads().subList(2, 4)).containsOnly(Thread.currentThread());
  }

  @Test
  void theGaugesCountAcrossThePagesOfOneRunner_andReturnToZeroOnlyAfterBothEnd() throws Exception {
    var runner = runner(4);
    for (String id : new String[] {"a", "b", "c", "d"}) {
      bus.gate(id);
    }
    var first = deliverInBackground(runner, page(1, started("a", "b")));
    var second = deliverInBackground(runner, page(3, started("c", "d")));
    for (String id : new String[] {"a", "b", "c", "d"}) {
      bus.gate(id).awaitEntered();
    }

    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(4);

    bus.gate("a").release();
    bus.gate("b").release();
    assertThat(outcome(first)).isNull();
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT))
        .as("the second page is still in flight")
        .hasValue(2);

    bus.releaseAll();
    assertThat(outcome(second)).isNull();
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(0);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(0);
  }

  @Test
  void theDefaultParallelismIsFour_andTheBuilderRefusesZeroAnd65() {
    assertThat(SagaRunner.DEFAULT_PARALLELISM).isEqualTo(4);
    assertThat(SagaRunner.MAX_PARALLELISM).isEqualTo(64);
    assertThat(builder().build().parallelism()).isEqualTo(4);
    assertThat(builder().parallelism(1).build().parallelism()).isEqualTo(1);
    assertThat(builder().parallelism(64).build().parallelism()).isEqualTo(64);

    assertThatThrownBy(() -> builder().parallelism(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("parallelism must be between 1 and 64, got: 0");
    assertThatThrownBy(() -> builder().parallelism(65))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("parallelism must be between 1 and 64, got: 65");
    assertThatThrownBy(() -> SagaRunner.requireParallelismInRange("streamrune.saga.x", -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("streamrune.saga.x must be between 1 and 64, got: -1");
    assertThat(SagaRunner.requireParallelismInRange("p", 7)).isEqualTo(7);
  }

  @Test
  void parallelismOneDeliversOnTheCallingThread() {
    var runner = runner(1);

    runner.asEventListener().onEvents(page(1, started("a", "b", "c")));

    assertThat(bus.sagas()).containsExactly("a", "b", "c");
    assertThat(bus.threads()).containsOnly(Thread.currentThread());
  }

  @Test
  void parallelismOneRecordsNoPageMetricAndLogsNoPageWarn() {
    var runner = runner(1);
    bus.failing("b", () -> new CircuitBreakerOpenException("open"));

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "b", "c"))))
        .isInstanceOf(CircuitBreakerOpenException.class);

    assertThat(bus.sagas())
        .as("the serial loop ends at the first failure")
        .containsExactly("a", "b");
    assertThat(metrics.samples(MetricNames.SAGA_STEPS_IN_FLIGHT)).isEmpty();
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_PENDING)).isEmpty();
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_EFFECTIVE_PARALLELISM)).isEmpty();
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
    assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().contains(NOT_ACKNOWLEDGED));
  }

  // ==================== A failed step ====================

  @Test
  void aRetryLaterFailureOfOneSagaLetsTheOthersFinish_andThePageIsNotAcknowledged() {
    var runner = runner(4);
    bus.failing("b", () -> new CircuitBreakerOpenException("open"));

    assertThatThrownBy(
            () -> runner.asEventListener().onEvents(page(1, started("a", "b", "c", "d"))))
        .isInstanceOf(CircuitBreakerOpenException.class);

    for (String id : List.of("a", "c", "d")) {
      LoadedSaga<ParState> row = row(id).orElseThrow();
      assertThat(row.status()).as(id).isEqualTo(SagaStatus.RUNNING);
      assertThat(row.genesisApplied()).as(id).isTrue();
      assertThat(row.state().applied()).as(id).hasSize(1);
    }
    LoadedSaga<ParState> b = row("b").orElseThrow();
    assertThat(b.genesisApplied()).as("b stays genesis-pending").isFalse();
    assertThat(b.version()).isEqualTo(1);
    assertThat(b.lastAppliedOffset()).isNull();
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
    assertThat(pageWarns())
        .containsExactly(
            "Saga type "
                + SAGA_TYPE.value()
                + ": a page of 4 step(s) was not acknowledged: 1 failed, first for saga b at"
                + " offset 2; the other sagas of the page ran to their end; 0 not started. The"
                + " subscription delivers the page again; finished steps are answered from their"
                + " saga rows. This line is written at most once per 5 minutes;"
                + " streamrune.saga.page.failed counts every page.");
  }

  @Test
  void theWarnNamesTheFailedSagaThroughTheLogSanitizer() {
    var runner = runner(4);
    String forged = "b\nINFO forged line";
    bus.failing(forged, () -> new CircuitBreakerOpenException("open"));

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", forged))))
        .isInstanceOf(CircuitBreakerOpenException.class);

    assertThat(pageWarns()).hasSize(1);
    String warn = pageWarns().getFirst();
    assertThat(warn).contains(LogSanitizer.sanitizeForLog(forged)).doesNotContain("\n");
  }

  @Test
  void nothingMoreOfTheFailedSagaIsStartedInThePage() {
    var runner = runner(4);
    bus.failing("b", () -> new CircuitBreakerOpenException("open"));

    assertThatThrownBy(
            () ->
                runner
                    .asEventListener()
                    .onEvents(page(1, new Started("b"), new Started("a"), new Replied("b"))))
        .isInstanceOf(CircuitBreakerOpenException.class);

    assertThat(bus.commandsOf("b")).containsExactly(new Step("b", 1));
    assertThat(row("b").orElseThrow().state().applied()).doesNotContain(3L);
    assertThat(row("b").orElseThrow().genesisApplied()).isFalse();
    assertThat(row("b").orElseThrow().lastAppliedOffset()).isNull();
    assertThat(row("a").orElseThrow().genesisApplied()).isTrue();
  }

  /**
   * Six sagas at parallelism 2: s1 fails while s2 is held; what runs of s3..s6 is the rule's. Both
   * are held until both are in flight, so s1's failure cannot stop the page before s2 started.
   */
  private Throwable sixSagasWhereS1FailsWhileS2IsHeld(SagaRunner<ParState> runner)
      throws Exception {
    bus.gate("s1");
    bus.gate("s2");
    var done = deliverInBackground(runner, page(1, started(sagas("s", 6))));
    bus.gate("s1").awaitEntered();
    bus.gate("s2").awaitEntered();
    bus.gate("s1").release();
    // s1 started, s2 started, s1 ended: the end of s1's step comes after the rule was asked
    awaitUntil(() -> inFlightSamples() >= 3, "the end of s1's step");
    bus.gate("s2").release();
    return outcome(done);
  }

  @Test
  void aConnectionClassFailureStopsThePage() throws Exception {
    var runner = runner(2);
    bus.failing("s1", () -> new EventStoreException("x", new SQLException("down", "08006")));

    Throwable failure = sixSagasWhereS1FailsWhileS2IsHeld(runner);

    assertThat(failure).isInstanceOf(EventStoreException.class);
    assertThat(bus.sagas()).containsExactlyInAnyOrder("s1", "s2");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION)).isEqualTo(1);
    assertThat(pageWarns())
        .singleElement()
        .asString()
        .contains("the page stopped on a connection-class failure; 4 not started.");
  }

  @Test
  void aStatementTimeoutOfOneSagaLetsTheOthersFinish() throws Exception {
    var runner = runner(2);
    bus.failing("s1", () -> new EventStoreException("x", new SQLException("timeout", "57014")));

    Throwable failure = sixSagasWhereS1FailsWhileS2IsHeld(runner);

    assertThat(failure).isInstanceOf(EventStoreException.class);
    for (String id : sagas("s", 6)) {
      if (!id.equals("s1")) {
        assertThat(row(id).orElseThrow().genesisApplied()).as(id).isTrue();
      }
    }
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
  }

  /** A saga store whose classifier counts an SQLState XX000 anywhere in the chain. */
  private SagaStore storeCountingXx000() {
    return new org.streamrune.test.ForwardingSagaStore(rows) {
      @Override
      public ConnectionFailureClassifier connectionFailureClassifier() {
        return f -> {
          for (Throwable c = f; c != null; c = c.getCause()) {
            if (c instanceof SQLException sql && "XX000".equals(sql.getSQLState())) {
              return true;
            }
          }
          return false;
        };
      }
    };
  }

  @Test
  void theSagaStoresClassifierDecidesWhatStopsThePage() throws Exception {
    var runner = builder(storeCountingXx000()).parallelism(2).build();
    // retry-later (an SQLException in the chain), and GENERIC does not count it
    bus.failing("s1", () -> new RuntimeException(new SQLException("internal", "XX000")));

    Throwable failure = sixSagasWhereS1FailsWhileS2IsHeld(runner);

    assertThat(failure).isInstanceOf(RuntimeException.class);
    assertThat(bus.sagas()).containsExactlyInAnyOrder("s1", "s2");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION)).isEqualTo(1);
  }

  @Test
  void theBuildersClassifierReplacesTheStores() throws Exception {
    var runner =
        builder(storeCountingXx000())
            .parallelism(2)
            .connectionFailureClassifier(f -> false)
            .build();
    bus.failing("s1", () -> new RuntimeException(new SQLException("internal", "XX000")));

    Throwable failure = sixSagasWhereS1FailsWhileS2IsHeld(runner);

    assertThat(failure).isInstanceOf(RuntimeException.class);
    assertThat(bus.sagas()).containsExactlyInAnyOrder("s1", "s2", "s3", "s4", "s5", "s6");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
  }

  @Test
  void aStoreThatAnswersNoClassifierCountsAsGeneric() throws Exception {
    SagaStore answersNull =
        new org.streamrune.test.ForwardingSagaStore(rows) {
          @Override
          public ConnectionFailureClassifier connectionFailureClassifier() {
            return null;
          }
        };
    var runner = builder(answersNull).parallelism(2).build();
    bus.failing("s1", () -> new EventStoreException("x", new SQLException("down", "08006")));

    Throwable failure = sixSagasWhereS1FailsWhileS2IsHeld(runner);

    assertThat(failure).isInstanceOf(EventStoreException.class);
    assertThat(bus.sagas()).containsExactlyInAnyOrder("s1", "s2");
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION)).isEqualTo(1);
  }

  @Test
  void theSagaStoresClassifierIsReadOnce_andNotAtTheBuild() throws Exception {
    AtomicInteger reads = new AtomicInteger();
    SagaStore counting =
        new org.streamrune.test.ForwardingSagaStore(rows) {
          @Override
          public ConnectionFailureClassifier connectionFailureClassifier() {
            reads.incrementAndGet();
            return super.connectionFailureClassifier();
          }
        };
    var runner = builder(counting).parallelism(4).build();
    assertThat(reads).as("not read at the build").hasValue(0);
    for (String id : List.of("a", "b", "c", "d")) {
      bus.failing(id, () -> new CircuitBreakerOpenException("open"));
    }

    assertThatThrownBy(
            () -> runner.asEventListener().onEvents(page(1, started("a", "b", "c", "d"))))
        .isInstanceOf(CircuitBreakerOpenException.class);
    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a"))))
        .isInstanceOf(CircuitBreakerOpenException.class);

    assertThat(reads).as("read once, by whichever thread asked first").hasValue(1);
  }

  @Test
  void aOneSagaPageThatLosesItsConnectionIsTaggedConnection() {
    var runner = runner(4);
    bus.failing("a", () -> new EventStoreException("x", new SQLException("down", "08006")));

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, new Started("a"))))
        .isInstanceOf(EventStoreException.class);

    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION)).isEqualTo(1);
    assertThat(pageWarns())
        .singleElement()
        .asString()
        .contains("1 failed, first for saga a at offset 1; the page stopped on a connection-class");
  }

  @Test
  void theThirdFailureStopsThePage() throws Exception {
    var gateS2 = bus.gate("s2");
    var stepEnds = new AtomicInteger();
    var recorder =
        new org.streamrune.test.RecordingStreamRuneMetrics() {
          private int last;

          @Override
          public void recordSagaStepsInFlight(String sagaType, int inFlight, int pending) {
            super.recordSagaStepsInFlight(sagaType, inFlight, pending);
            if (inFlight < last && stepEnds.incrementAndGet() == 3) {
              gateS2.release(); // the end of the third failed step: the page has stopped
            }
            last = inFlight;
          }
        };
    var runner = builder().metrics(recorder).parallelism(2).build();
    for (String id : List.of("s1", "s3", "s4")) {
      bus.failing(id, () -> new CircuitBreakerOpenException("open"));
    }
    for (String id : List.of("s5", "s6", "s7", "s8")) {
      bus.gate(id);
    }

    Throwable failure = outcome(deliverInBackground(runner, page(1, started(sagas("s", 8)))));

    assertThat(failure).isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(new HashSet<>(bus.sagas())).containsExactlyInAnyOrder("s1", "s2", "s3", "s4");
    assertThat(bus.sagas()).hasSize(4);
    assertThat(recorder.sagaPageFailed(MetricNames.STOP_THIRD_FAILURE)).isEqualTo(1);
    assertThat(pageWarns())
        .singleElement()
        .asString()
        .contains(
            "3 failed, first for saga s1 at offset 1; the page stopped on its third failure;"
                + " 4 not started.");
  }

  @Test
  void aRedeliveryOfAFailedPageDedupsTheFinishedSteps() {
    var runner = runner(4);
    var page = page(1, started("a", "b", "c", "d"));
    bus.failing("b", () -> new CircuitBreakerOpenException("open"));
    assertThatThrownBy(() -> runner.asEventListener().onEvents(page))
        .isInstanceOf(CircuitBreakerOpenException.class);
    List<Long> versions = new ArrayList<>();
    for (String id : List.of("a", "c", "d")) {
      versions.add(row(id).orElseThrow().version());
    }
    int before = bus.commands().size();

    bus.clearFailure("b");
    runner.asEventListener().onEvents(page);

    assertThat(bus.commands().subList(before, bus.commands().size()))
        .containsExactly(new Step("b", 2));
    List<Long> after = new ArrayList<>();
    for (String id : List.of("a", "c", "d")) {
      after.add(row(id).orElseThrow().version());
      assertThat(row(id).orElseThrow().state().applied()).as(id).hasSize(1);
    }
    assertThat(after).isEqualTo(versions);
    assertThat(row("b").orElseThrow().state().applied()).containsExactly(2L);
  }

  // ==================== Poison ====================

  /** The test saga with one of its methods replaced. */
  private static SagaOrchestrator<ParState> sagaWith(
      UnaryOperator<SagaOrchestrator<ParState>> change) {
    return change.apply(orchestrator());
  }

  @Test
  void poisonInOneSagaQuarantinesIt_andThePageIsAcknowledged() {
    var saga =
        sagaWith(
            d ->
                new Delegating(d) {
                  @Override
                  public ParState evolve(ParState state, EventEnvelope event) {
                    if (state.sagaId().equals("b")) {
                      throw new IllegalStateException("poison for b");
                    }
                    return super.evolve(state, event);
                  }
                });
    var runner = builder().orchestrator(saga).parallelism(4).build();

    runner.asEventListener().onEvents(page(1, started("a", "b", "c")));

    assertThat(row("b").orElseThrow().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(deadLetters.findBySaga(SagaId.of("b"))).hasSize(1);
    assertThat(row("a").orElseThrow().state().applied()).containsExactly(1L);
    assertThat(row("c").orElseThrow().state().applied()).containsExactly(3L);
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
  }

  private final Thread testThread = Thread.currentThread();

  @Test
  void aRoutingPoisonIsQuarantinedBeforeAnyStepStarts() {
    var saga =
        sagaWith(
            d ->
                new Delegating(d) {
                  @Override
                  public SagaId extractSagaId(EventEnvelope event) {
                    return ((Started) event.event()).id().equals("nul")
                        ? null
                        : super.extractSagaId(event);
                  }
                });
    // when the null-saga entry is written: commands seen, steps started, and on which thread
    List<String> whenQuarantined = new ArrayList<>();
    var recordingDeadLetters =
        new org.streamrune.test.ForwardingSagaDeadLetterStore(deadLetters) {
          @Override
          public void publish(SagaDeadLetterEntry entry) {
            if (entry.sagaId() == null) {
              whenQuarantined.add(snapshot());
            }
            super.publish(entry);
          }

          @Override
          public void publishShielded(SagaDeadLetterEntry entry) {
            if (entry.sagaId() == null) {
              whenQuarantined.add(snapshot());
            }
            super.publishShielded(entry);
          }

          private String snapshot() {
            return bus.entered()
                + " commands, "
                + inFlightSamples()
                + " step calls, "
                + (Thread.currentThread() == testThread ? "delivering thread" : "another thread");
          }
        };
    var runner =
        builder()
            .orchestrator(saga)
            .sagaDeadLetterStore(recordingDeadLetters)
            .parallelism(4)
            .build();

    runner.asEventListener().onEvents(page(1, started("a", "nul", "c")));

    assertThat(whenQuarantined).containsExactly("0 commands, 0 step calls, delivering thread");
    assertThat(row("a").orElseThrow().genesisApplied()).isTrue();
    assertThat(row("c").orElseThrow().genesisApplied()).isTrue();
  }

  /** A saga that names no saga for the event of {@code "nul"}: routing quarantines it. */
  private SagaOrchestrator<ParState> sagaWithARoutingPoison() {
    return sagaWith(
        d ->
            new Delegating(d) {
              @Override
              public SagaId extractSagaId(EventEnvelope event) {
                return ((Started) event.event()).id().equals("nul")
                    ? null
                    : super.extractSagaId(event);
              }
            });
  }

  /** A dead-letter store whose null-saga writes throw {@code failure}. */
  private org.streamrune.test.ForwardingSagaDeadLetterStore quarantineFailing(
      RuntimeException failure) {
    return new org.streamrune.test.ForwardingSagaDeadLetterStore(deadLetters) {
      @Override
      public void publish(SagaDeadLetterEntry entry) {
        if (entry.sagaId() == null) {
          throw failure;
        }
        super.publish(entry);
      }

      @Override
      public void publishShielded(SagaDeadLetterEntry entry) {
        if (entry.sagaId() == null) {
          throw failure;
        }
        super.publishShielded(entry);
      }
    };
  }

  @Test
  void aPageWhoseRoutingFailsIsCountedAndLoggedAsAFailedPage() {
    var deadLettersDown = new IllegalStateException("dead letters down");
    var runner =
        builder()
            .orchestrator(sagaWithARoutingPoison())
            .sagaDeadLetterStore(quarantineFailing(deadLettersDown))
            .parallelism(4)
            .build();

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "nul", "c"))))
        .isSameAs(deadLettersDown);

    assertThat(bus.entered()).as("no step started").isZero();
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
    assertThat(pageWarns())
        .singleElement()
        .asString()
        .contains("a page of 3 event(s) was not acknowledged: routing it failed before any step")
        .contains("dead letters down");
  }

  @Test
  void aPageWhoseRoutingFailsOnAConnectionIsTaggedConnection() {
    var connectionDown = new EventStoreException("down", new SQLException("down", "08006"));
    var runner =
        builder()
            .orchestrator(sagaWithARoutingPoison())
            .sagaDeadLetterStore(quarantineFailing(connectionDown))
            .parallelism(4)
            .build();

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "nul", "c"))))
        .isSameAs(connectionDown);

    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION)).isEqualTo(1);
    assertThat(pageWarns()).singleElement().asString().contains("on a connection-class failure");
  }

  @Test
  void anErrorOfTheMetricsBackendAfterARoutingFailureLeavesWithTheRoutingFailureSuppressed() {
    var deadLettersDown = new IllegalStateException("dead letters down");
    var metricsError = new AssertionError("metrics down");
    var runner =
        builder()
            .orchestrator(sagaWithARoutingPoison())
            .sagaDeadLetterStore(quarantineFailing(deadLettersDown))
            .metrics(
                new org.streamrune.test.RecordingStreamRuneMetrics() {
                  @Override
                  public void recordSagaPageFailed(String sagaType, String stop) {
                    throw metricsError;
                  }
                })
            .parallelism(4)
            .build();

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "nul", "c"))))
        .isSameAs(metricsError)
        .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(deadLettersDown));
  }

  @Test
  void aPageWhoseRoutingFailsAtParallelismOneRecordsAndLogsNothing() {
    var deadLettersDown = new IllegalStateException("dead letters down");
    var runner =
        builder()
            .orchestrator(sagaWithARoutingPoison())
            .sagaDeadLetterStore(quarantineFailing(deadLettersDown))
            .parallelism(1)
            .build();

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "nul", "c"))))
        .isSameAs(deadLettersDown);

    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
    assertThat(pageWarns()).isEmpty();
  }

  // ==================== Errors ====================

  @Test
  void anErrorInAStepLeavesTheListenerAsAnError_andRecordsNoFailedPage() throws Exception {
    var runner = runner(4);
    bus.gate("a");
    bus.failing("b", () -> new AssertionError("boom"));

    var done = deliverInBackground(runner, page(1, started("a", "b", "c")));

    bus.gate("a").awaitEntered();
    awaitUntil(() -> !bus.commandsOf("b").isEmpty(), "b's command");
    Thread.sleep(100);
    assertThat(done).as("the listener waits for a, in flight").isNotDone();
    bus.gate("a").release();

    assertThat(outcome(done)).isInstanceOf(AssertionError.class).hasMessage("boom");
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
    assertThat(pageWarns()).isEmpty();
  }

  // ==================== Shared state, bound, metrics ====================

  @Test
  void theRunnerSharesNoMutableStateBetweenSteps() {
    for (int repetition = 0; repetition < 20; repetition++) {
      try (World world = new World()) {
        String[] ids = sagas("r" + repetition + "-s", 200);
        List<EventEnvelope> page = world.append(started(ids));
        var runner = world.runner().parallelism(16).build();

        runner.asEventListener().onEvents(page);

        EndState end = world.endState(ProjectionName.of("unused"));
        assertThat(end.rowsBySaga()).hasSize(200);
        for (EventEnvelope event : page) {
          String id = ((Started) event.event()).id();
          long offset = event.globalOffset().value();
          EndState.Row row = end.rowsBySaga().get(id);
          assertThat(row.state().sagaId()).as(id).isEqualTo(id);
          assertThat(row.state().applied()).as(id).containsExactly(offset);
          assertThat(end.commandsBySaga().get(id)).as(id).containsExactly(new Step(id, offset));
          assertThat(end.eventsByAggregate().get(StreamId.of(STEPS, AggregateId.of(id))))
              .as(id)
              .containsExactly(new Stepped(id, offset));
          assertThat(end.inboxKeys())
              .contains(SagaCommandDispatch.forwardKey(SagaId.of(id), offset, 0));
        }
        assertThat(end.inboxKeys()).hasSize(200);
        assertThat(end.deadLettersBySaga()).isEmpty();
      }
    }
  }

  @Test
  void stepsInFlightReachParallelismAndNeverExceedIt() throws Exception {
    var runner = runner(4);
    bus.gateEverySaga();

    var done = deliverInBackground(runner, page(1, started(sagas("s", 50))));

    awaitUntil(() -> bus.entered() == 4, "four commands");
    Thread.sleep(100);
    assertThat(bus.entered()).as("no fifth while four are held").isEqualTo(4);
    bus.releaseAll();

    assertThat(outcome(done)).isNull();
    assertThat(bus.entered()).isEqualTo(50);
    assertThat(metrics.maxSagaStepsInFlight()).isEqualTo(4);
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(0);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(0);
  }

  @Test
  void effectiveParallelismIsRecordedForAFannedOutPage_andNotForAnInlineOne() {
    var runner = runner(4);

    runner.asEventListener().onEvents(page(1, started("a", "b")));
    assertThat(metrics.sagaPageEffectiveParallelism())
        .singleElement()
        .satisfies(value -> assertThat(value).isPositive());

    runner.asEventListener().onEvents(page(3, new Started("c"), new Replied("c")));
    assertThat(metrics.sagaPageEffectiveParallelism()).as("one saga: inline").hasSize(1);

    runner(1).asEventListener().onEvents(page(5, started("d", "e")));
    assertThat(metrics.sagaPageEffectiveParallelism()).as("parallelism 1").hasSize(1);
  }

  @Test
  void theGaugesAreZeroAfterAStoppedPage() throws Exception {
    var runner = runner(2);
    bus.failing("s1", () -> new EventStoreException("x", new SQLException("down", "08006")));

    sixSagasWhereS1FailsWhileS2IsHeld(runner);

    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(0);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(0);
  }

  // ==================== A throwing metrics backend or classifier ====================

  @Test
  void aThrowingMetricsBackendNeverReplacesThePagesFailure() {
    var throwing =
        new org.streamrune.test.RecordingStreamRuneMetrics() {
          @Override
          public void recordSagaPageFailed(String sagaType, String stop) {
            throw new IllegalStateException("metrics down");
          }

          @Override
          public void recordSagaPageEffectiveParallelism(String sagaType, double value) {
            throw new IllegalStateException("metrics down");
          }
        };
    var runner = builder().metrics(throwing).parallelism(4).build();
    bus.failing("b", () -> new CircuitBreakerOpenException("open"));

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "b"))))
        .isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(pageWarns()).hasSize(1);
  }

  @Test
  void aThrowingClassifierAtTheTagNeverReplacesThePagesFailure() {
    var runner =
        builder()
            .parallelism(4)
            .connectionFailureClassifier(
                f -> {
                  throw new IllegalStateException("classifier down");
                })
            .build();
    bus.failing("a", () -> new CircuitBreakerOpenException("open"));

    // one saga: the rule is not asked, so the tag is the classifier's first call
    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, new Started("a"))))
        .isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
  }

  @Test
  void anErrorFromTheMetricsBackendAfterAFailedPageLeaves_withThePagesFailureSuppressed() {
    var throwing =
        new org.streamrune.test.RecordingStreamRuneMetrics() {
          @Override
          public void recordSagaPageFailed(String sagaType, String stop) {
            throw new AssertionError("metrics error");
          }
        };
    var runner = builder().metrics(throwing).parallelism(4).build();
    bus.failing("b", () -> new CircuitBreakerOpenException("open"));

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "b"))))
        .isInstanceOf(AssertionError.class)
        .hasMessage("metrics error")
        .satisfies(
            t ->
                assertThat(t.getSuppressed())
                    .singleElement()
                    .isInstanceOf(CircuitBreakerOpenException.class));
  }

  @Test
  void anErrorFromTheClassifierAfterAFailedPageLeaves_withThePagesFailureSuppressed() {
    var runner =
        builder()
            .parallelism(4)
            .connectionFailureClassifier(
                f -> {
                  throw new AssertionError("classifier error");
                })
            .build();
    bus.failing("a", () -> new CircuitBreakerOpenException("open"));

    // one saga: the rule is not asked, so the classifier's first call is the stop tag's
    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, new Started("a"))))
        .isInstanceOf(AssertionError.class)
        .hasMessage("classifier error")
        .satisfies(
            t ->
                assertThat(t.getSuppressed())
                    .singleElement()
                    .isInstanceOf(CircuitBreakerOpenException.class));
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(0);
  }

  // ==================== Pages that must not be acknowledged ====================

  /** A tracing whose propagating wrapper drops its task: a worker thread then runs nothing. */
  private static final StreamRuneTracing DROPPING =
      new StreamRuneTracing() {
        @Override
        public boolean enabled(TracingArea area) {
          return false;
        }

        @Override
        public Runnable propagating(Runnable task) {
          return () -> {};
        }

        @Override
        public TraceScope start(
            TracingArea area,
            String spanName,
            SpanKind kind,
            SpanAttributes attributes,
            SpanLink link,
            SpanBaggage baggage) {
          return TraceScope.NOOP;
        }
      };

  @Test
  void aPageWhoseWorkersDropTheirTaskIsNotAcknowledged() {
    var runner = builder().tracing(DROPPING).parallelism(4).build();

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "b"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("2 of 2 worker(s) of the page ended without running their items");

    assertThat(bus.commands()).isEmpty();
    assertThat(row("a")).isEmpty();
    assertThat(row("b")).isEmpty();
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_NONE)).isEqualTo(1);
    assertThat(pageWarns())
        .singleElement()
        .asString()
        .contains("although none of its steps failed: 2 of 2 worker(s) of the page ended")
        .contains("2 not started");
  }

  @Test
  void atParallelismOneTheDroppingTracingRunsNoWorker_andThePageRuns() {
    var runner = builder().tracing(DROPPING).parallelism(1).build();

    runner.asEventListener().onEvents(page(1, started("a", "b")));

    assertThat(bus.sagas()).containsExactly("a", "b");
    assertThat(row("a").orElseThrow().genesisApplied()).isTrue();
  }

  @Test
  void aWorkerThatCannotStartFailsThePage_andTheGaugesAreZeroed() {
    var runner =
        builder()
            .parallelism(2)
            .workerThreads(
                (n, task) -> {
                  if (n == 0) {
                    return Thread.ofVirtual().unstarted(task);
                  }
                  Thread finished = Thread.ofVirtual().start(() -> {});
                  try {
                    finished.join();
                  } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                  }
                  return finished; // its start() throws IllegalThreadStateException
                })
            .build();

    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started(sagas("s", 4)))))
        .isInstanceOf(IllegalThreadStateException.class);

    // whatever the started worker ran before the page stopped, the gauges end at 0
    assertThat(metrics.lastGauge(MetricNames.SAGA_STEPS_IN_FLIGHT)).hasValue(0);
    assertThat(metrics.lastGauge(MetricNames.SAGA_PAGE_PENDING)).hasValue(0);
  }

  @Test
  void aCheckedExceptionOfAStepLeavesTheListenerAsItWasThrown() {
    SagaStore throwingChecked =
        new org.streamrune.test.ForwardingSagaStore(rows) {
          @Override
          public <S extends SagaState> Optional<LoadedSaga<S>> load(
              SagaId sagaId, SagaType sagaType, Class<S> stateType) {
            throw SagaRunnerParallelTest.<RuntimeException>sneaky(new IOException("disk"));
          }
        };
    for (int parallelism : new int[] {1, 4}) {
      var runner = builder(throwingChecked).parallelism(parallelism).build();

      assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "b"))))
          .as("parallelism %d", parallelism)
          .isInstanceOf(IOException.class)
          .hasMessage("disk");
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T sneaky(Throwable failure) throws T {
    throw (T) failure;
  }

  @Test
  void aStoreWhoseClassifierReadThrowsOnAWorkerFailsAndStopsThePage() throws Exception {
    SagaStore throwingRead =
        new org.streamrune.test.ForwardingSagaStore(rows) {
          @Override
          public ConnectionFailureClassifier connectionFailureClassifier() {
            throw new IllegalStateException("no classifier");
          }
        };
    var runner = builder(throwingRead).parallelism(2).build();
    bus.failing("s1", () -> new CircuitBreakerOpenException("open"));

    Throwable failure = sixSagasWhereS1FailsWhileS2IsHeld(runner);

    assertThat(failure).isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(failure.getSuppressed())
        .anyMatch(
            s -> s instanceof IllegalStateException && "no classifier".equals(s.getMessage()));
    assertThat(bus.sagas()).containsExactlyInAnyOrder("s1", "s2");
    assertThat(metrics.samples(MetricNames.SAGA_PAGE_FAILED)).isEmpty();
  }

  @Test
  void aFailingStepGaugeIsLoggedOnce_onTheDeliveringThread() {
    var throwing =
        new org.streamrune.test.RecordingStreamRuneMetrics() {
          @Override
          public void recordSagaStepsInFlight(String sagaType, int inFlight, int pending) {
            throw new IllegalStateException("gauge down");
          }
        };
    var runner = builder().metrics(throwing).parallelism(4).build();

    runner.asEventListener().onEvents(page(1, started("a", "b", "c")));
    runner.asEventListener().onEvents(page(4, started("d", "e")));

    assertThat(appender.list)
        .filteredOn(e -> e.getFormattedMessage().contains("steps in flight failed"))
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.getThreadName()).isEqualTo(Thread.currentThread().getName());
              assertThat(e.getFormattedMessage()).contains("failed 7 time(s)");
            });
    assertThat(appender.list)
        .noneMatch(e -> e.getFormattedMessage().startsWith("Metrics recording failed"));
    assertThat(row("e").orElseThrow().genesisApplied()).isTrue();
  }

  @Test
  void anErrorFromTheMetricsBackendAfterAPageThatRanLeavesTheListener() {
    var throwing =
        new org.streamrune.test.RecordingStreamRuneMetrics() {
          @Override
          public void recordSagaPageEffectiveParallelism(String sagaType, double value) {
            throw new AssertionError("metrics error");
          }
        };
    var runner = builder().metrics(throwing).parallelism(4).build();

    // the page is not acknowledged; every item ran, so its redelivery is answered by the rows
    assertThatThrownBy(() -> runner.asEventListener().onEvents(page(1, started("a", "b"))))
        .isInstanceOf(AssertionError.class)
        .hasMessage("metrics error");
    assertThat(row("a").orElseThrow().genesisApplied()).isTrue();
    assertThat(row("b").orElseThrow().genesisApplied()).isTrue();
  }

  // ==================== With a subscription ====================

  @Test
  void closeWaitsForTheItemsInFlightOfAFailedPage() throws Exception {
    try (World world = new World()) {
      var runner =
          SagaRunner.<ParState>builder()
              .orchestrator(orchestrator())
              .sagaStore(world.rows)
              .commandBus(bus)
              .sagaDeadLetterStore(world.deadLetters)
              .forwardFence(false)
              .parallelism(4)
              .build();
      world.append(started("a", "b"));
      bus.failing("a", () -> new CircuitBreakerOpenException("open"));
      bus.gate("b");
      SubscriptionName name = SubscriptionName.of("par-close");
      var subscription =
          PollingEventSubscription.builder()
              .subscriptionName(name)
              .eventStore(world.eventStore)
              .offsetStore(world.offsets)
              .sagaRunner(runner)
              .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ZERO))
              .build();
      subscription.start();
      bus.gate("b").awaitEntered();
      awaitUntil(() -> !bus.commandsOf("a").isEmpty(), "a's failed command");

      Thread closer = Thread.ofVirtual().start(subscription::close);
      awaitUntil(
          () ->
              closer.getState() == Thread.State.WAITING
                  || closer.getState() == Thread.State.TIMED_WAITING,
          "close() to wait");
      assertThat(closer.join(Duration.ofMillis(200))).as("close() waits for b").isFalse();

      bus.gate("b").release();

      assertThat(closer.join(AWAIT)).as("close() returns once b ended").isTrue();
      assertThat(world.offsets.getLastOffset(ProjectionName.of(name.value())).value())
          .as("the failed page is not acknowledged")
          .isZero();
      assertThat(row(world, "b").genesisApplied()).isTrue();
    }
  }

  @Test
  void aSubscriptionOverAWorldRecordsTheRepliesOfItsOwnCommands_andCompensatesARejection() {
    try (World world = new World()) {
      var runner = world.runner().parallelism(4).build();
      world.rejecting.add("c");
      List<EventEnvelope> started = world.append(started("a", "b", "c"));
      SubscriptionName name = SubscriptionName.of("par-world");
      ProjectionName checkpoint = ProjectionName.of(name.value());
      var subscription = world.subscription(name, runner);
      subscription.start();
      try {
        // three starts, two Stepped replies, one Undone reply, all delivered
        awaitUntil(
            () -> world.stored().size() == 6 && world.endState(checkpoint).checkpoint() == 6,
            "every event delivered");
      } finally {
        subscription.close();
      }

      EndState end = world.endState(checkpoint);
      assertThat(end.deadLettersBySaga()).as("no saga faulted on its own reply").isEmpty();
      for (String id : List.of("a", "b")) {
        assertThat(end.rowsBySaga().get(id).status()).as(id).isEqualTo(SagaStatus.RUNNING);
        assertThat(end.rowsBySaga().get(id).state().applied()).as(id).hasSize(2);
      }
      assertThat(end.rowsBySaga().get("c").status()).isEqualTo(SagaStatus.COMPENSATED);
      long offsetOfC = started.get(2).globalOffset().value();
      assertThat(end.commandsBySaga().get("c"))
          .containsExactly(new Step("c", offsetOfC), new SagaParallelFixtures.Undo("c"));
      assertThat(end.inboxKeys())
          .contains(
              SagaCommandDispatch.forwardKey(
                  SagaId.of("a"), started.get(0).globalOffset().value(), 0))
          .hasSize(3); // a's and b's forward keys and c's compensation key
    }
  }

  @Test
  void theGatedBusHoldsOneCommandOfASaga_afterItsFirstPassed() throws Exception {
    var runner = runner(4);
    bus.gate("a", 3);

    var done =
        deliverInBackground(runner, page(1, new Started("a"), new Started("b"), new Replied("a")));

    bus.gate("a", 3).awaitEntered();
    assertThat(row("a").orElseThrow().state().applied()).containsExactly(1L);
    assertThat(done).isNotDone();
    bus.releaseAll();
    assertThat(outcome(done)).isNull();
    assertThat(EndState.of(rows, deadLetters, bus).commandsBySaga().get("a"))
        .containsExactly(new Step("a", 1), new Step("a", 3));
    assertThat(EndState.of(rows, deadLetters, bus).inboxKeys()).hasSize(3);
  }

  private static LoadedSaga<ParState> row(World world, String id) {
    return world.rows.load(SagaId.of(id), SAGA_TYPE, ParState.class).orElseThrow();
  }
}
