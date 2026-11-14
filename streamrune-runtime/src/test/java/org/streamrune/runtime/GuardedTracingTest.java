package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.runtime.ThrowingTracing.Fault;

/** {@link GuardedTracing}: nothing the tracing it wraps throws reaches the traced work. */
class GuardedTracingTest {

  private static final SpanAttributes NONE = SpanAttributes.empty();

  private static TraceScope start(StreamRuneTracing tracing) {
    return tracing.start(TracingArea.COMMAND, "command/Test", SpanKind.INTERNAL, NONE);
  }

  /** Every operation of the seam and of a scope, driven through {@code tracing}. */
  private static void exerciseEverything(StreamRuneTracing tracing) {
    tracing.enabled(TracingArea.COMMAND);
    try (TraceScope scope = start(tracing)) {
      scope.setAttribute("a", "b");
      scope.setAttribute("a", 1L);
      scope.setAttribute("a", true);
      scope.recordSuccess();
      scope.recordError(new IllegalStateException("work failed"));
      assertThat(scope.link()).isNotNull();
    }
    tracing.start(TracingArea.COMMAND, "s", SpanKind.INTERNAL, NONE, Instant.now()).close();
    tracing.startChild(TracingArea.COMMAND, "s", SpanKind.INTERNAL, NONE, Instant.now()).close();
    tracing
        .start(TracingArea.COMMAND, "s", SpanKind.INTERNAL, NONE, SpanLink.NONE, SpanBaggage.KEEP)
        .close();
    tracing.propagating(() -> {}).run();
  }

  @Test
  void noopAndAnAlreadyGuardedTracingAreReturnedAsTheyAre() {
    assertThat(GuardedTracing.of(StreamRuneTracing.NOOP)).isSameAs(StreamRuneTracing.NOOP);
    StreamRuneTracing guarded = GuardedTracing.of(ThrowingTracing.on());
    assertThat(GuardedTracing.of(guarded)).isSameAs(guarded);
    assertThatThrownBy(() -> GuardedTracing.of(null)).isInstanceOf(NullPointerException.class);
  }

  @ParameterizedTest
  @EnumSource(Fault.class)
  void noOperationThatThrowsReachesTheCaller(Fault fault) {
    ThrowingTracing broken = ThrowingTracing.on(fault);
    StreamRuneTracing guarded = GuardedTracing.of(broken);

    assertThatCode(() -> exerciseEverything(guarded)).doesNotThrowAnyException();

    assertThat(broken.thrown.get()).as("the tracing did throw").isPositive();
    assertThat(((GuardedTracing) guarded).failureCount()).isEqualTo(broken.thrown.get());
  }

  @ParameterizedTest
  @EnumSource(Fault.class)
  void anErrorOfTheTracingIsCaughtToo(Fault fault) {
    ThrowingTracing broken =
        ThrowingTracing.throwing(() -> new StackOverflowError("in the tracing"), fault);

    assertThatCode(() -> exerciseEverything(GuardedTracing.of(broken))).doesNotThrowAnyException();
    assertThat(broken.thrown.get()).isPositive();
  }

  @Test
  void aStartThatThrowsGivesTheNoopScope_andSoDoesANullScope() {
    assertThat(start(GuardedTracing.of(ThrowingTracing.on(Fault.START)))).isSameAs(TraceScope.NOOP);
    StreamRuneTracing returnsNull =
        new StreamRuneTracing() {
          @Override
          public boolean enabled(TracingArea area) {
            return true;
          }

          @Override
          public TraceScope start(
              TracingArea area,
              String spanName,
              SpanKind kind,
              SpanAttributes attributes,
              SpanLink link,
              SpanBaggage baggage) {
            return null;
          }

          @Override
          public Runnable propagating(Runnable task) {
            return null;
          }
        };
    StreamRuneTracing guarded = GuardedTracing.of(returnsNull);
    assertThat(start(guarded)).isSameAs(TraceScope.NOOP);
    Runnable task = () -> {};
    assertThat(guarded.propagating(task)).isSameAs(task);
  }

  @Test
  void anEnabledThatThrowsAnswersFalse_andALinkThatThrowsOrIsNullIsNone() {
    assertThat(GuardedTracing.of(ThrowingTracing.on(Fault.ENABLED)).enabled(TracingArea.SAGA))
        .isFalse();
    try (TraceScope scope = start(GuardedTracing.of(ThrowingTracing.on(Fault.LINK)))) {
      assertThat(scope.link()).isSameAs(SpanLink.NONE);
    }
  }

  @Test
  void theScopeOfABrokenTracingIsStillClosedOnce() {
    ThrowingTracing broken = ThrowingTracing.onEveryScopeMethod();
    try (TraceScope scope = start(GuardedTracing.of(broken))) {
      scope.recordSuccess();
    }
    assertThat(broken.started.get()).isEqualTo(1);
    assertThat(broken.closed.get()).isEqualTo(1);
  }

  @Test
  void nullArgumentsTheSeamRejectsAreStillRejected() {
    StreamRuneTracing guarded = GuardedTracing.of(ThrowingTracing.on());
    assertThatThrownBy(
            () ->
                guarded.start(
                    TracingArea.COMMAND, "s", SpanKind.INTERNAL, NONE, null, SpanBaggage.KEEP))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () ->
                guarded.start(
                    TracingArea.COMMAND, "s", SpanKind.INTERNAL, NONE, SpanLink.NONE, null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () -> guarded.start(TracingArea.COMMAND, "s", SpanKind.INTERNAL, NONE, (Instant) null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () -> guarded.startChild(TracingArea.COMMAND, "s", SpanKind.INTERNAL, NONE, null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> guarded.propagating(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void toStringIsTheGuardedTracingsOwn() {
    StreamRuneTracing named =
        new StreamRuneTracing() {
          @Override
          public boolean enabled(TracingArea area) {
            return true;
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

          @Override
          public String toString() {
            return "MyTracing";
          }
        };
    assertThat(GuardedTracing.of(named)).hasToString("MyTracing");
  }

  // ── propagating ────────────────────────────────────────────────────────────────────────────

  /** A tracing whose {@code propagating} wraps the task in {@code around}. */
  private static StreamRuneTracing propagatingThrough(
      java.util.function.UnaryOperator<Runnable> around) {
    return new StreamRuneTracing() {
      @Override
      public boolean enabled(TracingArea area) {
        return true;
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

      @Override
      public Runnable propagating(Runnable task) {
        return around.apply(task);
      }
    };
  }

  @Test
  void aPropagatingThatThrowsGivesTheTaskItself() {
    Runnable task = () -> {};
    assertThat(GuardedTracing.of(ThrowingTracing.on(Fault.PROPAGATING)).propagating(task))
        .isSameAs(task);
    assertThat(GuardedTracing.of(ThrowingTracing.on()).propagating(task))
        .as("a tracing that propagates nothing adds no wrapper")
        .isSameAs(task);
  }

  @Test
  void aWrapperThatThrowsBeforeTheTaskStillRunsTheTaskOnce() {
    AtomicInteger runs = new AtomicInteger();
    StreamRuneTracing guarded =
        GuardedTracing.of(
            propagatingThrough(
                task ->
                    () -> {
                      throw new IllegalStateException("the context cannot be made current");
                    }));

    Runnable propagated = guarded.propagating(runs::incrementAndGet);
    assertThatCode(propagated::run).doesNotThrowAnyException();
    assertThat(runs.get()).isEqualTo(1);
    // The wrapper is reusable: each invocation is judged on its own.
    assertThatCode(propagated::run).doesNotThrowAnyException();
    assertThat(runs.get()).isEqualTo(2);
  }

  @Test
  void aWrapperThatReturnsNormallyIsNeverFollowedByASecondRun() throws Exception {
    AtomicInteger runs = new AtomicInteger();
    // A wrapper that hands the task to another thread and returns: the guard sees no run on its
    // own thread and must not add one.
    java.util.List<Thread> threads = new java.util.concurrent.CopyOnWriteArrayList<>();
    StreamRuneTracing guarded =
        GuardedTracing.of(
            propagatingThrough(task -> () -> threads.add(Thread.ofVirtual().start(task))));

    guarded.propagating(runs::incrementAndGet).run();
    for (Thread thread : threads) {
      thread.join(10_000);
    }

    assertThat(runs.get()).isEqualTo(1);
  }

  @Test
  void aWrapperThatThrowsAfterTheTaskIsDropped_andTheTaskRanOnce() {
    AtomicInteger runs = new AtomicInteger();
    StreamRuneTracing guarded =
        GuardedTracing.of(
            propagatingThrough(
                task ->
                    () -> {
                      task.run();
                      throw new IllegalStateException("the scope could not be closed");
                    }));

    assertThatCode(guarded.propagating(runs::incrementAndGet)::run).doesNotThrowAnyException();
    assertThat(runs.get()).isEqualTo(1);
  }

  @Test
  void whatTheTaskThrowsPassesThroughUnchanged_anErrorToo() {
    StreamRuneTracing guarded = GuardedTracing.of(propagatingThrough(task -> task::run));
    AssertionError error = new AssertionError("the task's own");
    assertThatThrownBy(
            guarded.propagating(
                    () -> {
                      throw error;
                    })
                ::run)
        .isSameAs(error);
    assertThat(((GuardedTracing) guarded).failureCount()).isZero();
  }

  @Test
  void aWrapperThatReplacesTheTasksFailureDoesNotHideIt() {
    StreamRuneTracing guarded =
        GuardedTracing.of(
            propagatingThrough(
                task ->
                    () -> {
                      try {
                        task.run();
                      } finally {
                        throw new IllegalStateException("the scope could not be closed");
                      }
                    }));
    IllegalArgumentException own = new IllegalArgumentException("the task's own");

    assertThatThrownBy(
            guarded.propagating(
                    () -> {
                      throw own;
                    })
                ::run)
        .isSameAs(own);
  }

  @Test
  void aPropagatedTaskThatRunsAnotherPropagatedTaskKeepsBothInvocationsApart() {
    AtomicInteger inner = new AtomicInteger();
    AtomicInteger outer = new AtomicInteger();
    StreamRuneTracing guarded =
        GuardedTracing.of(
            propagatingThrough(
                task ->
                    () -> {
                      task.run();
                      throw new IllegalStateException("after the task");
                    }));
    Runnable innerTask = guarded.propagating(inner::incrementAndGet);

    guarded
        .propagating(
            () -> {
              innerTask.run();
              outer.incrementAndGet();
            })
        .run();

    assertThat(inner.get()).isEqualTo(1);
    assertThat(outer.get()).isEqualTo(1);
  }

  @Test
  void aWrapperThatHandsTheTaskOffAndThenThrowsIsNotFollowedByASecondRun() {
    AtomicInteger runs = new AtomicInteger();
    StreamRuneTracing guarded =
        GuardedTracing.of(
            propagatingThrough(
                task ->
                    () -> {
                      Thread thread = Thread.ofVirtual().start(task);
                      try {
                        thread.join();
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      throw new IllegalStateException("the scope could not be closed");
                    }));

    assertThatCode(guarded.propagating(runs::incrementAndGet)::run).doesNotThrowAnyException();

    assertThat(runs.get()).isEqualTo(1);
  }

  // ── one guard per tracing, interrupts, metrics ─────────────────────────────────────────────

  @Test
  void aGuardIsWhatItsArgumentsSay_nothingIsRememberedBetweenCalls() {
    ThrowingTracing broken = ThrowingTracing.on(Fault.START);
    StreamRuneTracing first = GuardedTracing.of(broken);

    assertThat(GuardedTracing.of(first)).as("a guard is returned as it is").isSameAs(first);
    assertThat(GuardedTracing.of(broken))
        .as("the same tracing, asked again, gets a guard of its own")
        .isNotSameAs(first);

    start(first).close();
    start(GuardedTracing.of(broken)).close();
    assertThat(((GuardedTracing) first).failureCount()).isEqualTo(1);
  }

  private static long counted(
      org.streamrune.test.RecordingStreamRuneMetrics metrics, String operation) {
    return metrics.total(
        org.streamrune.core.metrics.MetricNames.TRACING_FAILURES,
        org.streamrune.core.metrics.MetricNames.TAG_OPERATION,
        operation);
  }

  @Test
  void aGuardCountsInTheMetricsItIsBoundTo() {
    org.streamrune.test.RecordingStreamRuneMetrics metrics =
        new org.streamrune.test.RecordingStreamRuneMetrics();
    org.streamrune.test.RecordingStreamRuneMetrics other =
        new org.streamrune.test.RecordingStreamRuneMetrics();
    ThrowingTracing broken = ThrowingTracing.on(Fault.START, Fault.PROPAGATING);

    // A tracing that is not guarded gets a new guard that counts in the metrics.
    StreamRuneTracing bound = GuardedTracing.of(broken, metrics);
    assertThat(GuardedTracing.of(broken, metrics)).isNotSameAs(bound);
    // A guard that already counts there is returned as it is.
    assertThat(GuardedTracing.of(bound, metrics)).isSameAs(bound);
    // A guard that counts elsewhere keeps its metrics; the tracing gets another guard for these.
    StreamRuneTracing forOther = GuardedTracing.of(bound, other);
    assertThat(forOther).isNotSameAs(bound);

    start(bound).close();
    bound.propagating(() -> {}).run();
    start(forOther).close();

    assertThat(counted(metrics, "start")).isEqualTo(1);
    assertThat(counted(metrics, "propagating")).isEqualTo(1);
    assertThat(counted(other, "start")).isEqualTo(1);
    assertThat(counted(other, "propagating")).isZero();
  }

  @Test
  void noGuardIsEverRebound_aGuardForNewMetricsSharesTheCountAndTheWarnStream() {
    Logger logger = (Logger) LoggerFactory.getLogger(GuardedTracing.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      org.streamrune.test.RecordingStreamRuneMetrics first =
          new org.streamrune.test.RecordingStreamRuneMetrics();
      org.streamrune.test.RecordingStreamRuneMetrics second =
          new org.streamrune.test.RecordingStreamRuneMetrics();
      // The guard a component was handed without metrics, as OpenTelemetryTracing.of returns it.
      StreamRuneTracing unbound = GuardedTracing.of(ThrowingTracing.on(Fault.START));

      StreamRuneTracing forFirst = GuardedTracing.of(unbound, first);
      StreamRuneTracing forSecond = GuardedTracing.of(forFirst, second);
      assertThat(forFirst).as("a new guard: the unbound one is not changed").isNotSameAs(unbound);
      assertThat(forSecond).isNotSameAs(forFirst);
      assertThat(GuardedTracing.of(forFirst, first)).isSameAs(forFirst);

      start(unbound).close();
      start(forFirst).close();
      start(forSecond).close();
      start(forSecond).close();

      // Each counts in the metrics it was made for, and in no other.
      assertThat(counted(first, "start")).isEqualTo(1);
      assertThat(counted(second, "start")).isEqualTo(2);
      // One failure count and one WARN stream for the tracing, however often it was handed on.
      assertThat(((GuardedTracing) unbound).failureCount()).isEqualTo(4);
      assertThat(((GuardedTracing) forSecond).failureCount()).isEqualTo(4);
      assertThat(appender.list).hasSize(1);
    } finally {
      logger.detachAppender(appender);
    }
  }

  @Test
  void withNoMetricsToCountInTheGuardCountsNowhere_andNoopStaysNoop() {
    ThrowingTracing broken = ThrowingTracing.on(Fault.START);
    StreamRuneTracing guard = GuardedTracing.of(broken);

    assertThat(GuardedTracing.of(guard, null)).isSameAs(guard);
    assertThat(GuardedTracing.of(guard, org.streamrune.core.StreamRuneMetrics.NOOP))
        .isSameAs(guard);
    assertThat(GuardedTracing.of(broken, null)).isInstanceOf(GuardedTracing.class);
    assertThat(
            GuardedTracing.of(
                StreamRuneTracing.NOOP, new org.streamrune.test.RecordingStreamRuneMetrics()))
        .isSameAs(StreamRuneTracing.NOOP);
  }

  @Test
  void theOperationTagIsExactlyTheDocumentedNine() {
    org.streamrune.test.RecordingStreamRuneMetrics metrics =
        new org.streamrune.test.RecordingStreamRuneMetrics();

    // A tracing whose start throws hands out no scope, so the scope's operations get a tracing of
    // their own.
    exerciseEverything(
        GuardedTracing.of(
            ThrowingTracing.on(Fault.ENABLED, Fault.START, Fault.PROPAGATING), metrics));
    exerciseEverything(GuardedTracing.of(ThrowingTracing.onEveryScopeMethod(), metrics));

    java.util.Set<String> operations =
        metrics.samples(org.streamrune.core.metrics.MetricNames.TRACING_FAILURES).stream()
            .map(sample -> sample.tags().get(org.streamrune.core.metrics.MetricNames.TAG_OPERATION))
            .collect(java.util.stream.Collectors.toSet());
    assertThat(operations)
        .as("every operation of the seam was driven, and each counted under its own name")
        .containsExactlyInAnyOrder(
            "enabled",
            "start",
            "startChild",
            "propagating",
            "setAttribute",
            "recordSuccess",
            "recordError",
            "link",
            "close")
        .isEqualTo(GuardedTracing.OPERATIONS);
  }

  @Test
  void aHandBuiltBusBindsItsTracingToItsMetrics() {
    org.streamrune.test.RecordingStreamRuneMetrics metrics =
        new org.streamrune.test.RecordingStreamRuneMetrics();
    ThrowingTracing broken = ThrowingTracing.on(Fault.START);
    org.streamrune.core.types.AggregateType lamp =
        org.streamrune.core.types.AggregateType.of("lamp");
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(
                new org.streamrune.test.InMemoryEventStore(TracingFailureIsolationTest.TYPES))
            .register(
                lamp,
                TracingFailureIsolationTest.Switch.class,
                c -> org.streamrune.core.types.AggregateId.of(c.lampId()),
                new TracingFailureIsolationTest.LampDecider())
            .tracing(broken)
            .metrics(metrics)
            .build()) {
      bus.execute(new TracingFailureIsolationTest.Switch("l-1"));
    }

    assertThat(broken.thrown.get()).as("the command span and the lock span").isEqualTo(2);
    assertThat(counted(metrics, "start")).isEqualTo(2);
  }

  @Test
  void aMetricsBackendThatThrowsDoesNotReachTheCaller() {
    org.streamrune.core.StreamRuneMetrics throwing =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordTracingFailure(String operation) {
            throw new IllegalStateException("the metrics backend failed");
          }
        };
    StreamRuneTracing guarded = GuardedTracing.of(ThrowingTracing.on(Fault.START), throwing);

    assertThatCode(() -> start(guarded).close()).doesNotThrowAnyException();
  }

  @Test
  void anInterruptedExceptionOfTheTracingSetsTheInterruptStatusAgain() {
    StreamRuneTracing guarded =
        GuardedTracing.of(
            ThrowingTracing.throwing(() -> new InterruptedException("in onEnd"), Fault.CLOSE));
    try {
      start(guarded).close();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  // ── reporting ──────────────────────────────────────────────────────────────────────────────

  @Test
  void failuresAreCountedAndLoggedOnceWithinTheInterval() {
    Logger logger = (Logger) LoggerFactory.getLogger(GuardedTracing.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      ThrowingTracing broken = ThrowingTracing.on(Fault.START);
      StreamRuneTracing guarded = GuardedTracing.of(broken);
      for (int i = 0; i < 50; i++) {
        start(guarded).close();
      }

      assertThat(((GuardedTracing) guarded).failureCount()).isEqualTo(50);
      assertThat(appender.list)
          .as("one WARN for fifty failures within the interval")
          .hasSize(1)
          .first()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("Tracing failed in start");
                assertThat(event.getThrowableProxy()).isNotNull();
              });
    } finally {
      logger.detachAppender(appender);
    }
  }
}
