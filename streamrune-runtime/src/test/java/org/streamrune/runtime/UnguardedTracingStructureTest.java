package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.ThrowingTracing.Fault;
import org.streamrune.runtime.TracingFailureIsolationTest.CancelOrder;
import org.streamrune.runtime.TracingFailureIsolationTest.OrderPlaced;
import org.streamrune.runtime.TracingFailureIsolationTest.OrderSaga;
import org.streamrune.runtime.TracingFailureIsolationTest.OrderState;
import org.streamrune.runtime.TracingFailureIsolationTest.ReserveStock;
import org.streamrune.runtime.TracingFailureIsolationTest.StockReserved;
import org.streamrune.test.SagaFixture;

/**
 * The code around a span that holds a resource is safe by its own structure, not only because the
 * tracing is guarded: with the guard taken away — the component's tracing replaced, by reflection,
 * with one that throws — a lock that was acquired is released before the failure propagates, and
 * the timeout claim's hold on a saga's step lock is released.
 */
class UnguardedTracingStructureTest {

  private static final StreamId LAMP_1 =
      StreamId.of(AggregateType.of("lamp"), AggregateId.of("l-1"));

  /** Replaces the private {@code tracing} of {@code component} with the unguarded {@code raw}. */
  private static void bypassTheGuard(Object component, StreamRuneTracing raw) throws Exception {
    Field tracing = component.getClass().getDeclaredField("tracing");
    tracing.setAccessible(true);
    assertThat(tracing.get(component)).isInstanceOf(GuardedTracing.class);
    tracing.set(component, raw);
  }

  private static Throwable onItsOwnThread(Runnable work) throws InterruptedException {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread thread =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    work.run();
                  } catch (Throwable t) {
                    failure.set(t);
                  }
                });
    thread.join(30_000);
    return failure.get();
  }

  private static void acquireAndRelease(LocalStripedLocker locker) {
    try (AutoCloseable lock = locker.acquireLock(LAMP_1, Duration.ofMillis(300))) {
      assertThat(lock).isNotNull();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = Fault.class,
      names = {"SET_ATTRIBUTE", "RECORD_SUCCESS", "CLOSE"})
  void aLockAcquiredBeforeTheTracingThrewIsReleasedBeforeTheFailurePropagates(Fault fault)
      throws Exception {
    LocalStripedLocker plain = new LocalStripedLocker(16);
    ThrowingTracing raw = ThrowingTracing.on(fault);
    LocalStripedLocker traced = plain.traced(raw);
    bypassTheGuard(traced, raw);

    Throwable seen = onItsOwnThread(() -> acquireAndRelease(traced));

    assertThat(seen)
        .as("the caller got no handle, and sees why")
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("the tracing failed");
    assertThat(onItsOwnThread(() -> acquireAndRelease(plain)))
        .as("another thread takes the stream at once: the lock was released")
        .isNull();
  }

  @Test
  void aLockNotAcquiredIsReportedByItsOwnFailure_withTheTracingsSuppressed() throws Exception {
    LocalStripedLocker plain = new LocalStripedLocker(16);
    ThrowingTracing raw = ThrowingTracing.on(Fault.SET_ATTRIBUTE, Fault.CLOSE);
    LocalStripedLocker traced = plain.traced(raw);
    bypassTheGuard(traced, raw);

    try (AutoCloseable held = plain.acquireLock(LAMP_1, Duration.ofMillis(300))) {
      Throwable refused = onItsOwnThread(() -> acquireAndRelease(traced));
      assertThat(refused).isInstanceOf(LockAcquisitionException.class);
      assertThat(refused.getSuppressed()).singleElement().isInstanceOf(IllegalStateException.class);
    }
    assertThat(raw.closed.get()).as("the scope was closed all the same").isEqualTo(1);
  }

  @Test
  void aTracingThatRethrowsTheFailureItRecordsIsNotSuppressedOnItself() throws Exception {
    StreamRuneTracing rethrowing =
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
            return new TraceScope() {
              @Override
              public void setAttribute(String key, String value) {}

              @Override
              public void setAttribute(String key, long value) {}

              @Override
              public void setAttribute(String key, boolean value) {}

              @Override
              public void recordSuccess() {}

              @Override
              public void recordError(Throwable error) {
                throw (RuntimeException) error;
              }

              @Override
              public void close() {}
            };
          }
        };
    LocalStripedLocker traced = new LocalStripedLocker(16).traced(rethrowing);
    bypassTheGuard(traced, rethrowing);

    // An invalid timeout: the acquisition's own IllegalArgumentException, which the scope rethrows.
    assertThatThrownBy(() -> traced.acquireLock(LAMP_1, Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(
            thrown -> {
              assertThat(thrown.getMessage()).doesNotContain("Self-suppression");
              assertThat(thrown.getSuppressed()).isEmpty();
            });
  }

  @Test
  void anAcquisitionAllocatesNoHandle_theStripesHandleIsBuiltOnce() throws Exception {
    LocalStripedLocker plain = new LocalStripedLocker(16);
    AutoCloseable first = plain.acquireLock(LAMP_1, Duration.ofMillis(300));
    first.close();
    AutoCloseable second = plain.traced(ThrowingTracing.on()).acquireLock(LAMP_1, Duration.ZERO);
    second.close();

    assertThat(second)
        .as("nothing is allocated between the grant of the lock and the returned handle")
        .isSameAs(first);
  }

  // ── the timeout claim's hold on the saga's step lock ───────────────────────────────────────

  @Test
  void aTimeoutSpanThatThrowsOnStartReleasesTheClaimHold_withoutTheGuard() throws Exception {
    AtomicInteger thrown = new AtomicInteger();
    StreamRuneTracing throwsForTheTimeoutSpan =
        new StreamRuneTracing() {
          @Override
          public boolean enabled(TracingArea area) {
            return area == TracingArea.SAGA;
          }

          @Override
          public TraceScope start(
              TracingArea area,
              String spanName,
              SpanKind kind,
              SpanAttributes attributes,
              SpanLink link,
              SpanBaggage baggage) {
            if (spanName.endsWith("/timeout") && thrown.getAndIncrement() == 0) {
              throw new IllegalStateException("the sampler failed");
            }
            return TraceScope.NOOP;
          }
        };
    SagaStepLock lock = new SagaStepLock(Duration.ofMillis(400));
    SagaFixture<OrderState> fixture =
        SagaFixture.of(
                OrderState.class,
                builder ->
                    builder
                        .decider(new OrderSaga())
                        .startWhen(event -> event.event() instanceof OrderPlaced)
                        .extractSagaId(
                            event -> SagaId.of("order-" + ((OrderPlaced) event.event()).orderId()))
                        .stepLock(lock)
                        .tracing(throwsForTheTimeoutSpan))
            .withTimeouts(
                builder -> builder.decider(new OrderSaga()).tracing(throwsForTheTimeoutSpan));
    Field runnerField = SagaFixture.class.getDeclaredField("timeoutRunner");
    runnerField.setAccessible(true);
    bypassTheGuard(runnerField.get(fixture), throwsForTheTimeoutSpan);

    StreamId order = StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));
    fixture.when(order, new OrderPlaced("o-1"));

    // The poll runs on a thread of its own that lives on, as the runner's poll thread does.
    CountDownLatch polled = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(1);
    Thread poll =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    fixture.advance(Duration.ofMinutes(6)).runTimeouts();
                  } finally {
                    polled.countDown();
                  }
                  try {
                    done.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    try {
      assertThat(polled.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(thrown.get()).as("the timeout span's start threw into the runner").isEqualTo(1);
      assertThat(lock.sagasInFlight()).as("the claim hold is released").isZero();
      assertThat(fixture.dispatched())
          .as("nothing was claimed by the poll whose span failed")
          .containsExactly(new ReserveStock("o-1"));

      // The next poll claims the timeout: exactly one compensation, as untraced.
      fixture.runTimeouts();
      assertThat(fixture.dispatched())
          .containsExactly(new ReserveStock("o-1"), new CancelOrder("o-1"));
      assertThat(lock.sagasInFlight()).isZero();
      // A late reply, delivered on this thread, is not refused for the step lock.
      assertThatCode(() -> fixture.replyingTo(SagaId.of("order-o-1"), new StockReserved("o-1")))
          .doesNotThrowAnyException();
    } finally {
      done.countDown();
      poll.join(10_000);
    }
  }
}
