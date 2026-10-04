package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.LateDataPolicy;
import org.streamrune.core.projection.WindowResult;

/**
 * {@link WindowedProjection} is driven concurrently by framework-sanctioned flows (a {@code
 * ProjectionDeadLetterReplayer.replay(...)} while the owning runner is LIVE; the
 * javadoc-recommended inline+runner dual registration, where {@code InlineProjectionInterceptor}
 * runs on every command thread), yet its {@code TreeMap}s, {@code watermark} and offset fence are
 * plain unsynchronized fields.
 *
 * <p>Both tests pin a DETERMINISTIC interleaving rather than a probabilistic stress race: the
 * accumulator / sink callback parks the first driver at the exact point the second driver's
 * mutation is destructive, so the wrong outcome is produced every run rather than occasionally.
 * Each park uses a bounded wait so that the corrected (mutually exclusive) implementation simply
 * serializes the two drivers instead of deadlocking.
 */
class WindowedProjectionConcurrencyTest {

  private static final Instant T0 = Instant.parse("2026-03-01T00:00:00Z");

  /** Bounded park: the fixed implementation never releases this latch before the timeout. */
  private static void parkBriefly(CountDownLatch latch) {
    try {
      latch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Two drivers accumulating into the SAME window interleave their read-modify-write of {@code
   * openWindows} (get → accumulate → put), so the second driver's put is overwritten and its event
   * vanishes from the aggregate the window then closes with, {@code isFinal = true} — permanently
   * wrong, with nothing to correct it.
   */
  @Test
  void concurrentProcess_doesNotLoseAnEventFromASharedWindow() throws Exception {
    var emissions = Collections.synchronizedList(new ArrayList<WindowResult<Integer>>());
    var insideFirstAccumulate = new CountDownLatch(1);
    var secondDriverDone = new CountDownLatch(1);
    var firstAccumulate = new AtomicBoolean(true);

    var projection =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .lateData(LateDataPolicy.DROP)
            .initialState(() -> 0)
            .accumulator(
                (acc, e) -> {
                  if (firstAccumulate.compareAndSet(true, false)) {
                    // Parked strictly between openWindows.get(...) and openWindows.put(...).
                    insideFirstAccumulate.countDown();
                    parkBriefly(secondDriverDone);
                  }
                  return acc + 1;
                })
            .sink(emissions::add)
            .build();

    // Offsets are assigned in creation order, so the fixed (serialized) run sees them ascending
    // and the redelivery fence never legitimately drops either event.
    EventEnvelope runnerEvent = evt(T0.plusSeconds(60));
    EventEnvelope replayerEvent = evt(T0.plusSeconds(120));
    EventEnvelope closingEvent = evt(T0.plusSeconds(1800));

    var escaped = new AtomicReference<Throwable>();
    var runner = new Thread(() -> projection.process(List.of(runnerEvent)), "proj-runner");
    runner.setUncaughtExceptionHandler((t, e) -> escaped.set(e));
    runner.start();
    assertThat(insideFirstAccumulate.await(10, TimeUnit.SECONDS)).isTrue();

    var replayer =
        new Thread(
            () -> {
              try {
                projection.process(List.of(replayerEvent));
              } finally {
                secondDriverDone.countDown();
              }
            },
            "proj-replayer");
    replayer.setUncaughtExceptionHandler((t, e) -> escaped.set(e));
    replayer.start();

    runner.join(30_000);
    replayer.join(30_000);
    assertThat(escaped.get()).isNull();

    // Close [T0, T0+10m).
    projection.process(List.of(closingEvent));

    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).isFinal()).isTrue();
    assertThat(emissions.get(0).state())
        .as("both concurrently-accumulated events must survive into the final window state")
        .isEqualTo(2);
  }

  /**
   * A second driver structurally modifying {@code openWindows} while the first is inside its
   * close-detection iterator throws {@link java.util.ConcurrentModificationException} out of the
   * first driver's {@code process}, aborting its batch — and, because the first driver never
   * reached {@code it.remove()}, the second driver re-emits the very same windows a second time
   * with {@code isFinal = true}.
   */
  @Test
  void concurrentProcess_doesNotCorruptTheCloseScanOrDoubleEmitAFinalWindow() throws Exception {
    var emissions = Collections.synchronizedList(new ArrayList<WindowResult<Integer>>());
    var insideFirstEmission = new CountDownLatch(1);
    var secondDriverDone = new CountDownLatch(1);
    var firstEmission = new AtomicBoolean(true);

    var projection =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .lateData(LateDataPolicy.DROP)
            .initialState(() -> 0)
            .accumulator((acc, e) -> acc + 1)
            .sink(
                result -> {
                  emissions.add(result);
                  if (firstEmission.compareAndSet(true, false)) {
                    // Parked inside the close-detection iterator, before its it.remove().
                    insideFirstEmission.countDown();
                    parkBriefly(secondDriverDone);
                  }
                })
            .build();

    // Opens [T0,+10m), [T0+10m,+20m) and [T0+30m,+40m); watermark T0+31m closes the first two.
    List<EventEnvelope> runnerBatch =
        List.of(evt(T0.plusSeconds(60)), evt(T0.plusSeconds(660)), evt(T0.plusSeconds(1860)));
    // Opens a brand-new window key [T0+40m,+50m) — a structural modification of the TreeMap the
    // runner is iterating.
    EventEnvelope replayerEvent = evt(T0.plusSeconds(2700));

    var escaped = new AtomicReference<Throwable>();
    var runner = new Thread(() -> projection.process(runnerBatch), "proj-runner");
    runner.setUncaughtExceptionHandler((t, e) -> escaped.set(e));
    runner.start();
    assertThat(insideFirstEmission.await(10, TimeUnit.SECONDS)).isTrue();

    var replayer =
        new Thread(
            () -> {
              try {
                projection.process(List.of(replayerEvent));
              } finally {
                secondDriverDone.countDown();
              }
            },
            "proj-replayer");
    replayer.setUncaughtExceptionHandler((t, e) -> escaped.set(e));
    replayer.start();

    runner.join(30_000);
    replayer.join(30_000);

    assertThat(escaped.get())
        .as("no exception may escape process() because another sanctioned driver ran concurrently")
        .isNull();

    Set<Instant> finalStarts = new HashSet<>();
    for (WindowResult<Integer> r : emissions) {
      if (r.isFinal()) {
        assertThat(finalStarts.add(r.window().start()))
            .as("window %s was emitted final more than once", r.window().start())
            .isTrue();
      }
    }
  }
}
