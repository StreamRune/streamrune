package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.LateDataPolicy;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.WindowResult;
import org.streamrune.core.projection.WindowSink;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * {@link WindowedProjection} accumulates into the JVM heap from inside the {@link
 * org.streamrune.core.projection.AtomicBatchProcessor}'s updater lambda. A rollback (or any of the
 * runners' ordinary abandon-and-re-read paths) undoes the DATABASE work only — the heap keeps the
 * accumulation, and the re-delivered batch is accumulated a second time. The window then emits a
 * final aggregate that is a multiple of the truth, and nothing ever corrects it.
 */
class WindowedProjectionRetryTest {

  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void transientOffsetSaveFailure_doesNotDoubleCountTheReDeliveredBatch() throws Exception {
    // Window [T0, T0+10m), grace 0. Batch 1 = three events inside the window (does NOT close it);
    // batch 2 = one event past end+grace, which closes the window and emits the final aggregate.
    var store =
        SimpleTestEventStore.of(
            List.of(
                evt(T0.plusSeconds(10)),
                evt(T0.plusSeconds(20)),
                evt(T0.plusSeconds(30)),
                evt(T0.plusSeconds(700)))); // 11m40s — past the 10m boundary, closes the window

    var backing = new InMemoryOffsetStore();
    var failNextSave = new AtomicInteger(1); // the FIRST checkpoint advance blips (DB failover)
    OffsetStore flaky =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            return backing.getLastOffset(n);
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            if (failNextSave.getAndDecrement() > 0) {
              throw new RuntimeException("offset store blip");
            }
            backing.saveOffset(n, o);
          }

          @Override
          public void reset(ProjectionName n) {
            backing.reset(n);
          }
        };

    var emissions = new CopyOnWriteArrayList<WindowResult<Integer>>();
    var windowed =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((acc, e) -> acc + 1)
            .sink(emissions::add)
            .build();

    ProjectionName name = ProjectionName.of("windowed-retry");
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(flaky)
            .fetchSize(10)
            .batchSize(3) // batch 1 = offsets 1..3, batch 2 = offset 4
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .build();

    Thread t = Thread.ofVirtual().start(() -> runner.run(name, windowed, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      // The checkpoint-save failure is backed off (about a second) and the runner re-reads offsets
      // 1..3 from the un-advanced checkpoint before batch 2 closes the window.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (emissions.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }

      assertThat(emissions).as("the window must close exactly once, with isFinal=true").hasSize(1);
      assertThat(emissions.getFirst().isFinal()).isTrue();
      assertThat(emissions.getFirst().state())
          .as(
              "the window contains three events; the re-delivered batch must not be accumulated a"
                  + " second time")
          .isEqualTo(3);
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // An event's application must be atomic with its fence advance. A throw from the
  // accumulator or the sink mid-event must leave the event fully un-applied (maps unchanged,
  // fence unmoved) so the re-delivered batch re-applies it cleanly — no double-count from
  // partially-committed windows, no drop from an early watermark advance. Redelivery is modeled
  // exactly as the runners produce it: process() called again with the SAME envelope list.
  // ---------------------------------------------------------------------------------------------

  @Test
  void reopenLateReEmission_sinkThrows_retryCountsTheEventExactlyOnce() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var sinkFailures = new AtomicInteger(0);
    WindowSink<Integer> sink =
        r -> {
          if (sinkFailures.getAndDecrement() > 0) {
            throw new RuntimeException("sink down");
          }
          emissions.add(r);
        };
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ofSeconds(10))
            .lateData(LateDataPolicy.REOPEN)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(sink)
            .build();

    // Close window [T0, T0+1m): one event at 30s, watermark jumps to 80s (>= 60+10).
    w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(80))));
    assertThat(emissions).hasSize(1);
    assertThat(emissions.getFirst().state()).isEqualTo(1);

    // Late event at 40s — REOPEN re-emission, but the sink throws on the first delivery. The
    // batch fails; the runner re-delivers the SAME batch.
    var late = List.of(evt(T0.plusSeconds(40)));
    sinkFailures.set(1);
    assertThatThrownBy(() -> w.process(late)).hasMessage("sink down");
    w.process(late); // retry succeeds

    assertThat(emissions).hasSize(2);
    assertThat(emissions.get(1).isFinal()).isFalse();
    assertThat(emissions.get(1).window().start()).isEqualTo(T0);
    assertThat(emissions.get(1).state())
        .as(
            "the late event must be counted exactly once across the sink-throw retry — a"
                + " pre-emission map commit would make the retry double-count it")
        .isEqualTo(2);
  }

  @Test
  void sliding_accumulatorThrowsOnSecondOwningWindow_retryCountsEachWindowOnce() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var accCalls = new AtomicInteger();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(2))
            .slide(Duration.ofMinutes(1))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator(
                (a, e) -> {
                  if (accCalls.incrementAndGet() == 2) {
                    throw new RuntimeException("accumulator down");
                  }
                  return a + 1;
                })
            .sink(emissions::add)
            .build();

    // Event at 90s owns [T0, T0+2m) and [T0+1m, T0+3m). The accumulator throws on the SECOND
    // owning window; the first window's state must not stay committed across the retry.
    var batch = List.of(evt(T0.plusSeconds(90)));
    assertThatThrownBy(() -> w.process(batch)).hasMessage("accumulator down");
    w.process(batch); // retry succeeds (calls 3 and 4)

    // Close both windows: event at 240s (watermark 240 >= 120 and >= 180).
    w.process(List.of(evt(T0.plusSeconds(240))));

    assertThat(emissions).hasSize(2);
    assertThat(emissions.get(0).window().start()).isEqualTo(T0);
    assertThat(emissions.get(0).isFinal()).isTrue();
    assertThat(emissions.get(0).state())
        .as(
            "the first owning window must count the event exactly once — a partial commit before"
                + " the second window's throw would make the retry double-count it")
        .isEqualTo(1);
    assertThat(emissions.get(1).window().start()).isEqualTo(T0.plusSeconds(60));
    assertThat(emissions.get(1).isFinal()).isTrue();
    assertThat(emissions.get(1).state()).isEqualTo(1);
  }

  @Test
  void reopenLateReEmission_consecutiveSinkFailures_doNotCompound() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var sinkFailures = new AtomicInteger(0);
    WindowSink<Integer> sink =
        r -> {
          if (sinkFailures.getAndDecrement() > 0) {
            throw new RuntimeException("sink down");
          }
          emissions.add(r);
        };
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ofSeconds(10))
            .lateData(LateDataPolicy.REOPEN)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(sink)
            .build();

    w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(80))));
    assertThat(emissions).hasSize(1);

    // Three consecutive sink failures (MAX_CONSECUTIVE_ERRORS-style retry storm), then success.
    var late = List.of(evt(T0.plusSeconds(40)));
    sinkFailures.set(3);
    for (int attempt = 0; attempt < 3; attempt++) {
      assertThatThrownBy(() -> w.process(late)).hasMessage("sink down");
    }
    w.process(late); // fourth delivery succeeds

    assertThat(emissions).hasSize(2);
    assertThat(emissions.get(1).state())
        .as("N failed deliveries + 1 success must count the event once, not N+1 times")
        .isEqualTo(2);
  }

  @Test
  void redeliveryAfterFullApply_stillFenceSkips() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ofSeconds(10))
            .lateData(LateDataPolicy.REOPEN)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    var batch = List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(80)));
    w.process(batch); // closes [T0, T0+1m) with state 1
    w.process(batch); // genuine redelivery of a fully-applied batch: complete no-op
    assertThat(emissions).as("a wholly re-delivered batch must not re-emit").hasSize(1);

    // The skipped redelivery must not have re-counted either: the late re-emission sees 2, not 3.
    w.process(List.of(evt(T0.plusSeconds(40))));
    assertThat(emissions).hasSize(2);
    assertThat(emissions.get(1).state()).isEqualTo(2);
  }

  @Test
  void accumulatorThrows_retryDoesNotReclassifyOnTimeEventAsLate() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var accCalls = new AtomicInteger();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ofSeconds(10))
            .lateData(LateDataPolicy.REOPEN)
            .initialState(() -> 0)
            .accumulator(
                (a, e) -> {
                  if (accCalls.incrementAndGet() == 2) {
                    throw new RuntimeException("accumulator down");
                  }
                  return a + 1;
                })
            .sink(emissions::add)
            .build();

    // Window [T0, T0+1m) open with one event; watermark 30s.
    w.process(List.of(evt(T0.plusSeconds(30))));

    // Batch: a high-timestamp event (throws, call 2), then an on-time event for the open window.
    // If the watermark advances before the batch is applied, the retry re-classifies the 40s
    // event as late-past-grace (60+10 <= 200), finds no closed state, and silently DROPS it.
    var batch = List.of(evt(T0.plusSeconds(200)), evt(T0.plusSeconds(40)));
    assertThatThrownBy(() -> w.process(batch)).hasMessage("accumulator down");
    w.process(batch); // retry succeeds (calls 3 and 4)

    // The retry's own watermark advance (200 >= 60+10) closes [T0, T0+1m).
    assertThat(emissions).hasSize(1);
    assertThat(emissions.getFirst().isFinal()).isTrue();
    assertThat(emissions.getFirst().window().start()).isEqualTo(T0);
    assertThat(emissions.getFirst().state())
        .as(
            "the on-time 40s event must be counted: a watermark advanced by the FAILED attempt"
                + " would re-classify it as late and drop it on the retry")
        .isEqualTo(2);
  }
}
