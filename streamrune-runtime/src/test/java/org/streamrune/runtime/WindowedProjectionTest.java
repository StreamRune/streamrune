package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.WindowResult;
import org.streamrune.core.projection.WindowSink;

class WindowedProjectionTest {

  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void tumbling_emitsOnceOnClose() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    WindowSink<Integer> sink = emissions::add;

    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((acc, e) -> acc + 1)
            .sink(sink)
            .build();

    // Window [T0, T0+10m). Three events inside, one after end advances watermark past end+grace.
    w.process(
        List.of(
            evt(T0.plusSeconds(60)),
            evt(T0.plusSeconds(120)),
            evt(T0.plusSeconds(180)),
            evt(T0.plusSeconds(700)))); // 11m40s — past 10m boundary

    assertThat(emissions).hasSize(1);
    var r = emissions.get(0);
    assertThat(r.window().start()).isEqualTo(T0);
    assertThat(r.window().end()).isEqualTo(T0.plus(Duration.ofMinutes(10)));
    assertThat(r.state()).isEqualTo(3);
    assertThat(r.isFinal()).isTrue();
  }

  @Test
  void emptyBatch_noEmission() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    w.process(List.of());

    assertThat(emissions).isEmpty();
  }

  @Test
  void watermark_advancesOnHighestEventTime() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    // One event at 30s in window [T0, T0+1m); then a much later event closes the window.
    w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(120))));

    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).state()).isEqualTo(1);
  }

  @Test
  void outOfOrderEvents_correctBucketing() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    // Events at 30s, 90s (window2), 45s — out of order, but still inside their windows.
    // Then event at 130s closes window1 (start T0..+1m) AND window2 (start +1m..+2m).
    w.process(
        List.of(
            evt(T0.plusSeconds(30)),
            evt(T0.plusSeconds(90)),
            evt(T0.plusSeconds(45)),
            evt(T0.plusSeconds(130))));

    assertThat(emissions).hasSize(2);
    assertThat(emissions.get(0).window().start()).isEqualTo(T0);
    assertThat(emissions.get(0).state()).isEqualTo(2);
    assertThat(emissions.get(1).window().start()).isEqualTo(T0.plusSeconds(60));
    // window2 contains only the 90s event — the 130s event is in window3 which doesn't close yet
    assertThat(emissions.get(1).state()).isEqualTo(1);
  }

  @Test
  void accumulatorThrows_propagates() {
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .initialState(() -> 0)
            .accumulator(
                (a, e) -> {
                  throw new RuntimeException("boom");
                })
            .sink(r -> {})
            .build();

    assertThatThrownBy(() -> w.process(List.of(evt(T0))))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("boom");
  }

  @Test
  void sinkThrows_propagates() {
    WindowSink<Integer> sink =
        r -> {
          throw new RuntimeException("sink-boom");
        };
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(sink)
            .build();

    assertThatThrownBy(() -> w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(120)))))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("sink-boom");
  }

  @Test
  void builder_missingSize_throws() {
    assertThatThrownBy(
            () ->
                WindowedProjection.<Integer>builder()
                    .initialState(() -> 0)
                    .accumulator((a, e) -> a)
                    .sink(r -> {})
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("size");
  }

  @Test
  void builder_missingSink_throws() {
    assertThatThrownBy(
            () ->
                WindowedProjection.<Integer>builder()
                    .size(Duration.ofMinutes(1))
                    .initialState(() -> 0)
                    .accumulator((a, e) -> a)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sink");
  }

  @Test
  void builder_missingAccumulator_throws() {
    assertThatThrownBy(
            () ->
                WindowedProjection.<Integer>builder()
                    .size(Duration.ofMinutes(1))
                    .initialState(() -> 0)
                    .sink(r -> {})
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("accumulator");
  }

  @Test
  void builder_missingInitialState_throws() {
    assertThatThrownBy(
            () ->
                WindowedProjection.<Integer>builder()
                    .size(Duration.ofMinutes(1))
                    .accumulator((a, e) -> a)
                    .sink(r -> {})
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("initialState");
  }

  @Test
  void builder_zeroSize_throws() {
    assertThatThrownBy(
            () ->
                WindowedProjection.<Integer>builder()
                    .size(Duration.ZERO)
                    .initialState(() -> 0)
                    .accumulator((a, e) -> a)
                    .sink(r -> {})
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("size");
  }

  @Test
  void singleRun_replayedBatchesAccumulateAcrossInvocations() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    // Batch 1: two events in same window
    w.process(List.of(evt(T0.plusSeconds(10)), evt(T0.plusSeconds(20))));
    assertThat(emissions).isEmpty();

    // Batch 2: one more event in same window, then close
    w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(120))));

    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).state()).isEqualTo(3);
  }

  @Test
  void replayedIdenticalBatch_doesNotDoubleCount() {
    // Accumulation happens on the JVM heap inside the AtomicBatchProcessor's updater
    // lambda, so a rolled-back commit — or any of the runners' abandon-and-re-read paths — leaves
    // the accumulation in place and re-delivers the very same batch. The projection must recognise
    // the re-delivered offsets and treat the second application as a no-op; the offset axis is the
    // only state it can trust, since the checkpoint only guarantees forward progress ACROSS
    // COMMITS, never across the retry of an uncommitted batch.
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ofMinutes(5)) // keeps the closed state alive for the REOPEN path
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    // Two events in window [T0, T0+1m); the third pushes the watermark past end+grace and closes
    // it. Re-processing the SAME list must change nothing.
    var batch = List.of(evt(T0.plusSeconds(10)), evt(T0.plusSeconds(20)), evt(T0.plusSeconds(400)));

    w.process(batch);
    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).state()).isEqualTo(2);
    assertThat(emissions.get(0).isFinal()).isTrue();

    w.process(batch);

    assertThat(emissions)
        .as("a re-delivered batch must neither re-accumulate nor re-emit the closed window")
        .hasSize(1);
    assertThat(emissions.get(0).state()).isEqualTo(2);
  }

  @Test
  void accumulatorFailureMidBatch_retryResumesAtTheLastAppliedEvent() {
    // Crash point: the accumulator throws part-way through a batch. The prefix is
    // already on the heap, so the fence must sit at the last FULLY applied event — not at the
    // batch's last offset (which would swallow the rest of the batch on the retry) and not
    // un-advanced (which would re-count the prefix).
    var emissions = new ArrayList<WindowResult<Integer>>();
    var poison = new AtomicBoolean(true);
    var e1 = evt(T0.plusSeconds(10));
    var e2 = evt(T0.plusSeconds(20));
    var e3 = evt(T0.plusSeconds(30));
    var e4 = evt(T0.plusSeconds(40));
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator(
                (a, e) -> {
                  if (poison.get() && e == e3) {
                    throw new IllegalStateException("transient accumulator blip");
                  }
                  return a + 1;
                })
            .sink(emissions::add)
            .build();

    var batch = List.of(e1, e2, e3, e4);
    assertThatThrownBy(() -> w.process(batch)).isInstanceOf(IllegalStateException.class);

    poison.set(false);
    w.process(batch); // the runner re-reads the identical range from the un-advanced checkpoint
    w.process(List.of(evt(T0.plusSeconds(700)))); // closes the window

    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).state())
        .as("each of the four events counts exactly once across the failed attempt and its retry")
        .isEqualTo(4);
  }

  @Test
  void overlappingReRead_countsEachOffsetOnce() {
    // Crash point: a partially-overlapping re-read (an abandoned commit followed by a
    // re-read that fetches a range extending past the previous attempt).
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    var e1 = evt(T0.plusSeconds(10));
    var e2 = evt(T0.plusSeconds(20));
    var e3 = evt(T0.plusSeconds(30));
    var e4 = evt(T0.plusSeconds(40));

    w.process(List.of(e1, e2, e3));
    w.process(List.of(e2, e3, e4)); // overlaps the previous attempt by two offsets
    w.process(List.of(evt(T0.plusSeconds(700)))); // closes the window

    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).state())
        .as("four distinct offsets were delivered, so the window holds four")
        .isEqualTo(4);
  }
}
