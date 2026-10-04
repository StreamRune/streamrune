package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.LateDataPolicy;
import org.streamrune.core.projection.WindowResult;

class WindowedProjectionLateDataTest {

  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void sliding_eventInOverlappingWindows() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(2))
            .slide(Duration.ofMinutes(1))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    // Event at 90s lands in windows [T0, T0+2m) and [T0+1m, T0+3m).
    // Watermark advance to 4m closes [T0,T0+2m) and [T0+1m,T0+3m) and [T0+2m,T0+4m).
    w.process(List.of(evt(T0.plusSeconds(90)), evt(T0.plusSeconds(240))));

    // [T0,T0+2m): 1 event (90s)   — only this overlaps the 90s event
    // [T0+1m,T0+3m): 2 events (90s, 240s? — 240s NOT in [60s,180s)) — wait, 240s >= 180s, so NO
    // Actually 240s is in [120s,240s)? 240 not < 240, so NO. In [180s,300s)? 180<=240<300 YES.
    // Re-examine: event at 90s, event at 240s.
    // 90s windows: floor((90-120+60)/60)=0 → starts 0; check 0+120>90 yes (window [0,120) ); next
    // start 60, 60+120=180 > 90 yes (window [60,180)); next start 120, 120+120=240 > 90 yes BUT
    // 120>90 so stop. Owned by [0,120) and [60,180).
    // 240s windows: floor((240-120+60)/60)=3 → starts 180; 180+120=300 > 240 yes (window
    // [180,300)); next 240, 240<=240 so 240+120=360>240 yes (window [240,360)); next 300, 300>240
    // stop. Owned by [180,300) and [240,360).
    // After 240s event, watermark=240s. Closed iff watermark >= end+grace=end. So [0,120) closed
    // (120<=240); [60,180) closed; [120,240) — never created (no events in it); [180,300) NOT
    // closed (300<=240 false); [240,360) NOT closed.
    // Emissions: [0,120) state=1 (only 90s); [60,180) state=1 (only 90s).
    assertThat(emissions).hasSize(2);
    assertThat(emissions.get(0).window().start()).isEqualTo(T0);
    assertThat(emissions.get(0).state()).isEqualTo(1);
    assertThat(emissions.get(1).window().start()).isEqualTo(T0.plusSeconds(60));
    assertThat(emissions.get(1).state()).isEqualTo(1);
  }

  @Test
  void lateEventWithinGrace_REOPEN_includedInCloseEmission() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ofMinutes(2))
            .lateData(LateDataPolicy.REOPEN)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    // Window [T0, T0+1m). Event at 30s. Watermark advances to 90s — NOT yet closed (need 1m+2m=3m).
    // Late event at 45s arrives — within grace, window still open — accumulated.
    // Then event at 240s closes BOTH windows: [T0,T0+1m) (240 >= 60+120) AND
    // [T0+1m,T0+2m) (240 >= 120+120).
    w.process(
        List.of(
            evt(T0.plusSeconds(30)),
            evt(T0.plusSeconds(90)), // window [T0+1m, T0+2m), advances watermark to 90s
            evt(T0.plusSeconds(45)), // late but within open window [T0,T0+1m)
            evt(
                T0.plusSeconds(
                    240)) // closes [T0,T0+1m) (240 >= 60+120) AND [T0+1m,T0+2m) (240 >= 120+120)
            ));

    // [T0,T0+1m): 2 events (30s, 45s). [T0+1m,T0+2m): 1 event (90s).
    assertThat(emissions).hasSize(2);
    assertThat(emissions)
        .anySatisfy(
            r -> {
              assertThat(r.window().start()).isEqualTo(T0);
              assertThat(r.state()).isEqualTo(2);
              assertThat(r.isFinal()).isTrue();
            });
    assertThat(emissions)
        .anySatisfy(
            r -> {
              assertThat(r.window().start()).isEqualTo(T0.plusSeconds(60));
              assertThat(r.state()).isEqualTo(1);
            });
  }

  @Test
  void lateEventPastGrace_DROP_dropsAndDoesNotEmitAgain() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var w =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(1))
            .grace(Duration.ofSeconds(10))
            .lateData(LateDataPolicy.DROP)
            .initialState(() -> 0)
            .accumulator((a, e) -> a + 1)
            .sink(emissions::add)
            .build();

    // First batch closes window [T0,T0+1m): event at 30s, then event at 80s closes it.
    w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(80))));
    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).state()).isEqualTo(1);

    // Second batch: late event at 40s, should be dropped (DROP policy). No new emission.
    w.process(List.of(evt(T0.plusSeconds(40))));

    assertThat(emissions).hasSize(1);
  }

  @Test
  void lateEventPastGrace_REOPEN_reEmitsWithIsFinalFalse() {
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

    // Close window [T0,T0+1m): event at 30s, watermark jumps to 80s (>= 60+10=70).
    w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(80))));
    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).isFinal()).isTrue();
    assertThat(emissions.get(0).state()).isEqualTo(1);

    // Late event at 40s — past grace, REOPEN re-emits with isFinal=false.
    w.process(List.of(evt(T0.plusSeconds(40))));

    assertThat(emissions).hasSize(2);
    assertThat(emissions.get(1).isFinal()).isFalse();
    assertThat(emissions.get(1).state()).isEqualTo(2);
    assertThat(emissions.get(1).window().start()).isEqualTo(T0);
  }

  @Test
  void lateEventPastGrace_REOPEN_dropsWhenStateEvicted() {
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

    // Close window [T0,T0+1m) and advance watermark far enough to evict closed state.
    // Eviction: watermark > windowEnd + 2×grace = 60 + 20 = 80s.
    // Watermark 81s > 80s → evicted.
    w.process(List.of(evt(T0.plusSeconds(30)), evt(T0.plusSeconds(81))));
    assertThat(emissions).hasSize(1);

    // Late event at 40s — REOPEN but state evicted → dropped.
    w.process(List.of(evt(T0.plusSeconds(40))));

    assertThat(emissions).hasSize(1);
  }
}
