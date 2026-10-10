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

/** The two recovery paths of {@link WindowedProjection} that its offset fence used to defeat. */
class WindowedProjectionRecoveryTest {

  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  /**
   * The closing batch commits its event, advances the fence and the watermark, and then the sink
   * throws on the window's {@code isFinal} emission. The runner re-delivers the SAME batch
   * (un-advanced checkpoint); it is wholly fenced, and pre-fix the early return declared it "a
   * complete no-op, including the sink" — on a quiescent stream the window then stayed open forever
   * and its final emission never happened. The wholly-fenced path must re-run the close scan
   * against the current watermark.
   */
  @Test
  void sinkThrowsInsideTheCloseScan_theRedeliveredBatchClosesTheWindow() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var throwOnce = new AtomicBoolean(true);
    WindowSink<Integer> sink =
        result -> {
          if (result.isFinal() && throwOnce.getAndSet(false)) {
            throw new RuntimeException("sink blip");
          }
          emissions.add(result);
        };
    var windowed =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((acc, e) -> acc + 1)
            .sink(sink)
            .build();
    windowed.process(List.of(evt(T0.plusSeconds(10)), evt(T0.plusSeconds(20))));
    var closer = evt(T0.plusSeconds(700)); // 11m40s — past end+grace, closes [T0, T0+10m)

    assertThatThrownBy(() -> windowed.process(List.of(closer)))
        .as("the crashed run: the sink threw on the isFinal emission")
        .hasMessage("sink blip");
    assertThat(emissions).as("nothing was delivered by the crashed run").isEmpty();

    // The runner re-delivers the same batch; it is wholly fenced (the closer already committed).
    windowed.process(List.of(closer));

    assertThat(emissions)
        .as("the redelivered (wholly fenced) batch must still run the close scan")
        .hasSize(1);
    assertThat(emissions.get(0).isFinal()).isTrue();
    assertThat(emissions.get(0).state()).isEqualTo(2);
    assertThat(emissions.get(0).window().start()).isEqualTo(T0);

    // Idempotent: a third delivery of the same batch emits nothing more.
    windowed.process(List.of(closer));
    assertThat(emissions).hasSize(1);
  }

  /**
   * The replay probe reports whether anything passed the fence: a range wholly at or below the
   * fence applied nothing (the replayer keeps its entry), while a range the fence sits inside
   * applied its suffix — and a fence inside a range proves its prefix was genuinely accumulated
   * (the fence advances per applied event; only later batches can move it past a hole, and then the
   * whole range is below it).
   */
  @Test
  void processDeadLetterReplay_reportsFalseWhenWhollyFenced_trueWhenAnythingApplied() {
    var emissions = new ArrayList<WindowResult<Integer>>();
    var windowed =
        WindowedProjection.<Integer>builder()
            .size(Duration.ofMinutes(10))
            .grace(Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((acc, e) -> acc + 1)
            .sink(emissions::add)
            .build();
    var e1 = evt(T0.plusSeconds(10));
    var e2 = evt(T0.plusSeconds(20));
    var e3 = evt(T0.plusSeconds(30));
    windowed.process(List.of(e1, e2, e3)); // fence at e3

    assertThat(windowed.processDeadLetterReplay(List.of(e1, e2), null))
        .as("wholly fenced: nothing applied — the replayer must keep the entry")
        .isFalse();

    var e4 = evt(T0.plusSeconds(40));
    assertThat(windowed.processDeadLetterReplay(List.of(e3, e4), null))
        .as("partially fenced: e4 applied — the entry may be discarded")
        .isTrue();
    assertThat(windowed.processDeadLetterReplay(List.of(e3, e4), null))
        .as("and the fence still dedups the re-delivery of the same range")
        .isFalse();
  }
}
