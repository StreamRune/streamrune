package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.context.SmartLifecycle;

class RunnerLifecycleTest {

  @Test
  void startInvokesStartActionOnce() {
    var starts = new AtomicInteger();
    var lifecycle = new RunnerLifecycle("test", starts::incrementAndGet, () -> {});

    assertThat(lifecycle.isRunning()).isFalse();
    lifecycle.start();
    lifecycle.start(); // second call is a no-op

    assertThat(starts.get()).isEqualTo(1);
    assertThat(lifecycle.isRunning()).isTrue();
  }

  @Test
  void stopInvokesStopActionOnlyWhenRunning() {
    var stops = new AtomicInteger();
    var lifecycle = new RunnerLifecycle("test", () -> {}, stops::incrementAndGet);

    lifecycle.stop(); // not running — no-op
    assertThat(stops.get()).isZero();

    lifecycle.start();
    lifecycle.stop();
    lifecycle.stop(); // second call is a no-op

    assertThat(stops.get()).isEqualTo(1);
    assertThat(lifecycle.isRunning()).isFalse();
  }

  @Test
  void phaseRunsBeforeWebServerLifecycles() {
    var lifecycle = new RunnerLifecycle("test", () -> {}, () -> {});
    assertThat(lifecycle.getPhase()).isEqualTo(RunnerLifecycle.PHASE);
    assertThat(lifecycle.getPhase()).isLessThan(SmartLifecycle.DEFAULT_PHASE - 1024);
    assertThat(lifecycle.isAutoStartup()).isTrue();
  }

  @Test
  void rejectsMissingArguments() {
    assertThatThrownBy(() -> new RunnerLifecycle(null, () -> {}, () -> {}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RunnerLifecycle(" ", () -> {}, () -> {}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RunnerLifecycle("test", null, () -> {}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RunnerLifecycle("test", () -> {}, null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
