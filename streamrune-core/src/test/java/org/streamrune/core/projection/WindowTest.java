package org.streamrune.core.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class WindowTest {

  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant T1 = Instant.parse("2026-01-01T01:00:00Z");

  @Test
  void contains_returnsTrueForStartAndMiddle_falseForEnd() {
    var w = new Window(T0, T1);
    assertThat(w.contains(T0)).isTrue();
    assertThat(w.contains(T0.plusSeconds(1800))).isTrue();
    assertThat(w.contains(T1)).isFalse(); // half-open [start, end)
  }

  @Test
  void contains_returnsFalseForBeforeStart() {
    var w = new Window(T0, T1);
    assertThat(w.contains(T0.minusSeconds(1))).isFalse();
  }

  @Test
  void contains_nullInstant_throws() {
    var w = new Window(T0, T1);
    assertThatThrownBy(() -> w.contains(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("t is required");
  }

  @Test
  void invalidEndBeforeStart_throws() {
    assertThatThrownBy(() -> new Window(T1, T0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("end must be > start");
  }

  @Test
  void invalidEndEqualsStart_throws() {
    assertThatThrownBy(() -> new Window(T0, T0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("end must be > start");
  }
}
