package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.tracing.TracingArea;

class TracingSettingsTest {

  @Test
  void allEnabled_tracesEveryArea() {
    TracingSettings settings = TracingSettings.allEnabled();

    assertThat(settings.enabled()).isTrue();
    for (TracingArea area : TracingArea.values()) {
      assertThat(settings.enabled(area)).as(area.name()).isTrue();
    }
    assertThat(TracingSettings.builder().build()).isEqualTo(settings);
  }

  @Test
  void disabled_tracesNoArea_whateverTheAreaSwitchesSay() {
    TracingSettings settings = TracingSettings.disabled();

    assertThat(settings.enabled()).isFalse();
    for (TracingArea area : TracingArea.values()) {
      assertThat(settings.enabled(area)).as(area.name()).isFalse();
    }
  }

  @Test
  void anAreaSwitch_turnsOnlyThatAreaOff_andOnAgain() {
    TracingSettings off =
        TracingSettings.builder()
            .area(TracingArea.LOCK, false)
            .area(TracingArea.SAGA, false)
            .build();

    assertThat(off.enabled(TracingArea.LOCK)).isFalse();
    assertThat(off.enabled(TracingArea.SAGA)).isFalse();
    assertThat(off.enabled(TracingArea.COMMAND)).isTrue();
    assertThat(
            TracingSettings.builder()
                .area(TracingArea.LOCK, false)
                .area(TracingArea.LOCK, true)
                .build()
                .enabled(TracingArea.LOCK))
        .isTrue();
  }

  @Test
  void equality_followsTheSwitches() {
    TracingSettings one = TracingSettings.builder().area(TracingArea.OUTBOX, false).build();
    TracingSettings two = TracingSettings.builder().area(TracingArea.OUTBOX, false).build();

    assertThat(one).isEqualTo(two).hasSameHashCodeAs(two);
    assertThat(one).isNotEqualTo(TracingSettings.allEnabled());
    assertThat(TracingSettings.disabled())
        .isNotEqualTo(TracingSettings.allEnabled())
        .isNotEqualTo("disabled");
    assertThat(one.toString()).contains("enabled=true").doesNotContain("OUTBOX");
  }

  @Test
  void aNullArea_isRefused() {
    assertThatThrownBy(() -> TracingSettings.builder().area(null, true))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> TracingSettings.allEnabled().enabled(null))
        .isInstanceOf(NullPointerException.class);
  }
}
