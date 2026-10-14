package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The three-valued {@code streamrune.saga.forward-fence} setting shared by the integrations. */
class SagaForwardFenceSettingTest {

  @Test
  void parsesAutoOnOffInAnyCase() {
    assertThat(SagaForwardFenceSetting.parse("auto")).isEqualTo(SagaForwardFenceSetting.AUTO);
    assertThat(SagaForwardFenceSetting.parse("AUTO")).isEqualTo(SagaForwardFenceSetting.AUTO);
    assertThat(SagaForwardFenceSetting.parse("on")).isEqualTo(SagaForwardFenceSetting.ON);
    assertThat(SagaForwardFenceSetting.parse("On")).isEqualTo(SagaForwardFenceSetting.ON);
    assertThat(SagaForwardFenceSetting.parse("off")).isEqualTo(SagaForwardFenceSetting.OFF);
    assertThat(SagaForwardFenceSetting.parse(" OFF ")).isEqualTo(SagaForwardFenceSetting.OFF);
  }

  @ParameterizedTest
  @ValueSource(strings = {"true", "TRUE", "yes", "y", "t", "1"})
  void readsTheBooleanSpellings_true(String value) {
    assertThat(SagaForwardFenceSetting.parse(value)).isEqualTo(SagaForwardFenceSetting.ON);
  }

  @ParameterizedTest
  @ValueSource(strings = {"false", "FALSE", "no", "n", "f", "0"})
  void readsTheBooleanSpellings_false(String value) {
    assertThat(SagaForwardFenceSetting.parse(value)).isEqualTo(SagaForwardFenceSetting.OFF);
  }

  @Test
  void nullOrBlankIsAuto() {
    assertThat(SagaForwardFenceSetting.parse(null)).isEqualTo(SagaForwardFenceSetting.AUTO);
    assertThat(SagaForwardFenceSetting.parse("")).isEqualTo(SagaForwardFenceSetting.AUTO);
    assertThat(SagaForwardFenceSetting.parse("  ")).isEqualTo(SagaForwardFenceSetting.AUTO);
  }

  @Test
  void refusesAnythingElseNamingThePropertyAndTheValue() {
    assertThatThrownBy(() -> SagaForwardFenceSetting.parse("maybe"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "streamrune.saga.forward-fence must be auto, on or off (case-insensitive;"
                + " true/false and the other boolean spellings of on/off are accepted); got:"
                + " 'maybe'");
  }

  @Test
  void autoFollowsSingleActiveConsumer() {
    assertThat(SagaForwardFenceSetting.AUTO.enabled(true)).isTrue();
    assertThat(SagaForwardFenceSetting.AUTO.enabled(false)).isFalse();
  }

  @Test
  void onAndOffIgnoreIt() {
    assertThat(SagaForwardFenceSetting.ON.enabled(true)).isTrue();
    assertThat(SagaForwardFenceSetting.ON.enabled(false)).isTrue();
    assertThat(SagaForwardFenceSetting.OFF.enabled(true)).isFalse();
    assertThat(SagaForwardFenceSetting.OFF.enabled(false)).isFalse();
  }
}
