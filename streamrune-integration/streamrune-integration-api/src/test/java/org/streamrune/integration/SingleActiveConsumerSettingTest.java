package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The one parser of {@code streamrune.subscription.single-active-consumer.enabled}: the leadership
 * bean of each integration and the saga forward fence's {@code auto} read the value through it, so
 * a spelling cannot switch one on and the other off.
 */
class SingleActiveConsumerSettingTest {

  @ParameterizedTest
  @CsvSource({
    "true,true",
    "TRUE,true",
    "on,true",
    "On,true",
    "yes,true",
    "y,true",
    "t,true",
    "1,true",
    "false,false",
    "off,false",
    "no,false",
    "n,false",
    "f,false",
    "0,false",
    "' no ',false"
  })
  void everyBooleanSpellingMeansOneThing(String value, boolean expected) {
    assertThat(SingleActiveConsumerSetting.enabled(value)).isEqualTo(expected);
  }

  @Test
  void absentOrBlankMeansTheDefault_on() {
    assertThat(SingleActiveConsumerSetting.enabled(null)).isTrue();
    assertThat(SingleActiveConsumerSetting.enabled("")).isTrue();
    assertThat(SingleActiveConsumerSetting.enabled("  ")).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"maybe", "enabled", "2", "on off"})
  void anUnknownSpellingIsRefusedWithAMessageNamingTheProperty(String value) {
    assertThatThrownBy(() -> SingleActiveConsumerSetting.enabled(value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "streamrune.subscription.single-active-consumer.enabled must be true or false"
                + " (case-insensitive; on/off, yes/no, y/n, t/f and 1/0 are accepted too); got: '"
                + value
                + "'");
  }

  @Test
  void thePropertyNameIsTheDocumentedKey() {
    assertThat(SingleActiveConsumerSetting.PROPERTY)
        .isEqualTo("streamrune.subscription.single-active-consumer.enabled");
  }
}
