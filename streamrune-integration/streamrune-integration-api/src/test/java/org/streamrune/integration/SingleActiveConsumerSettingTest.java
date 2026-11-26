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
    assertThat(SingleActiveConsumerSetting.enabled(value, "postgres")).isEqualTo(expected);
  }

  @Test
  void absentOrBlankMeansTheDefault_onForPostgres() {
    assertThat(SingleActiveConsumerSetting.enabled(null, "postgres")).isTrue();
    assertThat(SingleActiveConsumerSetting.enabled("", "postgres")).isTrue();
    assertThat(SingleActiveConsumerSetting.enabled("  ", "postgres")).isTrue();
  }

  @Test
  void absentOrBlankMeansOffForSqlite_whichIsSingleInstance() {
    assertThat(SingleActiveConsumerSetting.enabled(null, "sqlite")).isFalse();
    assertThat(SingleActiveConsumerSetting.enabled("", "sqlite")).isFalse();
    assertThat(SingleActiveConsumerSetting.enabled("  ", "sqlite")).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"false", "off", "no", "0"})
  void anExplicitOffIsAcceptedForSqlite(String value) {
    assertThat(SingleActiveConsumerSetting.enabled(value, "sqlite")).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"true", "on", "YES", "1"})
  void anExplicitOnIsRefusedForSqlite(String value) {
    assertThatThrownBy(() -> SingleActiveConsumerSetting.enabled(value, "sqlite"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.backend=sqlite: SQLite is single-instance; set"
                + " streamrune.subscription.single-active-consumer.enabled=false (or leave it"
                + " unset, which means false on SQLite)");
  }

  @Test
  void anUnknownSpellingIsRefusedOnSqliteToo() {
    assertThatThrownBy(() -> SingleActiveConsumerSetting.enabled("maybe", "sqlite"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("got: 'maybe'");
  }

  @Test
  void anUnparsedBackendIsRefused() {
    assertThatThrownBy(() -> SingleActiveConsumerSetting.enabled("true", "oracle"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "backend must be a parsed backend name (postgres, sqlite), got 'oracle': parse"
                + " streamrune.backend with BackendSelection.parse first");
    assertThatThrownBy(() -> SingleActiveConsumerSetting.enabled("true", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("backend is required");
  }

  @ParameterizedTest
  @ValueSource(strings = {"true", "off", "1"})
  void requireBooleanAcceptsEverySpellingAndNull(String value) {
    SingleActiveConsumerSetting.requireBoolean(value);
    SingleActiveConsumerSetting.requireBoolean(null);
  }

  @ParameterizedTest
  @ValueSource(strings = {"maybe", "enabled", "2", "on off"})
  void anUnknownSpellingIsRefusedWithAMessageNamingTheProperty(String value) {
    assertThatThrownBy(() -> SingleActiveConsumerSetting.enabled(value, "postgres"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "streamrune.subscription.single-active-consumer.enabled must be true or false"
                + " (case-insensitive; on/off, yes/no, y/n, t/f and 1/0 are accepted too); got: '"
                + value
                + "'");
    assertThatThrownBy(() -> SingleActiveConsumerSetting.requireBoolean(value))
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
