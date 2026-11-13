package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ConfigurationSpellingTest {

  @Test
  void aNumberWithoutAUnitIsBare() {
    for (String bare : List.of("0", "5000", "-5", "+5", " 60000 ")) {
      assertTrue(ConfigurationSpelling.isBareNumber(bare), bare);
    }
    for (String withUnit : List.of("0s", "5000ms", "PT5S", "7d", "", "  ", "-5s")) {
      assertFalse(ConfigurationSpelling.isBareNumber(withUnit), withUnit);
    }
    assertFalse(ConfigurationSpelling.isBareNumber(null));
  }

  @Test
  void onlyLowerCaseTrueAndFalseAreSwitches() {
    assertTrue(ConfigurationSpelling.isSwitch("true"));
    assertTrue(ConfigurationSpelling.isSwitch("false"));
    for (String other : List.of("TRUE", "False", "yes", "on", "1", "0", "", " true", "ture")) {
      assertFalse(ConfigurationSpelling.isSwitch(other), other);
    }
  }

  @Test
  void theRefusalNamesEveryPropertyAndValue() {
    assertDoesNotThrow(() -> ConfigurationSpelling.refuse(List.of()));
    var one =
        assertThrows(
            IllegalStateException.class,
            () ->
                ConfigurationSpelling.refuse(
                    List.of(ConfigurationSpelling.bareNumber("streamrune.lock-timeout", "5000"))));
    assertEquals(
        "StreamRune cannot read this configuration value:\n  streamrune.lock-timeout=5000: "
            + ConfigurationSpelling.DURATION_HINT,
        one.getMessage());
    var two =
        assertThrows(
            IllegalStateException.class,
            () ->
                ConfigurationSpelling.refuse(
                    List.of(
                        ConfigurationSpelling.notASwitch("streamrune.saga.enabled", "yes"),
                        ConfigurationSpelling.bareNumber("streamrune.sse.timeout", "0"))));
    assertTrue(two.getMessage().startsWith("StreamRune cannot read these configuration values"));
    assertTrue(
        two.getMessage()
            .contains("streamrune.saga.enabled=yes: " + ConfigurationSpelling.SWITCH_HINT));
  }
}
