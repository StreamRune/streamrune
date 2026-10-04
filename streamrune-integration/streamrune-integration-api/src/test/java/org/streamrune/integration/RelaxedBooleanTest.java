package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for the canonical {@link RelaxedBoolean} parser shared by the three integrations. */
class RelaxedBooleanTest {

  @ParameterizedTest
  @ValueSource(
      strings = {"true", "TRUE", "True", "on", "ON", "yes", "YES", "y", "Y", "t", "1", " on "})
  void canonicalAndNonCanonicalTrueValues(String value) {
    assertTrue(RelaxedBoolean.parse(value, false), () -> "'" + value + "' should parse true");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"false", "FALSE", "False", "off", "OFF", "no", "NO", "n", "N", "f", "0", " off "})
  void canonicalAndNonCanonicalFalseValues(String value) {
    assertFalse(RelaxedBoolean.parse(value, true), () -> "'" + value + "' should parse false");
  }

  @Test
  void nullOrBlankUsesDefault() {
    assertTrue(RelaxedBoolean.parse(null, true));
    assertFalse(RelaxedBoolean.parse(null, false));
    assertTrue(RelaxedBoolean.parse("", true));
    assertFalse(RelaxedBoolean.parse("   ", false));
  }

  @ParameterizedTest
  @ValueSource(strings = {"treu", "enable", "2", "maybe", "on off", "nope"})
  void unrecognizedValueFailsFast(String value) {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> RelaxedBoolean.parse(value, true));
    assertTrue(
        ex.getMessage().contains(value), () -> "message should echo the bad value: " + value);
  }
}
