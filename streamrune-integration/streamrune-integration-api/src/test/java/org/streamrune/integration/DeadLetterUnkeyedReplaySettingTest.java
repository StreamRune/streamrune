package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterRetryPolicy.UnkeyedReplay;

class DeadLetterUnkeyedReplaySettingTest {

  @Test
  void unsetMeansOperatorOnly() {
    assertEquals(UnkeyedReplay.OPERATOR_ONLY, DeadLetterUnkeyedReplaySetting.parse(null));
    assertEquals(UnkeyedReplay.OPERATOR_ONLY, DeadLetterUnkeyedReplaySetting.parse("  "));
  }

  @Test
  void acceptsBothSpellingsOfEachValue() {
    for (String v : new String[] {"operator-only", "OPERATOR_ONLY", " Operator-Only "}) {
      assertEquals(UnkeyedReplay.OPERATOR_ONLY, DeadLetterUnkeyedReplaySetting.parse(v), v);
    }
    for (String v : new String[] {"automatic", "AUTOMATIC", " Automatic "}) {
      assertEquals(UnkeyedReplay.AUTOMATIC, DeadLetterUnkeyedReplaySetting.parse(v), v);
    }
  }

  @Test
  void refusesAnUnknownValueNamingThePropertyAndTheValue() {
    var ex =
        assertThrows(
            IllegalArgumentException.class, () -> DeadLetterUnkeyedReplaySetting.parse("true"));
    assertTrue(ex.getMessage().contains(DeadLetterUnkeyedReplaySetting.PROPERTY), ex.getMessage());
    assertTrue(ex.getMessage().contains("'true'"), ex.getMessage());
  }
}
