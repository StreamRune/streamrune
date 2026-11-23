package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SagaParallelismSettingTest {

  @Test
  void blankMeansFour() {
    assertEquals(4, SagaParallelismSetting.parse(null, Map.of()).global());
    assertEquals(4, SagaParallelismSetting.parse("  ", Map.of()).global());
  }

  @Test
  void readsTheGlobalAndThePerSubscriptionValues() {
    var parsed = SagaParallelismSetting.parse("8", Map.of("orders", "16"));

    assertEquals(8, parsed.global());
    assertEquals(Map.of("orders", 16), parsed.bySubscription());
  }

  @Test
  void refusesTextZeroAnd65NamingThePropertyAndTheValue() {
    for (String bad : new String[] {"abc", "0", "65", "4.5"}) {
      var global =
          assertThrows(
              IllegalArgumentException.class, () -> SagaParallelismSetting.parse(bad, Map.of()));
      assertEquals(
          "streamrune.saga.parallelism must be a whole number between 1 and 64; got: '" + bad + "'",
          global.getMessage());
      var perSubscription =
          assertThrows(
              IllegalArgumentException.class,
              () -> SagaParallelismSetting.parse("4", Map.of("orders", bad)));
      assertEquals(
          "streamrune.saga.subscriptions.orders.parallelism must be a whole number between 1 and"
              + " 64; got: '"
              + bad
              + "'",
          perSubscription.getMessage());
    }
  }

  @Test
  void aBlankPerSubscriptionValueIsRefusedWithTheUsualMessage() {
    for (String blank : new String[] {"", "  "}) {
      var refused =
          assertThrows(
              IllegalArgumentException.class,
              () -> SagaParallelismSetting.parse("4", Map.of("orders", blank)));
      assertEquals(
          "streamrune.saga.subscriptions.orders.parallelism must be a whole number between 1 and"
              + " 64; got: '"
              + blank
              + "'",
          refused.getMessage());
    }
  }

  @Test
  void aNullPerSubscriptionValueIsRefusedWithTheUsualMessage() {
    var values = new java.util.HashMap<String, String>();
    values.put("orders", null);

    var refused =
        assertThrows(
            IllegalArgumentException.class, () -> SagaParallelismSetting.parse("4", values));
    assertEquals(
        "streamrune.saga.subscriptions.orders.parallelism must be a whole number between 1 and"
            + " 64; got: 'null'",
        refused.getMessage());
  }

  @Test
  void theRefusalSanitizesTheSubscriptionNameForTheLog() {
    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () -> SagaParallelismSetting.parse("4", Map.of("orders\nINFO forged", "abc")));
    assertEquals(
        "streamrune.saga.subscriptions.ordersINFO forged.parallelism must be a whole number"
            + " between 1 and 64; got: 'abc'",
        refused.getMessage());
  }

  @Test
  void keepsTheOrderOfTheSubscriptions() {
    var values = new java.util.LinkedHashMap<String, String>();
    values.put("zeta", "1");
    values.put("alpha", "2");
    values.put("mid", "3");

    assertEquals(
        java.util.List.of("zeta", "alpha", "mid"),
        java.util.List.copyOf(SagaParallelismSetting.parse("4", values).bySubscription().keySet()));
  }

  @Test
  void theDefaultAndTheBoundsAreTheRuntimesOwn() {
    assertEquals(
        org.streamrune.runtime.SagaRunner.DEFAULT_PARALLELISM, SagaParallelismSetting.DEFAULT);
    assertEquals(
        org.streamrune.runtime.SagaRunner.MAX_PARALLELISM,
        SagaParallelismSetting.parse(
                String.valueOf(org.streamrune.runtime.SagaRunner.MAX_PARALLELISM), Map.of())
            .global());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SagaParallelismSetting.parse(
                String.valueOf(org.streamrune.runtime.SagaRunner.MAX_PARALLELISM + 1), Map.of()));
    assertEquals(1, SagaParallelismSetting.parse("1", Map.of()).global());
    assertThrows(IllegalArgumentException.class, () -> SagaParallelismSetting.parse("0", Map.of()));
  }

  @Test
  void surroundingWhitespaceIsIgnored() {
    assertEquals(8, SagaParallelismSetting.parse(" 8 ", Map.of()).global());
  }

  @Test
  void theBoundsAreAccepted() {
    assertEquals(1, SagaParallelismSetting.parse("1", Map.of()).global());
    assertEquals(64, SagaParallelismSetting.parse("64", Map.of()).global());
  }
}
