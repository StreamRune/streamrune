package org.streamrune.core.subscription;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SubscriptionHealthTest {

  @Test
  void construction_validFields() {
    var health =
        new SubscriptionHealth(
            "orders", SubscriptionLifecycleState.RUNNING, 42, 3, SubscriptionHealth.Status.UP);
    assertEquals("orders", health.name());
    assertEquals(SubscriptionLifecycleState.RUNNING, health.state());
    assertEquals(42, health.lag());
    assertEquals(3, health.errorCount());
    assertEquals(SubscriptionHealth.Status.UP, health.status());
  }

  @Test
  void statusEnum_allValues() {
    var values = SubscriptionHealth.Status.values();
    assertEquals(3, values.length);
    assertEquals(SubscriptionHealth.Status.UP, SubscriptionHealth.Status.valueOf("UP"));
    assertEquals(SubscriptionHealth.Status.DOWN, SubscriptionHealth.Status.valueOf("DOWN"));
    assertEquals(SubscriptionHealth.Status.DEGRADED, SubscriptionHealth.Status.valueOf("DEGRADED"));
  }

  @Test
  void equality() {
    var a =
        new SubscriptionHealth(
            "x", SubscriptionLifecycleState.RUNNING, 0, 0, SubscriptionHealth.Status.UP);
    var b =
        new SubscriptionHealth(
            "x", SubscriptionLifecycleState.RUNNING, 0, 0, SubscriptionHealth.Status.UP);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void nullName_throws() {
    assertThrows(
        NullPointerException.class,
        () ->
            new SubscriptionHealth(
                null, SubscriptionLifecycleState.RUNNING, 0, 0, SubscriptionHealth.Status.UP));
  }

  @Test
  void nullState_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new SubscriptionHealth("x", null, 0, 0, SubscriptionHealth.Status.UP));
  }

  @Test
  void nullStatus_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new SubscriptionHealth("x", SubscriptionLifecycleState.RUNNING, 0, 0, null));
  }

  @Test
  void negativeLag_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SubscriptionHealth(
                "x", SubscriptionLifecycleState.RUNNING, -1, 0, SubscriptionHealth.Status.UP));
  }

  @Test
  void negativeErrorCount_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SubscriptionHealth(
                "x", SubscriptionLifecycleState.RUNNING, 0, -1, SubscriptionHealth.Status.UP));
  }
}
