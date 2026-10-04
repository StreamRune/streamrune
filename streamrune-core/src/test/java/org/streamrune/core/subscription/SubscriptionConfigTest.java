package org.streamrune.core.subscription;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SubscriptionConfigTest {

  @Test
  void defaultConfig() {
    var config = SubscriptionConfig.DEFAULT;
    assertTrue(config.listenNotifyEnabled());
    assertEquals(Duration.ofSeconds(5), config.pollingInterval());
    assertEquals(Duration.ofSeconds(1), config.pollingJitter());
  }

  @Test
  void pollingOnlyDisablesListenNotify() {
    var config = SubscriptionConfig.pollingOnly(Duration.ofSeconds(10));
    assertFalse(config.listenNotifyEnabled());
    assertEquals(Duration.ofSeconds(10), config.pollingInterval());
  }

  @Test
  void pollingOnlyScalesJitterToOneFifthOfInterval() {
    assertEquals(
        Duration.ofSeconds(2),
        SubscriptionConfig.pollingOnly(Duration.ofSeconds(10)).pollingJitter());
    assertEquals(
        Duration.ofMillis(20),
        SubscriptionConfig.pollingOnly(Duration.ofMillis(100)).pollingJitter());
  }

  @Test
  void pollingOnlyRejectsNullInterval() {
    assertThrows(IllegalArgumentException.class, () -> SubscriptionConfig.pollingOnly(null));
  }

  @Test
  void rejectsNullPollingInterval() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SubscriptionConfig(true, null, Duration.ofSeconds(1)));
  }

  @Test
  void rejectsZeroPollingInterval() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SubscriptionConfig(true, Duration.ZERO, Duration.ofSeconds(1)));
  }

  @Test
  void rejectsNegativePollingInterval() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SubscriptionConfig(true, Duration.ofSeconds(-5), Duration.ofSeconds(1)));
  }

  @Test
  void rejectsNullPollingJitter() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SubscriptionConfig(true, Duration.ofSeconds(5), null));
  }

  @Test
  void rejectsNegativePollingJitter() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SubscriptionConfig(true, Duration.ofSeconds(5), Duration.ZERO.minusMillis(1)));
  }

  @Test
  void allowsZeroJitter() {
    var config = new SubscriptionConfig(false, Duration.ofSeconds(5), Duration.ZERO);
    assertEquals(Duration.ZERO, config.pollingJitter());
  }
}
