package org.streamrune.core.subscription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  /**
   * A subscription sleeps whole milliseconds, so half a millisecond would be no pause between two
   * reads of the event store: the interval is refused, and the refusal names the property.
   */
  @Test
  void rejectsAPollingIntervalBelowOneMillisecond() {
    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SubscriptionConfig(false, Duration.ofNanos(500_000), Duration.ZERO));
    assertTrue(refused.getMessage().contains("streamrune.polling-interval"), refused::getMessage);
    assertTrue(refused.getMessage().contains("at least 1ms"), refused::getMessage);
  }

  /** A positive jitter below one millisecond would be slept as no jitter: it is refused. */
  @Test
  void rejectsAPositivePollingJitterBelowOneMillisecond() {
    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SubscriptionConfig(false, Duration.ofSeconds(5), Duration.ofNanos(500_000)));
    assertTrue(refused.getMessage().contains("streamrune.polling-jitter"), refused::getMessage);
  }

  @Test
  void acceptsOneMillisecondForIntervalAndJitter() {
    var config = new SubscriptionConfig(false, Duration.ofMillis(1), Duration.ofMillis(1));
    assertEquals(Duration.ofMillis(1), config.pollingInterval());
    assertEquals(Duration.ofMillis(1), config.pollingJitter());
  }

  /** The derived jitter is whole milliseconds, so a short interval still builds a valid config. */
  @Test
  void pollingOnlyRoundsTheJitterDownToWholeMilliseconds() {
    assertEquals(
        Duration.ZERO, SubscriptionConfig.pollingOnly(Duration.ofMillis(3)).pollingJitter());
    assertEquals(
        Duration.ofMillis(1), SubscriptionConfig.pollingOnly(Duration.ofMillis(7)).pollingJitter());
  }
}
