package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MutableClockTest {

  private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void startsAtTheGivenInstantInUtc() {
    var clock = MutableClock.startingAt(START);
    assertEquals(START, clock.instant());
    assertEquals(ZoneOffset.UTC, clock.getZone());
  }

  @Test
  void startsAtTheGivenInstantInTheGivenZone() {
    var prague = ZoneId.of("Europe/Prague");
    var clock = MutableClock.startingAt(START, prague);
    assertEquals(START, clock.instant());
    assertEquals(prague, clock.getZone());
  }

  @Test
  void advanceMovesTheClockForward() {
    var clock = MutableClock.startingAt(START);
    clock.advance(Duration.ofSeconds(90));
    assertEquals(START.plusSeconds(90), clock.instant());
  }

  @Test
  void advanceWithNegativeDurationMovesTheClockBackward() {
    var clock = MutableClock.startingAt(START);
    clock.advance(Duration.ofSeconds(-30));
    assertEquals(START.minusSeconds(30), clock.instant());
  }

  @Test
  void setInstantJumpsToTheGivenInstant() {
    var clock = MutableClock.startingAt(START);
    var later = Instant.parse("2026-06-15T08:30:00Z");
    clock.setInstant(later);
    assertEquals(later, clock.instant());
  }

  @Test
  void withZoneReturnsAnIndependentCopy() {
    var clock = MutableClock.startingAt(START);
    var copy = clock.withZone(ZoneId.of("Europe/Prague"));

    assertEquals(ZoneId.of("Europe/Prague"), copy.getZone());
    assertEquals(START, copy.instant());

    clock.advance(Duration.ofHours(1));
    assertEquals(START, copy.instant(), "advancing the original must not move the copy");
  }

  @Test
  void concurrentAdvancesAreNeverLost() throws Exception {
    // The javadoc promises thread safety, and the in-memory fixtures share one clock across the
    // threads of a concurrency test. advance() used to read the volatile instant, add, and write it
    // back as two separate steps, so two threads advancing at once could both read the same
    // instant and one advance was lost. Every advance must land: the final instant is the start
    // plus the sum of all of them.
    int threads = 8;
    int advancesPerThread = 20_000;
    var clock = MutableClock.startingAt(START);
    var ready = new CountDownLatch(threads);
    var go = new CountDownLatch(1);
    try (var pool = Executors.newFixedThreadPool(threads)) {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            pool.submit(
                () -> {
                  ready.countDown();
                  go.await();
                  for (int i = 0; i < advancesPerThread; i++) {
                    clock.advance(Duration.ofMillis(1));
                  }
                  return null;
                }));
      }
      assertTrue(ready.await(10, TimeUnit.SECONDS), "all advancing threads must start");
      go.countDown();
      for (var future : futures) {
        future.get(60, TimeUnit.SECONDS);
      }
    }
    assertEquals(
        START.plusMillis((long) threads * advancesPerThread),
        clock.instant(),
        "an advance racing another advance must not be lost");
  }

  @Test
  void rejectsNullArguments() {
    var clock = MutableClock.startingAt(START);
    assertThrows(NullPointerException.class, () -> MutableClock.startingAt(null));
    assertThrows(NullPointerException.class, () -> MutableClock.startingAt(START, null));
    assertThrows(NullPointerException.class, () -> clock.advance(null));
    assertThrows(NullPointerException.class, () -> clock.setInstant(null));
  }
}
