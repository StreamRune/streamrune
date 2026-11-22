package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The first failure is reported, then at most once per interval with the count since. */
class FailureLogLimiterTest {

  private final AtomicLong now = new AtomicLong(-1_000);
  private final FailureLogLimiter limiter = new FailureLogLimiter(now::get);

  private void advanceMinutes(long minutes) {
    now.addAndGet(Duration.ofMinutes(minutes).toNanos());
  }

  @Test
  void nothingCountedIsNothingToReport() {
    assertThat(limiter.claim()).isZero();
    assertThat(limiter.total()).isZero();
  }

  @Test
  void theFirstFailureIsReported_thenOncePerInterval_withTheCountSince() {
    limiter.count();
    assertThat(limiter.claim()).isEqualTo(1);

    limiter.count();
    limiter.count();
    assertThat(limiter.claim()).as("within the interval").isZero();

    advanceMinutes(FailureLogLimiter.INTERVAL_MINUTES);
    limiter.count();
    assertThat(limiter.claim()).as("three since the last report").isEqualTo(3);
    assertThat(limiter.claim()).as("nothing since").isZero();
    assertThat(limiter.total()).isEqualTo(4);
  }

  @Test
  void severalFailuresBeforeTheFirstClaimAreReportedTogether() {
    for (int i = 0; i < 5; i++) {
      limiter.count();
    }
    assertThat(limiter.claim()).isEqualTo(5);
  }

  @Test
  void ofRacingClaimsOneGetsTheCount() throws Exception {
    limiter.count();
    AtomicLong reported = new AtomicLong();
    Thread[] threads = new Thread[8];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = Thread.ofVirtual().start(() -> reported.addAndGet(limiter.claim()));
    }
    for (Thread t : threads) {
      t.join(5000);
    }
    assertThat(reported).hasValue(1);
  }
}
