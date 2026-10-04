package org.streamrune.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Clock} whose instant is set explicitly by the test — no wall-clock reads, no sleeps.
 *
 * <p>Inject into time-dependent fixtures ({@link InMemoryOutboxStore}, {@link InMemorySagaStore},
 * {@link InMemoryDeadLetterQueue}) to test retry backoff, saga timeouts, and timestamp stamping
 * deterministically:
 *
 * <pre>{@code
 * var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
 * var outbox = new InMemoryOutboxStore(clock);
 * // ... markRetry with nextRetryAt 5s in the future ...
 * clock.advance(Duration.ofSeconds(5));
 * // entry is now visible to loadPending
 * }</pre>
 *
 * <p>Thread-safe: the current instant is held in an {@link AtomicReference} and every path goes
 * through it. {@link #advance(Duration)} is an atomic read-modify-write, so advances made
 * concurrently from several threads are never lost; {@link #setInstant(Instant)} is an atomic write
 * that is ordered with them; {@link #instant()}, {@link #millis()} and {@link #withZone(ZoneId)}
 * each read one consistent snapshot. The zone is immutable. {@link #withZone(ZoneId)} returns an
 * <b>independent</b> copy — advancing one clock does not affect the other.
 */
public final class MutableClock extends Clock {

  private final AtomicReference<Instant> instant;
  private final ZoneId zone;

  private MutableClock(Instant instant, ZoneId zone) {
    this.instant = new AtomicReference<>(Objects.requireNonNull(instant, "instant is required"));
    this.zone = Objects.requireNonNull(zone, "zone is required");
  }

  /** Creates a clock at the given instant in UTC. */
  public static MutableClock startingAt(Instant instant) {
    return new MutableClock(instant, ZoneOffset.UTC);
  }

  /** Creates a clock at the given instant in the given zone. */
  public static MutableClock startingAt(Instant instant, ZoneId zone) {
    return new MutableClock(instant, zone);
  }

  /**
   * Moves the clock forward (or backward, with a negative duration) by the given amount,
   * atomically: two threads advancing at once both land.
   */
  public void advance(Duration duration) {
    Objects.requireNonNull(duration, "duration is required");
    instant.updateAndGet(current -> current.plus(duration));
  }

  /** Sets the clock to the given instant. */
  public void setInstant(Instant instant) {
    this.instant.set(Objects.requireNonNull(instant, "instant is required"));
  }

  @Override
  public Instant instant() {
    return instant.get();
  }

  @Override
  public ZoneId getZone() {
    return zone;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return new MutableClock(instant.get(), zone);
  }
}
