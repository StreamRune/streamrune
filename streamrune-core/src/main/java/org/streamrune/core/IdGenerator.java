package org.streamrune.core;

import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;

/**
 * Generates unique identifiers for events and commands.
 *
 * <p>IDs combine a fixed-width base-36 timestamp prefix with random bytes, so IDs of the same kind
 * sort lexicographically in creation order (millisecond granularity) and are unique.
 *
 * <p><b>Randomness is non-cryptographic.</b> The random component uses {@link ThreadLocalRandom}
 * (not {@link java.security.SecureRandom}) for uniqueness, not unguessability — a single shared
 * SecureRandom serialized id generation under concurrency. StreamRune ids are correlation /
 * causation / audit handles, never security capabilities or tokens, so a guessable id is not a
 * threat. Do NOT use these ids where unguessability is a security requirement.
 */
public final class IdGenerator {

  private static final Instant EPOCH = Instant.parse("2024-01-01T00:00:00Z");

  /** Fixed width of the base-36 timestamp; 36^9 ms covers ~3,200 years from {@link #EPOCH}. */
  private static final int TIMESTAMP_WIDTH = 9;

  private static final long MAX_TIMESTAMP = 101_559_956_668_415L; // 36^9 - 1

  private IdGenerator() {}

  /**
   * Generates a new event ID. Uses time-based prefix for sortability + random suffix.
   *
   * @return unique event ID
   */
  public static EventId generateEventId() {
    return EventId.of(generateIdString("evt_"));
  }

  /**
   * Generates a new command ID.
   *
   * @return unique command ID
   */
  public static CommandId generateCommandId() {
    return CommandId.of(generateIdString("cmd_"));
  }

  private static String prefix(String type) {
    long timestamp = Instant.now().toEpochMilli() - EPOCH.toEpochMilli();
    return type + encodeTimestamp(timestamp);
  }

  /**
   * Encodes milliseconds since {@link #EPOCH} as a fixed-width, zero-padded base-36 string.
   * Zero-padding keeps every encoded timestamp the same length, so lexicographic order matches
   * chronological order (base-36 digits {@code 0-9a-z} are in ASCII order).
   *
   * <p>Visible for testing.
   *
   * @param millisSinceEpoch milliseconds elapsed since {@link #EPOCH}; must be in {@code [0, 36^9)}
   * @return base-36 encoding, zero-padded to exactly {@value #TIMESTAMP_WIDTH} characters
   */
  static String encodeTimestamp(long millisSinceEpoch) {
    if (millisSinceEpoch < 0 || millisSinceEpoch > MAX_TIMESTAMP) {
      throw new IllegalArgumentException(
          "Timestamp out of encodable range [0, 36^9): " + millisSinceEpoch);
    }
    String encoded = Long.toString(millisSinceEpoch, 36);
    return "0".repeat(TIMESTAMP_WIDTH - encoded.length()) + encoded;
  }

  private static String generateIdString(String prefix) {
    // Non-cryptographic random suffix (ThreadLocalRandom): uncontended per-thread generation.
    byte[] bytes = new byte[16];
    ThreadLocalRandom.current().nextBytes(bytes);
    String random = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    // Trim to reasonable length
    return prefix(prefix) + random.substring(0, 16);
  }

  /**
   * Generates a correlation ID for a new business process.
   *
   * @return unique correlation ID
   */
  public static CorrelationId generateCorrelationId() {
    return CorrelationId.of("corr_" + randomUuid());
  }

  /**
   * A well-formed (version-4 layout) UUID built from {@link ThreadLocalRandom} instead of {@code
   * UUID.randomUUID()}, which is backed by a shared SecureRandom. The bits carry no cryptographic
   * guarantee — see the class note — only uniqueness for correlation handles.
   */
  private static UUID randomUuid() {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    long msb = (r.nextLong() & 0xffffffffffff0fffL) | 0x0000000000004000L; // version 4
    long lsb = (r.nextLong() & 0x3fffffffffffffffL) | 0x8000000000000000L; // IETF variant
    return new UUID(msb, lsb);
  }
}
