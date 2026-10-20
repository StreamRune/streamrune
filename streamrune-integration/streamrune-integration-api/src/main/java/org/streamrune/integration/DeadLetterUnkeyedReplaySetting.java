package org.streamrune.integration;

import java.util.Locale;
import org.streamrune.core.DeadLetterRetryPolicy.UnkeyedReplay;

/**
 * The one reading of {@code streamrune.dead-letter.unkeyed-replay}: whether the dead-letter retry
 * runner an integration builds replays entries of commands that ran without an idempotency key on
 * its schedule.
 *
 * <ul>
 *   <li>{@code operator-only} (the default) — {@link UnkeyedReplay#OPERATOR_ONLY}: such an entry
 *       stays in the queue until an operator retries or discards it
 *   <li>{@code automatic} — {@link UnkeyedReplay#AUTOMATIC}: such an entry is replayed like a keyed
 *       one, a second execution whenever its caller already retried it successfully
 * </ul>
 *
 * <p>The three integrations bind the property as text and parse it here, so the spellings and the
 * refusal of an unknown one are the same on each.
 */
public final class DeadLetterUnkeyedReplaySetting {

  /** The property name, {@value}. */
  public static final String PROPERTY = "streamrune.dead-letter.unkeyed-replay";

  private DeadLetterUnkeyedReplaySetting() {}

  /**
   * Parses a configured value, case-insensitively, ignoring surrounding whitespace and accepting
   * {@code _} for {@code -}.
   *
   * @param value the configured text; {@code null} and blank mean the default, {@code
   *     operator-only}
   * @return the policy choice
   * @throws IllegalArgumentException if the value is neither {@code operator-only} nor {@code
   *     automatic}; the message names the property and the value
   */
  public static UnkeyedReplay parse(String value) {
    if (value == null || value.isBlank()) {
      return UnkeyedReplay.OPERATOR_ONLY;
    }
    return switch (value.strip().toLowerCase(Locale.ROOT).replace('_', '-')) {
      case "operator-only" -> UnkeyedReplay.OPERATOR_ONLY;
      case "automatic" -> UnkeyedReplay.AUTOMATIC;
      default ->
          throw new IllegalArgumentException(
              PROPERTY
                  + " must be operator-only or automatic (case-insensitive); got: '"
                  + value
                  + "'");
    };
  }
}
