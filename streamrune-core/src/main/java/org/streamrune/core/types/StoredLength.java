package org.streamrune.core.types;

/**
 * The length rule for a value a store writes into a bounded text column.
 *
 * <p>PostgreSQL counts a {@code VARCHAR(n)} in characters, which in a UTF8 database are code
 * points, so a value is measured here the same way: a supplementary-plane character counts once,
 * not as its two UTF-16 units. A value that passes is stored by every store; a longer one is
 * refused in core, before any store is reached, so the in-memory stores refuse it exactly as the
 * column would. The refusal states the length and the maximum and never echoes the value.
 *
 * <p>The value types and {@code require…} helpers that bound a stored value call it: {@link
 * EventType}, {@link CommandId}, {@link ProjectionName}, {@link SubscriptionName}, {@link
 * SagaType}, {@code OutboxEntryId}, {@code CommandInbox.requireCommandType}, {@code
 * EventStore.requireStateType}, {@code OutboxEntry} and {@code OutboxStore.requireSkippedBy}.
 */
public final class StoredLength {

  private StoredLength() {}

  /**
   * Refuses {@code value} if it is longer than {@code max} code points.
   *
   * @param value the value; never {@code null} (nullability is the caller's own rule)
   * @param max the longest value accepted, in code points
   * @param name the value's name, used in the failure message
   * @return {@code value}
   * @throws IllegalArgumentException if {@code value} is longer than {@code max} code points; the
   *     message names {@code name}, the length and the maximum, and does not echo the value
   */
  public static String requireAtMost(String value, int max, String name) {
    int length = value.codePointCount(0, value.length());
    if (length > max) {
      throw new IllegalArgumentException(name + " is " + length + " characters; at most " + max);
    }
    return value;
  }
}
