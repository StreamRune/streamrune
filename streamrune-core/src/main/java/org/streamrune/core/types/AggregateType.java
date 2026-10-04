package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.regex.Pattern;

/**
 * The registered name of an aggregate type — the first half of every {@link StreamId}. Short,
 * lowercase and stable: it is written into the {@code aggregate_type} column of every stream-keyed
 * table for the life of the store, so renaming it later renames streams. Keep one constant per
 * aggregate in the domain module ({@code public static final AggregateType TYPE =
 * AggregateType.of("order");}) and use it wherever the type is needed.
 *
 * <p><b>Two doors, as for {@link AggregateId}.</b> {@link #of(String)} is the registration and
 * ingress door: it applies the full syntax {@code [a-z][a-z0-9_]{0,31}} (lowercase ASCII letter
 * first, then letters, digits and {@code _}; at most {@value #MAX_LENGTH} characters). Call it in
 * constants, in {@code register(...)} and on an HTTP path variable. The canonical constructor is
 * the decode door (Jackson, row mappers, {@link StreamId#parse}): it is lenient about the alphabet,
 * so a stored row never becomes unreadable over a case difference, but it still refuses the two
 * structural facts the text form {@code <type>:<id>} depends on — a {@code ':'} anywhere and more
 * than {@value #MAX_LENGTH} characters. No message echoes the refused value.
 *
 * @param value the type name
 */
public record AggregateType(@JsonValue String value) {

  /** The {@code aggregate_type} column width. */
  public static final int MAX_LENGTH = 32;

  /** The registration syntax: lowercase ASCII letter, then letters, digits or {@code _}. */
  public static final Pattern SYNTAX = Pattern.compile("[a-z][a-z0-9_]{0,31}");

  /** Decode door: non-blank, no {@code ':'}, at most {@value #MAX_LENGTH} characters. */
  @JsonCreator
  public AggregateType {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("aggregateType is required");
    }
    if (value.indexOf(StreamId.SEPARATOR) >= 0) {
      throw new IllegalArgumentException(
          "aggregateType must not contain '"
              + StreamId.SEPARATOR
              + "' (the separator of the stream id text form)");
    }
    if (value.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          "aggregateType is " + value.length() + " characters; at most " + MAX_LENGTH);
    }
  }

  /**
   * The registration and ingress factory.
   *
   * @param value the type name; must match {@link #SYNTAX}
   * @return the aggregate type
   * @throws IllegalArgumentException if {@code value} is blank, longer than {@value #MAX_LENGTH}
   *     characters or outside {@link #SYNTAX} (the value is not echoed)
   */
  public static AggregateType of(String value) {
    AggregateType type = new AggregateType(value);
    if (!SYNTAX.matcher(value).matches()) {
      throw new IllegalArgumentException(
          "aggregateType must match "
              + SYNTAX.pattern()
              + " (lowercase ASCII letters, digits and '_', starting with a letter, at most "
              + MAX_LENGTH
              + " characters)");
    }
    return type;
  }

  @Override
  public String toString() {
    return value;
  }
}
