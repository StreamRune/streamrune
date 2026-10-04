package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/**
 * The identity of one aggregate instance's event stream: the {@link AggregateType} its decider was
 * registered under plus its {@link AggregateId}. Two aggregate types may use the same id value —
 * {@code product:p-1} and {@code inventory:p-1} are two streams.
 *
 * <p>The command bus builds it once per command, from the decider registration and the id the
 * registration's extractor returned. The database stores the two parts as the {@code
 * (aggregate_type, aggregate_id)} columns of every stream-keyed table and never a composed value;
 * Java, JSON, log lines and the Kafka message key show it as {@code <aggregateType>:<aggregateId>}
 * ({@link #value()}). Applications read the parts with {@link #aggregateType()} and {@link
 * #aggregateId()} and never parse the text.
 *
 * <p>The id is bounded by its column: the constructor refuses an id of more than {@value
 * IdConstraints#MAX_LENGTH} characters, counted as the {@code aggregate_id VARCHAR(255)} columns
 * count them (code points), so a command whose extractor produced one fails before any interceptor
 * runs instead of failing the append, and a value read back from a stream column always passes.
 *
 * @param aggregateType the registered aggregate type
 * @param aggregateId the aggregate id
 */
public record StreamId(AggregateType aggregateType, AggregateId aggregateId) {

  /** Separates the two parts in {@link #value()}; never part of an {@link AggregateType}. */
  public static final char SEPARATOR = ':';

  private static final String NOT_A_STORED_STREAM_ID =
      "a stored stream id must have the form <aggregateType>:<aggregateId>";

  public StreamId {
    Objects.requireNonNull(aggregateType, "aggregateType is required");
    Objects.requireNonNull(aggregateId, "aggregateId is required");
    String id = aggregateId.value();
    int length = id.codePointCount(0, id.length());
    if (length > IdConstraints.MAX_LENGTH) {
      throw new IllegalArgumentException(
          "aggregateId is "
              + length
              + " characters; the aggregate_id columns hold at most "
              + IdConstraints.MAX_LENGTH);
    }
  }

  /**
   * The stream of {@code aggregateId} under {@code aggregateType}.
   *
   * @param aggregateType the registered aggregate type
   * @param aggregateId the aggregate id
   * @return the stream id
   */
  public static StreamId of(AggregateType aggregateType, AggregateId aggregateId) {
    return new StreamId(aggregateType, aggregateId);
  }

  /**
   * The text form {@code <aggregateType>:<aggregateId>}: JSON (a plain string, like every other id
   * type), log lines and the Kafka message key. No column stores it.
   *
   * @return the text form
   */
  @JsonValue
  public String value() {
    return aggregateType.value() + SEPARATOR + aggregateId.value();
  }

  /**
   * The decode door for a text form the framework wrote — a {@code StreamId} inside JSON, or a
   * Kafka message key. Splits at the FIRST separator: a type never contains one, so the split is
   * exact even when the id does. Both parts are rebuilt through their lenient constructors. Never
   * call it on client input: an HTTP request carries the two parts as two values, each built with
   * its {@code of} factory.
   *
   * @param stored the text form
   * @return the stream id
   * @throws IllegalArgumentException if {@code stored} is not {@code <type>:<id>} with both parts
   *     non-empty
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static StreamId parse(String stored) {
    if (stored == null) {
      throw new IllegalArgumentException(NOT_A_STORED_STREAM_ID);
    }
    int separator = stored.indexOf(SEPARATOR);
    if (separator <= 0 || separator == stored.length() - 1) {
      throw new IllegalArgumentException(NOT_A_STORED_STREAM_ID);
    }
    return new StreamId(
        new AggregateType(stored.substring(0, separator)),
        new AggregateId(stored.substring(separator + 1)));
  }

  @Override
  public String toString() {
    return value();
  }
}
