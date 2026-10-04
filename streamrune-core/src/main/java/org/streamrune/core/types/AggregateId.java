package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Type-safe identifier for an aggregate instance.
 *
 * <p>Replaces loose String parameters for aggregate IDs in command handling. Serializes as a plain
 * JSON string, like the other identifier value types.
 *
 * <p><b>Two doors, two rule sets (the same split as {@link IdempotencyKey}).</b> {@link
 * #of(String)} is the INGRESS door — the factory an application's id extractor calls with a value a
 * client just sent (a path variable, a request-body field) — and it applies {@link
 * IdConstraints#requireBoundedAndPrintable}: at most {@value IdConstraints#MAX_LENGTH} characters,
 * and no control character — C0 ({@code U+0000}–{@code U+001F}), DEL ({@code U+007F}) or C1 ({@code
 * U+0080}–{@code U+009F}). An aggregate id is written verbatim into the {@code aggregate_id} column
 * of every stream-keyed table — {@code event_stream}, {@code snapshot_store}, {@code
 * command_inbox}, {@code dead_letter_queue}, {@code outbox_events}, {@code event_audit_log} —
 * beside the {@code aggregate_type} its decider was registered under, and into {@code
 * audit_log.aggregate_id}; no table stores the composed {@code <type>:<id>} (see {@link StreamId}).
 * A CR/LF there forges a line for every reader of those columns, and a NUL fails the PostgreSQL
 * write outright. The stream columns are {@code VARCHAR(255)}: {@code of} refuses a longer id at
 * ingress, and {@link StreamId}'s constructor refuses one wherever the pair is built, so it never
 * reaches the append. Refused here, a command whose extractor uses {@code of} fails with {@link
 * IllegalArgumentException} inside the command bus's id extraction, before any interceptor runs, so
 * nothing is audited, appended or dead-lettered; map the exception to a {@code 400}.
 *
 * <p>The length is counted in UTF-16 code units ({@link String#length()}), as every bound in {@link
 * IdConstraints} is; {@code VARCHAR(255)} counts characters, which in a UTF8 database are code
 * points. A code point is one or two code units, so an id {@code of} accepts is never more than 255
 * characters and always fits. The bound is stricter than the column only for supplementary-plane
 * characters: an id of 128 emoji is 256 code units and is refused, although it is 128 characters.
 * An id the application derives by adding a prefix or suffix to a client value can exceed the bound
 * even when the client value did not, so bound such a value at its own ingress with the headroom
 * the derivation needs.
 *
 * <p>The canonical constructor keeps only the blank rule, because it is the DECODE door: Jackson
 * rebuilds an id through it (inside a dead-lettered command's payload, an event, saga state), and
 * the dead-letter, command-audit and outbox row mappers rebuild a stored id through it. Use it, not
 * {@code of}, wherever the value comes from data already inside the system — a saga or process
 * manager deriving a command's target from its stored state included: a charset or length rule
 * there would stop no write (the value is already stored) and would only make that row unreadable
 * or that saga step poison, the failure {@link IdConstraints} records. A command built that way
 * still reaches the application's id extractor, which applies {@code of} again; a refusal there
 * fails that one dispatch with {@link IllegalArgumentException}, which a saga treats as a business
 * rejection and compensates. Don't declare {@code AggregateId} itself as a request-bound field:
 * Jackson binds it through the lenient constructor. Bind the client value as a {@code String} and
 * build the id with {@code of}. What an id built through the decode door can still carry is
 * neutralised at the sinks the framework owns: {@link LogSanitizer#sanitizeForLog} in every log
 * record it writes, {@link LogSanitizer#sanitizeFreeText} in every error text it persists. The id
 * columns themselves store it verbatim — it is the identity.
 */
public record AggregateId(@JsonValue String value) {

  @JsonCreator
  public AggregateId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("aggregateId is required");
    }
  }

  /**
   * The ingress factory: an aggregate id whose value a client supplied — call it in the id
   * extractor you register with the command bus.
   *
   * @param value the aggregate identifier; non-blank, at most {@value IdConstraints#MAX_LENGTH}
   *     UTF-16 code units and free of control characters
   * @return new AggregateId
   * @throws IllegalArgumentException if {@code value} is blank, is longer than {@value
   *     IdConstraints#MAX_LENGTH} UTF-16 code units, or contains a C0, DEL or C1 control character
   *     (the value is not echoed: it is untrusted input)
   */
  public static AggregateId of(String value) {
    AggregateId id = new AggregateId(value);
    IdConstraints.requireBoundedAndPrintable(value, "aggregateId");
    return id;
  }

  @Override
  public String toString() {
    return value;
  }
}
