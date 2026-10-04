package org.streamrune.core.types;

/**
 * INGRESS bounds for the identifier value types that are built directly from unauthenticated HTTP
 * headers and then persisted into the append-only event log.
 *
 * <p>{@code X-Correlation-Id} and {@code X-Trace-Id} are client-supplied, and each framework's
 * request filter turns them into a {@link CorrelationId} / {@link TraceId} with no validation
 * beyond "not blank". Those values reach {@code event_stream.metadata} (plaintext, immutable,
 * outside crypto-shredding's reach) on every event a request produces, {@code
 * audit_log.correlation_id} (an unbounded {@code TEXT} column) and the {@code VARCHAR(255)}
 * request-context columns of {@code dead_letter_queue}. Unbounded, that is a write-amplified
 * storage-and-disclosure channel with no erasure path; a NUL additionally breaks the {@code jsonb}
 * metadata write outright, and CR/LF forge log lines.
 *
 * <p><b>Three values cross that door, not two.</b> {@link UserId} does too, and is bounded there on
 * the same terms. "Already authenticated" is not "bounded and charset-safe": in header-identity
 * mode ({@code X-User-Id} with no resolver wired, or {@code trust-user-id-header=true}) the value
 * is client-supplied outright, and in authenticated mode it is whatever the identity provider
 * asserted — a JWT {@code sub}, a self-service signup username. It reaches every channel listed
 * above, plus one that makes it worse than a disclosure vector: an identity longer than {@link
 * #MAX_LENGTH} cannot be written to {@code dead_letter_queue.user_id VARCHAR(255)} at all, and that
 * INSERT failure is caught and logged inside {@code VirtualThreadCommandBus} rather than propagated
 * — so an already-accepted command's ONLY recovery channel is silently never created. Enforced at
 * {@code fromRequest} and NOT in {@link UserId}'s constructor, for the reason the whole section
 * below gives: that type is rebuilt from stored columns by the DLQ, command-audit and event-audit
 * row mappers.
 *
 * <p>Rejecting rather than truncating is deliberate: a truncated identifier is a <em>wrong</em>
 * identifier, and it would silently break the correlation the id exists to provide. A caller
 * sending a 4 KB "id" is not making a typo. For an <em>identity</em> the argument is stronger
 * still: two principals sharing a {@value #MAX_LENGTH}-character prefix truncate to the same value,
 * and {@code Authorization.requireOwner} authorizes against exactly that field — truncation would
 * make an authorization decision on a value nobody chose.
 *
 * <p><b>Where this runs, and where it deliberately does not.</b> This check is applied at exactly
 * one place: {@link org.streamrune.core.StreamRuneContext.RequestContext#fromRequest}, the factory
 * all three framework request filters call with the values they just read off the wire. It is NOT
 * applied in the compact constructors of {@link CorrelationId} / {@link TraceId}, because those
 * also run on the READ path: {@code EventMetadata} is rebuilt through Jackson on every {@code
 * PostgresEventStore.load}, every projection catch-up page and every live subscription delivery,
 * and the DLQ/audit row mappers rebuild the same ids from stored columns. A constraint enforced
 * there would not stop anything from being written — the write already happened, through a path
 * that never crosses the ingress door (a context built with the plain {@code RequestContext}
 * constructor, a saga-derived correlation id, {@code EventMetadata} an application built and
 * appended itself) — it would only make that data permanently unreadable: the aggregate could never
 * be loaded again (every command on it fails, forever, with no self-heal because the log is
 * append-only and there is no metadata-rewrite API) and any projection reading past that offset
 * would halt there. Ingress validation stops bad data from entering; it must never be repurposed as
 * a decoding rule for data that is already inside.
 *
 * <p><b>Nor is it applied in the {@code RequestContext} CONSTRUCTOR.</b> That constructor reads
 * like a boundary but is not one, and putting the check there re-created the same defect one call
 * further down. A {@code RequestContext} is built at ingress by the three request filters — and it
 * is also RECONSTRUCTED from storage on two paths that read no untrusted input at all:
 *
 * <ul>
 *   <li>{@code DeadLetterRetryRunner.replayContextFrom} rebuilds it from the persisted {@code
 *       dead_letter_queue} correlation/user/trace columns so a dead-lettered command replays with
 *       its original identity. The bus persists whatever context the command ran under, so an entry
 *       can carry an id the bound refuses (its context was built with the plain constructor and
 *       never crossed the door); rejecting there fails every retry of that entry deterministically,
 *       burning the whole ladder and permanently discarding an already-accepted business command.
 *   <li>{@code SagaCommandDispatch.executeCorrelated} rebuilds it from {@code
 *       CorrelationId.of(sagaId.value())}, where the {@code SagaId} came straight out of {@code
 *       saga_state} (or {@code saga_dead_letters}) via {@code PostgresSagaStore}. A {@code SagaId}
 *       is bounded in length, not in charset; rejecting there terminally FAILS an in-flight saga
 *       whose id the bound refuses, with its undo never dispatched.
 * </ul>
 *
 * <p><b>What then stops an untrusted value from entering through the reconstruction door?</b> Four
 * things, and none of them is this check. (1) The door is fed exclusively by values already at rest
 * in stores the framework itself wrote — never by a socket read; every path that reads a header
 * goes through {@code fromRequest}. (2) The write those values came from is already committed, so
 * rejecting them now prevents nothing and only destroys the recovery path. (3) The persisted length
 * arm is enforced by the schema: {@code dead_letter_queue}'s context columns are {@code
 * VARCHAR(255)} and {@code saga_state.saga_id} is {@code VARCHAR(255)}, which {@code SagaId}
 * mirrors at construction. (4) The one residual the bound names that a stored value can still reach
 * — CR/LF forging a log line — was never closed by checking here anyway: the id has already been
 * read out of the row, and in {@code SagaTimeoutRunner}'s case already interpolated into a {@code
 * WARNING}, before any context is built. Rejecting on this path makes that strictly worse, by
 * turning one stored row into a failure logged on every poll cycle. Log forging is a property of
 * the SINK and belongs to the {@code LogSanitizer} policy, which is applied per sink.
 *
 * <p>The one honest consequence of accepting these values: a replay re-emits a stored id the bound
 * would refuse into the metadata of the new events it produces. That is a one-for-one re-emission
 * of a value already in the log — not a channel a remote caller can drive, since every ingress path
 * goes through {@code fromRequest} — and preserving it verbatim is the point. A rewritten id would
 * orphan the replayed command from the events it already produced, and would change every
 * idempotency key derived from it (both saga dispatch keys embed the correlation value), so a
 * rolling upgrade would execute the same compensation twice.
 *
 * <p><b>Ingress FACTORIES.</b> Two value types are built by the application itself from a value it
 * just read off the wire, not by a framework filter: {@link IdempotencyKey} (a client retry token)
 * and {@link AggregateId} (the target a command is routed to — a path variable or a request-body
 * field the application's id extractor hands to the bus). For each, the type's static factory is
 * the ingress door — {@link IdempotencyKey#of} and {@link IdempotencyKey#scopedTo} apply {@link
 * #requireNoControlCharacters}, {@link AggregateId#of} applies {@link #requireBoundedAndPrintable}
 * — while the canonical constructor, Jackson's creator, stays the lenient decode door for the
 * reasons above: the dead-letter, command-audit and outbox row mappers rebuild stored ids through
 * it, Jackson rebuilds ids inside dead-lettered command payloads, events and saga state through it,
 * and a saga deriving a command's target from its stored state is a reconstruction, not an ingress.
 * A control character in an aggregate id reached the {@code aggregate_id} columns of {@code
 * event_stream}, {@code audit_log} and {@code dead_letter_queue} verbatim — a CR/LF forges a line
 * for every reader of those columns, and a NUL fails the PostgreSQL write outright (for the
 * dead-letter row that loses the command's only recovery channel). Refused at {@link
 * AggregateId#of}, a routed command fails with {@link IllegalArgumentException} inside the command
 * bus's id extraction, before any interceptor runs — so nothing is audited, appended or
 * dead-lettered — and the application maps it to a {@code 400} like the blank-id refusal at the
 * same point.
 *
 * <p>The length arm is the aggregate id's too, because every stream-keyed table stores the
 * aggregate id in its own {@code aggregate_id VARCHAR(255)} column, beside the {@code
 * aggregate_type} it was registered under ({@code event_stream}, {@code snapshot_store}, {@code
 * command_inbox}, {@code dead_letter_queue}, {@code outbox_events}). A longer id passed the charset
 * rule, then failed the append with an SQL error — a server error for a malformed request — and
 * failed the dead-letter insert on the same column; dispatched by a saga, that SQL error is
 * retry-later evidence, so the triggering event was redelivered without end. Refused at {@link
 * AggregateId#of} it is the same client error as a control character, and a saga's forward dispatch
 * compensates it like any business rejection. {@link StreamId}'s constructor enforces the same
 * bound, counted in code points as the columns count it, on both doors and before any interceptor
 * runs. The idempotency-key factories keep the charset rule only: a key's 512-character bound
 * ({@code command_inbox.idempotency_key VARCHAR(512)}) is a rule of both of its doors.
 */
public final class IdConstraints {

  /**
   * Maximum length of a header-sourced identifier and of a client-supplied aggregate id. Matches
   * the {@code VARCHAR(255)} columns these ids are persisted into (e.g. {@code
   * dead_letter_queue.correlation_id} / {@code user_id} / {@code trace_id} and {@code
   * event_stream.aggregate_id} in the PostgreSQL event-store schema), and is an order of magnitude
   * above any legitimate value — a W3C trace id is 32 hex characters and a generated correlation id
   * is a 36-character UUID.
   *
   * <p>Counted in UTF-16 code units ({@link String#length()}), the "characters" of the failure
   * message. PostgreSQL counts a {@code VARCHAR(n)} in characters, which in a UTF8 database are
   * code points, and a code point is one or two code units: a value within this bound is never more
   * than 255 characters, so it always fits the column. It can refuse a value the column would hold
   * — one with supplementary-plane characters, which count twice here — but never accepts one the
   * column would not. {@link StreamId} reuses the constant counted in code points, as the columns
   * count it.
   */
  public static final int MAX_LENGTH = 255;

  private IdConstraints() {}

  /**
   * Enforces the ingress bound on an untrusted identifier value — a header-sourced id, or a
   * client-supplied aggregate id. Call this only where a value ENTERS the system; never on a value
   * being reconstructed from storage — and note that "a constructor every ingress path happens to
   * cross" is not the same thing as "a constructor only ingress paths cross". The callers are
   * {@link org.streamrune.core.StreamRuneContext.RequestContext#fromRequest} and {@link
   * AggregateId#of}.
   *
   * @param value the identifier value; never {@code null} (nullability is each id type's own
   *     invariant, checked by the caller before it reaches here)
   * @param name the identifier's name, used in the failure message
   * @throws IllegalArgumentException if {@code value} exceeds {@link #MAX_LENGTH} UTF-16 code units
   *     or contains a Unicode control character; the message names {@code name} and the length but
   *     never echoes the value
   */
  public static void requireBoundedAndPrintable(String value, String name) {
    if (value.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          name + " must be at most " + MAX_LENGTH + " characters, got " + value.length());
    }
    requireNoControlCharacters(value, name);
  }

  /**
   * Enforces the ingress charset rule on an untrusted identifier value: no C0 ({@code
   * U+0000}–{@code U+001F}), DEL ({@code U+007F}) or C1 ({@code U+0080}–{@code U+009F}) character —
   * exactly {@link Character#isISOControl(char)}. The charset arm of {@link
   * #requireBoundedAndPrintable}, and on its own the rule the ingress factories {@link
   * IdempotencyKey#of} and {@link IdempotencyKey#scopedTo} apply. The same placement rule holds:
   * call it only where a value ENTERS the system, never on a value being rebuilt from storage.
   *
   * @param value the identifier value; never {@code null}
   * @param name the identifier's name, used in the failure message
   * @throws IllegalArgumentException if {@code value} contains a control character; the message
   *     names {@code name} and the refused ranges but never echoes the value
   */
  public static void requireNoControlCharacters(String value, String name) {
    for (int i = 0; i < value.length(); i++) {
      if (Character.isISOControl(value.charAt(i))) {
        // The offending value is deliberately NOT echoed: it is untrusted input, and this message
        // would copy it into the very log line, audit row or response the rule exists to protect.
        throw new IllegalArgumentException(
            name + " must not contain control characters (U+0000-U+001F, U+007F, U+0080-U+009F)");
      }
    }
  }
}
