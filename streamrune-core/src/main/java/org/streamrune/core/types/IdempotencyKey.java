package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Caller-supplied deduplication identity for a command. Passed to {@link
 * org.streamrune.core.CommandBus#execute(org.streamrune.core.Command, IdempotencyKey)} to enable
 * effectively-once delivery: the bus checks the {@link org.streamrune.core.CommandInbox} before
 * running the handler, and commits the inbox row atomically with the command's events via {@link
 * org.streamrune.core.EventStore#appendWithKey}. A redelivered call with the same key returns the
 * previously recorded result without re-running the handler.
 *
 * <p>Saga commands use deterministic keys derived from the triggering event's global offset and the
 * command's index in the dispatch list — see {@code SagaCommandDispatch.forwardKey} and {@code
 * SagaCommandDispatch.episodeCompensationKey}.
 *
 * <p><b>The key namespace is caller-owned and trusted.</b> For one command type on one stream, a
 * key is a <em>bearer token for the recorded result</em>: whoever presents it receives that
 * execution's outcome, and their own command is deduplicated away instead of executed. That is
 * precisely what a redelivery needs — and a hazard when keys are derived from untrusted client
 * input. The framework deliberately does not bind an inbox row to an ambient caller identity: it
 * cannot see the application's tenant model, and the same logical command legitimately reaches the
 * bus under different (or absent) request contexts — the saga timeout runner and the
 * correlated-event path share one compensation key by design, across threads that do not share a
 * principal, so identity-scoping would either execute a compensation twice or reject it forever.
 * Applications that build keys from client input must namespace them themselves: see {@link
 * #scopedTo(String, String)}.
 *
 * <p><b>Two doors, two rule sets.</b> {@link #of(String)} and {@link #scopedTo(String, String)} are
 * the INGRESS doors — the factories an application calls with a value it just read off the wire —
 * and they refuse control characters: C0 ({@code U+0000}–{@code U+001F}), DEL ({@code U+007F}) and
 * C1 ({@code U+0080}–{@code U+009F}). A key is echoed into an exception message when it is reused
 * across commands, and that message is persisted into {@code audit_log} and logged; CR/LF there
 * forge a line, and a NUL makes the PostgreSQL write fail outright. The canonical constructor keeps
 * only the blank and length rules, because it is the DECODE door: Jackson rebuilds a key through
 * it, the dead-letter row mapper rebuilds a stored key through it, and the framework derives saga
 * and DLQ-replay keys through it from ids it read back from storage — a {@link
 * org.streamrune.core.saga.SagaId} is bounded in length, not in charset. A charset rule there would
 * stop no write (the value is already inside) and would make that row unreadable and that saga's
 * commands undispatchable, the failure {@link IdConstraints} records. What a key built through that
 * door can still carry is neutralised at every sink instead: {@link LogSanitizer#sanitizeForLog}
 * where it is rendered into a message, {@link LogSanitizer#sanitizeFreeText} where a message is
 * persisted.
 */
public record IdempotencyKey(@JsonValue String value) {

  /**
   * Separates the caller-supplied scope from the key in {@link #scopedTo}. Forbidden inside a
   * scope, so exactly one parse of {@code scope|key} exists.
   */
  private static final char SCOPE_SEPARATOR = '|';

  @JsonCreator
  public IdempotencyKey {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("idempotencyKey is required");
    }
    if (value.length() > 512) {
      throw new IllegalArgumentException("idempotencyKey must be at most 512 characters");
    }
  }

  /**
   * The ingress factory: a key whose value the application supplies, typically from a request.
   *
   * @param value the key; non-blank, at most 512 characters, free of control characters
   * @throws IllegalArgumentException if {@code value} is blank, longer than 512 characters, or
   *     contains a C0, DEL or C1 control character (the value is not echoed: it is untrusted input)
   */
  public static IdempotencyKey of(String value) {
    IdempotencyKey key = new IdempotencyKey(value);
    IdConstraints.requireNoControlCharacters(value, "idempotencyKey");
    return key;
  }

  /**
   * Builds a key inside an isolation namespace the application controls — the authenticated
   * principal, the tenant, or whatever boundary it enforces — so two callers presenting the same
   * client-supplied {@code key} cannot collide on one inbox row.
   *
   * <p>Use this whenever {@code key} is derived from client input (an {@code Idempotency-Key}
   * header, a request id). Plain {@link #of(String)} is right when the key is already
   * server-derived and unguessable, as saga keys are.
   *
   * <p>The encoding is {@code scope + '|' + key}, and {@code scope} may not itself contain {@code
   * '|'} — so no client-supplied {@code key} can be crafted to parse as a different scope. It is
   * purely deterministic (same scope, same key, same result), so redeliveries still dedupe. The
   * combined value is subject to the same 512-character limit as any other key.
   *
   * <p>Both parts are refused when they carry a control character, exactly as {@link #of(String)}
   * refuses one — the scope as well as the key: a principal name or tenant id is no more
   * charset-safe for having been authenticated.
   *
   * @param scope the isolation namespace; non-blank, free of {@code '|'} and of control characters
   * @param key the operation identity within that scope; non-blank and free of control characters
   * @throws IllegalArgumentException if {@code scope} is blank or contains {@code '|'}, if {@code
   *     key} is blank, if either contains a C0, DEL or C1 control character, or if the combined
   *     value exceeds 512 characters
   */
  public static IdempotencyKey scopedTo(String scope, String key) {
    if (scope == null || scope.isBlank()) {
      throw new IllegalArgumentException("scope is required");
    }
    if (scope.indexOf(SCOPE_SEPARATOR) >= 0) {
      throw new IllegalArgumentException(
          "scope must not contain '"
              + SCOPE_SEPARATOR
              + "' — it separates the scope from the key, and an ambiguous split would let one"
              + " caller's key impersonate another scope");
    }
    if (key == null || key.isBlank()) {
      throw new IllegalArgumentException("key is required");
    }
    IdConstraints.requireNoControlCharacters(scope, "scope");
    IdConstraints.requireNoControlCharacters(key, "key");
    return new IdempotencyKey(scope + SCOPE_SEPARATOR + key);
  }

  @Override
  public String toString() {
    return value;
  }
}
