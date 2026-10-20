package org.streamrune.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Filters OpenTelemetry baggage down to an allowlist of keys before it is captured into {@link
 * StreamRuneContext.RequestContext#baggage()} — and from there into every produced {@link
 * EventMetadata#baggage()}, which is persisted verbatim into the append-only, plaintext {@code
 * event_stream.metadata} column.
 *
 * <p>OTel baggage is populated from the client-supplied, unauthenticated W3C {@code baggage:} HTTP
 * header. Without filtering, any caller can smuggle arbitrary data — including PII — permanently
 * into the immutable event log, outside crypto-shredding's reach (a {@code Map} field cannot be
 * {@code @Encrypted}, and metadata must stay queryable). Each framework's request filter (Spring's
 * {@code ScopedValueFilter}, Micronaut's {@code StreamRuneContextFilter}, Quarkus's {@code
 * StreamRuneRequestFilter}) calls {@link #filter(Map, Set)} on the raw baggage it reads from OTel
 * before placing it into the request context, so non-allowlisted keys never reach {@code
 * RequestContext} or {@code EventMetadata} in the first place.
 *
 * <p>The default allowlist is empty: the framework reads no key of incoming baggage, so with no
 * configuration no baggage entry of a request reaches event metadata. A baggage entry is data the
 * application asked to keep and nothing more. No entry, under any key, becomes the {@link
 * EventMetadata#causationId() causation id} or the {@link EventMetadata#sagaOrigin() saga origin}
 * of an event; the command bus takes both from {@link StreamRuneContext#CAUSE}, which no request
 * filter binds.
 *
 * <p>{@link #ROLE_BAGGAGE_KEY} is deliberately <em>not</em> a default key. The request context's
 * role is set by each framework filter's role rule ({@code RequestIdentityPolicy#applyRole} in
 * {@code streamrune-integration-api}): the {@code X-User-Role} header, read only in the
 * trusted-gateway identity mode, and nothing else — a client's W3C {@code baggage: role=} entry
 * never reaches it, whatever the allowlist says.
 *
 * <p>Applications that rely on baggage keys reaching event metadata configure them via each
 * framework's {@code streamrune.metadata.baggage-allowlist} property (the list of permitted keys).
 * Listing {@code role} there has no effect: the request filters' role rule replaces whatever the
 * baggage carried under it.
 */
public final class BaggageAllowlist {

  /**
   * Baggage key under which each framework filter places the {@code X-User-Role} header — in the
   * trusted-gateway identity mode only, and never from incoming baggage (see the class comment).
   */
  public static final String ROLE_BAGGAGE_KEY = "role";

  /**
   * The baggage keys kept regardless of configuration: none. Every key that reaches the request
   * context is one the application listed; {@link #ROLE_BAGGAGE_KEY} is not a default key either
   * (see the class comment).
   */
  public static final Set<String> DEFAULT_ALLOWED_KEYS = Set.of();

  private BaggageAllowlist() {}

  /**
   * Maximum length of a retained baggage value. A value longer than this is DROPPED, not truncated.
   *
   * <p>Legitimate values are ids and short labels: a tenant id is a UUID (36 chars), a role is a
   * word. 256 is far above any such value and far below the ~8 KB an HTTP header permits.
   * Truncation was rejected deliberately — the first 256 characters of a smuggled medical note are
   * still the note, and silently shortening an application's legitimate value would corrupt it
   * rather than reject it.
   */
  public static final int MAX_VALUE_LENGTH = 256;

  /**
   * Returns a copy of {@code raw} containing only entries whose key is in {@link
   * #DEFAULT_ALLOWED_KEYS} or in {@code extraAllowedKeys}, with {@link #sanitize(Map) the value
   * policy} applied to every retained entry. Entries for every other key — including arbitrary
   * client-supplied W3C baggage entries such as an email address — are dropped.
   *
   * @param raw the raw baggage read from the OTel context (or an equivalent source); {@code null}
   *     is treated as empty
   * @param extraAllowedKeys the keys to permit besides {@link #DEFAULT_ALLOWED_KEYS} (which is
   *     empty), typically sourced from the {@code streamrune.metadata.baggage-allowlist}
   *     configuration property; {@code null} is treated as empty
   * @return an immutable map containing only the allowlisted, value-policy-compliant entries
   *     present in {@code raw}
   */
  public static Map<String, String> filter(Map<String, String> raw, Set<String> extraAllowedKeys) {
    if (raw == null || raw.isEmpty()) {
      return Map.of();
    }
    Set<String> allowed = new HashSet<>(DEFAULT_ALLOWED_KEYS);
    if (extraAllowedKeys != null) {
      allowed.addAll(extraAllowedKeys);
    }
    Map<String, String> result = new HashMap<>();
    for (var entry : raw.entrySet()) {
      if (!allowed.contains(entry.getKey())) {
        continue;
      }
      String value = sanitizeValue(entry.getValue());
      if (value != null) {
        result.put(entry.getKey(), value);
      }
    }
    return Map.copyOf(result);
  }

  /**
   * Applies the baggage VALUE policy — independent of the key allowlist — dropping every entry
   * whose value does not survive it. Returns an immutable map.
   *
   * <p>A {@link #filter(Map, Set)} scoped to keys only would drop non-allowlisted keys but copy
   * allowlisted entries' values <em>verbatim</em>. With {@link #ROLE_BAGGAGE_KEY} on an allowlist,
   * an unauthenticated caller could send {@code baggage: role=<a victim's national id / free-text
   * medical note / 8 KB of anything>} — or the {@code X-User-Role} header, which each framework
   * filter adds AFTER calling {@code filter} (only in the trusted-gateway mode) — and that value
   * would be written into the append-only, plaintext {@code event_stream.metadata} column of every
   * event the command produces. That column is outside crypto-shredding's reach and the framework
   * offers no API to rewrite or delete it, so a GDPR Article-17 erasure cannot remove the injected
   * data.
   *
   * <p>{@link StreamRuneContext.RequestContext} applies this to whatever baggage it is handed, so
   * the post-{@code filter} merge is closed at the boundary every path crosses on the way to {@code
   * EventMetadata} rather than in each framework's request filter separately.
   *
   * <p>Policy, applied per value:
   *
   * <ul>
   *   <li>{@code null}, or longer than {@link #MAX_VALUE_LENGTH} — the entry is dropped.
   *   <li>Unicode control characters are stripped. A NUL breaks the {@code jsonb} write outright;
   *       CR/LF forge log lines and confuse anything that reads metadata as text.
   *   <li>If nothing but whitespace remains, the entry is dropped.
   * </ul>
   *
   * <p>Dropping is silent by design: this is a boundary control against untrusted input, and
   * logging the rejected value would copy the smuggled payload into the logs — the same disclosure
   * in a different store.
   *
   * @param baggage the baggage to apply the value policy to; {@code null} is treated as empty
   * @return an immutable map of the entries that survived the policy
   */
  public static Map<String, String> sanitize(Map<String, String> baggage) {
    if (baggage == null || baggage.isEmpty()) {
      return Map.of();
    }
    Map<String, String> result = new HashMap<>();
    for (var entry : baggage.entrySet()) {
      String value = sanitizeValue(entry.getValue());
      if (entry.getKey() != null && value != null) {
        result.put(entry.getKey(), value);
      }
    }
    return Map.copyOf(result);
  }

  /** Returns the policy-compliant form of {@code value}, or {@code null} if it must be dropped. */
  private static String sanitizeValue(String value) {
    if (value == null || value.length() > MAX_VALUE_LENGTH) {
      return null;
    }
    String stripped = value;
    for (int i = 0; i < value.length(); i++) {
      if (Character.isISOControl(value.charAt(i))) {
        stripped = stripControlCharacters(value);
        break;
      }
    }
    return stripped.isBlank() ? null : stripped;
  }

  private static String stripControlCharacters(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (!Character.isISOControl(c)) {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
