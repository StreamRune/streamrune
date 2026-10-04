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
 * <p>The default allowlist covers only the baggage key the framework itself reads from incoming
 * baggage: {@link #CAUSATION_ID_BAGGAGE_KEY}, read by {@code
 * VirtualThreadCommandBus.causationIdFrom} / {@code InMemoryCommandBus} to populate {@link
 * EventMetadata#causationId()}. It is duplicated here as a literal (not referenced from {@code
 * streamrune-runtime}) because {@code streamrune-runtime} depends on {@code streamrune-core}, not
 * the other way around — the same duplication already exists between {@code
 * VirtualThreadCommandBus} and {@code InMemoryCommandBus}.
 *
 * <p>{@link #ROLE_BAGGAGE_KEY} is deliberately <em>not</em> a default key. The request context's
 * role is set by each framework filter's role rule ({@code RequestIdentityPolicy#applyRole} in
 * {@code streamrune-integration-api}): the {@code X-User-Role} header, read only in the
 * trusted-gateway identity mode, and nothing else — a client's W3C {@code baggage: role=} entry
 * never reaches it, whatever the allowlist says.
 *
 * <p>Applications that rely on additional baggage keys reaching event metadata configure them via
 * each framework's {@code streamrune.metadata.baggage-allowlist} property (a list of extra
 * permitted keys, added on top of the default set — not a replacement for it). Listing {@code role}
 * there has no effect: the request filters' role rule replaces whatever the baggage carried under
 * it.
 */
public final class BaggageAllowlist {

  /**
   * Baggage key naming the id of the message (typically an event) that caused the current command.
   * Must stay equal to {@code VirtualThreadCommandBus.CAUSATION_ID_BAGGAGE_KEY} / {@code
   * InMemoryCommandBus.CAUSATION_ID_BAGGAGE_KEY}.
   */
  public static final String CAUSATION_ID_BAGGAGE_KEY = "streamrune.causation-id";

  /**
   * Baggage key under which each framework filter places the {@code X-User-Role} header — in the
   * trusted-gateway identity mode only, and never from incoming baggage (see the class comment).
   */
  public static final String ROLE_BAGGAGE_KEY = "role";

  /**
   * The framework-meaningful baggage keys preserved regardless of configuration: {@link
   * #CAUSATION_ID_BAGGAGE_KEY}. {@link #ROLE_BAGGAGE_KEY} is not one of them (see the class
   * comment).
   */
  public static final Set<String> DEFAULT_ALLOWED_KEYS = Set.of(CAUSATION_ID_BAGGAGE_KEY);

  private BaggageAllowlist() {}

  /**
   * Maximum length of a retained baggage value. A value longer than this is DROPPED, not truncated.
   *
   * <p>Framework-meaningful values are ids and short labels: a causation id is a UUID (36 chars), a
   * role is a word. 256 is far above any legitimate value and far below the ~8 KB an HTTP header
   * permits. Truncation was rejected deliberately — the first 256 characters of a smuggled medical
   * note are still the note, and silently shortening an application's legitimate value would
   * corrupt it rather than reject it.
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
   * @param extraAllowedKeys additional keys to permit on top of {@link #DEFAULT_ALLOWED_KEYS},
   *     typically sourced from the {@code streamrune.metadata.baggage-allowlist} configuration
   *     property; {@code null} is treated as empty
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
   * <p>{@link #filter(Map, Set)} used to be key-scoped only: it dropped non-allowlisted keys but
   * copied allowlisted entries' values <em>verbatim</em>. Since {@link #ROLE_BAGGAGE_KEY} was then
   * allowlisted by default, an unauthenticated caller could send {@code baggage: role=<a victim's
   * national id / free-text medical note / 8 KB of anything>} — or the {@code X-User-Role} header,
   * which each framework filter adds AFTER calling {@code filter} (now only in the trusted-gateway
   * mode) — and that value was written into the append-only, plaintext {@code
   * event_stream.metadata} column of every event the command produced. That column is outside
   * crypto-shredding's reach and the framework offers no API to rewrite or delete it, so a GDPR
   * Article-17 erasure cannot remove the injected data. The class javadoc's promise ("any caller
   * can smuggle arbitrary data — including PII — permanently into the immutable event log") was
   * only half kept.
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
