package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests for {@link BaggageAllowlist}. */
class BaggageAllowlistTest {

  /** A key an application allow-lists; the framework gives it no meaning. */
  private static final String TENANT = "tenant";

  /**
   * A baggage key by which a client claims to name the event that caused its command. It is an
   * ordinary key: not allow-listed by default, and read by nothing when an application allows it.
   */
  private static final String CAUSATION = "streamrune.causation-id";

  @Test
  void theDefaultAllowListIsEmpty() {
    // `role` is not a default key either. The request context's role is set by the request
    // filters' role rule (the X-User-Role header, behind a trusted gateway only), never by a
    // client's W3C `baggage: role=` entry.
    assertEquals(Set.of(), BaggageAllowlist.DEFAULT_ALLOWED_KEYS);
    assertEquals("role", BaggageAllowlist.ROLE_BAGGAGE_KEY);
  }

  @Test
  void withNoKeyAllowedEveryEntryIsDropped() {
    var raw =
        Map.of(CAUSATION, "evt-1", "role", "admin", "email", "pii@example.com", "tracking", "leak");

    assertEquals(Map.of(), BaggageAllowlist.filter(raw, Set.of()));
    assertEquals(Map.of(), BaggageAllowlist.filter(raw, null));
  }

  @Test
  void aCausationEntryIsDroppedUnlessTheApplicationAllowsIt() {
    var raw = Map.of(CAUSATION, "evt-1", TENANT, "acme");

    assertFalse(BaggageAllowlist.filter(raw, Set.of()).containsKey(CAUSATION));
    assertFalse(BaggageAllowlist.filter(raw, Set.of(TENANT)).containsKey(CAUSATION));
    assertEquals("evt-1", BaggageAllowlist.filter(raw, Set.of(CAUSATION)).get(CAUSATION));
  }

  @Test
  void anAllowedCausationEntryIsOrdinaryBaggage() {
    // Allowed, it is kept like any other key and goes through the same value policy; nothing in
    // the request context treats it as a causation id.
    var allowed = Set.of(CAUSATION, TENANT);

    var filtered = BaggageAllowlist.filter(Map.of(CAUSATION, "evt\r\n-1", TENANT, "acme"), allowed);
    assertEquals(Map.of(CAUSATION, "evt-1", TENANT, "acme"), filtered);

    var oversized = "x".repeat(BaggageAllowlist.MAX_VALUE_LENGTH + 1);
    assertFalse(
        BaggageAllowlist.filter(Map.of(CAUSATION, oversized), allowed).containsKey(CAUSATION));

    var context =
        new StreamRuneContext.RequestContext(
            null,
            null,
            org.streamrune.core.types.CorrelationId.of("corr-1"),
            java.time.Instant.now(),
            filtered);
    assertEquals("evt-1", context.baggage().get(CAUSATION));
    ScopedValue.where(StreamRuneContext.CURRENT, context)
        .run(() -> assertNull(StreamRuneContext.cause(), "baggage binds no dispatch cause"));
  }

  @Test
  void additionalAllowedKeysAreKept() {
    var raw = Map.of(TENANT, "acme", "email", "pii@example.com", "tracking", "leak");

    var filtered = BaggageAllowlist.filter(raw, Set.of(TENANT, "tracking"));

    assertEquals("acme", filtered.get(TENANT));
    assertEquals("leak", filtered.get("tracking"));
    assertFalse(filtered.containsKey("email"));
  }

  @Test
  void emptyRawBaggageReturnsEmptyMap() {
    assertTrue(BaggageAllowlist.filter(Map.of(), Set.of("tracking")).isEmpty());
    assertTrue(BaggageAllowlist.filter(null, Set.of("tracking")).isEmpty());
  }

  @Test
  void nullExtraAllowedKeysTreatedAsEmpty() {
    var raw = Map.of(TENANT, "acme", "email", "leak");
    assertEquals(Map.of(), BaggageAllowlist.filter(raw, null));
  }

  @Test
  void resultIsImmutable() {
    var filtered = BaggageAllowlist.filter(Map.of(TENANT, "acme"), Set.of(TENANT));
    assertThrows(UnsupportedOperationException.class, () -> filtered.put("x", "y"));
  }

  // ==== The allowlist is key-scoped; values need a policy too ====

  @Test
  void anOversizedValueUnderAnAllowlistedKeyIsDropped() {
    // An allow-listed key lets an unauthenticated caller send
    // `baggage: tenant=<8 KB of a victim's medical notes>` and, without a value policy, have the
    // payload copied verbatim into the append-only, plaintext event_stream.metadata of every event
    // the request produced — outside crypto-shredding's reach and with no API to rewrite or delete
    // it. Article-17 erasure cannot remove it.
    var smuggled = "x".repeat(BaggageAllowlist.MAX_VALUE_LENGTH + 1);

    var filtered = BaggageAllowlist.filter(Map.of(TENANT, smuggled), Set.of(TENANT));

    assertFalse(
        filtered.containsKey(TENANT),
        "an over-cap value is dropped, not truncated — a truncated PII string is still PII");
  }

  @Test
  void aValueExactlyAtTheCapIsKept() {
    var atCap = "r".repeat(BaggageAllowlist.MAX_VALUE_LENGTH);
    assertEquals(atCap, BaggageAllowlist.filter(Map.of(TENANT, atCap), Set.of(TENANT)).get(TENANT));
  }

  @Test
  void controlCharactersAreStrippedFromRetainedValues() {
    // A NUL breaks the jsonb write outright; CR/LF forge log lines and JSON payload boundaries.
    var filtered =
        BaggageAllowlist.filter(Map.of(TENANT, "admin\r\nX-Injected: 1"), Set.of(TENANT));

    assertEquals("adminX-Injected: 1", filtered.get(TENANT));
    assertTrue(
        filtered.get(TENANT).chars().noneMatch(Character::isISOControl),
        "no control character may reach event metadata");
  }

  @Test
  void aValueThatIsOnlyControlCharactersIsDropped() {
    assertFalse(
        BaggageAllowlist.filter(Map.of(TENANT, "\r\n\t"), Set.of(TENANT)).containsKey(TENANT));
  }

  @Test
  void theValuePolicyAppliesToEveryConfiguredKey() {
    var raw = Map.of("tracking", "y".repeat(BaggageAllowlist.MAX_VALUE_LENGTH + 1), TENANT, "acme");

    var filtered = BaggageAllowlist.filter(raw, Set.of("tracking", TENANT));

    assertFalse(
        filtered.containsKey("tracking"),
        "opting a key into the allowlist opts it into the value policy, not out of it");
    assertEquals("acme", filtered.get(TENANT));
  }

  @Test
  void sanitizeAppliesTheValuePolicyWithoutTheKeyAllowlist() {
    // The shared entry point for callers that add entries AFTER filter() has run — the framework
    // request filters add the X-User-Role header (behind a trusted gateway) at that point, past
    // the key allowlist. RequestContext applies this to whatever it is handed, so the value policy
    // holds at the boundary rather than in three separate request filters.
    var sanitized =
        BaggageAllowlist.sanitize(
            Map.of(
                "role",
                "adm\r\nin",
                "anything",
                "kept",
                "huge",
                "z".repeat(BaggageAllowlist.MAX_VALUE_LENGTH + 1)));

    assertEquals("admin", sanitized.get("role"));
    assertEquals("kept", sanitized.get("anything"), "sanitize() enforces values, not keys");
    assertFalse(sanitized.containsKey("huge"));
  }
}
