package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests for {@link BaggageAllowlist}. */
class BaggageAllowlistTest {

  private static final String CAUSATION = BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY;

  @Test
  void defaultAllowlistIsTheCausationIdOnly() {
    // `role` is not a default key. The request context's role is set by the request
    // filters' role rule (the X-User-Role header, behind a trusted gateway only), never by a
    // client's W3C `baggage: role=` entry.
    assertEquals(Set.of(CAUSATION), BaggageAllowlist.DEFAULT_ALLOWED_KEYS);
    assertEquals("role", BaggageAllowlist.ROLE_BAGGAGE_KEY);
  }

  @Test
  void causationIdKeyMatchesRuntimeConstant() {
    // Must stay in lockstep with VirtualThreadCommandBus.CAUSATION_ID_BAGGAGE_KEY /
    // InMemoryCommandBus.CAUSATION_ID_BAGGAGE_KEY, which streamrune-core cannot depend on.
    assertEquals("streamrune.causation-id", BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY);
  }

  @Test
  void filtersOutNonAllowlistedKeysByDefault() {
    var raw =
        Map.of(
            BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY,
            "x",
            "role",
            "admin",
            "email",
            "pii@example.com",
            "tracking",
            "leak");

    var filtered = BaggageAllowlist.filter(raw, Set.of());

    assertEquals(Map.of(CAUSATION, "x"), filtered);
    assertFalse(filtered.containsKey("role"), "a client's W3C role entry is not a default key");
    assertFalse(filtered.containsKey("email"));
    assertFalse(filtered.containsKey("tracking"));
  }

  @Test
  void additionalAllowedKeysAreKept() {
    var raw = Map.of(CAUSATION, "admin", "email", "pii@example.com", "tracking", "leak");

    var filtered = BaggageAllowlist.filter(raw, Set.of("tracking"));

    assertEquals("admin", filtered.get(CAUSATION));
    assertEquals("leak", filtered.get("tracking"));
    assertFalse(filtered.containsKey("email"));
  }

  @Test
  void emptyRawBaggageReturnsEmptyMap() {
    assertTrue(BaggageAllowlist.filter(Map.of(), Set.of("tracking")).isEmpty());
  }

  @Test
  void nullExtraAllowedKeysTreatedAsEmpty() {
    var raw = Map.of(CAUSATION, "admin", "email", "leak");
    var filtered = BaggageAllowlist.filter(raw, null);
    assertEquals(Map.of(CAUSATION, "admin"), filtered);
  }

  @Test
  void resultIsImmutable() {
    var filtered = BaggageAllowlist.filter(Map.of(CAUSATION, "admin"), Set.of());
    assertThrows(UnsupportedOperationException.class, () -> filtered.put("x", "y"));
  }

  // ==== The allowlist is key-scoped; values need a policy too ====

  @Test
  void anOversizedValueUnderAnAllowlistedKeyIsDropped() {
    // A default-allowlisted key (`role` used to be one) let an unauthenticated caller send
    // `baggage: role=<8 KB of a victim's medical notes>` and have its payload copied verbatim into
    // the append-only, plaintext event_stream.metadata of every event the request produced —
    // outside crypto-shredding's reach and with no API to rewrite or delete it. Article-17 erasure
    // cannot remove it. The causation id is the default key now; the policy is the same.
    var smuggled = "x".repeat(BaggageAllowlist.MAX_VALUE_LENGTH + 1);

    var filtered = BaggageAllowlist.filter(Map.of(CAUSATION, smuggled), Set.of());

    assertFalse(
        filtered.containsKey(CAUSATION),
        "an over-cap value is dropped, not truncated — a truncated PII string is still PII");
  }

  @Test
  void aValueExactlyAtTheCapIsKept() {
    var atCap = "r".repeat(BaggageAllowlist.MAX_VALUE_LENGTH);
    assertEquals(atCap, BaggageAllowlist.filter(Map.of(CAUSATION, atCap), Set.of()).get(CAUSATION));
  }

  @Test
  void controlCharactersAreStrippedFromRetainedValues() {
    // A NUL breaks the jsonb write outright; CR/LF forge log lines and JSON payload boundaries.
    var filtered = BaggageAllowlist.filter(Map.of(CAUSATION, "admin\r\nX-Injected: 1"), Set.of());

    assertEquals("adminX-Injected: 1", filtered.get(CAUSATION));
    assertTrue(
        filtered.get(CAUSATION).chars().noneMatch(Character::isISOControl),
        "no control character may reach event metadata");
  }

  @Test
  void aValueThatIsOnlyControlCharactersIsDropped() {
    assertFalse(
        BaggageAllowlist.filter(Map.of(CAUSATION, "\r\n\t"), Set.of()).containsKey(CAUSATION));
  }

  @Test
  void theValuePolicyAppliesToConfiguredExtraKeysToo() {
    var raw =
        Map.of("tracking", "y".repeat(BaggageAllowlist.MAX_VALUE_LENGTH + 1), CAUSATION, "admin");

    var filtered = BaggageAllowlist.filter(raw, Set.of("tracking"));

    assertFalse(
        filtered.containsKey("tracking"),
        "opting a key into the allowlist opts it into the value policy, not out of it");
    assertEquals("admin", filtered.get(CAUSATION));
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
