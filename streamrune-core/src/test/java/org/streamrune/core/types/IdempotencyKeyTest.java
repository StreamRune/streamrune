package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class IdempotencyKeyTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void of_returnsValueWithCorrectValue() {
    var key = IdempotencyKey.of("saga:order-1:42:0");
    assertEquals("saga:order-1:42:0", key.value());
  }

  @Test
  void toString_returnsValue() {
    var key = IdempotencyKey.of("saga:order-1:42:0");
    assertEquals("saga:order-1:42:0", key.toString());
  }

  @Test
  void jsonRoundTrip() throws Exception {
    var key = IdempotencyKey.of("saga:order-1:42:0");
    assertEquals("\"saga:order-1:42:0\"", mapper.writeValueAsString(key));
    assertEquals(key, mapper.readValue("\"saga:order-1:42:0\"", IdempotencyKey.class));
  }

  @Test
  void of_null_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.of(null));
  }

  @Test
  void of_empty_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.of(""));
  }

  @Test
  void of_blank_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.of("  "));
  }

  @Test
  void of_513Chars_throwsIllegalArgumentException() {
    String over512 = "x".repeat(513);
    var ex = assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.of(over512));
    assertTrue(ex.getMessage().contains("512"), ex.getMessage());
  }

  @Test
  void of_512Chars_isAccepted() {
    String exactly512 = "x".repeat(512);
    var key = IdempotencyKey.of(exactly512);
    assertEquals(exactly512, key.value());
  }

  @Test
  void compactConstructor_null_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> new IdempotencyKey(null));
  }

  // === scopedTo — caller-owned key namespacing ===

  @Test
  void scopedTo_prefixesTheScope_andIsDeterministic() {
    var key = IdempotencyKey.scopedTo("tenant-a", "order-create-12345");
    assertEquals("tenant-a|order-create-12345", key.value());
    assertEquals(key, IdempotencyKey.scopedTo("tenant-a", "order-create-12345"));
  }

  /**
   * The whole point: the same client-supplied key presented by two principals must not collide on
   * one inbox row.
   */
  @Test
  void scopedTo_sameClientKey_differentScopes_areDifferentKeys() {
    var alice = IdempotencyKey.scopedTo("alice", "12345");
    var bob = IdempotencyKey.scopedTo("bob", "12345");
    assertNotEquals(alice, bob);
  }

  /**
   * A separator-free scope is what makes the split unambiguous — otherwise {@code scopedTo("a|b",
   * "c")} and {@code scopedTo("a", "b|c")} would produce the same key and one caller could
   * impersonate another's scope through the client-controlled half.
   */
  @Test
  void scopedTo_scopeContainingSeparator_throws() {
    var ex =
        assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.scopedTo("a|b", "c"));
    assertTrue(ex.getMessage().contains("|"), ex.getMessage());
  }

  @Test
  void scopedTo_nullScope_throws() {
    assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.scopedTo(null, "c"));
  }

  @Test
  void scopedTo_blankScope_throws() {
    assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.scopedTo("  ", "c"));
  }

  @Test
  void scopedTo_nullKey_throws() {
    assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.scopedTo("tenant-a", null));
  }

  @Test
  void scopedTo_blankKey_throws() {
    assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.scopedTo("tenant-a", " "));
  }

  @Test
  void scopedTo_combinedValueOver512_throws() {
    var ex =
        assertThrows(
            IllegalArgumentException.class, () -> IdempotencyKey.scopedTo("t", "x".repeat(511)));
    assertTrue(ex.getMessage().contains("512"), ex.getMessage());
  }

  // === Control characters at the ingress doors ===

  /**
   * One representative per refused range and both edges of each: C0 (U+0000-U+001F, including the
   * CR/LF/TAB a log line or an audit row is forged with), DEL (U+007F) and C1 (U+0080-U+009F,
   * including U+0085 NEXT LINE). Exactly {@link Character#isISOControl(char)}.
   */
  private static final char[] REFUSED = {
    '\u0000', '\u0001', '\t', '\n', '\r', '\u001B', '\u001F', '\u007F', '\u0080', '\u0085', '\u009F'
  };

  /** The printable neighbours of each refused range stay accepted. */
  private static final char[] NEIGHBOURS = {' ', '~', '\u00A0', '\u00FF'};

  @Test
  void of_controlCharacter_throws_withoutEchoingTheValue() {
    for (char c : REFUSED) {
      String value = "client-token-" + c + "-SECRET";
      var ex = assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.of(value), hex(c));
      assertTrue(ex.getMessage().contains("control characters"), hex(c) + ": " + ex.getMessage());
      // The value is untrusted input: the message would copy it into a log line or an audit row.
      assertFalse(ex.getMessage().contains("SECRET"), hex(c) + ": " + ex.getMessage());
    }
  }

  @Test
  void of_printableNeighboursOfTheRefusedRanges_areAccepted() {
    for (char c : NEIGHBOURS) {
      String value = "k" + c + "k";
      assertEquals(value, IdempotencyKey.of(value).value(), hex(c));
    }
  }

  @Test
  void scopedTo_controlCharacterInTheScope_throws_namingTheScope() {
    for (char c : REFUSED) {
      String scope = "tenant" + c + "SECRET";
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> IdempotencyKey.scopedTo(scope, "order-create-1"),
              hex(c));
      assertTrue(ex.getMessage().startsWith("scope "), hex(c) + ": " + ex.getMessage());
      assertTrue(ex.getMessage().contains("control characters"), hex(c) + ": " + ex.getMessage());
      assertFalse(ex.getMessage().contains("SECRET"), hex(c) + ": " + ex.getMessage());
    }
  }

  @Test
  void scopedTo_controlCharacterInTheKey_throws_namingTheKey() {
    for (char c : REFUSED) {
      String key = "order-create-" + c + "SECRET";
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> IdempotencyKey.scopedTo("tenant-a", key),
              hex(c));
      assertTrue(ex.getMessage().startsWith("key "), hex(c) + ": " + ex.getMessage());
      assertTrue(ex.getMessage().contains("control characters"), hex(c) + ": " + ex.getMessage());
      assertFalse(ex.getMessage().contains("SECRET"), hex(c) + ": " + ex.getMessage());
    }
  }

  @Test
  void scopedTo_printableNeighbours_areAccepted() {
    for (char c : NEIGHBOURS) {
      String part = "p" + c + "p";
      assertEquals(part + "|" + part, IdempotencyKey.scopedTo(part, part).value(), hex(c));
    }
  }

  /**
   * The canonical constructor is the DECODE door, not an ingress door: Jackson rebuilds a key
   * through it, the dead-letter row mapper rebuilds a stored key through it, and the framework
   * derives saga and DLQ-replay keys from stored ids through it. A charset rule there would make an
   * already-persisted row unreadable and a stored saga id undispatchable (the IdConstraints
   * post-mortem), so it keeps only the blank and length rules.
   */
  @Test
  void canonicalConstructorAndJackson_stayLenient_forTheDecodeDoor() throws Exception {
    assertEquals("a\nb", new IdempotencyKey("a\nb").value());
    assertEquals("a\u0001b", mapper.readValue("\"a\\u0001b\"", IdempotencyKey.class).value());
  }

  private static String hex(char c) {
    return String.format("U+%04X", (int) c);
  }
}
