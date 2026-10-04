package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class AggregateIdTest {

  @Test
  void constructsWithValue() {
    var id = new AggregateId("cart-1");
    assertEquals("cart-1", id.value());
  }

  @Test
  void ofIsAlias() {
    assertEquals(new AggregateId("cart-1"), AggregateId.of("cart-1"));
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> new AggregateId(null));
  }

  @Test
  void rejectsBlank() {
    assertThrows(IllegalArgumentException.class, () -> new AggregateId("  "));
    assertThrows(IllegalArgumentException.class, () -> new AggregateId(""));
  }

  @Test
  void serializesAsPlainStringAndRoundTrips() throws Exception {
    var mapper = new ObjectMapper();
    var id = AggregateId.of("cart-1");
    assertEquals("cart-1", id.toString());
    assertEquals("\"cart-1\"", mapper.writeValueAsString(id));
    assertEquals(id, mapper.readValue("\"cart-1\"", AggregateId.class));
  }

  // === Control characters refused at the ingress door ===

  /**
   * One representative per refused range and both edges of each: C0 (U+0000-U+001F, including the
   * CR/LF/TAB an audit row or a log line is forged with and the NUL that fails a PostgreSQL text
   * write), DEL (U+007F) and C1 (U+0080-U+009F, including U+0085 NEXT LINE). Exactly {@link
   * Character#isISOControl(char)}.
   */
  private static final char[] REFUSED = {
    '\u0000', '\u0001', '\t', '\n', '\r', '\u001B', '\u001F', '\u007F', '\u0080', '\u0085', '\u009F'
  };

  /** The printable neighbours of each refused range stay accepted. */
  private static final char[] NEIGHBOURS = {' ', '~', '\u00A0', '\u00FF'};

  @Test
  void of_controlCharacter_throws_withoutEchoingTheValue() {
    for (char c : REFUSED) {
      String value = "order-" + c + "-SECRET";
      var ex = assertThrows(IllegalArgumentException.class, () -> AggregateId.of(value), hex(c));
      assertTrue(ex.getMessage().startsWith("aggregateId "), hex(c) + ": " + ex.getMessage());
      assertTrue(ex.getMessage().contains("control characters"), hex(c) + ": " + ex.getMessage());
      // The value is untrusted input: the message would copy it into a log line or a 400 body.
      assertFalse(ex.getMessage().contains("SECRET"), hex(c) + ": " + ex.getMessage());
    }
  }

  @Test
  void of_printableNeighboursOfTheRefusedRanges_areAccepted() {
    for (char c : NEIGHBOURS) {
      String value = "order" + c + "1";
      assertEquals(value, AggregateId.of(value).value(), hex(c));
    }
  }

  @Test
  void of_blank_isStillRefused() {
    assertThrows(IllegalArgumentException.class, () -> AggregateId.of(null));
    assertThrows(IllegalArgumentException.class, () -> AggregateId.of(" "));
  }

  /**
   * The canonical constructor is the DECODE door, not an ingress door: Jackson rebuilds an id
   * through it (a dead-lettered command's payload, an event or saga state carrying one), and the
   * dead-letter, command-audit and outbox row mappers rebuild a stored id through it. A charset
   * rule there would stop no write (the value is already inside) and would make that row
   * unreadable, the failure {@link IdConstraints} records, so it keeps only the blank rule.
   */
  @Test
  void canonicalConstructorAndJackson_stayLenient_forTheDecodeDoor() throws Exception {
    assertEquals("order\r\n-1", new AggregateId("order\r\n-1").value());
    assertEquals(
        "order\u0085-1",
        new ObjectMapper().readValue("\"order\\u0085-1\"", AggregateId.class).value());
  }

  // === Length bound at the ingress door: the VARCHAR(255) stream columns ===

  /** U+1F600: one code point, two UTF-16 code units (a surrogate pair). */
  private static final String SUPPLEMENTARY = "😀";

  @Test
  void of_atTheLengthBound_isAccepted() {
    String value = "a".repeat(IdConstraints.MAX_LENGTH);
    assertEquals(value, AggregateId.of(value).value());
  }

  /**
   * One code unit over the bound is refused at the door, before the id can reach the {@code
   * aggregate_id} columns of {@code event_stream} or {@code dead_letter_queue}, where the
   * over-length write would fail with an SQL error instead of a client error.
   */
  @Test
  void of_overTheLengthBound_throws_withoutEchoingTheValue() {
    String value = "SECRET" + "a".repeat(IdConstraints.MAX_LENGTH - 5);
    assertEquals(IdConstraints.MAX_LENGTH + 1, value.length());

    var ex = assertThrows(IllegalArgumentException.class, () -> AggregateId.of(value));

    assertEquals("aggregateId must be at most 255 characters, got 256", ex.getMessage());
  }

  /**
   * The bound counts UTF-16 code units ({@link String#length()}), and PostgreSQL's {@code
   * VARCHAR(255)} counts characters (code points). A code point is one or two code units, so a
   * value within the bound is never more than 255 characters: the check can refuse an id the column
   * would hold (here, 128 supplementary-plane characters), but never accepts one it would not.
   */
  @Test
  void of_supplementaryPlaneCharacters_countAsTwoCodeUnitsAgainstTheBound() {
    String atBound = SUPPLEMENTARY.repeat(127) + "a";
    assertEquals(IdConstraints.MAX_LENGTH, atBound.length());
    assertEquals(128, atBound.codePointCount(0, atBound.length()));
    assertEquals(atBound, AggregateId.of(atBound).value());

    String overBound = SUPPLEMENTARY.repeat(128);
    assertEquals(128, overBound.codePointCount(0, overBound.length()));
    var ex = assertThrows(IllegalArgumentException.class, () -> AggregateId.of(overBound));
    assertEquals("aggregateId must be at most 255 characters, got 256", ex.getMessage());
  }

  @Test
  void of_overTheLengthBoundAndBlank_reportsTheBlankRuleFirst() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> AggregateId.of(" ".repeat(IdConstraints.MAX_LENGTH + 1)));
    assertEquals("aggregateId is required", ex.getMessage());
  }

  /**
   * The decode door keeps no length rule either: a stored id is rebuilt whatever its length, so a
   * row written before the bound existed, or a value an application built with the constructor,
   * stays readable.
   */
  @Test
  void canonicalConstructorAndJackson_stayLenientOnLength_forTheDecodeDoor() throws Exception {
    String overBound = "a".repeat(IdConstraints.MAX_LENGTH + 1);
    assertEquals(overBound, new AggregateId(overBound).value());
    assertEquals(
        overBound,
        new ObjectMapper().readValue("\"" + overBound + "\"", AggregateId.class).value());
  }

  private static String hex(char c) {
    return String.format("U+%04X", (int) c);
  }
}
