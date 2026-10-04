package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AggregateTypeTest {

  // A compile-time constant (it feeds @ValueSource): "a" followed by 31 "b".
  private static final String THIRTY_TWO = "abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @ParameterizedTest
  @ValueSource(strings = {"order", "order_line", "a", "inventory2", THIRTY_TWO})
  void of_acceptsTheSyntax(String value) {
    assertEquals(value, AggregateType.of(value).value());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"Order", "order-line", "1order", "_order", "order:x", "ord er", "ord\ner"})
  void of_refusesEverythingOutsideTheSyntax(String value) {
    var ex = assertThrows(IllegalArgumentException.class, () -> AggregateType.of(value));
    assertFalse(ex.getMessage().contains(value), "the refused value is never echoed");
  }

  @Test
  void of_refusesBlankAndNullAndTooLong() {
    assertThrows(IllegalArgumentException.class, () -> AggregateType.of(null));
    assertThrows(IllegalArgumentException.class, () -> AggregateType.of(" "));
    var tooLong =
        assertThrows(IllegalArgumentException.class, () -> AggregateType.of(THIRTY_TWO + "c"));
    assertTrue(tooLong.getMessage().contains("33"), tooLong.getMessage());
    assertTrue(tooLong.getMessage().contains("32"), tooLong.getMessage());
  }

  @Test
  void canonicalConstructor_isLenientAboutTheAlphabet_butKeepsTheStructuralRules() {
    // Decode door: a stored type outside the syntax must stay readable.
    assertEquals("Order", new AggregateType("Order").value());
    assertEquals("order-line", new AggregateType("order-line").value());
    // ...but never a separator (it would make the text form undecodable) and never more than the
    // aggregate_type column holds.
    var separator = assertThrows(IllegalArgumentException.class, () -> new AggregateType("a:b"));
    assertTrue(separator.getMessage().contains("':'"), separator.getMessage());
    var length =
        assertThrows(IllegalArgumentException.class, () -> new AggregateType(THIRTY_TWO + "c"));
    assertTrue(length.getMessage().contains("at most 32"), length.getMessage());
    assertThrows(IllegalArgumentException.class, () -> new AggregateType(""));
  }

  @Test
  void jackson_roundTripsAsAPlainString_andRefusesASeparator() throws Exception {
    var mapper = new ObjectMapper();
    assertEquals("\"order\"", mapper.writeValueAsString(AggregateType.of("order")));
    assertEquals(AggregateType.of("order"), mapper.readValue("\"order\"", AggregateType.class));
    assertThrows(
        JsonMappingException.class, () -> mapper.readValue("\"a:b\"", AggregateType.class));
  }

  @Test
  void theLongestAcceptedFixture_isExactlyTheColumnWidth() {
    assertEquals(AggregateType.MAX_LENGTH, THIRTY_TWO.length());
  }

  @Test
  void toString_isTheValue() {
    assertEquals("order", AggregateType.of("order").toString());
  }
}
