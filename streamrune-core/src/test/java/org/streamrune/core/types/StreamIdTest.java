package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The typed stream identity. A {@code StreamId} has no constructor or factory that takes a bare
 * string: the compiler is the test for that (there is nothing to call).
 */
class StreamIdTest {

  private static final AggregateType PRODUCT = AggregateType.of("product");
  private static final AggregateType INVENTORY = AggregateType.of("inventory");

  // Lenient-door alphabet: no ':' and no whitespace (so a generated value is never blank).
  private static final String TYPE_CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-./|#é";
  private static final String SYNTAX_FIRST = "abcdefghijklmnopqrstuvwxyz";
  private static final String SYNTAX_REST = "abcdefghijklmnopqrstuvwxyz0123456789_";
  // Ids may contain the separator anywhere.
  private static final String ID_CHARS = TYPE_CHARS + ":::";

  @Test
  void value_isTypeColonId() {
    assertEquals("product:p-1", StreamId.of(PRODUCT, AggregateId.of("p-1")).value());
    assertEquals("product:p-1", StreamId.of(PRODUCT, AggregateId.of("p-1")).toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"a:b", ":x", "x:", "a::b", "order:o-1"})
  void parse_invertsValue_evenWhenTheIdContainsTheSeparator(String id) {
    var pair = StreamId.of(PRODUCT, new AggregateId(id));
    var decoded = StreamId.parse(pair.value());
    assertEquals(pair, decoded);
    assertEquals(PRODUCT, decoded.aggregateType());
    assertEquals(id, decoded.aggregateId().value());
  }

  @Test
  void decodingIsExact_forEveryPairTheConstructorsAdmit() {
    var random = new Random(20261003L);
    for (int n = 0; n < 10_000; n++) {
      String type = randomString(random, TYPE_CHARS, 1, AggregateType.MAX_LENGTH);
      String id = randomString(random, ID_CHARS, 1, 64);
      var pair = StreamId.of(new AggregateType(type), new AggregateId(id));
      assertEquals(pair, StreamId.parse(pair.value()), () -> "lenient type=" + type + " id=" + id);
    }
    for (int n = 0; n < 10_000; n++) {
      String type =
          randomString(random, SYNTAX_FIRST, 1, 1)
              + randomString(random, SYNTAX_REST, 0, AggregateType.MAX_LENGTH - 1);
      String id = randomString(random, ID_CHARS, 1, 64);
      var pair = StreamId.of(AggregateType.of(type), new AggregateId(id));
      assertEquals(pair, StreamId.parse(pair.value()), () -> "syntax type=" + type + " id=" + id);
    }
  }

  @Test
  void aTypeContainingTheSeparatorCanNeverBeBuilt_soNoUndecodablePairExists() {
    var random = new Random(20261003L);
    for (int n = 0; n < 10_000; n++) {
      String base = randomString(random, TYPE_CHARS, 0, AggregateType.MAX_LENGTH - 1);
      int at = random.nextInt(base.length() + 1);
      String withSeparator = base.substring(0, at) + ':' + base.substring(at);
      assertThrows(
          IllegalArgumentException.class,
          () -> new AggregateType(withSeparator),
          () -> "type=" + withSeparator);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "order", "order:", ":x"})
  void parse_refusesAnythingThatIsNotTypeColonId(String stored) {
    assertThrows(IllegalArgumentException.class, () -> StreamId.parse(stored));
  }

  @Test
  void parse_refusesNull() {
    assertThrows(IllegalArgumentException.class, () -> StreamId.parse(null));
  }

  @Test
  void bothPartsAreRequired() {
    assertThrows(NullPointerException.class, () -> StreamId.of(null, AggregateId.of("x")));
    assertThrows(NullPointerException.class, () -> StreamId.of(PRODUCT, null));
  }

  @Test
  void theIdIsBoundedByItsColumn_andTheMessageDoesNotEchoIt() {
    String max = "x".repeat(255);
    assertEquals(255, StreamId.of(PRODUCT, new AggregateId(max)).aggregateId().value().length());
    String tooLong = "y".repeat(256);
    var ex =
        assertThrows(
            IllegalArgumentException.class, () -> StreamId.of(PRODUCT, new AggregateId(tooLong)));
    assertTrue(ex.getMessage().contains("256"), ex.getMessage());
    assertTrue(ex.getMessage().contains("255"), ex.getMessage());
    assertFalse(ex.getMessage().contains(tooLong), "the untrusted id is never echoed");
  }

  @Test
  void theIdBoundCountsCodePoints_asTheColumnDoes() {
    // 255 supplementary-plane characters: 510 UTF-16 code units, 255 characters in a
    // VARCHAR(255). A stored row like this fits its column, so the decode door must accept it.
    String grin = new String(Character.toChars(0x1F600));
    assertEquals(
        255,
        StreamId.of(PRODUCT, new AggregateId(grin.repeat(255)))
            .aggregateId()
            .value()
            .codePointCount(0, 510));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> StreamId.of(PRODUCT, new AggregateId(grin.repeat(256))));
    assertTrue(ex.getMessage().contains("256"), ex.getMessage());
  }

  @Test
  void theLongestTextFormIs288Characters() {
    var longest = StreamId.of(new AggregateType("t".repeat(32)), new AggregateId("i".repeat(255)));
    assertEquals(288, longest.value().length());
    assertEquals(longest, StreamId.parse(longest.value()));
  }

  @Test
  void equalityDistinguishesTypesSharingAnIdValue() {
    var product = StreamId.of(PRODUCT, AggregateId.of("p-1"));
    var inventory = StreamId.of(INVENTORY, AggregateId.of("p-1"));
    assertNotEquals(product, inventory);
    assertEquals(product, StreamId.of(AggregateType.of("product"), AggregateId.of("p-1")));
  }

  @Test
  void jackson_roundTripsAsAPlainString() throws Exception {
    var mapper = new ObjectMapper();
    var id = StreamId.of(PRODUCT, AggregateId.of("a:b"));
    assertEquals("\"product:a:b\"", mapper.writeValueAsString(id));
    assertEquals(id, mapper.readValue("\"product:a:b\"", StreamId.class));
    assertThrows(JsonMappingException.class, () -> mapper.readValue("\"product\"", StreamId.class));
  }

  private static String randomString(Random random, String alphabet, int minLen, int maxLen) {
    int len = minLen + random.nextInt(maxLen - minLen + 1);
    var sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
    }
    return sb.toString();
  }
}
