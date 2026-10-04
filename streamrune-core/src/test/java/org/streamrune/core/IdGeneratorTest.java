package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import org.junit.jupiter.api.Test;

class IdGeneratorTest {

  @Test
  void eventIdHasPrefixAndFixedLength() {
    var id = IdGenerator.generateEventId().value();
    assertTrue(id.startsWith("evt_"), "expected evt_ prefix: " + id);
    // "evt_" + 9-char base-36 timestamp + 16-char random suffix
    assertEquals(4 + 9 + 16, id.length());
  }

  @Test
  void commandIdHasPrefixAndFixedLength() {
    var id = IdGenerator.generateCommandId().value();
    assertTrue(id.startsWith("cmd_"), "expected cmd_ prefix: " + id);
    assertEquals(4 + 9 + 16, id.length());
  }

  @Test
  void correlationIdHasPrefix() {
    assertTrue(IdGenerator.generateCorrelationId().value().startsWith("corr_"));
  }

  @Test
  void generatedIdsAreUnique() {
    var seen = new HashSet<String>();
    for (int i = 0; i < 1_000; i++) {
      assertTrue(seen.add(IdGenerator.generateEventId().value()));
    }
  }

  @Test
  void encodedTimestampsAreFixedWidth() {
    assertEquals("000000000", IdGenerator.encodeTimestamp(0));
    assertEquals(9, IdGenerator.encodeTimestamp(1).length());
    assertEquals(9, IdGenerator.encodeTimestamp(101_559_956_668_415L).length());
    assertEquals("zzzzzzzzz", IdGenerator.encodeTimestamp(101_559_956_668_415L));
  }

  @Test
  void encodedTimestampsSortLexicographicallyAcrossDigitRollover() {
    // 36^7 ms after the custom epoch (~2026-06-26) is where the unpadded encoding
    // used to gain a digit and invert the sort order.
    long rollover = 78_364_164_096L; // 36^7
    long[] samples = {0, 1, 35, 36, rollover - 1, rollover, rollover + 1, rollover * 36};
    for (int i = 1; i < samples.length; i++) {
      String earlier = IdGenerator.encodeTimestamp(samples[i - 1]);
      String later = IdGenerator.encodeTimestamp(samples[i]);
      assertTrue(
          earlier.compareTo(later) < 0,
          "expected " + earlier + " < " + later + " for ts " + samples[i - 1] + " < " + samples[i]);
    }
  }

  @Test
  void encodedTimestampsSortForConsecutiveMillis() {
    long now = System.currentTimeMillis() - 1_704_067_200_000L; // ms since 2024-01-01
    String first = IdGenerator.encodeTimestamp(now);
    String next = IdGenerator.encodeTimestamp(now + 1);
    assertTrue(first.compareTo(next) < 0);
  }

  @Test
  void encodeTimestampRejectsOutOfRangeValues() {
    assertThrows(IllegalArgumentException.class, () -> IdGenerator.encodeTimestamp(-1));
    assertThrows(
        IllegalArgumentException.class, () -> IdGenerator.encodeTimestamp(101_559_956_668_416L));
  }
}
