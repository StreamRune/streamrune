package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class IdConstraintsTest {

  @Test
  void theReservedPrefixIsStreamruneColon() {
    assertEquals("streamrune:", IdConstraints.RESERVED_PREFIX);
  }

  @Test
  void isReserved_matchesThePrefixExactly() {
    assertTrue(IdConstraints.isReserved("streamrune:x"));
    assertTrue(IdConstraints.isReserved("streamrune:"), "the prefix alone is reserved too");
    assertTrue(IdConstraints.isReserved("streamrune:saga:order-saga-1"));

    assertFalse(IdConstraints.isReserved("Streamrune:x"), "the check is case-sensitive");
    assertFalse(IdConstraints.isReserved("STREAMRUNE:x"), "the check is case-sensitive");
    assertFalse(IdConstraints.isReserved("streamrune"), "the colon is part of the prefix");
    assertFalse(IdConstraints.isReserved(" streamrune:x"), "nothing is trimmed");
    assertFalse(IdConstraints.isReserved("x-streamrune:x"), "a prefix, not a substring");
    assertFalse(IdConstraints.isReserved(""));
    assertFalse(IdConstraints.isReserved(null));
    assertFalse(IdConstraints.isReserved("order-saga-1"));
    assertFalse(IdConstraints.isReserved("saga:order-saga-1"));
  }
}
