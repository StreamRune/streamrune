package org.streamrune.runtime.health;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Pins every relay component name as the literal an operator's alert matches: a change to one of
 * them is a change to the health contract, and fails here first.
 */
class RelayComponentNamesTest {

  @Test
  void theNamesAreTheDocumentedLiterals() {
    assertEquals("outbox-relay", RelayComponentNames.OUTBOX_RELAY);
    assertEquals("dead-letter-retry", RelayComponentNames.DEAD_LETTER_RETRY);
    assertEquals(
        "saga-compensation-retry:com.shop.OrderSagaState",
        RelayComponentNames.sagaCompensationRetry("com.shop.OrderSagaState"));
    assertEquals(
        "saga-timeout:com.shop.OrderSagaState",
        RelayComponentNames.sagaTimeout("com.shop.OrderSagaState"));
    assertEquals("inbox-retention-sweeper", RelayComponentNames.INBOX_RETENTION_SWEEPER);
    assertEquals(
        "dead-letter-retention-sweeper", RelayComponentNames.DEAD_LETTER_RETENTION_SWEEPER);
    assertEquals("outbox-retention-sweeper", RelayComponentNames.OUTBOX_RETENTION_SWEEPER);
    assertEquals(
        "saga-dead-letter-retention-sweeper",
        RelayComponentNames.SAGA_DEAD_LETTER_RETENTION_SWEEPER);
    assertEquals("crypto-forget-listen", RelayComponentNames.CRYPTO_FORGET_LISTEN);
    assertEquals("sse-event-feed", RelayComponentNames.SSE_EVENT_FEED);
  }

  @Test
  void theParameterisedNamesAreBuiltFromTheirPrefixes() {
    assertEquals("saga-compensation-retry:", RelayComponentNames.SAGA_COMPENSATION_RETRY_PREFIX);
    assertEquals("saga-timeout:", RelayComponentNames.SAGA_TIMEOUT_PREFIX);
    assertEquals(":sampling-only", RelayComponentNames.SAMPLING_ONLY_SUFFIX);
  }
}
