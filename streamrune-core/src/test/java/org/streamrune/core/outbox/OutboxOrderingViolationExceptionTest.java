package org.streamrune.core.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OutboxOrderingViolationExceptionTest {

  @Test
  void message_namesEntryAndPayloadType_sanitized_neverThePayload() {
    // The id is user-derived (aggregate-seeded) and the message is rendered by the caller's logger,
    // so it is sanitized at construction; the payload is never part of the message (PII rule).
    var ex =
        new OutboxOrderingViolationException(
            OutboxEntryId.of("evt-1\nFAKE LINE"), "OrderConfirmed\r\n");
    assertThat(ex).isInstanceOf(IllegalArgumentException.class);
    assertThat(ex.getMessage())
        .contains("STRICT_PER_AGGREGATE")
        .contains("has no streamId")
        .contains("OrderConfirmed")
        .doesNotContain("\n")
        .doesNotContain("\r");
    assertThat(ex.entryId()).isEqualTo(OutboxEntryId.of("evt-1\nFAKE LINE"));
    assertThat(ex.payloadType()).isEqualTo("OrderConfirmed\r\n");
  }

  @Test
  void constructor_rejectsNulls() {
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class, () -> new OutboxOrderingViolationException(null, "T"));
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class,
        () -> new OutboxOrderingViolationException(OutboxEntryId.of("x"), null));
  }
}
