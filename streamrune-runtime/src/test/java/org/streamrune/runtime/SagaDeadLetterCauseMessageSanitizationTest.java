package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.GlobalOffset;

/**
 * The three cause exceptions a saga dead-letter entry records ({@link
 * SagaStaleCompensationEpisodeException}, {@link SagaEventHeldException}, {@link
 * SagaSkippedWhileFaultedException}) build their message with the saga id, and that message is
 * stored on the entry and reaches log records. A {@code SagaId} is a business id that validates
 * non-blank and a length ceiling only, so a control character passes; the messages therefore render
 * it through {@code LogSanitizer.sanitizeForLog}.
 */
class SagaDeadLetterCauseMessageSanitizationTest {

  private static final SagaId FORGING_ID = SagaId.of("order-1\r\nFORGED WARNING line");

  @Test
  void everyCauseMessage_rendersTheSagaIdWithoutControlCharacters() {
    List<RuntimeException> causes =
        List.of(
            new SagaStaleCompensationEpisodeException(
                FORGING_ID,
                Instant.parse("2026-09-01T00:00:00Z"),
                Duration.ofDays(8),
                Duration.ofDays(7)),
            new SagaEventHeldException(
                FORGING_ID, GlobalOffset.of(42), SagaEventHeldException.HoldReason.BACKLOG_PENDING),
            new SagaSkippedWhileFaultedException(FORGING_ID, GlobalOffset.of(42)));

    for (RuntimeException cause : causes) {
      assertThat(cause.getMessage())
          .as(cause.getClass().getSimpleName())
          .doesNotContain("\r")
          .doesNotContain("\n")
          .contains("order-1FORGED WARNING line");
    }
  }
}
