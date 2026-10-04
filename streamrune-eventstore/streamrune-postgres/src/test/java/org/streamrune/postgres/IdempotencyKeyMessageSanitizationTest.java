package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;

/**
 * The two PostgreSQL exception messages that name an idempotency key render it through {@link
 * LogSanitizer#sanitizeForLog}. Each such exception reaches a log line and — through {@code
 * AuditCommandInterceptor.onError} and the dead-letter queue — a persisted row; a key rebuilt
 * through {@link IdempotencyKey}'s decode door (the canonical constructor keeps no charset rule)
 * carries its control characters this far.
 */
class IdempotencyKeyMessageSanitizationTest {

  private static final IdempotencyKey FORGED =
      new IdempotencyKey("client-key\n2026-10-02 10:00:00 WARN forged\r\u0085");

  @Test
  void inboxReadFailure_namesTheKeySanitized() {
    DataSource down =
        (DataSource)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getConnection")) {
                    throw new SQLException("connection refused");
                  }
                  throw new UnsupportedOperationException(method.getName());
                });

    var ex =
        assertThrows(EventStoreException.class, () -> new PostgresCommandInbox(down).find(FORGED));

    assertRendersTheKeySanitized(ex.getMessage());
  }

  /**
   * The claim-lost dead end ({@code appendWithKey} lost the claim, yet the winner's row is gone
   * when it looks it up) is a race window no test can open deterministically, so its message is
   * built by one helper, pinned here.
   */
  @Test
  void inboxConflictWithoutARow_namesTheKeySanitized() {
    assertRendersTheKeySanitized(PostgresEventStore.inboxConflictWithoutRow(FORGED).getMessage());
  }

  private static void assertRendersTheKeySanitized(String message) {
    assertTrue(message.contains(LogSanitizer.sanitizeForLog(FORGED.value())), message);
    assertFalse(message.chars().anyMatch(Character::isISOControl), message);
  }
}
