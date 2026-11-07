package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

/**
 * Which records of Quarkus REST's publisher response handler {@link SseClientDisconnectLogFilter}
 * keeps off {@code ERROR}, and how it is installed and removed.
 */
class SseClientDisconnectLogFilterTest {

  /** What Netty fails a write on a closed channel with: a subtype of the JDK's exception. */
  private static final class ClosedWithoutAStackTrace extends ClosedChannelException {
    private static final long serialVersionUID = 1L;
  }

  private static LogRecord error(Throwable thrown) {
    var logRecord =
        new LogRecord(
            Level.SEVERE, "Exception in SSE server handling, impossible to send it to client");
    logRecord.setThrown(thrown);
    return logRecord;
  }

  @Test
  void aClosedConnection_isNotPublished_bareOrAsACause() {
    Logger logger = Logger.getLogger(SseClientDisconnectLogFilter.QUARKUS_REST_PUBLISHER_LOGGER);
    Filter before = logger.getFilter();
    Runnable remove = SseClientDisconnectLogFilter.install();
    try {
      Filter filter = logger.getFilter();
      assertThat(filter.isLoggable(error(new ClosedWithoutAStackTrace()))).isFalse();
      assertThat(filter.isLoggable(error(new IOException("write", new ClosedChannelException()))))
          .isFalse();
      assertThat(filter.isLoggable(error(new IOException("Connection reset by peer"))))
          .as("another failure is published at its own level")
          .isTrue();
      assertThat(filter.isLoggable(error(null))).isTrue();
    } finally {
      remove.run();
    }
    assertThat(logger.getFilter()).as("removing restores the filter before it").isSameAs(before);
  }

  @Test
  void aFilterInstalledBefore_decidesEveryOtherRecord_andIsRestored() {
    Logger logger = Logger.getLogger(SseClientDisconnectLogFilter.QUARKUS_REST_PUBLISHER_LOGGER);
    Filter original = logger.getFilter();
    Filter refusesEverything = logRecord -> false;
    logger.setFilter(refusesEverything);
    try {
      Runnable remove = SseClientDisconnectLogFilter.install();
      Runnable second = SseClientDisconnectLogFilter.install();
      assertThat(logger.getFilter().isLoggable(error(new IOException("other")))).isFalse();
      second.run();
      assertThat(logger.getFilter())
          .as("a second install changes nothing, and removing it neither")
          .isInstanceOf(SseClientDisconnectLogFilter.class);
      remove.run();
      assertThat(logger.getFilter()).isSameAs(refusesEverything);
    } finally {
      logger.setFilter(original);
    }
  }
}
