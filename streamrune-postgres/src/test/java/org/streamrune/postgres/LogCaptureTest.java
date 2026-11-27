package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** The log capture itself: one category, filtered by level, nothing after it is closed. */
class LogCaptureTest {

  @Test
  void capturesTheCategoryByLevelUntilClosed() {
    var log = LoggerFactory.getLogger(LogCaptureTest.class);
    var other = LoggerFactory.getLogger(RecordingDataSource.class);
    LogCapture capture = LogCapture.attach(LogCaptureTest.class);
    try (capture) {
      log.warn("first {}", 1);
      log.error("broken");
      other.warn("another category");
      log.warn("second");
    }
    log.warn("after close");

    assertThat(capture.events(Level.WARN))
        .extracting(ILoggingEvent::getFormattedMessage, ILoggingEvent::getLoggerName)
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple("first 1", LogCaptureTest.class.getName()),
            org.assertj.core.groups.Tuple.tuple("second", LogCaptureTest.class.getName()));
    assertThat(capture.events(Level.ERROR))
        .extracting(ILoggingEvent::getFormattedMessage)
        .containsExactly("broken");
    assertThat(capture.events(Level.INFO)).isEmpty();
  }
}
