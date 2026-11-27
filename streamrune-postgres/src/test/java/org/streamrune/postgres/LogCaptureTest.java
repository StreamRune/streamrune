package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * The log capture itself: one category at every level while attached, filtered by level, nothing
 * after it is closed, and the logger's own level and additivity restored.
 */
class LogCaptureTest {

  @Test
  void capturesTheCategoryAtEveryLevelUntilClosed() {
    var log = (Logger) LoggerFactory.getLogger(LogCaptureTest.class);
    var other = LoggerFactory.getLogger(RecordingDataSource.class);
    assertNull(log.getLevel(), "the category inherits its level from the WARN root");
    assertTrue(log.isAdditive());

    LogCapture capture = LogCapture.attach(LogCaptureTest.class);
    try (capture) {
      assertEquals(Level.TRACE, log.getLevel());
      log.warn("first {}", 1);
      log.error("broken");
      log.info("an info line");
      log.debug("a debug line");
      other.warn("another category");
      log.warn("second");
    }
    log.warn("after close");
    log.info("info after close");

    assertNull(log.getLevel(), "close() restores the inherited level");
    assertTrue(log.isAdditive(), "close() restores the additivity");
    assertThat(capture.events(Level.WARN))
        .extracting(ILoggingEvent::getFormattedMessage, ILoggingEvent::getLoggerName)
        .containsExactly(
            tuple("first 1", LogCaptureTest.class.getName()),
            tuple("second", LogCaptureTest.class.getName()));
    assertThat(capture.events(Level.ERROR))
        .extracting(ILoggingEvent::getFormattedMessage)
        .containsExactly("broken");
    assertThat(capture.events(Level.INFO))
        .extracting(ILoggingEvent::getFormattedMessage)
        .containsExactly("an info line");
    assertThat(capture.events(Level.DEBUG))
        .extracting(ILoggingEvent::getFormattedMessage)
        .containsExactly("a debug line");
  }

  @Test
  void closeRestoresAnExplicitLevelAndAdditivity() {
    var log = (Logger) LoggerFactory.getLogger(LogCapture.class);
    log.setLevel(Level.ERROR);
    log.setAdditive(false);
    try {
      LogCapture.attach(LogCapture.class).close();
      assertEquals(Level.ERROR, log.getLevel());
      assertEquals(false, log.isAdditive());
    } finally {
      log.setLevel(null);
      log.setAdditive(true);
    }
  }
}
