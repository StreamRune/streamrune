package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The log capture the characterization tests read: every level while attached, nothing after. */
class LogCaptureTest {

  private static final Logger log = LoggerFactory.getLogger(LogCaptureTest.class);

  @Test
  void capturesAnInfoLineBelowTheConfiguredLevelAndRestoresTheLoggerOnClose() {
    var logger = (ch.qos.logback.classic.Logger) log;
    Level levelBefore = logger.getLevel();
    boolean additiveBefore = logger.isAdditive();
    assertThat(log.isInfoEnabled()).as("logback-test.xml keeps INFO off").isFalse();

    try (LogCapture capture = LogCapture.attach(LogCaptureTest.class)) {
      log.info("captured at info");
      log.debug("captured at debug");
      log.warn("captured at warn");

      assertThat(capture.events(Level.INFO))
          .extracting(ILoggingEvent::getFormattedMessage)
          .containsExactly("captured at info");
      assertThat(capture.events(Level.INFO))
          .extracting(ILoggingEvent::getLoggerName)
          .containsExactly(LogCaptureTest.class.getName());
      assertThat(capture.events(Level.DEBUG))
          .extracting(ILoggingEvent::getFormattedMessage)
          .containsExactly("captured at debug");
      assertThat(capture.events(Level.WARN))
          .extracting(ILoggingEvent::getFormattedMessage)
          .containsExactly("captured at warn");
      assertThat(capture.events(Level.ERROR)).isEmpty();

      capture.close();
      log.warn("logged after close");
      assertThat(capture.events(Level.WARN))
          .as("nothing is captured after close")
          .extracting(ILoggingEvent::getFormattedMessage)
          .containsExactly("captured at warn");
    }

    assertThat(logger.getLevel()).isEqualTo(levelBefore);
    assertThat(logger.isAdditive()).isEqualTo(additiveBefore);
    assertThat(log.isInfoEnabled()).isFalse();
  }
}
