package org.streamrune.postgres;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * Captures the log events of one logger category: a logback {@link ListAppender} attached to the
 * logger named after a class until {@link #close()} detaches it. A test pins a log line through its
 * category, level and formatted text.
 */
final class LogCapture implements AutoCloseable {

  private final Logger logger;
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  private LogCapture(Logger logger) {
    this.logger = logger;
  }

  /** Starts capturing the events of the logger named after {@code category}. */
  static LogCapture attach(Class<?> category) {
    var capture = new LogCapture((Logger) LoggerFactory.getLogger(category));
    capture.appender.start();
    capture.logger.addAppender(capture.appender);
    return capture;
  }

  /** The events captured so far at exactly {@code level}, in order. */
  List<ILoggingEvent> events(Level level) {
    synchronized (appender) {
      return appender.list.stream().filter(e -> e.getLevel() == level).toList();
    }
  }

  /** Detaches and stops the appender; the captured events stay readable. */
  @Override
  public void close() {
    logger.detachAppender(appender);
    appender.stop();
  }
}
