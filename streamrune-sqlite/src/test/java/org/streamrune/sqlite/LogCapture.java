package org.streamrune.sqlite;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * Captures the log events of one logger category: a logback {@link ListAppender} attached to the
 * logger named after a class, detached by {@link #close()}. The logger's level is left as {@code
 * logback-test.xml} sets it (the root at {@code WARN}), so a capture sees {@code WARN} and {@code
 * ERROR} events.
 */
final class LogCapture implements AutoCloseable {

  private final Logger logger;
  private final ListAppender<ILoggingEvent> appender;

  private LogCapture(Logger logger, ListAppender<ILoggingEvent> appender) {
    this.logger = logger;
    this.appender = appender;
  }

  /** Starts capturing the events of the logger named after {@code category}. */
  static LogCapture attach(Class<?> category) {
    Logger logger = (Logger) LoggerFactory.getLogger(category);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    return new LogCapture(logger, appender);
  }

  /** The captured events of exactly {@code level}, in the order they were logged. */
  List<ILoggingEvent> events(Level level) {
    synchronized (appender) {
      return appender.list.stream().filter(e -> e.getLevel() == level).toList();
    }
  }

  /** Stops capturing: detaches and stops the appender. */
  @Override
  public void close() {
    logger.detachAppender(appender);
    appender.stop();
  }
}
