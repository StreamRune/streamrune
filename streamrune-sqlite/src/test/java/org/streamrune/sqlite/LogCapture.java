package org.streamrune.sqlite;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * Captures the log events of one logger category: a logback {@link ListAppender} attached to the
 * logger named after a class, detached by {@link #close()}.
 *
 * <p>While attached, the logger's level is {@code TRACE}, so a test can pin {@code INFO} and {@code
 * DEBUG} lines although {@code logback-test.xml} sets the root to {@code WARN}, and the logger is
 * not additive, so the captured lines stay out of the build log. {@link #close()} restores the
 * logger's previous level (an inherited level stays inherited) and additivity.
 */
final class LogCapture implements AutoCloseable {

  private final Logger logger;
  private final ListAppender<ILoggingEvent> appender;
  private final Level previousLevel;
  private final boolean previousAdditivity;

  private LogCapture(
      Logger logger,
      ListAppender<ILoggingEvent> appender,
      Level previousLevel,
      boolean previousAdditivity) {
    this.logger = logger;
    this.appender = appender;
    this.previousLevel = previousLevel;
    this.previousAdditivity = previousAdditivity;
  }

  /** Starts capturing every event of the logger named after {@code category}, down to TRACE. */
  static LogCapture attach(Class<?> category) {
    Logger logger = (Logger) LoggerFactory.getLogger(category);
    Level previousLevel = logger.getLevel();
    boolean previousAdditivity = logger.isAdditive();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.TRACE);
    logger.setAdditive(false);
    return new LogCapture(logger, appender, previousLevel, previousAdditivity);
  }

  /** The captured events of exactly {@code level}, in the order they were logged. */
  List<ILoggingEvent> events(Level level) {
    synchronized (appender) {
      return appender.list.stream().filter(e -> e.getLevel() == level).toList();
    }
  }

  /**
   * Stops capturing: restores the logger's level and additivity, then detaches and stops the
   * appender.
   */
  @Override
  public void close() {
    logger.setLevel(previousLevel);
    logger.setAdditive(previousAdditivity);
    logger.detachAppender(appender);
    appender.stop();
  }
}
