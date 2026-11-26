package org.streamrune.crypto.aws;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * Captures the events of one logger category with a logback {@link ListAppender}. The captured
 * logger stops passing its events to the root's console appender while attached, so the warnings a
 * test provokes on purpose stay out of the build log; {@link #close()} restores it.
 */
final class LogCapture implements AutoCloseable {

  private final Logger logger;
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private final boolean additive;

  private LogCapture(Logger logger) {
    this.logger = logger;
    this.additive = logger.isAdditive();
    appender.start();
    logger.addAppender(appender);
    logger.setAdditive(false);
  }

  /** Starts capturing the logger named after {@code category}. */
  static LogCapture attach(Class<?> category) {
    return new LogCapture((Logger) LoggerFactory.getLogger(category));
  }

  /** The WARN events captured so far, in order. */
  List<ILoggingEvent> warnings() {
    return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
  }

  @Override
  public void close() {
    logger.setAdditive(additive);
    logger.detachAppender(appender);
    appender.stop();
  }
}
