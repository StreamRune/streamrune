package org.streamrune.quarkus;

import java.nio.channels.ClosedChannelException;
import java.util.logging.Filter;
import java.util.logging.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs a Server-Sent Events client that closed its connection at {@code DEBUG}, where Quarkus REST
 * logs it at {@code ERROR}.
 *
 * <p>Quarkus REST writes each frame of a stream asynchronously, and its publisher response handler
 * logs a frame it cannot write once the response has started as "Exception in SSE server handling,
 * impossible to send it to client", at {@code ERROR}. A client that closes its connection makes the
 * next frame fail with a {@link ClosedChannelException} (Netty's {@code
 * StacklessClosedChannelException}) whenever that frame is already on its way when the connection
 * closes; Quarkus REST then cancels the stream and {@link SseController}'s teardown runs as on
 * every other end. That is the client going away, which every Server-Sent Events client does.
 *
 * <p>This filter sits on that handler's logger ({@value #QUARKUS_REST_PUBLISHER_LOGGER}). A record
 * whose exception, or a cause of it, is a {@link ClosedChannelException} is logged by {@link
 * SseController}'s logger at {@code DEBUG} and not published at its own level; every other record
 * passes to the filter that was installed before, or is published. Quarkus logs through JBoss
 * LogManager, an implementation of {@code java.util.logging}, so the filter is one of that API.
 * {@link SseController} installs it when it is created ({@link #install()}) and removes it when it
 * is destroyed.
 */
final class SseClientDisconnectLogFilter implements Filter {

  /** The logger of Quarkus REST's publisher response handler, which writes the frames. */
  static final String QUARKUS_REST_PUBLISHER_LOGGER =
      "org.jboss.resteasy.reactive.server.handlers.PublisherResponseHandler";

  private static final Logger LOG = LoggerFactory.getLogger(SseController.class);

  /** Depth cap so a cyclic cause chain cannot loop. */
  private static final int MAX_CAUSE_DEPTH = 50;

  private final Filter previous;

  private SseClientDisconnectLogFilter(Filter previous) {
    this.previous = previous;
  }

  @Override
  public boolean isLoggable(LogRecord logRecord) {
    if (isClientGone(logRecord.getThrown())) {
      LOG.debug(
          "A Server-Sent Events client closed its connection while a frame was on its way; its"
              + " stream ends: {}",
          logRecord.getThrown().toString());
      return false;
    }
    return previous == null || previous.isLoggable(logRecord);
  }

  /** Whether {@code failure} or a cause of it is a {@link ClosedChannelException}. */
  static boolean isClientGone(Throwable failure) {
    Throwable c = failure;
    for (int depth = 0; c != null && depth < MAX_CAUSE_DEPTH; depth++, c = c.getCause()) {
      if (c instanceof ClosedChannelException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Installs the filter on {@value #QUARKUS_REST_PUBLISHER_LOGGER}, in front of the filter
   * installed there before, unless it is installed already.
   *
   * @return what removes the filter again, restoring the one before it; does nothing when the
   *     filter was installed already, or when another filter replaced it since
   */
  static Runnable install() {
    java.util.logging.Logger logger =
        java.util.logging.Logger.getLogger(QUARKUS_REST_PUBLISHER_LOGGER);
    synchronized (SseClientDisconnectLogFilter.class) {
      Filter current = logger.getFilter();
      if (current instanceof SseClientDisconnectLogFilter) {
        return () -> {};
      }
      SseClientDisconnectLogFilter filter = new SseClientDisconnectLogFilter(current);
      logger.setFilter(filter);
      return () -> {
        synchronized (SseClientDisconnectLogFilter.class) {
          if (logger.getFilter() == filter) {
            logger.setFilter(current);
          }
        }
      };
    }
  }
}
