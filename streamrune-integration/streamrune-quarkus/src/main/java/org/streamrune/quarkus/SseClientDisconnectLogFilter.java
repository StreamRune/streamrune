package org.streamrune.quarkus;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * {@link SseController} installs it when it is created ({@link #install()}) and releases it when it
 * is destroyed. Installs are counted: the filter stays on the logger while one of them is held, and
 * the last release removes it.
 *
 * <p>The logger is Quarkus REST's, not the endpoint's, so while the filter is installed it applies
 * to every streaming endpoint of the application: a client of the application's own stream that
 * closes its connection is logged at {@code DEBUG} too.
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
   * The filter on the logger and the installs that hold it; guarded by the class's lock. {@code
   * null} when no install holds one.
   */
  private static Installation installation;

  /** One filter on the logger, and how many installs hold it; guarded by the class's lock. */
  private static final class Installation {
    private final java.util.logging.Logger logger;
    private final SseClientDisconnectLogFilter filter;
    private int holders;

    private Installation(java.util.logging.Logger logger, SseClientDisconnectLogFilter filter) {
      this.logger = logger;
      this.filter = filter;
    }
  }

  /**
   * Installs the filter on {@value #QUARKUS_REST_PUBLISHER_LOGGER}, in front of the filter
   * installed there before, or holds the one installed already.
   *
   * @return what releases this install, once; the last release of the filter restores the filter
   *     before it, unless another filter replaced it since
   */
  static Runnable install() {
    java.util.logging.Logger logger =
        java.util.logging.Logger.getLogger(QUARKUS_REST_PUBLISHER_LOGGER);
    synchronized (SseClientDisconnectLogFilter.class) {
      Installation held = installation;
      if (held == null || logger.getFilter() != held.filter) {
        held = new Installation(logger, new SseClientDisconnectLogFilter(logger.getFilter()));
        logger.setFilter(held.filter);
        installation = held;
      }
      held.holders++;
      Installation release = held;
      AtomicBoolean released = new AtomicBoolean();
      return () -> {
        if (released.compareAndSet(false, true)) {
          release(release);
        }
      };
    }
  }

  private static void release(Installation held) {
    synchronized (SseClientDisconnectLogFilter.class) {
      held.holders--;
      if (held.holders > 0) {
        return;
      }
      if (held.logger.getFilter() == held.filter) {
        held.logger.setFilter(held.filter.previous);
      }
      if (installation == held) {
        installation = null;
      }
    }
  }
}
