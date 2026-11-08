package org.streamrune.runtime;

import org.slf4j.Logger;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.ScopedValueReads;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.core.types.LogSanitizer;

/**
 * The two rules the buses of this module share about the correlation id bound to a dispatch.
 *
 * <p><b>Which id a dispatch records.</b> {@link #inEffect()}: the bound saga origin's when an
 * origin is bound, otherwise the bound request context's. A command with neither has the id the
 * command bus generates for it before its interceptors run ({@link #ofTheCommandInProgress()}). The
 * command bus writes that id into the events of the command and the command audit into its row, so
 * an audit row and the events of its command never name different correlation ids. The query audit
 * records {@link #inEffect()}: a query stores no event, and with neither bound its row names none.
 *
 * <p><b>Which dispatch is refused.</b> {@link #refuseForeignReserved}: one whose bound correlation
 * id is in the reserved form and is not the bound saga origin's ({@link
 * StreamRuneContext#requireNoForeignReservedCorrelationId()}). The refusal is logged here, once, at
 * {@code WARN}: it is a wiring error of the code that bound the context, and the caller may be a
 * listener that catches what it is thrown. The line names what the dispatch read bound ({@link
 * #bindingsRead()}), so the code that bound it can be found.
 */
final class BoundCorrelationId {

  /**
   * The form {@link SagaOrigin#correlationId()} gives a saga's correlation id, up to its saga id.
   */
  private static final String SAGA_CORRELATION_PREFIX = IdConstraints.RESERVED_PREFIX + "saga:";

  private BoundCorrelationId() {}

  /**
   * The correlation id {@code VirtualThreadCommandBus} generated for the command dispatch in
   * progress on this thread, bound for the length of that dispatch when neither a saga origin nor a
   * request context names one. Read through {@link #ofTheCommandInProgress()} only.
   */
  static final ScopedValue<CorrelationId> GENERATED = ScopedValue.newInstance();

  /**
   * The correlation id the dispatch in progress records.
   *
   * @return the bound saga origin's correlation id ({@link SagaOrigin#correlationId()}) when an
   *     origin is bound; otherwise the bound request context's; {@code null} when neither is bound
   *     (or the context is bound to {@code null})
   */
  static CorrelationId inEffect() {
    StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();
    if (cause != null && cause.sagaOrigin() != null) {
      return cause.sagaOrigin().correlationId();
    }
    StreamRuneContext.RequestContext context = StreamRuneContext.capture();
    return context == null ? null : context.correlationId();
  }

  /**
   * The correlation id the command dispatch in progress records, in its events and in its audit
   * row: {@link #inEffect()}, or — when that names none — the one the command bus generated for
   * this dispatch.
   *
   * <p>The generated id is the command's own. A query executed inside the dispatch, and a command
   * executed inside it under a context or an origin, do not see it ({@link #inEffect()}); a command
   * executed inside it with nothing bound gets one of its own.
   *
   * @return the id; {@code null} only outside a dispatch of {@code VirtualThreadCommandBus} with
   *     neither an origin nor a context bound
   */
  static CorrelationId ofTheCommandInProgress() {
    CorrelationId bound = inEffect();
    if (bound != null) {
      return bound;
    }
    return ScopedValueReads.valueOrNull(GENERATED);
  }

  /**
   * Refuses the dispatch in progress when its bound correlation id is reserved and is not the bound
   * saga origin's, and logs the refusal at {@code WARN} with its fixed reason and what the dispatch
   * read bound ({@link #bindingsRead()}). The exception holds neither, and the log line holds the
   * bound value only as the saga it names, and only where the framework bound it.
   *
   * @param log the logger of the refusing bus
   * @param kind what is dispatched, for the log line: {@code "command"} or {@code "query"}
   * @param type the class of the dispatched command or query; its simple name is logged
   * @throws ReservedCorrelationIdException if the dispatch is refused
   */
  static void refuseForeignReserved(Logger log, String kind, Class<?> type) {
    try {
      StreamRuneContext.requireNoForeignReservedCorrelationId();
    } catch (ReservedCorrelationIdException refused) {
      log.warn(
          "Refusing {} {}: {}. Read bound {}",
          kind,
          type.getSimpleName(),
          refused.getMessage(),
          bindingsRead());
      throw refused;
    }
  }

  /**
   * What the dispatch in progress reads bound, for the line of a refusal: the thread it runs on,
   * whether a request context and a dispatch cause are bound, whether the cause carries a saga
   * origin, and whether the dispatch is a saga's ({@link StreamRuneContext#isSagaDispatch()}).
   *
   * <p>A saga dispatch binds the context, the cause and the saga marker together, in one carrier,
   * so a line that names a saga's dispatch with no cause, or a cause and a correlation id of
   * different sagas, shows that the bindings read are not the ones the dispatch made. To tell
   * which, the line names the saga of the cause's origin and the saga the bound correlation id
   * names when it has the saga form ({@link SagaOrigin#correlationId()}) — the latter only when a
   * saga origin is bound or the dispatch is a saga's: the framework bound those values from saga
   * ids. A reserved correlation id bound with neither is not named, since a request may have
   * supplied it. Every value passes {@link LogSanitizer#sanitizeForLog}.
   *
   * @return the description, starting with the thread
   */
  static String bindingsRead() {
    Thread thread = Thread.currentThread();
    StreamRuneContext.RequestContext context = StreamRuneContext.capture();
    StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();
    SagaOrigin origin = cause == null ? null : cause.sagaOrigin();
    boolean sagaDispatch = StreamRuneContext.isSagaDispatch();
    StringBuilder line =
        new StringBuilder("on ")
            .append(thread.isVirtual() ? "virtual thread " : "thread ")
            .append(LogSanitizer.sanitizeForLog(thread.getName()))
            .append(": ")
            .append(context == null ? "no request context" : "a request context");
    if (cause == null) {
      line.append(", no dispatch cause");
    } else if (origin == null) {
      line.append(", a dispatch cause without a saga origin");
    } else {
      line.append(", a dispatch cause of saga ").append(sagaIdOf(origin));
    }
    line.append(sagaDispatch ? ", a saga's dispatch" : ", not a saga's dispatch");
    if (context != null && (origin != null || sagaDispatch)) {
      String bound = context.correlationId().value();
      line.append(
          bound.startsWith(SAGA_CORRELATION_PREFIX)
              ? ", a correlation id of saga "
                  + LogSanitizer.sanitizeForLog(bound.substring(SAGA_CORRELATION_PREFIX.length()))
              : ", a reserved correlation id of no saga");
    }
    return line.toString();
  }

  private static String sagaIdOf(SagaOrigin origin) {
    return LogSanitizer.sanitizeForLog(origin.sagaId().value());
  }
}
