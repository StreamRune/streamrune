package org.streamrune.core;

import java.time.Instant;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;

/**
 * Intercepts command execution for cross-cutting concerns such as audit logging, rate limiting,
 * custom validation, and monitoring.
 *
 * <p><b>Lifecycle contract</b> (implemented by the runtime command bus):
 *
 * <ul>
 *   <li>{@link #before(CommandContext)} is called in registration order before the command
 *       executes.
 *   <li>On success, {@link #after(CommandContext)} is called in reverse order on every interceptor
 *       with {@link CommandContext#result()} populated (never {@code null}). At that point the
 *       command outcome is already decided — events, if any, are committed — so an exception thrown
 *       from {@code after()} is logged by the bus and neither fails the command nor prevents other
 *       interceptors' {@code after()} from running.
 *   <li>If the call is an idempotent replay (the command already ran under its {@code
 *       IdempotencyKey}), nothing runs again: {@code after()} is called in reverse order on every
 *       interceptor with a result whose {@link CommandBus.CommandResult#reason()} is {@link
 *       CommandBus.ShortCircuitReason#IDEMPOTENT_REPLAY}. That is a success, not a rejection, and
 *       {@code onError()} is not called.
 *   <li>If {@code before()} returns {@code false}, the command is <em>not</em> executed. The bus
 *       returns a short-circuit result whose {@link CommandBus.CommandResult#shortCircuitedBy()}
 *       names the rejecting interceptor. {@code after()} is then called in reverse order only on
 *       interceptors whose {@code before()} previously returned {@code true}, with that (never
 *       {@code null}) short-circuit result. {@link #onError(CommandContext, Throwable)} is NOT
 *       called — a short-circuit is a decision, not a failure. The short-circuiting interceptor
 *       receives no further callback.
 *   <li>If {@code before()} throws, the exception propagates to the caller and {@code onError()} is
 *       called in reverse order only on interceptors whose {@code before()} completed (returned
 *       {@code true}). Neither the throwing interceptor nor interceptors that never ran are
 *       notified.
 *   <li>If command execution fails before events are committed, {@code onError()} is called in
 *       reverse order on all interceptors (all {@code before()} calls completed). Exceptions thrown
 *       from {@code onError()} are logged by the bus and never replace the original failure.
 * </ul>
 *
 * <p>Example — audit logging:
 *
 * <pre>{@code
 * CommandInterceptor audit = new CommandInterceptor() {
 *     @Override
 *     public boolean before(CommandContext ctx) {
 *         logger.info("Command {} of type {} on aggregate {} received",
 *             ctx.commandId(), ctx.commandType(), ctx.aggregateId());
 *         return true;
 *     }
 *
 *     @Override
 *     public void after(CommandContext ctx) {
 *         logger.info("Command {} succeeded with {} events",
 *             ctx.commandId(), ctx.result().events().size());
 *     }
 *
 *     @Override
 *     public void onError(CommandContext ctx, Throwable error) {
 *         logger.error("Command {} failed: {}", ctx.commandId(), error.getMessage());
 *     }
 * };
 * }</pre>
 */
public interface CommandInterceptor {

  /**
   * Context provided to interceptor methods. Holds all information about a command execution.
   *
   * @param command the command object
   * @param commandType the simple class name of the command
   * @param commandId the unique ID assigned to this command execution
   * @param aggregateType the aggregate type the command's registration declares, or null
   * @param aggregateId the aggregate ID targeted by the command, or null
   * @param result the result of the execution (success or short-circuit); null in {@code before}
   *     and {@code onError}
   * @param timestamp when the command was first received
   */
  record CommandContext(
      Command command,
      String commandType,
      CommandId commandId,
      AggregateType aggregateType,
      AggregateId aggregateId,
      CommandBus.CommandResult result,
      Instant timestamp) {

    /**
     * The stream the command targets — {@code aggregateType():aggregateId()} — or {@code null} when
     * either part is {@code null}.
     */
    public StreamId streamId() {
      return aggregateType == null || aggregateId == null
          ? null
          : StreamId.of(aggregateType, aggregateId);
    }
  }

  /**
   * Called before a command is executed. Return {@code false} to short-circuit the execution: the
   * command is not executed and the bus returns a {@link CommandBus.ShortCircuitReason#VETOED
   * VETOED} result (a rejection) with {@link CommandBus.CommandResult#shortCircuitedBy()} naming
   * this interceptor.
   *
   * @param ctx the command context (result is null)
   * @return {@code true} to proceed with execution, {@code false} to short-circuit
   */
  default boolean before(CommandContext ctx) {
    return true;
  }

  /**
   * Called after the command outcome is decided: the command executed successfully (events are
   * committed), the call was an idempotent replay, or a later interceptor vetoed it. So this does
   * <em>not</em> mean the command ran. {@link CommandContext#result()} is never {@code null}; read
   * its {@link CommandBus.CommandResult#reason()} (or {@link CommandBus.CommandResult#vetoed()} and
   * {@link CommandBus.CommandResult#idempotentReplay()}) to tell the cases apart, because {@link
   * CommandBus.CommandResult#shortCircuited()} is {@code true} for a veto and for a replay alike.
   * Exceptions thrown here are logged by the bus and do not change the command outcome.
   *
   * @param ctx the command context with result populated
   */
  default void after(CommandContext ctx) {}

  /**
   * Called when a command execution fails (including when a later interceptor's {@code before()}
   * throws), provided this interceptor's own {@code before()} completed. Not called on
   * short-circuit. Exceptions thrown here are logged by the bus and never replace the original
   * failure.
   *
   * @param ctx the command context (result is null)
   * @param error the exception that caused the failure
   */
  default void onError(CommandContext ctx, Throwable error) {}

  /**
   * Composes two interceptors into a single unit, executing {@code first} then {@code second} for
   * {@code before}, and in reverse order for {@code after} and {@code onError}.
   *
   * <p>Short-circuit semantics follow the bus contract applied at the granularity of the composed
   * unit: if either member's {@code before()} returns {@code false}, the composed {@code before()}
   * returns {@code false} and the bus treats the whole unit as the short-circuiting interceptor —
   * neither member receives {@code after()} for that command.
   *
   * <p>If {@code second.before()} throws, {@code first.onError()} is notified (its {@code before()}
   * had completed) before the exception propagates; a failure inside that notification is attached
   * as a suppressed exception of the original.
   *
   * <p>{@code after()} and {@code onError()} invoke both members even when the first-called member
   * throws: the remaining member still runs, and any secondary failure is attached as a suppressed
   * exception of the one that propagates (the bus logs and isolates it).
   */
  static CommandInterceptor compose(CommandInterceptor first, CommandInterceptor second) {
    return new CommandInterceptor() {
      @Override
      public boolean before(CommandContext ctx) {
        if (!first.before(ctx)) {
          return false;
        }
        try {
          return second.before(ctx);
        } catch (RuntimeException e) {
          // first.before() completed, so per the lifecycle contract it must be notified of the
          // failure even though the bus only sees the composed unit as the thrower.
          try {
            first.onError(ctx, e);
          } catch (RuntimeException onErrorFailure) {
            e.addSuppressed(onErrorFailure);
          }
          throw e;
        }
      }

      @Override
      public void after(CommandContext ctx) {
        try {
          second.after(ctx);
        } catch (RuntimeException e) {
          try {
            first.after(ctx);
          } catch (RuntimeException alsoFailed) {
            e.addSuppressed(alsoFailed);
          }
          throw e;
        }
        first.after(ctx);
      }

      @Override
      public void onError(CommandContext ctx, Throwable error) {
        try {
          second.onError(ctx, error);
        } catch (RuntimeException e) {
          try {
            first.onError(ctx, error);
          } catch (RuntimeException alsoFailed) {
            e.addSuppressed(alsoFailed);
          }
          throw e;
        }
        first.onError(ctx, error);
      }
    };
  }

  /** Returns a no-op interceptor that does nothing. */
  static CommandInterceptor noop() {
    return new CommandInterceptor() {};
  }
}
