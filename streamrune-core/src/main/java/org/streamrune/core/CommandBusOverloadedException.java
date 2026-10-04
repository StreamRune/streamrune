package org.streamrune.core;

/**
 * Thrown when {@link AsyncCommandBus#executeAsync(Command)} refuses admission because the bus's
 * in-flight async budget is exhausted: the command was <b>refused admission</b> — nothing was
 * attempted, nothing was decided, no infrastructure was touched. The synchronous {@link
 * CommandBus#execute(Command)} path is unaffected — it is bounded by the caller's own threads, not
 * by this budget, and stays as-is.
 *
 * <p><b>Why an admission limit exists at all.</b> {@code executeAsync} spawns one virtual thread
 * per call; virtual threads are cheap, but the futures and captured command state they hold are not
 * free, and nothing else in the framework bounds how many can be in flight at once. Left unbounded,
 * a submission burst that outpaces the shared, commit-ordered append path (see {@code
 * PostgresEventStore}'s single-writer global-offset serialization) queues unboundedly in memory
 * instead of being rejected — a slow-degradation-into-OOM failure mode instead of a fast, visible
 * one. The bus now enforces an explicit budget ({@code Builder.maxInFlightAsyncCommands(int)},
 * default {@code 10_000}) and rejects fast once it is exhausted, incrementing {@code
 * streamrune.commands.async.rejected} — an operator signal to size the budget, add capacity, or
 * apply backpressure upstream, instead of an unexplained OOM days later.
 *
 * <p>Same rejection semantics as {@link CommandBusClosedException} — both fire before anything
 * durable is attempted, so both are safe to treat as retry-later: a saga forward dispatch
 * propagates the failure for redelivery instead of claiming a compensation episode, and DLQ replay
 * treats it identically to a closed-bus refusal (see {@code SagaCommandDispatch} and {@code
 * DeadLetterRetryRunner}).
 *
 * <p>Extends {@link IllegalStateException} deliberately, mirroring {@link
 * CommandBusClosedException} exactly: every {@code catch (IllegalStateException)} also covers this
 * subtype, and only consumers that need the overload/closed/misuse split test for the specific
 * subtype.
 */
public class CommandBusOverloadedException extends IllegalStateException {

  public CommandBusOverloadedException(String message) {
    super(message);
  }
}
