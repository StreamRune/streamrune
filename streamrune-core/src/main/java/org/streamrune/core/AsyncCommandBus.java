package org.streamrune.core;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous command bus that processes commands non-blocking.
 *
 * <p>This interface provides async command processing for high-throughput scenarios where blocking
 * the calling thread is not acceptable.
 */
public interface AsyncCommandBus {

  /**
   * Executes the given command asynchronously.
   *
   * <p>The returned future is completed exclusively by the bus — normally with the command result,
   * exceptionally with the execution failure. Callers must not {@code complete}, {@code obtrude},
   * or {@code cancel} it: the bus may share the same instance internally, and cancelling does not
   * stop the command, which may still execute and persist events.
   *
   * @param command the command to execute
   * @param <C> command type
   * @return a CompletableFuture that completes with the command result
   */
  <C extends Command> CompletableFuture<CommandBus.CommandResult> executeAsync(C command);
}
