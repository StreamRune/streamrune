package org.streamrune.test;

import java.util.Objects;
import java.util.Set;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.types.IdempotencyKey;

/**
 * A {@link CommandBus} that forwards every call to a delegate and injects the faults of a {@link
 * FaultSchedule}: the base class of a bus decorator in a test, and a model of what a decorator in
 * an application forwards.
 *
 * <pre>{@code
 * class CountingBus extends ForwardingCommandBus {
 *   final AtomicInteger dispatched = new AtomicInteger();
 *
 *   CountingBus(CommandBus delegate) {
 *     super(delegate);
 *   }
 *
 *   @Override
 *   public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
 *     dispatched.incrementAndGet();
 *     return super.execute(command, key);
 *   }
 * }
 * }</pre>
 *
 * <p>Every method of the interface is overridden and forwarded to the same method of the delegate,
 * on the calling thread, the default methods included: {@code execute(command, key)}, {@code
 * supportsIdempotentExecution()} and {@code sagaFenceStorageIdentity()} reach the delegate's own
 * implementation, never the defaults of {@link CommandBus}. A saga runner therefore accepts the
 * decorator wherever it accepts the delegate (see the decorator section of {@link CommandBus}). A
 * subclass may override a method to observe, block or fail it; {@code super} applies the schedule,
 * {@link #delegate} does not. A subclass that overrides {@code execute} hands the delegate the
 * command instance and the key it received.
 */
public class ForwardingCommandBus implements CommandBus {

  private static final Set<String> METHODS = FaultSchedule.methodsOf(CommandBus.class);

  /** The bus every call is forwarded to. */
  protected final CommandBus delegate;

  /** The faults injected into the forwarded calls, and their count. */
  protected final FaultSchedule schedule;

  /**
   * Forwards every call to {@code delegate} and injects no fault.
   *
   * @param delegate the bus to forward to
   */
  public ForwardingCommandBus(CommandBus delegate) {
    this(delegate, FaultSchedule.none());
  }

  /**
   * Forwards every call to {@code delegate} and injects the faults of {@code schedule}.
   *
   * @param delegate the bus to forward to
   * @param schedule the faults to inject; it learns the method names of {@link CommandBus}
   * @throws IllegalArgumentException if {@code schedule} names a method {@link CommandBus} does not
   *     have
   */
  public ForwardingCommandBus(CommandBus delegate, FaultSchedule schedule) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.schedule = Objects.requireNonNull(schedule, "schedule is required").forMethods(METHODS);
  }

  @Override
  public <C extends Command> CommandResult execute(C command) {
    return schedule.call("execute", () -> delegate.execute(command));
  }

  @Override
  public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
    return schedule.call("execute", () -> delegate.execute(command, key));
  }

  @Override
  public boolean supportsIdempotentExecution() {
    return schedule.call("supportsIdempotentExecution", delegate::supportsIdempotentExecution);
  }

  @Override
  public Object sagaFenceStorageIdentity() {
    return schedule.call("sagaFenceStorageIdentity", delegate::sagaFenceStorageIdentity);
  }
}
