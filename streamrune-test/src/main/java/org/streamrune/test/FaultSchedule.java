package org.streamrune.test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The faults a forwarding decorator injects into the calls it forwards, and the count of those
 * calls. Every {@code Forwarding*} store of the kit takes one ({@link ForwardingEventStore}, {@link
 * ForwardingSagaStore}, {@link ForwardingSagaDeadLetterStore}, {@link ForwardingDeadLetterQueue},
 * {@link ForwardingOffsetStore}, {@link ForwardingProjectionDeadLetterStore}, {@link
 * ForwardingOutboxStore}) and consults it on every call, before and after the delegate runs:
 *
 * <pre>{@code
 * var faults = FaultSchedule.none().crashAfter("saveOffset");
 * var offsets = new ForwardingOffsetStore(new InMemoryOffsetStore(), faults);
 * // the next saveOffset stores the checkpoint, then throws: a crash between two writes
 * }</pre>
 *
 * <p>A method is named by its Java name in the decorated interface; the overloads of one name share
 * it ({@code "load"} is both {@code EventStore.load} methods). A decorator registers the names of
 * its interface when it is built, and from then on a name that is not one of them is refused with
 * {@link IllegalArgumentException} — a misspelled name fails the test where it is written, not by a
 * fault that never fires. A name scheduled before the decorator is built is checked when it is
 * built. A schedule handed to decorators of several interfaces knows the methods of each and counts
 * a name for whichever decorator calls it.
 *
 * <ul>
 *   <li>{@link #failOnce} and {@link #failAt} make one call throw before the delegate runs: the
 *       store is not touched, as when the connection is gone before the statement is sent.
 *   <li>{@link #failAlways} makes every later call throw before the delegate runs: an outage.
 *   <li>{@link #crashAfter} lets the next call run on the delegate and then throws: the write
 *       happened and the caller never learns it, as when the process dies between a commit and the
 *       next step. A flow that writes in more than one step has a crash point after each write;
 *       {@code crashAfter} is how a test stops the flow there, runs it again and asserts that it
 *       converges.
 * </ul>
 *
 * <p>The injected failure is an {@link InjectedFaultException} unless {@link #throwing} names
 * another one, such as an {@code EventStoreException} around an {@code SQLException} to exercise
 * the classification of a transient failure. A delegate that throws on its own propagates its own
 * failure, and a {@code crashAfter} scheduled for that call is spent. Every call is counted, the
 * failed ones included ({@link #calls(String)}).
 *
 * <p>Thread-safe: the decorators may be called from the threads of the runners under test.
 */
public final class FaultSchedule {

  /**
   * One call a decorator forwards: the method, the number of the call (from 1, per method) and
   * whether it throws once the delegate has returned.
   */
  record Call(String method, int number, boolean throwsAfterDelegating) {}

  private final ReentrantLock lock = new ReentrantLock();
  private final Set<String> methods = new HashSet<>();
  private final Map<String, Integer> calls = new HashMap<>();
  private final Map<String, Integer> pendingFailures = new HashMap<>();
  private final Map<String, Set<Integer>> failingCalls = new HashMap<>();
  private final Set<String> alwaysFailing = new HashSet<>();
  private final Map<String, Integer> pendingCrashes = new HashMap<>();
  private Supplier<? extends RuntimeException> failure;

  private FaultSchedule() {}

  /**
   * A schedule that fails nothing and counts every call; the methods below add faults to it.
   *
   * @return a new, empty schedule
   */
  public static FaultSchedule none() {
    return new FaultSchedule();
  }

  /**
   * The next call of {@code method} that no other fault takes throws, before the delegate runs.
   * Called twice, it fails the next two calls.
   *
   * @param method the method's name in the decorated interface
   * @return this schedule
   * @throws IllegalArgumentException if the decorated interface has no such method
   */
  public FaultSchedule failOnce(String method) {
    lock.lock();
    try {
      requireKnown(method);
      pendingFailures.merge(method, 1, Integer::sum);
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The {@code nthCall}-th call of {@code method} throws, before the delegate runs. Calls are
   * numbered from 1, from the creation of the schedule or its last {@link #reset()}.
   *
   * @param method the method's name in the decorated interface
   * @param nthCall the number of the call that fails, 1 or more
   * @return this schedule
   * @throws IllegalArgumentException if the decorated interface has no such method, if {@code
   *     nthCall} is below 1, or if that call has happened already
   */
  public FaultSchedule failAt(String method, int nthCall) {
    lock.lock();
    try {
      requireKnown(method);
      if (nthCall < 1) {
        throw new IllegalArgumentException("nthCall must be 1 or more, got " + nthCall);
      }
      int made = calls.getOrDefault(method, 0);
      if (nthCall <= made) {
        throw new IllegalArgumentException(
            "call " + nthCall + " of " + method + " has happened already (" + made + " made)");
      }
      failingCalls.computeIfAbsent(method, m -> new HashSet<>()).add(nthCall);
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Every later call of {@code method} throws, before the delegate runs, until {@link #reset()}.
   *
   * @param method the method's name in the decorated interface
   * @return this schedule
   * @throws IllegalArgumentException if the decorated interface has no such method
   */
  public FaultSchedule failAlways(String method) {
    lock.lock();
    try {
      requireKnown(method);
      alwaysFailing.add(method);
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The next call of {@code method} that does not fail before the delegate runs on the delegate,
   * then throws: the delegate's write is done, and its caller sees a failure. Called twice, it
   * crashes the next two such calls.
   *
   * @param method the method's name in the decorated interface
   * @return this schedule
   * @throws IllegalArgumentException if the decorated interface has no such method
   */
  public FaultSchedule crashAfter(String method) {
    lock.lock();
    try {
      requireKnown(method);
      pendingCrashes.merge(method, 1, Integer::sum);
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The failure every fault of this schedule throws from now on, in place of {@link
   * InjectedFaultException}: the supplier is asked for a new exception at each fault.
   *
   * @param failure creates the exception to throw; it must not return {@code null}
   * @return this schedule
   */
  public FaultSchedule throwing(Supplier<? extends RuntimeException> failure) {
    Objects.requireNonNull(failure, "failure is required");
    lock.lock();
    try {
      this.failure = failure;
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The calls of {@code method} made so far, the failed ones included.
   *
   * @param method the method's name in the decorated interface
   * @return the number of calls since the schedule was created or last {@link #reset()}
   * @throws IllegalArgumentException if the decorated interface has no such method
   */
  public int calls(String method) {
    lock.lock();
    try {
      requireKnown(method);
      return calls.getOrDefault(method, 0);
    } finally {
      lock.unlock();
    }
  }

  /**
   * The calls of every method made so far, the failed ones included.
   *
   * @return the number of calls since the schedule was created or last {@link #reset()}
   */
  public int calls() {
    lock.lock();
    try {
      return calls.values().stream().mapToInt(Integer::intValue).sum();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Forgets the calls and the faults: the schedule counts from zero again and fails nothing until a
   * fault is added. The failure set by {@link #throwing} and the method names of the decorators
   * stay.
   */
  public void reset() {
    lock.lock();
    try {
      calls.clear();
      pendingFailures.clear();
      failingCalls.clear();
      alwaysFailing.clear();
      pendingCrashes.clear();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Registers the method names of a decorated interface; a decorator calls it when it is built.
   *
   * @throws IllegalArgumentException if a fault scheduled before names a method the registered
   *     interfaces do not have
   */
  FaultSchedule forMethods(Set<String> names) {
    lock.lock();
    try {
      methods.addAll(names);
      Set<String> scheduled = new TreeSet<>();
      scheduled.addAll(pendingFailures.keySet());
      scheduled.addAll(failingCalls.keySet());
      scheduled.addAll(alwaysFailing);
      scheduled.addAll(pendingCrashes.keySet());
      scheduled.removeAll(methods);
      if (!scheduled.isEmpty()) {
        throw new IllegalArgumentException(
            "FaultSchedule: faults are scheduled for "
                + scheduled
                + ", which the decorated interface does not have; its methods are "
                + new TreeSet<>(methods));
      }
      return this;
    } finally {
      lock.unlock();
    }
  }

  /** The names of the instance methods of an interface, its default methods included. */
  static Set<String> methodsOf(Class<?> type) {
    return Arrays.stream(type.getMethods())
        .filter(method -> !Modifier.isStatic(method.getModifiers()))
        .map(Method::getName)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Counts a call of {@code method} and decides it: throws the injected failure when a fault fails
   * the call before the delegate runs, otherwise returns the call, which says whether it throws
   * after the delegate.
   */
  Call before(String method) {
    RuntimeException injected;
    lock.lock();
    try {
      requireKnown(method);
      int number = calls.merge(method, 1, Integer::sum);
      boolean atThisCall = removeIfPresent(failingCalls, method, number);
      boolean fails =
          atThisCall || alwaysFailing.contains(method) || takeOne(pendingFailures, method);
      if (!fails) {
        return new Call(method, number, takeOne(pendingCrashes, method));
      }
      injected = failure(method, number, "before the delegate");
    } finally {
      lock.unlock();
    }
    throw injected;
  }

  /** Throws the injected failure when {@code call} crashes after the delegate returned. */
  void after(Call call) {
    if (!call.throwsAfterDelegating()) {
      return;
    }
    RuntimeException injected;
    lock.lock();
    try {
      injected = failure(call.method(), call.number(), "after the delegate");
    } finally {
      lock.unlock();
    }
    throw injected;
  }

  /**
   * One call of a decorator that returns a value: {@link #before}, the delegate, {@link #after}.
   */
  <T> T call(String method, Supplier<T> delegate) {
    Call call = before(method);
    T result = delegate.get();
    after(call);
    return result;
  }

  /**
   * One call of a decorator that returns nothing: {@link #before}, the delegate, {@link #after}.
   */
  void run(String method, Runnable delegate) {
    Call call = before(method);
    delegate.run();
    after(call);
  }

  private RuntimeException failure(String method, int number, String when) {
    if (failure == null) {
      return new InjectedFaultException(
          "injected: " + method + " (call " + number + ", " + when + ")");
    }
    return Objects.requireNonNull(
        failure.get(), "the failure supplier given to FaultSchedule.throwing returned null");
  }

  private void requireKnown(String method) {
    Objects.requireNonNull(method, "method is required");
    if (!methods.isEmpty() && !methods.contains(method)) {
      throw new IllegalArgumentException(
          "FaultSchedule: "
              + method
              + " is not a method of the decorated interface; its methods are "
              + new TreeSet<>(methods));
    }
  }

  private static boolean takeOne(Map<String, Integer> pending, String method) {
    Integer left = pending.get(method);
    if (left == null) {
      return false;
    }
    if (left == 1) {
      pending.remove(method);
    } else {
      pending.put(method, left - 1);
    }
    return true;
  }

  private static boolean removeIfPresent(
      Map<String, Set<Integer>> numbers, String method, int number) {
    Set<Integer> forMethod = numbers.get(method);
    return forMethod != null && forMethod.remove(number);
  }
}
