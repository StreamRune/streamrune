package org.streamrune.test;

/**
 * The failure a {@link FaultSchedule} injects into a call of a forwarding decorator ({@link
 * ForwardingEventStore}, {@link ForwardingOutboxStore} and the others), unless the schedule names
 * another one with {@link FaultSchedule#throwing}. It stands for a crash or an outage the test
 * arranged, not for a failure of the store: assert on it to tell the two apart.
 *
 * <p>Its message names the method, the number of the call, and whether the delegate ran first:
 * {@code injected: append (call 2, before the delegate)} or {@code injected: append (call 2, after
 * the delegate)}.
 */
public class InjectedFaultException extends RuntimeException {

  /**
   * Creates the exception with its message.
   *
   * @param message what was injected where
   */
  public InjectedFaultException(String message) {
    super(message);
  }
}
