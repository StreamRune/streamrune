package org.streamrune.core;

/** Thrown when a command is rejected because the circuit breaker is OPEN or HALF_OPEN. */
public class CircuitBreakerOpenException extends RuntimeException {

  public CircuitBreakerOpenException(String message) {
    super(message);
  }
}
