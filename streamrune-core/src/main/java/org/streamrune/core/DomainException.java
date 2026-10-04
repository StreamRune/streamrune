package org.streamrune.core;

/**
 * Thrown when a domain invariant is violated during command processing.
 *
 * <p>Root of the command-rejection hierarchy: {@link ValidationException} (structural validation
 * failed) and {@link AuthorizationException} (caller not permitted) both extend this class, so
 * catching {@code DomainException} at an API boundary handles every "command rejected by business
 * rules" case. Infrastructure misconfiguration (e.g. {@link NoDeciderException}) is deliberately
 * not part of this hierarchy.
 */
public class DomainException extends RuntimeException {

  public DomainException(String message) {
    super(message);
  }
}
