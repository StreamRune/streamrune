package org.streamrune.core;

/** Thrown when a command is rejected because the caller lacks the required authorization. */
public class AuthorizationException extends DomainException {

  public AuthorizationException(String message) {
    super(message);
  }
}
