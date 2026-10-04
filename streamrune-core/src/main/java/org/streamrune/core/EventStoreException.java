package org.streamrune.core;

/**
 * Thrown when an event store operation — appending events, loading aggregates, saving snapshots —
 * fails for infrastructure reasons such as connection errors, serialization failures, or schema
 * problems.
 *
 * <p>Deliberately <b>not</b> the supertype of {@link OptimisticLockException} or {@link
 * LockException}: a version conflict is an expected, retryable concurrency-control signal (the
 * command bus retries it automatically), not a store failure, and lock acquisition is a separate
 * concern. Callers wrapping store operations must catch those types explicitly — {@code catch
 * (EventStoreException e)} alone does not see them.
 */
public class EventStoreException extends RuntimeException {

  public EventStoreException(String message) {
    super(message);
  }

  public EventStoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
