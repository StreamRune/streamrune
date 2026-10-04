package org.streamrune.core;

/**
 * The canonical exception for {@link AggregateLocker} acquisition failures. Every shipped locker
 * throws this type or a subtype of it: the PostgreSQL advisory locker throws {@code LockException}
 * directly, while the local striped locker throws an aggregate-scoped subtype that still {@code
 * is-a} {@code LockException}. Callers therefore need only catch (or {@code instanceof}-check) this
 * one type — the command bus does exactly that to classify the failure as transient and retryable.
 *
 * <p>Like {@link OptimisticLockException}, deliberately not a subtype of {@link
 * EventStoreException}: failing to acquire a lock is a concurrency outcome, not an event store
 * failure.
 */
public class LockException extends RuntimeException {

  public LockException(String message) {
    super(message);
  }

  public LockException(String message, Throwable cause) {
    super(message, cause);
  }
}
