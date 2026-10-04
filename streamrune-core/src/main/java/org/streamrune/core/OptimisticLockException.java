package org.streamrune.core;

/**
 * Thrown when a concurrent modification is detected via optimistic locking: a stream version
 * mismatch on {@link EventStore#append}, or a read-model version mismatch on {@link
 * org.streamrune.core.projection.ProjectionRepository#save}.
 *
 * <p>Extends {@link RuntimeException} directly rather than {@link EventStoreException} on purpose:
 * a version conflict is an expected, retryable concurrency-control signal (the command bus retries
 * it automatically), not an event store failure — and it is also thrown by projection repositories,
 * which are not event stores. Catch it explicitly; an {@code EventStoreException} handler will not
 * see it.
 */
public class OptimisticLockException extends RuntimeException {

  /**
   * Creates an exception describing a version conflict on a known stream.
   *
   * @param streamId the stream where the conflict occurred
   * @param expectedVersion the version the caller assumed
   * @param actualVersion the version found in the store
   */
  public OptimisticLockException(String streamId, long expectedVersion, long actualVersion) {
    super(
        "Optimistic lock conflict on stream '%s': expected version %d but was %d"
            .formatted(streamId, expectedVersion, actualVersion));
  }

  /**
   * Creates an exception with a pre-formatted message.
   *
   * @param message the detail message
   */
  public OptimisticLockException(String message) {
    super(message);
  }

  /**
   * Creates an exception with a message and a cause (typically a JDBC {@code SQLException} with
   * SQLSTATE {@code 23505}).
   *
   * @param message the detail message
   * @param cause the underlying exception
   */
  public OptimisticLockException(String message, Throwable cause) {
    super(message, cause);
  }
}
