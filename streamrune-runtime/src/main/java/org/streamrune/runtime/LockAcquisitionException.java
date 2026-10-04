package org.streamrune.runtime;

import org.streamrune.core.LockException;
import org.streamrune.core.types.StreamId;

/**
 * Thrown by {@link LocalStripedLocker} when lock acquisition fails, carrying the contended {@link
 * StreamId}. A subtype of the canonical {@link LockException} so callers (e.g. the command bus'
 * retryable-failure classification) treat every locker's acquisition failure uniformly, while this
 * variant still exposes the stream for diagnostics.
 */
public class LockAcquisitionException extends LockException {

  private final StreamId streamId;

  public LockAcquisitionException(StreamId streamId, String message) {
    super(message);
    this.streamId = streamId;
  }

  public LockAcquisitionException(StreamId streamId, String message, Throwable cause) {
    super(message, cause);
    this.streamId = streamId;
  }

  /** The stream whose lock could not be acquired; its id is {@code streamId().aggregateId()}. */
  public StreamId streamId() {
    return streamId;
  }

  @Override
  public String toString() {
    return "LockAcquisitionException{streamId=" + streamId + ", message=" + getMessage() + "}";
  }
}
