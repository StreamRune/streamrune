package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Identifies the authenticated user who triggered a command. Populated from {@link
 * org.streamrune.core.StreamRuneContext} when a request context is bound. Nullable.
 *
 * <p><b>Do not add a length or charset bound to this constructor.</b> The bound exists — it lives
 * on {@code RequestContext.fromRequest}, the ingress door every request filter and every
 * authenticated-principal resolver crosses, and it rejects an identity longer than {@link
 * IdConstraints#MAX_LENGTH} or carrying control characters before a command is accepted. It is
 * deliberately NOT here, because this constructor is also the RECONSTRUCTION path: {@code
 * PostgresDeadLetterQueue}, {@code PostgresCommandAuditQuery} and {@code PostgresEventAuditQuery}
 * each rebuild a {@code UserId} from a stored column, and {@code
 * DeadLetterRetryRunner.replayContextFrom} rebuilds a whole request context from those columns to
 * replay a dead-lettered command. A bound here would stop no write — the write already happened,
 * through a context built with the plain {@code RequestContext} constructor, which does not run the
 * check — and would instead make every retry of such an entry fail deterministically, burning its
 * whole ladder and permanently discarding an already-accepted business command. That is the same
 * defect as for the other id types, one type over, and it is why the check lives on the door.
 */
public record UserId(@JsonValue String value) {

  @JsonCreator
  public UserId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("userId is required");
    }
  }

  public static UserId of(String value) {
    return new UserId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
