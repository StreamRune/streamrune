package org.streamrune.core.audit;

/** Outcome of a command execution recorded in an {@link AuditEntry}. */
public enum AuditOutcome {
  /** The command was executed and events were persisted successfully. */
  SUCCESS,
  /** The command failed after all retries were exhausted. */
  FAILURE,
  /**
   * The command was <b>rejected</b> (vetoed) by an interceptor before it executed — nothing ran and
   * no events were persisted. Distinct from {@link #SUCCESS} (a false record for a rejection) and
   * from {@link #FAILURE} (an execution failure, not a rejection). The rejecting interceptor is
   * named in {@link AuditEntry#errorMessage()}.
   */
  VETOED
}
