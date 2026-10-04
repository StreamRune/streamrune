package org.streamrune.core.gdpr;

/** GDPR operation type emitted to the audit log for compliance tracking. */
public enum GdprAction {
  /** Right to erasure (GDPR Article 17). */
  FORGET,
  /** Right to data portability (GDPR Article 20). */
  EXPORT,
  /**
   * Lifting a forgotten subject's terminal-erasure tombstone so that it may re-register. Reverses
   * part of an erasure (on AWS KMS, where the shared key is never destroyed, all of it).
   */
  REINSTATE
}
