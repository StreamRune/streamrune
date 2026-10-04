package org.streamrune.core.gdpr;

import org.streamrune.core.types.SubjectId;

/**
 * Application-provided purger that deletes or redacts a data subject's rows from one read model,
 * projection, or external system during GDPR Article 17 (right to erasure). The symmetric
 * write-side counterpart of {@link SubjectDataCollector}: a collector reads a subject's data for an
 * export, a purger removes it for a forget.
 *
 * <p>Crypto-shredding alone (deleting the subject's encryption key) only makes {@code @Encrypted}
 * fields undecryptable — the ciphertext rows, and any unencrypted derived data, remain in read
 * models and projections. Register one purger per such read model so the forget operation actually
 * removes the rows; the forget service invokes every registered purger after the key is deleted.
 *
 * <p>Each purger is stateless and idempotent: purging a subject with no data, or purging the same
 * subject twice, must be a safe no-op (a forget may be retried after a partial failure).
 * Implementations are typically thin adapters over a {@code DELETE ... WHERE subject_id = ?} or an
 * UPDATE that redacts the relevant columns.
 */
public interface SubjectDataPurger {

  /**
   * Stable, human-readable name identifying which read model this purger targets. Recorded in the
   * GDPR audit log so each purge outcome is attributable to a specific source. Must be non-blank
   * and unique within the registered set of purgers.
   *
   * <p>The forget service reads this once, when it is built, before any key can be deleted, and
   * reports this purger under it in every forget; it never calls it during a forget. Two purgers
   * under one name are refused there: the service's builder throws {@link IllegalArgumentException}
   * naming them, as their outcomes and audit rows could not be told apart. A {@code name()} that
   * throws an exception, or returns {@code null} or a blank string, does not stop the build: the
   * purger is named by its class name in the outcome, the audit rows and the metric, with a WARN
   * naming the class, so two such instances of one class are refused as a shared name too. An
   * {@link Error} from it propagates from the builder. Return a stable, literal name: a class name
   * changes when the class is renamed or moved, and the audit trail should not.
   *
   * @return the purger name
   */
  String name();

  /**
   * Deletes or redacts all data tied to the given subject from this purger's source. Must be
   * idempotent: a subject with no matching rows, or a repeated purge, is a safe no-op.
   *
   * @param subjectId the data subject identifier; never null
   * @throws RuntimeException if the underlying store cannot be purged; the forget service audits
   *     the failure per purger and continues with the remaining purgers
   */
  void purge(SubjectId subjectId);
}
