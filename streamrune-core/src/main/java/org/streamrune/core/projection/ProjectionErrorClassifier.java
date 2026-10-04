package org.streamrune.core.projection;

/**
 * Classifies a projection processing failure as {@link ProjectionErrorClass#TRANSIENT} or {@link
 * ProjectionErrorClass#POISON}. Consulted only by the {@link ProjectionErrorStrategy#DLQ} strategy:
 * TRANSIENT failures are retried with bounded backoff (dead-lettered only if the bound is
 * exhausted); POISON failures are dead-lettered after the first failure.
 */
@FunctionalInterface
public interface ProjectionErrorClassifier {

  /**
   * Classifies {@code error} as a transient infrastructure blip or a deterministic poison batch.
   */
  ProjectionErrorClass classify(Throwable error);

  /**
   * Default classifier: infrastructure/SQL/IO failures are {@link ProjectionErrorClass#TRANSIENT};
   * deterministic typed frames and everything unrecognized are {@link ProjectionErrorClass#POISON}.
   * Mirrors {@code SagaRunner}'s split — store I/O propagates and retries, orchestrator logic is
   * poison.
   *
   * <p>The cause chain is walked, because a projection's {@code process()} typically wraps a SQL
   * failure as an {@link org.streamrune.core.EventStoreException} or a {@code SQLException}-caused
   * runtime: a read-model DB failover blip therefore classifies TRANSIENT, while a {@code
   * NullPointerException} / mapping error from projection logic classifies POISON.
   *
   * <p>A CAS/lock conflict ({@link org.streamrune.core.OptimisticLockException} or {@link
   * org.streamrune.core.LockException}, deliberately <b>not</b> subtypes of {@link
   * org.streamrune.core.EventStoreException}) is also classified TRANSIENT. The atomic projection
   * offset guard throws {@code OptimisticLockException} on a benign already-applied re-delivery in
   * a split-brain window; treating it as POISON would spuriously dead-letter a batch that was in
   * fact already applied. As TRANSIENT it is retried-then-skipped — the projection recovers on the
   * next read of the advanced offset rather than dead-lettering.
   *
   * <p><b>The WHOLE chain is walked with the family's precedence</b> (mirroring {@code
   * ReadPoisonClassifier} and {@code SagaStateConversion.isDeterministic}), instead of
   * early-returning TRANSIENT on the first infrastructure-looking frame. The rules, in the order
   * each frame is tested:
   *
   * <ul>
   *   <li>{@link org.streamrune.core.crypto.SubjectForgottenException} anywhere ⇒ <b>POISON</b>
   *       (short-circuit). The subject was crypto-shredded by a GDPR erasure; no retry can ever
   *       succeed. It typically surfaces buried — {@code JdbcProjectionRepository} wraps the
   *       Jackson {@code JsonMappingException} carrying it into an {@code EventStoreException} —
   *       which is exactly why the old first-frame-ESE rule retried it forever.
   *   <li>{@link org.streamrune.core.EventDeserializationException} / {@link
   *       org.streamrune.core.UnknownEventTypeException} / {@link
   *       org.streamrune.core.crypto.CryptoMappingException} ⇒ flag <b>deterministic</b>, keep
   *       walking. The first is the store's typed frame for a stored payload that deterministically
   *       cannot be bound (it extends {@code EventStoreException}, so the old rule mis-binned it
   *       TRANSIENT); the second is a registry lookup that re-fails identically; the third is the
   *       crypto layer's deterministic verdict, raised both by {@code CryptoShreddingModule} (a
   *       mapping refusal) and by every shipped {@code CryptoEngine} (a verdict on the blob itself
   *       — see {@link org.streamrune.core.crypto.CryptoMappingException}).
   *   <li>a bare {@link org.streamrune.core.crypto.CryptoOperationException} (i.e. NOT its {@code
   *       CryptoMappingException} subtype) or a {@link java.sql.SQLException} ⇒ <b>TRANSIENT</b>
   *       (short-circuit). Engine/JDBC evidence outranks every deterministic marker: it reports a
   *       condition of the backend or the key (an outage, a DB blip, a disabled or inaccessible key
   *       an operator restores), not of the event, so the same event can succeed once that
   *       condition clears — even when it surfaced wrapped in a deterministic-looking outer frame.
   *   <li>a plain {@link org.streamrune.core.EventStoreException} / {@link
   *       org.streamrune.core.OptimisticLockException} / {@link org.streamrune.core.LockException}
   *       / {@link java.io.IOException} / {@link java.io.UncheckedIOException} ⇒ flag
   *       <b>transient</b>, keep walking. Weak evidence only: it must not override a deterministic
   *       typed frame (the typed frames always wrap a Jackson {@code IOException} cause), but a
   *       deeper {@code SubjectForgottenException} must still be reachable.
   * </ul>
   *
   * <p>After the walk: a flagged deterministic frame wins over weak transient evidence ⇒ POISON;
   * otherwise transient evidence ⇒ TRANSIENT; otherwise (projection logic bugs — NPE, mapping
   * errors) ⇒ POISON.
   */
  ProjectionErrorClassifier DEFAULT = ProjectionErrorClassifier::classifyDefault;

  /**
   * Implementation of {@link #DEFAULT}. Walks the whole cause chain with the precedence documented
   * on {@link #DEFAULT}.
   *
   * <p>The walk is depth-bounded: a self-referential or cyclic cause chain (e.g. {@code
   * a.initCause(b); b.initCause(a)}, or a JDK/library edge that returns a non-terminating {@code
   * getCause()}) would otherwise loop forever. The cap is far beyond any legitimate exception
   * chain, so a real chain is always classified fully; only a pathological cycle stops early (as
   * POISON — the safe default: an unclassifiable failure is retried at most zero times, never
   * silently treated as a recoverable blip).
   */
  private static ProjectionErrorClass classifyDefault(Throwable error) {
    boolean sawDeterministic = false;
    boolean sawTransient = false;
    int remaining = 128;
    for (Throwable t = error; t != null && remaining-- > 0; t = t.getCause()) {
      if (t instanceof org.streamrune.core.crypto.SubjectForgottenException) {
        return ProjectionErrorClass.POISON;
      }
      if (t instanceof org.streamrune.core.EventDeserializationException
          || t instanceof org.streamrune.core.UnknownEventTypeException
          || t instanceof org.streamrune.core.crypto.CryptoMappingException) {
        sawDeterministic = true;
      } else if (t instanceof org.streamrune.core.crypto.CryptoOperationException
          || t instanceof java.sql.SQLException) {
        // Fail-safe short-circuit: engine/JDBC evidence outranks every deterministic marker, so a
        // key-store outage that surfaced wrapped in a typed deterministic frame still retries.
        return ProjectionErrorClass.TRANSIENT;
      } else if (t instanceof org.streamrune.core.EventStoreException
          || t instanceof org.streamrune.core.OptimisticLockException
          || t instanceof org.streamrune.core.LockException
          || t instanceof java.io.IOException
          || t instanceof java.io.UncheckedIOException) {
        sawTransient = true;
      }
    }
    if (sawDeterministic) {
      return ProjectionErrorClass.POISON;
    }
    return sawTransient ? ProjectionErrorClass.TRANSIENT : ProjectionErrorClass.POISON;
  }
}
