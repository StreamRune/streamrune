package org.streamrune.core.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;

/** Cause-chain classification unit tests for {@link ProjectionErrorClassifier#DEFAULT}. */
class ProjectionErrorClassifierTest {

  private final ProjectionErrorClassifier classifier = ProjectionErrorClassifier.DEFAULT;

  @Test
  void eventStoreException_isTransient() {
    assertThat(classifier.classify(new EventStoreException("db down")))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void sqlException_isTransient() {
    assertThat(classifier.classify(new SQLException("connection reset")))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void ioException_isTransient() {
    assertThat(classifier.classify(new IOException("broken pipe")))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void uncheckedIoException_isTransient() {
    assertThat(classifier.classify(new UncheckedIOException(new IOException("nope"))))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void nestedSqlExceptionUnderRuntime_isTransient() {
    // A projection wraps a SQL failure in a plain RuntimeException — the chain must still resolve
    // to TRANSIENT so a read-model failover blip is retried, not dead-lettered.
    var wrapped = new RuntimeException("processing failed", new SQLException("timeout"));
    assertThat(classifier.classify(wrapped)).isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void deeplyNestedInfraCause_isTransient() {
    var deep =
        new IllegalStateException(
            "outer", new RuntimeException("mid", new EventStoreException("inner store error")));
    assertThat(classifier.classify(deep)).isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void optimisticLockException_isTransient() {
    // The atomic projection offset guard throws OptimisticLockException on a
    // benign already-applied re-delivery. Under the DLQ strategy that must be TRANSIENT (retried
    // then skipped once the advanced offset is re-read), NOT POISON — a spurious dead-letter of a
    // batch that was already applied.
    assertThat(classifier.classify(new OptimisticLockException("projection offset advanced")))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void lockException_isTransient() {
    assertThat(classifier.classify(new LockException("could not acquire advisory lock")))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void lockExceptionSubtype_isTransient() {
    // LockException documents that shipped lockers may throw an aggregate-scoped subtype; the
    // instanceof walk must classify those TRANSIENT too, not just the exact type.
    class StripedLockException extends LockException {
      StripedLockException(String m) {
        super(m);
      }
    }
    assertThat(classifier.classify(new StripedLockException("striped lock busy")))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void nestedOptimisticLockUnderRuntime_isTransient() {
    var wrapped =
        new RuntimeException("apply failed", new OptimisticLockException("offset advanced"));
    assertThat(classifier.classify(wrapped)).isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void nullPointerException_isPoison() {
    assertThat(classifier.classify(new NullPointerException("bad mapping")))
        .isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void plainRuntimeException_isPoison() {
    assertThat(classifier.classify(new RuntimeException("boom")))
        .isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void nonInfraCauseChain_isPoison() {
    var chain = new IllegalArgumentException("bad", new IllegalStateException("also bad"));
    assertThat(classifier.classify(chain)).isEqualTo(ProjectionErrorClass.POISON);
  }

  // ── The walk must resolve the FAMILY precedence (mirroring
  // ReadPoisonClassifier and SagaStateConversion.isDeterministic), not stop at the first
  // EventStoreException frame. ──

  @Test
  void subjectForgottenBuriedUnderEventStoreException_isPoison() {
    // JdbcProjectionRepository wraps a Jackson JsonMappingException carrying a
    // SubjectForgottenException (a projection save touching a GDPR-erased subject) into an
    // EventStoreException. The old walk returned TRANSIENT on that first ESE frame — an endless
    // retry of a failure no retry can ever clear.
    var wrapped =
        new EventStoreException(
            "Failed to save projection",
            new IOException(
                "jackson mapping", new SubjectForgottenException("subject key deleted")));
    assertThat(classifier.classify(wrapped)).isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void subjectForgottenTopLevel_isPoison() {
    assertThat(classifier.classify(new SubjectForgottenException("subject key deleted")))
        .isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void eventDeserializationException_isPoison() {
    // The store's typed frame for a stored payload that deterministically cannot be bound. It
    // extends EventStoreException, so the old first-frame ESE rule binned it TRANSIENT — an
    // endless retry against stored bytes that re-fail identically forever.
    var typed =
        new EventDeserializationException(
            "Failed to deserialize event data", new IOException("unmappable payload"));
    assertThat(classifier.classify(typed)).isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void unknownEventTypeException_isPoison() {
    // A persisted type name with no registered class re-fails identically on every retry.
    var unknown =
        new UnknownEventTypeException("event type", "RetiredEvent", java.util.List.of("Kept"));
    assertThat(classifier.classify(unknown)).isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void cryptoMappingException_isPoison() {
    // The crypto layer's deterministic mapping refusal (e.g. a record constructor rejecting the
    // post-forget [REDACTED] tombstone) — minted only by CryptoShreddingModule, never by an
    // engine, so it is always deterministic.
    assertThat(classifier.classify(new CryptoMappingException("constructor rejected tombstone")))
        .isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void bareCryptoOperationException_isTransient() {
    // A bare CryptoOperationException is engine-raised evidence about the key store or the key (a
    // Vault/KMS blip, a disabled key), not about the event. The old rule matched none of the six
    // frames, so the first routine vault hiccup dead-lettered the batch as POISON.
    assertThat(classifier.classify(new CryptoOperationException("KMS timeout")))
        .isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void wrappedBareCryptoOperationException_isTransient() {
    var wrapped =
        new RuntimeException(
            "process failed",
            new IOException("jackson mapping", new CryptoOperationException("vault outage")));
    assertThat(classifier.classify(wrapped)).isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void transientEngineEvidenceUnderDeterministicFrame_isTransient() {
    // Family precedence (ReadPoisonClassifier / SagaStateConversion): engine/JDBC evidence
    // anywhere in the chain outranks every deterministic-looking outer frame — a key-store outage
    // that surfaced wrapped in the store's typed deterministic frame must still retry.
    var chain =
        new EventDeserializationException(
            "Failed to deserialize event data", new CryptoOperationException("KMS outage"));
    assertThat(classifier.classify(chain)).isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void sqlEvidenceUnderDeterministicFrame_isTransient() {
    var chain =
        new EventDeserializationException(
            "Failed to deserialize event data",
            new IOException("socket read", new SQLException("connection reset")));
    assertThat(classifier.classify(chain)).isEqualTo(ProjectionErrorClass.TRANSIENT);
  }

  @Test
  void deterministicFrameOutranksItsOwnIoCause_isPoison() {
    // The typed deterministic frame ALWAYS wraps an IOException (Jackson parse/bind failures are
    // IOExceptions) — the weak IO-transient evidence underneath must NOT override the typed frame,
    // or every EventDeserializationException would classify TRANSIENT and the fix would vanish.
    var chain =
        new RuntimeException(
            "process failed",
            new EventDeserializationException(
                "unmappable stored payload", new IOException("unrecognized field")));
    assertThat(classifier.classify(chain)).isEqualTo(ProjectionErrorClass.POISON);
  }

  @Test
  void cyclicCauseChain_terminates_andIsPoison() {
    // A two-node cycle: a → b → a. A naive unbounded getCause() walk would loop forever; the
    // depth-bounded walk must terminate. Neither node is infrastructure, so the classification is
    // POISON. assertTimeoutPreemptively fails the test if the walk hangs (the pre-fix behavior).
    var a = new RuntimeException("a");
    var b = new RuntimeException("b");
    a.initCause(b);
    b.initCause(a);

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> assertThat(classifier.classify(a)).isEqualTo(ProjectionErrorClass.POISON));
  }

  @Test
  void transientCauseInACycle_isStillTransient() {
    // Even inside a cycle, an infrastructure cause reached before the depth cap must classify
    // TRANSIENT — the guard bounds the walk without changing what a reachable infra cause means.
    var infra = new EventStoreException("db down");
    var wrapper = new RuntimeException("wrap", infra);
    // Close the loop back to wrapper so the chain would never terminate on its own.
    infra.initCause(wrapper);

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> assertThat(classifier.classify(wrapper)).isEqualTo(ProjectionErrorClass.TRANSIENT));
  }
}
