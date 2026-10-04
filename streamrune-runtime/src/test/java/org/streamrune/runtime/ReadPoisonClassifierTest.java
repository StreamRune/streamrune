package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;

/** Unit tests for {@link ReadPoisonClassifier}. */
class ReadPoisonClassifierTest {

  @Test
  void unknownEventType_isPoison() {
    var e = new UnknownEventTypeException("event type", "GoneEvent", List.of("KnownEvent"));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }

  @Test
  void unknownEventType_wrappedDeeper_isPoison() {
    var e =
        new RuntimeException(
            "outer",
            new UnknownEventTypeException("event type", "GoneEvent", List.of("KnownEvent")));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }

  @Test
  void deserializationIoFailure_isPoison() {
    // Mirrors PostgresEventStore.toEnvelope wrapping a Jackson IOException as EventStoreException.
    var e =
        new EventStoreException("Failed to deserialize event data", new IOException("bad json"));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }

  @Test
  void uncheckedIoFailure_isPoison() {
    var e = new UncheckedIOException(new IOException("boom"));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }

  @Test
  void sqlBackedStoreFailure_isTransient() {
    // Mirrors PostgresEventStore.readGlobalStream wrapping a SQLException as EventStoreException.
    var e =
        new EventStoreException("Failed to read global stream", new SQLException("conn refused"));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isFalse();
  }

  @Test
  void sqlWrappingIoSocketError_isTransient_sqlCheckedFirst() {
    // Some JDBC drivers wrap a socket IOException inside a SQLException — a DB connection blip must
    // stay TRANSIENT, decided by the SQLException before the IOException rule is reached.
    var e =
        new EventStoreException(
            "Failed to read global stream",
            new SQLException("socket write error", new IOException("broken pipe")));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isFalse();
  }

  @Test
  void plainRuntimeException_isTransientByDefault() {
    // An unrecognized read failure defaults to transient (keep retrying) — never a silent kill.
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(new RuntimeException("connection")))
        .isFalse();
  }

  @Test
  void bareEventStoreExceptionWithNoCause_isTransient() {
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(new EventStoreException("opaque")))
        .isFalse();
  }

  @Test
  void selfReferentialCauseChain_doesNotLoopForever_defaultsTransient() {
    var a = new RuntimeException("a");
    var b = new RuntimeException("b", a);
    a.initCause(b); // cycle
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(a)).isFalse();
  }

  // ---- A transient crypto outage during nested-@Encrypted decryption surfaces WRAPPED
  // by Jackson into a JsonMappingException (an IOException). The early-return-on-IOException rule
  // classified it POISON without ever looking underneath, terminally halting projections during a
  // routine Vault/KMS blip. Engine/JDBC evidence must outrank the IO rule, exactly as
  // SagaStateConversion.isDeterministic already decides. ----

  @Test
  void transientCryptoUnderMappingIo_isTransient() {
    var e =
        new EventStoreException(
            "Failed to deserialize event data",
            new JsonMappingException(
                (java.io.Closeable) null,
                "decrypt",
                new CryptoOperationException("vault outage during nested @Encrypted decrypt")));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e))
        .as("a key-store outage heals by itself — it must keep retrying, never halt terminally")
        .isFalse();
  }

  @Test
  void bareCryptoOperationFailure_isTransient() {
    var e = new EventStoreException("read failed", new CryptoOperationException("KMS timeout"));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isFalse();
  }

  @Test
  void cryptoMappingRefusalUnderMappingIo_isPoison() {
    // The DETERMINISTIC crypto subtype (a shape/annotation refusal, a constructor rejecting the
    // [REDACTED] tombstone) re-fails identically forever — it stays poison even under the same
    // JsonMappingException wrapping.
    var e =
        new EventStoreException(
            "Failed to deserialize event data",
            new JsonMappingException(
                (java.io.Closeable) null,
                "map",
                new CryptoMappingException("constructor rejects tombstone")));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }

  @Test
  void subjectForgottenAnywhere_isPoison() {
    // Mirrors SagaStateConversion: a crypto-shredded subject can never be read back by retrying.
    var e =
        new EventStoreException(
            "read failed",
            new JsonMappingException(
                (java.io.Closeable) null,
                "encrypt",
                new SubjectForgottenException("subject erased under GDPR")));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }

  // ---- A user upcaster's deterministic throw (NPE/CCE) now surfaces wrapped in the
  // typed EventDeserializationException. The classifier must treat that frame as
  // poison; before, the raw NPE matched no rule, classified TRANSIENT, and
  // PollingEventSubscription even RESET the poison counter — an unbounded retry wedge. ----

  @Test
  void typedDeserializationFrame_isPoison() {
    var e =
        new EventDeserializationException(
            "Failed to deserialize event data for stream: cart-1",
            new NullPointerException("upcaster bug"));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }

  @Test
  void typedDeserializationFrame_wrappedDeeper_isPoison() {
    var e =
        new RuntimeException(
            "outer",
            new EventDeserializationException(
                "Failed to deserialize event data for stream: cart-1",
                new IllegalArgumentException("malformed upcaster output")));
    assertThat(ReadPoisonClassifier.isDeterministicReadPoison(e)).isTrue();
  }
}
