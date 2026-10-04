package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * The event store's decrypt mapper must be built through the metrics-aware {@link
 * CryptoShreddingModule} constructor, so a mass crypto-redaction on the event-replay / projection
 * decrypt path (a wiped or misconfigured key store) emits the {@code streamrune.crypto
 * .subject_redacted} alerting signal instead of being silent.
 *
 * <p>Before the fix, {@code PostgresEventStore} built its mapper inline with the single-arg {@code
 * new CryptoShreddingModule(cryptoEngine)} (NOOP metrics) on every production construction path, so
 * the documented GDPR-outage counter stayed permanently 0. This test drives the mapper through the
 * production factory {@link PostgresEventStore#createObjectMapper(org.streamrune.core.crypto
 * .CryptoEngine, StreamRuneMetrics)} — the exact code {@code create()} and {@code
 * builder().build()} now share — with a recording metrics collector, and asserts the counter fires.
 */
class PostgresEventStoreCryptoMetricsTest {

  /** Event with one {@code @Encrypted} PII field, keyed off {@code userId}. */
  record UserRegistered(String userId, @Encrypted(subjectId = "userId") String email) {
    @JsonCreator
    UserRegistered(@JsonProperty("userId") String userId, @JsonProperty("email") String email) {
      this.userId = userId;
      this.email = email;
    }
  }

  /** Recording metrics double: counts the redaction signal. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final AtomicInteger redacted = new AtomicInteger();

    @Override
    public void recordSubjectRedacted() {
      redacted.incrementAndGet();
    }
  }

  @Test
  void eventStoreProductionMapper_emitsSubjectRedactedMetric_onKeyMissingDecrypt()
      throws Exception {
    var metrics = new RecordingMetrics();
    var engine = new InMemoryCryptoEngine();

    // Build the mapper via the PRODUCTION event-store factory path (shared by create()/builder()).
    ObjectMapper mapper = PostgresEventStore.createObjectMapper(engine, metrics);

    var event = new UserRegistered("user-1", "alice@example.com");
    String json = mapper.writeValueAsString(event); // encrypt
    engine.deleteKey(SubjectId.of("user-1")); // key gone (GDPR forget / wiped key store)
    var restored = mapper.readValue(json, UserRegistered.class); // decrypt -> [REDACTED]

    assertThat(restored.email())
        .as("a missing key redacts the field on the event-store decrypt path")
        .isEqualTo(CryptoShreddingModule.REDACTED);
    assertThat(metrics.redacted.get())
        .as("the event-store production decrypt path must feed subject_redacted, not NOOP")
        .isGreaterThanOrEqualTo(1);
  }
}
