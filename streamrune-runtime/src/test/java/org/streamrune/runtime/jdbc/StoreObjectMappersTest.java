package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.test.InMemoryCryptoEngine;

class StoreObjectMappersTest {

  /** One {@code @Encrypted} field keyed off {@code customerId}. */
  public record Customer(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  /** A value with an {@link Instant}, to see how dates are written. */
  public record Stamped(Instant at) {}

  private static final Instant AT = Instant.parse("2026-11-27T10:15:30.500Z");

  @Test
  void metricsAreRequired() {
    var events =
        assertThrows(
            NullPointerException.class, () -> StoreObjectMappers.eventsAndSagas(null, null));
    assertThat(events.getMessage()).isEqualTo("metrics is required");

    var readModels =
        assertThrows(NullPointerException.class, () -> StoreObjectMappers.readModels(null, null));
    assertThat(readModels.getMessage()).isEqualTo("metrics is required");
  }

  @Test
  void eventsAndSagasWritesDatesAsTimestamps() throws Exception {
    ObjectMapper mapper = StoreObjectMappers.eventsAndSagas(null, StreamRuneMetrics.NOOP);

    String json = mapper.writeValueAsString(new Stamped(AT));

    assertThat(json).isEqualTo("{\"at\":" + AT.getEpochSecond() + ".500000000}");
    assertThat(mapper.readValue(json, Stamped.class).at()).isEqualTo(AT);
  }

  @Test
  void readModelsWritesDatesAsIsoText() throws Exception {
    ObjectMapper mapper = StoreObjectMappers.readModels(null, StreamRuneMetrics.NOOP);

    String json = mapper.writeValueAsString(new Stamped(AT));

    assertThat(json).isEqualTo("{\"at\":\"2026-11-27T10:15:30.500Z\"}");
    assertThat(mapper.readValue(json, Stamped.class).at()).isEqualTo(AT);
  }

  @Test
  void registersCryptoOnlyWithAnEngine() throws Exception {
    var customer = new Customer("customer-1", "ada@example.com");
    var engine = new InMemoryCryptoEngine();

    for (ObjectMapper encrypting :
        new ObjectMapper[] {
          StoreObjectMappers.eventsAndSagas(engine, StreamRuneMetrics.NOOP),
          StoreObjectMappers.readModels(engine, StreamRuneMetrics.NOOP)
        }) {
      String json = encrypting.writeValueAsString(customer);
      assertThat(json).contains("customer-1").doesNotContain("ada@example.com");
      assertThat(encrypting.readValue(json, Customer.class)).isEqualTo(customer);
    }

    for (ObjectMapper blind :
        new ObjectMapper[] {
          StoreObjectMappers.eventsAndSagas(null, StreamRuneMetrics.NOOP),
          StoreObjectMappers.readModels(null, StreamRuneMetrics.NOOP)
        }) {
      assertThat(blind.writeValueAsString(customer)).contains("\"ada@example.com\"");
    }
  }

  @Test
  void eachCallBuildsAnIndependentMapper() {
    assertThat(StoreObjectMappers.eventsAndSagas(null, StreamRuneMetrics.NOOP))
        .isNotSameAs(StoreObjectMappers.eventsAndSagas(null, StreamRuneMetrics.NOOP));
    assertThat(StoreObjectMappers.readModels(null, StreamRuneMetrics.NOOP))
        .isNotSameAs(StoreObjectMappers.readModels(null, StreamRuneMetrics.NOOP));
  }
}
