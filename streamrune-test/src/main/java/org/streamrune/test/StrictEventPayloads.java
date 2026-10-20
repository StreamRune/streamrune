package org.streamrune.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.core.upcasting.UpcasterChain;

/**
 * Strict checks of event payloads for a test suite. The event stores read event payloads as a
 * tolerant reader: a property the event class does not declare is ignored. That keeps an older
 * replica reading what a newer release wrote, and it also hides two mistakes: an event class that
 * no longer writes what it reads, and an upcaster step that writes a misspelled key (the bound
 * event then carries a default instead of the upcast value). These methods bind strictly, with a
 * mapper configured like the event store's (Java time support, no crypto module), so either mistake
 * fails the test.
 *
 * <pre>{@code
 * StrictEventPayloads.assertRoundTrips(new OrderPlaced("o-1", Money.of(10)));
 *
 * OrderPlaced upcast =
 *     StrictEventPayloads.upcastStrictly(
 *         new OrderPlacedUpcaster(), Map.of("orderId", "o-1"), 1, OrderPlaced.class);
 * assertEquals("EUR", upcast.currency());
 * }</pre>
 */
public final class StrictEventPayloads {

  private static final ObjectMapper STRICT =
      new ObjectMapper().registerModule(new JavaTimeModule());

  private StrictEventPayloads() {}

  /**
   * Serializes {@code event}, binds the JSON strictly to its class and asserts the result equals
   * {@code event}.
   *
   * @param event the event (its class must implement {@code equals}, as a record does)
   * @param <E> the event type
   * @return the event bound back from its JSON
   * @throws AssertionError when the JSON does not bind strictly or binds to a different value
   */
  public static <E extends DomainEvent> E assertRoundTrips(E event) {
    Objects.requireNonNull(event, "event is required");
    @SuppressWarnings("unchecked")
    Class<E> type = (Class<E>) event.getClass();
    String json;
    E bound;
    try {
      json = STRICT.writeValueAsString(event);
      bound = STRICT.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new AssertionError(
          type.getName() + " does not round-trip strictly: " + e.getOriginalMessage(), e);
    }
    if (!event.equals(bound)) {
      throw new AssertionError(
          type.getName() + " does not round-trip: wrote " + json + ", read back " + bound);
    }
    return bound;
  }

  /**
   * Runs {@code upcaster} on {@code storedPayload} from {@code fromVersion} up to its current
   * version, as the event store does on read, and binds the result strictly to {@code eventClass}.
   *
   * @param upcaster the upcaster of the event type
   * @param storedPayload the payload as stored at {@code fromVersion}
   * @param fromVersion the stored schema version
   * @param eventClass the class the current release binds the type to
   * @param <E> the event type
   * @return the bound event
   * @throws AssertionError when the upcast payload carries a property {@code eventClass} does not
   *     declare, or does not bind for another reason
   */
  public static <E extends DomainEvent> E upcastStrictly(
      EventUpcaster upcaster,
      Map<String, Object> storedPayload,
      int fromVersion,
      Class<E> eventClass) {
    Objects.requireNonNull(upcaster, "upcaster is required");
    Map<String, Object> upcast =
        new UpcasterChain(List.of(upcaster))
            .upcast(upcaster.eventType(), new java.util.HashMap<>(storedPayload), fromVersion);
    try {
      return STRICT.convertValue(upcast, eventClass);
    } catch (IllegalArgumentException e) {
      throw new AssertionError(
          upcaster.eventType().name()
              + " upcast from version "
              + fromVersion
              + " does not bind strictly to "
              + eventClass.getName()
              + ": "
              + e.getMessage(),
          e);
    }
  }
}
