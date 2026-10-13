package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Query;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CorrelationId;

class InMemoryQueryBusTest {

  record GetById(String id) implements Query<ItemView> {}

  record GetAll() implements Query<ItemView> {}

  record ItemView(String id, String name) {}

  @Test
  void shouldDispatchQueryToRegisteredHandler() {
    var bus =
        InMemoryQueryBus.builder()
            .register(GetById.class, q -> new ItemView(q.id(), "Item-" + q.id()))
            .build();

    ItemView result = bus.dispatch(new GetById("42"));

    assertEquals("42", result.id());
    assertEquals("Item-42", result.name());
  }

  @Test
  void shouldSupportMultipleHandlers() {
    var bus =
        InMemoryQueryBus.builder()
            .register(GetById.class, q -> new ItemView(q.id(), "item"))
            .register(GetAll.class, q -> new ItemView("*", "all"))
            .build();

    ItemView byId = bus.dispatch(new GetById("1"));
    ItemView all = bus.dispatch(new GetAll());

    assertEquals("1", byId.id());
    assertEquals("item", byId.name());
    assertEquals("*", all.id());
    assertEquals("all", all.name());
  }

  @Test
  void shouldThrowForUnregisteredQuery() {
    var bus = InMemoryQueryBus.builder().build();

    var ex = assertThrows(IllegalArgumentException.class, () -> bus.dispatch(new GetById("1")));

    assertTrue(ex.getMessage().contains("GetById"));
    assertTrue(ex.getMessage().contains("builder"), "Error message should guide to the fix");
  }

  @Test
  void shouldThrowForNullQuery() {
    var bus = InMemoryQueryBus.builder().build();

    assertThrows(IllegalArgumentException.class, () -> bus.dispatch(null));
  }

  @Test
  void shouldThrowForDuplicateHandlerRegistration() {
    var builder =
        InMemoryQueryBus.builder().register(GetById.class, q -> new ItemView(q.id(), "first"));

    assertThrows(
        IllegalArgumentException.class,
        () -> builder.register(GetById.class, q -> new ItemView(q.id(), "second")));
  }

  record FulfillmentState(SagaStatus status) implements SagaState {}

  private static StreamRuneContext.RequestContext contextWith(CorrelationId correlationId) {
    return new StreamRuneContext.RequestContext(null, null, correlationId, Instant.now(), Map.of());
  }

  @Test
  void aQueryUnderAReservedCorrelationIdThatIsNotAnOrigins_isRefusedBeforeItsHandlerRuns() {
    // As the production query buses refuse it: a context of the test's own that carries a saga's
    // reserved correlation id, with no saga origin bound.
    AtomicInteger handled = new AtomicInteger();
    var bus =
        InMemoryQueryBus.builder()
            .register(
                GetById.class,
                q -> {
                  handled.incrementAndGet();
                  return new ItemView(q.id(), "item");
                })
            .build();

    assertThrows(
        ReservedCorrelationIdException.class,
        () ->
            ScopedValue.where(
                    StreamRuneContext.CURRENT,
                    contextWith(CorrelationId.of("streamrune:saga:order-saga-42")))
                .run(() -> bus.dispatch(new GetById("1"))));
    assertThrows(
        ReservedCorrelationIdException.class,
        () ->
            ScopedValue.where(
                    StreamRuneContext.CURRENT,
                    contextWith(CorrelationId.of("streamrune:saga:order-saga-42")))
                .run(() -> bus.dispatch(new GetAll())),
        "refused before the handler lookup, like the command buses");

    assertEquals(0, handled.get());
  }

  @Test
  void aQueryInsideASagaDispatch_isDispatched() {
    SagaOrigin origin =
        SagaEnvelopes.metadataCausedBy(FulfillmentState.class, SagaId.of("order-saga-42"))
            .sagaOrigin();
    var bus =
        InMemoryQueryBus.builder()
            .register(GetById.class, q -> new ItemView(q.id(), "item"))
            .build();

    ItemView view =
        ScopedValue.where(StreamRuneContext.CURRENT, contextWith(origin.correlationId()))
            .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(null, origin))
            .call(() -> bus.dispatch(new GetById("7")));

    assertEquals("7", view.id());
  }
}
