package org.streamrune.core.saga;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.SagaType;

class SagaOriginTest {

  private static final SagaType TYPE = SagaType.of("x.S");
  private static final SagaId ID = SagaId.of("s-1");

  @Test
  void dispatchedBy_isHopZero() {
    SagaOrigin origin = SagaOrigin.dispatchedBy(TYPE, ID);

    assertEquals(TYPE, origin.sagaType());
    assertEquals(ID, origin.sagaId());
    assertEquals(0, origin.hop());
  }

  @Test
  void rejectsAMissingTypeAMissingIdAndANegativeHop() {
    assertEquals(
        "sagaType is required",
        assertThrows(IllegalArgumentException.class, () -> SagaOrigin.dispatchedBy(null, ID))
            .getMessage());
    assertEquals(
        "sagaId is required",
        assertThrows(IllegalArgumentException.class, () -> SagaOrigin.dispatchedBy(TYPE, null))
            .getMessage());

    assertEquals(
        "hop must not be negative",
        assertThrows(IllegalArgumentException.class, () -> new SagaOrigin(TYPE, ID, -1))
            .getMessage());
  }

  @Test
  void nextHop_keepsTypeAndIdAndAddsOne() {
    SagaOrigin direct = SagaOrigin.dispatchedBy(TYPE, ID);

    SagaOrigin relayed = direct.nextHop();

    assertEquals(new SagaOrigin(TYPE, ID, 1), relayed);
    assertEquals(new SagaOrigin(TYPE, ID, 2), relayed.nextHop());
    assertEquals(0, direct.hop(), "an origin is a value; taking the next hop changes nothing");
  }

  @Test
  void nextHop_atTheLargestHop_isRefusedAndSaysWhy() {
    SagaOrigin last = new SagaOrigin(TYPE, ID, Integer.MAX_VALUE);
    assertEquals(
        new SagaOrigin(TYPE, ID, Integer.MAX_VALUE),
        new SagaOrigin(TYPE, ID, Integer.MAX_VALUE - 1).nextHop());

    var ex = assertThrows(IllegalStateException.class, last::nextHop);

    assertEquals(
        "a saga origin at hop 2147483647 cannot be taken one hop further: the hop count is at its"
            + " limit",
        ex.getMessage());
  }

  @Test
  void writesThreeJsonKeysAndReadsThemBack() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SagaOrigin origin = SagaOrigin.dispatchedBy(TYPE, ID);

    String json = mapper.writeValueAsString(origin);

    assertEquals("{\"sagaType\":\"x.S\",\"sagaId\":\"s-1\",\"hop\":0}", json);
    assertEquals(origin, mapper.readValue(json, SagaOrigin.class));
    assertEquals(
        1,
        mapper
            .readValue("{\"sagaType\":\"x.S\",\"sagaId\":\"s-1\",\"hop\":1}", SagaOrigin.class)
            .hop());
  }

  @Test
  void correlationId_isTheReservedFormOfTheSagaId() {
    SagaOrigin origin = SagaOrigin.dispatchedBy(TYPE, SagaId.of("order-saga-42"));

    assertEquals(CorrelationId.of("streamrune:saga:order-saga-42"), origin.correlationId());
    assertEquals(
        origin.correlationId(),
        origin.nextHop().correlationId(),
        "the hop takes no part in it: every hop of one saga's work shares one correlation id");
    assertEquals(
        origin.correlationId(),
        SagaOrigin.dispatchedBy(SagaType.of("y.Other"), SagaId.of("order-saga-42")).correlationId(),
        "nor does the saga type");
  }

  @Test
  void correlationId_carriesTheSagaIdVerbatim() {
    // A saga id is bounded in length, not in charset, and is at most 255 characters long.
    String longest = "s".repeat(255);
    assertEquals(
        "streamrune:saga:" + longest,
        SagaOrigin.dispatchedBy(TYPE, SagaId.of(longest)).correlationId().value());
    assertEquals(
        "streamrune:saga:order\r\nsaga-1",
        SagaOrigin.dispatchedBy(TYPE, SagaId.of("order\r\nsaga-1")).correlationId().value());
  }

  @Test
  void theJsonStillHasThreeKeys() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SagaOrigin origin = new SagaOrigin(TYPE, SagaId.of("order-saga-42"), 2);

    String json = mapper.writeValueAsString(origin);

    assertEquals("{\"sagaType\":\"x.S\",\"sagaId\":\"order-saga-42\",\"hop\":2}", json);
    assertEquals(origin, mapper.readValue(json, SagaOrigin.class));
  }
}
