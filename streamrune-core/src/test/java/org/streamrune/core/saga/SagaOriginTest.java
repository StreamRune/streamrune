package org.streamrune.core.saga;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
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
}
