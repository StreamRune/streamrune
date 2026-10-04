package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class IdTypesTest {

  private final ObjectMapper mapper = new ObjectMapper();

  /** A NUL byte — breaks the jsonb metadata write outright if it is ever persisted. */
  private static final String NUL = String.valueOf((char) 0);

  @Test
  void eventIdRoundTrips() throws Exception {
    var id = EventId.of("evt_123");
    assertEquals("evt_123", id.value());
    assertEquals("evt_123", id.toString());
    assertEquals("\"evt_123\"", mapper.writeValueAsString(id));
    assertEquals(id, mapper.readValue("\"evt_123\"", EventId.class));
    assertThrows(IllegalArgumentException.class, () -> EventId.of(" "));
    assertThrows(IllegalArgumentException.class, () -> new EventId(null));
  }

  @Test
  void commandIdRoundTrips() throws Exception {
    var id = CommandId.of("cmd_123");
    assertEquals("cmd_123", id.value());
    assertEquals("cmd_123", id.toString());
    assertEquals(id, mapper.readValue("\"cmd_123\"", CommandId.class));
    assertThrows(IllegalArgumentException.class, () -> new CommandId(""));
  }

  @Test
  void traceIdRoundTrips() throws Exception {
    var id = TraceId.of("trace_1");
    assertEquals("trace_1", id.value());
    assertEquals("trace_1", id.toString());
    assertEquals(id, mapper.readValue("\"trace_1\"", TraceId.class));
    assertThrows(IllegalArgumentException.class, () -> TraceId.of(null));
  }

  @Test
  void spanIdRoundTrips() throws Exception {
    var id = SpanId.of("span_1");
    assertEquals("span_1", id.toString());
    assertEquals(id, mapper.readValue("\"span_1\"", SpanId.class));
    assertThrows(IllegalArgumentException.class, () -> SpanId.of(""));
  }

  @Test
  void correlationIdRoundTrips() throws Exception {
    var id = CorrelationId.of("corr_1");
    assertEquals("corr_1", id.toString());
    assertEquals(id, mapper.readValue("\"corr_1\"", CorrelationId.class));
    assertThrows(IllegalArgumentException.class, () -> CorrelationId.of(" "));
  }

  @Test
  void causationIdRoundTrips() throws Exception {
    var id = CausationId.of("caus_1");
    assertEquals("caus_1", id.toString());
    assertEquals(id, mapper.readValue("\"caus_1\"", CausationId.class));
    assertThrows(IllegalArgumentException.class, () -> CausationId.of(null));
  }

  @Test
  void userIdRoundTrips() throws Exception {
    var id = UserId.of("user_1");
    assertEquals("user_1", id.toString());
    assertEquals(id, mapper.readValue("\"user_1\"", UserId.class));
    assertThrows(IllegalArgumentException.class, () -> UserId.of(""));
  }

  // ==== Header-sourced ids and baggage values ====
  //
  // X-Correlation-Id and X-Trace-Id are unauthenticated client input that each framework filter
  // turns into a CorrelationId / TraceId, which are then persisted into the append-only
  // event_stream.metadata of every event AND into audit_log.correlation_id (TEXT — unbounded). The
  // bound on those values is real, but it belongs at INGRESS — StreamRuneContext.RequestContext,
  // pinned in StreamRuneContextTest — and NOT in these constructors, which also run every time
  // Jackson rebuilds EventMetadata from a stored row. A constraint here would reject events already
  // written through a path that never crosses the ingress door (a context built with the plain
  // constructor, a saga-derived correlation id, application-built EventMetadata), making those
  // aggregates permanently unloadable and halting every projection at that offset, while
  // preventing no write whatsoever.

  @Test
  void correlationIdReconstructsAnOversizedStoredValueVerbatim() {
    var oversized = "c".repeat(4096);
    assertDoesNotThrow(() -> CorrelationId.of(oversized));
    assertEquals(oversized, CorrelationId.of(oversized).value());
  }

  @Test
  void correlationIdDeserializesStoredControlCharacters() throws Exception {
    // Reconstruction is not validation: whatever is in the log must come back out of it. (A NUL
    // cannot reach a jsonb column, but CR/LF can: the constructor accepts both.)
    assertDoesNotThrow(() -> CorrelationId.of("corr" + NUL + "_1"));
    assertEquals("corr\r\n_1", mapper.readValue("\"corr\\r\\n_1\"", CorrelationId.class).value());
  }

  @Test
  void traceIdReconstructsOversizedAndControlBearingStoredValues() {
    assertDoesNotThrow(() -> TraceId.of("t".repeat(4096)));
    assertDoesNotThrow(() -> TraceId.of("trace" + NUL + "_1"));
    // A W3C trace id is 32 hex characters; the ingress cap is far above any legitimate value and
    // matches the VARCHAR(255) columns these ids are persisted into.
    assertDoesNotThrow(() -> TraceId.of("0af7651916cd43dd8448eb211c80319c"));
  }

  @Test
  void blanknessRemainsTheseTypesOwnInvariant() {
    // The one rule that is genuinely about the VALUE and not about where it came from: a blank id
    // was never writable, so a blank id on the read path is corruption, not history.
    assertThrows(IllegalArgumentException.class, () -> CorrelationId.of(" "));
    assertThrows(IllegalArgumentException.class, () -> TraceId.of(" "));
  }
}
