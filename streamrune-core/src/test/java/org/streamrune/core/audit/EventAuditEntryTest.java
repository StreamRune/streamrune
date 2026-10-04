package org.streamrune.core.audit;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

class EventAuditEntryTest {

  private static final AggregateType TYPE = AggregateType.of("audit");

  private static final EventId EVENT_ID = EventId.of("evt-1");
  private static final EventType EVENT_TYPE = new EventType("OrderPlaced");
  private static final StreamId STREAM_ID = StreamId.of(TYPE, AggregateId.of("order-42"));
  private static final Version VERSION = new Version(1);
  private static final CommandId COMMAND_ID = CommandId.of("cmd-1");
  private static final CorrelationId CORRELATION_ID = CorrelationId.of("corr-1");
  private static final CausationId CAUSATION_ID = CausationId.of("cause-1");
  private static final UserId USER_ID = UserId.of("user-42");
  private static final Instant NOW = Instant.now();

  @Test
  void validEntryIsCreated() {
    var entry =
        new EventAuditEntry(
            EVENT_ID,
            EVENT_TYPE,
            STREAM_ID,
            VERSION,
            COMMAND_ID,
            CORRELATION_ID,
            CAUSATION_ID,
            USER_ID,
            NOW);

    assertEquals(EVENT_ID, entry.eventId());
    assertEquals(EVENT_TYPE, entry.eventType());
    assertEquals(STREAM_ID, entry.streamId());
    assertEquals(VERSION, entry.version());
    assertEquals(COMMAND_ID, entry.commandId());
    assertEquals(CORRELATION_ID, entry.correlationId());
    assertEquals(CAUSATION_ID, entry.causationId());
    assertEquals(USER_ID, entry.userId());
    assertEquals(NOW, entry.occurredAt());
  }

  @Test
  void causationIdIsNullable() {
    var entry =
        new EventAuditEntry(
            EVENT_ID,
            EVENT_TYPE,
            STREAM_ID,
            VERSION,
            COMMAND_ID,
            CORRELATION_ID,
            null,
            USER_ID,
            NOW);

    assertNull(entry.causationId());
  }

  @Test
  void userIdIsNullable() {
    var entry =
        new EventAuditEntry(
            EVENT_ID,
            EVENT_TYPE,
            STREAM_ID,
            VERSION,
            COMMAND_ID,
            CORRELATION_ID,
            CAUSATION_ID,
            null,
            NOW);

    assertNull(entry.userId());
  }

  @Test
  void rejectsNullEventId() {
    assertThrows(
        NullPointerException.class,
        () ->
            new EventAuditEntry(
                null,
                EVENT_TYPE,
                STREAM_ID,
                VERSION,
                COMMAND_ID,
                CORRELATION_ID,
                CAUSATION_ID,
                USER_ID,
                NOW));
  }

  @Test
  void rejectsNullEventType() {
    assertThrows(
        NullPointerException.class,
        () ->
            new EventAuditEntry(
                EVENT_ID,
                null,
                STREAM_ID,
                VERSION,
                COMMAND_ID,
                CORRELATION_ID,
                CAUSATION_ID,
                USER_ID,
                NOW));
  }

  @Test
  void rejectsNullStreamId() {
    assertThrows(
        NullPointerException.class,
        () ->
            new EventAuditEntry(
                EVENT_ID,
                EVENT_TYPE,
                null,
                VERSION,
                COMMAND_ID,
                CORRELATION_ID,
                CAUSATION_ID,
                USER_ID,
                NOW));
  }

  @Test
  void rejectsNullVersion() {
    assertThrows(
        NullPointerException.class,
        () ->
            new EventAuditEntry(
                EVENT_ID,
                EVENT_TYPE,
                STREAM_ID,
                null,
                COMMAND_ID,
                CORRELATION_ID,
                CAUSATION_ID,
                USER_ID,
                NOW));
  }

  @Test
  void rejectsNullCommandId() {
    assertThrows(
        NullPointerException.class,
        () ->
            new EventAuditEntry(
                EVENT_ID,
                EVENT_TYPE,
                STREAM_ID,
                VERSION,
                null,
                CORRELATION_ID,
                CAUSATION_ID,
                USER_ID,
                NOW));
  }

  @Test
  void rejectsNullCorrelationId() {
    assertThrows(
        NullPointerException.class,
        () ->
            new EventAuditEntry(
                EVENT_ID,
                EVENT_TYPE,
                STREAM_ID,
                VERSION,
                COMMAND_ID,
                null,
                CAUSATION_ID,
                USER_ID,
                NOW));
  }

  @Test
  void rejectsNullOccurredAt() {
    assertThrows(
        NullPointerException.class,
        () ->
            new EventAuditEntry(
                EVENT_ID,
                EVENT_TYPE,
                STREAM_ID,
                VERSION,
                COMMAND_ID,
                CORRELATION_ID,
                CAUSATION_ID,
                USER_ID,
                null));
  }
}
