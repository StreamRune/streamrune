package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StoredLength;
import org.streamrune.core.types.SubscriptionName;

/**
 * Every value core bounds to the width of the column that stores it: a value of exactly the width
 * is accepted, counted in code points; one character more is refused with a message that states the
 * length and the maximum and does not echo the value.
 */
class StoredValueBoundsTest {

  private static final String SUPPLEMENTARY = "😀";

  static Stream<Arguments> bounds() {
    return Stream.of(
        bound("CommandId", CommandId.MAX_LENGTH, "commandId", CommandId::of),
        bound("ProjectionName", ProjectionName.MAX_LENGTH, "projectionName", ProjectionName::of),
        bound(
            "SubscriptionName",
            SubscriptionName.MAX_LENGTH,
            "subscriptionName",
            SubscriptionName::of),
        bound("SagaType", SagaType.MAX_LENGTH, "SagaType value", SagaType::of),
        bound("OutboxEntryId", OutboxEntryId.MAX_LENGTH, "OutboxEntryId value", OutboxEntryId::of),
        bound(
            "OutboxEntry.payloadType",
            OutboxEntry.MAX_PAYLOAD_TYPE_LENGTH,
            "payloadType",
            v -> OutboxEntry.pending(OutboxEntryId.of("e-1"), "{}", v)),
        bound(
            "OutboxStore.requireSkippedBy",
            OutboxStore.MAX_SKIPPED_BY_LENGTH,
            "skippedBy",
            OutboxStore::requireSkippedBy),
        bound(
            "OutboxEntry.SkipRecord",
            OutboxStore.MAX_SKIPPED_BY_LENGTH,
            "skippedBy",
            v -> new OutboxEntry.SkipRecord(Instant.EPOCH, v, "reason")),
        bound(
            "EventStore.requireStateType",
            EventStore.MAX_STATE_TYPE_LENGTH,
            "stateType",
            EventStore::requireStateType),
        bound(
            "SimpleEventTypeRegistry.registerState",
            EventStore.MAX_STATE_TYPE_LENGTH,
            "stateType",
            v -> SimpleEventTypeRegistry.builder().registerState(v, Object.class)),
        bound(
            "DeadLetterPublishRequest.commandType",
            CommandInbox.MAX_COMMAND_TYPE_LENGTH,
            "commandType",
            StoredValueBoundsTest::publishRequest),
        bound(
            "DeadLetterEntry.commandType",
            CommandInbox.MAX_COMMAND_TYPE_LENGTH,
            "commandType",
            StoredValueBoundsTest::entry));
  }

  private static Arguments bound(String what, int max, String name, Consumer<String> build) {
    return Arguments.of(what, max, name, build);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bounds")
  void acceptsTheColumnWidthAndRefusesOneMore(
      String what, int max, String name, Consumer<String> build) {
    assertEquals(255, max);
    build.accept("v".repeat(max));
    build.accept(SUPPLEMENTARY.repeat(max));

    var ascii =
        assertThrows(IllegalArgumentException.class, () -> build.accept("Q".repeat(max + 1)));
    assertEquals(name + " is 256 characters; at most 255", ascii.getMessage());
    assertFalse(ascii.getMessage().contains("QQ"));
    var supplementary =
        assertThrows(
            IllegalArgumentException.class, () -> build.accept(SUPPLEMENTARY.repeat(max) + "x"));
    assertFalse(supplementary.getMessage().contains(SUPPLEMENTARY));
  }

  @Test
  void storedLengthReturnsTheValueItAccepts() {
    String value = "v".repeat(10);

    assertSame(value, StoredLength.requireAtMost(value, 10, "value"));
    assertEquals(
        "value is 11 characters; at most 10",
        assertThrows(
                IllegalArgumentException.class,
                () -> StoredLength.requireAtMost(value + "v", 10, "value"))
            .getMessage());
  }

  @Test
  void requireHelpersRefuseNull() {
    assertThrows(NullPointerException.class, () -> OutboxStore.requireSkippedBy(null));
    assertThrows(NullPointerException.class, () -> EventStore.requireStateType(null));
  }

  @Test
  void jsonValueTypesRefuseAnOverlongValue() {
    var mapper = new ObjectMapper();
    String json = "\"" + "v".repeat(256) + "\"";

    for (Class<?> type :
        new Class<?>[] {
          CommandId.class,
          ProjectionName.class,
          SubscriptionName.class,
          SagaType.class,
          OutboxEntryId.class
        }) {
      assertThrows(JsonMappingException.class, () -> mapper.readValue(json, type), type.getName());
    }
  }

  private static void publishRequest(String commandType) {
    new DeadLetterQueue.DeadLetterPublishRequest(
        "{}",
        commandType,
        CommandId.of("c-1"),
        null,
        "java.lang.IllegalStateException",
        "msg",
        1,
        Instant.EPOCH,
        null,
        null,
        null,
        null);
  }

  private static void entry(String commandType) {
    new DeadLetterQueue.DeadLetterEntry(
        CommandId.of("c-1"),
        commandType,
        "{}",
        null,
        "java.lang.IllegalStateException",
        "msg",
        1,
        Instant.EPOCH,
        Instant.EPOCH,
        0,
        null,
        null,
        null,
        null,
        null);
  }
}
