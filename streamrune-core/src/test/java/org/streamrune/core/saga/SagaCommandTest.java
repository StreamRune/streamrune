package org.streamrune.core.saga;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.types.AggregateId;

class SagaCommandTest {

  record MyCommand(String name) implements Command {}

  @Test
  void constructs_with_command_and_aggregate_id() {
    var myCommand = new MyCommand("my-command");
    var cmd = SagaCommand.of(myCommand, AggregateId.of("agg-1"));
    assertEquals(myCommand, cmd.command());
    assertEquals(AggregateId.of("agg-1"), cmd.aggregateId());
  }

  @Test
  void rejects_null_command() {
    assertThrows(NullPointerException.class, () -> SagaCommand.of(null, AggregateId.of("agg-1")));
  }

  @Test
  void rejects_null_aggregate_id() {
    assertThrows(
        NullPointerException.class, () -> SagaCommand.of(new MyCommand("cmd"), (AggregateId) null));
  }

  @Test
  void blank_aggregate_id_is_rejected_by_aggregate_id_before_saga_command_sees_it() {
    // SagaCommand checks only for null: AggregateId's own constructor rejects an empty or blank
    // value, so a blank aggregate id cannot be constructed, let alone passed in.
    assertThrows(IllegalArgumentException.class, () -> AggregateId.of(""));
    assertThrows(IllegalArgumentException.class, () -> AggregateId.of("  "));
  }
}
