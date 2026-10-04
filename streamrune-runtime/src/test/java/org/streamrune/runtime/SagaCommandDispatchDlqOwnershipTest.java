package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.streamrune.core.*;
import org.streamrune.core.Command;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;

/**
 * Verifies that commands dispatched via {@link SagaCommandDispatch#executeCorrelated} are marked
 * saga-owned and therefore NOT published to the dead-letter queue on failure.
 *
 * <p>The saga already owns the failure (it compensates on failure). Independently DLQ-retrying the
 * command in addition would double-drive the compensation — both the retry infrastructure and the
 * saga would be trying to handle the same failure.
 */
class SagaCommandDispatchDlqOwnershipTest {

  private static final AggregateType TYPE = AggregateType.of("failing");

  // === Test domain: a command whose decider always throws an infrastructure exception ===

  sealed interface AlwaysFailingCommand extends Command {
    record Blow(String id) implements AlwaysFailingCommand {}
  }

  sealed interface AlwaysFailingEvent extends DomainEvent {
    record Blew() implements AlwaysFailingEvent {}
  }

  record AlwaysFailingState() implements AggregateState {}

  /**
   * Always throws a plain {@link RuntimeException}, never a {@link DomainException}. Both tests use
   * it: they are about the SAGA_OWNED suppression boundary, not about business-rejection
   * classification, so the failure must stay DLQ-eligible under {@link
   * VirtualThreadCommandBus#DEFAULT_DLQ_ELIGIBLE}. A DomainException is excluded from the DLQ
   * regardless of saga ownership, so with one the empty-DLQ assertion would hold trivially and the
   * published-to-DLQ assertion could never hold.
   */
  static class InfraFailingDecider
      implements Decider<AlwaysFailingCommand, AlwaysFailingState, AlwaysFailingEvent> {
    @Override
    public AlwaysFailingState initialState() {
      return new AlwaysFailingState();
    }

    @Override
    public List<AlwaysFailingEvent> decide(AlwaysFailingCommand command, AlwaysFailingState state) {
      throw new RuntimeException("Saga command always fails — simulated infra failure");
    }

    @Override
    public AlwaysFailingState evolve(AlwaysFailingState state, AlwaysFailingEvent event) {
      return state;
    }
  }

  // === Minimal in-memory EventStore (same structure as VirtualThreadCommandBusTest) ===

  static class SimpleInMemoryEventStore implements EventStore {
    @Override
    public AggregateHistory load(StreamId streamId) {
      return new AggregateHistory(null, List.of(), Version.initial(), Version.initial());
    }

    @Override
    public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
      return load(streamId);
    }

    @Override
    public synchronized AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      return new AppendResult(List.of(), expectedVersion);
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {}

    @Override
    public void saveSnapshot(
        StreamId streamId, Version version, AggregateState state, int snapshotVersion) {}

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return List.of();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return List.of();
    }
  }

  // === Recording DLQ ===

  static class RecordingDlq implements DeadLetterQueue {
    final CopyOnWriteArrayList<DeadLetterPublishRequest> entries = new CopyOnWriteArrayList<>();

    @Override
    public void publish(DeadLetterPublishRequest request) {
      entries.add(request);
    }

    @Override
    public List<DeadLetterEntry> read(int limit) {
      return List.of();
    }

    @Override
    public void discard(CommandId commandId) {}

    @Override
    public void updateAttempts(CommandId commandId, int dlqAttempts, Instant lastAttemptAt) {}

    @Override
    public List<DeadLetterEntry> readRetryable(int maxRetries, int limit) {
      return List.of();
    }
  }

  // === Helpers ===

  private VirtualThreadCommandBus buildBus(
      RecordingDlq dlq,
      Decider<AlwaysFailingCommand, AlwaysFailingState, AlwaysFailingEvent> decider) {
    var inbox = new InMemoryCommandInbox();
    return VirtualThreadCommandBus.builder()
        .eventStore(new SimpleInMemoryEventStore())
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .deadLetterQueue(dlq)
        .objectMapper(new ObjectMapper())
        .commandInbox(inbox)
        .register(
            TYPE,
            AlwaysFailingCommand.class,
            cmd ->
                switch (cmd) {
                  case AlwaysFailingCommand.Blow c -> AggregateId.of(c.id());
                },
            decider)
        .build();
  }

  private void runWithContext(Runnable action) {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("saga-ctx"), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx).run(action);
  }

  // === Tests ===

  @Test
  void sagaOwnedCommandFailure_isNotPublishedToDlq() {
    // A command dispatched through SagaCommandDispatch.executeCorrelated must NOT be published
    // to the DLQ on permanent failure: the SAGA subsystem owns its recovery — compensation,
    // redelivery, its own timeout(), or the compensation resume paths —
    // so the DLQ must not independently retry it too (double-drive).
    // Uses InfraFailingDecider (a DLQ-eligible RuntimeException, not a DomainException): otherwise
    // the empty-DLQ assertion would hold trivially via the business-rejection exclusion and would
    // not actually exercise SAGA_OWNED suppression (the load-bearing behaviour under test).
    var dlq = new RecordingDlq();
    var bus = buildBus(dlq, new InfraFailingDecider());

    runWithContext(
        () ->
            assertThrows(
                RuntimeException.class,
                () ->
                    SagaCommandDispatch.executeCorrelated(
                        bus,
                        CorrelationId.of("saga-x"),
                        new AlwaysFailingCommand.Blow("agg-1"),
                        IdempotencyKey.of("saga:saga-x:1:0")),
                "saga-dispatched command must propagate the original exception"));

    assertTrue(
        dlq.entries.isEmpty(),
        "saga-owned command failure must NOT be published to the DLQ — the saga subsystem owns its"
            + " recovery, and a second retry channel would re-drive it uncoordinated");
  }

  @Test
  void nonSagaCommandFailure_isStillPublishedToDlq() {
    // Regression guard: a command dispatched directly via bus.execute() (not saga-owned) still
    // reaches the DLQ on permanent failure — the SAGA_OWNED suppression must not over-suppress.
    // Uses InfraFailingDecider (a DLQ-eligible RuntimeException, not a DomainException): the
    // failure must be DLQ-eligible under the default business-rejection classification for this
    // guard to actually exercise the SAGA_OWNED boundary.
    var dlq = new RecordingDlq();
    var bus = buildBus(dlq, new InfraFailingDecider());

    runWithContext(
        () ->
            assertThrows(
                RuntimeException.class, () -> bus.execute(new AlwaysFailingCommand.Blow("agg-2"))));

    assertEquals(
        1,
        dlq.entries.size(),
        "a non-saga-owned permanent failure must still be published to the DLQ for independent retry");
  }
}
