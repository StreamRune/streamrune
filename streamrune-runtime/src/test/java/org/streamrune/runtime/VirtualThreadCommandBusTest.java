package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.*;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandBus.ShortCircuitReason;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class VirtualThreadCommandBusTest {

  private static final AggregateType COUNTER = AggregateType.of("counter");
  private static final AggregateType FAILING = AggregateType.of("failing");
  private static final AggregateType PII = AggregateType.of("pii");
  private static final AggregateType POLY = AggregateType.of("poly");
  private static final AggregateType A = AggregateType.of("a");
  private static final AggregateType B = AggregateType.of("b");

  // === Test domain: Counter ===

  sealed interface CounterCommand extends Command {
    record Inc(String counterId) implements CounterCommand {}
  }

  sealed interface CounterEvent extends DomainEvent {
    record Incremented() implements CounterEvent {}
  }

  record CounterState(int count) implements AggregateState {
    CounterState() {
      this(0);
    }
  }

  static class CounterDecider implements Decider<CounterCommand, CounterState, CounterEvent> {

    @Override
    public CounterState initialState() {
      return new CounterState();
    }

    @Override
    public List<CounterEvent> decide(CounterCommand command, CounterState state) {
      return switch (command) {
        case CounterCommand.Inc _ -> List.of(new CounterEvent.Incremented());
      };
    }

    @Override
    public CounterState evolve(CounterState state, CounterEvent event) {
      return switch (event) {
        case CounterEvent.Incremented _ -> new CounterState(state.count() + 1);
      };
    }
  }

  // === In-memory EventStore ===

  static class InMemoryEventStore implements EventStore {
    private final Map<String, List<EventEnvelope>> streams = new HashMap<>();
    private long globalOffset = 0;

    @Override
    public AggregateHistory load(StreamId streamId) {
      var events = streams.getOrDefault(streamId.value(), List.of());
      Version version = events.isEmpty() ? Version.initial() : events.getLast().version();
      return new AggregateHistory(null, events, version, Version.initial());
    }

    @Override
    public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
      // This store never returns snapshot state, so the schema version check is irrelevant
      return load(streamId);
    }

    @Override
    public synchronized AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      var existing = streams.getOrDefault(streamId.value(), List.of());
      long currentVersion = existing.isEmpty() ? 0 : existing.getLast().version().value();
      if (currentVersion != expectedVersion.value()) {
        throw new OptimisticLockException(
            "Expected version " + expectedVersion.value() + " but was " + currentVersion);
      }
      var updated = new ArrayList<>(existing);
      List<GlobalOffset> offsets = new ArrayList<>();
      for (EventEnvelope envelope : events) {
        globalOffset++;
        GlobalOffset offset = GlobalOffset.of(globalOffset);
        offsets.add(offset);
        updated.add(
            new EventEnvelope(
                offset,
                envelope.streamId(),
                envelope.version(),
                envelope.eventType(),
                envelope.event(),
                envelope.metadata()));
      }
      streams.put(streamId.value(), List.copyOf(updated));
      return new AppendResult(List.copyOf(offsets), new Version(currentVersion + events.size()));
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      // no-op for basic tests
    }

    @Override
    public void saveSnapshot(
        StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
      saveSnapshot(streamId, version, state);
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return streams.values().stream()
          .flatMap(List::stream)
          .filter(e -> e.globalOffset().value() > afterOffset.value())
          .sorted(java.util.Comparator.comparingLong(e -> e.globalOffset().value()))
          .limit(maxCount)
          .toList();
    }

    List<EventEnvelope> eventsFor(String streamId) {
      return streams.getOrDefault(streamId, List.of());
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      var events = streams.getOrDefault(streamId.value(), List.of());
      return events.stream()
          .filter(e -> e.version().value() > afterVersion.value())
          .limit(maxCount)
          .toList();
    }
  }

  // === Helpers ===

  private VirtualThreadCommandBus buildBus(InMemoryEventStore store) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .register(
            COUNTER,
            CounterCommand.class,
            cmd ->
                switch (cmd) {
                  case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                },
            new CounterDecider())
        .build();
  }

  private void runWithContext(Runnable action) {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx).run(action);
  }

  // === Tests ===

  @Test
  void executesCommandAndAppendsEvents() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-1")));

    var events = store.eventsFor("counter:counter-1");
    assertEquals(1, events.size());
    assertInstanceOf(CounterEvent.Incremented.class, events.getFirst().event());
    assertEquals(1, events.getFirst().version().value());
    assertEquals("Incremented", events.getFirst().eventType().name());
    assertEquals("counter:counter-1", events.getFirst().streamId().value());
  }

  @Test
  void reconstructsStateFromHistory() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    runWithContext(
        () -> {
          bus.execute(new CounterCommand.Inc("counter-1"));
          bus.execute(new CounterCommand.Inc("counter-1"));
        });

    var events = store.eventsFor("counter:counter-1");
    assertEquals(2, events.size());
    assertEquals(1, events.get(0).version().value());
    assertEquals(2, events.get(1).version().value());
  }

  @Test
  void rejectsUnregisteredCommand() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    runWithContext(
        () -> {
          record UnregisteredCommand() implements Command {}
          var ex =
              assertThrows(
                  IllegalArgumentException.class, () -> bus.execute(new UnregisteredCommand()));
          assertTrue(ex.getMessage().contains("No decider registered"));
        });
  }

  @Test
  void streamRuneFacadeWorks() {
    var store = new InMemoryEventStore();
    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    runWithContext(() -> streamRune.execute(new CounterCommand.Inc("counter-1")));

    var events = store.eventsFor("counter:counter-1");
    assertEquals(1, events.size());
    assertInstanceOf(CounterEvent.Incremented.class, events.getFirst().event());
  }

  // === Tests for builder convenience methods ===

  @Test
  void registerDeciderIsAlias() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .registerDecider(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-1")));

    var events = store.eventsFor("counter:counter-1");
    assertEquals(1, events.size());
    assertInstanceOf(CounterEvent.Incremented.class, events.getFirst().event());
  }

  // === Retry tests ===

  @Test
  void retriesOnOptimisticLockException() {
    var failCount = new AtomicInteger(2); // fail first 2 attempts
    var store =
        new InMemoryEventStore() {
          @Override
          public synchronized AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            if (failCount.getAndDecrement() > 0) {
              throw new OptimisticLockException("Conflict");
            }
            return super.append(streamId, events, expectedVersion);
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(5, Duration.ofMillis(1), 1.0))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-1")));

    var events = store.eventsFor("counter:counter-1");
    assertEquals(1, events.size());
  }

  // === Snapshot tests ===

  @Test
  void savesSnapshotAfterNEvents() {
    var snapshots = new CopyOnWriteArrayList<AggregateState>();
    var store =
        new InMemoryEventStore() {
          @Override
          public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
            snapshots.add(state);
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .snapshotPolicy(SnapshotPolicy.everyNEvents(3))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    runWithContext(
        () -> {
          for (int i = 0; i < 4; i++) {
            bus.execute(new CounterCommand.Inc("counter-1"));
          }
        });

    assertFalse(snapshots.isEmpty(), "Snapshot should have been saved");
    assertInstanceOf(CounterState.class, snapshots.getFirst());
  }

  @Test
  void maybeSnapshot_passesSnapshotVersion_toEventStore() {
    var capturedSnapshotVersions = new CopyOnWriteArrayList<Integer>();
    var store =
        new InMemoryEventStore() {
          @Override
          public void saveSnapshot(
              StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
            capturedSnapshotVersions.add(snapshotVersion);
          }
        };

    // everyNEvents(1, 3) → snapshot after every 1 event, snapshotVersion = 3
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .snapshotPolicy(SnapshotPolicy.everyNEvents(1, 3))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    // One command appending 1 event reaches n=1 immediately → triggers snapshot.
    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-1")));

    assertFalse(capturedSnapshotVersions.isEmpty(), "Snapshot should have been saved");
    assertEquals(3, capturedSnapshotVersions.getFirst(), "snapshotVersion must be passed through");
  }

  @Test
  void neverPolicySnapshotIsNeverConsulted_evenWhenOneExistsFromBeforeThePolicySwitched() {
    // A stream that was actively snapshotted (e.g. under EveryNEvents) before the policy
    // switched to never() leaves a stale snapshot row sitting in the store. Under the OLD
    // expectedSnapshotVersion == 0 ("skip the check, use anything") behavior that row would have
    // silently kept seeding decide() forever. never() must mean "off" on the read side too: the
    // decider must see the aggregate rebuilt purely from its events, never the stale snapshot's
    // claimed count=99 (no events back that value up at all).
    var store = new org.streamrune.test.InMemoryEventStore();
    StreamId streamId = StreamId.of(COUNTER, AggregateId.of("counter-1"));
    store.saveSnapshot(streamId, new Version(5), new CounterState(99), 1);

    var receivedState = new AtomicReference<CounterState>();
    Decider<CounterCommand, CounterState, CounterEvent> capturingDecider =
        new Decider<>() {
          @Override
          public CounterState initialState() {
            return new CounterState();
          }

          @Override
          public List<CounterEvent> decide(CounterCommand command, CounterState state) {
            receivedState.set(state);
            return List.of(new CounterEvent.Incremented());
          }

          @Override
          public CounterState evolve(CounterState state, CounterEvent event) {
            return new CounterState(state.count() + 1);
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .snapshotPolicy(SnapshotPolicy.never())
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                capturingDecider)
            .build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-1")));

    assertNotNull(receivedState.get());
    assertEquals(
        0,
        receivedState.get().count(),
        "the stale snapshot (count=99) must be ignored — decide() must see the fresh initial"
            + " state rebuilt from events (none exist yet), not the untrustworthy snapshot");
  }

  @Test
  void snapshotTriggersOnFirstCommandOfBrandNewAggregate() {
    // A brand-new aggregate has version == lastSnapshotVersion == 0; the just-appended events
    // must still count toward the snapshot threshold instead of deferring to the next command.
    var snapshots = new CopyOnWriteArrayList<AggregateState>();
    var store =
        new InMemoryEventStore() {
          @Override
          public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
            snapshots.add(state);
          }
        };
    var bus = counterBusBuilder(store).snapshotPolicy(SnapshotPolicy.everyNEvents(1)).build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-fresh-1")));

    assertEquals(
        1,
        snapshots.size(),
        "a single command reaching the threshold must snapshot immediately, not one command later");
  }

  // === DLQ tests ===

  sealed interface FailingCommand extends Command {
    record Fail(String id) implements FailingCommand {}
  }

  sealed interface FailingEvent extends DomainEvent {
    record Failed() implements FailingEvent {}
  }

  record FailingState() implements AggregateState {}

  static class FailingDecider implements Decider<FailingCommand, FailingState, FailingEvent> {
    @Override
    public FailingState initialState() {
      return new FailingState();
    }

    @Override
    public List<FailingEvent> decide(FailingCommand command, FailingState state) {
      throw new DomainException("Always fails");
    }

    @Override
    public FailingState evolve(FailingState state, FailingEvent event) {
      return state;
    }
  }

  /**
   * A decider simulating an infrastructure fault (as opposed to {@link FailingDecider}'s business
   * rejection): {@code decide} throws a plain {@link RuntimeException}, which is NOT a {@link
   * DomainException} and therefore remains DLQ-eligible under the default {@code dlqEligible}
   * predicate. Used by tests that must exercise a genuinely dead-letterable failure.
   */
  static class InfraFailingDecider implements Decider<FailingCommand, FailingState, FailingEvent> {
    @Override
    public FailingState initialState() {
      return new FailingState();
    }

    @Override
    public List<FailingEvent> decide(FailingCommand command, FailingState state) {
      throw new RuntimeException("simulated infra failure");
    }

    @Override
    public FailingState evolve(FailingState state, FailingEvent event) {
      return state;
    }
  }

  @Test
  void publishesToDlqOnFinalFailure() {
    // Uses InfraFailingDecider (not FailingDecider): a DomainException is a business rejection
    // and is never dead-lettered (see businessRejectionIsNotDeadLettered). This test asserts DLQ
    // publish — including the correlation/user/trace context-capture — for a genuinely
    // dead-letterable infrastructure failure.
    var store = new InMemoryEventStore();
    var dlqEntries = new CopyOnWriteArrayList<DeadLetterQueue.DeadLetterPublishRequest>();

    DeadLetterQueue dlq =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            dlqEntries.add(request);
          }

          @Override
          public List<DeadLetterEntry> read(int limit) {
            return List.of();
          }

          @Override
          public void discard(CommandId commandId) {}

          @Override
          public void updateAttempts(
              CommandId commandId, int dlqAttempts, java.time.Instant lastAttemptAt) {}

          @Override
          public List<DeadLetterEntry> readRetryable(int maxRetries, int limit) {
            return List.of();
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .register(
                FAILING,
                FailingCommand.class,
                cmd ->
                    switch (cmd) {
                      case FailingCommand.Fail c -> AggregateId.of(c.id());
                    },
                new InfraFailingDecider())
            .build();

    runWithContext(
        () ->
            assertThrows(
                RuntimeException.class, () -> bus.execute(new FailingCommand.Fail("fail-1"))));

    assertEquals(1, dlqEntries.size());
    assertEquals(StreamId.of(FAILING, AggregateId.of("fail-1")), dlqEntries.getFirst().streamId());
    assertEquals(AggregateId.of("fail-1"), dlqEntries.getFirst().aggregateId());
    // The bus persists the FULLY-QUALIFIED class name (not the simple name), so DLQ replay
    // resolves by FQN and same-simple-name commands across packages can never route to the wrong
    // decider.
    assertEquals(FailingCommand.Fail.class.getName(), dlqEntries.getFirst().commandType());
    // The bus must capture the bound request context onto the DLQ entry so a replay can rebind it
    // (events keep correlation; fail-closed authz re-evaluates as the original user).
    // runWithContext
    // binds correlation "corr-1" with no user/trace.
    assertNotNull(
        dlqEntries.getFirst().correlationId(), "DLQ entry must carry the bound correlation");
    assertEquals("corr-1", dlqEntries.getFirst().correlationId().value());
  }

  /** Command carrying one {@code @Encrypted} PII field, keyed off {@code customerId}. */
  record PiiCommand(
      String customerId,
      @org.streamrune.core.crypto.Encrypted(subjectId = "customerId") String email)
      implements Command {}

  sealed interface PiiFailingEvent extends DomainEvent {
    record Failed() implements PiiFailingEvent {}
  }

  record PiiFailingState() implements AggregateState {}

  /**
   * Infrastructure-failure decider for {@link PiiCommand}, mirroring {@link InfraFailingDecider}.
   */
  static class InfraFailingPiiDecider
      implements Decider<PiiCommand, PiiFailingState, PiiFailingEvent> {
    @Override
    public PiiFailingState initialState() {
      return new PiiFailingState();
    }

    @Override
    public List<PiiFailingEvent> decide(PiiCommand command, PiiFailingState state) {
      throw new RuntimeException("simulated infra failure");
    }

    @Override
    public PiiFailingState evolve(PiiFailingState state, PiiFailingEvent event) {
      return state;
    }
  }

  @Test
  void dlqPayloadIsEncrypted() {
    // The DLQ must never store an @Encrypted command field as plaintext PII: it currently is
    // never crypto-shredded on forget, and — with this fix — the bus's DLQ publish mapper is
    // wired with CryptoShreddingModule (mirroring PostgresSagaStore/PostgresEventStore) so the
    // stored payload is ciphertext instead of the raw email substring.
    var store = new InMemoryEventStore();
    var dlqEntries = new CopyOnWriteArrayList<DeadLetterQueue.DeadLetterPublishRequest>();
    DeadLetterQueue dlq =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            dlqEntries.add(request);
          }

          @Override
          public List<DeadLetterEntry> read(int limit) {
            return List.of();
          }

          @Override
          public void discard(CommandId commandId) {}

          @Override
          public void updateAttempts(
              CommandId commandId, int dlqAttempts, java.time.Instant lastAttemptAt) {}

          @Override
          public List<DeadLetterEntry> readRetryable(int maxRetries, int limit) {
            return List.of();
          }
        };

    var cryptoEngine = new org.streamrune.test.InMemoryCryptoEngine();
    var cryptoMapper = DeadLetterRetryRunner.createObjectMapper(cryptoEngine);

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(cryptoMapper)
            .register(
                PII,
                PiiCommand.class,
                cmd -> AggregateId.of(cmd.customerId()),
                new InfraFailingPiiDecider())
            .build();

    assertThrows(
        RuntimeException.class,
        () -> bus.execute(new PiiCommand("customer-1", "alice@example.com")));

    assertEquals(1, dlqEntries.size());
    assertFalse(
        dlqEntries.getFirst().commandPayload().contains("alice@example.com"),
        "the stored DLQ payload must be ciphertext, never the plaintext PII substring");
  }

  @Test
  void dlqSerializationFailureRecordsMetadataOnlyNullPayloadNotToString() {
    // When serializing the failed command for the DLQ throws — including the crypto module's
    // deliberate fail-fast CryptoOperationException (Vault/KMS outage or null @Encrypted subjectId)
    // — the bus must NOT degrade to command.toString(): that re-emits raw @Encrypted PII, which
    // leaks plaintext on a text store and, on PostgresDeadLetterQueue, fails the ?::jsonb cast so
    // the entry is silently dropped. The entry must still be recorded, with a valid JSON-null
    // payload and the original error metadata. Fail-first: HEAD stores command.toString() here.
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();

    // A mapper that fails serialization exactly like the crypto module's fail-fast path.
    ObjectMapper throwingMapper =
        new ObjectMapper() {
          @Override
          public String writeValueAsString(Object value) {
            throw new org.streamrune.core.crypto.CryptoOperationException(
                "no encryption key for subject — fail fast, do not persist plaintext");
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(throwingMapper)
            .register(
                PII,
                PiiCommand.class,
                cmd -> AggregateId.of(cmd.customerId()),
                new InfraFailingPiiDecider())
            .build();

    assertThrows(
        RuntimeException.class,
        () -> bus.execute(new PiiCommand("customer-1", "alice@example.com")));

    assertEquals(1, dlq.entries.size(), "the failed command must still be dead-lettered");
    var entry = dlq.entries.getFirst();
    assertEquals(
        "null", entry.commandPayload(), "payload must be JSON null, never command.toString()");
    assertFalse(
        entry.commandPayload().contains("alice@example.com"),
        "the metadata-only payload must not leak @Encrypted PII");
    // The recorded error metadata is the ORIGINAL command failure — why it was dead-lettered.
    assertEquals(RuntimeException.class.getName(), entry.errorType());
    assertEquals("simulated infra failure", entry.errorMessage());
  }

  /**
   * {@code dead_letter_queue.error_message} is a persisted-text sink like {@code audit_log}: an
   * operator console and every export render it, and a NUL in it fails the INSERT — which the bus
   * catches and logs, so the command's only recovery channel would silently never exist. The entry
   * carries the sanitized message.
   */
  @Test
  void dlqEntryErrorMessage_isPersistedSanitized() {
    String raw = "infra failure\r\n2026-10-02 INFO dlq - replayed OK\u0000";
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .register(
                FAILING,
                FailingCommand.class,
                cmd ->
                    switch (cmd) {
                      case FailingCommand.Fail c -> AggregateId.of(c.id());
                    },
                new InfraFailingDecider() {
                  @Override
                  public List<FailingEvent> decide(FailingCommand command, FailingState state) {
                    throw new RuntimeException(raw);
                  }
                })
            .build();

    var thrown =
        assertThrows(RuntimeException.class, () -> bus.execute(new FailingCommand.Fail("f-1")));

    assertEquals(1, dlq.entries.size());
    assertEquals(LogSanitizer.sanitizeFreeText(raw), dlq.entries.getFirst().errorMessage());
    assertFalse(
        dlq.entries.getFirst().errorMessage().chars().anyMatch(Character::isISOControl),
        dlq.entries.getFirst().errorMessage());
    // Only the persisted copy is sanitized: the caller's exception is untouched.
    assertTrue(
        String.valueOf(thrown.getMessage()).contains(raw)
            || (thrown.getCause() != null && raw.equals(thrown.getCause().getMessage())),
        String.valueOf(thrown.getMessage()));
  }

  @Test
  void dlqPublishWithNoBoundContextCarriesNullRequestContext() {
    // A command that fails with no StreamRuneContext bound publishes null context, so the runner
    // replays such entries unbound. Uses infraFailingBusBuilder (not failingBusBuilder): the
    // failure must be DLQ-eligible for this to reach a publish at all — a DomainException would
    // now be skipped before the DLQ is ever touched.
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus =
        infraFailingBusBuilder(store).deadLetterQueue(dlq).objectMapper(new ObjectMapper()).build();

    assertThrows(RuntimeException.class, () -> bus.execute(new FailingCommand.Fail("fail-nc")));

    assertEquals(1, dlq.entries.size());
    assertNull(dlq.entries.getFirst().correlationId());
    assertNull(dlq.entries.getFirst().userId());
    assertNull(dlq.entries.getFirst().traceId());
  }

  // === idExtractor validation tests ===

  /** Builds a counter bus whose idExtractor returns the given (possibly invalid) aggregate id. */
  private VirtualThreadCommandBus busWithIdExtractorReturning(
      InMemoryEventStore store, AggregateId extractedId, CommandInterceptor... interceptors) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .interceptors(interceptors)
        .register(COUNTER, CounterCommand.class, cmd -> extractedId, new CounterDecider())
        .build();
  }

  @Test
  void nullAggregateIdFailsBeforeAnyInterceptorRuns() {
    var store = new InMemoryEventStore();
    var beforeCalls = new AtomicInteger();
    CommandInterceptor counting =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            beforeCalls.incrementAndGet();
            return true;
          }
        };
    var bus = busWithIdExtractorReturning(store, null, counting);

    runWithContext(
        () -> {
          var ex =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> bus.execute(new CounterCommand.Inc("ignored")));
          assertTrue(
              ex.getMessage().contains("idExtractor returned null for command:"),
              "the failure must name the idExtractor contract, got: " + ex.getMessage());
        });
    assertEquals(
        0, beforeCalls.get(), "no interceptor may ever observe a null aggregateId in its context");
  }

  @Test
  void nullAggregateIdFailsBeforeShortCircuitingInterceptor() {
    // A short-circuiting interceptor; null AggregateId from the extractor must be caught before
    // any interceptor runs or StreamId.of() is invoked.
    var store = new InMemoryEventStore();
    var bus =
        busWithIdExtractorReturning(
            store, null, new RejectingInterceptor(new CopyOnWriteArrayList<>()));

    runWithContext(
        () -> {
          var ex =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> bus.execute(new CounterCommand.Inc("ignored")));
          assertTrue(
              ex.getMessage().contains("idExtractor returned null for command:"),
              "the failure must name the idExtractor contract, got: " + ex.getMessage());
        });
  }

  /**
   * Command routing is where a client-supplied aggregate id becomes an {@link AggregateId}, and the
   * id extractor builds it through {@link AggregateId#of(String)}, the ingress door. A control
   * character there is refused before anything is written: no interceptor runs (so the audit
   * interceptor writes no {@code audit_log} row), nothing is appended to {@code event_stream}, and
   * no {@code dead_letter_queue} row is created — the three columns that stored such an id verbatim
   * before.
   */
  @Test
  void controlCharacterInARoutedAggregateIdIsRefusedBeforeAnyInterceptorAppendOrDeadLetter() {
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var beforeCalls = new AtomicInteger();
    CommandInterceptor counting =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            beforeCalls.incrementAndGet();
            return true;
          }
        };
    var bus =
        counterBusBuilder(store)
            .interceptors(counting)
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();
    String forged = "counter-1\r\nINFO forged-SECRET";

    runWithContext(
        () -> {
          var ex =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> bus.execute(new CounterCommand.Inc(forged)));
          assertTrue(
              ex.getMessage().contains("aggregateId must not contain control characters"),
              ex.getMessage());
          assertFalse(ex.getMessage().contains("SECRET"), ex.getMessage());
        });

    assertEquals(0, beforeCalls.get(), "no interceptor (audit included) may see the refused id");
    assertTrue(
        store.eventsFor("counter:" + forged).isEmpty(),
        "nothing may be appended under the refused id");
    assertTrue(dlq.entries.isEmpty(), "a refused routing is a client error, never dead-lettered");
  }

  /**
   * An aggregate id longer than the {@code VARCHAR(255)} stream columns is refused at the same
   * point. Without the bound the command ran: its interceptors saw it, the PostgreSQL append failed
   * with an SQL error (a 500, not a 400), and the dead-letter insert failed on the same column, so
   * the command had no recovery row either.
   */
  @Test
  void overLengthRoutedAggregateIdIsRefusedBeforeAnyInterceptorAppendOrDeadLetter() {
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var beforeCalls = new AtomicInteger();
    CommandInterceptor counting =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            beforeCalls.incrementAndGet();
            return true;
          }
        };
    var bus =
        counterBusBuilder(store)
            .interceptors(counting)
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();
    String atBound = "c".repeat(255);
    String overLong = "c".repeat(256);

    runWithContext(
        () -> {
          var ex =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> bus.execute(new CounterCommand.Inc(overLong)));
          assertEquals("aggregateId must be at most 255 characters, got 256", ex.getMessage());
        });

    assertEquals(0, beforeCalls.get(), "no interceptor (audit included) may see the refused id");
    assertTrue(
        store.eventsFor("counter:" + overLong).isEmpty(),
        "nothing may be appended under the refused id");
    assertTrue(dlq.entries.isEmpty(), "a refused routing is a client error, never dead-lettered");

    runWithContext(() -> bus.execute(new CounterCommand.Inc(atBound)));
    assertEquals(1, beforeCalls.get(), "an id at the bound is routed like any other");
    assertEquals(1, store.eventsFor("counter:" + atBound).size());
  }

  // === Interceptor tests ===

  @Test
  void interceptorsCalledInCorrectOrder() {
    var callOrder = new CopyOnWriteArrayList<String>();

    CommandInterceptor first =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("first-before");
            return true;
          }

          @Override
          public void after(CommandContext ctx) {
            callOrder.add("first-after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("first-onError");
          }
        };

    CommandInterceptor second =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("second-before");
            return true;
          }

          @Override
          public void after(CommandContext ctx) {
            callOrder.add("second-after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("second-onError");
          }
        };

    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .interceptors(first, second)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-1")));

    assertEquals(
        List.of("first-before", "second-before", "second-after", "first-after"),
        callOrder,
        "before() should be forward order, after() should be reverse order");
  }

  @Test
  void onErrorCalledInReverseOrderWhenInterceptorThrows() {
    var callOrder = new CopyOnWriteArrayList<String>();

    CommandInterceptor first =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("first-before");
            return true;
          }

          @Override
          public void after(CommandContext ctx) {
            callOrder.add("first-after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("first-onError");
          }
        };

    CommandInterceptor throwing =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("throwing-before");
            throw new IllegalStateException("interceptor failure");
          }

          @Override
          public void after(CommandContext ctx) {
            callOrder.add("throwing-after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("throwing-onError");
          }
        };

    CommandInterceptor afterThrower =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("afterThrower-before");
            return true;
          }

          @Override
          public void after(CommandContext ctx) {
            callOrder.add("afterThrower-after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("afterThrower-onError");
          }
        };

    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .interceptors(first, throwing, afterThrower)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    runWithContext(
        () ->
            assertThrows(
                IllegalStateException.class,
                () -> bus.execute(new CounterCommand.Inc("counter-1")),
                "Exception should propagate to caller"));

    assertEquals(
        List.of("first-before", "throwing-before", "first-onError"),
        callOrder,
        "onError must only notify interceptors whose before() completed — neither the thrower"
            + " nor an interceptor registered after it gets any callback");
  }

  /** Named class so the short-circuit result carries a meaningful interceptor name. */
  static final class RejectingInterceptor implements CommandInterceptor {
    final List<String> callOrder;

    RejectingInterceptor(List<String> callOrder) {
      this.callOrder = callOrder;
    }

    @Override
    public boolean before(CommandContext ctx) {
      callOrder.add("rejecting-before");
      return false; // short-circuits; this interceptor receives no further callback
    }

    @Override
    public void after(CommandContext ctx) {
      callOrder.add("rejecting-after");
    }

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      callOrder.add("rejecting-onError");
    }
  }

  @Test
  void shortCircuitNotifiesAfterOnPriorInterceptorsWithoutOnError() {
    var callOrder = new CopyOnWriteArrayList<String>();
    var afterResults = new CopyOnWriteArrayList<CommandResult>();

    CommandInterceptor first =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("first-before");
            return true;
          }

          @Override
          public void after(CommandContext ctx) {
            callOrder.add("first-after");
            afterResults.add(ctx.result());
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("first-onError");
          }
        };

    CommandInterceptor rejecting = new RejectingInterceptor(callOrder);

    CommandInterceptor neverRan =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("neverRan-before");
            return true;
          }

          @Override
          public void after(CommandContext ctx) {
            callOrder.add("neverRan-after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("neverRan-onError");
          }
        };

    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .interceptors(first, rejecting, neverRan)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    var resultHolder = new CommandResult[1];
    runWithContext(() -> resultHolder[0] = bus.execute(new CounterCommand.Inc("counter-1")));

    assertEquals(
        List.of("first-before", "rejecting-before", "first-after"),
        callOrder,
        "short-circuit: after() only on prior interceptors, no onError anywhere");

    assertEquals(1, afterResults.size());
    assertNotNull(afterResults.getFirst(), "after() must never receive a null result");
    assertTrue(
        afterResults.getFirst().shortCircuited(), "after() receives the short-circuit result");

    var events = store.eventsFor("counter:counter-1");
    assertTrue(events.isEmpty(), "Command should not have executed after short-circuit");
  }

  @Test
  void shortCircuitResultIsDistinguishableFromSuccess() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .interceptors(new RejectingInterceptor(new CopyOnWriteArrayList<>()))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    var resultHolder = new CommandResult[1];
    runWithContext(() -> resultHolder[0] = bus.execute(new CounterCommand.Inc("counter-1")));

    var shortCircuit = resultHolder[0];
    assertTrue(shortCircuit.shortCircuited());
    assertEquals(
        ShortCircuitReason.VETOED, shortCircuit.reason(), "an interceptor veto is a rejection");
    assertTrue(shortCircuit.vetoed());
    assertFalse(shortCircuit.idempotentReplay());
    assertEquals("RejectingInterceptor", shortCircuit.shortCircuitedBy());
    assertTrue(shortCircuit.events().isEmpty());

    var normalBus = buildBus(store);
    runWithContext(() -> resultHolder[0] = normalBus.execute(new CounterCommand.Inc("counter-1")));
    assertFalse(resultHolder[0].shortCircuited(), "executed commands are not short-circuited");
    assertEquals(ShortCircuitReason.NONE, resultHolder[0].reason());
    assertNull(resultHolder[0].shortCircuitedBy());
  }

  // === Async tests ===

  @Test
  void executeAsyncCompletesSuccessfully() throws Exception {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    runWithContext(
        () -> {
          var future = bus.executeAsync(new CounterCommand.Inc("counter-1"));
          var result =
              assertDoesNotThrow(() -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
          assertNotNull(result);
        });

    var events = store.eventsFor("counter:counter-1");
    assertEquals(1, events.size());
  }

  @Test
  void streamRuneContextCorrelationIdPropagatesThroughExecuteAsync() {
    // StreamRuneContext.CURRENT is a ScopedValue and does not inherit across Thread.start();
    // the bus must re-bind the submitter's context on the spawned virtual thread so produced
    // events carry the caller's correlation id instead of a freshly generated one.
    var store = new InMemoryEventStore();
    var bus = buildBus(store);
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-async-7"), Instant.now(), Map.of());

    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(
            () -> {
              var future = bus.executeAsync(new CounterCommand.Inc("counter-async-corr"));
              assertDoesNotThrow(() -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
            });

    var events = store.eventsFor("counter:counter-async-corr");
    assertEquals(1, events.size());
    assertEquals(
        "corr-async-7",
        events.getFirst().metadata().correlationId().value(),
        "the submitter's bound context must propagate into the async execution");
  }

  @Test
  void asyncTaskWrapperCapturesCallerContextOnSubmittingThread() {
    // Emulates OpenTelemetry-style context propagation: the wrapper runs on the submitting
    // thread (where it can capture thread-bound caller context) and the returned runnable
    // re-installs that context around the execution on the spawned virtual thread.
    var store = new InMemoryEventStore();
    ThreadLocal<String> callerContext = new ThreadLocal<>();
    var observedDuringExecution = new AtomicReference<String>();
    var wrapperThread = new AtomicReference<Thread>();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .interceptors(
                new CommandInterceptor() {
                  @Override
                  public boolean before(CommandContext ctx) {
                    observedDuringExecution.set(callerContext.get());
                    return true;
                  }
                })
            .asyncTaskWrapper(
                task -> {
                  wrapperThread.set(Thread.currentThread());
                  String captured = callerContext.get();
                  return () -> {
                    callerContext.set(captured);
                    try {
                      task.run();
                    } finally {
                      callerContext.remove();
                    }
                  };
                })
            .build();

    Thread submitter = Thread.currentThread();
    callerContext.set("caller-ctx");
    try {
      runWithContext(
          () -> {
            var future = bus.executeAsync(new CounterCommand.Inc("counter-async-ctx"));
            assertDoesNotThrow(() -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
          });
    } finally {
      callerContext.remove();
    }

    assertSame(submitter, wrapperThread.get(), "wrapper must be invoked on the submitting thread");
    assertEquals(
        "caller-ctx",
        observedDuringExecution.get(),
        "interceptor on the async thread must see the context captured at submission");
    assertEquals(1, store.eventsFor("counter:counter-async-ctx").size());
  }

  @Test
  void asyncTaskWrapperFailureFailsFutureWithoutLeakingInFlightCount() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .asyncTaskWrapper(
                task -> {
                  throw new IllegalStateException("wrapper boom");
                })
            .build();

    var future = bus.executeAsync(new CounterCommand.Inc("counter-async-wrap-fail"));
    var ex =
        assertThrows(
            java.util.concurrent.ExecutionException.class,
            () -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
    assertInstanceOf(IllegalStateException.class, ex.getCause());
    assertTrue(store.eventsFor("counter:counter-async-wrap-fail").isEmpty());
    // The in-flight count was released despite the wrapper failure, so close() must not time out.
    assertDoesNotThrow(bus::close);
  }

  @Test
  void asyncTaskWrapperReturningNullFailsFuture() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .asyncTaskWrapper(task -> null)
            .build();

    var future = bus.executeAsync(new CounterCommand.Inc("counter-async-wrap-null"));
    var ex =
        assertThrows(
            java.util.concurrent.ExecutionException.class,
            () -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
    assertInstanceOf(IllegalStateException.class, ex.getCause());
    assertDoesNotThrow(bus::close);
  }

  // === Guard tests ===

  static class GuardingDecider implements Decider<CounterCommand, CounterState, CounterEvent> {

    @Override
    public CounterState initialState() {
      return new CounterState();
    }

    @Override
    public void guard(CounterCommand command, CounterState state) {
      throw new AuthorizationException("Not allowed");
    }

    @Override
    public List<CounterEvent> decide(CounterCommand command, CounterState state) {
      return List.of(new CounterEvent.Incremented());
    }

    @Override
    public CounterState evolve(CounterState state, CounterEvent event) {
      return new CounterState(state.count() + 1);
    }
  }

  @Test
  void guardThrowingAuthorizationExceptionPropagates() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new GuardingDecider())
            .build();

    runWithContext(
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> bus.execute(new CounterCommand.Inc("counter-1")),
                "AuthorizationException from guard() must propagate to the caller"));

    assertTrue(
        store.eventsFor("counter:counter-1").isEmpty(),
        "No events should be appended when guard() throws");
  }

  // === causationId tests ===

  @Test
  void causationIdIsNullForRootCommands() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-caus-1")));

    var events = store.eventsFor("counter:counter-caus-1");
    assertEquals(1, events.size());
    var metadata = events.getFirst().metadata();
    assertNull(
        metadata.causationId(),
        "causationId must be null for chain roots — commandId has its own field");
    assertNotNull(metadata.commandId(), "commandId must be populated in its own field");
  }

  @Test
  void causationIdComesFromCausingMessageInContextBaggage() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    var ctx =
        new StreamRuneContext.RequestContext(
            null,
            null,
            CorrelationId.of("corr-1"),
            Instant.now(),
            Map.of(VirtualThreadCommandBus.CAUSATION_ID_BAGGAGE_KEY, "evt-trigger-42"));
    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(() -> bus.execute(new CounterCommand.Inc("counter-caus-2")));

    var events = store.eventsFor("counter:counter-caus-2");
    assertEquals(1, events.size());
    var metadata = events.getFirst().metadata();
    assertNotNull(metadata.causationId());
    assertEquals(
        "evt-trigger-42",
        metadata.causationId().value(),
        "causationId must reference the causing message id from context");
    assertNotEquals(metadata.commandId().value(), metadata.causationId().value());
  }

  // === Post-commit failure tests (committed commands must report success) ===

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

  private VirtualThreadCommandBus.Builder counterBusBuilder(InMemoryEventStore store) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .register(
            COUNTER,
            CounterCommand.class,
            cmd ->
                switch (cmd) {
                  case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                },
            new CounterDecider());
  }

  private VirtualThreadCommandBus.Builder failingBusBuilder(InMemoryEventStore store) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .objectMapper(new ObjectMapper())
        .register(
            FAILING,
            FailingCommand.class,
            cmd ->
                switch (cmd) {
                  case FailingCommand.Fail c -> AggregateId.of(c.id());
                },
            new FailingDecider());
  }

  /**
   * Like {@link #failingBusBuilder}, but registers {@link InfraFailingDecider} so the command fails
   * with a plain {@link RuntimeException} instead of a {@link DomainException} — for tests that
   * need a genuinely DLQ-eligible (infrastructure) failure.
   */
  private VirtualThreadCommandBus.Builder infraFailingBusBuilder(InMemoryEventStore store) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .objectMapper(new ObjectMapper())
        .register(
            FAILING,
            FailingCommand.class,
            cmd ->
                switch (cmd) {
                  case FailingCommand.Fail c -> AggregateId.of(c.id());
                },
            new InfraFailingDecider());
  }

  @Test
  void afterThrowingDoesNotFailCommittedCommand() {
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    CommandInterceptor throwingAfter =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw new IllegalStateException("after blew up");
          }
        };
    var bus =
        counterBusBuilder(store)
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .interceptors(throwingAfter)
            .build();

    var resultHolder = new CommandResult[1];
    runWithContext(() -> resultHolder[0] = bus.execute(new CounterCommand.Inc("counter-pc-1")));

    assertNotNull(resultHolder[0], "committed command must report success despite after() failure");
    assertFalse(resultHolder[0].shortCircuited());
    assertEquals(1, store.eventsFor("counter:counter-pc-1").size(), "events are committed");
    assertTrue(dlq.entries.isEmpty(), "committed commands must never be published to the DLQ");
  }

  @Test
  void afterThrowingDoesNotStarveSiblingAfters() {
    var callOrder = new CopyOnWriteArrayList<String>();
    CommandInterceptor first =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            callOrder.add("first-after");
          }
        };
    CommandInterceptor second =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            callOrder.add("second-after");
            throw new IllegalStateException("second after blew up");
          }
        };
    var store = new InMemoryEventStore();
    var bus = counterBusBuilder(store).interceptors(first, second).build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-pc-2")));

    assertEquals(
        List.of("second-after", "first-after"),
        callOrder,
        "a throwing after() must not prevent sibling after() callbacks");
  }

  @Test
  void onErrorThrowingDoesNotSuppressOriginalFailureOrStarveSiblings() {
    var callOrder = new CopyOnWriteArrayList<String>();
    CommandInterceptor first =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("first-onError");
          }
        };
    CommandInterceptor second =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            callOrder.add("second-onError");
            throw new IllegalStateException("onError blew up");
          }
        };
    var store = new InMemoryEventStore();
    var bus = failingBusBuilder(store).interceptors(first, second).build();

    runWithContext(
        () ->
            assertThrows(
                DomainException.class,
                () -> bus.execute(new FailingCommand.Fail("fail-onerror")),
                "the original failure must propagate, not the onError() failure"));

    assertEquals(
        List.of("second-onError", "first-onError"),
        callOrder,
        "a throwing onError() must not prevent sibling onError() callbacks");
  }

  @Test
  void postCommitSnapshotFailureStillReportsSuccess() {
    var store =
        new InMemoryEventStore() {
          @Override
          public void saveSnapshot(
              StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
            throw new IllegalStateException("snapshot store down");
          }
        };
    var dlq = new RecordingDlq();
    var bus =
        counterBusBuilder(store)
            .snapshotPolicy(SnapshotPolicy.everyNEvents(1))
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();

    runWithContext(
        () -> {
          bus.execute(new CounterCommand.Inc("counter-pc-3"));
          bus.execute(new CounterCommand.Inc("counter-pc-3"));
        });

    assertEquals(2, store.eventsFor("counter:counter-pc-3").size(), "events are committed");
    assertTrue(dlq.entries.isEmpty(), "snapshot failure after commit must not reach the DLQ");
  }

  @Test
  void postCommitMetricsFailureStillReportsSuccess() {
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    StreamRuneMetrics throwingMetrics =
        new StreamRuneMetrics() {
          @Override
          public void recordEventAppended() {
            throw new IllegalStateException("metrics backend down");
          }

          @Override
          public void recordCommandSucceeded() {
            throw new IllegalStateException("metrics backend down");
          }

          @Override
          public void recordCommandDuration(long durationNanos) {
            throw new IllegalStateException("metrics backend down");
          }
        };
    var bus =
        counterBusBuilder(store)
            .metrics(throwingMetrics)
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();

    var resultHolder = new CommandResult[1];
    runWithContext(() -> resultHolder[0] = bus.execute(new CounterCommand.Inc("counter-pc-4")));

    assertNotNull(resultHolder[0]);
    assertEquals(1, store.eventsFor("counter:counter-pc-4").size());
    assertTrue(dlq.entries.isEmpty(), "metrics failure after commit must not reach the DLQ");
  }

  /**
   * {@code recordCommandDispatched()} runs on the PRE-commit path, before the decider is ever
   * consulted. A throwing metrics collector there used to propagate straight out of {@code
   * execute()} — a metrics-backend problem turned into a total command outage.
   */
  @Test
  void preCommitDispatchMetricsFailureStillExecutesTheCommand() {
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    StreamRuneMetrics throwingMetrics =
        new StreamRuneMetrics() {
          @Override
          public void recordCommandDispatched() {
            throw new IllegalStateException("metrics backend down");
          }
        };
    var bus =
        counterBusBuilder(store)
            .metrics(throwingMetrics)
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();

    var resultHolder = new CommandResult[1];
    runWithContext(() -> resultHolder[0] = bus.execute(new CounterCommand.Inc("counter-obs6-1")));

    assertNotNull(resultHolder[0], "a throwing dispatch meter must not fail the command");
    assertEquals(
        1,
        store.eventsFor("counter:counter-obs6-1").size(),
        "the decider must still run and its events must still be appended");
    assertTrue(dlq.entries.isEmpty(), "a metrics failure must never dead-letter a valid command");
  }

  /**
   * On the failure path {@code recordCommandFailed()} runs immediately before {@code throw e}. A
   * throwing collector there used to REPLACE the real failure, so a {@link DomainException} the
   * caller maps to HTTP 400 surfaced as an opaque metrics exception and the root cause of the
   * incident was destroyed.
   */
  @Test
  void failureMetricsFailureStillPropagatesTheOriginalDomainException() {
    var store = new InMemoryEventStore();
    StreamRuneMetrics throwingMetrics =
        new StreamRuneMetrics() {
          @Override
          public void recordCommandFailed() {
            throw new IllegalStateException("metrics backend down");
          }
        };
    var bus = failingBusBuilder(store).metrics(throwingMetrics).build();

    var thrown =
        assertThrows(
            RuntimeException.class,
            () -> runWithContext(() -> bus.execute(new FailingCommand.Fail("fail-obs6-2"))));

    assertInstanceOf(
        DomainException.class,
        thrown,
        "the decider's DomainException must reach the caller, not the metrics backend's failure");
    assertEquals("Always fails", thrown.getMessage());
  }

  @Test
  void globalOffsetMismatchAfterCommitStillReportsSuccess() {
    var store =
        new InMemoryEventStore() {
          @Override
          public synchronized AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            var result = super.append(streamId, events, expectedVersion);
            // Broken store implementation: commits but reports no offsets
            return new AppendResult(List.of(), result.finalVersion());
          }
        };
    var dlq = new RecordingDlq();
    var bus =
        counterBusBuilder(store).deadLetterQueue(dlq).objectMapper(new ObjectMapper()).build();

    var resultHolder = new CommandResult[1];
    runWithContext(() -> resultHolder[0] = bus.execute(new CounterCommand.Inc("counter-pc-5")));

    assertNotNull(resultHolder[0], "events committed — offset mismatch must not fail the command");
    assertEquals(1, store.eventsFor("counter:counter-pc-5").size());
    assertTrue(dlq.entries.isEmpty(), "offset mismatch after commit must not reach the DLQ");
  }

  @Test
  void lockReleaseFailureAfterCommitStillReportsSuccess() {
    AggregateLocker brokenRelease =
        (streamId, timeout) ->
            () -> {
              throw new IllegalStateException("unlock failed");
            };
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus =
        counterBusBuilder(store)
            .locker(brokenRelease)
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();

    var resultHolder = new CommandResult[1];
    runWithContext(() -> resultHolder[0] = bus.execute(new CounterCommand.Inc("counter-pc-6")));

    assertNotNull(resultHolder[0], "lock release failure after commit must not fail the command");
    assertEquals(1, store.eventsFor("counter:counter-pc-6").size());
    assertTrue(dlq.entries.isEmpty());
  }

  // === Lock acquisition retry tests ===

  @Test
  void lockAcquisitionTimeoutIsRetried() {
    var failures = new AtomicInteger(2);
    var delegate = new LocalStripedLocker(16);
    AggregateLocker flaky =
        (streamId, timeout) -> {
          if (failures.getAndDecrement() > 0) {
            throw new LockAcquisitionException(streamId, "Timeout acquiring lock");
          }
          return delegate.acquireLock(streamId, timeout);
        };
    var store = new InMemoryEventStore();
    var bus =
        counterBusBuilder(store)
            .locker(flaky)
            .retryPolicy(new RetryPolicy(5, Duration.ofMillis(1), 1.0))
            .build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-lock-1")));

    assertEquals(
        1, store.eventsFor("counter:counter-lock-1").size(), "command succeeds after lock retry");
  }

  @Test
  void lockAcquisitionTimeoutExhaustedGoesToDlq() {
    AggregateLocker alwaysTimesOut =
        (streamId, timeout) -> {
          throw new LockAcquisitionException(streamId, "Timeout acquiring lock");
        };
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus =
        counterBusBuilder(store)
            .locker(alwaysTimesOut)
            .retryPolicy(new RetryPolicy(2, Duration.ofMillis(1), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();

    runWithContext(
        () ->
            assertThrows(
                LockAcquisitionException.class,
                () -> bus.execute(new CounterCommand.Inc("counter-lock-2"))));

    assertEquals(1, dlq.entries.size(), "exhausted lock retries publish a single DLQ entry");
    assertEquals(2, dlq.entries.getFirst().attempts(), "all retry attempts were used");
    assertEquals(
        StreamId.of(COUNTER, AggregateId.of("counter-lock-2")), dlq.entries.getFirst().streamId());
    assertEquals(AggregateId.of("counter-lock-2"), dlq.entries.getFirst().aggregateId());
  }

  @Test
  void advisoryLockExceptionIsRetried() {
    // A distributed advisory-lock timeout (PgAdvisoryLocker throws core LockException) is a
    // transient contention outcome, not a permanent failure — the bus must retry it, exactly like
    // LockAcquisitionException, rather than immediately dead-lettering the command.
    var failures = new AtomicInteger(2);
    var delegate = new LocalStripedLocker(16);
    AggregateLocker flaky =
        (streamId, timeout) -> {
          if (failures.getAndDecrement() > 0) {
            throw new LockException("Timeout acquiring advisory lock for " + streamId);
          }
          return delegate.acquireLock(streamId, timeout);
        };
    var store = new InMemoryEventStore();
    var bus =
        counterBusBuilder(store)
            .locker(flaky)
            .retryPolicy(new RetryPolicy(5, Duration.ofMillis(1), 1.0))
            .build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-adv-lock-1")));

    assertEquals(
        1,
        store.eventsFor("counter:counter-adv-lock-1").size(),
        "command succeeds after advisory-lock (LockException) retry");
  }

  @Test
  void advisoryLockExceptionExhaustedGoesToDlq() {
    AggregateLocker alwaysTimesOut =
        (streamId, timeout) -> {
          throw new LockException("Timeout acquiring advisory lock for " + streamId);
        };
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus =
        counterBusBuilder(store)
            .locker(alwaysTimesOut)
            .retryPolicy(new RetryPolicy(2, Duration.ofMillis(1), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();

    runWithContext(
        () ->
            assertThrows(
                LockException.class,
                () -> bus.execute(new CounterCommand.Inc("counter-adv-lock-2"))));

    assertEquals(
        1, dlq.entries.size(), "advisory-lock retries were attempted, then a single DLQ entry");
    assertEquals(
        2, dlq.entries.getFirst().attempts(), "all retry attempts were used before dead-lettering");
  }

  // === DLQ replay tests ===

  @Test
  void dlqReplayExecutionDoesNotRepublishToDlq() {
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus = failingBusBuilder(store).deadLetterQueue(dlq).build();

    runWithContext(
        () ->
            ScopedValue.where(VirtualThreadCommandBus.DLQ_REPLAY, true)
                .run(
                    () ->
                        assertThrows(
                            DomainException.class,
                            () -> bus.execute(new FailingCommand.Fail("fail-replay")))));

    assertTrue(
        dlq.entries.isEmpty(),
        "a failing DLQ replay must not publish a new entry — that multiplies entries per cycle");
  }

  @Test
  void dlqReplayMarkerPropagatesThroughExecuteAsync() {
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus = failingBusBuilder(store).deadLetterQueue(dlq).build();

    runWithContext(
        () ->
            ScopedValue.where(VirtualThreadCommandBus.DLQ_REPLAY, true)
                .run(
                    () -> {
                      var future = bus.executeAsync(new FailingCommand.Fail("fail-replay-async"));
                      var ex =
                          assertThrows(
                              java.util.concurrent.ExecutionException.class,
                              () -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
                      assertInstanceOf(DomainException.class, ex.getCause());
                    }));

    assertTrue(dlq.entries.isEmpty(), "DLQ_REPLAY must survive the executeAsync thread hop");
  }

  @Test
  void dlqPublishFailureDoesNotMaskOriginalFailure() {
    // Uses infraFailingBusBuilder (not failingBusBuilder): the original failure must be
    // DLQ-eligible for this test to actually exercise the DLQ-publish-failure path — a
    // DomainException would now be skipped before deadLetterQueue.publish() is ever called.
    var store = new InMemoryEventStore();
    DeadLetterQueue brokenDlq =
        new RecordingDlq() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            throw new IllegalStateException("DLQ storage down");
          }
        };
    var bus = infraFailingBusBuilder(store).deadLetterQueue(brokenDlq).build();

    runWithContext(
        () -> {
          var ex =
              assertThrows(
                  RuntimeException.class,
                  () -> bus.execute(new FailingCommand.Fail("fail-dlq-down")),
                  "the original command failure must propagate even when the DLQ publish fails");
          assertEquals(1, ex.getSuppressed().length, "the DLQ failure is attached as suppressed");
          assertInstanceOf(IllegalStateException.class, ex.getSuppressed()[0]);
          assertEquals("DLQ storage down", ex.getSuppressed()[0].getMessage());
        });
  }

  // === dlqEligible classification tests ===

  @Test
  void businessRejectionIsNotDeadLettered() {
    // A DomainException (business rejection, e.g. FailingDecider's) must never be dead-lettered:
    // replaying it later would re-append a command the domain correctly, finally rejected.
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus = failingBusBuilder(store).deadLetterQueue(dlq).build();

    runWithContext(
        () ->
            assertThrows(
                DomainException.class,
                () -> bus.execute(new FailingCommand.Fail("fail-business-rejection")),
                "the business rejection must still propagate to the caller"));

    assertTrue(
        dlq.entries.isEmpty(),
        "a business rejection (DomainException) must never be dead-lettered");
  }

  @Test
  void subjectForgottenIsNotDlqEligibleEvenWhenWrapped() {
    // A command for a crypto-shredded (GDPR-erased) subject fails at event
    // serialization with a SubjectForgottenException — a PERMANENT rejection no retry can satisfy.
    // Dead-lettering it would futilely replay the rejected write forever AND persist the subject's
    // (potentially PII) identifier in dead_letter_queue.error_message, re-materializing the data
    // the
    // erasure removed. The rejection surfaces WRAPPED on the append path (Jackson
    // JsonMappingException -> EventStoreException), so the default classifier must walk the cause
    // chain, not just check the top-level type.
    var direct = new SubjectForgottenException("cannot be re-encrypted: <subject-hash>");
    var wrapped = new RuntimeException("Failed to serialize event", new RuntimeException(direct));

    assertFalse(
        VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(direct),
        "a direct SubjectForgottenException must not be DLQ-eligible");
    assertFalse(
        VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(wrapped),
        "a wrapped SubjectForgottenException must not be DLQ-eligible");
    assertTrue(
        VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(new RuntimeException("broker down")),
        "a genuine transient infra failure must remain DLQ-eligible");
  }

  @Test
  void infraFailureIsStillDeadLettered() {
    // Guards against an over-broad dlqEligible exclusion: a non-DomainException infra failure
    // with retries exhausted must still reach the DLQ exactly once.
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus = infraFailingBusBuilder(store).deadLetterQueue(dlq).build();

    runWithContext(
        () ->
            assertThrows(
                RuntimeException.class, () -> bus.execute(new FailingCommand.Fail("fail-infra"))));

    assertEquals(
        1, dlq.entries.size(), "a genuinely infrastructural failure must still be dead-lettered");
  }

  @Test
  void nullObjectMapperOnDlqPathIsGuarded() {
    // A DLQ configured without an objectMapper must fail fast at build() with a clear message,
    // not later as an NPE on the DLQ publish path that would shadow the original command failure.
    var dlq = new RecordingDlq();
    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .locker(new LocalStripedLocker(16))
            .deadLetterQueue(dlq)
            .register(
                FAILING,
                FailingCommand.class,
                cmd ->
                    switch (cmd) {
                      case FailingCommand.Fail c -> AggregateId.of(c.id());
                    },
                new InfraFailingDecider());

    var ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(
        ex.getMessage().contains("objectMapper is required when a dead-letter queue is configured"),
        "message must clearly name the missing objectMapper, got: " + ex.getMessage());
  }

  @Test
  void interruptDuringRetryBackoffStillPublishesToDlq() {
    AggregateLocker alwaysTimesOut =
        (streamId, timeout) -> {
          throw new LockAcquisitionException(streamId, "Timeout acquiring lock");
        };
    var store = new InMemoryEventStore();
    var dlq = new RecordingDlq();
    var bus =
        counterBusBuilder(store)
            .locker(alwaysTimesOut)
            .retryPolicy(new RetryPolicy(3, Duration.ofSeconds(10), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .build();

    runWithContext(
        () -> {
          Thread.currentThread().interrupt();
          try {
            var ex =
                assertThrows(
                    CommandExecutionException.class,
                    () -> bus.execute(new CounterCommand.Inc("counter-int-1")),
                    "an interrupt mid-backoff propagates a typed exception, not a bare"
                        + " RuntimeException");
            assertInstanceOf(
                LockAcquisitionException.class,
                ex.getCause(),
                "the original transient failure is the cause");
          } finally {
            assertTrue(Thread.interrupted(), "the interrupt flag must be restored (cleared here)");
          }
        });

    assertEquals(
        1, dlq.entries.size(), "an interrupt mid-backoff must still record the command in the DLQ");
    assertEquals(1, dlq.entries.getFirst().attempts(), "interrupted after the first attempt");
    assertEquals(
        StreamId.of(COUNTER, AggregateId.of("counter-int-1")), dlq.entries.getFirst().streamId());
    assertEquals(AggregateId.of("counter-int-1"), dlq.entries.getFirst().aggregateId());
  }

  @Test
  void repeatedInterceptorsCallsAccumulate() {
    var callOrder = new CopyOnWriteArrayList<String>();
    CommandInterceptor first =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("first-before");
            return true;
          }
        };
    CommandInterceptor second =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            callOrder.add("second-before");
            return true;
          }
        };
    var store = new InMemoryEventStore();
    var bus = counterBusBuilder(store).interceptors(first).interceptors(List.of(second)).build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("counter-acc-1")));

    assertEquals(
        List.of("first-before", "second-before"),
        callOrder,
        "a later interceptors() call must add to — not silently replace — earlier ones");
  }

  // === Close tests ===

  @Test
  void closePreventsFurtherCommands() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    bus.close();

    // The closed-bus rejection is the TYPED CommandBusClosedException (an
    // IllegalStateException subtype), so saga dispatch can classify a shutdown as retry-later
    // instead of compensating a healthy flow.
    runWithContext(
        () ->
            assertThrows(
                org.streamrune.core.CommandBusClosedException.class,
                () -> bus.execute(new CounterCommand.Inc("counter-1"))));
  }

  @Test
  void closeIsIdempotent() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    bus.close();

    assertDoesNotThrow(bus::close, "close() must be idempotent per the AutoCloseable convention");
    assertTrue(bus.isClosed());
  }

  @Test
  void executeAsyncAfterCloseReturnsFailedFuture() {
    var store = new InMemoryEventStore();
    var bus = buildBus(store);

    bus.close();

    var future = bus.executeAsync(new CounterCommand.Inc("counter-after-close"));
    var ex =
        assertThrows(
            java.util.concurrent.ExecutionException.class,
            () -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
    assertInstanceOf(org.streamrune.core.CommandBusClosedException.class, ex.getCause());
    assertTrue(store.eventsFor("counter:counter-after-close").isEmpty());
  }

  @Test
  void closeWaitsForAsyncCommandAcceptedBeforeClose() throws Exception {
    var store = new InMemoryEventStore();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var blockingDecider =
        new Decider<CounterCommand, CounterState, CounterEvent>() {
          @Override
          public CounterState initialState() {
            return new CounterState();
          }

          @Override
          public List<CounterEvent> decide(CounterCommand command, CounterState state) {
            entered.countDown();
            try {
              release.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            return List.of(new CounterEvent.Incremented());
          }

          @Override
          public CounterState evolve(CounterState state, CounterEvent event) {
            return new CounterState(state.count() + 1);
          }
        };
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                blockingDecider)
            .build();

    var futureRef = new AtomicReference<CompletableFuture<CommandResult>>();
    runWithContext(() -> futureRef.set(bus.executeAsync(new CounterCommand.Inc("counter-drain"))));
    assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));

    Thread closer = Thread.ofVirtual().start(bus::close);
    assertFalse(
        closer.join(Duration.ofMillis(200)),
        "close() must block while an accepted async command is still in flight");

    release.countDown();
    assertTrue(closer.join(Duration.ofSeconds(5)), "close() returns once the command drained");
    var result = futureRef.get().get(5, java.util.concurrent.TimeUnit.SECONDS);
    assertEquals(1, result.events().size(), "the accepted command completed during the drain");
    assertEquals(1, store.eventsFor("counter:counter-drain").size());
  }

  @Test
  void executeAsync_admissionControl_rejectsPastBudget_admitsAfterOneCompletes() throws Exception {
    // N = maxInFlightAsyncCommands commands held in-flight (each on its
    // own aggregate so all N block concurrently instead of serializing on one aggregate lock) ->
    // the (N+1)th executeAsync call must fail FAST — no waiting for any of the N to complete —
    // with CommandBusOverloadedException, and the rejection metric increments exactly once.
    // Completing exactly ONE of the N frees exactly one slot: a fresh submission is then admitted
    // while the remaining N-1 are still blocked.
    var store = new InMemoryEventStore();
    int n = 2;
    var enteredLatch = new CountDownLatch(n);
    var releaseLatches = new java.util.HashMap<String, CountDownLatch>();
    for (int i = 0; i < n; i++) {
      releaseLatches.put("admission-" + i, new CountDownLatch(1));
    }
    var blockingDecider =
        new Decider<CounterCommand, CounterState, CounterEvent>() {
          @Override
          public CounterState initialState() {
            return new CounterState();
          }

          @Override
          public List<CounterEvent> decide(CounterCommand command, CounterState state) {
            String id = ((CounterCommand.Inc) command).counterId();
            // Only the N pre-registered ids block — "admission-overflow" is rejected before
            // reaching decide() at all, and "admission-after-release" deliberately has no entry
            // so it completes immediately once admitted.
            CountDownLatch releaseLatch = releaseLatches.get(id);
            if (releaseLatch != null) {
              enteredLatch.countDown();
              try {
                assertTrue(
                    releaseLatch.await(5, java.util.concurrent.TimeUnit.SECONDS),
                    "release latch for " + id + " timed out");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
            return List.of(new CounterEvent.Incremented());
          }

          @Override
          public CounterState evolve(CounterState state, CounterEvent event) {
            return new CounterState(state.count() + 1);
          }
        };
    var rejectedCalls = new AtomicInteger();
    StreamRuneMetrics countingMetrics =
        new StreamRuneMetrics() {
          @Override
          public void recordCommandAsyncRejected() {
            rejectedCalls.incrementAndGet();
          }
        };

    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .maxInFlightAsyncCommands(n)
            .metrics(countingMetrics)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                blockingDecider)
            .build()) {

      // Fill the budget: N commands, N distinct aggregates.
      var inFlightFutures = new java.util.ArrayList<CompletableFuture<CommandResult>>();
      for (int i = 0; i < n; i++) {
        int idx = i;
        runWithContext(
            () ->
                inFlightFutures.add(bus.executeAsync(new CounterCommand.Inc("admission-" + idx))));
      }
      assertTrue(
          enteredLatch.await(5, java.util.concurrent.TimeUnit.SECONDS),
          "all N submissions must be admitted and reach the blocking decider");

      // The (N+1)th: rejected immediately — no wait for any of the N to complete.
      var overflowFuture = bus.executeAsync(new CounterCommand.Inc("admission-overflow"));
      var ex =
          assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> overflowFuture.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertInstanceOf(org.streamrune.core.CommandBusOverloadedException.class, ex.getCause());
      assertTrue(
          ex.getCause() instanceof IllegalStateException,
          "must extend IllegalStateException like CommandBusClosedException");
      assertEquals(1, rejectedCalls.get(), "the rejection metric must increment exactly once");
      assertTrue(
          store.eventsFor("counter:admission-overflow").isEmpty(),
          "a rejected command attempts nothing");

      // Complete exactly ONE of the N -> exactly one slot frees up. The bus frees a command's slot
      // before completing its future, so get() returning already proves the slot is free — the
      // resubmission below does not race the completed command's thread.
      releaseLatches.get("admission-0").countDown();
      inFlightFutures.get(0).get(5, java.util.concurrent.TimeUnit.SECONDS);

      // A fresh submission is now admitted, while admission-1 is STILL blocked (proving the
      // budget accounting is precise — exactly one slot freed, not the whole thing).
      var admittedFuture = bus.executeAsync(new CounterCommand.Inc("admission-after-release"));
      var result = admittedFuture.get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertEquals(1, result.events().size());
      assertEquals(1, store.eventsFor("counter:admission-after-release").size());

      // Release the remaining blocked command so close() below does not wait out its timeout.
      releaseLatches.get("admission-1").countDown();
      inFlightFutures.get(1).get(5, java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  // === Polymorphic dispatch determinism tests ===

  interface PolyA extends Command {}

  interface PolyB extends Command {}

  record PolyCommand(String id) implements PolyA, PolyB {}

  /** Trivial decider usable for any command type; always emits one Incremented event. */
  static final class AnyCommandDecider<C extends Command>
      implements Decider<C, CounterState, CounterEvent> {
    @Override
    public CounterState initialState() {
      return new CounterState();
    }

    @Override
    public List<CounterEvent> decide(C command, CounterState state) {
      return List.of(new CounterEvent.Incremented());
    }

    @Override
    public CounterState evolve(CounterState state, CounterEvent event) {
      return new CounterState(state.count() + 1);
    }
  }

  @Test
  void exactCommandClassWinsOverEarlierRegisteredSupertype() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                POLY,
                PolyA.class,
                c -> AggregateId.of("via-interface"),
                new AnyCommandDecider<PolyA>())
            .register(
                POLY,
                PolyCommand.class,
                cmd -> AggregateId.of(cmd.id()),
                new AnyCommandDecider<PolyCommand>())
            .build();

    runWithContext(() -> bus.execute(new PolyCommand("via-exact")));

    assertEquals(
        1,
        store.eventsFor("poly:via-exact").size(),
        "a registration for the command's exact class wins over an assignable supertype");
    assertTrue(store.eventsFor("poly:via-interface").isEmpty());
  }

  @Test
  void firstRegisteredAssignableTypeWinsDeterministically() {
    var storeAb = new InMemoryEventStore();
    var busAb =
        VirtualThreadCommandBus.builder()
            .eventStore(storeAb)
            .locker(new LocalStripedLocker(16))
            .register(A, PolyA.class, c -> AggregateId.of("via-a"), new AnyCommandDecider<PolyA>())
            .register(B, PolyB.class, c -> AggregateId.of("via-b"), new AnyCommandDecider<PolyB>())
            .build();

    var resultAb = new AtomicReference<CommandResult>();
    runWithContext(() -> resultAb.set(busAb.execute(new PolyCommand("p-1"))));

    assertEquals(
        1, storeAb.eventsFor("a:via-a").size(), "the first registered assignable type wins");
    assertEquals(
        AggregateType.of("a"),
        resultAb.get().streamId().aggregateType(),
        "registration order decides the aggregate type as well as the decider");
    assertTrue(storeAb.eventsFor("b:via-b").isEmpty());

    var storeBa = new InMemoryEventStore();
    var busBa =
        VirtualThreadCommandBus.builder()
            .eventStore(storeBa)
            .locker(new LocalStripedLocker(16))
            .register(B, PolyB.class, c -> AggregateId.of("via-b"), new AnyCommandDecider<PolyB>())
            .register(A, PolyA.class, c -> AggregateId.of("via-a"), new AnyCommandDecider<PolyA>())
            .build();

    var resultBa = new AtomicReference<CommandResult>();
    runWithContext(() -> resultBa.set(busBa.execute(new PolyCommand("p-2"))));

    assertEquals(
        1, storeBa.eventsFor("b:via-b").size(), "registration order governs dispatch, not hashing");
    assertEquals(
        AggregateType.of("b"),
        resultBa.get().streamId().aggregateType(),
        "registration order decides the aggregate type as well as the decider");
    assertTrue(storeBa.eventsFor("a:via-a").isEmpty());
  }

  // === Registration rule: refused at the register call ===

  @Test
  void register_refusesADuplicateCommandType_atTheCall() {
    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .register(
                PRODUCT, CreateProduct.class, c -> AggregateId.of(c.sku()), new ProductDecider());
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.register(
                    PRODUCT,
                    CreateProduct.class,
                    c -> AggregateId.of(c.sku()),
                    new ProductDecider()));
    assertTrue(
        ex.getMessage()
            .startsWith("command type " + CreateProduct.class.getName() + " is already registered"),
        ex.getMessage());
  }

  @Test
  void register_refusesOverlappingRootsWithDifferentTypes() {
    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .register(
                AggregateType.of("poly"),
                PolyA.class,
                c -> AggregateId.of("x"),
                new AnyCommandDecider<PolyA>());
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.register(
                    AggregateType.of("other"),
                    PolyCommand.class,
                    c -> AggregateId.of(c.id()),
                    new AnyCommandDecider<PolyCommand>()));
    assertTrue(
        ex.getMessage().contains("overlap (one is assignable to the other)"), ex.getMessage());
  }

  @Test
  void register_refusesOneDeciderInstanceUnderTwoTypes() {
    var shared = new AnyCommandDecider<PolyA>();
    // Deciders are invariant in their command type, so reusing one instance for two command
    // roots takes a cast — exactly the deliberate reuse the rule is about.
    @SuppressWarnings({"unchecked", "rawtypes"})
    Decider<PolyB, CounterState, CounterEvent> sameInstance = (Decider) shared;
    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .register(AggregateType.of("a"), PolyA.class, c -> AggregateId.of("x"), shared);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.register(
                    AggregateType.of("b"), PolyB.class, c -> AggregateId.of("y"), sameInstance));
    assertTrue(
        ex.getMessage()
            .startsWith("this decider instance is already registered under aggregate type 'a'"),
        ex.getMessage());
  }

  @Test
  void register_refusesATypeBuiltThroughTheLenientDoor() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            VirtualThreadCommandBus.builder()
                .eventStore(new InMemoryEventStore())
                .register(
                    new AggregateType("Product"),
                    CreateProduct.class,
                    c -> AggregateId.of(c.sku()),
                    new ProductDecider()));
  }

  @Test
  void register_aRefusedRegistrationIsNotStored() {
    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .register(
                PRODUCT, CreateProduct.class, c -> AggregateId.of(c.sku()), new ProductDecider());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder.register(
                new AggregateType("Inventory"),
                ReceiveShipment.class,
                c -> AggregateId.of(c.sku()),
                new StockDecider()));

    try (var bus = builder.build()) {
      assertEquals(java.util.Set.of(CreateProduct.class), bus.registeredCommandTypes());
    }
  }

  @Test
  void build_logsOneInfoLinePerRegistration() {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(VirtualThreadCommandBus.class);
    var previous = logger.getLevel();
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    try {
      VirtualThreadCommandBus.builder()
          .eventStore(new InMemoryEventStore())
          .register(
              PRODUCT, CreateProduct.class, c -> AggregateId.of(c.sku()), new ProductDecider())
          .register(
              INVENTORY, ReceiveShipment.class, c -> AggregateId.of(c.sku()), new StockDecider())
          .build()
          .close();
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
    }
    var lines =
        appender.list.stream()
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .filter(m -> m.startsWith("Registered aggregate type"))
            .toList();
    assertEquals(
        List.of(
            "Registered aggregate type 'product' for command root CreateProduct (2 registrations)",
            "Registered aggregate type 'inventory' for command root ReceiveShipment (2"
                + " registrations)"),
        lines);
  }

  // === Event timestamps are stamped at append time, not at request creation (#3) ===

  /** A clock whose instant advances by a fixed step on every read. */
  static final class SteppingClock extends java.time.Clock {
    private final java.time.ZoneId zone;
    private java.time.Instant current;
    private final java.time.Duration step;

    SteppingClock(java.time.Instant start, java.time.Duration step) {
      this(start, step, java.time.ZoneOffset.UTC);
    }

    private SteppingClock(java.time.Instant start, java.time.Duration step, java.time.ZoneId zone) {
      this.current = start;
      this.step = step;
      this.zone = zone;
    }

    @Override
    public java.time.ZoneId getZone() {
      return zone;
    }

    @Override
    public java.time.Clock withZone(java.time.ZoneId z) {
      return new SteppingClock(current, step, z);
    }

    @Override
    public synchronized java.time.Instant instant() {
      java.time.Instant snapshot = current;
      current = current.plus(step);
      return snapshot;
    }
  }

  /** Decider that emits two events per command, to exercise per-event append-time stamping. */
  static final class TwoEventDecider
      implements Decider<CounterCommand, CounterState, CounterEvent> {
    @Override
    public CounterState initialState() {
      return new CounterState();
    }

    @Override
    public List<CounterEvent> decide(CounterCommand command, CounterState state) {
      return List.of(new CounterEvent.Incremented(), new CounterEvent.Incremented());
    }

    @Override
    public CounterState evolve(CounterState state, CounterEvent event) {
      return new CounterState(state.count() + 1);
    }
  }

  @Test
  void twoCommandsUnderOneBoundContextGetDistinctAppendTimeTimestamps() {
    var store = new InMemoryEventStore();
    // Context created at a fixed instant in the past; the bus must NOT reuse it for event stamps.
    var contextInstant = Instant.parse("2020-01-01T00:00:00Z");
    var clock = new SteppingClock(Instant.parse("2026-06-13T10:00:00Z"), Duration.ofSeconds(1));
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .clock(clock)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-ts"), contextInstant, Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(
            () -> {
              bus.execute(new CounterCommand.Inc("counter-ts"));
              bus.execute(new CounterCommand.Inc("counter-ts"));
            });

    var events = store.eventsFor("counter:counter-ts");
    assertEquals(2, events.size());
    Instant t1 = events.get(0).metadata().timestamp();
    Instant t2 = events.get(1).metadata().timestamp();
    assertNotEquals(
        contextInstant,
        t1,
        "event timestamp must be append-time, not the request-creation instant");
    assertNotEquals(
        t1, t2, "events of distinct commands under one context get distinct timestamps");
    assertTrue(t2.isAfter(t1), "later command's event is stamped later");
    // Correlation from the bound context is preserved.
    assertEquals("corr-ts", events.get(0).metadata().correlationId().value());
    assertEquals("corr-ts", events.get(1).metadata().correlationId().value());
  }

  @Test
  void multiEventCommandStampsEachEventAtAppendTime() {
    var store = new InMemoryEventStore();
    var clock = new SteppingClock(Instant.parse("2026-06-13T10:00:00Z"), Duration.ofSeconds(1));
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .clock(clock)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new TwoEventDecider())
            .build();

    var ctx =
        new StreamRuneContext.RequestContext(
            null,
            null,
            CorrelationId.of("corr-multi"),
            Instant.parse("2020-01-01T00:00:00Z"),
            Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(() -> bus.execute(new CounterCommand.Inc("counter-multi")));

    var events = store.eventsFor("counter:counter-multi");
    assertEquals(2, events.size());
    Instant e1 = events.get(0).metadata().timestamp();
    Instant e2 = events.get(1).metadata().timestamp();
    assertNotEquals(
        e1, e2, "each event of a single multi-event command is stamped at its own append time");
    assertTrue(e2.isAfter(e1));
  }

  @Test
  void unboundContextStampsEventsViaClock() {
    var store = new InMemoryEventStore();
    var fixed = Instant.parse("2026-06-13T12:00:00Z");
    var clock = java.time.Clock.fixed(fixed, java.time.ZoneOffset.UTC);
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .clock(clock)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    // No bound context.
    bus.execute(new CounterCommand.Inc("counter-unbound"));

    var events = store.eventsFor("counter:counter-unbound");
    assertEquals(1, events.size());
    assertEquals(fixed, events.getFirst().metadata().timestamp());
  }

  // === Two aggregate types sharing an id value ===

  private static final AggregateType PRODUCT = AggregateType.of("product");
  private static final AggregateType INVENTORY = AggregateType.of("inventory");

  record CreateProduct(String sku) implements Command {}

  record ReceiveShipment(String sku) implements Command {}

  record ProductCreated() implements DomainEvent {}

  record ShipmentReceived() implements DomainEvent {}

  record ProductState(int events) implements AggregateState {}

  record StockState(int events) implements AggregateState {}

  /** Throws on any event that is not its own: proves it is never handed another type's history. */
  static final class ProductDecider implements Decider<CreateProduct, ProductState, DomainEvent> {
    @Override
    public ProductState initialState() {
      return new ProductState(0);
    }

    @Override
    public List<DomainEvent> decide(CreateProduct command, ProductState state) {
      return List.of(new ProductCreated());
    }

    @Override
    public ProductState evolve(ProductState state, DomainEvent event) {
      if (!(event instanceof ProductCreated)) {
        throw new IllegalStateException("product saw " + event.getClass().getSimpleName());
      }
      return new ProductState(state.events() + 1);
    }
  }

  static final class StockDecider implements Decider<ReceiveShipment, StockState, DomainEvent> {
    @Override
    public StockState initialState() {
      return new StockState(0);
    }

    @Override
    public List<DomainEvent> decide(ReceiveShipment command, StockState state) {
      return List.of(new ShipmentReceived());
    }

    @Override
    public StockState evolve(StockState state, DomainEvent event) {
      if (!(event instanceof ShipmentReceived)) {
        throw new IllegalStateException("inventory saw " + event.getClass().getSimpleName());
      }
      return new StockState(state.events() + 1);
    }
  }

  @Test
  void twoDecidersSharingAnIdValue_writeTwoStreams_andNeverFoldEachOthersEvents() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                PRODUCT, CreateProduct.class, c -> AggregateId.of(c.sku()), new ProductDecider())
            .register(
                INVENTORY, ReceiveShipment.class, c -> AggregateId.of(c.sku()), new StockDecider())
            .build();
    var created = new AtomicReference<CommandResult>();
    var secondShipment = new AtomicReference<CommandResult>();

    runWithContext(
        () -> {
          created.set(bus.execute(new CreateProduct("sku-1")));
          bus.execute(new ReceiveShipment("sku-1"));
          // The second shipment folds the stream's history: it must see only ShipmentReceived.
          secondShipment.set(bus.execute(new ReceiveShipment("sku-1")));
        });

    assertEquals(StreamId.of(PRODUCT, AggregateId.of("sku-1")), created.get().streamId());
    assertEquals(StreamId.of(INVENTORY, AggregateId.of("sku-1")), secondShipment.get().streamId());
    assertEquals(1, store.eventsFor("product:sku-1").size());
    assertEquals(2, store.eventsFor("inventory:sku-1").size());
    assertEquals(1, store.eventsFor("inventory:sku-1").getFirst().version().value());
    assertEquals(2, secondShipment.get().finalVersion().value());
  }

  @Test
  void anIdOverTheColumnBound_failsBeforeAnyInterceptorRuns() {
    var store = new InMemoryEventStore();
    var interceptorCalls = new AtomicInteger();
    CommandInterceptor recording =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandInterceptor.CommandContext ctx) {
            interceptorCalls.incrementAndGet();
            return true;
          }

          @Override
          public void onError(CommandInterceptor.CommandContext ctx, Throwable error) {
            interceptorCalls.incrementAndGet();
          }
        };
    var recorded = new CopyOnWriteArrayList<String>();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .metrics(recordingMetrics(recorded))
            .interceptors(recording)
            .register(
                COUNTER,
                CounterCommand.class,
                // the lenient door, as a saga deriving a target from stored state would use
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> new AggregateId(c.counterId());
                    },
                new CounterDecider())
            .build();
    String tooLong = "x".repeat(256);

    runWithContext(
        () -> {
          var ex =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> bus.execute(new CounterCommand.Inc(tooLong)));
          assertTrue(ex.getMessage().contains("255"), ex.getMessage());
        });

    assertEquals(
        0, interceptorCalls.get(), "no interceptor observes a command whose stream is invalid");
    assertTrue(store.eventsFor("counter:" + tooLong).isEmpty());
    // Exactly what the extraction-failure path records: dispatched before extraction, failed in
    // the extraction catch, duration in the finally — nothing else.
    assertEquals(List.of("dispatched", "failed", "duration"), recorded);
  }

  /** Records the name of every metric the bus emits on the paths these tests drive. */
  private static StreamRuneMetrics recordingMetrics(List<String> recorded) {
    return new StreamRuneMetrics() {
      @Override
      public void recordCommandDispatched() {
        recorded.add("dispatched");
      }

      @Override
      public void recordCommandSucceeded() {
        recorded.add("succeeded");
      }

      @Override
      public void recordCommandFailed() {
        recorded.add("failed");
      }

      @Override
      public void recordCommandDuration(long durationNanos) {
        recorded.add("duration");
      }

      @Override
      public void recordCommandRetried(String commandType) {
        recorded.add("retried");
      }

      @Override
      public void recordCommandShortCircuited(String commandType) {
        recorded.add("shortCircuited");
      }

      @Override
      public void recordInboxReplayHit(String commandType) {
        recorded.add("replayHit");
      }

      @Override
      public void recordDeadLetterPublished(String commandType) {
        recorded.add("deadLetter");
      }

      @Override
      public void recordLockWait(long durationNanos) {
        recorded.add("lockWait");
      }

      @Override
      public void recordEventAppended() {
        recorded.add("appended");
      }

      @Override
      public void recordEventReplayed() {
        recorded.add("replayed");
      }
    };
  }

  @Test
  void twoTypesSharingAnIdValue_snapshotIntoTwoRows() {
    // The real streamrune-test store: the local test store's saveSnapshot is a no-op.
    var store = new org.streamrune.test.InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .snapshotPolicy(SnapshotPolicy.everyNEvents(1))
            .register(
                PRODUCT, CreateProduct.class, c -> AggregateId.of(c.sku()), new ProductDecider())
            .register(
                INVENTORY, ReceiveShipment.class, c -> AggregateId.of(c.sku()), new StockDecider())
            .build();

    runWithContext(
        () -> {
          bus.execute(new CreateProduct("sku-1"));
          bus.execute(new ReceiveShipment("sku-1"));
          bus.execute(new ReceiveShipment("sku-1"));
        });

    assertEquals(
        new ProductState(1),
        store.load(StreamId.of(PRODUCT, AggregateId.of("sku-1"))).snapshotState());
    assertEquals(
        new StockState(2),
        store.load(StreamId.of(INVENTORY, AggregateId.of("sku-1"))).snapshotState());
  }

  @Test
  void interceptorsSeeTheAggregateType_andAVetoCarriesTheTypedStream() {
    var seen = new AtomicReference<CommandInterceptor.CommandContext>();
    CommandInterceptor veto =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandInterceptor.CommandContext ctx) {
            seen.set(ctx);
            return false;
          }
        };
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .locker(new LocalStripedLocker(16))
            .interceptors(veto)
            .register(
                COUNTER,
                CounterCommand.class,
                cmd ->
                    switch (cmd) {
                      case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                    },
                new CounterDecider())
            .build();

    runWithContext(
        () -> {
          var result = bus.execute(new CounterCommand.Inc("c-1"));
          assertTrue(result.vetoed());
          assertEquals(StreamId.of(COUNTER, AggregateId.of("c-1")), result.streamId());
        });

    assertEquals(COUNTER, seen.get().aggregateType());
    assertEquals(StreamId.of(COUNTER, AggregateId.of("c-1")), seen.get().streamId());
  }
}
