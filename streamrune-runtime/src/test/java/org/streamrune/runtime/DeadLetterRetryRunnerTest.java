package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.invoke.MethodHandles;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.runtime.dlqfixture.NonPublicCommands;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.MutableClock;
import org.streamrune.test.UnregisteredSealedTypes;

class DeadLetterRetryRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  /**
   * A trace id carrying a Unicode control character, as {@code dead_letter_queue.trace_id} holds it
   * for a command that ran under a context built with the plain {@code RequestContext} constructor
   * (which does not run the ingress check). Written as an escape so the source file itself stays
   * control-character free.
   */
  private static final String CONTROL_CHAR_TRACE = "trace" + (char) 0x01 + "orig";

  InMemoryDeadLetterQueue dlq;
  CommandBus commandBus;
  ObjectMapper objectMapper;
  DeadLetterRetryPolicy policy;

  @BeforeEach
  void setUp() {
    dlq = new InMemoryDeadLetterQueue();
    commandBus = mock(CommandBus.class);
    objectMapper = new ObjectMapper();
    // Zero initial delay: entries are due immediately, so tests not about backoff scheduling
    // can call processBatch() right after publishing.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
  }

  private DeadLetterRetryRunner runner() {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(TestCmd.class)
        .build();
  }

  private void publishTestEntry(String id) {
    try {
      dlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              objectMapper.writeValueAsString(new TestCmd("hello")),
              TestCmd.class.getName(),
              CommandId.of(id),
              StreamId.of(TYPE, AggregateId.of("s1")),
              "Err",
              "msg",
              1,
              Instant.now(),
              null,
              null,
              null,
              null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void publishContextEntry(String id, String correlation, String user, String trace) {
    try {
      dlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              objectMapper.writeValueAsString(new TestCmd("hello")),
              TestCmd.class.getName(),
              CommandId.of(id),
              StreamId.of(TYPE, AggregateId.of("s1")),
              "Err",
              "msg",
              1,
              Instant.now(),
              correlation != null ? CorrelationId.of(correlation) : null,
              user != null ? UserId.of(user) : null,
              trace != null ? TraceId.of(trace) : null,
              null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void successfulRetryDiscardsEntry() {
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    runner().processBatch();

    assertTrue(dlq.all().isEmpty());
    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
  }

  @Test
  void failedRetryIncrementsDlqAttempts() {
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    runner().processBatch();

    assertEquals(1, dlq.all().size());
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
    assertNotNull(dlq.all().getFirst().lastAttemptAt());
  }

  // ── A closed-bus admission refusal is not a failed attempt ──

  @Test
  void closedBusAdmissionRefusalDoesNotCountAFailedAttempt() {
    // A CommandBusClosedException means the command was REFUSED ADMISSION — nothing was attempted,
    // nothing was decided. Counting it as a failed attempt lets shutdown windows age entries
    // toward the terminal drop for failures that say nothing about the command. The entry must
    // stay completely untouched for the next run.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new CommandBusClosedException("bus is shutting down"));

    runner().processBatch();

    assertEquals(1, dlq.all().size(), "the entry must be retained");
    assertEquals(0, dlq.all().getFirst().dlqAttempts(), "no attempt counted during shutdown");
    assertNull(dlq.all().getFirst().lastAttemptAt(), "the entry stays untouched for the next run");
  }

  @Test
  void wrappedClosedBusRefusalDoesNotCountAFailedAttemptEither() {
    // The refusal may surface wrapped (a dispatch adapter re-wrapping the bus's throw) — the
    // cause chain is what carries the signal, not the top-level frame.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(
            new RuntimeException(
                "dispatch failed", new CommandBusClosedException("bus is shutting down")));

    runner().processBatch();

    assertEquals(1, dlq.all().size());
    assertEquals(0, dlq.all().getFirst().dlqAttempts());
    assertNull(dlq.all().getFirst().lastAttemptAt());
  }

  @Test
  void closedBusRefusalNeverTriggersTheTerminalTransition() {
    // Sharpest form of the defect: maxRetries=1 with discardAfterMaxRetries=true means ONE counted
    // attempt permanently DROPS the command. A node bouncing during a shutdown window must not be
    // able to destroy the entry via an admission refusal.
    policy = new DeadLetterRetryPolicy(1, Duration.ZERO, 2.0, true);
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new CommandBusClosedException("bus is shutting down"));

    runner().processBatch();

    assertEquals(1, dlq.all().size(), "the entry must survive the shutdown window untouched");
    assertEquals(0, dlq.all().getFirst().dlqAttempts());
  }

  // ── DLQ replay resolves the persisted fully-qualified class name ─────────────────────

  /** Two commands sharing the simple name "CancelCommand" in distinct (nested) namespaces. */
  static final class OrdersNs {
    public record CancelCommand(String id) implements Command {}
  }

  static final class BillingNs {
    public record CancelCommand(String id) implements Command {}
  }

  private DeadLetterRetryRunner fqnRunner() {
    // Mirrors the auto-configs: the registry is keyed by fully-qualified class name.
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(OrdersNs.CancelCommand.class)
        .registerCommand(BillingNs.CancelCommand.class)
        .build();
  }

  private void publishRaw(String commandType, String payloadJson, String id) {
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            payloadJson,
            commandType,
            CommandId.of(id),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
  }

  @Test
  void simpleNameRow_withCollidingRegisteredClasses_isUnresolvable_neverRoutesToEitherDecider()
      throws Exception {
    // A commandType that is not a registered class's fully-qualified name is unresolvable. Here it
    // is the simple name "CancelCommand", which two FQN-registered commands
    // share: routing the payload to either decider risks committing wrong-context events, so the
    // runner must execute nothing and record a failed attempt (the entry ages toward discard).
    publishRaw(
        "CancelCommand",
        objectMapper.writeValueAsString(new OrdersNs.CancelCommand("o-1")),
        "ambiguous-1");

    fqnRunner().processBatch();

    verify(commandBus, never()).execute(any(Command.class), any(IdempotencyKey.class));
    assertEquals(1, dlq.all().size(), "an unresolvable entry must stay queued, not be discarded");
    assertEquals(
        1,
        dlq.all().getFirst().dlqAttempts(),
        "an unresolvable entry must record a failed attempt");
  }

  @Test
  void fqnRow_routesToTheExactRegisteredClass() throws Exception {
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);
    // A new-format row stores the FQN → resolves to exactly OrdersNs.CancelCommand, never Billing.
    publishRaw(
        OrdersNs.CancelCommand.class.getName(),
        objectMapper.writeValueAsString(new OrdersNs.CancelCommand("o-9")),
        "fqn-1");

    fqnRunner().processBatch();

    ArgumentCaptor<Command> captor = ArgumentCaptor.forClass(Command.class);
    verify(commandBus).execute(captor.capture(), any(IdempotencyKey.class));
    assertInstanceOf(OrdersNs.CancelCommand.class, captor.getValue());
  }

  @Test
  void registerCommand_keysByTheFullyQualifiedName_soOnlyThePersistedFqnResolves()
      throws Exception {
    // registerCommand(Class) is the registry's only entry point and keys by getName() — the
    // same function the bus applies when it persists the row — so the persisted FQN resolves by
    // one exact lookup, and nothing else does: a row carrying the simple name of the one
    // registered class is unresolvable, records a failed attempt, and dispatches nothing.
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(OrdersNs.CancelCommand.class)
            .build();
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);
    publishRaw(
        OrdersNs.CancelCommand.class.getName(),
        objectMapper.writeValueAsString(new OrdersNs.CancelCommand("o-1")),
        "fqn-1");
    publishRaw(
        "CancelCommand",
        objectMapper.writeValueAsString(new OrdersNs.CancelCommand("o-2")),
        "simple-1");

    runner.processBatch();

    ArgumentCaptor<Command> captor = ArgumentCaptor.forClass(Command.class);
    verify(commandBus).execute(captor.capture(), any(IdempotencyKey.class));
    assertEquals(new OrdersNs.CancelCommand("o-1"), captor.getValue());
    assertEquals(1, dlq.all().size(), "only the simple-name row stays queued");
    assertEquals(CommandId.of("simple-1"), dlq.all().getFirst().commandId());
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "and it records a failed attempt");
  }

  /**
   * A sealed command hierarchy, registered with the bus by its root the way the guides register
   * {@code OrderCommand.class}. A nested sealed level and a non-sealed leaf pin the recursion.
   */
  public sealed interface ShopCommand extends Command {
    record Place(String id) implements ShopCommand {}

    sealed interface Refund extends ShopCommand {
      record Full(String id) implements Refund {}

      record Partial(String id, int cents) implements Refund {}
    }

    non-sealed class Custom implements ShopCommand {
      public String id;
    }
  }

  @Test
  void registerCommand_sealedRoot_resolvesEveryPermittedCommandByTheFqnTheBusPersists()
      throws Exception {
    // The bus persists command.getClass().getName() — the CONCRETE class — while a decider
    // registration (and therefore the auto-configs' seeding) names the sealed root. Keying only
    // the root's name left every row of the hierarchy unresolvable: each aged out as "Unknown
    // command type" without ever executing.
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(ShopCommand.class)
            .build();
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);
    var custom = new ShopCommand.Custom();
    custom.id = "c-1";
    publishRaw(
        ShopCommand.Place.class.getName(),
        objectMapper.writeValueAsString(new ShopCommand.Place("p-1")),
        "sealed-place");
    publishRaw(
        ShopCommand.Refund.Partial.class.getName(),
        objectMapper.writeValueAsString(new ShopCommand.Refund.Partial("r-1", 250)),
        "sealed-partial");
    publishRaw(
        ShopCommand.Custom.class.getName(),
        objectMapper.writeValueAsString(custom),
        "sealed-custom");

    runner.processBatch();

    ArgumentCaptor<Command> captor = ArgumentCaptor.forClass(Command.class);
    verify(commandBus, times(3)).execute(captor.capture(), any(IdempotencyKey.class));
    var replayed = captor.getAllValues();
    assertTrue(replayed.contains(new ShopCommand.Place("p-1")), "the Place row replays");
    assertTrue(
        replayed.contains(new ShopCommand.Refund.Partial("r-1", 250)),
        "the row of a nested sealed level replays");
    assertTrue(
        replayed.stream()
            .anyMatch(
                c ->
                    c instanceof ShopCommand.Custom replayedCustom
                        && "c-1".equals(replayedCustom.id)),
        "the row of a non-sealed permitted class replays");
    assertTrue(dlq.all().isEmpty(), "every row of the hierarchy replays and is discarded");
  }

  @Test
  void
      registerCommand_sealedRootWhosePermittedSubclassesCannotBeRead_refusesInsteadOfRegisteringTheRootAlone() {
    // The native-image shape: a sealed root not registered for reflection reports isSealed() and
    // NO permitted subclasses. Registering the root alone left every concrete row of the hierarchy
    // "Unknown command type" at replay time — measured in the Spring demo's native binary.
    Class<?> unregistered = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());
    var builder = DeadLetterRetryRunner.builder();

    var thrown =
        assertThrows(IllegalStateException.class, () -> builder.registerCommand(unregistered));

    assertTrue(thrown.getMessage().contains(unregistered.getName()), thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("dead-letter command registration"), thrown.getMessage());
  }

  @Test
  void registerCommand_rejectsANullClass() {
    var builder = DeadLetterRetryRunner.builder();
    var thrown = assertThrows(IllegalArgumentException.class, () -> builder.registerCommand(null));
    assertEquals("commandClass is required", thrown.getMessage());
  }

  // ── A VETO on replay is a rejection, not a success ────────────────────

  private static CommandBus.CommandResult vetoedResult() {
    // A short-circuit result an interceptor's before()==false produces: no events, reason=VETOED.
    return new CommandBus.CommandResult(
        List.of(),
        StreamId.of(TYPE, AggregateId.of("s1")),
        Version.initial(),
        List.of(),
        List.of(),
        "MaintenanceModeInterceptor",
        CommandBus.ShortCircuitReason.VETOED);
  }

  private static CommandBus.CommandResult idempotentReplayResult() {
    // A successful idempotent replay of a prior keyed execution: events were persisted then, so
    // this
    // IS a success and the entry should be discarded.
    return new CommandBus.CommandResult(
        List.of(),
        StreamId.of(TYPE, AggregateId.of("s1")),
        Version.initial(),
        List.of(),
        List.of(),
        "dlq-replay:cmd-1",
        CommandBus.ShortCircuitReason.IDEMPOTENT_REPLAY);
  }

  @Test
  void vetoedReplayIsNotDiscardedAndBumpsAttempts() {
    // A veto interceptor (maintenance mode / tenant-suspended / kill-switch) returns a VETOED
    // CommandResult WITHOUT throwing: executeCounted returns it. The runner must treat that as a
    // NON-success — the command never ran and produced no events — so the entry stays queued and
    // its
    // attempt counter is bumped, instead of being silently discarded as "DLQ retry succeeded".
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenReturn(vetoedResult());

    runner().processBatch();

    assertEquals(
        1, dlq.all().size(), "a vetoed replay must NOT be discarded — the command never ran");
    assertEquals(
        1, dlq.all().getFirst().dlqAttempts(), "a vetoed replay must bump the attempt count");
    assertNotNull(dlq.all().getFirst().lastAttemptAt());
  }

  @Test
  void idempotentReplayResultIsTreatedAsSuccessAndDiscards() {
    // The other short-circuit case must NOT regress: an IDEMPOTENT_REPLAY is a success (its events
    // were persisted by the original keyed execution), so the entry is correctly discarded.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenReturn(idempotentReplayResult());

    runner().processBatch();

    assertTrue(
        dlq.all().isEmpty(),
        "an idempotent-replay result is a success — the entry must still be discarded");
  }

  @Test
  void vetoedReplayOnUnkeyedBusIsNotDiscarded() {
    // Same guarantee on the unkeyed path (no inbox): execute(Command) returns a VETOED result.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(false);
    when(commandBus.execute(any(Command.class))).thenReturn(vetoedResult());

    runner().processBatch();

    assertEquals(1, dlq.all().size(), "a vetoed unkeyed replay must NOT be discarded");
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void exhaustedRetriesWithDiscardRemovesEntry() {
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, true);
    publishTestEntry("cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, Instant.now());
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    runner().processBatch();

    assertTrue(dlq.all().isEmpty(), "entry should be discarded after exhausting retries");
  }

  @Test
  void exhaustedRetriesWithoutDiscardLeavesEntry() {
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
    publishTestEntry("cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, Instant.now());
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    runner().processBatch();

    assertEquals(1, dlq.all().size());
    assertEquals(3, dlq.all().getFirst().dlqAttempts());
  }

  // ── Retry-exhaustion metric (permanent command drop) ───────────────────────────────────

  private DeadLetterRetryRunner runnerWithMetrics(StreamRuneMetrics metrics) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(TestCmd.class)
        .metrics(metrics)
        .build();
  }

  @Test
  void processBatch_samplesCommandDlqDepthAndDegradationEachCycle() {
    // The runner samples the command DLQ depth gauge (streamrune.dlq.pending) and its own
    // degradation gauge every cycle, BEFORE the leadership gate, so operators can alert on a
    // growing
    // command DLQ and a stalled runner instead of scraping ERROR logs.
    var metrics = new RecordingStreamRuneMetrics();
    publishTestEntry("cmd-1");
    publishTestEntry("cmd-2");
    // Not due for retry, so processBatch does not execute anything — but the depth is still
    // sampled.
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);

    runnerWithMetrics(metrics).processBatch();

    assertEquals(2, metrics.lastDlqBacklog(), "DLQ depth gauge must reflect the 2 queued entries");
    assertEquals(1, metrics.count("dlq.backlog"), "sampled once per cycle");
    assertEquals(0, metrics.lastDlqRetryDegradation(), "no failures on a clean direct cycle");
    assertEquals(1, metrics.count("dlq.retry.degradation"), "degradation gauge sampled once");
  }

  @Test
  void retryExhaustion_firesDlqExhaustedCounterExactlyOnce_discardEnabled() {
    var metrics = new RecordingStreamRuneMetrics();
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, true);
    publishTestEntry("cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, Instant.now()); // next failure hits maxRetries=3
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    var runner = runnerWithMetrics(metrics);
    runner.processBatch(); // crosses maxRetries -> terminal exhaustion (and discard)
    runner.processBatch(); // entry is gone; the counter must NOT fire again

    assertTrue(dlq.all().isEmpty(), "discard policy removes the exhausted entry");
    assertEquals(
        1,
        metrics.count("command.dlqExhausted"),
        "the terminal exhaustion counter fires exactly once as the entry crosses maxRetries");
    assertEquals(1, metrics.count("command.dlqExhausted", "TestCmd"), "tagged by command type");
  }

  @Test
  void retryExhaustion_firesDlqExhaustedCounterExactlyOnce_discardDisabled_defaultPolicy() {
    var metrics = new RecordingStreamRuneMetrics();
    // Default policy: discardAfterMaxRetries=false — the exhausted entry is RETAINED and silently
    // aged out by readRetryable's dlq_attempts < maxRetries filter, so the terminal counter + ERROR
    // log are the ONLY signal that a command was permanently lost. They must still fire here.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    var runner = runnerWithMetrics(metrics);

    // Attempts 1 and 2 of 3 are non-terminal — the exhaustion counter must NOT fire yet.
    runner.processBatch(); // attempt 1/3
    runner.processBatch(); // attempt 2/3
    assertEquals(
        0,
        metrics.count("command.dlqExhausted"),
        "non-terminal failed attempts must not fire the exhaustion counter");

    runner.processBatch(); // attempt 3/3 -> terminal exhaustion
    runner.processBatch(); // entry now filtered out (dlq_attempts == maxRetries); no re-fire

    assertEquals(1, dlq.all().size(), "no-discard policy retains the exhausted entry");
    assertEquals(3, dlq.all().getFirst().dlqAttempts());
    assertEquals(
        1,
        metrics.count("command.dlqExhausted"),
        "the terminal exhaustion counter fires exactly once at maxRetries, even without discard");
    assertEquals(1, metrics.count("command.dlqExhausted", "TestCmd"), "tagged by command type");
  }

  @Test
  void dlqExhausted_isTaggedWithSimpleName_matchingThePublishedMetric() throws Exception {
    // recordDeadLetterPublished tags streamrune.dlq.published with the SIMPLE command name
    // (VirtualThreadCommandBus uses getSimpleName()), but the entry persists the FQN,
    // so recordDeadLetterExhausted(entry.commandType()) drifted to the FQN — a dashboard
    // correlating
    // published↔exhausted by the command-type tag silently stops matching. The exhausted tag must
    // be
    // the simple name too.
    var metrics = new RecordingStreamRuneMetrics();
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, true);
    String fqn = OrdersNs.CancelCommand.class.getName();
    publishRaw(fqn, objectMapper.writeValueAsString(new OrdersNs.CancelCommand("o-1")), "cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, Instant.now()); // next failure hits maxRetries=3
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(OrdersNs.CancelCommand.class)
        .metrics(metrics)
        .build()
        .processBatch();

    assertEquals(1, metrics.count("command.dlqExhausted"), "the exhaustion counter fired once");
    assertEquals(
        1,
        metrics.count("command.dlqExhausted", "CancelCommand"),
        "the exhausted metric must be tagged with the SIMPLE name (parity with dlq.published)");
    assertEquals(
        0,
        metrics.count("command.dlqExhausted", fqn),
        "the exhausted metric must NOT be tagged with the FQN");
  }

  @Test
  void unresolvableCommandTypeBumpsAttempts() {
    // An unresolvable command type must NOT starve the queue: it must count as a failed attempt
    // (recordFailedAttempt), aging toward discard-per-policy exactly like any other failure — a
    // bare skip that never increments dlqAttempts would let unresolvable entries permanently
    // occupy the oldest-first, batchSize-bounded readRetryable() window forever.
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "UnknownCmd",
            CommandId.of("cmd-1"),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    runner().processBatch();

    assertEquals(1, dlq.all().size());
    assertEquals(
        1, dlq.all().getFirst().dlqAttempts(), "an unresolvable type must bump dlqAttempts");
    assertNotNull(dlq.all().getFirst().lastAttemptAt());
    // We must still never execute anything for an unresolvable command type.
    verifyNoInteractions(commandBus);
  }

  @Test
  void unresolvableCommandTypeIsDiscardedOnceMaxRetriesReached() {
    // Once dlqAttempts reaches maxRetries under a discard-enabled policy, an unresolvable entry
    // must be discarded per policy exactly like any other exhausted entry — never stuck forever.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, true);
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "UnknownCmd",
            CommandId.of("cmd-1"),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, Instant.now());

    runner().processBatch();

    assertTrue(
        dlq.all().isEmpty(),
        "an unresolvable entry must be discarded after exhausting retries, like any other entry");
    verifyNoInteractions(commandBus);
  }

  // ========== GDPR: forgotten-subject entries must not be replayed ==========

  /** Command carrying one {@code @Encrypted} PII field, keyed off {@code customerId}. */
  public record PiiCmd(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements Command {}

  /** Nested record whose {@code @Encrypted} PII (cardHolder) is keyed off its own subjectId. */
  public record Billing(String subjectId, @Encrypted(subjectId = "subjectId") String cardHolder) {}

  /** Command whose only {@code @Encrypted} PII lives on a NESTED record. */
  public record NestedPiiCmd(String customerId, Billing billing) implements Command {}

  private DeadLetterRetryRunner cryptoAwareRunner(
      DeadLetterQueue queue, ObjectMapper cryptoAwareMapper) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(queue)
        .commandBus(commandBus)
        .objectMapper(cryptoAwareMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(PiiCmd.class)
        .build();
  }

  @Test
  void forgottenSubjectEntryIsDiscardedNotReplayed() throws Exception {
    // The entry's @Encrypted field was encrypted for "customer-1"; the subject's key is then
    // deleted (GDPR forget) BEFORE the retry runs — simulating a subject forgotten while their
    // command still sat in the DLQ. Decryption during processEntry's readValue() therefore
    // produces CryptoShreddingModule.REDACTED for that field instead of throwing. The runner must
    // detect this and discard the entry WITHOUT ever calling commandBus.execute(...) — replaying a
    // redacted command would silently corrupt domain state with the literal string "[REDACTED]".
    var cryptoEngine = new InMemoryCryptoEngine();
    var cryptoMapper = new ObjectMapper();
    cryptoMapper.registerModule(new CryptoShreddingModule(cryptoEngine));

    String payload = cryptoMapper.writeValueAsString(new PiiCmd("customer-1", "alice@example.com"));
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            payload,
            PiiCmd.class.getName(),
            CommandId.of("cmd-forgotten"),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    // Forget the subject before the replay runs.
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("customer-1"));

    cryptoAwareRunner(dlq, cryptoMapper).processBatch();

    verifyNoInteractions(commandBus);
    // The entry is not silently lost either: it is recorded as a failed attempt so it eventually
    // ages out per the DLQ retention/discard policy instead of being retried forever.
    assertEquals(1, dlq.all().size());
    assertEquals(
        1,
        dlq.all().getFirst().dlqAttempts(),
        "a discarded-as-redacted entry must still count as a failed attempt");
  }

  @Test
  void forgottenSubjectEntryWithNestedEncryptedFieldIsDiscardedNotReplayed() throws Exception {
    // Regression: the @Encrypted PII lives on a NESTED record (Billing.cardHolder).
    // CryptoShreddingModule redacts @Encrypted fields at ANY depth, so the runner's redaction scan
    // must recurse into nested records — otherwise a nested-redacted command replays with the
    // literal "[REDACTED]" string in place of the erased PII.
    var cryptoEngine = new InMemoryCryptoEngine();
    var cryptoMapper = new ObjectMapper();
    cryptoMapper.registerModule(new CryptoShreddingModule(cryptoEngine));

    String payload =
        cryptoMapper.writeValueAsString(
            new NestedPiiCmd("customer-1", new Billing("cust-nested", "Alice Smith")));
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            payload,
            NestedPiiCmd.class.getName(),
            CommandId.of("cmd-nested-forgotten"),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    // Forget the nested subject before the replay runs.
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-nested"));

    DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(cryptoMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(NestedPiiCmd.class)
        .build()
        .processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void nonRedactedEntryWithCryptoAwareMapperReplaysNormally() throws Exception {
    // Sanity/negative case for the redaction guard above: an entry whose subject was NOT forgotten
    // must still replay normally through a crypto-aware mapper — the guard must not be a blanket
    // block on encrypted commands, only on actually-redacted ones.
    var cryptoEngine = new InMemoryCryptoEngine();
    var cryptoMapper = new ObjectMapper();
    cryptoMapper.registerModule(new CryptoShreddingModule(cryptoEngine));

    String payload = cryptoMapper.writeValueAsString(new PiiCmd("customer-2", "bob@example.com"));
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            payload,
            PiiCmd.class.getName(),
            CommandId.of("cmd-ok"),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    cryptoAwareRunner(dlq, cryptoMapper).processBatch();

    verify(commandBus).execute(any(PiiCmd.class), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty(), "a non-redacted entry must replay and be discarded on success");
  }

  // ── The redaction scan reads non-public commands, and fails closed when it cannot ─────────
  //
  // containsRedacted read each record component with a plain Method.invoke. For a command record
  // that is not public and lives outside org.streamrune.runtime — which Jackson deserializes fine
  // and the framework never forbids — that throws IllegalAccessException, and the scan treated the
  // failure as "not redacted": the DLQ replayed a command whose @Encrypted field held the literal
  // "[REDACTED]", the GDPR fail-open the guard exists to prevent.
  //
  // Durable flow (dead letter). The fix adds no write: an entry the scan cannot clear goes down the
  // existing recordFailedAttempt path (updateAttempts, then the terminal transition at
  // maxRetries). Crash points: a crash before updateAttempts leaves the entry untouched, and the
  // rerun applies the same rule — dispatch only if the scan completes without finding the marker —
  // so it either fails closed again or dispatches a command verified clean (a user accessor need
  // not be deterministic, so the outcome may differ, never the rule); a crash after it is the
  // existing recordFailedAttempt crash point. The
  // multi-cycle test below pins that every rerun converges on "no dispatch" and ages the entry to
  // exhaustion instead of replaying it.

  /** JSON twin of {@code NonPublicCommands.UnreadableLabelCmd} (its own accessor throws). */
  public record LabelledPiiCmd(
      String customerId, @Encrypted(subjectId = "customerId") String email, String label)
      implements Command {}

  /** Publishes {@code twin}'s JSON as a row persisted under {@code commandClass}'s name. */
  private void publishPayload(ObjectMapper mapper, Object twin, Class<?> commandClass, String id)
      throws Exception {
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            mapper.writeValueAsString(twin),
            commandClass.getName(),
            CommandId.of(id),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
  }

  private DeadLetterRetryRunner fixtureRunner(ObjectMapper mapper, Class<?> commandClass) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(mapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(commandClass)
        .build();
  }

  private static ObjectMapper cryptoMapper(InMemoryCryptoEngine cryptoEngine) {
    var mapper = new ObjectMapper();
    mapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    return mapper;
  }

  @Test
  void nonPublicCommandInAnotherPackage_forgottenSubject_isNeverReplayed_andAgesToExhaustion()
      throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new PiiCmd("customer-1", "alice@example.com"),
        NonPublicCommands.HIDDEN_PII_CMD,
        "cmd-hidden");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("customer-1"));
    var runner = fixtureRunner(mapper, NonPublicCommands.HIDDEN_PII_CMD);

    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "the entry must stay in the DLQ");
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "and count one failed attempt");

    // Rerun convergence: every further cycle repeats the same scan to the same outcome — still no
    // dispatch — and the attempt count climbs to exhaustion under the policy (maxRetries 3,
    // discardAfterMaxRetries false: retained for inspection, no longer retried).
    runner.processBatch();
    runner.processBatch();
    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "an exhausted entry is retained under this policy");
    assertEquals(
        policy.maxRetries(),
        dlq.all().getFirst().dlqAttempts(),
        "the attempts progress to maxRetries and stop there: the exhausted entry is not re-read");
  }

  @Test
  void nonPublicCommandInAnotherPackage_forgottenNestedSubject_isNotReplayed() throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new NestedPiiCmd("customer-1", new Billing("cust-nested", "Alice Smith")),
        NonPublicCommands.HIDDEN_NESTED_PII_CMD,
        "cmd-hidden-nested");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-nested"));

    fixtureRunner(mapper, NonPublicCommands.HIDDEN_NESTED_PII_CMD).processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void nonPublicCommandInAnotherPackage_notForgotten_stillReplays() throws Exception {
    // The fix reads non-public records; it must not block them.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new PiiCmd("customer-2", "bob@example.com"),
        NonPublicCommands.HIDDEN_PII_CMD,
        "cmd-hidden-ok");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    fixtureRunner(mapper, NonPublicCommands.HIDDEN_PII_CMD).processBatch();

    verify(commandBus).execute(any(NonPublicCommands.HIDDEN_PII_CMD), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty(), "a non-redacted entry must replay and be discarded on success");
  }

  @Test
  void unreadableEncryptedComponent_failsClosed_noDispatch_onEveryCycle() throws Exception {
    // The @Encrypted component cannot be read even with access granted (its accessor throws), so
    // the scan cannot rule out "[REDACTED]". It must not guess "not redacted".
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new PiiCmd("customer-1", "alice@example.com"),
        NonPublicCommands.UNREADABLE_PII_CMD,
        "cmd-unread");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("customer-1"));
    var runner = fixtureRunner(mapper, NonPublicCommands.UNREADABLE_PII_CMD);

    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "the entry must stay in the DLQ");
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "and count one failed attempt");

    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(2, dlq.all().getFirst().dlqAttempts(), "a rerun fails closed the same way");
  }

  @Test
  void unreadableRecordOnThePathToAnEncryptedField_failsClosed() throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new NestedPiiCmd("customer-1", new Billing("cust-nested", "Alice Smith")),
        NonPublicCommands.UNREADABLE_NESTED_PII_CMD,
        "cmd-unread-nested");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-nested"));

    fixtureRunner(mapper, NonPublicCommands.UNREADABLE_NESTED_PII_CMD).processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void unreadableComponentWhoseTypeCannotBeWalked_failsClosed() throws Exception {
    // The unreadable component's type declares a malformed @Encrypted field, so the walk that
    // would show it free of PII throws instead of answering. Not provably PII-free means fail
    // closed. (A crypto-blind mapper, since CryptoShreddingModule itself rejects the type.)
    publishPayload(
        objectMapper,
        Map.of("customerId", "c-1", "pii", Map.of("subjectId", "s-1", "secret", "x")),
        NonPublicCommands.UNREADABLE_MALFORMED_PATH_CMD,
        "cmd-unread-malformed");

    fixtureRunner(objectMapper, NonPublicCommands.UNREADABLE_MALFORMED_PATH_CMD).processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void unreadableComponentThatCannotCarryAnEncryptedField_doesNotBlockTheReplay() throws Exception {
    // Fail closed only where PII can be: an unreadable plain String component can never hold an
    // @Encrypted value, and the readable @Encrypted one decrypts fine — the command replays.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new LabelledPiiCmd("customer-2", "bob@example.com", "gift"),
        NonPublicCommands.UNREADABLE_LABEL_CMD,
        "cmd-unread-label");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    fixtureRunner(mapper, NonPublicCommands.UNREADABLE_LABEL_CMD).processBatch();

    verify(commandBus)
        .execute(any(NonPublicCommands.UNREADABLE_LABEL_CMD), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty());
  }

  // ── A non-record command, or a non-record value inside one, is scanned like a record ─────────
  //
  // containsRedacted returned false at once for anything that is not a record. Yet
  // CryptoShreddingModule decrypts an @Encrypted record wherever Jackson builds it, a plain class's
  // field included, and substitutes "[REDACTED]" once the subject is forgotten; so a command class
  // (or a plain class nested in a command record) holding such a record replayed the literal
  // "[REDACTED]". The scan now walks a plain object's instance fields, its superclasses' included,
  // and keeps the fail-closed rule: a field it cannot read whose declared type can reach an
  // @Encrypted field blocks the replay.
  //
  // Durable flow (dead letter): no new write. An entry the scan refuses goes down the existing
  // recordFailedAttempt path, so the crash points are the ones pinned above: a crash before
  // updateAttempts leaves the row unchanged and the rerun reaches the same verdict (a field read
  // runs no application code, and a deleted key stays deleted); a crash after it is the existing
  // recordFailedAttempt crash point. The multi-cycle test below pins that every rerun converges on
  // "no dispatch" and ages the entry to exhaustion.

  /** A command that is a plain class, not a record, holding the @Encrypted record in a field. */
  public static final class PojoBillingCmd implements Command {
    public String customerId;
    public Billing billing;

    public PojoBillingCmd() {}

    PojoBillingCmd(String customerId, Billing billing) {
      this.customerId = customerId;
      this.billing = billing;
    }
  }

  /** Holds the @Encrypted record in a PRIVATE field that subclasses inherit. */
  public abstract static class BillingHolder {
    private Billing billing;

    public Billing getBilling() {
      return billing;
    }

    public void setBilling(Billing billing) {
      this.billing = billing;
    }
  }

  /** A plain command class whose @Encrypted record lives in an inherited private field. */
  public static final class InheritedBillingCmd extends BillingHolder implements Command {
    private String customerId;

    public String getCustomerId() {
      return customerId;
    }

    public void setCustomerId(String customerId) {
      this.customerId = customerId;
    }
  }

  /** A plain class in the middle of a record command's value graph. */
  public static final class BillingEnvelope {
    public Billing billing;

    public BillingEnvelope() {}

    BillingEnvelope(Billing billing) {
      this.billing = billing;
    }
  }

  /** A record command that reaches its @Encrypted record through a plain class. */
  public record EnvelopedPiiCmd(String customerId, BillingEnvelope envelope) implements Command {}

  /** A record command holding its @Encrypted record inside a JDK AtomicReference. */
  public record AtomicRefPiiCmd(
      String customerId, java.util.concurrent.atomic.AtomicReference<Billing> billing)
      implements Command {}

  @Test
  void atomicReferenceHeldRecord_liveSubject_isReplayed() throws Exception {
    // Control for the test below: the AtomicReference command round-trips and replays while its
    // subject's key exists, so the refusal below is the redaction scan, not a decode failure.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new AtomicRefPiiCmd(
            "customer-1",
            new java.util.concurrent.atomic.AtomicReference<>(
                new Billing("cust-atomic-live", "Alice Smith"))),
        AtomicRefPiiCmd.class,
        "cmd-atomic-live");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    fixtureRunner(mapper, AtomicRefPiiCmd.class).processBatch();

    verify(commandBus).execute(any(AtomicRefPiiCmd.class), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty());
  }

  @Test
  void atomicReferenceHeldRecord_forgottenSubject_isNotReplayed() throws Exception {
    // Jackson builds an AtomicReference's content with the bean deserializer, so
    // CryptoShreddingModule redacts the record inside it; the scan used to treat the JDK holder as
    // a leaf and replay the literal "[REDACTED]".
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new AtomicRefPiiCmd(
            "customer-1",
            new java.util.concurrent.atomic.AtomicReference<>(
                new Billing("cust-atomic", "Alice Smith"))),
        AtomicRefPiiCmd.class,
        "cmd-atomic");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-atomic"));
    var runner = fixtureRunner(mapper, AtomicRefPiiCmd.class);

    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "the entry must stay in the DLQ");
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "and count one failed attempt");
  }

  /** A record command holding its @Encrypted record as the value of a JDK Map.Entry. */
  public record EntryPiiCmd(String customerId, Map.Entry<String, Billing> billing)
      implements Command {}

  @Test
  void mapEntryHeldRecord_liveSubject_isReplayed() throws Exception {
    // Control for the test below: the Map.Entry command round-trips and replays while its
    // subject's key exists, so the refusal below is the redaction scan, not a decode failure.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new EntryPiiCmd(
            "customer-1", Map.entry("card", new Billing("cust-entry-live", "Alice Smith"))),
        EntryPiiCmd.class,
        "cmd-entry-live");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    fixtureRunner(mapper, EntryPiiCmd.class).processBatch();

    var replayed = ArgumentCaptor.forClass(Command.class);
    verify(commandBus).execute(replayed.capture(), any(IdempotencyKey.class));
    var cmd = assertInstanceOf(EntryPiiCmd.class, replayed.getValue());
    assertEquals("Alice Smith", cmd.billing().getValue().cardHolder());
    assertTrue(dlq.all().isEmpty());
  }

  @Test
  void mapEntryHeldRecord_forgottenSubject_isNeverReplayed() throws Exception {
    // Jackson's MapEntryDeserializer builds the entry's value with the bean deserializer, so
    // CryptoShreddingModule redacts the record inside it; the runtime holder is a JDK
    // AbstractMap.SimpleEntry, which the scan treated as a leaf and so replayed "[REDACTED]".
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new EntryPiiCmd("customer-1", Map.entry("card", new Billing("cust-entry", "Alice Smith"))),
        EntryPiiCmd.class,
        "cmd-entry");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-entry"));
    // Premise: the decoded entry really carries the tombstone.
    var decoded = mapper.readValue(dlq.all().getFirst().commandPayload(), EntryPiiCmd.class);
    assertEquals(CryptoShreddingModule.REDACTED, decoded.billing().getValue().cardHolder());
    var runner = fixtureRunner(mapper, EntryPiiCmd.class);

    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "the entry must stay in the DLQ");
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "and count one failed attempt");

    runner.processBatch();
    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(policy.maxRetries(), dlq.all().getFirst().dlqAttempts());
  }

  /** A plain command class holding its @Encrypted record inside a JDK AtomicReference field. */
  public static final class PojoAtomicBillingCmd implements Command {
    public String customerId;
    public java.util.concurrent.atomic.AtomicReference<Billing> billing;

    public PojoAtomicBillingCmd() {}

    PojoAtomicBillingCmd(String customerId, Billing billing) {
      this.customerId = customerId;
      this.billing = new java.util.concurrent.atomic.AtomicReference<>(billing);
    }
  }

  @Test
  void nonRecordCommand_atomicReferenceHeldRecord_liveSubject_isReplayed() throws Exception {
    // Control for the test below, on the plain-class path: the scan reaches the AtomicReference
    // through containsRedactedFields, and a live subject's record inside it decrypts and replays.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new PojoAtomicBillingCmd("customer-1", new Billing("cust-pojo-atomic-live", "Alice Smith")),
        PojoAtomicBillingCmd.class,
        "cmd-pojo-atomic-live");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    fixtureRunner(mapper, PojoAtomicBillingCmd.class).processBatch();

    var replayed = ArgumentCaptor.forClass(Command.class);
    verify(commandBus).execute(replayed.capture(), any(IdempotencyKey.class));
    var cmd = assertInstanceOf(PojoAtomicBillingCmd.class, replayed.getValue());
    assertEquals("Alice Smith", cmd.billing.get().cardHolder());
    assertTrue(dlq.all().isEmpty());
  }

  @Test
  void nonRecordCommand_atomicReferenceHeldRecord_forgottenSubject_isNeverReplayed()
      throws Exception {
    // The plain-class path into an AtomicReference: containsRedactedFields reads the field and
    // hands the holder to containsRedactedValue, whose AtomicReference arm opens it. Without that
    // arm the JDK holder is a leaf and the literal "[REDACTED]" is replayed.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new PojoAtomicBillingCmd("customer-1", new Billing("cust-pojo-atomic", "Alice Smith")),
        PojoAtomicBillingCmd.class,
        "cmd-pojo-atomic");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-pojo-atomic"));
    // Premise: Jackson builds the AtomicReference's content with the bean deserializer, so
    // CryptoShreddingModule redacts the record inside it.
    var decoded =
        mapper.readValue(dlq.all().getFirst().commandPayload(), PojoAtomicBillingCmd.class);
    assertEquals(CryptoShreddingModule.REDACTED, decoded.billing.get().cardHolder());
    var runner = fixtureRunner(mapper, PojoAtomicBillingCmd.class);

    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "the entry must stay in the DLQ");
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "and count one failed attempt");

    // Rerun convergence: the scan reads fields only and the key stays deleted, so every cycle
    // reaches the same verdict and the entry ages to exhaustion, never a dispatch.
    runner.processBatch();
    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(policy.maxRetries(), dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void unreadableAtomicReferenceOnThePathToAnEncryptedField_failsClosed() throws Exception {
    // Fail-closed for the holder itself: an AtomicReference component the scan cannot read is
    // judged by its
    // declared type, and AtomicReference<HiddenBilling> reaches an @Encrypted field through its
    // type argument. The subject is NOT forgotten, so the only reason to refuse is that the scan
    // cannot rule a redaction out.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new AtomicRefPiiCmd(
            "customer-1",
            new java.util.concurrent.atomic.AtomicReference<>(
                new Billing("cust-unread-atomic", "Alice Smith"))),
        NonPublicCommands.UNREADABLE_ATOMIC_PII_CMD,
        "cmd-unread-atomic");

    var appender = attachRunnerLogAppender();
    try {
      fixtureRunner(mapper, NonPublicCommands.UNREADABLE_ATOMIC_PII_CMD).processBatch();
    } finally {
      detachRunnerLogAppender(appender);
    }

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
    // The refusal is the fail-closed rule's, not some other decode or dispatch failure: the WARN
    // names the unreadable AtomicReference component the scan could not rule out.
    assertTrue(
        appender.list.stream()
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .anyMatch(
                m ->
                    m.contains("cannot be checked for crypto-shredded PII")
                        && m.contains("UnreadableAtomicPiiCmd.billing")
                        && m.contains("whose type can hold an @Encrypted field")),
        () -> "expected the fail-closed WARN, got " + appender.list);
  }

  @Test
  void nonRecordCommand_forgottenSubject_isNeverReplayed_andAgesToExhaustion() throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new PojoBillingCmd("customer-1", new Billing("cust-pojo", "Alice Smith")),
        PojoBillingCmd.class,
        "cmd-pojo");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-pojo"));
    var runner = fixtureRunner(mapper, PojoBillingCmd.class);

    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "the entry must stay in the DLQ");
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "and count one failed attempt");

    // Rerun convergence: every cycle reaches the same verdict and the attempts climb to
    // exhaustion (maxRetries 3, retained for inspection), never a dispatch.
    runner.processBatch();
    runner.processBatch();
    runner.processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(policy.maxRetries(), dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void nonRecordCommand_forgottenSubjectInAnInheritedPrivateField_isNotReplayed() throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    var command = new InheritedBillingCmd();
    command.setCustomerId("customer-1");
    command.setBilling(new Billing("cust-inherited", "Alice Smith"));
    publishPayload(mapper, command, InheritedBillingCmd.class, "cmd-inherited");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-inherited"));

    fixtureRunner(mapper, InheritedBillingCmd.class).processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void recordCommand_forgottenSubjectBehindAPlainClass_isNotReplayed() throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new EnvelopedPiiCmd(
            "customer-1", new BillingEnvelope(new Billing("cust-enveloped", "Alice Smith"))),
        EnvelopedPiiCmd.class,
        "cmd-enveloped");
    cryptoEngine.deleteKey(org.streamrune.core.types.SubjectId.of("cust-enveloped"));

    fixtureRunner(mapper, EnvelopedPiiCmd.class).processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void nonRecordCommand_notForgotten_stillReplays() throws Exception {
    // The scan reads plain classes; it must not block them when nothing was redacted.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    publishPayload(
        mapper,
        new PojoBillingCmd("customer-2", new Billing("cust-pojo-ok", "Bob Jones")),
        PojoBillingCmd.class,
        "cmd-pojo-ok");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    fixtureRunner(mapper, PojoBillingCmd.class).processBatch();

    verify(commandBus).execute(any(PojoBillingCmd.class), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty(), "a non-redacted entry must replay and be discarded on success");
  }

  /** A plain command class whose line points back at it once Jackson links the back-reference. */
  public static final class CyclicBillingCmd implements Command {
    public String customerId;
    @JsonManagedReference public CyclicBillingLine line;
  }

  /** The line of a {@link CyclicBillingCmd}: holds the @Encrypted record and the owner back. */
  public static final class CyclicBillingLine {
    @JsonBackReference public CyclicBillingCmd owner;
    public Billing billing;
  }

  @Test
  void nonRecordCommand_withABackReferenceCycle_isWalkedOnce_andReplays() throws Exception {
    // A plain class, unlike a record, can form a cycle through a Jackson back-reference. The scan
    // must visit each object once and finish (no StackOverflowError), then replay the clean entry.
    var cryptoEngine = new InMemoryCryptoEngine();
    var mapper = cryptoMapper(cryptoEngine);
    var command = new CyclicBillingCmd();
    command.customerId = "customer-3";
    command.line = new CyclicBillingLine();
    command.line.owner = command;
    command.line.billing = new Billing("cust-cyclic", "Carol White");
    publishPayload(mapper, command, CyclicBillingCmd.class, "cmd-cyclic");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    fixtureRunner(mapper, CyclicBillingCmd.class).processBatch();

    var replayed = ArgumentCaptor.forClass(Command.class);
    verify(commandBus).execute(replayed.capture(), any(IdempotencyKey.class));
    var cmd = assertInstanceOf(CyclicBillingCmd.class, replayed.getValue());
    assertSame(cmd, cmd.line.owner, "the deserialized command really is cyclic");
    assertEquals("Carol White", cmd.line.billing.cardHolder());
    assertTrue(dlq.all().isEmpty());
  }

  @Test
  void emptyDlqIsNoop() {
    runner().processBatch();
    verifyNoInteractions(commandBus);
  }

  @Test
  void entryInsideBackoffWindowIsSkipped() {
    // First retry is due delayForAttempt(1) after publish — at least 30 minutes with a
    // 1-hour initial delay (jitter lower bound is base/2). A just-published entry is not due.
    policy = new DeadLetterRetryPolicy(3, Duration.ofHours(1), 2.0, false);
    publishTestEntry("cmd-1");

    runner().processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(
        0,
        dlq.all().getFirst().dlqAttempts(),
        "an entry inside its backoff window keeps its attempt count");
  }

  @Test
  void entryPastBackoffWindowIsRetried() {
    // delayForAttempt(2) with multiplier 1.0 is at most 1.5 hours (jitter upper bound);
    // a last attempt one day ago is past any possible window.
    policy = new DeadLetterRetryPolicy(3, Duration.ofHours(1), 1.0, false);
    publishTestEntry("cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 1, Instant.now().minus(Duration.ofDays(1)));
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    runner().processBatch();

    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty(), "the successful retry discards the entry");
  }

  @Test
  void discardFailureAfterSuccessfulExecutionIsNotTreatedAsCommandFailure() {
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    var discardFailures = new java.util.concurrent.atomic.AtomicInteger(0);
    var brokenDiscardDlq =
        new org.streamrune.core.DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            dlq.publish(request);
          }

          @Override
          public java.util.List<DeadLetterEntry> read(int limit) {
            return dlq.read(limit);
          }

          @Override
          public java.util.List<DeadLetterEntry> readRetryable(int maxAttempts, int limit) {
            return dlq.readRetryable(maxAttempts, limit);
          }

          @Override
          public void discard(CommandId commandId) {
            discardFailures.incrementAndGet();
            throw new RuntimeException("discard unavailable");
          }

          @Override
          public void updateAttempts(CommandId commandId, int attempts, Instant lastAttemptAt) {
            dlq.updateAttempts(commandId, attempts, lastAttemptAt);
          }
        };

    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(brokenDiscardDlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(TestCmd.class)
            .build();

    runner.processBatch();

    // The command executed exactly once; the failed discard must not re-dispatch it in-cycle.
    verify(commandBus, times(1)).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertEquals(1, discardFailures.get());
    // The entry stays queued (at-least-once), but the attempt counter is bumped so a
    // persistently failing discard cannot re-execute the command forever.
    assertEquals(1, dlq.all().size());
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
    assertNotNull(dlq.all().getFirst().lastAttemptAt());
  }

  @Test
  void replayBindsDlqReplayMarkerSoFailedRetriesCannotMultiplyEntries() {
    // The runner must bind VirtualThreadCommandBus.DLQ_REPLAY around execute(): while bound,
    // the bus skips its own DLQ publish on failure, so a still-failing retry bumps the
    // original entry's attempt counter instead of adding a fresh entry every cycle
    // (unbounded entry multiplication). The bus half of this contract is pinned in
    // VirtualThreadCommandBusTest.dlqReplayExecutionDoesNotRepublishToDlq.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var markerBoundDuringReplay = new java.util.concurrent.atomic.AtomicBoolean(false);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              markerBoundDuringReplay.set(
                  VirtualThreadCommandBus.DLQ_REPLAY.isBound()
                      && Boolean.TRUE.equals(VirtualThreadCommandBus.DLQ_REPLAY.get()));
              throw new RuntimeException("still broken");
            });

    runner().processBatch();

    assertTrue(
        markerBoundDuringReplay.get(),
        "the runner must execute replays with the DLQ_REPLAY marker bound");
    assertEquals(1, dlq.all().size(), "the failed retry must not enqueue a second entry");
    assertEquals(1, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void replayRebindsOriginalRequestContextPersistedOnEntry() {
    // The entry persists the originating correlation/user/trace, and the runner rebinds them on
    // StreamRuneContext.CURRENT around execute() — so a fail-closed authorization interceptor that
    // reads the current user re-evaluates the replay as the original user (and passes), instead of
    // seeing an anonymous execution and rejecting a command that originally passed.
    publishContextEntry("cmd-1", "corr-orig", "user-orig", "trace-orig");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var observed = new java.util.concurrent.atomic.AtomicReference<RequestContext>();
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              observed.set(StreamRuneContext.capture());
              return null;
            });

    runner().processBatch();

    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    RequestContext ctx = observed.get();
    assertNotNull(ctx, "a DLQ replay must rebind the original request context");
    assertEquals("corr-orig", ctx.correlationId().value());
    assertNotNull(ctx.userId());
    assertEquals("user-orig", ctx.userId().value());
    assertNotNull(ctx.traceId());
    assertEquals("trace-orig", ctx.traceId().value());
  }

  @Test
  void replayRebindsCorrelationWhenUserAndTraceAreNull() {
    // A correlation id is enough to rebind a context (RequestContext requires one); a null user
    // and trace must round-trip as null, not blow up RequestContext / the value-type constructors.
    publishContextEntry("cmd-1", "corr-only", null, null);
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var observed = new java.util.concurrent.atomic.AtomicReference<RequestContext>();
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              observed.set(StreamRuneContext.capture());
              return null;
            });

    runner().processBatch();

    RequestContext ctx = observed.get();
    assertNotNull(ctx, "a correlation id alone is enough to rebind a context");
    assertEquals("corr-only", ctx.correlationId().value());
    assertNull(ctx.userId());
    assertNull(ctx.traceId());
  }

  @Test
  void replayWithoutPersistedContextRunsUnbound() {
    // An entry for a command that ran with no context bound carries a null correlation id — the
    // replay runs with StreamRuneContext unbound and must not throw building a context from null
    // fields.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var contextBoundDuringReplay = new java.util.concurrent.atomic.AtomicBoolean(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              contextBoundDuringReplay.set(StreamRuneContext.CURRENT.isBound());
              return null;
            });

    runner().processBatch();

    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertFalse(
        contextBoundDuringReplay.get(), "an entry with no persisted context replays unbound");
  }

  // ========== A stored id the ingress bound refuses must still replay ==========

  @Test
  void replayOfAStoredControlCharacterCorrelationIdExecutesInsteadOfBurningTheRetryLadder() {
    // Repro (chain A). A command that ran under a context built with the plain
    // RequestContext constructor (which does not run the ingress check) carries correlation id
    // "corr\r\nFORGED"; it failed transiently and was dead-lettered, and
    // dead_letter_queue.correlation_id is VARCHAR(255) (event-store baseline), so a control
    // character is perfectly storable.
    //
    // replayContextFrom() rebuilds a RequestContext from those stored columns. If the ingress bound
    // runs there, EVERY retry throws IllegalArgumentException before the bus is ever called, the
    // deterministic failure burns the whole ladder, and an already-authorized, accepted business
    // command is permanently discarded — the exact permanent-command-loss class
    // the off-request authorization rule was written to prevent, re-created through a different
    // door.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 1.0, true);
    publishContextEntry("cmd-control-char", "corr\r\nFORGED", "user-orig", CONTROL_CHAR_TRACE);
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var observed = new java.util.concurrent.atomic.AtomicReference<RequestContext>();
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              observed.set(StreamRuneContext.capture());
              return null;
            });

    runner().processBatch();

    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    RequestContext ctx = observed.get();
    assertNotNull(ctx, "the stored entry must still replay with its original context rebound");
    assertEquals(
        "corr\r\nFORGED",
        ctx.correlationId().value(),
        "the stored id is rebound VERBATIM — truncating or scrubbing it would break the very"
            + " correlation the id exists to provide");
    assertEquals(CONTROL_CHAR_TRACE, ctx.traceId().value());
    assertTrue(dlq.all().isEmpty(), "a successful replay discards the entry");
  }

  @Test
  void aControlCharacterCorrelationIdNeverExhaustsTheLadderWithoutTheCommandEverReachingTheBus() {
    // The permanent-loss shape stated end to end: drive the real runner for the entry's whole
    // retry ladder. A deterministic context-construction failure would consume every attempt
    // without the bus ever seeing the command and then discard it per policy — a lost business
    // operation whose domain events were never produced, with the logs blaming a correlation-id
    // constraint. The ladder must instead end on the first attempt, by SUCCEEDING.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 1.0, true);
    publishContextEntry("cmd-control-char", "corr" + (char) 0x07 + "id", "user-orig", null);
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    DeadLetterRetryRunner runner = runner();
    runner.processBatch();
    runner.processBatch();
    runner.processBatch();

    verify(commandBus, times(1)).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty(), "discarded because it SUCCEEDED, not because it was exhausted");
  }

  @Test
  void replayOfAStoredOverlongCorrelationIdExecutes() {
    // The length arm of the same bound. Unreachable through the shipped VARCHAR(255) schema, but a
    // custom DeadLetterQueue implementation (an in-memory or document store) has no such cap, and
    // the read path must not be the thing that discovers it.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 1.0, true);
    publishContextEntry("cmd-overlong", "c".repeat(4096), null, null);
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    runner().processBatch();

    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertTrue(dlq.all().isEmpty());
  }

  // ========== idempotent replay: cross-instance duplicate replay must not double-execute
  // ==========

  @Test
  void replayUsesIdempotencyKeyDerivedFromCommandId() {
    // Two instances (or two poll cycles) can both claim the same DLQ entry — the FOR UPDATE SKIP
    // LOCKED claim is released before execute(). A deterministic key derived from the entry's
    // commandId lets the bus's CommandInbox turn a duplicate replay into a no-op hit instead of a
    // second real execution.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    runner().processBatch();

    verify(commandBus).execute(any(TestCmd.class), eq(IdempotencyKey.of("dlq-replay:cmd-1")));
    verify(commandBus, never()).execute(any(Command.class));
    assertTrue(dlq.all().isEmpty());
  }

  /**
   * The fallback key is DERIVED from the stored command id, so it is built through {@link
   * IdempotencyKey}'s decode door, not the ingress factory that refuses control characters — an
   * entry whose command id carries one must still replay, under the same key every replay derives.
   */
  @Test
  void replayFallbackKey_fromAStoredCommandIdWithAControlCharacter_isDerivedVerbatim() {
    publishTestEntry("cmd-1\u0001x");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    runner().processBatch();

    verify(commandBus)
        .execute(any(TestCmd.class), eq(new IdempotencyKey("dlq-replay:cmd-1\u0001x")));
    assertTrue(dlq.all().isEmpty());
  }

  @Test
  void replayUsesUnkeyedWhenBusLacksIdempotencySupport() {
    // The capability check, not exception-catching, decides keyed vs unkeyed: when the bus reports
    // supportsIdempotentExecution() == false (a Mockito mock defaults to false), the runner must
    // never attempt the keyed call at all — it goes straight to the unkeyed execute(command). This
    // matters because the real VirtualThreadCommandBus throws IllegalStateException (not
    // UnsupportedOperationException) from the keyed overload when it has no CommandInbox — a
    // catch-by-exception-type fallback misses that and the replay is lost (see
    // replayExecutesOnInboxLessBus for the real-bus regression proof).
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(false);
    when(commandBus.execute(any(Command.class))).thenReturn(null);

    assertDoesNotThrow(() -> runner().processBatch());

    verify(commandBus, never()).execute(any(Command.class), any(IdempotencyKey.class));
    verify(commandBus).execute(any(TestCmd.class));
    assertTrue(dlq.all().isEmpty(), "the unkeyed execution succeeded, so the entry is discarded");
  }

  @Test
  void replayExecutesOnInboxLessBus() {
    // Regression test for the exact production bug this fix addresses: a REAL
    // VirtualThreadCommandBus with NO CommandInbox configured (a fully supported deployment shape)
    // must still have its DLQ entries replayed. Before this fix, the runner tried the keyed
    // execute(command, key) first and caught UnsupportedOperationException to fall back — but the
    // real bus throws IllegalStateException when it has no inbox (see
    // VirtualThreadCommandBus.execute(C, IdempotencyKey)), so the catch missed it, the exception
    // propagated to processEntry's outer catch, and the command was silently NEVER replayed
    // (recordFailedAttempt bumped dlqAttempts instead of executing anything). With the capability
    // check (supportsIdempotentExecution() == false for an inbox-less bus), the runner now goes
    // straight to the unkeyed path and this must actually execute the command and commit its event.
    var store = new InMemoryEventStore();
    var realBus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(TYPE, TestCmd.class, cmd -> AggregateId.of("a1"), new TestCmdDecider())
            .build();
    assertFalse(
        realBus.supportsIdempotentExecution(),
        "precondition: this bus has no CommandInbox configured");

    var realDlq = new InMemoryDeadLetterQueue();
    try {
      realDlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              objectMapper.writeValueAsString(new TestCmd("hello")),
              TestCmd.class.getName(),
              CommandId.of("cmd-real-1"),
              StreamId.of(TYPE, AggregateId.of("s1")),
              "Err",
              "msg",
              1,
              Instant.now(),
              null,
              null,
              null,
              null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    var realRunner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(realDlq)
            .commandBus(realBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(TestCmd.class)
            .build();

    realRunner.processBatch();

    assertEquals(
        1,
        store.eventsFor("test:a1").size(),
        "the command must have actually executed and appended its event on an inbox-less bus");
    assertTrue(
        realDlq.all().isEmpty(),
        "a successfully replayed entry must be discarded, not bumped/lost");
  }

  // ========== retry(CommandId): operator-triggered single-entry retry ==========

  @Test
  void retryFoundEntryWhoseCommandSucceedsReturnsTrueAndDiscardsEntry() {
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var replayMarkerBound = new java.util.concurrent.atomic.AtomicBoolean(false);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              // retry() must re-execute under DLQ_REPLAY just like the scheduled poll, so a
              // success path here does not double-publish and a failure path does not retry-storm.
              replayMarkerBound.set(
                  VirtualThreadCommandBus.DLQ_REPLAY.isBound()
                      && Boolean.TRUE.equals(VirtualThreadCommandBus.DLQ_REPLAY.get()));
              return null;
            });

    boolean result = runner().retry(CommandId.of("cmd-1"));

    assertTrue(result, "retry must return true when a matching entry was found and re-dispatched");
    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertTrue(
        replayMarkerBound.get(), "retry must execute the replay under the DLQ_REPLAY marker");
    assertTrue(dlq.all().isEmpty(), "a successful retry discards the entry");
  }

  @Test
  void retryIdNotFoundReturnsFalseAndExecutesNothing() {
    // The queue is non-empty but has no entry with this id — retry must not execute anything.
    publishTestEntry("cmd-present");

    boolean result = runner().retry(CommandId.of("cmd-absent"));

    assertFalse(result, "retry must return false when no entry matches the id");
    verifyNoInteractions(commandBus);
    assertEquals(1, dlq.all().size(), "the unrelated entry is untouched");
    assertEquals(0, dlq.all().getFirst().dlqAttempts());
  }

  @Test
  void retryFoundEntryWhoseCommandThrowsReturnsTrueAndBumpsAttempts() {
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    boolean result = runner().retry(CommandId.of("cmd-1"));

    // retry returns true once a matching entry was found and re-dispatched, regardless of whether
    // the command itself then succeeded — the failure outcome is reflected in the entry's state.
    assertTrue(result, "retry returns true even when the re-dispatched command then fails");
    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertEquals(1, dlq.all().size(), "a failed retry leaves the entry queued");
    assertEquals(1, dlq.all().getFirst().dlqAttempts(), "a failed retry bumps the attempt count");
    assertNotNull(dlq.all().getFirst().lastAttemptAt());
  }

  // ========== retry(CommandId): the replay never runs under the caller's identity ==========

  /**
   * The context an operator's admin request has bound when it calls {@code retry()}: its own user,
   * correlation, trace, baggage and a captured authority. None of it belongs to the dead-lettered
   * command.
   */
  private static RequestContext operatorContext() {
    return new RequestContext(
        TraceId.of("trace-operator"),
        UserId.of("admin-1"),
        CorrelationId.of("corr-operator"),
        Instant.now(),
        Map.of("role", "ADMIN"),
        new org.streamrune.core.UserAuthority(java.util.Set.of("ADMIN"), java.util.Set.of()));
  }

  @Test
  void retry_ofAnEntryWithoutPersistedContext_replaysUnboundEvenUnderAnOperatorsContext() {
    // An entry for a command that ran with no context bound (a boot-time or background dispatch)
    // replays unbound on the poll thread. An operator's retry() must replay it the same way: the
    // admin request's context must not become the replayed command's identity, or its events and
    // audit row would name the operator as the actor of a command they never issued.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var contextBound = new java.util.concurrent.atomic.AtomicBoolean(true);
    var observed = new java.util.concurrent.atomic.AtomicReference<RequestContext>();
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              contextBound.set(StreamRuneContext.CURRENT.isBound());
              observed.set(StreamRuneContext.capture());
              return null;
            });
    var runner = runner();

    boolean result =
        ScopedValue.where(StreamRuneContext.CURRENT, operatorContext())
            .call(() -> runner.retry(CommandId.of("cmd-1")));

    assertTrue(result);
    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertFalse(
        contextBound.get(),
        "an entry with no persisted context replays unbound, never under the caller's context;"
            + " observed "
            + observed.get());
    assertTrue(dlq.all().isEmpty(), "the successful replay discards the entry");
  }

  @Test
  void retry_ofAnEntryWithPersistedContext_runsAsTheRecordedUserNotTheOperator() {
    publishContextEntry("cmd-1", "corr-orig", "user-orig", "trace-orig");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var observed = new java.util.concurrent.atomic.AtomicReference<RequestContext>();
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              observed.set(StreamRuneContext.capture());
              return null;
            });
    var runner = runner();

    ScopedValue.where(StreamRuneContext.CURRENT, operatorContext())
        .call(() -> runner.retry(CommandId.of("cmd-1")));

    RequestContext ctx = observed.get();
    assertNotNull(ctx, "the recorded context is rebound for the replay");
    assertEquals("user-orig", ctx.userId().value());
    assertEquals("corr-orig", ctx.correlationId().value());
    assertEquals("trace-orig", ctx.traceId().value());
    assertTrue(ctx.baggage().isEmpty(), "the operator's baggage must not reach the replay");
    assertNull(ctx.authority(), "the operator's captured authority must not reach the replay");
  }

  @Test
  void retry_neverExposesTheCallersThreadStateToTheReplay() {
    // Off-request means off the caller's thread state as a whole, not only StreamRuneContext: a
    // saga-dispatch marker bound by the caller would run the replay as the trusted saga principal,
    // and an inheritable thread-local (e.g. a security context propagated to child threads) would
    // let a resolver or interceptor read the operator's identity.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var callerSecurity = new InheritableThreadLocal<String>();
    var sagaOwnedSeen = new java.util.concurrent.atomic.AtomicBoolean(true);
    var securitySeen = new java.util.concurrent.atomic.AtomicReference<String>("unset");
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenAnswer(
            invocation -> {
              sagaOwnedSeen.set(StreamRuneContext.isSagaDispatch());
              securitySeen.set(callerSecurity.get());
              return null;
            });
    var runner = runner();
    callerSecurity.set("admin-1");
    try {
      ScopedValue.where(StreamRuneContext.CURRENT, operatorContext())
          .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
          .call(() -> runner.retry(CommandId.of("cmd-1")));
    } finally {
      callerSecurity.remove();
    }

    assertFalse(sagaOwnedSeen.get(), "a replay is never a saga dispatch");
    assertNull(
        securitySeen.get(), "the caller's inheritable thread-locals must not reach a replay");
  }

  @Test
  void retry_underAnOperatorsContext_appendsEventsThatCarryNoOperatorIdentity() {
    // End to end on a real bus: the events of an operator-triggered replay of a context-less entry
    // carry no user, no trace, no baggage and a correlation id of their own, and an interceptor
    // (the audit interceptor reads the actor the same way) sees no bound user.
    var store = new InMemoryEventStore();
    var actorSeen = new java.util.concurrent.atomic.AtomicReference<String>("unset");
    org.streamrune.core.CommandInterceptor actorProbe =
        new org.streamrune.core.CommandInterceptor() {
          @Override
          public boolean before(org.streamrune.core.CommandInterceptor.CommandContext ctx) {
            RequestContext current = StreamRuneContext.capture();
            actorSeen.set(
                current == null || current.userId() == null ? null : current.userId().value());
            return true;
          }
        };
    var realBus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .interceptors(actorProbe)
            .register(TYPE, TestCmd.class, cmd -> AggregateId.of("a1"), new TestCmdDecider())
            .build();
    var realDlq = new InMemoryDeadLetterQueue();
    try {
      realDlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              objectMapper.writeValueAsString(new TestCmd("hello")),
              TestCmd.class.getName(),
              CommandId.of("cmd-boot-1"),
              StreamId.of(TYPE, AggregateId.of("s1")),
              "Err",
              "msg",
              1,
              Instant.now(),
              null,
              null,
              null,
              null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    var realRunner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(realDlq)
            .commandBus(realBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(TestCmd.class)
            .build();

    boolean result =
        ScopedValue.where(StreamRuneContext.CURRENT, operatorContext())
            .call(() -> realRunner.retry(CommandId.of("cmd-boot-1")));

    assertTrue(result);
    assertTrue(realDlq.all().isEmpty(), "the replay succeeded and discarded the entry");
    List<EventEnvelope> events = store.eventsFor("test:a1");
    assertEquals(1, events.size());
    var metadata = events.getFirst().metadata();
    assertNull(metadata.userId(), "the replayed event must not be attributed to the operator");
    assertNull(metadata.traceId(), "the operator's trace must not be stamped on the event");
    assertTrue(metadata.baggage().isEmpty(), "the operator's baggage must not reach the event");
    assertNotEquals(
        "corr-operator",
        metadata.correlationId().value(),
        "the replay must not join the operator's request correlation");
    assertNull(actorSeen.get(), "interceptors must see no bound user for this replay");
  }

  @Test
  void retry_propagatesAFailureToRecordTheAttemptToTheCaller() {
    // The replay's bookkeeping runs off the caller's thread; a store failure there must still
    // reach the caller (an admin endpoint answering 500) instead of vanishing on another thread.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));
    var brokenUpdateDlq =
        new org.streamrune.core.DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            dlq.publish(request);
          }

          @Override
          public java.util.List<DeadLetterEntry> read(int limit) {
            return dlq.read(limit);
          }

          @Override
          public void discard(CommandId commandId) {
            dlq.discard(commandId);
          }

          @Override
          public void updateAttempts(CommandId commandId, int attempts, Instant lastAttemptAt) {
            throw new IllegalStateException("dead letter store unavailable");
          }
        };
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(brokenUpdateDlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(TestCmd.class)
            .build();

    var thrown =
        assertThrows(IllegalStateException.class, () -> runner.retry(CommandId.of("cmd-1")));

    assertEquals("dead letter store unavailable", thrown.getMessage());
    assertEquals(0, dlq.all().getFirst().dlqAttempts(), "nothing was recorded");
  }

  @Test
  void retry_calledWithTheInterruptFlagSet_completesTheReplayAndKeepsTheFlag() {
    // An interrupt of the waiting caller neither abandons the replay half-way nor gets lost: the
    // replay completes and records its outcome, and the caller returns with its interrupt status.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);
    var runner = runner();

    Thread.currentThread().interrupt();
    boolean result;
    boolean stillInterrupted;
    try {
      result = runner.retry(CommandId.of("cmd-1"));
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    assertTrue(result);
    assertTrue(stillInterrupted, "the caller's interrupt status must be preserved");
    assertTrue(dlq.all().isEmpty(), "the replay completed and discarded the entry");
  }

  @Test
  void retry_ofAnAlreadyExhaustedEntryThatFailsAgain_doesNotReportASecondLoss() {
    // The default policy retains an exhausted entry, and an operator may retry it. The terminal
    // exhaustion counter and ERROR fired when the entry crossed maxRetries; a click that fails
    // again is not another permanently lost command and must not page as one.
    var metrics = new RecordingStreamRuneMetrics();
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
    publishTestEntry("cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 3, Instant.now()); // exhausted and retained
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));
    var runner = runnerWithMetrics(metrics);
    var appender = attachRunnerLogAppender();
    try {
      runner.retry(CommandId.of("cmd-1"));
      runner.retry(CommandId.of("cmd-1"));

      assertEquals(0, metrics.count("command.dlqExhausted"), "no second exhaustion sample");
      assertEquals(5, dlq.all().getFirst().dlqAttempts(), "each failed retry is still recorded");
      assertTrue(
          appender.list.stream().noneMatch(e -> e.getLevel() == ch.qos.logback.classic.Level.ERROR),
          "no second terminal ERROR for an entry that was already exhausted");
      assertEquals(
          2,
          appender.list.stream()
              .filter(
                  e ->
                      e.getLevel() == ch.qos.logback.classic.Level.WARN
                          && e.getFormattedMessage().contains("already exhausted"))
              .count(),
          "each failed operator retry of an exhausted entry is logged as a WARN");
    } finally {
      detachRunnerLogAppender(appender);
    }
  }

  @Test
  void metadataOnlyEntry_isNeverHandedToTheBus_andCountsAsAFailedAttempt() {
    // The bus dead-letters a command whose payload could not be serialized as metadata only (a
    // JSON null payload). There is no command to replay: the runner must say so and age the entry
    // like any other unreplayable one, without dispatching a null command.
    publishRaw(TestCmd.class.getName(), "null", "cmd-1");
    var appender = attachRunnerLogAppender();
    try {
      runner().processBatch();

      verify(commandBus, never()).execute(any());
      verify(commandBus, never()).execute(any(), any());
      assertEquals(1, dlq.all().getFirst().dlqAttempts(), "a failed attempt is recorded");
      assertTrue(
          appender.list.stream()
              .anyMatch(
                  e ->
                      e.getLevel() == ch.qos.logback.classic.Level.WARN
                          && e.getFormattedMessage().contains("metadata only")),
          "the WARN names the entry as metadata-only");
    } finally {
      detachRunnerLogAppender(appender);
    }
  }

  /**
   * A plain class with two constructor arguments and no creator annotation: Jackson writes it
   * through its getters but has nothing to rebuild it with.
   */
  public static final class WriteOnlyCmd implements Command {
    private final String name;
    private final int quantity;

    public WriteOnlyCmd(String name, int quantity) {
      this.name = name;
      this.quantity = quantity;
    }

    public String getName() {
      return name;
    }

    public int getQuantity() {
      return quantity;
    }
  }

  @Test
  void aCommandTheMapperCanWriteButNotRebuild_isNeverExecuted_andEveryCycleSpendsOneAttempt()
      throws Exception {
    // The bus stores a dead-lettered command as the JSON its ObjectMapper writes, and the runner
    // rebuilds it from that JSON on every replay (Command's javadoc and the DLQ guide state this
    // contract). A type that serializes but has no Jackson creator is stored normally, then cannot
    // be read back: nothing may reach the bus, and each cycle ages the entry like any other
    // unreplayable one until it is exhausted.
    String storedByTheBus = objectMapper.writeValueAsString(new WriteOnlyCmd("widget", 3));
    assertTrue(storedByTheBus.contains("widget"), "the write side succeeds: " + storedByTheBus);
    publishRaw(WriteOnlyCmd.class.getName(), storedByTheBus, "cmd-1");
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(WriteOnlyCmd.class)
            .build();

    for (int cycle = 1; cycle <= 3; cycle++) {
      runner.processBatch();
      assertEquals(
          cycle,
          dlq.all().getFirst().dlqAttempts(),
          "cycle " + cycle + " spends exactly one attempt on the unreadable payload");
    }
    runner.processBatch();

    assertEquals(3, dlq.all().getFirst().dlqAttempts(), "an exhausted entry is not read again");
    verify(commandBus, never()).execute(any());
    verify(commandBus, never()).execute(any(), any());
  }

  @Test
  void builder_rejectsANonPositiveBatchSize() {
    for (int batchSize : new int[] {0, -1}) {
      var builder =
          DeadLetterRetryRunner.builder()
              .deadLetterQueue(dlq)
              .commandBus(commandBus)
              .objectMapper(objectMapper)
              .policy(policy)
              .pollInterval(Duration.ofMinutes(1))
              .batchSize(batchSize);
      assertThrows(
          IllegalArgumentException.class, builder::build, "batchSize " + batchSize + " rejected");
    }
  }

  @Test
  void aFailedReplaysExceptionMessageIsLoggedOnOneLine() {
    // The replayed command's exception message is free text a client can shape (a decider that
    // echoes an id it rejected). Rendered raw, a newline in it forges a second log record.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new IllegalArgumentException("unknown product X\n2026-10-03 ERROR forged line"));
    var appender = attachRunnerLogAppender();
    try {
      runner().processBatch();
      runner().processBatch();
      runner().processBatch(); // the third failure is the terminal one (maxRetries = 3)

      var lines =
          appender.list.stream()
              .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
              .filter(m -> m.contains("unknown product X"))
              .toList();
      assertEquals(3, lines.size(), "every failed attempt logs the message: " + lines);
      assertTrue(
          lines.stream().noneMatch(m -> m.contains("\n") || m.contains("\r")),
          "the message must be rendered on one line: " + lines);
    } finally {
      detachRunnerLogAppender(appender);
    }
  }

  @Test
  void startAndCloseLifecycle() throws Exception {
    var runner = runner();
    runner.start();
    assertTrue(runner.isRunning());
    runner.close();
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (runner.isRunning() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertFalse(runner.isRunning());
  }

  @Test
  void startTwiceThrows() {
    var runner = runner();
    try {
      runner.start();
      // A second start must not spawn a concurrent loop re-executing the same DLQ commands.
      assertThrows(IllegalStateException.class, runner::start);
    } finally {
      runner.close();
    }
  }

  @Test
  void pollLoopSurvivesDlqFailureAndRecovers() throws Exception {
    publishTestEntry("cmd-resilient");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    var failuresLeft = new java.util.concurrent.atomic.AtomicInteger(2);
    var flakyDlq =
        new org.streamrune.core.DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            dlq.publish(request);
          }

          @Override
          public java.util.List<DeadLetterEntry> read(int limit) {
            return dlq.read(limit);
          }

          @Override
          public java.util.List<DeadLetterEntry> readRetryable(int maxAttempts, int limit) {
            if (failuresLeft.getAndDecrement() > 0) {
              throw new RuntimeException("DLQ temporarily unreachable");
            }
            return dlq.readRetryable(maxAttempts, limit);
          }

          @Override
          public void discard(CommandId commandId) {
            dlq.discard(commandId);
          }

          @Override
          public void updateAttempts(CommandId commandId, int attempts, Instant lastAttemptAt) {
            dlq.updateAttempts(commandId, attempts, lastAttemptAt);
          }
        };

    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(flakyDlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMillis(10))
            .registerCommand(TestCmd.class)
            .build();

    runner.start();
    try {
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (!dlq.all().isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertTrue(
          dlq.all().isEmpty(),
          "runner must survive transient DLQ failures and retry the entry after recovery");
      assertTrue(runner.isRunning(), "runner stays alive through transient failures");
      assertEquals(0, runner.consecutiveFailures(), "failure counter resets after recovery");
    } finally {
      runner.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Deterministic due-window boundary tests. The runner reads "now" from an injected Clock and the
  // DLQ stamps publishedAt from its own injected clock, so the per-entry due check is pinned to an
  // exact instant instead of "way inside" / "way past" the window. A Duration.ZERO initial delay
  // makes delayForAttempt() return exactly ZERO (jitter * 0 == 0), so the backoff window collapses
  // to lastActivity itself — letting us assert the precise >= boundary and the lastAttemptAt-over-
  // publishedAt selection without the policy's jitter making the result non-deterministic.
  // ---------------------------------------------------------------------------------------------

  private static final Instant T0 = Instant.parse("2026-06-14T12:00:00Z");

  private DeadLetterRetryRunner runnerWith(DeadLetterQueue queue, Clock clock) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(queue)
        .commandBus(commandBus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(TestCmd.class)
        .clock(clock)
        .build();
  }

  private void publishInto(InMemoryDeadLetterQueue queue, String id) {
    try {
      queue.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              objectMapper.writeValueAsString(new TestCmd("hello")),
              TestCmd.class.getName(),
              CommandId.of(id),
              StreamId.of(TYPE, AggregateId.of("s1")),
              "Err",
              "msg",
              1,
              T0,
              null,
              null,
              null,
              null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void entryDueExactlyAtBackoffBoundaryIsRetried() {
    // ZERO backoff => first retry due at publishedAt itself. Stamp publishedAt at T0 (DLQ clock),
    // run the cycle with the runner's clock at EXACTLY T0. The boundary is inclusive (now is not
    // before lastActivity+backoff), so the entry is due at the exact instant — not one tick later.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
    var queue = new InMemoryDeadLetterQueue(MutableClock.startingAt(T0));
    publishInto(queue, "cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    runnerWith(queue, Clock.fixed(T0, java.time.ZoneOffset.UTC)).processBatch();

    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertTrue(queue.all().isEmpty(), "an entry exactly at its backoff boundary is due (>=)");
  }

  @Test
  void entryOneNanoBeforeBoundaryIsNotRetried() {
    // ZERO backoff => boundary is publishedAt (T0). With the runner's clock one nanosecond before
    // T0, now IS before lastActivity+backoff, so the entry is not yet due.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
    var queue = new InMemoryDeadLetterQueue(MutableClock.startingAt(T0));
    publishInto(queue, "cmd-1");

    runnerWith(queue, Clock.fixed(T0.minusNanos(1), java.time.ZoneOffset.UTC)).processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(
        0,
        queue.all().getFirst().dlqAttempts(),
        "an entry one nanosecond before its boundary keeps its attempt count");
  }

  @Test
  void lastAttemptAtWinsOverPublishedAtForBackoffWindow() {
    // The due window must anchor on lastAttemptAt, not publishedAt, once the entry has been
    // retried. publishedAt is well in the past (T0); a recent lastAttemptAt sits one minute in the
    // FUTURE of the runner's clock. With ZERO backoff: anchored on lastAttemptAt the entry is not
    // due (now < lastAttemptAt); anchored on publishedAt it would be due (now >= publishedAt). The
    // entry staying un-retried proves lastAttemptAt wins.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
    var queue = new InMemoryDeadLetterQueue(MutableClock.startingAt(T0));
    publishInto(queue, "cmd-1");
    Instant now = T0.plus(Duration.ofHours(1));
    Instant recentAttempt = now.plus(Duration.ofMinutes(1)); // after "now"
    queue.updateAttempts(CommandId.of("cmd-1"), 1, recentAttempt);

    runnerWith(queue, Clock.fixed(now, java.time.ZoneOffset.UTC)).processBatch();

    verifyNoInteractions(commandBus);
    assertEquals(
        1,
        queue.all().getFirst().dlqAttempts(),
        "the due window anchors on lastAttemptAt (future) — publishedAt (past) must not make it due");
  }

  @Test
  void dueEntryIsNotStarvedByAFullPageOfNotYetDueHeadEntries() {
    // Head-of-line starvation: under a backlog, the entries the store returns first
    // are recently-attempted with a long (exponential) backoff — NOT due — while a never-attempted
    // entry behind them IS due. The old selection read exactly `batchSize` oldest entries and only
    // THEN filtered isDue in Java, so a page full of not-due head entries returned zero and starved
    // the due entry poll after poll. Due-ness ordering (least-recently-active first) plus
    // over-fetch
    // must still retry the due entry.
    //
    // Policy: 1s initial delay, x100 backoff. A never-attempted entry is due 1s after publish; an
    // entry with 3 attempts backs off 1s*100^3 (~11.5 days), so a 60s-old last attempt is not due.
    policy = new DeadLetterRetryPolicy(10, Duration.ofSeconds(1), 100.0, false);
    var queue = new InMemoryDeadLetterQueue(MutableClock.startingAt(T0)); // publishedAt == T0
    // Three not-yet-due head entries: high attempts, last attempt 60s before T0, enormous backoff.
    // Their last-activity (T0-60s) is OLDER than the due entry's (its publishedAt T0), so under
    // due-ness ordering they sort AHEAD of it — proving over-fetch, not just ordering, is required.
    for (int i = 1; i <= 3; i++) {
      publishInto(queue, "head-" + i);
      queue.updateAttempts(CommandId.of("head-" + i), 3, T0.minus(Duration.ofSeconds(60)));
    }
    // One due entry: never retried, published at T0, due 1s later.
    publishInto(queue, "due-1");

    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    // batchSize=2 is smaller than the 3-entry not-due head, so a naive "LIMIT batchSize then
    // filter"
    // reads only head entries and never reaches due-1. Runner clock is T0+10s (past due-1's 1s
    // delay, far short of the head entries' multi-day backoff).
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(queue)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .batchSize(2)
            .registerCommand(TestCmd.class)
            .clock(Clock.fixed(T0.plus(Duration.ofSeconds(10)), java.time.ZoneOffset.UTC))
            .build();

    runner.processBatch();

    verify(commandBus).execute(any(TestCmd.class), eq(IdempotencyKey.of("dlq-replay:due-1")));
    assertTrue(
        queue.all().stream().noneMatch(e -> e.commandId().value().equals("due-1")),
        "the due entry must be retried and discarded, not starved behind the not-due head entries");
    assertEquals(3, queue.all().size(), "the not-yet-due head entries stay queued, untouched");
    assertTrue(
        queue.all().stream().allMatch(e -> e.dlqAttempts() == 3),
        "the not-due head entries are never retried (attempt count unchanged)");
  }

  // ========== restart: stop()->start() on the same instance ==========

  @Test
  void stopAndRestartProcessesNewEntries() throws Exception {
    var runner = runner();
    runner.start();
    assertTrue(runner.isRunning());
    runner.close();
    // Wait for the thread to observe the stop
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (runner.isRunning() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertFalse(runner.isRunning(), "runner must report stopped before restart");

    // Publish an entry, then restart — the runner must process it
    publishTestEntry("cmd-restart");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);
    assertDoesNotThrow(runner::start, "start() after close() must not throw");
    deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (!dlq.all().isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(dlq.all().isEmpty(), "restarted runner must process a new DLQ entry");
    runner.close();
  }

  /** Simple command record for test deserialization. */
  public record TestCmd(String value) implements Command {}

  /** Trivial event proving {@link TestCmd} actually reached the decider and committed. */
  public record TestCmdEvent(String value) implements DomainEvent {}

  /** Trivial state — the decider does not branch on it. */
  public record TestCmdState() implements AggregateState {}

  /** Decider for {@link TestCmd} that always succeeds, producing one {@link TestCmdEvent}. */
  static class TestCmdDecider implements Decider<TestCmd, TestCmdState, TestCmdEvent> {
    @Override
    public TestCmdState initialState() {
      return new TestCmdState();
    }

    @Override
    public List<TestCmdEvent> decide(TestCmd command, TestCmdState state) {
      return List.of(new TestCmdEvent(command.value()));
    }

    @Override
    public TestCmdState evolve(TestCmdState state, TestCmdEvent event) {
      return state;
    }
  }

  /**
   * Minimal in-memory {@link EventStore} for wiring a REAL {@link VirtualThreadCommandBus} in
   * {@link #replayExecutesOnInboxLessBus()} — no Testcontainers/Postgres needed for a unit-level
   * regression test. Mirrors the equivalent nested store in {@link VirtualThreadCommandBusTest}.
   */
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
      // no-op for this test
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

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      var events = streams.getOrDefault(streamId.value(), List.of());
      return events.stream()
          .filter(e -> e.version().value() > afterVersion.value())
          .limit(maxCount)
          .toList();
    }

    List<EventEnvelope> eventsFor(String streamId) {
      return streams.getOrDefault(streamId, List.of());
    }
  }

  // =============================================================================================
  // Single-active-consumer leadership gate. Without a CommandInbox, SKIP LOCKED protects only the
  // read transaction and the runner executes AFTER it commits, so two replicas' staggered polls
  // would both re-execute the same DLQ entry (duplicate domain events). The leadership gate makes
  // exactly ONE replica poll. NOOP (the default) is always-leader, so single-instance behavior is
  // byte-identical to a runner with no leadership configured.
  // =============================================================================================

  /**
   * Shared single-active-consumer coordinator: for each consumer name exactly ONE replica view can
   * hold leadership at a time, modeling one replica winning the advisory lock. Call {@link
   * #viewFor(String)} to get a distinct per-replica {@link SubscriptionLeadership}; the first view
   * to call {@code tryAcquire} for a name wins it and keeps it until it {@code resign}s (or {@code
   * close}s), after which another view may win. Deterministic — no timing — so the competing-runner
   * test can assert exactly one replica polls.
   */
  static final class FakeSingleActiveLeadership {
    private final Map<String, Object> holders = new HashMap<>();

    synchronized boolean tryBecomeLeader(Object view, String name) {
      Object current = holders.get(name);
      if (current == null) {
        holders.put(name, view);
        return true;
      }
      return current == view;
    }

    synchronized boolean isLeader(Object view, String name) {
      return holders.get(name) == view;
    }

    synchronized void resign(Object view, String name) {
      if (holders.get(name) == view) {
        holders.remove(name);
      }
    }

    /** A distinct replica's leadership view onto this shared coordinator. */
    SubscriptionLeadership viewFor(String replicaId) {
      return new SubscriptionLeadership() {
        // The shared coordinator returns a boolean "do I hold it?"; wrap it as an epoch-1 lease so
        // the runner sees a present lease for the winner and empty for a standby.
        private static final Optional<Lease> LEASE = Optional.of(new Lease(1L));

        @Override
        public Optional<Lease> tryAcquire(String consumerName) {
          return FakeSingleActiveLeadership.this.tryBecomeLeader(this, consumerName)
              ? LEASE
              : Optional.empty();
        }

        @Override
        public Optional<Lease> current(String consumerName) {
          return FakeSingleActiveLeadership.this.isLeader(this, consumerName)
              ? LEASE
              : Optional.empty();
        }

        @Override
        public void resign(String consumerName) {
          FakeSingleActiveLeadership.this.resign(this, consumerName);
        }

        @Override
        public void close() {
          FakeSingleActiveLeadership.this.resign(this, DLQ_CONSUMER_NAME);
        }

        @Override
        public String toString() {
          return "leadership-view[" + replicaId + "]";
        }
      };
    }
  }

  /**
   * The fixed dead-letter-retry consumer name the runner gates on (mirrors the production const).
   */
  private static final String DLQ_CONSUMER_NAME = "dead-letter-retry";

  private DeadLetterRetryRunner runnerWithLeadership(
      DeadLetterQueue queue, SubscriptionLeadership leadership) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(queue)
        .commandBus(commandBus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(TestCmd.class)
        .leadership(leadership)
        .build();
  }

  /**
   * A DLQ decorator that counts {@code readRetryable} calls, delegating everything to {@link #dlq}.
   */
  private DeadLetterQueue readCountingDlq(java.util.concurrent.atomic.AtomicInteger reads) {
    return new DeadLetterQueue() {
      @Override
      public void publish(DeadLetterPublishRequest request) {
        dlq.publish(request);
      }

      @Override
      public List<DeadLetterEntry> read(int limit) {
        return dlq.read(limit);
      }

      @Override
      public List<DeadLetterEntry> readRetryable(int maxRetries, int limit) {
        reads.incrementAndGet();
        return dlq.readRetryable(maxRetries, limit);
      }

      @Override
      public void discard(CommandId commandId) {
        dlq.discard(commandId);
      }

      @Override
      public void updateAttempts(CommandId commandId, int attempts, Instant lastAttemptAt) {
        dlq.updateAttempts(commandId, attempts, lastAttemptAt);
      }
    };
  }

  @Test
  void onlyTheLeaderRunnerPollsAndRetries_nonLeaderDoesNothing() {
    // Two runners share ONE dead letter queue and a single-active leadership: only one view can
    // ever
    // hold the lease (tryAcquire) for the dead-letter-retry name. Both are driven to poll; exactly
    // one
    // (the
    // leader) must process every entry, and the non-leader must do nothing at all — it must never
    // even READ the queue (no readRetryable side effects) and never execute. This is the missing
    // multi-replica double-execute guard.
    var coordinator = new FakeSingleActiveLeadership();
    var leaderView = coordinator.viewFor("leader");
    var standbyView = coordinator.viewFor("standby");

    var leaderReads = new java.util.concurrent.atomic.AtomicInteger(0);
    var standbyReads = new java.util.concurrent.atomic.AtomicInteger(0);

    publishTestEntry("cmd-1");
    publishTestEntry("cmd-2");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    var leaderRunner = runnerWithLeadership(readCountingDlq(leaderReads), leaderView);
    var standbyRunner = runnerWithLeadership(readCountingDlq(standbyReads), standbyView);

    // The leader polls first and wins leadership, draining both entries.
    leaderRunner.processBatch();
    // The standby polls after — it is denied leadership, so it returns immediately: never reads the
    // queue, never executes anything.
    standbyRunner.processBatch();
    // A second standby poll must STILL be denied (leadership is sticky to the leader view).
    standbyRunner.processBatch();

    assertTrue(dlq.all().isEmpty(), "the leader alone must drain every entry");
    verify(commandBus, times(2)).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertTrue(leaderReads.get() >= 1, "the leader must poll the queue");
    assertEquals(
        0,
        standbyReads.get(),
        "the non-leader must never read the queue — the gate returns before readRetryable");
  }

  @Test
  void aStandbyTakesOverAfterTheLeaderResigns() {
    // If the leader resigns (e.g. on close), a standby that polls next wins leadership and drains
    // the queue — proving the gate is a live single-active handoff, not a permanent lockout.
    var coordinator = new FakeSingleActiveLeadership();
    var leaderView = coordinator.viewFor("leader");
    var standbyView = coordinator.viewFor("standby");

    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    var leaderRunner = runnerWithLeadership(dlq, leaderView);
    var standbyRunner = runnerWithLeadership(dlq, standbyView);

    leaderRunner.processBatch(); // leader wins, drains cmd-1
    assertTrue(dlq.all().isEmpty());

    // Publish a second entry, resign the leader, then let the standby poll: it must now win.
    publishTestEntry("cmd-2");
    leaderView.resign(DLQ_CONSUMER_NAME);
    standbyRunner.processBatch();

    assertTrue(dlq.all().isEmpty(), "the standby must drain cmd-2 after taking over leadership");
    verify(commandBus, times(2)).execute(any(TestCmd.class), any(IdempotencyKey.class));
  }

  // ==================== Leadership + idempotent-execution pairing ====================

  @Test
  void buildRejectsRealLeadershipWithNonIdempotentBus() {
    // Real (multi-replica) leadership paired with a bus that cannot dedup a keyed replay makes the
    // leadership gate look like a guarantee it cannot keep: a stale leader racing a fresh one
    // across an already-fetched batch has nothing to stop it re-executing the same entry.
    when(commandBus.supportsIdempotentExecution()).thenReturn(false);
    var leadership = mock(SubscriptionLeadership.class);

    var builder =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .leadership(leadership);

    var ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("idempotent"), ex.getMessage());
    assertTrue(ex.getMessage().contains("NOOP"), ex.getMessage());
  }

  @Test
  void buildAllowsRealLeadershipWithIdempotentBus() {
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var leadership = mock(SubscriptionLeadership.class);

    assertDoesNotThrow(
        () ->
            DeadLetterRetryRunner.builder()
                .deadLetterQueue(dlq)
                .commandBus(commandBus)
                .objectMapper(objectMapper)
                .policy(policy)
                .pollInterval(Duration.ofMinutes(1))
                .leadership(leadership)
                .build());
  }

  @Test
  void buildAllowsNoopLeadershipWithNonIdempotentBus() {
    // The inbox-less bus remains a fully supported shape when NOT paired with real leadership —
    // the framework's own documented single-instance degradation (replays still work, just
    // without cross-instance de-duplication).
    when(commandBus.supportsIdempotentExecution()).thenReturn(false);

    assertDoesNotThrow(
        () ->
            DeadLetterRetryRunner.builder()
                .deadLetterQueue(dlq)
                .commandBus(commandBus)
                .objectMapper(objectMapper)
                .policy(policy)
                .pollInterval(Duration.ofMinutes(1))
                .build()); // no .leadership(...) call => NOOP default
  }

  @Test
  void processBatchStopsWhenLeadershipIsLostMidBatch() {
    // The gate at the top of processBatch() only checks ONCE per batch and discards the acquired
    // lease. A batch that outlives this replica's lease TTL (e.g. paused by GC) must not keep
    // processing entries after a fresh leader has taken over — it must re-check per entry and stop
    // the moment it is no longer the leader.
    publishTestEntry("cmd-1");
    publishTestEntry("cmd-2");
    publishTestEntry("cmd-3");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    var currentCalls = new java.util.concurrent.atomic.AtomicInteger(0);
    var leadership =
        new SubscriptionLeadership() {
          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return java.util.Optional.of(new Lease(1L));
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            // Leader for the first per-entry check (entry 1), fenced out by the second (entry 2) —
            // simulates the lease expiring mid-batch and a fresh leader taking over.
            return currentCalls.getAndIncrement() == 0
                ? java.util.Optional.of(new Lease(1L))
                : java.util.Optional.empty();
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    var runner = runnerWithLeadership(dlq, leadership);
    runner.processBatch();

    verify(commandBus, times(1)).execute(any(TestCmd.class), any(IdempotencyKey.class));
    assertEquals(
        2,
        dlq.all().size(),
        "the entries not yet processed when leadership was lost must remain in the queue");
  }

  @Test
  void noopLeadershipDefault_singleRunnerProcessesNormally() {
    // With no leadership configured (the default is SubscriptionLeadership.NOOP, always-leader), a
    // single runner processes exactly as before the gate existed — byte-identical behavior.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    // runner() builds WITHOUT calling .leadership(...) — proving the default path.
    runner().processBatch();

    assertTrue(dlq.all().isEmpty());
    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
  }

  @Test
  void builderDefaultsToNoopLeadership() {
    // The default leadership is NOOP (always leader), so a runner built without .leadership(...)
    // polls unconditionally — the single-instance contract.
    assertSame(SubscriptionLeadership.NOOP, runner().leadership());
  }

  @Test
  void builderThreadsSuppliedLeadership() {
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var coordinator = new FakeSingleActiveLeadership();
    var view = coordinator.viewFor("only");
    assertSame(view, runnerWithLeadership(dlq, view).leadership());
  }

  @Test
  void nullLeadershipCoalescesToNoop() {
    // Passing null to the builder must coalesce to NOOP, never leave a null that NPEs on poll.
    var built =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq)
            .commandBus(commandBus)
            .objectMapper(objectMapper)
            .policy(policy)
            .pollInterval(Duration.ofMinutes(1))
            .leadership(null)
            .build();
    assertSame(SubscriptionLeadership.NOOP, built.leadership());
  }

  @Test
  void closeResignsLeadership() {
    // Closing the runner must resign its consumer name so a standby can take over immediately,
    // instead of waiting for process exit to release the advisory lock.
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var leadership = mock(SubscriptionLeadership.class);
    var runner = runnerWithLeadership(dlq, leadership);

    runner.close();

    verify(leadership).resign(DLQ_CONSUMER_NAME);
  }

  @Test
  void close_throwingResign_stillCompletesAndStaysRestartable() {
    // Sweep: close() calls leadership.resign() mid-shutdown relying on its no-throw
    // contract. A resign that throws unchecked (a wrapped DataSource dying at exactly shutdown
    // time) previously escaped close() — the started guard was never reset (runner permanently
    // not-restartable) and the throw propagated into the shutting-down lifecycle adapter.
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    var leadership = mock(SubscriptionLeadership.class);
    doThrow(new IllegalStateException("wrapped DataSource torn down"))
        .when(leadership)
        .resign(DLQ_CONSUMER_NAME);
    var runner = runnerWithLeadership(dlq, leadership);
    runner.start();

    assertDoesNotThrow(runner::close, "close() must survive a throwing resign");
    assertFalse(runner.isStarted(), "the started guard must be reset so the runner is restartable");

    // Restartable for real: a second lifecycle cycle works.
    runner.start();
    assertTrue(runner.isRunning());
    assertDoesNotThrow(runner::close);
  }

  @Test
  void onDemandRetryIsNotGatedByLeadership() {
    // The operator-triggered retry(CommandId) is an explicit override and must run on ANY replica,
    // even one that is NOT the leader — it must never consult tryAcquire.
    var coordinator = new FakeSingleActiveLeadership();
    var leaderView = coordinator.viewFor("leader");
    var standbyView = coordinator.viewFor("standby");
    // Make "leader" hold the lock so "standby" is a genuine non-leader.
    assertTrue(leaderView.tryAcquire(DLQ_CONSUMER_NAME).isPresent());
    assertTrue(standbyView.tryAcquire(DLQ_CONSUMER_NAME).isEmpty());

    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    // The NON-leader replica performs an on-demand retry — it must still execute.
    boolean retried = runnerWithLeadership(dlq, standbyView).retry(CommandId.of("cmd-1"));

    assertTrue(retried);
    assertTrue(dlq.all().isEmpty(), "on-demand retry succeeded and discarded the entry");
    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
  }

  // ── A throwing metrics backend must abort neither the poll cycle nor the terminal
  // exhaustion transition ────────────────────────────────────────────────────────────────────────

  /**
   * Metrics double standing in for a broken observability backend — e.g. Micrometer registering a
   * meter lazily against a registry that was closed or misconfigured. {@link StreamRuneMetrics} is
   * a pluggable sink, so the runner must treat a {@link RuntimeException} from it as a logged
   * nuisance, never as a poll-cycle failure or a reason to skip a terminal transition.
   */
  private static StreamRuneMetrics throwingMetrics() {
    return new StreamRuneMetrics() {
      @Override
      public void recordDlqRetryDegradation(long consecutiveFailures) {
        throw new IllegalStateException("simulated metrics backend failure (degradation gauge)");
      }

      @Override
      public void recordDeadLetterExhausted(String commandType) {
        throw new IllegalStateException("simulated metrics backend failure (exhausted counter)");
      }
    };
  }

  private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>
      attachRunnerLogAppender() {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(DeadLetterRetryRunner.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachRunnerLogAppender(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(DeadLetterRetryRunner.class))
        .detachAppender(appender);
  }

  @Test
  void processBatch_survivesThrowingDegradationGauge_andStillRetriesEntries() {
    // sampleObservability() called recordDlqRetryDegradation BARE while its sibling
    // recordDlqBacklog a few lines below was try/caught. Because the sample runs FIRST in every
    // processBatch(), a metrics backend that throws aborted EVERY poll cycle before the leadership
    // gate: the runner stayed isRunning()=true with its consecutive-failure counter climbing, yet
    // never retried a single dead-lettered command again.
    publishTestEntry("cmd-1");
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class))).thenReturn(null);

    assertDoesNotThrow(() -> runnerWithMetrics(throwingMetrics()).processBatch());

    assertTrue(dlq.all().isEmpty(), "the entry must still be retried (and discarded on success)");
    verify(commandBus).execute(any(TestCmd.class), any(IdempotencyKey.class));
  }

  @Test
  void retryExhaustion_throwingExhaustedCounter_stillDiscardsPerPolicy() {
    // recordDeadLetterExhausted ran bare AFTER updateAttempts had already bumped the
    // entry to maxRetries. A throwing backend therefore skipped the discard — and since
    // readRetryable filters dlq_attempts < maxRetries, no later poll ever touched the entry again.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, true);
    publishTestEntry("cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, Instant.now()); // next failure hits maxRetries=3
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));

    assertDoesNotThrow(() -> runnerWithMetrics(throwingMetrics()).processBatch());

    assertTrue(dlq.all().isEmpty(), "discard policy must still remove the exhausted entry");
  }

  @Test
  void retryExhaustion_throwingExhaustedCounter_stillLogsTerminalError_defaultPolicy() {
    // Under the default retain policy the terminal ERROR log is the ONLY signal that
    // a command was permanently lost (the counter is the thing that just failed). It ran after the
    // bare metric call, so a throwing backend lost it — permanently, because the exhausted entry
    // is never re-read by readRetryable.
    policy = new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false);
    publishTestEntry("cmd-1");
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, Instant.now()); // next failure hits maxRetries=3
    when(commandBus.supportsIdempotentExecution()).thenReturn(true);
    when(commandBus.execute(any(Command.class), any(IdempotencyKey.class)))
        .thenThrow(new RuntimeException("still broken"));
    var appender = attachRunnerLogAppender();
    try {
      assertDoesNotThrow(() -> runnerWithMetrics(throwingMetrics()).processBatch());

      assertEquals(3, dlq.all().getFirst().dlqAttempts(), "the entry crossed maxRetries");
      assertTrue(
          appender.list.stream()
              .anyMatch(
                  e ->
                      e.getLevel() == ch.qos.logback.classic.Level.ERROR
                          && e.getFormattedMessage().contains("exhausted all 3 retries")),
          "the terminal ERROR log must fire even when the exhausted counter throws");
      assertTrue(
          appender.list.stream()
              .anyMatch(
                  e ->
                      e.getLevel() == ch.qos.logback.classic.Level.WARN
                          && e.getFormattedMessage().contains("recordDeadLetterExhausted")),
          "the metrics failure itself must surface as a WARN, not vanish silently");
    } finally {
      detachRunnerLogAppender(appender);
    }
  }
}
