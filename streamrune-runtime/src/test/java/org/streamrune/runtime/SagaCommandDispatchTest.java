package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class SagaCommandDispatchTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  private static final Logger LOG = LoggerFactory.getLogger(SagaCommandDispatchTest.class);

  private record Cmd(String id) implements Command {}

  private static SagaCommand sc(String id) {
    return new SagaCommand(new Cmd(id), AggregateId.of(id));
  }

  @Test
  void executeCorrelated_bindsCorrelationIdDuringExecution() {
    var capturedCorrelationId = new AtomicReference<CorrelationId>();
    CommandBus bus =
        new CommandBus() {
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            StreamRuneContext.RequestContext ctx = StreamRuneContext.capture();
            capturedCorrelationId.set(ctx != null ? ctx.correlationId() : null);
            return null;
          }
        };
    SagaCommandDispatch.executeCorrelated(
        bus, CorrelationId.of("saga-corr-x"), new Cmd("c"), IdempotencyKey.of("saga:x:1:0"));
    assertThat(capturedCorrelationId.get()).isEqualTo(CorrelationId.of("saga-corr-x"));
  }

  @Test
  void allCompensationsSucceed_returnsCompensated() {
    var executed = new AtomicInteger();
    CommandBus bus =
        new CommandBus() {
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            executed.incrementAndGet();
            return null;
          }
        };
    var result =
        SagaCommandDispatch.compensateAndClassify(
            List.of(sc("a"), sc("b")), bus, CorrelationId.of("saga-1"), episodeKey("saga-1"), LOG);
    assertThat(result).isEqualTo(SagaCommandDispatch.CompensationOutcome.COMPENSATED);
    assertThat(executed.get()).isEqualTo(2);
  }

  @Test
  void aTransientCompensationFailure_returnsRetry_andStillRunsTheRest() {
    // An infra/transient failure (not a business rejection) must be RETRIABLE,
    // NOT terminalize the saga FAILED — otherwise a momentary blip abandons the undo. Remaining
    // compensations still run so a genuine double-delivery dedups in the inbox on resume.
    var executed = new AtomicInteger();
    CommandBus bus =
        new CommandBus() {
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            executed.incrementAndGet();
            if (((Cmd) command).id().equals("a")) throw new RuntimeException("boom");
            return null;
          }
        };
    var result =
        SagaCommandDispatch.compensateAndClassify(
            List.of(sc("a"), sc("b")), bus, CorrelationId.of("saga-1"), episodeKey("saga-1"), LOG);
    assertThat(result).isEqualTo(SagaCommandDispatch.CompensationOutcome.RETRY);
    assertThat(executed.get()).isEqualTo(2); // does not break on the first failure
  }

  @Test
  void aDeterministicCompensationFailure_returnsFailed() {
    // A business rejection (IllegalArgumentException / DomainException, per DEFAULT_DLQ_ELIGIBLE)
    // can
    // never succeed on retry, so it terminalizes FAILED — reserving FAILED for permanent failures.
    var executed = new AtomicInteger();
    CommandBus bus =
        new CommandBus() {
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            executed.incrementAndGet();
            if (((Cmd) command).id().equals("a")) throw new IllegalArgumentException("rejected");
            return null;
          }
        };
    var result =
        SagaCommandDispatch.compensateAndClassify(
            List.of(sc("a"), sc("b")), bus, CorrelationId.of("saga-1"), episodeKey("saga-1"), LOG);
    assertThat(result).isEqualTo(SagaCommandDispatch.CompensationOutcome.FAILED);
    assertThat(executed.get()).isEqualTo(2);
  }

  @Test
  void aPermanentFailureDominatesATransientOne_returnsFailed() {
    // Mixed: one transient and one permanent failure. The permanent one dominates (FAILED) —
    // retrying a deterministic failure forever is worse than terminalizing.
    CommandBus bus =
        new CommandBus() {
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            if (((Cmd) command).id().equals("a")) throw new RuntimeException("transient");
            if (((Cmd) command).id().equals("b")) throw new IllegalArgumentException("permanent");
            return null;
          }
        };
    var result =
        SagaCommandDispatch.compensateAndClassify(
            List.of(sc("a"), sc("b")), bus, CorrelationId.of("saga-1"), episodeKey("saga-1"), LOG);
    assertThat(result).isEqualTo(SagaCommandDispatch.CompensationOutcome.FAILED);
  }

  @Test
  void noCompensations_returnsFailed() {
    CommandBus bus =
        new CommandBus() {
          public <C extends Command> CommandResult execute(C command) {
            return null;
          }
        };
    var result =
        SagaCommandDispatch.compensateAndClassify(
            List.of(), bus, CorrelationId.of("saga-1"), episodeKey("saga-1"), LOG);
    assertThat(result).isEqualTo(SagaCommandDispatch.CompensationOutcome.FAILED);
  }

  // ── An interceptor VETO must not be indistinguishable from success ────────────────

  /**
   * The short-circuit result a veto interceptor's {@code before() == false} makes the bus return.
   */
  private static CommandBus.CommandResult vetoedResult(String interceptorName) {
    return new CommandBus.CommandResult(
        List.of(),
        StreamId.of(TYPE, AggregateId.of("s1")),
        Version.initial(),
        List.of(),
        List.of(),
        interceptorName,
        CommandBus.ShortCircuitReason.VETOED);
  }

  /** A successful idempotent replay of a prior keyed execution — a SUCCESS, never a rejection. */
  private static CommandBus.CommandResult idempotentReplayResult() {
    return new CommandBus.CommandResult(
        List.of(),
        StreamId.of(TYPE, AggregateId.of("s1")),
        Version.initial(),
        List.of(),
        List.of(),
        "saga:x:1:0",
        CommandBus.ShortCircuitReason.IDEMPOTENT_REPLAY);
  }

  private static CommandBus busReturning(CommandBus.CommandResult result) {
    return new CommandBus() {
      public <C extends Command> CommandResult execute(C command) {
        throw new UnsupportedOperationException("use keyed execute");
      }

      @Override
      public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
        return result;
      }
    };
  }

  @Test
  void executeCorrelated_vetoedResult_throwsVetoException() {
    // The bus RETURNS (never throws) a VETOED CommandResult when a user interceptor's before()
    // returns false. Discarding it made the veto indistinguishable from success;
    // executeCorrelated must surface it instead.
    CommandBus bus = busReturning(vetoedResult("MaintenanceModeInterceptor"));

    assertThatThrownBy(
            () ->
                SagaCommandDispatch.executeCorrelated(
                    bus, CorrelationId.of("saga-1"), new Cmd("c"), IdempotencyKey.of("saga:x:1:0")))
        .isInstanceOf(SagaCommandVetoedException.class)
        // Classification contract: a veto is deliberately NOT a DomainException. It
        // refuses to ADMIT the command, so the domain never gave an answer to record permanently —
        // the same "carries no evidence" reading the circuit breaker gives it.
        // Keeping it in that bucket made one maintenance window terminally FAIL every in-flight
        // saga, and would re-answer the question wrongly in the next consumer of
        // CommandFailureClassification.
        .isNotInstanceOf(DomainException.class)
        .hasMessageContaining("MaintenanceModeInterceptor")
        .hasMessageContaining("Cmd")
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(SagaCommandVetoedException.class))
        .extracting(SagaCommandVetoedException::interceptorName)
        .isEqualTo("MaintenanceModeInterceptor");
  }

  @Test
  void executeCorrelated_idempotentReplayResult_isSuccess_noThrow() {
    // The other short-circuit reason is a SUCCESS (events were persisted by the prior keyed
    // execution) and must NOT be treated as a rejection.
    CommandBus bus = busReturning(idempotentReplayResult());
    assertThatCode(
            () ->
                SagaCommandDispatch.executeCorrelated(
                    bus, CorrelationId.of("saga-1"), new Cmd("c"), IdempotencyKey.of("saga:x:1:0")))
        .doesNotThrowAnyException();
  }

  @Test
  void vetoedCompensation_staysCompensating_neitherCompensatedNorTerminalFailed() {
    // A vetoed compensation command must NOT classify as COMPENSATED (the undo never ran — silent
    // money loss) and must NOT terminalize FAILED either: a veto refuses
    // to ADMIT the command, so it decides nothing about the business. RETRY leaves the episode
    // COMPENSATING for the resume paths, bounded by SagaCompensationRetrySweeper's give-up.
    var executed = new AtomicInteger();
    CommandBus bus =
        new CommandBus() {
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            executed.incrementAndGet();
            if (((Cmd) command).id().equals("a")) {
              return vetoedResult("MaintenanceModeInterceptor");
            }
            return null;
          }
        };
    var result =
        SagaCommandDispatch.compensateAndClassify(
            List.of(sc("a"), sc("b")), bus, CorrelationId.of("saga-1"), episodeKey("saga-1"), LOG);
    // A veto REFUSES ADMISSION — RETRY-class (the episode stays COMPENSATING), surfaced
    // as RETRY_REFUSED so the sweeper's give-up bookkeeping does not count it as an attempt.
    assertThat(result).isEqualTo(SagaCommandDispatch.CompensationOutcome.RETRY_REFUSED);
    assertThat(result.isTerminal()).isFalse();
    assertThat(executed.get()).as("the remaining compensation still dispatches").isEqualTo(2);
  }

  /**
   * {@link IdempotencyKey#of} now refuses control characters — it is the INGRESS door. Saga
   * dispatch keys are DERIVED from a stored saga correlation, and a {@code SagaId} is bounded in
   * length, not in charset, so they are built through the decode door instead. Through {@code of},
   * a saga whose id carries a control character could never dispatch a forward or compensation
   * command again; and the derivation must stay byte-identical, or a rerun after a crash would
   * present a different key than the one the earlier run recorded and execute the command twice.
   */
  @Test
  void dispatchKeys_fromAStoredCorrelationWithAControlCharacter_areDerivedVerbatim() {
    CorrelationId stored = CorrelationId.of("saga-1\n\u0085x");
    assertThat(SagaCommandDispatch.forwardKey(stored, 42L, 0).value())
        .isEqualTo("saga:saga-1\n\u0085x:42:0");
    assertThat(SagaCommandDispatch.episodeCompensationKey(stored, 7L, 1).value())
        .isEqualTo("saga:saga-1\n\u0085x:episode:7:comp:1");
  }

  /**
   * Episode-scoped key function (version 1), mirroring the production compensation-dispatch key.
   */
  private static java.util.function.IntFunction<IdempotencyKey> episodeKey(String correlation) {
    return i -> SagaCommandDispatch.episodeCompensationKey(CorrelationId.of(correlation), 1L, i);
  }
}
