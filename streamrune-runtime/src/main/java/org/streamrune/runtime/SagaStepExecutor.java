package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.saga.SagaUnstampedCompensationEpisodeException;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * Executes a single saga step for one {@link SagaTrigger}. Adapter-agnostic: routing and load stay
 * in the adapters ({@code SagaRunner}/{@code SagaTimeoutRunner}); this class only advances an
 * already-loaded saga.
 *
 * <p>All five triggers are implemented: {@code ForwardStep} (evolve/handle/dispatch forward, with
 * classified failure handling on a failed command and COMPENSATING-resume routing), {@code
 * ResumeCompensation} (re-drive an already-claimed episode at its current version), {@code
 * ResumeFaultedEpisode} (the operator's resume of a give-up FAULTED episode, the same re-drive
 * behind the same key-age guard as a replayed resume), {@code ClaimTimeout} (fresh CAS-claim to
 * COMPENSATING, then compensate), and {@code CompensateForwardFault} (the operator's compensation
 * of a forward-faulted saga with nothing left to drain: the same fresh claim, from the FAULTED
 * row). This executor is the sole writer of saga status transitions and the sole caller of {@code
 * SagaCommandDispatch}.
 *
 * <p><b>Forward-command failures are classified, not blindly compensated.</b> A failed forward
 * command is routed by {@link SagaCommandDispatch#isRetryLater}: PROVEN retry-later evidence (an
 * OPEN circuit breaker, an interceptor veto, a lock not acquired, a JDBC blip, a key-store outage)
 * leaves the saga untouched and propagates for redelivery ({@link #forwardRetryLater}); a business
 * rejection — and any failure that cannot be PROVEN retriable — takes the unchanged claim-first
 * compensation branch ({@link #claimFirstAndCompensate}). The asymmetric default is deliberate:
 * compensating is halted and bounded, whereas an unbounded batch retry of a deterministic failure
 * wedges every listener on the subscription.
 *
 * <p>The two admission gates in that list — {@code CircuitBreakerOpenException} and {@code
 * SagaCommandVetoedException} — are the same event in two spellings (an interceptor's {@code
 * before()} refusing, by throwing and by returning {@code false}), and they are now treated alike.
 * Neither decides anything about the business, so neither may trigger an undo.
 *
 * <p><b>Create-first genesis.</b> A fresh start event evolves from {@code initialState}; the
 * executor then writes the genesis-pending row ({@link SagaStore#createGenesisPending}: version 1,
 * {@code genesis_applied = FALSE}) BEFORE the first forward command is dispatched, dispatches, and
 * commits the genesis with {@link SagaStore#applyEvent}({@code startPath = true}), which stamps
 * {@code genesis_applied = TRUE}, {@code last_applied_offset} and version 2. Every crash point
 * converges on rerun: a redelivered start over a genesis-pending row re-evolves from {@code
 * initialState} and re-dispatches (committed indices dedup on their offset-stable forward keys);
 * over an applied genesis it is {@code SKIPPED_DEDUP}. A start whose evolve is already terminal is
 * written complete with {@link SagaStore#create} (nothing to dispatch). A correlated event on an
 * absent row is {@code SKIPPED_NO_ROW} — the executor never creates rows for correlated events; a
 * correlated redelivery at or below {@code last_applied_offset} (live) or equal to {@code
 * last_replayed_offset} (replay) is {@code SKIPPED_DEDUP}. Both genesis-path creates carry {@code
 * ForwardStep.sagaOwnsDeadLetterRecord} as the row's initial shield: a saga that already owns
 * dead-letter entries (the row-less residual, and the correlated events held behind it) must not
 * get an unshielded row when its genesis finally lands. A live event never revives a FAULTED saga;
 * a replay re-drive routes on {@link LoadedSaga#effectiveStatus()} — the recorded pre-fault status
 * — so a faulted COMPENSATING episode resumes and a faulted genesis-pending row re-evolves. The
 * single fault write is {@link SagaStore#markFaulted} (no state write; it records {@code
 * pre_fault_status}).
 *
 * <p><b>Every compensation episode is stamped .</b> A forward step whose {@code evolve()} yields a
 * state with {@code status() == COMPENSATING} enters the compensation episode through the event
 * write that follows its forward loop — {@link SagaStore#applyEvent}, on a start event and on a
 * correlated event alike — and the store stamps that write with claim semantics ({@code
 * episode_version} = the version the row lands at, {@code episode_claimed_at} = now, the applied
 * event as the cause: the episode stamp rule, {@link SagaStore#claimCompensating}). The status and
 * the stamp ride in one statement, so there is no crash point between "row is COMPENSATING" and
 * "episode stamped": a crash before the write leaves a forward row (the redelivered event
 * re-evolves, and the forward commands already dispatched dedup on their offset-stable forward
 * keys), a crash after it leaves a stamped episode that the redelivery resumes under that episode's
 * own keys. The executor never inserts a row at {@code COMPENSATING}: a start event's row is
 * genesis-pending with a forward status until its forward loop has run ({@link #genesisStep}),
 * because a {@code COMPENSATING} row is a claimed episode to every other path from the statement
 * that writes it. Every resume — a {@code ForwardStep} over a COMPENSATING row, {@code
 * ResumeCompensation}, {@code ResumeFaultedEpisode} — therefore reads the stamp with no fallback
 * and refuses a row without one ({@link #requireStampedEpisode}).
 *
 * <p><b>The executor never computes a stamp value.</b> The stamp of an episode is three values the
 * store writes in one statement: the episode version, which scopes the compensation idempotency
 * keys; the claim instant, which the key-age guards measure from; and the cause, which is the
 * causation id of the episode's compensation commands. Every dispatch of a compensation command
 * takes all three from a saga row read after the write that stamped the episode, through {@link
 * #requireStampedEpisode} and nowhere else: a resume takes them from the row its adapter loaded,
 * and a call that opens an episode with a claim of its own reads the row back after the claim
 * ({@link #readClaimedEpisode}) before it dispatches anything. The executor computes one version
 * only, {@code rowVersion + 1}: the version its own claim landed the row at. It is the
 * optimistic-lock token of the terminal or fault write and the mark by which the read recognises
 * the claimed row; it is never a stamp value. That number is the episode version only when the
 * claim was the stamping write; a claim over a row that already carries a stamp (a start event
 * whose evolve yielded {@code COMPENSATING}, whose forward command is then rejected) keeps the
 * stamp it found, which is why the stamp is read and not derived. The call that opens an episode
 * and every path that resumes it — a redelivery, a later event, a second replica's delivery, the
 * timeout runner, the compensation-retry sweeper, an operator — therefore present the same keys to
 * the command inbox, sequentially or concurrently, and each compensation command is stored once.
 *
 * <p><b>Why the read after the claim needs no transaction.</b> A stamp is immutable: no statement
 * of a {@link SagaStore} re-stamps or clears one (the episode stamp rule). Whatever happens to the
 * row between the claim and the read, the read returns the stamp the claim wrote or kept. Only the
 * row's status and version can have moved, and neither is taken from that read: the terminal write
 * stays a compare-and-swap at the claim's own version, so a concurrent writer makes it lose, as it
 * does when it writes a moment after the read. What the read can find:
 *
 * <ul>
 *   <li>the row as claimed: the commands are dispatched under the row's stamp;
 *   <li>the row terminal — another path completed the episode: the stamp is on the row, the
 *       commands are dispatched under it and the command inbox answers for each one the other path
 *       committed; the terminal write loses its compare-and-swap and is skipped;
 *   <li>the row {@code FAULTED} out of the episode — another path quarantined it: the stamp is on
 *       the row, the commands are dispatched under it, and the terminal write loses its
 *       compare-and-swap, so the row stays {@code FAULTED} for the operator, whose resume finds the
 *       committed commands in the inbox;
 *   <li>no row — the application deleted it: there is no stamp, so nothing is dispatched. A claim
 *       with no event in hand ends as {@code CLAIM_LOST}. A delivery or a replay feed reports a
 *       stale view ({@link OptimisticLockException}); what follows is what a deleted saga gets on
 *       any path: a redelivered start event finds no row and starts the saga again (its forward
 *       keys dedup), a redelivered correlated event finds no row and is dropped (or held when the
 *       saga id still owns a dead-letter record), and a replay leaves the drain with its entry
 *       kept;
 *   <li>another row — the application deleted the row and a start event inserted one for the same
 *       saga id again, all between the claim and the read: the argument above holds for one row,
 *       and this is not that row. A row's version only grows and an inserted row starts at 1, below
 *       every claim (a claim lands at 2 or higher), so the read refuses a row whose version is
 *       below the claim's and takes the "no row" exit: nothing is dispatched for the deleted row
 *       and the other row is neither read for a stamp nor written. The check cannot tell the
 *       claimed row from a new one that was itself written up to the claim's version inside that
 *       window; only a claim that returns its stamp could;
 *   <li>the read fails, or the process dies before it: nothing is dispatched and the failure
 *       propagates. The row is a claimed episode with no command dispatched, which every resume
 *       path picks up from its stamp. This includes a state the store wrote and cannot read back: a
 *       deterministic conversion failure stops the opening call before any compensation command (a
 *       delivery's event is recorded as a dead letter, a claim with no event reports the failure to
 *       its caller), and the episode is completed by a resume, or by the replay of that entry, once
 *       the state reads again.
 * </ul>
 *
 * <p>The read relies on one property of the store: a {@code load} that follows a returned {@code
 * claimCompensating} sees the claimed row ({@link SagaStore#claimCompensating}).
 *
 * <p><b>Every compensation command names the event that opened its episode.</b> The write that
 * stamps an episode records the id of the event in hand: the event whose forward command was
 * refused ({@link SagaStore#claimCompensating}), or the event whose evolve yielded {@code
 * COMPENSATING} ({@link SagaStore#applyEvent}, after that event's forward loop); a timeout claim
 * and an operator's compensation of a faulted forward step have no event and record none. Every
 * dispatch of a compensation command sets the cause the row records as the command's causation id.
 * The event that happens to drive a resume is never used. The stored compensation events therefore
 * carry the same saga origin and causation id whichever path commits their command — the opening
 * delivery, a redelivery, a later event, the timeout runner, the compensation-retry sweeper, an
 * operator.
 *
 * @param <S> the saga state type
 */
final class SagaStepExecutor<S extends SagaState> {

  private static final Logger LOG = LoggerFactory.getLogger(SagaStepExecutor.class);

  private final SagaOrchestrator<S> orchestrator;
  private final SagaStore sagaStore;
  private final CommandBus commandBus;
  private final StreamRuneMetrics metrics;

  /**
   * The command-inbox retention window, or {@code null} when unconfigured (guard disabled). Bounds
   * the ForwardStep COMPENSATING resume: an episode older than this cannot prove its
   * succeeded-compensation dedup keys still exist. See {@link
   * SagaStaleCompensationEpisodeException}.
   *
   * <p>Mutable+{@code volatile} so the auto-configurations can supply {@code
   * streamrune.inbox.retention-max-age} as a DEFAULT at startup on an application-constructed
   * {@code SagaRunner} bean, which the framework never builds itself (see {@link
   * SagaEventPathRetention}). Applying it late is harmless in the only direction that matters: an
   * unset window leaves the guard inert, which is exactly the unguarded behaviour, so a delivery
   * that raced startup can never take a WRONG decision — only the old one.
   */
  private volatile Duration inboxRetentionMaxAge;

  private final Clock clock;

  /**
   * Constructor for the paths that never route a {@code ForwardStep} — {@code SagaTimeoutRunner}
   * drives only {@code ResumeCompensation}/{@code ClaimTimeout}, both of which enforce their own
   * key-age bound in the adapter. Leaves the ForwardStep guard disabled.
   */
  SagaStepExecutor(
      SagaOrchestrator<S> orchestrator,
      SagaStore sagaStore,
      CommandBus commandBus,
      StreamRuneMetrics metrics) {
    this(orchestrator, sagaStore, commandBus, metrics, null, Clock.systemUTC());
  }

  SagaStepExecutor(
      SagaOrchestrator<S> orchestrator,
      SagaStore sagaStore,
      CommandBus commandBus,
      StreamRuneMetrics metrics,
      Duration inboxRetentionMaxAge,
      Clock clock) {
    this.orchestrator = orchestrator;
    this.sagaStore = sagaStore;
    this.commandBus = commandBus;
    this.metrics = metrics;
    this.inboxRetentionMaxAge = normalizeRetentionWindow(inboxRetentionMaxAge);
    this.clock = clock;
  }

  /**
   * Convention shared with {@code SagaCompensationRetrySweeper}, {@code SagaTimeoutRunner} and
   * {@code SagaDeadLetterReplayer}: zero or negative means the {@code InboxRetentionSweeper} is
   * DISABLED, so dedup keys are never pruned and the key-age guard must be inert — exactly as when
   * the window is simply unknown ({@code null}).
   *
   * <p>Without this the event path was the ONE guard that read {@code Duration.ZERO} literally:
   * every episode is older than zero, so wiring the documented "disable pruning" value would have
   * made it refuse (and FAULT) every single compensating resume. The three siblings all normalize;
   * this one did not, and the gap only became reachable once the auto-configs started feeding it
   * the property.
   */
  private static Duration normalizeRetentionWindow(Duration window) {
    return window == null || window.isZero() || window.isNegative() ? null : window;
  }

  /** The effective ForwardStep key-age window ({@code null} = guard disabled). */
  Duration inboxRetentionMaxAge() {
    return inboxRetentionMaxAge;
  }

  /** Supplies the ForwardStep key-age window after construction — see the field javadoc. */
  void inboxRetentionMaxAge(Duration window) {
    this.inboxRetentionMaxAge = normalizeRetentionWindow(window);
  }

  StepOutcome execute(SagaId sagaId, Optional<LoadedSaga<S>> loaded, SagaTrigger trigger) {
    return switch (trigger) {
      case SagaTrigger.ForwardStep fs -> forwardStep(sagaId, loaded, fs);
      case SagaTrigger.ResumeCompensation rc -> {
        // Sweeper / timeout-resume / no-event resume of a COMPENSATING episode. Absent or
        // non-COMPENSATING row -> nothing to resume. Otherwise re-drive the episode at its CURRENT
        // version (no re-claim). No triggering event to quarantine, so catchPoison=true: a throwing
        // compensate() is CAS-FAULTed (via faultEpisode) -> FAULTED, never thrown.
        if (loaded.isEmpty() || loaded.get().status() != SagaStatus.COMPENSATING) {
          yield StepOutcome.NOT_COMPENSATING;
        }
        yield compensateEpisode(
            sagaId,
            loaded.get().state(),
            requireStampedEpisode(sagaId, loaded.get(), /* hasTriggeringEvent= */ false),
            loaded.get().version(),
            rc.resumeCause(),
            null,
            /* catchPoison= */ true);
      }
      case SagaTrigger.ResumeFaultedEpisode rf -> {
        // The operator's resumeFaulted: a give-up FAULTED episode (pre_fault_status COMPENSATING)
        // with no triggering event. Any other row shape has no episode to resume here. The same
        // key-age guard as the ForwardStep resume (force passes it loudly), then the same resume as
        // ResumeCompensation: the CURRENT row version (no re-claim), the DURABLE episode keys, and
        // catchPoison=true — no event to quarantine, so a throwing compensate() is re-faulted in
        // place (markFaulted keeps pre_fault_status). Only the terminal CAS clears the fault.
        if (loaded.isEmpty()
            || loaded.get().status() != SagaStatus.FAULTED
            || loaded.get().effectiveStatus() != SagaStatus.COMPENSATING) {
          yield StepOutcome.NOT_COMPENSATING;
        }
        LoadedSaga<S> row = loaded.get();
        EpisodeStamp episode = requireStampedEpisode(sagaId, row, /* hasTriggeringEvent= */ false);
        requireFreshEpisodeKeys(
            sagaId, episode, rf.operatorForced(), /* hasTriggeringEvent= */ false);
        yield compensateEpisode(
            sagaId,
            row.state(),
            episode,
            row.version(),
            new SagaCompensationResumeException(),
            null,
            /* catchPoison= */ true);
      }
      case SagaTrigger.ClaimTimeout ct -> {
        // Fresh timeout claim (SagaTimeoutRunner.processTimedOutSaga's fresh-claim half). An absent
        // row means nothing to time out (defensive; the adapter already guards halted rows).
        if (loaded.isEmpty()) {
          yield StepOutcome.NOT_COMPENSATING;
        }
        yield claimFreshEpisodeAndCompensate(sagaId, loaded.get(), ct.timeoutCause());
      }
      case SagaTrigger.CompensateForwardFault _ -> {
        // The operator's compensateFaulted: a FAULTED row whose forward fault has nothing left to
        // drain. Only that shape is claimed. A live row belongs to the event path and the timeout
        // runner; a row that faulted inside a compensation episode keeps that episode's stamp and
        // is resumed under it (ResumeFaultedEpisode) — a fresh claim here would key a new episode
        // and re-execute every compensation the faulted one already committed.
        if (loaded.isEmpty()
            || loaded.get().status() != SagaStatus.FAULTED
            || loaded.get().effectiveStatus() == SagaStatus.COMPENSATING) {
          yield StepOutcome.NOT_COMPENSATING;
        }
        yield claimFreshEpisodeAndCompensate(
            sagaId, loaded.get(), new SagaForwardFaultCompensationException());
      }
    };
  }

  /**
   * The fresh-claim half shared by {@link SagaTrigger.ClaimTimeout} and {@link
   * SagaTrigger.CompensateForwardFault}: CAS-claim to {@code COMPENSATING} at the loaded version
   * BEFORE any compensation is computed or dispatched (claim-first). A lost claim (a concurrent
   * writer advanced the row) records the conflict and yields {@code CLAIM_LOST} without dispatching
   * anything. The winning claim lands the row at {@code loaded.version() + 1}, which is the
   * optimistic-lock token of the terminal or fault write that follows. The episode's stamp is read
   * back from the row ({@link #readClaimedEpisode}), never taken from that number: a row deleted
   * since the claim has no stamp, and the pass ends as {@code CLAIM_LOST} with nothing dispatched.
   * No triggering event to quarantine, so {@code catchPoison = true}: a throwing {@code
   * compensate()} is CAS-FAULTed (the row keeps its stamp and records {@code pre_fault_status =
   * COMPENSATING}), never thrown. Neither trigger has an event in hand, so the episode is claimed
   * with no cause and, where this claim is the stamping write, its compensation commands carry no
   * causation id.
   */
  private StepOutcome claimFreshEpisodeAndCompensate(
      SagaId sagaId, LoadedSaga<S> loaded, Throwable cause) {
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    long baseVersion = loaded.version();
    S state = loaded.state();
    try {
      sagaStore.claimCompensating(sagaId, type, state, baseVersion, /* episodeCause= */ null);
    } catch (SagaStateSerializationException conversionFailed) {
      // Deterministic conversion failures are poison, outages infrastructure; nothing dispatched.
      throw SagaStateConversion.classify(sagaId, conversionFailed);
    } catch (OptimisticLockException _) {
      recordCasConflict();
      LOG.debug(
          "Saga {} advanced concurrently; skip before claiming compensation",
          LogSanitizer.sanitizeForLog(sagaId.value()));
      return StepOutcome.CLAIM_LOST;
    }
    long claimVersion = baseVersion + 1;
    Optional<EpisodeStamp> episode =
        readClaimedEpisode(sagaId, type, claimVersion, /* hasTriggeringEvent= */ false);
    if (episode.isEmpty()) {
      recordCasConflict();
      LOG.debug(
          "Saga {} was removed after its compensation claim; nothing dispatched",
          LogSanitizer.sanitizeForLog(sagaId.value()));
      return StepOutcome.CLAIM_LOST;
    }
    return compensateEpisode(
        sagaId, state, episode.get(), claimVersion, cause, null, /* catchPoison= */ true);
  }

  /**
   * The stamp of the episode a claim of this executor has just committed, read from the row the
   * store holds; empty when the claimed row is gone — no row, or a row whose version is below
   * {@code claimVersion}, which is another row inserted for the saga id after the claimed one was
   * deleted (a row's version only grows, and an insert starts at 1). This read is why no claim path
   * computes a stamp value: the claim keeps a stamp the row already carries, so the version the
   * claim landed at is the episode version only when the claim was the stamping write. The stamp is
   * immutable once written, so a plain read after the claim returns it whatever else happened to
   * the row in between; the row's status and version are not taken from this read (the caller keeps
   * the version of its own claim as the optimistic-lock token).
   *
   * <p>A read that fails propagates before anything is dispatched: a deterministic state-conversion
   * failure as poison, anything else as the infrastructure failure it is. The row is then a claimed
   * episode with no command dispatched, and every resume path reads its stamp.
   */
  private Optional<EpisodeStamp> readClaimedEpisode(
      SagaId sagaId, SagaType type, long claimVersion, boolean hasTriggeringEvent) {
    return reload(sagaId, type)
        .filter(row -> row.version() >= claimVersion)
        .map(row -> requireStampedEpisode(sagaId, row, hasTriggeringEvent));
  }

  private StepOutcome forwardStep(
      SagaId sagaId, Optional<LoadedSaga<S>> loaded, SagaTrigger.ForwardStep fs) {
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    if (loaded.isPresent()) {
      LoadedSaga<S> row = loaded.get();
      if (row.status().isTerminal()) {
        return StepOutcome.SKIPPED_HALTED;
      }
      if (row.status() == SagaStatus.FAULTED && !fs.replayRedrive()) {
        return StepOutcome.SKIPPED_HALTED; // a live event never revives a faulted saga
      }
      if (row.effectiveStatus() == SagaStatus.COMPENSATING) {
        // The episode stamp is required first (requireStampedEpisode — the key scope and
        // the age anchor both come from it, with no fallback). Then the key-age
        // precondition BEFORE any dispatch (see requireFreshEpisodeKeys). Then resume the claimed
        // episode at its CURRENT row version (no re-claim, no version bump), keyed by the DURABLE
        // episode identity. A ForwardStep HAS a triggering event, so poison propagates
        // and the adapter quarantines it. A FAULTED row whose recorded pre-fault status is
        // COMPENSATING resumes here on a replay re-drive: the episode is never
        // re-opened forward, whatever the genesis flag says. The causation id of the compensation
        // commands is the cause recorded on the row, not the event that drives this resume.
        EpisodeStamp episode = requireStampedEpisode(sagaId, row, /* hasTriggeringEvent= */ true);
        requireFreshEpisodeKeys(
            sagaId, episode, fs.operatorForced(), /* hasTriggeringEvent= */ true);
        return compensateEpisode(
            sagaId,
            row.state(),
            episode,
            row.version(),
            new SagaCompensationResumeException(),
            null,
            /* catchPoison= */ false);
      }
      if (fs.startPath() && row.genesisApplied()) {
        return StepOutcome.SKIPPED_DEDUP; // start dedup: the genesis is recorded as applied
      }
      if (!fs.startPath() && isRedelivery(row, fs)) {
        return StepOutcome.SKIPPED_DEDUP; // redelivery at or below the recorded maximum
      }
    } else if (!fs.startPath()) {
      return StepOutcome.SKIPPED_NO_ROW;
    }
    return fs.startPath()
        ? genesisStep(sagaId, type, loaded, fs)
        : correlatedStep(sagaId, type, loaded.orElseThrow(), fs);
  }

  /**
   * A live correlated event at or below the recorded {@code last_applied_offset} is a redelivery; a
   * replay feed is a rerun only when it carries EXACTLY the recorded {@code last_replayed_offset}
   * (a drain that later feeds an OLDER deferred entry must still apply it).
   */
  private static boolean isRedelivery(LoadedSaga<?> row, SagaTrigger.ForwardStep fs) {
    long offset = fs.event().globalOffset().value();
    if (fs.replayRedrive()) {
      return row.lastReplayedOffset() != null && row.lastReplayedOffset() == offset;
    }
    return row.lastAppliedOffset() != null && offset <= row.lastAppliedOffset();
  }

  /**
   * The genesis path: evolve from {@code initialState}, create the intent row BEFORE any command is
   * dispatched, dispatch, then {@code applyEvent(startPath=true)}. A re-drive (genesis-pending row
   * present, from a redelivery or a replay) re-evolves from {@code initialState} — start events are
   * genesis events; the recorded state on a genesis-pending row exists for the timeout runner's
   * compensation, not as a resume point.
   *
   * <p><b>The row is inserted with a forward status, whatever {@code evolve} returned.</b> A start
   * event whose evolve yields {@code COMPENSATING} is inserted with {@code STARTED} and the evolved
   * state, and the event write after the forward loop lands {@code COMPENSATING} and stamps the
   * episode with the start event as its cause — the order a correlated event's step has. The row is
   * visible to every path from the insert on, and a {@code COMPENSATING} row is a claimed episode
   * to all of them: inserted as {@code COMPENSATING} it would be compensated by another path while
   * this delivery still dispatches its forward commands, and a redelivery after a retry-later
   * failure or a crash would resume the compensation and never finish the forward loop. {@code
   * STARTED} is what a saga is before its start step is applied; it is not a status this event's
   * evolve produced, and every reader already has a rule for a genesis-pending row in a forward
   * status, none of which loses the transition the evolved state asks for:
   *
   * <ul>
   *   <li>the redelivered start event, a second replica's delivery of it, a replayed start entry:
   *       re-evolve from {@code initialState}, dispatch the forward commands again (committed ones
   *       answer from the inbox), then the event write;
   *   <li>a correlated event: held as genesis-pending and fed after the genesis lands; it is never
   *       applied to this row;
   *   <li>the compensation-retry sweeper: does not see the row;
   *   <li>the timeout runner: claims the row at the saga's deadline, as it claims any start whose
   *       dispatch never completes, and compensates the persisted evolved state;
   *   <li>a poison in {@code handle}: the fault of a genesis-pending forward row ({@code
   *       pre_fault_status} {@code NULL}); the replayed start entry re-evolves.
   * </ul>
   *
   * <p><b>Crash points of a start event whose evolve yields {@code COMPENSATING}.</b>
   *
   * <ul>
   *   <li>the insert is not committed: nothing is stored; the redelivery starts over;
   *   <li>the insert is committed and forward command <i>k</i> is not (a retry-later failure, which
   *       propagates, or a crash): the genesis-pending {@code STARTED} row; the redelivery
   *       re-evolves, the commands before <i>k</i> answer from the inbox, <i>k</i> and the rest
   *       run, then the event write;
   *   <li>forward command <i>k</i> is rejected: the claim at the row's version is the stamping
   *       write (cause: the start event), the compensation is dispatched under the stamp read back
   *       from the row, and the row is a claimed episode whose genesis is not applied — resumed by
   *       every path, re-driven forward by none;
   *   <li>the forward loop finished and the event write is not committed: as the second point, with
   *       every command answering from the inbox;
   *   <li>the event write is committed: {@code COMPENSATING}, stamped at the version it landed at
   *       with the start event as cause, genesis applied. This call dispatches no compensation
   *       command; the episode is driven by the sweeper, the timeout runner's re-pick or the saga's
   *       next event, and a redelivery of the start event resumes it.
   * </ul>
   */
  private StepOutcome genesisStep(
      SagaId sagaId, SagaType type, Optional<LoadedSaga<S>> loaded, SagaTrigger.ForwardStep fs) {
    EventEnvelope event = fs.event();
    // Every orchestrator SPI call routes through poison(), which turns a
    // deterministic throw AND a null return into a quarantine of the one event.
    S initial = poison(sagaId, "initialState", () -> orchestrator.initialState(sagaId));
    S state = poison(sagaId, "evolve", () -> orchestrator.evolve(initial, event));
    SagaStatus evolvedStatus = poison(sagaId, "SagaState.status", state::status);
    if (evolvedStatus.isTerminal() && loaded.isEmpty()) {
      // DONE at genesis: nothing to dispatch, so the row is complete from the start —
      // a genesis-pending create followed by a CAS would refuse the terminal row. Born
      // shielded when the saga already owns entries: the drain discards them as moot.
      try {
        sagaStore.create(sagaId, type, state, evolvedStatus, fs.sagaOwnsDeadLetterRecord());
      } catch (SagaStateSerializationException conversionFailed) {
        throw SagaStateConversion.classify(sagaId, conversionFailed);
      } catch (OptimisticLockException _) {
        recordCasConflict(); // a concurrent deliverer created it: dedup
      }
      return evolvedStatus == SagaStatus.COMPLETED
          ? StepOutcome.COMPLETED
          : StepOutcome.FORWARD_PROGRESSED;
    }
    long rowVersion;
    if (loaded.isPresent()) {
      rowVersion = loaded.get().version();
    } else {
      try {
        // The row is born shielded when the saga already owns dead-letter
        // entries (the row-less residual and its held correlated events), so a live event is never
        // applied ahead of them and retention never prunes them — shielded from the first
        // statement. The inserted status is always a forward one: an evolved COMPENSATING is
        // landed by the event write after the forward loop, so the insert opens no episode and
        // names no cause.
        sagaStore.createGenesisPending(
            sagaId,
            type,
            state,
            evolvedStatus == SagaStatus.COMPENSATING ? SagaStatus.STARTED : evolvedStatus,
            fs.sagaOwnsDeadLetterRecord(),
            /* episodeCause= */ null);
        rowVersion = 1L;
      } catch (SagaStateSerializationException conversionFailed) {
        throw SagaStateConversion.classify(sagaId, conversionFailed);
      } catch (OptimisticLockException concurrentlyCreated) {
        recordCasConflict();
        Optional<LoadedSaga<S>> reloaded = reload(sagaId, type);
        if (reloaded.isEmpty()) {
          throw concurrentlyCreated; // created and deleted underneath us: let the redelivery retry
        }
        return forwardStep(sagaId, reloaded, fs); // route on the recorded facts
      }
    }
    if (evolvedStatus.isTerminal()) {
      applyEvent(sagaId, type, state, evolvedStatus, rowVersion, fs);
      return evolvedStatus == SagaStatus.COMPLETED
          ? StepOutcome.COMPLETED
          : StepOutcome.FORWARD_PROGRESSED;
    }
    List<SagaCommand> commands =
        poisonCommands(sagaId, "handle", () -> orchestrator.handle(state, event));
    StepOutcome compensated = dispatchForward(sagaId, type, state, rowVersion, fs, commands);
    if (compensated != null) {
      return compensated;
    }
    applyEvent(sagaId, type, state, evolvedStatus, rowVersion, fs);
    return StepOutcome.FORWARD_PROGRESSED;
  }

  /** A correlated event advances the already-loaded state (never {@code initialState}). */
  private StepOutcome correlatedStep(
      SagaId sagaId, SagaType type, LoadedSaga<S> row, SagaTrigger.ForwardStep fs) {
    EventEnvelope event = fs.event();
    S state = poison(sagaId, "evolve", () -> orchestrator.evolve(row.state(), event));
    SagaStatus evolvedStatus = poison(sagaId, "SagaState.status", state::status);
    if (evolvedStatus.isTerminal()) {
      applyEvent(sagaId, type, state, evolvedStatus, row.version(), fs);
      return evolvedStatus == SagaStatus.COMPLETED
          ? StepOutcome.COMPLETED
          : StepOutcome.FORWARD_PROGRESSED;
    }
    List<SagaCommand> commands =
        poisonCommands(sagaId, "handle", () -> orchestrator.handle(state, event));
    StepOutcome compensated = dispatchForward(sagaId, type, state, row.version(), fs, commands);
    if (compensated != null) {
      return compensated;
    }
    applyEvent(sagaId, type, state, evolvedStatus, row.version(), fs);
    return StepOutcome.FORWARD_PROGRESSED;
  }

  /**
   * Dispatches in order; returns the compensation outcome on a deterministic failure, else null.
   *
   * <p>A failed forward command is classified BEFORE any decision to undo the business flow. A
   * PROVEN retry-later failure never compensates ({@link #forwardRetryLater}); everything else (a
   * business rejection, and any failure that cannot be proven retriable) keeps the claim-first
   * contract, so a deterministic non-business failure still terminalizes rather than wedging the
   * subscription on an unbounded batch retry.
   */
  private StepOutcome dispatchForward(
      SagaId sagaId,
      SagaType type,
      S state,
      long rowVersion,
      SagaTrigger.ForwardStep fs,
      List<SagaCommand> commands) {
    SagaOrigin origin = origin(sagaId);
    long triggerOffset = fs.event().globalOffset().value();
    for (int i = 0; i < commands.size(); i++) {
      final int index = i;
      SagaCommand sagaCommand = commands.get(i);
      try {
        SagaCommandDispatch.dispatch(
            commandBus,
            origin,
            fs.event().metadata().eventId(),
            sagaCommand.command(),
            SagaCommandDispatch.forwardKey(sagaId, triggerOffset, index));
      } catch (Exception primary) {
        if (SagaCommandDispatch.isRetryLater(primary)) {
          throw forwardRetryLater(sagaId, index, sagaCommand, primary);
        }
        LOG.warn(
            "Saga "
                + LogSanitizer.sanitizeForLog(sagaId.value())
                + " forward command "
                + sagaCommand.command().getClass().getSimpleName()
                + " (index "
                + index
                + ") failed and the failure is not provably retriable; claiming the"
                + " compensation episode and undoing the flow. A business rejection is the"
                + " expected reason; any other failure type here is one the framework cannot"
                + " prove transient, and terminalizing is preferred to retrying a batch that"
                + " may never succeed — see SagaCommandDispatch.isRetryLater.",
            primary);
        return claimFirstAndCompensate(
            sagaId,
            type,
            state,
            rowVersion,
            fs.startPath(),
            fs.event().metadata().eventId(),
            primary,
            sagaCommand);
      }
    }
    return null;
  }

  /**
   * A forward command failed with PROVEN retry-later evidence ({@link
   * SagaCommandDispatch#isRetryLater}), so the saga is left exactly as it was and the failure is
   * propagated for redelivery instead of compensating.
   *
   * <p><b>Why propagating is the correct "retry", and why the saga is not modified.</b> The forward
   * loop runs BEFORE {@link #applyEvent}, so no {@code claimCompensating}, no status change and no
   * version bump have happened for this delivery. On a CORRELATED event nothing durable at all is
   * written; on a fresh START event the ONE durable write is the genesis-pending row ({@link
   * SagaStore#createGenesisPending} — version 1, evolved status, {@code genesis_applied = FALSE},
   * written BEFORE the first dispatch), which exists so the saga is visible to {@code
   * SagaTimeoutRunner} and so the redelivery has a recorded fact to route on; it is left untouched
   * here (no escape write, no CAS). Propagating leaves the subscription checkpoint unadvanced, so
   * the same event is redelivered; the executor re-loads the row, re-evolves to the same state (a
   * start event always evolves from genesis, and the row's recorded {@code genesis_applied = FALSE}
   * bypasses the start dedup), re-handles to the same command list, and re-dispatches under the
   * SAME deterministic forward keys {@code saga:<id>:<triggerOffset>:<index>} ({@link
   * SagaCommandDispatch#forwardKey}) — the indices that already committed dedup in the command
   * inbox and only the failed index re-runs. Returning a "skipped" outcome instead would let the
   * checkpoint advance past the event and lose it outright, which is the lost-event class.
   *
   * <p><b>Why the saga is deliberately left {@code RUNNING} rather than quarantined.</b> The
   * failure carries retry-later evidence, not a defect of this saga: it clears without the saga
   * changing (on its own, or once an operator restores the backend or key). Quarantining would
   * FAULT a healthy saga, and while it stayed FAULTED every correlated event arriving behind it
   * would pile up as held entries. If the outage instead outlives the saga's configured {@code
   * timeout()}, the saga is picked up by {@code SagaTimeoutRunner} and compensated on the
   * operator's own deadline — a bounded, configured escape, instead of a compensation taken
   * unilaterally on the very first blip. (On the start path that bound exists only BECAUSE the
   * genesis-pending row precedes the first dispatch — {@code SELECT_TIMED_OUT} selects rows, so
   * without that row the timeout runner would have nothing to select for exactly the deliveries
   * whose earlier indices had already moved money.)
   *
   * <p><b>Batch-level cost, stated honestly.</b> {@code PollingEventSubscription} checkpoints per
   * BATCH, so redelivery re-delivers the events that preceded this one in the batch. That is the
   * framework's existing at-least-once behaviour on every path that already propagates (a CAS
   * conflict from {@link #applyEvent}, any {@code SagaStore} infrastructure failure, a transient
   * state-conversion failure) — see {@code SagaRunner}'s concurrency contract. The commands of
   * those earlier events dedup on their own forward keys; their state advance dedups on the
   * recorded {@code last_applied_offset}.
   */
  private RuntimeException forwardRetryLater(
      SagaId sagaId, int index, SagaCommand sagaCommand, Exception failure) {
    LOG.warn(
        "Saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + " forward command "
            + sagaCommand.command().getClass().getSimpleName()
            + " (index "
            + index
            + ") failed transiently, or was refused admission by a circuit breaker or an"
            + " interceptor veto; the saga is left untouched (RUNNING, same version, no"
            + " compensation claimed) and the failure propagates so the subscription redelivers"
            + " the event. Already-dispatched forward commands dedup on their forward keys, so"
            + " only this index re-runs.",
        failure);
    return failure instanceof RuntimeException re ? re : new RuntimeException(failure);
  }

  /**
   * The event CAS ({@link SagaStore#applyEvent}): status, state, version+1, {@code
   * last_applied_offset = GREATEST(old, offset)}, {@code genesis_applied} when {@code startPath},
   * {@code last_replayed_offset} when {@code replayRedrive}. A deterministic conversion failure
   * becomes poison (the adapter quarantines the triggering event); a transient one propagates. A
   * CAS conflict PROPAGATES for batch retry — the redelivery reloads and dedups on the recorded
   * offsets — on the start and the correlated path alike (with create-first there is no "swallow
   * the concurrent create" case left here: the create happened in {@link #genesisStep}).
   */
  private void applyEvent(
      SagaId sagaId,
      SagaType type,
      S state,
      SagaStatus status,
      long rowVersion,
      SagaTrigger.ForwardStep fs) {
    try {
      sagaStore.applyEvent(sagaId, type, state, status, rowVersion, appliedEvent(fs));
    } catch (SagaStateSerializationException conversionFailed) {
      throw SagaStateConversion.classify(sagaId, conversionFailed);
    } catch (OptimisticLockException conflict) {
      recordCasConflict();
      throw conflict; // propagate: the redelivery reloads and dedups on the recorded offsets
    }
  }

  /**
   * The {@link AppliedEvent} naming this step's delivery path and the applied event's id. The id is
   * recorded only when the write stamps a compensation episode (an evolved {@code COMPENSATING}),
   * as that episode's cause.
   */
  private static AppliedEvent appliedEvent(SagaTrigger.ForwardStep fs) {
    GlobalOffset offset = fs.event().globalOffset();
    AppliedEvent applied;
    if (fs.replayRedrive()) {
      applied = fs.startPath() ? AppliedEvent.replayedStart(offset) : AppliedEvent.replayed(offset);
    } else {
      applied = fs.startPath() ? AppliedEvent.liveStart(offset) : AppliedEvent.live(offset);
    }
    return applied.withEventId(fs.event().metadata().eventId());
  }

  private Optional<LoadedSaga<S>> reload(SagaId sagaId, SagaType type) {
    try {
      return sagaStore.load(sagaId, type, orchestrator.stateType());
    } catch (SagaStateSerializationException conversionFailed) {
      throw SagaStateConversion.classify(sagaId, conversionFailed);
    }
  }

  /**
   * Claim-first for a forward command that failed with NO proof of retriability — a business
   * rejection (the intended case) or an unclassifiable failure (a proven retry-later failure is
   * routed to {@link #forwardRetryLater} instead and never reaches here). The row always exists
   * (the genesis-pending row precedes the first dispatch), so both paths CAS-claim with {@link
   * SagaStore#claimCompensating} at the row version. Start path: a lost claim is SWALLOWED ->
   * {@code CLAIM_LOST} (no orphaned refund — claim-first). Correlated path: a lost claim is
   * RETHROWN (batch retry). Claim COMPENSATING BEFORE invoking compensate() or dispatching any
   * compensation command.
   *
   * <p>The claim lands the row at {@code rowVersion + 1}; that number is the optimistic-lock token
   * of the terminal write and nothing more. The episode's stamp — the version that scopes the
   * compensation keys, and the cause — is read back from the row ({@link #readClaimedEpisode}). On
   * a row without a stamp the claim writes one ({@code rowVersion + 1}, {@code episodeCause}); on a
   * row that already carries one — the row of a start event whose evolve yielded {@code
   * COMPENSATING}, inserted and stamped earlier in this same call — the claim keeps it, and the
   * commands are keyed with the stamp the insert wrote. A row deleted since the claim has no stamp:
   * nothing is dispatched and the stale view propagates on both paths. The redelivered start event
   * then finds no row and starts the saga again; a redelivered correlated event finds no row and is
   * dropped.
   *
   * <p>{@code episodeCause} is the id of the event whose forward command failed, handed to the
   * claim to be recorded with a stamp the claim writes.
   */
  private StepOutcome claimFirstAndCompensate(
      SagaId sagaId,
      SagaType type,
      S state,
      long rowVersion,
      boolean startPath,
      EventId episodeCause,
      Exception primary,
      SagaCommand failedCommand) {
    try {
      // The row always exists.
      sagaStore.claimCompensating(sagaId, type, state, rowVersion, episodeCause);
    } catch (SagaStateSerializationException conversionFailed) {
      // A deterministic claim-write conversion failure quarantines the triggering event
      // rather than wedging the subscription. Claim-first still holds — nothing was dispatched.
      throw SagaStateConversion.classify(sagaId, conversionFailed);
    } catch (OptimisticLockException claimLost) {
      recordCasConflict();
      if (startPath) {
        LOG.debug(
            "Saga already claimed concurrently; skipping start-event compensation: {}",
            LogSanitizer.sanitizeForLog(sagaId.value()));
        return StepOutcome.CLAIM_LOST; // start: SWALLOW (dedup) — no orphaned refund (claim-first)
      }
      throw claimLost; // correlated: PROPAGATE (batch retry)
    }
    long claimVersion = rowVersion + 1;
    EpisodeStamp episode =
        readClaimedEpisode(sagaId, type, claimVersion, /* hasTriggeringEvent= */ true)
            .orElseThrow(
                () -> {
                  recordCasConflict();
                  return new OptimisticLockException(
                      "Saga "
                          + LogSanitizer.sanitizeForLog(sagaId.value())
                          + " was removed after its compensation claim; nothing dispatched");
                });
    return compensateEpisode(
        sagaId, state, episode, claimVersion, primary, failedCommand, /* catchPoison= */ false);
  }

  /**
   * Refuses a ForwardStep resume of a {@code COMPENSATING} episode whose command-inbox dedup keys
   * cannot be proven younger than the configured retention window, by throwing a {@link
   * SagaPoisonException} wrapping {@link SagaStaleCompensationEpisodeException}.
   *
   * <p>Raised as POISON rather than the siblings' bare CAS-FAULT because this is the one member of
   * the re-driver set that HAS a triggering event. {@code SagaRunner.asEventListener} then
   * quarantines that event and marks the saga {@code FAULTED} — the siblings' exact end state
   * (halted, zero dispatches) plus a durable record of the event, which would otherwise be dropped
   * behind an advancing subscription checkpoint and be unrecoverable (the event-path loss). The
   * fault write ({@code SagaStore.markFaulted}) records the ROW's {@code pre_fault_status =
   * COMPENSATING} (the status it replaced), which arms the replayer's own {@code
   * STALE_COMPENSATION_BLOCKED} guard on the same anchor: a plain replay is refused until the
   * operator reconciles and forces.
   *
   * <p><b>A forced replay passes .</b> {@code force} on {@code
   * SagaDeadLetterReplayer.replay}/{@code replayAll} is the operator's explicit, per-saga
   * acknowledgement of at-least-once, and it reaches this guard as {@code
   * ForwardStep.operatorForced}: a stale episode is then resumed rather than refused — every
   * compensation command re-dispatches under its ORIGINAL episode key, so one whose key was pruned
   * executes again, which is exactly what the operator acknowledged. It is loud: a WARNING naming
   * the saga and {@link StreamRuneMetrics#recordSagaForcedStaleResume}. Before, the replayer let
   * the forced entry past its own refusal and this guard refused the same episode, so the
   * documented remedy re-recorded the fault ({@code STILL_POISON}) and the saga could never be
   * resumed. A live delivery and a non-forced replay cannot set the flag, so for them the guard is
   * unchanged.
   *
   * <p><b>No triggering event .</b> The operator's {@code SagaDeadLetterReplayer.resumeFaulted}
   * resumes a give-up FAULTED episode through {@link SagaTrigger.ResumeFaultedEpisode} with {@code
   * hasTriggeringEvent = false}: the same anchor, the same window and the same forced pass, but a
   * refusal has nothing to quarantine and the row is already FAULTED, so it throws the bare {@link
   * SagaStaleCompensationEpisodeException} — no dispatch, no write — and the replayer reports
   * {@code STALE_COMPENSATION_BLOCKED}. This is the second line behind the replayer's own refusal,
   * for a replayer built without the window.
   *
   * <p>Anchor: the durable, fault-cycle-immutable {@code episodeClaimedAt} — the earliest instant
   * any of the episode's keys can have been written — and nothing else: the SAME anchor as {@code
   * SagaTimeoutRunner}, {@code SagaCompensationRetrySweeper} and the replayer, never a third clock,
   * never the dead-letter entry's resettable {@code faultedAt}, and never the row's {@code
   * updatedAt}, which a fault refreshes and so would make a stale episode look fresh. Every row in
   * a compensation episode carries the stamp, and {@link #requireStampedEpisode} has already
   * refused one that does not on every path that reaches this guard, so the old "no anchor ⇒
   * proceed unguarded" case no longer exists.
   *
   * <p>Deliberately NOT unified with the three sibling guards into one implementation: they end in
   * a CAS-FAULT with no event to record, this one ends in a quarantine-and-fault driven by the
   * adapter. The shared, load-bearing part is the anchor and the remedy, both stated here and in
   * each sibling.
   */
  private void requireFreshEpisodeKeys(
      SagaId sagaId, EpisodeStamp episode, boolean operatorForced, boolean hasTriggeringEvent) {
    if (inboxRetentionMaxAge == null) {
      return;
    }
    Instant claimInstant = episode.claimedAt();
    Duration age = Duration.between(claimInstant, clock.instant());
    if (age.compareTo(inboxRetentionMaxAge) <= 0) {
      return;
    }
    if (operatorForced) {
      LOG.warn(
          "Operator forced an at-least-once resume of a stale compensation episode for saga {}:"
              + " the episode has dwelled COMPENSATING for {}, past the command-inbox retention"
              + " window ({}), so its succeeded-compensation dedup keys may already be pruned."
              + " Resuming anyway because the operator forced it (replay(..., true) /"
              + " replayAll(sagaId, true) for a dead-letter replay, resumeFaulted(sagaId, true) for"
              + " a give-up fault): every compensation command is re-dispatched under its original"
              + " episode key, and any whose key was pruned EXECUTES AGAIN. This must match a"
              + " reconciliation an operator performed.",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          age,
          inboxRetentionMaxAge);
      recordForcedStaleResume();
      return;
    }
    SagaStaleCompensationEpisodeException stale =
        new SagaStaleCompensationEpisodeException(sagaId, claimInstant, age, inboxRetentionMaxAge);
    if (!hasTriggeringEvent) {
      LOG.warn(
          "Refusing resumeFaulted for saga {}: its faulted compensation episode was claimed {}"
              + " ago, past the command-inbox retention window ({}), so its succeeded-compensation"
              + " dedup keys may already be pruned and a re-dispatch could re-execute them (double"
              + " refund). Nothing was dispatched and the row is unchanged. Reconcile what executed"
              + " downstream, then resumeFaulted(sagaId, true).",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          age,
          inboxRetentionMaxAge);
      throw stale; // no event to quarantine: the replayer reports STALE_COMPENSATION_BLOCKED
    }
    LOG.warn(
        "Refusing event-path compensation resume for saga {}: the episode has dwelled COMPENSATING"
            + " for {}, past the command-inbox retention window ({}), so its succeeded-"
            + "compensation dedup keys may already be pruned and a re-dispatch could re-execute"
            + " them (double refund). Quarantining the triggering event and marking the saga"
            + " FAULTED WITHOUT dispatching anything.",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        age,
        inboxRetentionMaxAge);
    throw new SagaPoisonException(sagaId, stale);
  }

  /**
   * The durable compensation-episode identity of a loaded row — its {@link EpisodeStamp}: the
   * stamped {@code episodeVersion}, the value the compensation idempotency key is scoped by, with
   * the claim instant and the cause recorded beside it. It is decoupled from the row version so a
   * fault->replay cycle (which bumps the row version but not {@code episode_version}) does not
   * shift the key and re-execute an already-executed compensation.
   *
   * <p><b>No fallback .</b> Every write that lands a row at {@code COMPENSATING} stamps the episode
   * in the same statement — the claim writes and, for an evolved {@code SagaState.status()} of
   * {@code COMPENSATING}, {@code applyEvent} / {@code createGenesisPending} (the episode stamp
   * rule, {@link SagaStore#claimCompensating}) — so a row in a compensation episode without BOTH
   * {@code episodeVersion} and {@code episodeClaimedAt} violates the {@link SagaStore} contract.
   * Guessing either would be unsafe: a key derived from the row version re-executes a committed
   * compensation after the first fault->replay cycle, and an age measured from {@code updatedAt}
   * measures from the last fault. So the episode is REFUSED, nothing dispatched: as poison when a
   * triggering event is in scope (the adapter quarantines the event and marks the saga FAULTED,
   * exactly as the siblings' refusals end), bare otherwise (the sweeper, the timeout runner and the
   * replayer's {@code resumeFaulted} have no event to quarantine — they log it and leave the row
   * alone).
   */
  private static EpisodeStamp requireStampedEpisode(
      SagaId sagaId, LoadedSaga<?> row, boolean hasTriggeringEvent) {
    if (row.episodeVersion() == null || row.episodeClaimedAt() == null) {
      var unstamped = SagaUnstampedCompensationEpisodeException.ofRow(sagaId, row);
      if (hasTriggeringEvent) {
        throw new SagaPoisonException(sagaId, unstamped);
      }
      throw unstamped;
    }
    return new EpisodeStamp(
        row.episodeVersion(), row.episodeClaimedAt(), row.episodeCauseEventId());
  }

  /**
   * The stamp of a compensation episode exactly as a saga row carries it. {@link
   * #requireStampedEpisode} is the only place that creates one, and it copies the three values from
   * a {@link LoadedSaga}; {@link #compensateEpisode} and {@link #requireFreshEpisodeKeys} accept
   * nothing else, so no caller can hand them a version, an instant or a cause it worked out itself.
   *
   * @param version the episode version: the scope of the compensation idempotency keys
   * @param claimedAt the claim instant: the anchor of the key-age guards
   * @param causeEventId the id of the event that opened the episode, the causation id of its
   *     compensation commands; {@code null} when no event opened it
   */
  private record EpisodeStamp(long version, Instant claimedAt, EventId causeEventId) {}

  /**
   * Shared: compute compensations, dispatch under the episode-scoped key, transient -> leave
   * COMPENSATING ({@code COMPENSATING_LEFT}), terminal -> CAS-write (benign concurrent-terminalize
   * skipped). When {@code catchPoison}, a throwing {@code compensate()} is CAS-FAULTed here (no
   * event to quarantine); otherwise it propagates (adapter quarantines the triggering event).
   * Unifies the resume-compensation re-drive with the compensate tail of the two claim-first
   * variants — every adapter's compensation dispatch now funnels through this one method.
   *
   * <p>{@code episode} is the stamp read from the saga row: its version scopes the compensation
   * idempotency key (the DURABLE episode identity) and its cause is the causation id of every
   * command dispatched here ({@code null} when no event opened the episode). {@code rowVersion} is
   * the CAS expected-version for the terminal / FAULT write: the row version a resume loaded, or
   * the version the caller's own claim landed at. The two versions are equal when a claim stamps a
   * row and DIFFER on a claim over a row that already carries a stamp and on a resume after a
   * fault->replay cycle bumped the row version — the whole point.
   */
  private StepOutcome compensateEpisode(
      SagaId sagaId,
      S state,
      EpisodeStamp episode,
      long rowVersion,
      Throwable cause,
      SagaCommand failedCommand,
      boolean catchPoison) {
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    SagaOrigin origin = origin(sagaId);
    List<SagaCommand> compensations;
    try {
      compensations =
          poisonCommands(
              sagaId, "compensate", () -> orchestrator.compensate(state, cause, failedCommand));
    } catch (SagaPoisonException poison) {
      if (!catchPoison) {
        throw poison; // ForwardStep -> adapter quarantines
      }
      return faultEpisode(sagaId, rowVersion, poison); // resume/timeout -> FAULT
    }
    SagaCommandDispatch.CompensationOutcome outcome =
        SagaCommandDispatch.compensateAndClassify(
            compensations,
            commandBus,
            origin,
            episode.causeEventId(),
            i -> SagaCommandDispatch.episodeCompensationKey(sagaId, episode.version(), i),
            LOG);
    recordCompensation(outcome.name());
    if (!outcome.isTerminal()) {
      LOG.warn(
          "Saga {} compensation failed transiently; leaving COMPENSATING to resume",
          LogSanitizer.sanitizeForLog(sagaId.value()));
      // A wholly refused-admission re-drive attempted nothing — surface it distinctly so
      // the sweeper's give-up bookkeeping does not count it.
      return outcome == SagaCommandDispatch.CompensationOutcome.RETRY_REFUSED
          ? StepOutcome.COMPENSATING_REFUSED
          : StepOutcome.COMPENSATING_LEFT;
    }
    SagaStatus classified = outcome.terminalStatus();
    try {
      sagaStore.update(sagaId, type, state, classified, rowVersion);
    } catch (SagaStateSerializationException conversionFailed) {
      // Only reachable with a triggering event in scope for catchPoison=false; on the
      // event-less triggers the poison propagates to the sweeper / timeout runner's per-saga catch,
      // which logs and re-drives next cycle exactly as it does for any other store failure.
      throw SagaStateConversion.classify(sagaId, conversionFailed);
    } catch (OptimisticLockException _) {
      recordCasConflict();
      LOG.debug(
          "Saga {} advanced concurrently; skipping terminalization",
          LogSanitizer.sanitizeForLog(sagaId.value()));
    }
    return classified == SagaStatus.COMPENSATED ? StepOutcome.COMPENSATED : StepOutcome.FAILED;
  }

  /**
   * CAS-FAULT a COMPENSATING episode at its version (no event to quarantine) via {@link
   * SagaStore#markFaulted} — the single fault write: NO state write, {@code pre_fault_status}
   * records the status being replaced (COMPENSATING here) so a later replay re-drive resumes the
   * episode from {@link LoadedSaga#effectiveStatus()}. The metric counts the TRANSITION into
   * FAULTED only ({@code markFaulted} returns {@code false} on a still-poison refresh). Shared
   * FAULT path for the {@code catchPoison} resume/timeout triggers — parallels the sweeper's
   * give-up branch ({@code SagaRunner.faultStuckCompensation}).
   */
  private StepOutcome faultEpisode(SagaId sagaId, long version, Throwable cause) {
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    LOG.warn(
        "Compensation threw while resuming saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + "; marking FAULTED — fix the orchestrator, then resume the episode with"
            + " SagaDeadLetterReplayer.resumeFaulted(sagaId)",
        cause);
    try {
      if (sagaStore.markFaulted(sagaId, type, version)) {
        recordFaulted();
      }
    } catch (OptimisticLockException _) {
      recordCasConflict();
      LOG.debug(
          "Saga {} advanced concurrently; skipping FAULTED write",
          LogSanitizer.sanitizeForLog(sagaId.value()));
    }
    return StepOutcome.FAULTED;
  }

  /**
   * Runs one orchestrator pure-logic SPI call, converting BOTH failure modes of the SPI contract
   * into a {@link SagaPoisonException}: a thrown {@link RuntimeException} and a {@code null}
   * RETURN.
   *
   * <p><b>The executor's own SPI calls.</b> Converting only THROWS and handing {@code step.get()}
   * back unchecked is not enough. A {@code null} return — a deterministic user-logic contract
   * violation exactly like a throw, e.g. a {@code switch} arm that forgot to {@code return
   * List.of()} — would sail PAST the quarantine and NPE in this executor's own frame ({@code
   * state.status()} at the evolve site, {@code commands.size()} at the handle site, {@code
   * compensations.isEmpty()} inside {@code compensateAndClassify} at the compensate site). That NPE
   * is not a {@code SagaPoisonException}, so {@code SagaRunner.asEventListener}'s catch would never
   * see it: it would propagate out of {@code onEvents}, the subscription checkpoint would never
   * advance, and {@code ResilientPollLoop} would retry the identical batch forever — permanent
   * head-of-line blocking for every listener on that subscription, with no dead-letter entry and no
   * FAULTED saga.
   *
   * <p>Rejecting the null HERE, at the single choke point every SPI call in this class already
   * routes through, closes the whole class in one place: the violation becomes a poison quarantine
   * of the one offending event (entry written, saga FAULTED, batch continues) instead of an
   * infra-classified wedge. The message names the SPI method so the dead-letter entry tells the
   * operator which orchestrator method to fix. Deliberately a RUNTIME rejection rather than a
   * nullness-annotation pass: the orchestrator is compiled in the USER's build, where a static
   * checker in this build never runs.
   *
   * @param spiCall the SPI method being invoked, named in the recorded cause
   */
  private <T> T poison(SagaId sagaId, String spiCall, Supplier<T> step) {
    T value;
    try {
      value = step.get();
    } catch (RuntimeException e) {
      throw new SagaPoisonException(sagaId, e);
    }
    if (value == null) {
      throw new SagaPoisonException(
          sagaId,
          new NullPointerException(
              "SagaOrchestrator."
                  + spiCall
                  + " returned null — the SagaOrchestrator contract requires a non-null value"
                  + " (return an empty List, not null, when no action is needed). The offending"
                  + " event is quarantined rather than retried, so the subscription is not"
                  + " wedged."));
    }
    return value;
  }

  /**
   * {@link #poison} for the two SPI calls that return a command LIST, additionally rejecting a null
   * ELEMENT. A null element is the same contract violation one level down, and it escaped into a
   * WORSE outcome than a wedge — a silent misclassification. On the forward path the NPE from
   * {@code sagaCommand.command()} fires INSIDE the dispatch {@code try}, so {@code catch (Exception
   * primary)} read it as a failed forward command and drove a full claim-first COMPENSATION (a real
   * refund) for a command that never failed. On the compensation path {@code
   * compensateAndClassify}'s per-command catch classified it TRANSIENT, returning {@code RETRY} and
   * leaving the saga {@code COMPENSATING} forever behind a deterministic bug no re-drive can fix.
   * Validating the whole list BEFORE the first dispatch makes both a quarantine, and makes the
   * validation all-or-nothing: no command from a malformed list is dispatched.
   *
   * <p>{@code SagaCommand}'s compact constructor already rejects a null {@code command}/{@code
   * aggregateId}, so the element itself is the only reachable null.
   */
  private List<SagaCommand> poisonCommands(
      SagaId sagaId, String spiCall, Supplier<List<SagaCommand>> step) {
    List<SagaCommand> commands = poison(sagaId, spiCall, step);
    for (int i = 0; i < commands.size(); i++) {
      if (commands.get(i) == null) {
        throw new SagaPoisonException(
            sagaId,
            new NullPointerException(
                "SagaOrchestrator."
                    + spiCall
                    + " returned a null SagaCommand at index "
                    + i
                    + " — the SagaOrchestrator contract requires a list of non-null commands. No"
                    + " command from this list was dispatched; the offending event is"
                    + " quarantined."));
      }
    }
    return commands;
  }

  /**
   * The origin every command of saga {@code sagaId} is dispatched under: this executor's saga type
   * and the saga id, at hop 0. A function of those two alone, so every path that dispatches for the
   * saga (event delivery, redelivery, replay feed, timeout claim, sweeper resume) binds an equal
   * origin.
   */
  private SagaOrigin origin(SagaId sagaId) {
    return SagaOrigin.dispatchedBy(SagaType.fromClass(orchestrator.stateType()), sagaId);
  }

  /**
   * The {@code saga.type} metric tag: {@link SagaType#value()}, the saga state's fully-qualified
   * class name. Never the simple class name — two saga-state classes may share one across packages,
   * and their series would merge.
   */
  private String sagaTypeTag() {
    return SagaType.fromClass(orchestrator.stateType()).value();
  }

  private void recordCasConflict() {
    try {
      metrics.recordSagaCasConflict(sagaTypeTag());
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaCasConflict", e);
    }
  }

  private void recordCompensation(String outcome) {
    try {
      metrics.recordSagaCompensation(sagaTypeTag(), outcome);
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaCompensation", e);
    }
  }

  private void recordForcedStaleResume() {
    try {
      metrics.recordSagaForcedStaleResume(sagaTypeTag());
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaForcedStaleResume", e);
    }
  }

  private void recordFaulted() {
    try {
      metrics.recordSagaFaulted(sagaTypeTag());
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaFaulted", e);
    }
  }
}
