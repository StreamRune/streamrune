package org.streamrune.core.saga;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

/**
 * Persistence abstraction for saga state. Implementations: {@code PostgresSagaStore} (in
 * streamrune-postgres) and {@code InMemorySagaStore} (in streamrune-test).
 *
 * <p>Write operations use an explicit create / CAS-update split so the framework detects lost
 * updates at the persistence layer rather than relying on application-level checks.
 *
 * <p>Every operation the saga runtime needs is abstract. Besides the four-argument {@code create}
 * convenience, the only {@code default} methods are {@link #countByStatus} and {@link
 * #findCompensating}, which back optional features (the saga backlog gauges and the
 * compensation-retry sweeper) and throw {@link UnsupportedOperationException} unless a store
 * supports them.
 *
 * <p><b>Type-scoped, with no untyped mode.</b> The store is keyed by {@code saga_id} alone, and
 * every operation that takes a {@link SagaType} is scoped to it. A {@code null} saga type is a
 * caller bug: every such operation (the two optional defaults included) rejects it with {@link
 * IllegalArgumentException} before any I/O — never reading it as "any type" (an untyped delete or
 * CAS would reach a colliding foreign type's row) and never letting it surface as a storage
 * failure, which the saga runtime retries as infrastructure forever. A {@code null} {@link SagaId}
 * is the same caller bug: every operation that takes one rejects it with {@link
 * IllegalArgumentException} before any I/O.
 *
 * <p><b>Every other argument is required too.</b> The id and the type are not the only ones a
 * caller can get wrong: the saga state, the lifecycle status, the {@link AppliedEvent}, the state
 * class of a load, and a query's cutoff, age bound and status filter are all required, and a {@code
 * null} of any of them is the same caller bug, rejected with {@link IllegalArgumentException}
 * (naming the argument) before any I/O — never a {@link NullPointerException} (the in-memory store
 * used to raise one for the applied event), never a storage failure the saga runtime retries as
 * infrastructure forever (the Postgres store raised one for a status, a cutoff and an age bound),
 * never a state stored as a JSON {@code null}, and never a query silently answering "nothing" for a
 * {@code null} filter. {@code SagaStoreContract} pins all of it for every implementation. Primitive
 * arguments ({@code expectedVersion}, {@code limit}, the shield flag) cannot be {@code null}.
 */
public interface SagaStore {

  /**
   * Atomically inserts a new saga at {@code version = 1} as a COMPLETE row: {@code genesis_applied
   * = TRUE} (nothing of the start is left to dispatch — the executor's DONE-at-genesis write,
   * tooling and tests; a store that leaves the column at its default would hold every such saga's
   * correlated events forever as genesis-pending), {@code pre_fault_status = NULL}, no applied
   * offsets, and the {@code dead_letter_pending} shield as given. If a saga with the same {@code
   * sagaId} already exists the insert is a no-op and an {@link OptimisticLockException} is thrown —
   * the caller's view is stale.
   *
   * <p>The shield is carried from birth because a saga may already OWN dead-letter entries before
   * its row exists — the row-less residual (a start whose {@code initialState} threw) and the
   * correlated events held BEFORE_START behind it. A row born unshielded while those entries exist
   * lets the next live event apply ahead of them and lets retention prune them. The executor passes
   * what it knows: {@code true} on the replay channel (the entry being fed exists), the runner's
   * own-record lookup on a live start.
   *
   * <p>A row created with {@code status = COMPENSATING} is born stamped — {@code episode_version =
   * 1}, {@code episode_claimed_at = now}, in the same insert: the episode stamp rule, see {@link
   * #claimCompensating}. Any other status leaves both {@code NULL}.
   *
   * @param sagaId the saga identifier
   * @param sagaType the saga type (the framework derives it from the saga state's fully-qualified
   *     class name via {@link SagaType#fromClass(Class)})
   * @param state the current saga state to persist
   * @param status the framework-owned authoritative lifecycle status to persist
   * @param deadLetterPending the shield the row is born with ({@code true} when the saga already
   *     owns a dead-letter entry)
   * @throws OptimisticLockException if a saga with {@code sagaId} already exists
   * @throws IllegalArgumentException if {@code sagaId}, {@code sagaType}, {@code state} or {@code
   *     status} is {@code null}
   */
  void create(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending);

  /**
   * {@link #create(SagaId, SagaType, SagaState, SagaStatus, boolean)} with the shield off: a
   * complete, unshielded row — the tooling and test form. The executor never calls this one.
   */
  default void create(SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status) {
    create(sagaId, sagaType, state, status, false);
  }

  /**
   * Persists an updated saga using compare-and-swap on the version token. The update is accepted
   * only when the stored {@code version} equals {@code expectedVersion} AND the stored status is
   * not terminal ({@code COMPLETED}, {@code COMPENSATED}, {@code FAILED}). Any other condition
   * (version mismatch, terminal row, or missing row) means the caller's view is stale and an {@link
   * OptimisticLockException} is thrown.
   *
   * <p>Note: {@code saga_type} is immutable after {@link #create} and is deliberately not updated.
   *
   * <p>The episode stamp rule applies here too (see {@link #claimCompensating}): landing the row at
   * {@code COMPENSATING} stamps the episode ({@code episode_version = expectedVersion + 1}, {@code
   * episode_claimed_at = now}) unless the row already carries one; any other status leaves the
   * stamp exactly as it is — a terminal write never clears it.
   *
   * @param sagaId the saga identifier
   * @param sagaType the saga type (must match the type set at {@link #create} time)
   * @param state the updated saga state to persist
   * @param status the framework-owned authoritative lifecycle status to persist
   * @param expectedVersion the version token obtained from {@link LoadedSaga#version()}
   * @throws OptimisticLockException on version mismatch, terminal row, or missing row — the
   *     caller's view is stale
   * @throws IllegalArgumentException if {@code sagaId}, {@code sagaType}, {@code state} or {@code
   *     status} is {@code null}
   */
  void update(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status, long expectedVersion);

  /**
   * The executor's create-first write for a fresh saga: version 1, {@code genesis_applied = FALSE},
   * {@code pre_fault_status = NULL}, {@code dead_letter_pending} as given, no applied offsets,
   * holding the genesis-EVOLVED state and status (so {@code findTimedOut} can see the saga during a
   * start-path outage). {@link #create(SagaId, SagaType, SagaState, SagaStatus, boolean)} is the
   * same insert with {@code genesis_applied = TRUE} — a complete row.
   *
   * <p>{@code deadLetterPending} exists for the same reason as on {@code create}: the saga may
   * already own dead-letter entries (the row-less residual and its held correlated events), and a
   * row born unshielded while they exist breaks the shield invariant the moment the genesis lands.
   * The executor passes {@code true} on the replay channel and the runner's own-record lookup on a
   * live start; the flag rides in the one INSERT statement, so there is no crash point between "row
   * exists" and "row shielded".
   *
   * <p>When the genesis-evolved {@code status} is {@code COMPENSATING} the row is born stamped
   * ({@code episode_version = 1}, {@code episode_claimed_at = now}) in the same insert — the
   * episode stamp rule, see {@link #claimCompensating} — so there is no crash point between "row is
   * COMPENSATING" and "episode stamped" either.
   *
   * <p>This insert must happen BEFORE any command is dispatched, and it must not be implemented by
   * delegating to {@code create}: that records an applied genesis for a start whose dispatch loop
   * never ran, so a redelivery dedup-skips it and the saga strands.
   *
   * @param deadLetterPending the shield the row is born with
   * @throws org.streamrune.core.OptimisticLockException when a row already exists
   * @throws SagaTypeCollisionException when the existing row belongs to another saga type
   * @throws IllegalArgumentException if {@code sagaId}, {@code sagaType}, {@code state} or {@code
   *     status} is {@code null}
   */
  void createGenesisPending(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending);

  /**
   * The evolve-poison create: version 1, status FAULTED, {@code genesis_applied = FALSE}, {@code
   * pre_fault_status = NULL}, {@code dead_letter_pending = TRUE}, holding {@code initialState}.
   * Written AFTER the dead-letter entry, so no FAULTED row exists without its entry.
   *
   * <p>Must not be implemented by delegating to {@code create}: {@code genesis_applied = TRUE}
   * would make a replayed START entry {@code SKIPPED_DEDUP} instead of re-driven, stranding the
   * saga at {@code initialState} with its own dead-letter entry discarded, and {@code
   * dead_letter_pending = FALSE} would leave the saga's own live traffic unheld while that entry
   * sits unresolved.
   *
   * @throws org.streamrune.core.OptimisticLockException when a row already exists
   * @throws IllegalArgumentException if {@code sagaId}, {@code sagaType} or {@code initialState} is
   *     {@code null}
   */
  void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState);

  /**
   * What {@link SagaStore#applyEvent} records about the event it applies: the event's global offset
   * and the path it was delivered on. Build it with one of the four named factories; each names one
   * combination of the two flags. The canonical constructor stays public only because a record
   * requires it, and a call through it can still swap the two booleans, so use the factories:
   *
   * <ul>
   *   <li>{@link #live}: a correlated event, live ({@code startPath = false}, {@code replayRedrive
   *       = false})
   *   <li>{@link #liveStart}: the start event, live ({@code startPath = true}, {@code replayRedrive
   *       = false})
   *   <li>{@link #replayed}: a correlated event fed by the replay drain ({@code startPath = false},
   *       {@code replayRedrive = true})
   *   <li>{@link #replayedStart}: the start event fed by the replay drain ({@code startPath =
   *       true}, {@code replayRedrive = true})
   * </ul>
   *
   * <p>Nested here, like {@code CommandBus.CommandResult}, because it is an SPI argument that never
   * crosses a Jackson boundary, so it needs no native-image reflection registration.
   *
   * @param eventOffset the applied event's global offset; {@code last_applied_offset} becomes
   *     {@code GREATEST(existing, eventOffset)}
   * @param startPath {@code true} when the event is the saga's start event: the write sets {@code
   *     genesis_applied} (never clears it)
   * @param replayRedrive {@code true} when a dead-letter replay feed delivers the event: the write
   *     sets {@code last_replayed_offset = eventOffset} (left unchanged otherwise)
   */
  record AppliedEvent(GlobalOffset eventOffset, boolean startPath, boolean replayRedrive) {

    /**
     * Validates the offset; prefer the named factories over this constructor.
     *
     * @throws IllegalArgumentException if {@code eventOffset} is {@code null}
     */
    public AppliedEvent {
      if (eventOffset == null) {
        throw new IllegalArgumentException("eventOffset is required");
      }
    }

    /** A correlated event delivered live by the subscription. */
    public static AppliedEvent live(GlobalOffset eventOffset) {
      return new AppliedEvent(eventOffset, false, false);
    }

    /** The saga's start event delivered live by the subscription. */
    public static AppliedEvent liveStart(GlobalOffset eventOffset) {
      return new AppliedEvent(eventOffset, true, false);
    }

    /** A correlated event fed by the dead-letter replay drain. */
    public static AppliedEvent replayed(GlobalOffset eventOffset) {
      return new AppliedEvent(eventOffset, false, true);
    }

    /** The saga's start event fed by the dead-letter replay drain. */
    public static AppliedEvent replayedStart(GlobalOffset eventOffset) {
      return new AppliedEvent(eventOffset, true, true);
    }
  }

  /**
   * The event CAS: like {@link #update} (status, state, version+1, {@code pre_fault_status = NULL})
   * plus, from the {@link AppliedEvent}, {@code last_applied_offset =
   * GREATEST(COALESCE(last_applied_offset, -1), eventOffset)}, {@code genesis_applied =
   * genesis_applied OR startPath}, and {@code last_replayed_offset = eventOffset} when {@code
   * replayRedrive} (unchanged otherwise).
   *
   * <p>When {@code status} is {@code COMPENSATING} — the forward path persisting an evolved {@code
   * SagaState.status()} — the same statement stamps the episode ({@code episode_version =
   * expectedVersion + 1}, {@code episode_claimed_at = now}) unless the row already carries a stamp:
   * the episode stamp rule, see {@link #claimCompensating}. The evolved episode is thereby as
   * durable as a claimed one, and there is no second write for a crash to fall between.
   *
   * <p>Must not be implemented by delegating to {@code update}: that leaves all three columns
   * untouched, so a redelivered event re-applies its side effects (the live-path redelivery dedup),
   * a committed genesis keeps quarantining the saga's own live traffic as genesis-pending, and a
   * crash between a replay's CAS and its entry discard re-executes the same event (the replay
   * drain's feed→discard crash dedup).
   *
   * @param applied the applied event's offset and delivery path: {@link AppliedEvent#live}, {@link
   *     AppliedEvent#liveStart}, {@link AppliedEvent#replayed} or {@link
   *     AppliedEvent#replayedStart}
   * @throws org.streamrune.core.OptimisticLockException on a stale {@code expectedVersion} or a
   *     terminal row
   * @throws IllegalArgumentException if {@code sagaId}, {@code sagaType}, {@code state}, {@code
   *     status} or {@code applied} is {@code null}
   */
  void applyEvent(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      long expectedVersion,
      AppliedEvent applied);

  /**
   * The single fault write, a CAS with NO state write: status becomes FAULTED, version+1, and
   * {@code pre_fault_status} records what a replayed step resumes from — the status being replaced,
   * with ONE refinement both shipped stores and the contract enforce: on a genesis-pending row
   * ({@code genesis_applied = FALSE}) whose status is not {@code COMPENSATING} it records {@code
   * NULL}, because a genesis-pending FORWARD fault has nothing to resume (a replay re-evolves from
   * {@code initialState}); a claimed genesis-pending row records {@code COMPENSATING} (the episode
   * resumes, whatever the genesis flag says). Kept unchanged when the row is already FAULTED (a
   * still-poison refresh). Readers treat {@code NULL}, {@code STARTED} and {@code RUNNING} alike
   * (forward), so a store that wrote the raw status instead would not misroute — but it would fail
   * {@code SagaStoreContract.markFaulted_onAGenesisPendingRow_…}.
   *
   * @return {@code true} when the status changed, {@code false} when the row was already FAULTED (a
   *     still-poison refresh) — callers count faulted transitions, not attempts
   * @throws org.streamrune.core.OptimisticLockException on a stale {@code expectedVersion} or a
   *     terminal row
   * @throws IllegalArgumentException if {@code sagaId} or {@code sagaType} is {@code null}
   */
  boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion);

  /**
   * The durable {@code dead_letter_pending} shield flag: no version bump, no {@code updated_at}
   * change, a no-op when the row is absent (the row-less residual). The runtime writes {@code true}
   * here — the runner's hold before a named saga's entry is recorded, and the replayer when a
   * null-saga entry resolves to this saga. {@code false} is written only through the dead-letter
   * store's conditional clear, {@link SagaDeadLetterStore#clearShieldIfDrained}. A silent no-op
   * would leave the flag unset after a dead-letter publish, so a live event could apply ahead of an
   * older pending entry, breaking the drain's oldest-first ordering.
   *
   * @throws IllegalArgumentException if {@code sagaId} or {@code sagaType} is {@code null}
   */
  void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending);

  /**
   * Claim-first compensation, existing row: CAS-updates a saga to {@code status = COMPENSATING}
   * (same version/terminal-guard semantics as {@link #update}) <em>and</em> stamps the durable
   * compensation-episode identity ({@code episode_version = expectedVersion + 1}, i.e. the version
   * the row lands at after this claim) plus the episode claim instant ({@code episode_claimed_at =
   * now}). Used by the claim-first and timeout compensation claim paths. A row that already carries
   * a stamp keeps it (the rule below).
   *
   * <p>The stamped {@code episode_version} is the value {@link LoadedSaga#episodeVersion()} later
   * returns for this row, and is deliberately NOT touched by any other write — {@link #markFaulted}
   * and the CAS writes ({@link #applyEvent}, {@link #update}), terminal ones included — so a
   * replayed resume re-derives the original episode key and an already-executed compensation dedups
   * in the command inbox. The stamped {@code episode_claimed_at} ({@link
   * LoadedSaga#episodeClaimedAt()}) is equally untouched by those writes — it is the earliest
   * instant any of the episode's inbox dedup keys can have been written, anchoring the
   * stale-compensation replay guard (see {@link LoadedSaga}).
   *
   * <p><b>The episode stamp rule — every {@code COMPENSATING} row carries this stamp.</b> The stamp
   * is not this method's alone. A write that lands a row at {@code COMPENSATING} stamps {@code
   * episode_version} with the version the row lands at and {@code episode_claimed_at} with the
   * write's instant, in the SAME statement, unless the row already carries a stamp. This method is
   * that rule made explicit; {@link #create}, {@link #createGenesisPending}, {@link #applyEvent}
   * and {@link #update} apply it when the status they are given is {@code COMPENSATING} — the
   * executor's forward path persists an evolved {@code SagaState.status()} of {@code COMPENSATING}
   * through the latter two, and that episode must be as durable as a claimed one. No write ever
   * re-stamps a stamped row: an episode is absorbing ({@code COMPENSATING} only leaves to a
   * terminal status, or to {@code FAULTED} and back), so a stamped row is already in its one
   * episode and the stamp is that episode's identity. Writing a stamped row back to a forward
   * status ({@code RUNNING}) breaks that premise and is outside this contract: no store refuses it
   * and the stamp is kept, so if the row then entered {@code COMPENSATING} again, the new episode
   * would reuse the old one's compensation keys and its compensations would dedup-skip against the
   * old episode's inbox rows. The framework never issues such a write; a tool must not either. A
   * store MUST NOT implement this method as the plain {@link #update} CAS of old, and MUST NOT
   * leave a {@code COMPENSATING} row unstamped on any write: the runtime derives the compensation
   * idempotency keys from {@code episode_version} and the key-age anchors from {@code
   * episode_claimed_at} with no fallback, and refuses an unstamped episode with {@link
   * SagaUnstampedCompensationEpisodeException} (nothing dispatched). {@code SagaStoreContract} pins
   * the rule for every write of every implementation; the framework's {@code PostgresSagaStore}
   * (which also refuses an unstamped episode at the schema, a CHECK constraint on {@code
   * saga_state}) and {@code InMemorySagaStore} follow it.
   *
   * @param sagaId the saga identifier
   * @param sagaType the saga type (must match the type set at {@link #create} time)
   * @param state the saga state to persist
   * @param expectedVersion the version token obtained from {@link LoadedSaga#version()}
   * @throws OptimisticLockException on version mismatch, terminal row, or missing row
   * @throws IllegalArgumentException if {@code sagaId}, {@code sagaType} or {@code state} is {@code
   *     null}
   */
  void claimCompensating(SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion);

  /**
   * Type-scoped load: loads a saga only if it is of {@code sagaType}, returning the deserialized
   * state together with its persisted lifecycle status and the version CAS token, or {@link
   * Optional#empty()} when no row exists <em>or</em> the row is owned by a different saga type.
   *
   * <p>The saga store is keyed by {@code saga_id} alone, so two distinct saga types deriving the
   * same {@link SagaId} would otherwise collide — the second type reading (and possibly
   * CAS-overwriting) the first type's state, or failing to deserialize it and wedging the
   * subscription. Filtering the load by {@code saga_type} makes a cross-type id look like "no saga
   * for this type", so the caller ({@code SagaRunner}) takes its create path and the store's {@code
   * create} surfaces the collision loudly (see {@link SagaTypeCollisionException}) rather than
   * silently reading the wrong type's state.
   *
   * <p>Must not drop the {@code saga_type} filter: the colliding foreign type's row would be
   * returned, and the caller's next CAS-update would destroy the other saga type's state.
   *
   * @param sagaId the saga identifier
   * @param sagaType the saga type the caller expects to own {@code sagaId} (required: there is no
   *     untyped load)
   * @param stateType the concrete state class for deserialization
   * @return the loaded saga (state + persisted status + version) if it exists AND is of {@code
   *     sagaType}; otherwise {@link Optional#empty()}
   * @throws IllegalArgumentException if {@code sagaId}, {@code sagaType} or {@code stateType} is
   *     {@code null}
   */
  <S extends SagaState> Optional<LoadedSaga<S>> load(
      SagaId sagaId, SagaType sagaType, Class<S> stateType);

  /**
   * Type-scoped delete: removes a saga only if it is of {@code sagaType}. A no-op when no row
   * exists <em>or</em> the row is owned by a different saga type.
   *
   * <p>The store is keyed by {@code saga_id} alone, so the {@code saga_type} guard — the same one
   * the type-scoped {@link #load(SagaId, SagaType, Class)} / {@link #update} paths apply — is what
   * keeps one saga type from deleting another type's colliding row.
   *
   * @param sagaId the saga identifier to remove
   * @param sagaType the saga type the caller expects to own {@code sagaId} (required: there is no
   *     untyped delete, which would remove a colliding foreign type's row)
   * @throws IllegalArgumentException if {@code sagaId} or {@code sagaType} is {@code null}
   */
  void delete(SagaId sagaId, SagaType sagaType);

  /**
   * Finds saga IDs of the given type that have exceeded their timeout as of {@code cutoff} ({@code
   * now - timeout}): one batch drawn from two populations, each served in its own deadline order.
   *
   * <p><b>Forward timeouts — absolute from the start.</b> The timeout is "the maximum duration a
   * saga may remain in a non-terminal state" ({@link SagaDecider#timeout()}) — measured from the
   * saga's <em>start instant</em>, not an inactivity window. A {@code STARTED}/{@code RUNNING} saga
   * is selected when its <b>start instant</b> ({@code created_at}) is before {@code cutoff}: a
   * multi-step saga that keeps receiving events (each bumping its last-write instant) must still
   * time out once it has been non-terminal past its deadline — it cannot keep resetting the clock
   * and blow its SLA without ever timing out. These are ordered by start instant, oldest first, so
   * the earliest deadline is served first.
   *
   * <p><b>Compensation re-picks — inactivity.</b> A {@code COMPENSATING} saga is selected when its
   * <b>last-write instant</b> ({@code updated_at}) is before {@code cutoff}, so a crash-resume or
   * transiently failing episode is re-picked only once it has been idle for a timeout interval,
   * spacing out re-drives (see {@link SagaStatus#COMPENSATING}). These are ordered by last-write
   * instant, idle longest first. Returning {@code COMPENSATING} rows is intentional, not an
   * oversight: {@code COMPENSATING} means a prior compensation episode already claimed the saga but
   * may have crashed before persisting a terminal status, and re-selecting it here is what lets
   * {@code SagaTimeoutRunner} resume that episode (reusing its idempotency keys) instead of the
   * saga wedging forever.
   *
   * <p><b>Neither population can starve the other.</b> The batch interleaves the two by rank — the
   * oldest forward timeout, the oldest re-pick, the second forward timeout, the second re-pick, and
   * so on, skipping a population once it is exhausted — and stops at {@code limit}. When both have
   * enough eligible rows each gets half the batch (an odd slot goes to the forward population); a
   * population with fewer eligible rows leaves its unused slots to the other. One ordering over
   * both populations is not enough: a re-drive that fails transiently writes nothing, so a stuck
   * episode's last-write instant never moves, and a backlog of {@code limit} such episodes would
   * hold every slot on every poll while later sagas overran their deadline unserved. With {@code
   * limit == 1} only the forward population is served while it has an eligible row. Rows with equal
   * instants within a population come back in an unspecified order.
   *
   * <p>"Eligible" means the saga's status is <b>not</b> {@linkplain SagaStatus#isHalted() halted} —
   * i.e. not terminal ({@code COMPLETED}, {@code COMPENSATED}, {@code FAILED}) and not {@code
   * FAULTED}. {@code FAULTED} sagas are deliberately excluded: they are quarantined and must not be
   * picked up by the timeout sweep.
   *
   * <p>Implementors of third-party stores must replicate this precisely — exclude exactly the
   * statuses where {@link SagaStatus#isHalted()} is {@code true}, include {@code COMPENSATING},
   * apply the start-instant (forward) vs last-write (compensation) split for both the filter and
   * the order, and interleave the two populations as above — rather than re-deriving "non-terminal"
   * from {@link SagaStatus#isTerminal()} alone (which would incorrectly include {@code FAULTED}),
   * comparing only the last-write instant (which would let a multi-step saga never time out), or
   * ordering both populations by one column (which lets one starve the other). A {@code
   * limit}-bounded batch then selects the same sagas across implementations; {@code
   * SagaStoreContract} pins all of it.
   *
   * @param sagaType the saga type to filter by
   * @param cutoff {@code now - timeout}; select sagas timed out before this instant (measured from
   *     the start instant for {@code STARTED}/{@code RUNNING}, from the last-write instant for
   *     {@code COMPENSATING})
   * @param limit maximum number of results — the timeout runner's batch size
   * @return the batch of saga IDs, the two populations interleaved by rank as described above
   * @throws IllegalArgumentException if {@code sagaType} or {@code cutoff} is {@code null}
   */
  List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit);

  /**
   * Finds saga IDs of the given type currently in the given status, for operational enumeration
   * (e.g. listing {@code FAULTED} sagas for operator triage).
   *
   * @param sagaType the saga type to filter by
   * @param status the lifecycle status to filter by
   * @param limit maximum number of results
   * @return list of saga IDs matching the criteria, ordered by {@code updatedAt} descending (newest
   *     first)
   * @throws IllegalArgumentException if {@code sagaType} or {@code status} is {@code null}
   */
  List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit);

  /**
   * Counts sagas of the given type currently in the given status. Backs the saga backlog gauges
   * (e.g. {@code streamrune.saga.compensating}), sampled each cycle by {@code
   * SagaCompensationRetrySweeper} so a stalled-but-alive driver — where the thread survives but the
   * COMPENSATING backlog only grows — is visible on the metrics endpoint, matching the {@code
   * OUTBOX_PENDING}/{@code DLQ_PENDING} pattern.
   *
   * <p>Optional capability: the default throws {@link UnsupportedOperationException}, and a store
   * that does not support counting degrades the gauge gracefully (the sweeper disables backlog
   * sampling on the first such throw) rather than failing. The framework's {@code
   * PostgresSagaStore} and {@code InMemorySagaStore} override it.
   *
   * @param sagaType the saga type to filter by
   * @param status the lifecycle status to count
   * @return the number of sagas of {@code sagaType} currently in {@code status}
   * @throws IllegalArgumentException if {@code sagaType} or {@code status} is {@code null} (checked
   *     before the default's {@link UnsupportedOperationException})
   */
  default long countByStatus(SagaType sagaType, SagaStatus status) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    if (status == null) throw new IllegalArgumentException("status must not be null");
    throw new UnsupportedOperationException(
        "countByStatus is not implemented by this SagaStore; the saga backlog gauge "
            + "(streamrune.saga.compensating) is unavailable.");
  }

  /**
   * Finds {@code COMPENSATING} sagas of the given type last persisted before {@code updatedBefore},
   * for the compensation-retry sweeper to automatically re-drive a stuck compensation episode — the
   * recovery path for a saga with no timeout runner and no redelivered correlated event to re-drive
   * it.
   *
   * <p>Distinct from {@link #findByStatus}: this applies an age cutoff and returns each row's
   * {@code updatedAt} plus its durable {@code episodeClaimedAt}, which the sweeper needs to (a)
   * space out re-drives and avoid racing an event-path compensation that <em>just</em> claimed the
   * episode (a freshly-{@code COMPENSATING} saga is excluded until its {@code updatedAt} ages past
   * the cutoff), and (b) bound retries durably: {@code now - episodeClaimedAt} measures the total
   * time the EPISODE has been stuck {@code COMPENSATING}, letting the sweeper terminalize ({@code
   * FAULTED}) a compensation that never completes instead of looping forever. The give-up bound
   * deliberately does NOT use {@code updatedAt}: while a transient-failure re-drive leaves the row
   * untouched, a fault→replay cycle refreshes {@code updatedAt} ({@code markFaulted} and the
   * replayed step's CAS write that clears the fault both stamp it), which would reset the give-up
   * clock and let the episode dwell past the command-inbox retention window — after which a
   * re-drive misses the pruned dedup key of an already-succeeded compensation and re-executes it
   * (double refund). Only {@code COMPENSATING} rows are returned — never {@code STARTED}/{@code
   * RUNNING} (not compensating) nor halted rows.
   *
   * <p>Optional capability, needed only by the compensation-retry sweeper: the default throws
   * {@link UnsupportedOperationException}, so a store that does not support it fails loudly the
   * first time the sweeper polls it, rather than silently never recovering a stuck compensation.
   * The framework's {@code PostgresSagaStore} and {@code InMemorySagaStore} override it.
   *
   * @param sagaType the saga type to filter by
   * @param updatedBefore return only sagas last updated before this instant
   * @param limit maximum number of results
   * @return list of (sagaId, updatedAt), ordered by oldest first
   * @throws IllegalArgumentException if {@code sagaType} or {@code updatedBefore} is {@code null}
   *     (checked before the default's {@link UnsupportedOperationException})
   */
  default List<CompensatingSaga> findCompensating(
      SagaType sagaType, Instant updatedBefore, int limit) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    if (updatedBefore == null) throw new IllegalArgumentException("updatedBefore must not be null");
    throw new UnsupportedOperationException(
        "findCompensating is not implemented by this SagaStore; the compensation-retry sweeper "
            + " cannot automatically re-drive stuck COMPENSATING sagas. Implement it, or "
            + "disable streamrune.saga.compensation-retry.");
  }

  /**
   * A {@code COMPENSATING} saga awaiting a compensation re-drive, paired with the instant it was
   * last persisted ({@code updated_at}) and the durable episode claim instant ({@code
   * episode_claimed_at}) — see {@link #findCompensating}.
   *
   * @param sagaId the compensating saga's id
   * @param updatedAt when the saga row was last written. NOT necessarily the claim instant: while a
   *     transient-failure re-drive does not touch the row, a fault→replay cycle does ({@code
   *     markFaulted} and the replayed step's CAS write that clears the fault both refresh it) —
   *     which is why the sweeper's give-up horizon anchors on {@code episodeClaimedAt} instead;
   *     {@code updatedAt} still spaces re-drives (the due cutoff)
   * @param episodeClaimedAt the durable claim instant of the current compensation episode (stamped
   *     by the claim writes, deliberately untouched by every other write, so it never resets across
   *     a fault→replay cycle) — the anchor for the sweeper's give-up horizon. Never {@code null}
   *     from a conforming store: every write that lands a row at {@code COMPENSATING} stamps it
   *     (the episode stamp rule, {@link #claimCompensating}). The component stays nullable only so
   *     that a store which breaks the rule is refused per saga — the sweeper logs {@link
   *     SagaUnstampedCompensationEpisodeException} naming the saga and skips it, dispatching
   *     nothing — rather than failing the whole enumeration
   */
  record CompensatingSaga(SagaId sagaId, Instant updatedAt, Instant episodeClaimedAt) {
    public CompensatingSaga {
      if (sagaId == null) throw new IllegalArgumentException("sagaId is required");
      if (updatedAt == null) throw new IllegalArgumentException("updatedAt is required");
      // episodeClaimedAt is intentionally nullable — see the @param doc.
    }
  }
}
