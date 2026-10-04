package org.streamrune.test;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaTypeCollisionException;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * In-memory {@link SagaStore} for unit tests. Thread-safe, but not suitable for production use.
 * Stores objects by identity — no JSON serialization.
 *
 * <p>{@code createdAt} and {@code updatedAt} (compared by {@link #findTimedOut}) are stamped from
 * the injected {@link Clock} — pass a {@link MutableClock} to test saga timeouts without sleeping.
 * The no-arg constructor uses {@link Clock#systemUTC()}.
 *
 * <p>Every operation rejects a {@code null} argument — the id, the type, the state, the status, the
 * applied event, the state class, a query's cutoff — with {@link IllegalArgumentException} naming
 * it, before touching the map, exactly like {@code PostgresSagaStore}: there is no untyped mode and
 * no argument-less default (see {@code SagaStoreContract}).
 *
 * <p>The episode stamp rule ({@link SagaStore#claimCompensating}) is implemented once, in the
 * shared create and CAS paths: a write that lands an entry at {@code COMPENSATING} stamps {@code
 * episodeVersion} (the version it lands at) and {@code episodeClaimedAt} (the clock's instant)
 * unless the entry already carries a stamp; {@link #claimCompensating} is the CAS with that status.
 */
public final class InMemorySagaStore implements SagaStore {

  private record SagaEntry(
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      Instant createdAt,
      Instant updatedAt,
      long version,
      Long episodeVersion,
      Instant episodeClaimedAt,
      boolean genesisApplied,
      SagaStatus preFaultStatus,
      boolean deadLetterPending,
      Long lastAppliedOffset,
      Long lastReplayedOffset) {

    LoadedSaga<SagaState> toLoaded() {
      return new LoadedSaga<>(
          state,
          status,
          version,
          updatedAt,
          episodeVersion,
          episodeClaimedAt,
          genesisApplied,
          preFaultStatus,
          deadLetterPending,
          lastAppliedOffset,
          lastReplayedOffset);
    }
  }

  private final Map<SagaId, SagaEntry> store = new ConcurrentHashMap<>();
  private final Clock clock;

  /** Creates a store reading time from {@link Clock#systemUTC()}. */
  public InMemorySagaStore() {
    this(Clock.systemUTC());
  }

  /** Creates a store reading time from the given clock (e.g. a {@link MutableClock}). */
  public InMemorySagaStore(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock is required");
  }

  @Override
  public void create(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending) {
    doCreate(sagaId, sagaType, state, status, true, deadLetterPending);
  }

  @Override
  public void createGenesisPending(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending) {
    doCreate(sagaId, sagaType, state, status, false, deadLetterPending);
  }

  @Override
  public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
    doCreate(sagaId, sagaType, initialState, SagaStatus.FAULTED, false, true);
  }

  private void doCreate(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean genesisApplied,
      boolean deadLetterPending) {
    requireArgs(sagaId, sagaType, state, status);
    Instant now = clock.instant();
    // The episode stamp rule: a row born COMPENSATING is born stamped (episode 1, claimed now), in
    // the same insert — the start event's evolve yielded COMPENSATING (createGenesisPending), or a
    // tooling create did. Any other status enters no episode.
    boolean entersEpisode = status == SagaStatus.COMPENSATING;
    Long episodeVersion = entersEpisode ? 1L : null;
    Instant episodeClaimedAt = entersEpisode ? now : null;
    SagaEntry prior =
        store.putIfAbsent(
            sagaId,
            new SagaEntry(
                sagaType,
                state,
                status,
                now,
                now,
                1L,
                episodeVersion,
                episodeClaimedAt,
                genesisApplied,
                null,
                deadLetterPending,
                null,
                null));
    if (prior != null) {
      if (!prior.sagaType().equals(sagaType)) {
        throw new SagaTypeCollisionException(sagaId, prior.sagaType(), sagaType);
      }
      throw new OptimisticLockException(
          "Saga " + LogSanitizer.sanitizeForLog(sagaId.value()) + " already exists");
    }
  }

  @Override
  public void update(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status, long expectedVersion) {
    casUpdate(sagaId, sagaType, state, status, expectedVersion, null);
  }

  @Override
  public void applyEvent(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      long expectedVersion,
      AppliedEvent applied) {
    requireArgs(sagaId, sagaType, state, status);
    if (applied == null) throw new IllegalArgumentException("applied must not be null");
    casUpdate(sagaId, sagaType, state, status, expectedVersion, applied);
  }

  @Override
  public void claimCompensating(
      SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
    casUpdate(sagaId, sagaType, state, SagaStatus.COMPENSATING, expectedVersion, null);
  }

  /**
   * The shared CAS write. {@code applied} is {@code null} for {@link #update} and {@link
   * #claimCompensating}, which leave the recorded offsets and the genesis flag untouched. The
   * episode stamp rule lives here: landing at {@code COMPENSATING} stamps the episode identity
   * unless the entry already carries one — a claim and an evolved {@code COMPENSATING} alike — and
   * no write re-stamps or clears it.
   */
  private void casUpdate(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      long expectedVersion,
      AppliedEvent applied) {
    requireArgs(sagaId, sagaType, state, status);
    store.compute(
        sagaId,
        (id, entry) -> {
          if (entry == null
              || entry.version() != expectedVersion
              || entry.status().isTerminal()
              || !entry.sagaType().equals(sagaType)) {
            throw staleView(id, expectedVersion);
          }
          long newVersion = entry.version() + 1;
          boolean entersEpisode =
              status == SagaStatus.COMPENSATING && entry.episodeVersion() == null;
          Long episodeVersion = entersEpisode ? Long.valueOf(newVersion) : entry.episodeVersion();
          Instant episodeClaimedAt = entersEpisode ? clock.instant() : entry.episodeClaimedAt();
          Long lastApplied = entry.lastAppliedOffset();
          Long lastReplayed = entry.lastReplayedOffset();
          boolean genesisApplied = entry.genesisApplied();
          if (applied != null) {
            long offset = applied.eventOffset().value();
            lastApplied = lastApplied == null ? offset : Math.max(lastApplied, offset);
            if (applied.replayRedrive()) {
              lastReplayed = offset;
            }
            genesisApplied = genesisApplied || applied.startPath();
          }
          return new SagaEntry(
              entry.sagaType(),
              state,
              status,
              entry.createdAt(),
              clock.instant(),
              newVersion,
              episodeVersion,
              episodeClaimedAt,
              genesisApplied,
              null,
              entry.deadLetterPending(),
              lastApplied,
              lastReplayed);
        });
  }

  @Override
  public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
    requireKey(sagaId, sagaType);
    boolean[] transitioned = new boolean[1];
    store.compute(
        sagaId,
        (id, entry) -> {
          if (entry == null
              || entry.version() != expectedVersion
              || entry.status().isTerminal()
              || !entry.sagaType().equals(sagaType)) {
            throw staleView(id, expectedVersion);
          }
          boolean alreadyFaulted = entry.status() == SagaStatus.FAULTED;
          transitioned[0] = !alreadyFaulted;
          // A genesis-pending FORWARD fault records NULL (nothing to resume — a replay
          // re-evolves from initialState); a claimed compensation episode records COMPENSATING
          // whatever the genesis flag says (it resumes, never re-drives forward);
          // a still-poison refresh keeps whatever was recorded.
          SagaStatus preFault;
          if (alreadyFaulted) {
            preFault = entry.preFaultStatus();
          } else if (!entry.genesisApplied() && entry.status() != SagaStatus.COMPENSATING) {
            preFault = null;
          } else {
            preFault = entry.status();
          }
          return new SagaEntry(
              entry.sagaType(),
              entry.state(),
              SagaStatus.FAULTED,
              entry.createdAt(),
              clock.instant(),
              entry.version() + 1,
              entry.episodeVersion(),
              entry.episodeClaimedAt(),
              entry.genesisApplied(),
              preFault,
              entry.deadLetterPending(),
              entry.lastAppliedOffset(),
              entry.lastReplayedOffset());
        });
    return transitioned[0];
  }

  @Override
  public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
    requireKey(sagaId, sagaType);
    store.computeIfPresent(
        sagaId,
        (id, entry) ->
            entry.sagaType().equals(sagaType)
                ? new SagaEntry(
                    entry.sagaType(),
                    entry.state(),
                    entry.status(),
                    entry.createdAt(),
                    entry.updatedAt(),
                    entry.version(),
                    entry.episodeVersion(),
                    entry.episodeClaimedAt(),
                    entry.genesisApplied(),
                    entry.preFaultStatus(),
                    pending,
                    entry.lastAppliedOffset(),
                    entry.lastReplayedOffset())
                : entry);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S extends SagaState> Optional<LoadedSaga<S>> load(
      SagaId sagaId, SagaType sagaType, Class<S> stateType) {
    requireKey(sagaId, sagaType);
    if (stateType == null) throw new IllegalArgumentException("stateType must not be null");
    SagaEntry entry = store.get(sagaId);
    if (entry == null || !entry.sagaType().equals(sagaType)) {
      return Optional.empty();
    }
    return Optional.of((LoadedSaga<S>) (LoadedSaga<?>) entry.toLoaded());
  }

  @Override
  public void delete(SagaId sagaId, SagaType sagaType) {
    requireKey(sagaId, sagaType);
    // Only remove the row if this saga type owns it, so a cross-type SagaId collision
    // cannot delete another type's state.
    store.computeIfPresent(sagaId, (id, entry) -> entry.sagaType().equals(sagaType) ? null : entry);
  }

  @Override
  public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
    requireType(sagaType);
    if (cutoff == null) throw new IllegalArgumentException("cutoff must not be null");
    // Mirrors PostgresSagaStore.SELECT_TIMED_OUT: each population in its own deadline order, each
    // limited to the whole batch, then interleaved by rank, forward first (SagaStore#findTimedOut).
    List<SagaId> forward =
        oldestFirst(
            sagaType,
            cutoff,
            limit,
            status -> status == SagaStatus.STARTED || status == SagaStatus.RUNNING);
    List<SagaId> compensating =
        oldestFirst(sagaType, cutoff, limit, status -> status == SagaStatus.COMPENSATING);
    List<SagaId> batch = new ArrayList<>();
    for (int slot = 0; slot < Math.max(forward.size(), compensating.size()); slot++) {
      if (slot < forward.size() && batch.size() < limit) batch.add(forward.get(slot));
      if (slot < compensating.size() && batch.size() < limit) batch.add(compensating.get(slot));
    }
    return List.copyOf(batch);
  }

  /**
   * The timed-out rows of one population: those whose {@linkplain #timeoutReference timeout
   * reference} is before {@code cutoff}, oldest reference first, at most {@code limit}.
   */
  private List<SagaId> oldestFirst(
      SagaType sagaType, Instant cutoff, int limit, Predicate<SagaStatus> population) {
    return store.entrySet().stream()
        .filter(e -> e.getValue().sagaType().equals(sagaType))
        .filter(e -> population.test(e.getValue().status()))
        .filter(e -> timeoutReference(e.getValue()).isBefore(cutoff))
        .sorted(Comparator.comparing(e -> timeoutReference(e.getValue())))
        .limit(limit)
        .map(Map.Entry::getKey)
        .toList();
  }

  /**
   * The instant {@link #findTimedOut} compares against the cutoff and orders by. A {@code
   * STARTED}/{@code RUNNING} saga is timed out on its ABSOLUTE start instant ({@code createdAt}),
   * so a multi-step saga that keeps receiving events still times out once it has been non-terminal
   * past its deadline. A {@code COMPENSATING} saga uses {@code updatedAt} (inactivity) so a freshly
   * claimed crash-resume episode is spaced out until it ages past the cutoff — matching {@code
   * PostgresSagaStore.SELECT_TIMED_OUT}.
   */
  private static Instant timeoutReference(SagaEntry entry) {
    return entry.status() == SagaStatus.COMPENSATING ? entry.updatedAt() : entry.createdAt();
  }

  @Override
  public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
    requireType(sagaType);
    requireStatus(status);
    return store.entrySet().stream()
        .filter(e -> e.getValue().sagaType().equals(sagaType))
        .filter(e -> e.getValue().status() == status)
        .sorted(
            Comparator.<Map.Entry<SagaId, SagaEntry>, Instant>comparing(
                    e -> e.getValue().updatedAt())
                .reversed())
        .limit(limit)
        .map(Map.Entry::getKey)
        .toList();
  }

  @Override
  public long countByStatus(SagaType sagaType, SagaStatus status) {
    requireType(sagaType);
    requireStatus(status);
    return store.values().stream()
        .filter(e -> e.sagaType().equals(sagaType))
        .filter(e -> e.status() == status)
        .count();
  }

  @Override
  public List<CompensatingSaga> findCompensating(
      SagaType sagaType, Instant updatedBefore, int limit) {
    requireType(sagaType);
    if (updatedBefore == null) throw new IllegalArgumentException("updatedBefore must not be null");
    return store.entrySet().stream()
        .filter(e -> e.getValue().sagaType().equals(sagaType))
        .filter(e -> e.getValue().status() == SagaStatus.COMPENSATING)
        .filter(e -> e.getValue().updatedAt().isBefore(updatedBefore))
        .sorted(Comparator.comparing(e -> e.getValue().updatedAt()))
        .limit(limit)
        // Surface the fault-cycle-immutable claim instant — the sweeper's give-up
        // anchor (updatedAt remains only the due cutoff / re-drive spacing).
        .map(
            e ->
                new CompensatingSaga(
                    e.getKey(), e.getValue().updatedAt(), e.getValue().episodeClaimedAt()))
        .toList();
  }

  /** Returns all stored saga IDs. For test assertions only. */
  public Set<SagaId> all() {
    return Set.copyOf(store.keySet());
  }

  /**
   * The precondition of every write that takes a state and a status, run FIRST: a {@code null}
   * argument is a caller bug, refused before the map is touched ({@code SagaStoreContract}).
   */
  private static void requireArgs(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status) {
    requireKey(sagaId, sagaType);
    if (state == null) throw new IllegalArgumentException("state must not be null");
    requireStatus(status);
  }

  private static void requireKey(SagaId sagaId, SagaType sagaType) {
    if (sagaId == null) throw new IllegalArgumentException("sagaId must not be null");
    requireType(sagaType);
  }

  private static void requireType(SagaType sagaType) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
  }

  private static void requireStatus(SagaStatus status) {
    if (status == null) throw new IllegalArgumentException("status must not be null");
  }

  private static OptimisticLockException staleView(SagaId sagaId, long expectedVersion) {
    return new OptimisticLockException(
        "Stale view of saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + " (expectedVersion="
            + expectedVersion
            + ")");
  }
}
