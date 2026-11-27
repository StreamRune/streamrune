package org.streamrune.runtime.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import org.streamrune.core.Internal;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.CompensatingSaga;
import org.streamrune.core.saga.SagaStore.RowFacts;
import org.streamrune.core.saga.SagaTypeCollisionException;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * The rules both saga stores apply around their statements, and the decoding of the rows they read:
 * the argument preconditions, the log-safe rendering of a saga id, the refusals of a stale view and
 * of a conflicting create, the episode stamp rule's condition, the conversion of saga state to and
 * from JSON, and the mapping of a {@code saga_state} row to {@link LoadedSaga}, {@link RowFacts}
 * and {@link CompensatingSaga}.
 *
 * <p>It holds no statement: each store keeps its own SQL, binds, transaction and lock scope, and
 * every write of {@code saga_state} stays in the store that owns it. The row readers take the
 * caller's {@link ResultSet}, positioned on a row, read the timestamp and flag columns through the
 * backend's {@link JdbcInstantCodec} and let every {@link SQLException} propagate unwrapped; a
 * corrupt enum value surfaces as the {@link IllegalArgumentException} of {@link
 * SagaStatus#valueOf}, which the store wraps as it does any read failure. The helper never opens,
 * commits, rolls back or closes a connection and keeps no state.
 *
 * <p>The conversions run outside any connection scope, as the stores call them: with a crypto-aware
 * mapper they may call a key store. A conversion failure is a {@link
 * SagaStateSerializationException}, deliberately not a storage failure, so a deterministic failure
 * is not retried forever as infrastructure.
 *
 * @hidden
 */
@Internal
public final class SagaRows {

  private SagaRows() {}

  /**
   * The columns of a full saga read, decoded while the connection is open; the state stays JSON
   * until {@link #assemble} converts it after the connection is returned.
   *
   * @param stateJson the stored state
   * @param status the status
   * @param version the row version
   * @param updatedAt the last write's instant
   * @param episodeVersion the compensation episode's stamp, or {@code null}
   * @param episodeClaimedAt the episode's claim instant, or {@code null}
   * @param episodeCause the event that opened the episode, or {@code null}
   * @param genesisApplied whether the start event's forward step committed
   * @param preFaultStatus the status a fault replay resumes from, or {@code null}
   * @param deadLetterPending whether the saga owns a dead-letter entry
   * @param lastAppliedOffset the highest applied event offset, or {@code null}
   * @param lastReplayedOffset the last replayed event offset, or {@code null}
   * @hidden
   */
  @Internal
  public record StoredRow(
      String stateJson,
      SagaStatus status,
      long version,
      Instant updatedAt,
      Long episodeVersion,
      Instant episodeClaimedAt,
      EventId episodeCause,
      boolean genesisApplied,
      SagaStatus preFaultStatus,
      boolean deadLetterPending,
      Long lastAppliedOffset,
      Long lastReplayedOffset) {

    /**
     * Decodes the row {@code rs} is positioned on. It reads {@code state_json}, {@code status},
     * {@code version}, {@code updated_at} (required), {@code episode_version}, {@code
     * episode_claimed_at} (nullable), {@code episode_cause_event_id}, {@code genesis_applied},
     * {@code pre_fault_status}, {@code dead_letter_pending}, {@code last_applied_offset} and {@code
     * last_replayed_offset}, in that order.
     *
     * @param rs the caller's result set, positioned on a row
     * @param codec the backend's timestamp and flag decoding
     * @return the decoded row
     * @throws SQLException if a read fails
     * @throws IllegalArgumentException if a status column holds no {@link SagaStatus}, or the cause
     *     column is blank
     */
    public static StoredRow of(ResultSet rs, JdbcInstantCodec codec) throws SQLException {
      String stateJson = rs.getString("state_json");
      SagaStatus status = SagaStatus.valueOf(rs.getString("status"));
      long version = rs.getLong("version");
      Instant updatedAt = codec.readInstant(rs, "updated_at");
      // Both halves of the stamp are NULL only for a row that never entered a compensation episode.
      Long episodeVersion = nullableLong(rs, "episode_version");
      Instant episodeClaimedAt = codec.readNullableInstant(rs, "episode_claimed_at");
      // NULL for a row that never entered an episode and for an episode opened with no event in
      // hand (a timeout claim, an operator's compensation).
      String cause = rs.getString("episode_cause_event_id");
      EventId episodeCause = cause == null ? null : new EventId(cause);
      boolean genesisApplied = codec.readFlag(rs, "genesis_applied");
      String preFault = rs.getString("pre_fault_status");
      SagaStatus preFaultStatus = preFault == null ? null : SagaStatus.valueOf(preFault);
      boolean deadLetterPending = codec.readFlag(rs, "dead_letter_pending");
      Long lastAppliedOffset = nullableLong(rs, "last_applied_offset");
      Long lastReplayedOffset = nullableLong(rs, "last_replayed_offset");
      return new StoredRow(
          stateJson,
          status,
          version,
          updatedAt,
          episodeVersion,
          episodeClaimedAt,
          episodeCause,
          genesisApplied,
          preFaultStatus,
          deadLetterPending,
          lastAppliedOffset,
          lastReplayedOffset);
    }
  }

  /**
   * The type precondition every operation runs first, before it serializes (which may call a key
   * store) or borrows a connection, so a rejected call has no side effect. Left to the JDBC bind, a
   * {@code null} would fail inside the storage {@code catch} as a storage failure the saga runtime
   * retries forever.
   *
   * @param sagaType the saga type
   * @throws IllegalArgumentException if {@code sagaType} is {@code null}
   */
  public static void requireType(SagaType sagaType) {
    if (sagaType == null) {
      throw new IllegalArgumentException("sagaType must not be null");
    }
  }

  /**
   * The status precondition, run first for the same reason as {@link #requireType}.
   *
   * @param status the status
   * @throws IllegalArgumentException if {@code status} is {@code null}
   */
  public static void requireStatus(SagaStatus status) {
    if (status == null) {
      throw new IllegalArgumentException("status must not be null");
    }
  }

  /**
   * The id precondition, then {@link #requireType}, for every operation that takes a {@link
   * SagaId}.
   *
   * @param sagaId the saga id
   * @param sagaType the saga type
   * @throws IllegalArgumentException if either is {@code null}, the id checked first
   */
  public static void requireKey(SagaId sagaId, SagaType sagaType) {
    if (sagaId == null) {
      throw new IllegalArgumentException("sagaId must not be null");
    }
    requireType(sagaType);
  }

  /**
   * {@link #requireKey}, then the state, then the status, for every write that persists both.
   *
   * @param sagaId the saga id
   * @param sagaType the saga type
   * @param state the state to persist
   * @param status the status to persist
   * @throws IllegalArgumentException if an argument is {@code null}, checked in that order
   */
  public static void requireWrite(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status) {
    requireKey(sagaId, sagaType);
    if (state == null) {
      throw new IllegalArgumentException("state must not be null");
    }
    requireStatus(status);
  }

  /**
   * The log-safe rendering of a saga id for an exception message: a saga id is usually derived from
   * correlated event data and carries no charset bound. Callers have passed {@link #requireKey}.
   *
   * @param sagaId the saga id, not {@code null}
   * @return the sanitized id
   */
  public static String forLog(SagaId sagaId) {
    return LogSanitizer.sanitizeForLog(sagaId.value());
  }

  /**
   * The refusal of a compare-and-swap that changed no row: a stale version, a terminal row, a row
   * of another type or no row.
   *
   * @param sagaId the saga id
   * @param expectedVersion the version the caller expected
   * @return the exception to throw
   */
  public static OptimisticLockException staleView(SagaId sagaId, long expectedVersion) {
    return new OptimisticLockException(
        "Stale view of saga " + forLog(sagaId) + " (expectedVersion=" + expectedVersion + ")");
  }

  /**
   * The bind value of an episode cause.
   *
   * @param episodeCauseOrNull the event that opens the episode, or {@code null}
   * @return the event id's text, or {@code null} for SQL {@code NULL}
   */
  public static String causeValue(EventId episodeCauseOrNull) {
    return episodeCauseOrNull == null ? null : episodeCauseOrNull.value();
  }

  /**
   * The condition of the episode stamp rule: a write that lands the row at {@code COMPENSATING}
   * stamps the episode unless the row already carries a stamp.
   *
   * @param status the status the write lands the row at
   * @return whether {@code status} is {@code COMPENSATING}
   */
  public static boolean entersEpisode(SagaStatus status) {
    return status == SagaStatus.COMPENSATING;
  }

  /**
   * Converts saga state to its stored JSON. The store calls it before it borrows a connection: with
   * a crypto-aware mapper the conversion may call a key store, possibly through the store's own
   * pool.
   *
   * @param mapper the store's mapper
   * @param sagaId the saga id, for the failure
   * @param state the state
   * @return the JSON
   * @throws SagaStateSerializationException on any failure of the mapper
   */
  public static String serialize(ObjectMapper mapper, SagaId sagaId, SagaState state) {
    try {
      return mapper.writeValueAsString(state);
    } catch (Exception e) {
      throw new SagaStateSerializationException(
          "Failed to serialize saga state: " + forLog(sagaId), sagaId, e);
    }
  }

  /**
   * The refusal of a create whose insert hit a conflict; the store calls it only then. With no
   * stored type (the conflicting row is gone by the time its type is read) and with the same type,
   * the create is the benign concurrent-create dedup; with another type, it is a loud cross-type
   * collision. The stored type is handed to {@link SagaType#of} as read, so a blank one fails there
   * with {@link IllegalArgumentException}, and the caller decides what that becomes.
   *
   * @param sagaId the saga id
   * @param sagaType the type the create was for
   * @param storedType the type of the stored row, empty if none was found
   * @throws OptimisticLockException if no type was found, or the stored type is {@code sagaType}
   * @throws SagaTypeCollisionException if another type owns {@code sagaId}
   */
  public static void refuseConflict(SagaId sagaId, SagaType sagaType, Optional<String> storedType) {
    if (storedType.isPresent() && !storedType.get().equals(sagaType.value())) {
      throw new SagaTypeCollisionException(sagaId, SagaType.of(storedType.get()), sagaType);
    }
    throw new OptimisticLockException("Saga " + forLog(sagaId) + " already exists");
  }

  /**
   * Converts a row read by {@link StoredRow#of} to a {@link LoadedSaga}. The store calls it after
   * the connection is returned: with a crypto-aware mapper the conversion may decrypt through a key
   * store.
   *
   * @param mapper the store's mapper
   * @param sagaId the saga id, for the failure
   * @param row the decoded row
   * @param stateType the state class
   * @param <S> the state type
   * @return the loaded saga with the row's fields
   * @throws SagaStateSerializationException on any failure of the mapper
   */
  public static <S extends SagaState> LoadedSaga<S> assemble(
      ObjectMapper mapper, SagaId sagaId, StoredRow row, Class<S> stateType) {
    S state;
    try {
      state = mapper.readValue(row.stateJson(), stateType);
    } catch (Exception e) {
      throw new SagaStateSerializationException(
          "Failed to deserialize saga state: " + forLog(sagaId), sagaId, e);
    }
    return new LoadedSaga<>(
        state,
        row.status(),
        row.version(),
        row.updatedAt(),
        row.episodeVersion(),
        row.episodeClaimedAt(),
        row.episodeCause(),
        row.genesisApplied(),
        row.preFaultStatus(),
        row.deadLetterPending(),
        row.lastAppliedOffset(),
        row.lastReplayedOffset());
  }

  /**
   * Decodes a row-facts read: {@code status}, {@code dead_letter_pending} (a flag) and {@code
   * last_applied_offset} (nullable), in that order.
   *
   * @param rs the caller's result set, positioned on a row
   * @param codec the backend's flag decoding
   * @return the row's facts
   * @throws SQLException if a read fails
   * @throws IllegalArgumentException if the status column holds no {@link SagaStatus}
   */
  public static RowFacts rowFacts(ResultSet rs, JdbcInstantCodec codec) throws SQLException {
    SagaStatus status = SagaStatus.valueOf(rs.getString("status"));
    boolean deadLetterPending = codec.readFlag(rs, "dead_letter_pending");
    Long lastAppliedOffset = nullableLong(rs, "last_applied_offset");
    return new RowFacts(status, deadLetterPending, lastAppliedOffset);
  }

  /**
   * Decodes a compensation-scan row: {@code episode_claimed_at} (nullable; never {@code NULL} on a
   * conforming {@code COMPENSATING} row, but kept nullable so the sweeper refuses a non-conforming
   * one per saga), then {@code saga_id} and {@code updated_at} (required), in that order.
   *
   * @param rs the caller's result set, positioned on a row
   * @param codec the backend's timestamp decoding
   * @return the compensating saga
   * @throws SQLException if a read fails
   * @throws IllegalArgumentException if the id column holds no valid {@link SagaId}
   */
  public static CompensatingSaga compensatingSaga(ResultSet rs, JdbcInstantCodec codec)
      throws SQLException {
    Instant episodeClaimedAt = codec.readNullableInstant(rs, "episode_claimed_at");
    SagaId sagaId = SagaId.of(rs.getString("saga_id"));
    Instant updatedAt = codec.readInstant(rs, "updated_at");
    return new CompensatingSaga(sagaId, updatedAt, episodeClaimedAt);
  }

  private static Long nullableLong(ResultSet rs, String column) throws SQLException {
    long value = rs.getLong(column);
    return rs.wasNull() ? null : value;
  }
}
