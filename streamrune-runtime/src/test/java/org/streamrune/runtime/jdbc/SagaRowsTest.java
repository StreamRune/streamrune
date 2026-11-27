package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
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
import org.streamrune.core.types.SagaType;

class SagaRowsTest {

  private static final SagaId ID = SagaId.of("order-1");
  private static final SagaType TYPE = SagaType.of("Fulfillment");
  private static final TestState STATE = new TestState(SagaStatus.RUNNING, "n");

  /** A saga id that renders differently once sanitized for a log line. */
  private static final SagaId UNSAFE_ID = SagaId.of("order\n1");

  private static final Instant UPDATED = Instant.parse("2026-11-27T10:15:30.123456Z");
  private static final Instant CLAIMED = Instant.parse("2026-11-27T10:15:31.654321Z");

  @TempDir Path dir;

  private JdbcTestDatabase db;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void createTable() throws SQLException {
    db =
        JdbcTestDatabase.create(
            dir,
            "CREATE TABLE saga_state (saga_id TEXT PRIMARY KEY, state_json TEXT, status TEXT,"
                + " version INTEGER, updated_at INTEGER, episode_version INTEGER,"
                + " episode_claimed_at INTEGER, episode_cause_event_id TEXT, genesis_applied"
                + " INTEGER, pre_fault_status TEXT, dead_letter_pending INTEGER,"
                + " last_applied_offset INTEGER, last_replayed_offset INTEGER)");
  }

  // ---- rules ----

  @Test
  void validatorsMessages() {
    assertIllegal(() -> SagaRows.requireType(null), "sagaType must not be null");
    assertIllegal(() -> SagaRows.requireStatus(null), "status must not be null");
    assertIllegal(() -> SagaRows.requireKey(null, TYPE), "sagaId must not be null");
    assertIllegal(() -> SagaRows.requireKey(ID, null), "sagaType must not be null");
    assertIllegal(
        () -> SagaRows.requireWrite(null, TYPE, STATE, SagaStatus.RUNNING),
        "sagaId must not be null");
    assertIllegal(
        () -> SagaRows.requireWrite(ID, null, STATE, SagaStatus.RUNNING),
        "sagaType must not be null");
    assertIllegal(
        () -> SagaRows.requireWrite(ID, TYPE, null, SagaStatus.RUNNING), "state must not be null");
    assertIllegal(() -> SagaRows.requireWrite(ID, TYPE, STATE, null), "status must not be null");
    // The order of the checks: the id before the type, the state before the status.
    assertIllegal(() -> SagaRows.requireWrite(null, null, null, null), "sagaId must not be null");
    assertIllegal(() -> SagaRows.requireWrite(ID, TYPE, null, null), "state must not be null");

    SagaRows.requireType(TYPE);
    SagaRows.requireStatus(SagaStatus.FAULTED);
    SagaRows.requireKey(ID, TYPE);
    SagaRows.requireWrite(ID, TYPE, STATE, SagaStatus.RUNNING);
  }

  @Test
  void forLogSanitizesTheId() {
    assertThat(SagaRows.forLog(ID)).isEqualTo("order-1");
    assertThat(SagaRows.forLog(UNSAFE_ID)).doesNotContain("\n");
  }

  @Test
  void staleViewMessage() {
    OptimisticLockException e = SagaRows.staleView(ID, 7L);

    assertThat(e).hasMessage("Stale view of saga order-1 (expectedVersion=7)").hasNoCause();
    assertThat(SagaRows.staleView(UNSAFE_ID, 1L).getMessage())
        .isEqualTo("Stale view of saga " + SagaRows.forLog(UNSAFE_ID) + " (expectedVersion=1)");
  }

  @Test
  void entersEpisodeOnlyWhenCompensating() {
    for (SagaStatus status : SagaStatus.values()) {
      assertThat(SagaRows.entersEpisode(status))
          .as(status.name())
          .isEqualTo(status == SagaStatus.COMPENSATING);
    }
  }

  @Test
  void causeValueOfNullIsNull() {
    assertThat(SagaRows.causeValue(null)).isNull();
    assertThat(SagaRows.causeValue(new EventId("evt-9"))).isEqualTo("evt-9");
  }

  @Test
  void serializeWritesTheMappersJson() {
    assertThat(SagaRows.serialize(mapper, ID, STATE))
        .isEqualTo("{\"status\":\"RUNNING\",\"note\":\"n\"}");
  }

  @Test
  void serializeFailure_isSagaStateSerializationException() {
    var refusing =
        new ObjectMapper() {
          @Override
          public String writeValueAsString(Object value) throws JsonProcessingException {
            throw new JsonProcessingException("refused") {};
          }
        };

    var e =
        assertThrows(
            SagaStateSerializationException.class,
            () -> SagaRows.serialize(refusing, UNSAFE_ID, STATE));

    assertThat(e).hasMessage("Failed to serialize saga state: " + SagaRows.forLog(UNSAFE_ID));
    assertThat(e.sagaId()).isEqualTo(UNSAFE_ID);
    assertThat(e.getCause()).hasMessage("refused");
  }

  @Test
  void serializeWrapsAnUncheckedFailureToo() {
    var refusing =
        new ObjectMapper() {
          @Override
          public String writeValueAsString(Object value) {
            throw new IllegalStateException("crypto refused");
          }
        };

    var e =
        assertThrows(
            SagaStateSerializationException.class, () -> SagaRows.serialize(refusing, ID, STATE));

    assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void refuseConflict_emptyIsAlreadyExists() {
    var e =
        assertThrows(
            OptimisticLockException.class,
            () -> SagaRows.refuseConflict(UNSAFE_ID, TYPE, Optional.empty()));

    assertThat(e).hasMessage("Saga " + SagaRows.forLog(UNSAFE_ID) + " already exists");
  }

  @Test
  void refuseConflict_otherTypeIsCollision() {
    var e =
        assertThrows(
            SagaTypeCollisionException.class,
            () -> SagaRows.refuseConflict(ID, TYPE, Optional.of("Refund")));

    assertThat(e.sagaId()).isEqualTo(ID);
    assertThat(e.existingType()).isEqualTo(SagaType.of("Refund"));
    assertThat(e.attemptedType()).isEqualTo(TYPE);
  }

  @Test
  void refuseConflict_sameTypeIsAlreadyExists() {
    var e =
        assertThrows(
            OptimisticLockException.class,
            () -> SagaRows.refuseConflict(ID, TYPE, Optional.of("Fulfillment")));

    assertThat(e).hasMessage("Saga order-1 already exists");
  }

  @Test
  void refuseConflict_presentBlankTypeFailsInSagaType() {
    // The stored type is handed to SagaType.of as it is: a blank one fails there, and the caller's
    // catch decides what that becomes.
    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> SagaRows.refuseConflict(ID, TYPE, Optional.of("")));

    assertThat(e).hasMessage("SagaType value must not be blank");
  }

  // ---- row decoding ----

  @Test
  void storedRow_decodesEveryColumn() throws SQLException {
    insertFullRow("order-1", "COMPENSATING");

    SagaRows.StoredRow row = readStoredRow(new MicrosCodec());

    assertThat(row.stateJson()).isEqualTo("{\"status\":\"RUNNING\",\"note\":\"n\"}");
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.version()).isEqualTo(5L);
    assertThat(row.updatedAt()).isEqualTo(UPDATED);
    assertThat(row.episodeVersion()).isEqualTo(4L);
    assertThat(row.episodeClaimedAt()).isEqualTo(CLAIMED);
    assertThat(row.episodeCause()).isEqualTo(new EventId("evt-3"));
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(row.lastAppliedOffset()).isEqualTo(41L);
    assertThat(row.lastReplayedOffset()).isEqualTo(40L);
  }

  @Test
  void storedRow_decodesNullsAsNullAndZeroFlagsAsFalse() throws SQLException {
    exec(
        "INSERT INTO saga_state (saga_id, state_json, status, version, updated_at,"
            + " genesis_applied, dead_letter_pending) VALUES ('order-1', '{}', 'RUNNING', 1, "
            + micros(UPDATED)
            + ", 0, 0)");

    SagaRows.StoredRow row = readStoredRow(new MicrosCodec());

    assertThat(row.episodeVersion()).isNull();
    assertThat(row.episodeClaimedAt()).isNull();
    assertThat(row.episodeCause()).isNull();
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.preFaultStatus()).isNull();
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(row.lastAppliedOffset()).isNull();
    assertThat(row.lastReplayedOffset()).isNull();
  }

  @Test
  void storedRow_zeroOffsetsAreNotNull() throws SQLException {
    exec(
        "INSERT INTO saga_state (saga_id, state_json, status, version, updated_at,"
            + " episode_version, genesis_applied, dead_letter_pending, last_applied_offset,"
            + " last_replayed_offset) VALUES ('order-1', '{}', 'RUNNING', 1, 0, 0, 0, 0, 0, 0)");

    SagaRows.StoredRow row = readStoredRow(new MicrosCodec());

    assertThat(row.episodeVersion()).isZero();
    assertThat(row.lastAppliedOffset()).isZero();
    assertThat(row.lastReplayedOffset()).isZero();
  }

  @Test
  void storedRow_corruptStatusThrowsIllegalArgument() throws SQLException {
    insertFullRow("order-1", "NOT_A_STATUS");

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> readStoredRow(new MicrosCodec()));

    assertThat(e).hasMessageContaining("NOT_A_STATUS");
  }

  @Test
  void storedRow_corruptPreFaultStatusThrowsIllegalArgument() throws SQLException {
    insertFullRow("order-1", "FAULTED");
    exec("UPDATE saga_state SET pre_fault_status = 'BOGUS'");

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> readStoredRow(new MicrosCodec()));

    assertThat(e).hasMessageContaining("BOGUS");
  }

  @Test
  void storedRow_blankCauseFailsWithTheEventIdMessage() throws SQLException {
    insertFullRow("order-1", "COMPENSATING");
    exec("UPDATE saga_state SET episode_cause_event_id = ''");

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> readStoredRow(new MicrosCodec()));

    assertThat(e).hasMessage("eventId is required");
  }

  @Test
  void storedRow_aRowCorruptInSeveralColumnsFailsOnItsStatusFirst() throws SQLException {
    // Both backends decode the status before the cause and the cause before the pre-fault status,
    // so a row corrupt in all three fails the same way on either: on its status.
    insertFullRow("order-1", "NOT_A_STATUS");
    exec("UPDATE saga_state SET episode_cause_event_id = '', pre_fault_status = 'BOGUS'");

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> readStoredRow(new MicrosCodec()));

    assertThat(e).hasMessageContaining("NOT_A_STATUS").hasMessageNotContaining("BOGUS");
    assertThat(e.getMessage()).isNotEqualTo("eventId is required");
  }

  @Test
  void assemble_buildsTheLoadedSagaFromTheRow() throws SQLException {
    insertFullRow("order-1", "COMPENSATING");
    SagaRows.StoredRow row = readStoredRow(new MicrosCodec());

    LoadedSaga<TestState> loaded = SagaRows.assemble(mapper, ID, row, TestState.class);

    assertThat(loaded)
        .isEqualTo(
            new LoadedSaga<>(
                STATE,
                SagaStatus.COMPENSATING,
                5L,
                UPDATED,
                4L,
                CLAIMED,
                new EventId("evt-3"),
                true,
                SagaStatus.RUNNING,
                true,
                41L,
                40L));
  }

  @Test
  void assemble_corruptJsonIsSagaStateSerializationException() throws SQLException {
    insertFullRow("order-1", "RUNNING");
    exec("UPDATE saga_state SET state_json = 'not json'");
    SagaRows.StoredRow row = readStoredRow(new MicrosCodec());

    var e =
        assertThrows(
            SagaStateSerializationException.class,
            () -> SagaRows.assemble(mapper, UNSAFE_ID, row, TestState.class));

    assertThat(e).hasMessage("Failed to deserialize saga state: " + SagaRows.forLog(UNSAFE_ID));
    assertThat(e.sagaId()).isEqualTo(UNSAFE_ID);
    assertThat(e.getCause()).isInstanceOf(JsonProcessingException.class);
  }

  @Test
  void assemble_uncheckedMapperFailureIsSagaStateSerializationException() throws SQLException {
    insertFullRow("order-1", "RUNNING");
    SagaRows.StoredRow row = readStoredRow(new MicrosCodec());
    var refusing =
        new ObjectMapper() {
          @Override
          public <T> T readValue(String content, Class<T> valueType) {
            throw new IllegalStateException("key store down");
          }
        };

    var e =
        assertThrows(
            SagaStateSerializationException.class,
            () -> SagaRows.assemble(refusing, ID, row, TestState.class));

    assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void rowFacts_decodes() throws SQLException {
    insertFullRow("order-1", "FAULTED");

    RowFacts facts =
        readOne(
            "SELECT status, dead_letter_pending, last_applied_offset FROM saga_state",
            rs -> SagaRows.rowFacts(rs, new MicrosCodec()));

    assertThat(facts).isEqualTo(new RowFacts(SagaStatus.FAULTED, true, 41L));
  }

  @Test
  void rowFacts_decodesNullOffsetAndClearFlag() throws SQLException {
    exec(
        "INSERT INTO saga_state (saga_id, status, dead_letter_pending) VALUES ('order-1',"
            + " 'RUNNING', 0)");

    RowFacts facts =
        readOne(
            "SELECT status, dead_letter_pending, last_applied_offset FROM saga_state",
            rs -> SagaRows.rowFacts(rs, new MicrosCodec()));

    assertThat(facts).isEqualTo(new RowFacts(SagaStatus.RUNNING, false, null));
  }

  @Test
  void compensatingSaga_decodes() throws SQLException {
    insertFullRow("order-1", "COMPENSATING");

    CompensatingSaga saga =
        readOne(
            "SELECT saga_id, updated_at, episode_claimed_at FROM saga_state",
            rs -> SagaRows.compensatingSaga(rs, new MicrosCodec()));

    assertThat(saga).isEqualTo(new CompensatingSaga(ID, UPDATED, CLAIMED));
  }

  @Test
  void compensatingSaga_decodesAMissingClaimAsNull() throws SQLException {
    exec(
        "INSERT INTO saga_state (saga_id, status, updated_at) VALUES ('order-1', 'COMPENSATING', "
            + micros(UPDATED)
            + ")");

    CompensatingSaga saga =
        readOne(
            "SELECT saga_id, updated_at, episode_claimed_at FROM saga_state",
            rs -> SagaRows.compensatingSaga(rs, new MicrosCodec()));

    assertThat(saga).isEqualTo(new CompensatingSaga(ID, UPDATED, null));
  }

  @Test
  void codecIsUsedForEveryTimeAndFlagColumn() throws SQLException {
    insertFullRow("order-1", "COMPENSATING");
    var codec = new MicrosCodec();

    readStoredRow(codec);
    assertThat(codec.reads)
        .containsExactly(
            "required updated_at",
            "nullable episode_claimed_at",
            "flag genesis_applied",
            "flag dead_letter_pending");

    codec.reads.clear();
    readOne(
        "SELECT status, dead_letter_pending, last_applied_offset FROM saga_state",
        rs -> SagaRows.rowFacts(rs, codec));
    assertThat(codec.reads).containsExactly("flag dead_letter_pending");

    codec.reads.clear();
    readOne(
        "SELECT saga_id, updated_at, episode_claimed_at FROM saga_state",
        rs -> SagaRows.compensatingSaga(rs, codec));
    assertThat(codec.reads).containsExactly("nullable episode_claimed_at", "required updated_at");
  }

  // ---- fixtures ----

  /** A saga state Jackson round-trips. */
  record TestState(SagaStatus status, String note) implements SagaState {
    @JsonCreator
    TestState(@JsonProperty("status") SagaStatus status, @JsonProperty("note") String note) {
      this.status = status;
      this.note = note;
    }
  }

  /**
   * A codec decoding as SQLite does (epoch microseconds in an {@code INTEGER}, a flag that is true
   * when non-zero), recording every column it is asked for.
   */
  private static final class MicrosCodec implements JdbcInstantCodec {

    final List<String> reads = new ArrayList<>();

    @Override
    public Instant readInstant(ResultSet rs, String column) throws SQLException {
      reads.add("required " + column);
      return fromMicros(rs.getLong(column));
    }

    @Override
    public Instant readNullableInstant(ResultSet rs, String column) throws SQLException {
      reads.add("nullable " + column);
      long micros = rs.getLong(column);
      return rs.wasNull() ? null : fromMicros(micros);
    }

    @Override
    public boolean readFlag(ResultSet rs, String column) throws SQLException {
      reads.add("flag " + column);
      return rs.getInt(column) != 0;
    }

    @Override
    public void bindInstant(PreparedStatement ps, int index, Instant instant) throws SQLException {
      ps.setLong(index, micros(instant));
    }
  }

  @FunctionalInterface
  private interface RowReader<T> {
    T read(ResultSet rs) throws SQLException;
  }

  private static long micros(Instant instant) {
    return Math.addExact(
        Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
  }

  private static Instant fromMicros(long micros) {
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
  }

  private static void assertIllegal(Executable call, String message) {
    assertThat(assertThrows(IllegalArgumentException.class, call)).hasMessage(message);
  }

  private void insertFullRow(String sagaId, String status) throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement(
                "INSERT INTO saga_state (saga_id, state_json, status, version, updated_at,"
                    + " episode_version, episode_claimed_at, episode_cause_event_id,"
                    + " genesis_applied, pre_fault_status, dead_letter_pending,"
                    + " last_applied_offset, last_replayed_offset)"
                    + " VALUES (?, ?, ?, 5, ?, 4, ?, 'evt-3', 1, 'RUNNING', 1, 41, 40)")) {
      ps.setString(1, sagaId);
      ps.setString(2, "{\"status\":\"RUNNING\",\"note\":\"n\"}");
      ps.setString(3, status);
      ps.setLong(4, micros(UPDATED));
      ps.setLong(5, micros(CLAIMED));
      ps.executeUpdate();
    }
  }

  private void exec(String sql) throws SQLException {
    try (Connection c = db.connection();
        Statement st = c.createStatement()) {
      st.execute(sql);
    }
  }

  private SagaRows.StoredRow readStoredRow(JdbcInstantCodec codec) throws SQLException {
    return readOne(
        "SELECT state_json, status, version, updated_at, episode_version, episode_claimed_at,"
            + " episode_cause_event_id, genesis_applied, pre_fault_status, dead_letter_pending,"
            + " last_applied_offset, last_replayed_offset FROM saga_state",
        rs -> SagaRows.StoredRow.of(rs, codec));
  }

  private <T> T readOne(String sql, RowReader<T> reader) throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps = c.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).isTrue();
      return reader.read(rs);
    }
  }
}
