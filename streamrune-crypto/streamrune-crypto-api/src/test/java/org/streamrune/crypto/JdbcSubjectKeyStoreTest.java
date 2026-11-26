package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * The shared key-store flow on a SQLite file created from the shipped crypto DDL, with {@link
 * SqliteTestDialect}. Every call runs in a write transaction the test owns, on a connection that
 * fails the test when a helper commits, rolls back, closes it or changes its autoCommit.
 */
class JdbcSubjectKeyStoreTest {

  private static final String TABLE = "encryption_keys";
  private static final Instant NOW = Instant.parse("2026-01-02T03:04:05.123456Z");
  private static final SubjectId SUBJECT = SubjectId.of("alice@example.com");

  @TempDir Path dir;

  private SqliteCryptoTestDatabase db;
  private final List<String> calls = new ArrayList<>();
  private RecordingDialect dialect;
  private JdbcSubjectKeyStore store;

  @BeforeEach
  void setUp() throws Exception {
    db = SqliteCryptoTestDatabase.create(dir);
    dialect = new RecordingDialect(Clock.fixed(NOW, ZoneOffset.UTC), calls);
    store =
        new JdbcSubjectKeyStore(
            TABLE, dialect, new JdbcForgottenSubjects(dialect), () -> calls.add("raceHook"));
  }

  private String stored() {
    return JdbcSubjectKeyStore.storedSubjectId(SUBJECT);
  }

  @Test
  void constructorRejectsAnUnsafeTableName() {
    JdbcForgottenSubjects forgotten = new JdbcForgottenSubjects(dialect);
    for (String unsafe : List.of("keys; DROP", "a.b", "", "1keys", "keys\"")) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> new JdbcSubjectKeyStore(unsafe, dialect, forgotten, () -> {}),
              unsafe);
      assertTrue(e.getMessage().contains("[A-Za-z_][A-Za-z0-9_]*"), e.getMessage());
    }
    assertNotNull(new JdbcSubjectKeyStore("_Keys_2", dialect, forgotten, () -> {}));
  }

  @Test
  void constructorRejectsNullArguments() {
    JdbcForgottenSubjects forgotten = new JdbcForgottenSubjects(dialect);
    assertThrows(
        IllegalArgumentException.class,
        () -> new JdbcSubjectKeyStore(null, dialect, forgotten, () -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new JdbcSubjectKeyStore(TABLE, null, forgotten, () -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new JdbcSubjectKeyStore(TABLE, dialect, null, () -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new JdbcSubjectKeyStore(TABLE, dialect, forgotten, null));
  }

  @Test
  void storedSubjectIdIsTheSha256HexOfTheUtf8SubjectId() throws Exception {
    String expected =
        HexFormat.of()
            .formatHex(
                MessageDigest.getInstance("SHA-256")
                    .digest(SUBJECT.value().getBytes(StandardCharsets.UTF_8)));
    assertEquals(expected, JdbcSubjectKeyStore.storedSubjectId(SUBJECT));
    assertEquals(64, expected.length());
  }

  @Test
  void mintThenLoad_returnsTheSameKeyAndVersionOne() throws Exception {
    VersionedKey minted = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    assertEquals(1, minted.version());
    assertEquals("AES", minted.key().getAlgorithm());
    assertEquals(32, minted.key().getEncoded().length);

    VersionedKey loaded = db.inWrite(c -> store.loadKeyOrNull(c, SUBJECT));
    assertArrayEquals(minted.key().getEncoded(), loaded.key().getEncoded());
    assertEquals(1, loaded.version());

    calls.clear();
    VersionedKey again = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    assertArrayEquals(minted.key().getEncoded(), again.key().getEncoded());
    // An existing key is returned after the lock and before the tombstone check and the hook.
    assertEquals(List.of("lockSubject"), calls);
    assertEquals(
        NOW, SqliteTestDialect.fromMicros(db.selectLong("SELECT created_at FROM " + TABLE)));
  }

  @Test
  void loadKeyOrNull_returnsNullForASubjectWithoutAKey() throws Exception {
    assertNull(db.inWrite(c -> store.loadKeyOrNull(c, SUBJECT)));
  }

  @Test
  void mintRunsLockLoadTombstoneCheckHookThenInsert() throws Exception {
    db.inWrite(c -> store.loadOrCreateKey(recordingSql(c, calls), SUBJECT));
    assertEquals(
        List.of(
            "lockSubject",
            "SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?",
            "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?",
            "raceHook",
            "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?",
            "insertKey"),
        sharedTrace());
  }

  @Test
  void mintAfterErase_usesTheNextGeneration() throws Exception {
    VersionedKey first = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    db.inWrite(
        c -> {
          store.eraseKey(c, SUBJECT);
          return null;
        });
    db.inWrite(
        c -> {
          store.liftTombstone(c, SUBJECT);
          return null;
        });
    VersionedKey second = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    assertEquals(2, second.version());
    assertFalse(Arrays.equals(first.key().getEncoded(), second.key().getEncoded()));
  }

  @Test
  void mintBelowTheGenerationCap_usesTheLastGeneration() throws Exception {
    db.update(
        "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)",
        stored(),
        AesGcmV1Format.MAX_KEY_VERSION - 1);
    VersionedKey key = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    assertEquals(AesGcmV1Format.MAX_KEY_VERSION, key.version());
  }

  @Test
  void mintAtTheGenerationCap_throwsTodaysMessage() throws Exception {
    db.update(
        "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)",
        stored(),
        AesGcmV1Format.MAX_KEY_VERSION);
    CryptoOperationException e =
        assertThrows(
            CryptoOperationException.class,
            () -> db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT)));
    assertEquals(
        "Key-generation limit reached for subject: "
            + SUBJECT.redacted()
            + " — the subject has been erased and re-registered 126 times, exhausting the"
            + " single-byte generation counter. Re-register this person under a fresh subject"
            + " id.",
        e.getMessage());
    assertNull(e.getCause());
    assertEquals(0, db.count("SELECT count(*) FROM " + TABLE));
  }

  @Test
  void forgottenSubject_throwsSubjectForgotten_raceHookRunsAfterTheTombstoneCheck()
      throws Exception {
    db.update(
        "INSERT INTO forgotten_subjects (subject_id, forgotten_at) VALUES (?, ?)", stored(), 1L);
    List<String> order = new ArrayList<>();
    JdbcSubjectKeyStore hooked =
        new JdbcSubjectKeyStore(
            TABLE, dialect, new JdbcForgottenSubjects(dialect), () -> order.add("raceHook"));

    SubjectForgottenException e =
        assertThrows(
            SubjectForgottenException.class,
            () -> db.inWrite(c -> hooked.loadOrCreateKey(recordingSql(c, order), SUBJECT)));
    assertEquals(
        "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
            + SUBJECT.redacted()
            + " — call reinstate(subjectId) to re-register",
        e.getMessage());
    assertEquals(
        List.of(
            "SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?",
            "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?",
            "raceHook"),
        order);
    assertEquals(0, db.count("SELECT count(*) FROM " + TABLE));
  }

  @Test
  void insertConflict_returnsTheWinner() throws Exception {
    // A winner's row lands just before this mint's insert, so the insert's ON CONFLICT DO NOTHING
    // inserts nothing and returns false. SQLite's write lock keeps another connection out while the
    // test's transaction runs, so the winner is written on the same connection.
    byte[] winnerBytes = new byte[32];
    Arrays.fill(winnerBytes, (byte) 7);
    dialect.insertKey =
        (conn, table, subject, bytes, version) -> {
          dialect.realInsertKey(conn, table, subject, winnerBytes.clone(), version);
          return dialect.realInsertKey(conn, table, subject, bytes, version);
        };
    VersionedKey key = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    assertArrayEquals(winnerBytes, key.key().getEncoded());
    assertEquals(1, key.version());
    assertEquals(1, db.count("SELECT count(*) FROM " + TABLE));
  }

  @Test
  void insertConflictWithVanishedWinner_throwsTodaysMessage() throws Exception {
    dialect.insertKey = (conn, table, subject, bytes, version) -> false;
    CryptoOperationException e =
        assertThrows(
            CryptoOperationException.class,
            () -> db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT)));
    assertEquals(
        "Key for subject vanished after insert conflict: " + SUBJECT.redacted(), e.getMessage());
    assertNull(e.getCause());
  }

  @Test
  void mintZeroesTheEncodedKeyItInserted() throws Exception {
    List<byte[]> passed = new ArrayList<>();
    List<byte[]> copies = new ArrayList<>();
    dialect.insertKey =
        (conn, table, subject, bytes, version) -> {
          passed.add(bytes);
          copies.add(bytes.clone());
          return dialect.realInsertKey(conn, table, subject, bytes, version);
        };
    VersionedKey key = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    assertArrayEquals(new byte[32], passed.getFirst());
    assertArrayEquals(copies.getFirst(), key.key().getEncoded());
  }

  @Test
  void mintZeroesTheEncodedKeyWhenTheInsertThrows() throws Exception {
    List<byte[]> passed = new ArrayList<>();
    dialect.insertKey =
        (conn, table, subject, bytes, version) -> {
          passed.add(bytes);
          throw new SQLException("disk I/O error");
        };
    assertThrows(SQLException.class, () -> db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT)));
    assertArrayEquals(new byte[32], passed.getFirst());
  }

  @Test
  void eraseRecordsGenerationDeletesKeyAndTombstones() throws Exception {
    db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    calls.clear();
    db.inWrite(
        c -> {
          store.eraseKey(recordingSql(c, calls), SUBJECT);
          return null;
        });
    assertEquals(
        List.of(
            "lockSubject",
            "SELECT key_version FROM encryption_keys WHERE subject_id = ?",
            "recordErasedGeneration",
            "DELETE FROM encryption_keys WHERE subject_id = ?",
            "insertEraseTombstone"),
        sharedTrace());
    assertEquals(0, db.count("SELECT count(*) FROM " + TABLE));
    assertEquals(
        1L,
        db.selectLong(
            "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?",
            stored()));
    assertEquals(
        SqliteTestDialect.toMicros(NOW),
        db.selectLong(
            "SELECT forgotten_at FROM forgotten_subjects WHERE subject_id = ?", stored()));
    assertNull(
        db.selectLong(
            "SELECT key_created_at FROM forgotten_subjects WHERE subject_id = ?", stored()));

    // A rerun converges: nothing left to record, the tombstone stays.
    db.inWrite(
        c -> {
          store.eraseKey(c, SUBJECT);
          return null;
        });
    assertEquals(1, db.count("SELECT count(*) FROM forgotten_subjects"));
    assertEquals(1L, db.selectLong("SELECT max_erased_version FROM erased_key_generations"));
  }

  @Test
  void eraseOfASubjectWithoutAKey_tombstonesOnly() throws Exception {
    db.inWrite(
        c -> {
          store.eraseKey(c, SUBJECT);
          return null;
        });
    assertEquals(List.of("lockSubject", "insertEraseTombstone"), calls);
    assertEquals(0, db.count("SELECT count(*) FROM erased_key_generations"));
    assertEquals(1, db.count("SELECT count(*) FROM forgotten_subjects"));
  }

  @Test
  void liftTombstone_deletesIt() throws Exception {
    db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    db.inWrite(
        c -> {
          store.eraseKey(c, SUBJECT);
          return null;
        });
    calls.clear();
    db.inWrite(
        c -> {
          store.liftTombstone(recordingSql(c, calls), SUBJECT);
          return null;
        });
    assertEquals(
        List.of("lockSubject", "DELETE FROM forgotten_subjects WHERE subject_id = ?"),
        sharedTrace());
    assertEquals(0, db.count("SELECT count(*) FROM forgotten_subjects"));
    // The erased generation stays, so the next mint uses the generation above it.
    assertEquals(1, db.count("SELECT count(*) FROM erased_key_generations"));
  }

  @Test
  void hasKey() throws Exception {
    assertFalse(db.inWriteBoolean(c -> store.hasKey(c, SUBJECT)));
    db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    assertTrue(db.inWriteBoolean(c -> store.hasKey(c, SUBJECT)));
    assertFalse(db.inWriteBoolean(c -> store.hasKey(c, SubjectId.of("bob"))));
    db.inWrite(
        c -> {
          store.eraseKey(c, SUBJECT);
          return null;
        });
    assertFalse(db.inWriteBoolean(c -> store.hasKey(c, SUBJECT)));
  }

  @Test
  void requiredTables() {
    assertEquals(
        Set.of(TABLE, "forgotten_subjects", "erased_key_generations"), store.requiredTables());
    JdbcSubjectKeyStore custom =
        new JdbcSubjectKeyStore(
            "custom_keys", dialect, new JdbcForgottenSubjects(dialect), () -> {});
    assertEquals(
        Set.of("custom_keys", "forgotten_subjects", "erased_key_generations"),
        custom.requiredTables());
  }

  @Test
  void loadedKeyBytesAreZeroed() throws Exception {
    VersionedKey minted = db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    List<byte[]> driverBuffers = new ArrayList<>();
    VersionedKey loaded =
        db.inWrite(c -> store.loadKeyOrNull(capturingKeyBytes(c, driverBuffers), SUBJECT));
    assertEquals(1, driverBuffers.size());
    assertArrayEquals(new byte[32], driverBuffers.getFirst());
    assertArrayEquals(minted.key().getEncoded(), loaded.key().getEncoded());
  }

  @Test
  void loadedKeyBytesAreZeroedWhenReadingTheGenerationFails() throws Exception {
    db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT));
    List<byte[]> driverBuffers = new ArrayList<>();
    SQLException boom = new SQLException("column read failed");
    assertSame(
        boom,
        assertThrows(
            SQLException.class,
            () ->
                db.inWrite(
                    c -> store.loadKeyOrNull(capturingKeyBytes(c, driverBuffers, boom), SUBJECT))));
    assertArrayEquals(new byte[32], driverBuffers.getFirst());
  }

  @Test
  void sqlExceptionPropagatesUnwrapped() throws Exception {
    SQLException boom = new SQLException("database is locked");
    dialect.insertKey =
        (conn, table, subject, bytes, version) -> {
          throw boom;
        };
    SQLException thrown =
        assertThrows(SQLException.class, () -> db.inWrite(c -> store.loadOrCreateKey(c, SUBJECT)));
    assertSame(boom, thrown);

    SQLException lockFailure = new SQLException("lock failed");
    dialect.lockFailure = lockFailure;
    assertSame(
        lockFailure,
        assertThrows(
            SQLException.class,
            () ->
                db.inWrite(
                    c -> {
                      store.eraseKey(c, SUBJECT);
                      return null;
                    })));
  }

  @Test
  void helpersNeverCommitOrRollBack() throws Exception {
    try (Connection raw = db.open()) {
      Connection guarded = SqliteCryptoTestDatabase.callerOwnedGuard(raw);
      try (var st = raw.createStatement()) {
        st.execute("BEGIN IMMEDIATE");
      }
      assertFalse(store.hasKey(guarded, SUBJECT));
      assertNotNull(store.loadOrCreateKey(guarded, SUBJECT));
      assertNotNull(store.loadKeyOrNull(guarded, SUBJECT));
      store.eraseKey(guarded, SUBJECT);
      store.liftTombstone(guarded, SUBJECT);
      // Nothing was committed: a rollback by the caller undoes every write.
      try (var st = raw.createStatement()) {
        st.execute("ROLLBACK");
      }
    }
    assertEquals(0, db.count("SELECT count(*) FROM " + TABLE));
    assertEquals(0, db.count("SELECT count(*) FROM erased_key_generations"));
    assertEquals(0, db.count("SELECT count(*) FROM forgotten_subjects"));
  }

  /**
   * The dialect calls, the race hook and the SQL prepared, in order, without the dialect's own
   * statements: those are all inserts, and the shared flow itself never inserts.
   */
  private List<String> sharedTrace() {
    return calls.stream().filter(entry -> !entry.startsWith("INSERT ")).toList();
  }

  /** A view of {@code conn} that records the SQL of every statement it prepares. */
  private static Connection recordingSql(Connection conn, List<String> sql) {
    return SqliteCryptoTestDatabase.proxy(
        Connection.class,
        conn,
        (target, method, args) -> {
          if (method.getName().equals("prepareStatement")) {
            sql.add((String) args[0]);
          }
          return SqliteCryptoTestDatabase.invoke(target, method, args);
        });
  }

  /** A view of {@code conn} whose result sets hand out, and record, the {@code key_bytes} array. */
  private static Connection capturingKeyBytes(Connection conn, List<byte[]> buffers) {
    return capturingKeyBytes(conn, buffers, null);
  }

  /**
   * As {@link #capturingKeyBytes(Connection, List)}; when {@code getIntFailure} is not {@code
   * null}, reading an int column throws it.
   */
  private static Connection capturingKeyBytes(
      Connection conn, List<byte[]> buffers, SQLException getIntFailure) {
    return SqliteCryptoTestDatabase.proxy(
        Connection.class,
        conn,
        (target, method, args) -> {
          Object result = SqliteCryptoTestDatabase.invoke(target, method, args);
          if (!method.getName().equals("prepareStatement")) {
            return result;
          }
          return SqliteCryptoTestDatabase.proxy(
              PreparedStatement.class,
              (PreparedStatement) result,
              (ps, psMethod, psArgs) -> {
                Object psResult = SqliteCryptoTestDatabase.invoke(ps, psMethod, psArgs);
                if (!psMethod.getName().equals("executeQuery")) {
                  return psResult;
                }
                return SqliteCryptoTestDatabase.proxy(
                    ResultSet.class,
                    (ResultSet) psResult,
                    (rs, rsMethod, rsArgs) -> {
                      if (getIntFailure != null && rsMethod.getName().equals("getInt")) {
                        throw getIntFailure;
                      }
                      Object value = SqliteCryptoTestDatabase.invoke(rs, rsMethod, rsArgs);
                      if (rsMethod.getName().equals("getBytes")) {
                        buffers.add((byte[]) value);
                      }
                      return value;
                    });
              });
        });
  }

  /** One {@code insertKey} behaviour of {@link RecordingDialect}. */
  @FunctionalInterface
  interface InsertKey {
    boolean insert(Connection conn, String table, String subject, byte[] bytes, int version)
        throws SQLException;
  }

  /**
   * {@link SqliteTestDialect} recording the dialect methods the flow calls, with a replaceable
   * {@code insertKey} and an optional {@code lockSubject} failure.
   */
  static final class RecordingDialect extends SqliteTestDialect {
    private final List<String> calls;
    InsertKey insertKey = this::realInsertKey;
    SQLException lockFailure;

    RecordingDialect(Clock clock, List<String> calls) {
      super(clock);
      this.calls = calls;
    }

    boolean realInsertKey(Connection conn, String table, String subject, byte[] bytes, int version)
        throws SQLException {
      return super.insertKey(conn, table, subject, bytes, version);
    }

    @Override
    public void lockSubject(Connection conn, String storedSubjectId) throws SQLException {
      calls.add("lockSubject");
      if (lockFailure != null) {
        throw lockFailure;
      }
    }

    @Override
    public boolean insertKey(
        Connection conn, String keyTable, String storedSubjectId, byte[] keyBytes, int keyVersion)
        throws SQLException {
      calls.add("insertKey");
      return insertKey.insert(conn, keyTable, storedSubjectId, keyBytes, keyVersion);
    }

    @Override
    public void recordErasedGeneration(Connection conn, String storedSubjectId, int erasedVersion)
        throws SQLException {
      calls.add("recordErasedGeneration");
      super.recordErasedGeneration(conn, storedSubjectId, erasedVersion);
    }

    @Override
    public void insertTombstone(Connection conn, String storedSubjectId) throws SQLException {
      calls.add("insertTombstone");
      super.insertTombstone(conn, storedSubjectId);
    }

    @Override
    public void insertTombstoneWithEvidence(
        Connection conn, String storedSubjectId, Instant keyCreatedAtOrNull) throws SQLException {
      calls.add("insertTombstoneWithEvidence");
      super.insertTombstoneWithEvidence(conn, storedSubjectId, keyCreatedAtOrNull);
    }

    @Override
    public void insertEraseTombstone(Connection conn, String storedSubjectId) throws SQLException {
      calls.add("insertEraseTombstone");
      super.insertEraseTombstone(conn, storedSubjectId);
    }
  }
}
