package org.streamrune.crypto.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoBackend.KeyStoreEngineSettings;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * {@link SqliteCryptoBackend} builds the SQLite crypto classes over the caller's data source. Each
 * call checks the data source with a handle it closes before it returns; the products work over the
 * data source afterwards and touch no table when built.
 */
class SqliteCryptoBackendTest {

  @TempDir Path dir;

  private final CryptoBackend backend = new SqliteCryptoBackend();
  private DataSource dataSource;
  private SqliteDatabase storeHalf;

  @BeforeEach
  void open() {
    dataSource = SqliteFiles.dataSource(dir.resolve("backend.db"));
    // The store half's handle, which holds the instance guard for the whole run.
    storeHalf = SqliteDatabase.open(dataSource);
  }

  @AfterEach
  void close() {
    storeHalf.close();
  }

  @Test
  void theBackendIsNamedSqlite() {
    assertThat(backend.name()).isEqualTo("sqlite");
  }

  @Test
  void theForgottenSubjectStoreIsTheSqliteStoreAndWorksAfterTheCall() {
    storeHalf.migrate(org.streamrune.sqlite.SqliteSchemaSeries.CRYPTO);
    ForgottenSubjectStore store = backend.forgottenSubjects(dataSource);

    assertThat(store).isExactlyInstanceOf(SqliteForgottenSubjectStore.class);
    assertThat(backend.forgottenSubjects(dataSource)).isNotSameAs(store);
    SubjectId subject = SubjectId.of("backend-store");
    store.forget(subject);
    assertThat(store.isForgotten(subject)).isTrue();
  }

  @Test
  void theForgetSignalIsTheInProcessSignal_andTheFactoryIsNeverCalled() {
    var calls = new AtomicInteger();
    Supplier<DataSource> factory =
        () -> {
          calls.incrementAndGet();
          return dataSource;
        };
    try (CryptoForgetSignal signal = backend.forgetSignal(dataSource);
        CryptoForgetSignal other = backend.forgetSignal(dataSource, factory)) {
      assertThat(signal).isExactlyInstanceOf(InProcessCryptoForgetSignal.class);
      assertThat(other).isExactlyInstanceOf(InProcessCryptoForgetSignal.class).isNotSameAs(signal);
      var heard = new java.util.ArrayList<String>();
      other.subscribe(heard::add);
      signal.publish("token");
      assertThat(heard).as("both signals carry the file's forgets").containsExactly("token");
    }
    assertThat(calls).hasValue(0);
  }

  @Test
  void theKeyStoreEngineIsASqliteEngineWithTheSettings_andTouchesNoTable() throws Exception {
    StreamRuneMetrics metrics = new StreamRuneMetrics() {};
    CryptoEngine engine =
        backend.keyStoreEngine(dataSource, new KeyStoreEngineSettings("CryptoKeys", metrics));

    assertThat(engine).isExactlyInstanceOf(SqliteCryptoEngine.class);
    assertThat(engine.requiredCryptoTables())
        .isEqualTo(Set.of("cryptokeys", "forgotten_subjects", "erased_key_generations"));
    assertThat(field(engine, "metrics")).isSameAs(metrics);
    assertThat(SqliteFiles.query(dataSource, "SELECT name FROM sqlite_schema")).isEmpty();
    assertThat(
            backend.keyStoreEngine(dataSource, new KeyStoreEngineSettings("CryptoKeys", metrics)))
        .isNotSameAs(engine);
  }

  @Test
  void theKeyStoreEngineWorksOverTheDataSourceAfterTheCall() {
    storeHalf.migrate(org.streamrune.sqlite.SqliteSchemaSeries.CRYPTO);
    CryptoEngine engine =
        backend.keyStoreEngine(
            dataSource, new KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));
    SubjectId subject = SubjectId.of("backend-engine");
    byte[] plaintext = "pii".getBytes(StandardCharsets.UTF_8);

    assertThat(engine.decrypt(subject, engine.encrypt(subject, plaintext))).isEqualTo(plaintext);
  }

  @Test
  void aNullTableNameMeansTheDefaultKeyTable() {
    CryptoEngine engine =
        backend.keyStoreEngine(
            dataSource, new KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));
    assertThat(engine.requiredCryptoTables()).contains(SqliteCryptoEngine.DEFAULT_TABLE_NAME);
  }

  @Test
  void anInvalidTableNameIsRefusedByItsValueAlone_namingNoPropertyOfAnotherBackend() {
    assertThatThrownBy(
            () ->
                backend.keyStoreEngine(
                    dataSource, new KeyStoreEngineSettings("main.keys", StreamRuneMetrics.NOOP)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'main.keys'")
        .hasMessageNotContaining("streamrune.crypto.postgres")
        .hasMessageNotContaining("table-name");
  }

  /** Each call opens a handle to check the data source: a wrong setting is refused there. */
  @Test
  void aDataSourceWithAWrongSettingIsRefusedByEveryCall() {
    DataSource wrong = SqliteFiles.dataSource(dir.resolve("backend.db"), "synchronous=NORMAL");
    var settings = new KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP);

    assertThatThrownBy(() -> backend.forgottenSubjects(wrong))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("synchronous");
    assertThatThrownBy(() -> backend.forgetSignal(wrong))
        .isExactlyInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> backend.forgetSignal(wrong, () -> wrong))
        .isExactlyInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> backend.keyStoreEngine(wrong, settings))
        .isExactlyInstanceOf(IllegalStateException.class);
  }

  /**
   * The handle of a call is closed before it returns: the guard of the file keeps exactly the
   * reference of the store half's handle, which holds it for the whole run.
   */
  @Test
  void eachCallClosesTheHandleItOpened() throws Exception {
    assertThat(guardReferences()).isEqualTo(1);

    backend.forgottenSubjects(dataSource);
    backend.forgetSignal(dataSource).close();
    backend.forgetSignal(dataSource, () -> dataSource).close();
    backend.keyStoreEngine(dataSource, new KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));
    assertThatThrownBy(
            () ->
                backend.keyStoreEngine(
                    dataSource, new KeyStoreEngineSettings("bad name", StreamRuneMetrics.NOOP)))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    assertThat(guardReferences()).isEqualTo(1);
  }

  /** The instance guard's reference count for the database file, read from streamrune-sqlite. */
  private int guardReferences() throws Exception {
    Method references =
        Class.forName("org.streamrune.sqlite.InstanceGuardRegistry")
            .getDeclaredMethod("references", Path.class);
    references.setAccessible(true);
    Path lockFile = Path.of(storeHalf.file() + ".lock").toRealPath();
    return (Integer) references.invoke(null, lockFile);
  }

  @Test
  void nullArgumentsAreIllegalArguments() {
    var settings = new KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP);
    assertRefused(() -> backend.forgottenSubjects(null), "dataSource is required");
    assertRefused(() -> backend.forgetSignal(null), "dataSource is required");
    assertRefused(() -> backend.forgetSignal(null, () -> dataSource), "dataSource is required");
    assertRefused(
        () -> backend.forgetSignal(dataSource, null), "listenDataSourceFactory is required");
    assertRefused(() -> backend.keyStoreEngine(null, settings), "dataSource is required");
    assertRefused(() -> backend.keyStoreEngine(dataSource, null), "settings is required");
  }

  private static void assertRefused(Runnable call, String message) {
    assertThatThrownBy(call::run)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message);
  }

  private static Object field(Object target, String name) throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
