package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoBackend.KeyStoreEngineSettings;
import org.streamrune.crypto.CryptoForgetSignal;

/**
 * {@link PostgresCryptoBackend} builds the Postgres crypto classes the integrations build today.
 * None of them touches the database when built, so a driver DataSource pointing nowhere suffices.
 */
class PostgresCryptoBackendTest {

  private static final DataSource DATA_SOURCE = new PGSimpleDataSource();

  private final CryptoBackend backend = new PostgresCryptoBackend();

  @Test
  void theBackendIsNamedPostgres() {
    assertEquals("postgres", backend.name());
  }

  @Test
  void theForgottenSubjectStoreIsTheJdbcStore() {
    var store = backend.forgottenSubjects(DATA_SOURCE);
    assertEquals(JdbcForgottenSubjectStore.class, store.getClass());
    assertNotSame(store, backend.forgottenSubjects(DATA_SOURCE));
  }

  @Test
  void theForgetSignalIsThePostgresSignal() throws Exception {
    try (CryptoForgetSignal signal = backend.forgetSignal(DATA_SOURCE);
        CryptoForgetSignal other = backend.forgetSignal(DATA_SOURCE)) {
      assertEquals(PostgresCryptoForgetSignal.class, signal.getClass());
      assertNotSame(signal, other);
    }
  }

  @Test
  void theTwoArgumentForgetSignalListensOnTheGivenFactory() throws Exception {
    Supplier<DataSource> factory = () -> DATA_SOURCE;
    try (CryptoForgetSignal signal = backend.forgetSignal(DATA_SOURCE, factory)) {
      assertEquals(PostgresCryptoForgetSignal.class, signal.getClass());
      assertSame(factory, field(signal, "listenDataSourceFactory"));
    }
  }

  @Test
  void theKeyStoreEngineIsABuiltPostgresCryptoEngineWithTheSettings() throws Exception {
    StreamRuneMetrics metrics = new StreamRuneMetrics() {};
    CryptoEngine engine =
        backend.keyStoreEngine(DATA_SOURCE, new KeyStoreEngineSettings("CryptoKeys", metrics));

    assertEquals(PostgresCryptoEngine.class, engine.getClass());
    // The engine folds the name to lower case, as PostgreSQL does with the unquoted identifier.
    assertEquals(
        Set.of("cryptokeys", "forgotten_subjects", "erased_key_generations"),
        engine.requiredCryptoTables());
    // The engine hands its metrics to its VerifiedCipher, which records both crypto meters.
    assertSame(metrics, field(field(engine, "verified"), "metrics"));
    assertNotSame(
        engine,
        backend.keyStoreEngine(DATA_SOURCE, new KeyStoreEngineSettings("CryptoKeys", metrics)));
  }

  @Test
  void aNullTableNameMeansTheDefaultKeyTable() {
    CryptoEngine engine =
        backend.keyStoreEngine(
            DATA_SOURCE, new KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));
    assertTrue(engine.requiredCryptoTables().contains(PostgresCryptoEngine.DEFAULT_TABLE_NAME));
  }

  @Test
  void anInvalidTableNameIsRefusedNamingThePostgresProperty() {
    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                backend.keyStoreEngine(
                    DATA_SOURCE,
                    new KeyStoreEngineSettings("public.keys", StreamRuneMetrics.NOOP)));
    assertTrue(refused.getMessage().contains("streamrune.crypto.postgres.table-name"));
  }

  @Test
  void nullArgumentsAreIllegalArguments() {
    var settings = new KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP);
    assertRefused(() -> backend.forgottenSubjects(null), "dataSource is required");
    assertRefused(() -> backend.forgetSignal(null), "dataSource is required");
    assertRefused(() -> backend.forgetSignal(null, () -> DATA_SOURCE), "dataSource is required");
    assertRefused(
        () -> backend.forgetSignal(DATA_SOURCE, null), "listenDataSourceFactory is required");
    assertRefused(() -> backend.keyStoreEngine(null, settings), "dataSource is required");
    assertRefused(() -> backend.keyStoreEngine(DATA_SOURCE, null), "settings is required");
  }

  private static void assertRefused(Runnable call, String message) {
    var refused = assertThrows(IllegalArgumentException.class, call::run);
    assertEquals(IllegalArgumentException.class, refused.getClass());
    assertEquals(message, refused.getMessage());
  }

  private static Object field(Object target, String name) throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
