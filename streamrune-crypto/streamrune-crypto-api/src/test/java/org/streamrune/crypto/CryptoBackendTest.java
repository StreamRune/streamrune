package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;

class CryptoBackendTest {

  private static final DataSource DATA_SOURCE = stub(DataSource.class);
  private static final CryptoEngine ENGINE = stub(CryptoEngine.class);

  @SuppressWarnings("unchecked")
  private static <T> T stub(Class<T> type) {
    return (T)
        Proxy.newProxyInstance(
            CryptoBackendTest.class.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "equals" -> proxy == args[0];
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "toString" -> type.getSimpleName() + "Stub";
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  @Test
  void settingsKeepTheTableNameAndTheMetrics() {
    var settings = new CryptoBackend.KeyStoreEngineSettings("custom_keys", StreamRuneMetrics.NOOP);

    assertEquals("custom_keys", settings.tableName());
    assertSame(StreamRuneMetrics.NOOP, settings.metrics());
  }

  @Test
  void settingsPassTheTableNameOnUnchangedForTheBackendToValidate() {
    // Identifier rules (characters, case folding) differ per database: the backend applies them.
    var settings =
        new CryptoBackend.KeyStoreEngineSettings("Not A Valid; Name", StreamRuneMetrics.NOOP);

    assertEquals("Not A Valid; Name", settings.tableName());
  }

  @Test
  void aNullTableNameMeansTheBackendDefault() {
    var settings = new CryptoBackend.KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP);

    assertNull(settings.tableName());
  }

  @Test
  void aBlankTableNameMeansTheBackendDefaultToo() {
    assertNull(new CryptoBackend.KeyStoreEngineSettings("", StreamRuneMetrics.NOOP).tableName());
    assertNull(new CryptoBackend.KeyStoreEngineSettings(" \t", StreamRuneMetrics.NOOP).tableName());
  }

  @Test
  void settingsRequireMetrics() {
    var thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> new CryptoBackend.KeyStoreEngineSettings("custom_keys", null));
    assertEquals(IllegalArgumentException.class, thrown.getClass());
    assertEquals("metrics is required (pass StreamRuneMetrics.NOOP for none)", thrown.getMessage());
  }

  @Test
  void anIntegrationConfiguresTheKeyStoreEngineWithoutNamingABackendType() {
    // Compile-time pin of ruling P1: the caller holds only crypto-api and core types, and needs no
    // cast to get a CryptoEngine it can wrap in a CachedCryptoEngine.
    var recorded = new ArrayList<Object>();
    CryptoBackend backend = new RecordingCryptoBackend(recorded);
    var settings = new CryptoBackend.KeyStoreEngineSettings("custom_keys", StreamRuneMetrics.NOOP);

    CryptoEngine engine = backend.keyStoreEngine(DATA_SOURCE, settings);

    assertSame(ENGINE, engine);
    assertEquals(List.of(DATA_SOURCE, settings), recorded);
  }

  /** A backend that records what it is given; the shape a real backend implements. */
  private record RecordingCryptoBackend(List<Object> recorded) implements CryptoBackend {

    @Override
    public String name() {
      return "recording";
    }

    @Override
    public ForgottenSubjectStore forgottenSubjects(DataSource dataSource) {
      return new InMemoryForgottenSubjectStore();
    }

    @Override
    public CryptoForgetSignal forgetSignal(DataSource dataSource) {
      return CryptoForgetSignal.NOOP;
    }

    @Override
    public CryptoForgetSignal forgetSignal(
        DataSource dataSource, Supplier<? extends DataSource> listenDataSourceFactory) {
      return CryptoForgetSignal.NOOP;
    }

    @Override
    public CryptoEngine keyStoreEngine(DataSource dataSource, KeyStoreEngineSettings settings) {
      recorded.add(dataSource);
      recorded.add(settings);
      return ENGINE;
    }
  }
}
