package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;

class CryptoBackendTest {

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

  /**
   * Compile-time shape check of ruling P1, never run: an integration configures every crypto part
   * through {@link CryptoBackend} holding only crypto-api, core and JDK types, with no cast and no
   * backend type, including the forget signal with a dedicated LISTEN pool factory (Quarkus with
   * Agroal). If this stops compiling, the SPI shape the integrations rely on changed.
   */
  @SuppressWarnings("unused")
  private static CachedCryptoEngine integrationShape(
      CryptoBackend backend, DataSource dataSource, DataSource listenPool) {
    ForgottenSubjectStore store = backend.forgottenSubjects(dataSource);
    CryptoForgetSignal derived = backend.forgetSignal(dataSource);
    CryptoForgetSignal signal = backend.forgetSignal(dataSource, () -> listenPool);
    CryptoEngine engine =
        backend.keyStoreEngine(
            dataSource, new CryptoBackend.KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));
    return CachedCryptoEngine.builder().delegate(engine).forgetSignal(signal).build();
  }
}
