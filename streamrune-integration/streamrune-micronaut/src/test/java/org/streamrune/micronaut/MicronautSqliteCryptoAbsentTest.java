package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.integration.BackendSelection;
import org.streamrune.postgres.PostgresBackend;

/**
 * {@code streamrune.backend=sqlite} with a crypto engine enabled, on a class path without {@code
 * streamrune-sqlite-crypto}: the crypto backend fails naming the module. Runs in its own test JVM
 * ({@code sqliteCryptoAbsentTest}), whose class path leaves the module out.
 */
class MicronautSqliteCryptoAbsentTest {

  @Test
  void theCryptoModuleIsReallyAbsent() {
    assertThatThrownBy(() -> Class.forName(BackendSelection.SQLITE_CRYPTO_BACKEND_CLASS))
        .isInstanceOf(ClassNotFoundException.class);
  }

  @Test
  void aSqliteKeyStoreEngineWithoutTheCryptoModuleFailsNamingIt() {
    StreamRuneBackend sqlite = BackendSelection.resolve("sqlite", PostgresBackend::new);
    assertThatThrownBy(() -> StreamRuneCryptoFactory.cryptoBackendOf(sqlite, false, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.backend=sqlite with a crypto engine enabled needs the module"
                + " org.streamrune:streamrune-sqlite-crypto on the classpath")
        .hasCauseInstanceOf(ClassNotFoundException.class);
  }
}
