package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.integration.BackendSelection;

/**
 * {@code streamrune.backend=sqlite} with a crypto engine enabled, on a class path without {@code
 * streamrune-sqlite-crypto}: the start fails naming the module. Runs in its own test JVM ({@code
 * sqliteCryptoAbsentTest}), whose class path leaves the module out.
 */
class SpringSqliteCryptoAbsentTest {

  @TempDir Path dir;

  @Test
  void theCryptoModuleIsReallyAbsent() {
    assertThatThrownBy(() -> Class.forName(BackendSelection.SQLITE_CRYPTO_BACKEND_CLASS))
        .isInstanceOf(ClassNotFoundException.class);
  }

  @Test
  void aSqliteKeyStoreEngineWithoutTheCryptoModuleFailsNamingIt() {
    new ApplicationContextRunner()
        .withPropertyValues(
            SpringTestMocks.NO_SNAPSHOTS,
            "streamrune.backend=sqlite",
            "streamrune.crypto.sqlite.enabled=true")
        .withConfiguration(
            AutoConfigurations.of(
                StreamRuneAutoConfiguration.class,
                CryptoPropertiesConfiguration.class,
                SqliteCryptoEngineConfiguration.class))
        .withBean(DataSource.class, () -> SqliteDataSources.file(dir.resolve("app.db")))
        .run(
            ctx ->
                assertThat(ctx)
                    .getFailure()
                    .hasStackTraceContaining(
                        "streamrune.backend=sqlite with a crypto engine enabled needs the module"
                            + " org.streamrune:streamrune-sqlite-crypto on the classpath")
                    .rootCause()
                    .isInstanceOf(ClassNotFoundException.class));
  }
}
