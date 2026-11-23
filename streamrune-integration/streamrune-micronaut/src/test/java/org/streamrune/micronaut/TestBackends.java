package org.streamrune.micronaut;

import java.time.Clock;
import javax.sql.DataSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.postgres.PostgresBackend;

/**
 * The backend the factory methods take, for tests that call a factory method directly: the Postgres
 * backend's stores opened the way {@link StreamRuneMicronautModule#streamRuneBackendStores} opens
 * them by default, and its crypto backend.
 */
final class TestBackends {

  private TestBackends() {}

  /** The Postgres stores on {@code dataSource}, without a crypto engine. */
  static BackendStores stores(DataSource dataSource) {
    return stores(dataSource, null);
  }

  /** The Postgres stores on {@code dataSource}, with {@code cryptoEngine} (or none). */
  static BackendStores stores(DataSource dataSource, CryptoEngine cryptoEngine) {
    return new PostgresBackend()
        .open(
            new BackendContext(
                dataSource,
                cryptoEngine,
                Clock.systemUTC(),
                StreamRuneMetrics.NOOP,
                StreamRuneTracing.NOOP,
                true,
                true,
                true));
  }
}
