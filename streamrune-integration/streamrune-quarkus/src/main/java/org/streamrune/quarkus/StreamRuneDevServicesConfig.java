package org.streamrune.quarkus;

import io.quarkus.runtime.LaunchMode;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs StreamRune dev services integration status at startup in Quarkus dev mode.
 *
 * <p>When both {@code streamrune-eventstore-postgres} and {@code quarkus-jdbc-postgresql} are on
 * the classpath, Quarkus Dev Services auto-starts a PostgreSQL container. StreamRune's own Flyway
 * migrations (bundled in {@code streamrune-postgres} at {@code db/streamrune-migration/}, on the
 * {@code flyway_schema_history_streamrune} history table) are applied by {@code
 * PostgresEventStoreFactory.initializeSchema()} — NOT by {@code quarkus-flyway}, which scans the
 * application's own {@code db/migration}.
 */
@ApplicationScoped
public class StreamRuneDevServicesConfig {

  private static final Logger LOG = LoggerFactory.getLogger(StreamRuneDevServicesConfig.class);

  void onStartup(@Observes StartupEvent event) {
    if (LaunchMode.current() == LaunchMode.DEVELOPMENT) {
      LOG.info(
          "StreamRune Dev Services: PostgreSQL dev service active. "
              + "Add quarkus-flyway to auto-apply StreamRune schema migrations.");
    }
  }
}
