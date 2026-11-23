package org.streamrune.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.integration.BackendSelection;
import org.streamrune.postgres.PostgresBackend;

/**
 * The storage backend, {@value #BACKEND}: the {@link StreamRuneBackend} {@code streamrune.backend}
 * names, through {@link BackendSelection}. Imported by {@link StreamRuneAutoConfiguration}, which
 * opens its stores on the {@code DataSource}, and by {@link ReadModelStoresConfiguration}, which
 * names the default read-model store after it, so a context with only {@link ProjectionAutoConfig}
 * has it too.
 *
 * <p>The bean needs no {@code DataSource}: choosing a backend builds nothing and touches no
 * database. An unknown value, or {@code sqlite} without the module {@code
 * org.streamrune:streamrune-sqlite} on the classpath, fails the start with {@link
 * BackendSelection}'s message.
 */
@Configuration(proxyBeanMethods = false)
class BackendConfiguration {

  /** The bean name of the backend, {@value}. */
  static final String BACKEND = "streamRuneBackend";

  /**
   * The backend {@code streamrune.backend} names: {@code postgres} unless it is set.
   *
   * @param properties the bound {@code streamrune.*} properties
   * @return the backend
   */
  @Bean(BACKEND)
  StreamRuneBackend streamRuneBackend(StreamRuneProperties properties) {
    return BackendSelection.resolve(
        BackendSelection.parse(properties.backend()), PostgresBackend::new);
  }
}
