package org.streamrune.core.backend;

import java.time.Clock;
import javax.sql.DataSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.tracing.StreamRuneTracing;

/**
 * What a {@link StreamRuneBackend} opens its {@link BackendStores} with: the {@code DataSource} and
 * the settings every store of the bundle shares.
 *
 * <p>The context carries only core and JDK types. It carries no serializer: each backend builds the
 * mappers its stores need from {@link #cryptoEngine()} and {@link #metrics()}, and the saga store
 * and the read-model store need different ones (a saga's state keeps its timestamps numeric, a read
 * model writes dates as ISO-8601 text). The same engine is the event store's, so the context is the
 * single source of the crypto configuration.
 *
 * @param dataSource the application's {@code DataSource}, used as given; not {@code null}
 * @param cryptoEngine the engine that encrypts {@code @Encrypted} fields in events, saga state and
 *     read models; {@code null} for none
 * @param clock the clock the stores read the time from; not {@code null}
 * @param metrics the metrics the stores record to; not {@code null} ({@link StreamRuneMetrics#NOOP}
 *     records nothing)
 * @param tracing the tracing the stores start their spans through; not {@code null} ({@link
 *     StreamRuneTracing#NOOP} traces nothing)
 * @param migrateSchema whether the event store and the other framework stores apply the shipped
 *     schema migrations ({@code streamrune.event-store.schema.auto-initialize})
 * @param migrateReadModelSchema whether the default read-model store applies its shipped schema
 *     migrations ({@code streamrune.read-model.schema.auto-initialize})
 * @param validateSchema whether the stores check the schema they find before first use
 */
public record BackendContext(
    DataSource dataSource,
    CryptoEngine cryptoEngine,
    Clock clock,
    StreamRuneMetrics metrics,
    StreamRuneTracing tracing,
    boolean migrateSchema,
    boolean migrateReadModelSchema,
    boolean validateSchema) {

  /**
   * Validates the context.
   *
   * @throws IllegalArgumentException if {@code dataSource}, {@code clock}, {@code metrics} or
   *     {@code tracing} is {@code null}; the message names the component
   */
  public BackendContext {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock is required");
    }
    if (metrics == null) {
      throw new IllegalArgumentException("metrics is required");
    }
    if (tracing == null) {
      throw new IllegalArgumentException("tracing is required");
    }
  }
}
