package org.streamrune.core.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import javax.sql.DataSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.tracing.StreamRuneTracing;

/**
 * What a {@link StreamRuneBackend} opens its {@link BackendStores} with: the {@code DataSource} and
 * the settings every store of the bundle shares.
 *
 * @param dataSource the application's {@code DataSource}, used as given; not {@code null}
 * @param objectMapper the crypto-aware mapper the read-model and saga stores serialize with; not
 *     {@code null}
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
    ObjectMapper objectMapper,
    Clock clock,
    StreamRuneMetrics metrics,
    StreamRuneTracing tracing,
    boolean migrateSchema,
    boolean migrateReadModelSchema,
    boolean validateSchema) {

  /**
   * Validates the context.
   *
   * @throws IllegalArgumentException if {@code dataSource}, {@code objectMapper}, {@code clock},
   *     {@code metrics} or {@code tracing} is {@code null}; the message names the component
   */
  public BackendContext {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (objectMapper == null) {
      throw new IllegalArgumentException("objectMapper is required");
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
