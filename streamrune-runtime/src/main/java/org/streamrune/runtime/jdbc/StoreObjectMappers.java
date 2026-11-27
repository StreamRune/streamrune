package org.streamrune.runtime.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Objects;
import org.streamrune.core.Internal;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CryptoShreddingModule;

/**
 * The two Jackson mappers the stores build: one for events, snapshots and saga state, one for read
 * models. Both register the Java time module and, given a crypto engine, the {@link
 * CryptoShreddingModule} with the metrics collector, so the crypto-redaction signals fire when an
 * {@code @Encrypted} field decrypts to {@code [REDACTED]} because a key is gone. The stores' public
 * {@code createObjectMapper} methods delegate here.
 *
 * <p>Every call returns a new mapper, independent from any other mapper instance.
 *
 * @hidden
 */
@Internal
public final class StoreObjectMappers {

  private StoreObjectMappers() {}

  /**
   * The mapper of events, snapshots and saga state: dates are written as timestamps (Jackson's
   * default).
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} for a crypto-blind mapper
   * @param metrics the crypto-redaction metrics collector; {@link StreamRuneMetrics#NOOP} for none,
   *     not {@code null}
   * @return a new {@link ObjectMapper}
   * @throws NullPointerException if {@code metrics} is {@code null}
   */
  public static ObjectMapper eventsAndSagas(CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics is required");
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    if (cryptoEngine != null) {
      mapper.registerModule(new CryptoShreddingModule(cryptoEngine, metrics));
    }
    return mapper;
  }

  /**
   * The mapper of read models: dates are written as ISO-8601 text ({@link
   * SerializationFeature#WRITE_DATES_AS_TIMESTAMPS} disabled before the crypto module is
   * registered).
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} for a crypto-blind mapper
   * @param metrics the crypto-redaction metrics collector; {@link StreamRuneMetrics#NOOP} for none,
   *     not {@code null}
   * @return a new {@link ObjectMapper}
   * @throws NullPointerException if {@code metrics} is {@code null}
   */
  public static ObjectMapper readModels(CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics is required");
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    if (cryptoEngine != null) {
      mapper.registerModule(new CryptoShreddingModule(cryptoEngine, metrics));
    }
    return mapper;
  }
}
