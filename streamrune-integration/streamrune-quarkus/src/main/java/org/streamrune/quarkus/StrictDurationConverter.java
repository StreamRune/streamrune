package org.streamrune.quarkus;

import io.quarkus.runtime.configuration.DurationConverter;
import java.time.Duration;
import org.eclipse.microprofile.config.spi.Converter;
import org.streamrune.core.Internal;
import org.streamrune.integration.ConfigurationSpelling;

/**
 * Converts a {@code streamrune.*} duration as Quarkus does ({@code 30s}, {@code 500ms}, {@code 7d},
 * ISO-8601), except a number without a unit, which Quarkus would read as seconds: it is refused, so
 * that one value means one thing on the three integrations ({@link ConfigurationSpelling}). The
 * duration properties of {@link StreamRuneQuarkusProperties} and {@link
 * StreamRuneQuarkusCryptoProperties} name it in {@code WithConverter}.
 *
 * @hidden
 */
@Internal
public final class StrictDurationConverter implements Converter<Duration> {

  private static final DurationConverter QUARKUS = new DurationConverter();

  /** Creates the converter; SmallRye instantiates it for each property that names it. */
  public StrictDurationConverter() {}

  @Override
  public Duration convert(String value) {
    if (ConfigurationSpelling.isBareNumber(value)) {
      throw new IllegalArgumentException(ConfigurationSpelling.DURATION_HINT + ", got: " + value);
    }
    return QUARKUS.convert(value);
  }
}
