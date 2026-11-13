package org.streamrune.quarkus;

import org.eclipse.microprofile.config.spi.Converter;
import org.streamrune.core.Internal;
import org.streamrune.integration.ConfigurationSpelling;

/**
 * Converts a {@code streamrune.*} switch: {@code true} or {@code false}, in lower case. SmallRye
 * would read any other spelling as {@code false} ({@code TRUE}, {@code 1}, {@code yes} and {@code
 * on} as {@code true}); it is refused instead, so that one value means one thing on the three
 * integrations ({@link ConfigurationSpelling}). The boolean properties of {@link
 * StreamRuneQuarkusProperties} and {@link StreamRuneQuarkusCryptoProperties} name it in {@code
 * WithConverter}.
 *
 * @hidden
 */
@Internal
public final class StrictBooleanConverter implements Converter<Boolean> {

  /** Creates the converter; SmallRye instantiates it for each property that names it. */
  public StrictBooleanConverter() {}

  @Override
  public Boolean convert(String value) {
    if (!ConfigurationSpelling.isSwitch(value)) {
      throw new IllegalArgumentException(ConfigurationSpelling.SWITCH_HINT + ", got: " + value);
    }
    return Boolean.valueOf(value);
  }
}
