package org.streamrune.quarkus;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;

/**
 * Test fixture that binds the {@code @ConfigMapping} interfaces through a real SmallRyeConfig, the
 * same machinery Quarkus uses at runtime. This both provides property instances for unit tests and
 * verifies the mappings bind without a Quarkus extension build step.
 */
final class TestProperties {

  private TestProperties() {}

  /** Binds {@link StreamRuneQuarkusProperties} with defaults only. */
  static StreamRuneQuarkusProperties defaults() {
    return of(Map.of());
  }

  /** Binds {@link StreamRuneQuarkusProperties} with the given {@code streamrune.*} overrides. */
  static StreamRuneQuarkusProperties of(Map<String, String> overrides) {
    return build(overrides).getConfigMapping(StreamRuneQuarkusProperties.class);
  }

  /**
   * Binds {@link StreamRuneQuarkusCryptoProperties} with the given {@code streamrune.crypto.*}
   * overrides.
   */
  static StreamRuneQuarkusCryptoProperties crypto(Map<String, String> overrides) {
    return new SmallRyeConfigBuilder()
        .withMapping(StreamRuneQuarkusCryptoProperties.class)
        .withSources(new PropertiesConfigSource(overrides, "test-overrides", 1000))
        .build()
        .getConfigMapping(StreamRuneQuarkusCryptoProperties.class);
  }

  private static SmallRyeConfig build(Map<String, String> overrides) {
    return new SmallRyeConfigBuilder()
        .withMapping(StreamRuneQuarkusProperties.class)
        .withSources(new PropertiesConfigSource(overrides, "test-overrides", 1000))
        .build();
  }
}
