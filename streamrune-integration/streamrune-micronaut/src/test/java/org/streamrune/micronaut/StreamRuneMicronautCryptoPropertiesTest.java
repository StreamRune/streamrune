package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;

import io.micronaut.context.ApplicationContext;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The crypto configuration is immutable and is bound through the canonical constructors. */
class StreamRuneMicronautCryptoPropertiesTest {

  private static final Class<?>[] TYPES = {
    StreamRuneMicronautCryptoProperties.class,
    StreamRuneMicronautCryptoProperties.CacheConfig.class,
    StreamRuneMicronautCryptoProperties.FileSystemConfig.class,
    StreamRuneMicronautCryptoProperties.PostgresConfig.class,
    StreamRuneMicronautCryptoProperties.VaultConfig.class,
    StreamRuneMicronautCryptoProperties.AwsConfig.class
  };

  @Test
  void everyTypeIsAnImmutableRecordWithoutSetters() {
    for (Class<?> type : TYPES) {
      assertTrue(type.isRecord(), type.getSimpleName() + " must be a record");
      for (Method m : type.getMethods()) {
        assertFalse(
            m.getName().startsWith("set"),
            type.getSimpleName() + " must not expose the setter " + m.getName());
      }
    }
  }

  @Test
  void defaultsAreBoundWhenNothingIsConfigured() {
    try (var ctx = ApplicationContext.run()) {
      var props = ctx.getBean(StreamRuneMicronautCryptoProperties.class);
      assertTrue(props.cache().enabled());
      assertEquals(10_000, props.cache().maximumSize());
      assertEquals(Duration.ofMinutes(5), props.cache().expireAfterWrite());
      assertNull(props.cache().expireAfterAccess());
      assertEquals("/var/lib/streamrune/keys", props.filesystem().keyDirectory());
      assertEquals("encryption_keys", props.postgres().tableName());
      assertEquals("http://localhost:8200", props.vault().address());
      assertEquals("transit", props.vault().engineMount());
      assertNull(props.vault().token());
      assertNull(props.aws().region());
      assertNull(props.aws().kmsKeyId());
    }
  }

  @Test
  void everyKeyBindsThroughTheConstructors() {
    try (var ctx =
        ApplicationContext.run(
            Map.ofEntries(
                Map.entry("streamrune.crypto.cache.enabled", "false"),
                Map.entry("streamrune.crypto.cache.maximum-size", "500"),
                Map.entry("streamrune.crypto.cache.expire-after-write", "1h"),
                Map.entry("streamrune.crypto.cache.expire-after-access", "30m"),
                Map.entry("streamrune.crypto.filesystem.key-directory", "/custom/keys"),
                Map.entry("streamrune.crypto.postgres.table-name", "custom_keys"),
                Map.entry("streamrune.crypto.vault.address", "http://vault:8200"),
                Map.entry("streamrune.crypto.vault.token", "s.test"),
                Map.entry("streamrune.crypto.vault.engine-mount", "custom-transit"),
                Map.entry("streamrune.crypto.aws.region", "us-west-2"),
                Map.entry("streamrune.crypto.aws.kms-key-id", "key-123")))) {
      var props = ctx.getBean(StreamRuneMicronautCryptoProperties.class);
      assertFalse(props.cache().enabled());
      assertEquals(500, props.cache().maximumSize());
      assertEquals(Duration.ofHours(1), props.cache().expireAfterWrite());
      assertEquals(Duration.ofMinutes(30), props.cache().expireAfterAccess());
      assertEquals("/custom/keys", props.filesystem().keyDirectory());
      assertEquals("custom_keys", props.postgres().tableName());
      assertEquals("http://vault:8200", props.vault().address());
      assertEquals("s.test", props.vault().token());
      assertEquals("custom-transit", props.vault().engineMount());
      assertEquals("us-west-2", props.aws().region());
      assertEquals("key-123", props.aws().kmsKeyId());
    }
  }
}
