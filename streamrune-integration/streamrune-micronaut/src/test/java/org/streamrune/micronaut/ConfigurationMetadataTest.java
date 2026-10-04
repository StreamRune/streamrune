package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import org.junit.jupiter.api.Test;

class ConfigurationMetadataTest {

  @Test
  void configurationMetadataExists() {
    InputStream is =
        getClass().getClassLoader().getResourceAsStream("META-INF/configuration-metadata.json");
    assertThat(is).as("META-INF/configuration-metadata.json must be on classpath").isNotNull();
  }

  @Test
  void configurationMetadataContainsStreamRuneProperties() throws Exception {
    InputStream is =
        getClass().getClassLoader().getResourceAsStream("META-INF/configuration-metadata.json");
    String content = new String(is.readAllBytes());
    // One varargs contains reports every missing key in a single failure.
    assertThat(content)
        .contains(
            "streamrune.snapshot-every-n-events",
            "streamrune.saga.enabled",
            "streamrune.outbox.enabled",
            "streamrune.dead-letter.enabled",
            "streamrune.metrics.enabled",
            "streamrune.validation-enabled",
            "streamrune.startup-log");
  }

  /**
   * The command dead-letter queue's retention window is the one switch that turns its pruning off,
   * and {@code streamrune.dead-letter.enabled} governs only the automatic retry runner — both facts
   * must be visible to IDE completion, not just in the guide.
   */
  @Test
  void deadLetterRetentionKnobIsDeclaredAndTheEnabledSwitchIsDescribedAsRetryOnly()
      throws Exception {
    InputStream is =
        getClass().getClassLoader().getResourceAsStream("META-INF/configuration-metadata.json");
    String content = new String(is.readAllBytes());
    assertThat(content)
        .contains("\"streamrune.dead-letter.retention-max-age\"")
        .contains("Enable the automatic dead letter retry runner");
  }

  /**
   * The two knobs backed by {@link StreamRuneMicronautProperties} fields ({@code
   * outboxSkippedRetentionMaxAge}, {@code subscriptionSingleActiveConsumerEnabled}) must be
   * declared so IDE completion and Micronaut config validation see them. Names and defaults match
   * the Spring/Quarkus definitions. The outbox's FAILED window was renamed to the SKIPPED window
   * (FAILED entries are never pruned): the old key no longer binds, so the SKIPPED key must be the
   * only outbox retention key advertised to IDE completion, not one beside the old.
   */
  @Test
  void configurationMetadataContainsSkippedRetentionAndSingleActiveConsumerKnobs()
      throws Exception {
    InputStream is =
        getClass().getClassLoader().getResourceAsStream("META-INF/configuration-metadata.json");
    String content = new String(is.readAllBytes());
    assertThat(content)
        .contains(
            "streamrune.outbox.skipped-retention-max-age",
            "streamrune.subscription.single-active-consumer.enabled");
    assertThat(
            java.util.regex.Pattern.compile("\"streamrune\\.outbox\\.[a-z-]+-retention-max-age\"")
                .matcher(content)
                .results()
                .map(java.util.regex.MatchResult::group)
                .toList())
        .as("the renamed knob replaces the FAILED window's entry; it is not kept beside it")
        .containsExactly("\"streamrune.outbox.skipped-retention-max-age\"");
    assertThat(content).doesNotContain("streamrune.outbox.failed-retention-max-age");
  }

  @Test
  void configurationMetadataDeclaresTheSubscriptionHealthLagThreshold() throws Exception {
    InputStream is =
        getClass().getClassLoader().getResourceAsStream("META-INF/configuration-metadata.json");
    String content = new String(is.readAllBytes());
    assertThat(content).contains("\"streamrune.subscription.health.lag-threshold\"");
  }

  /**
   * The trusted-gateway flag decides whether {@code X-User-Id} is an identity at all, so it must be
   * declared like on Spring (configuration metadata) and Quarkus ({@code @ConfigMapping}).
   */
  @Test
  void configurationMetadataDeclaresTheTrustUserIdHeaderFlag() throws Exception {
    InputStream is =
        getClass().getClassLoader().getResourceAsStream("META-INF/configuration-metadata.json");
    String content = new String(is.readAllBytes());
    assertThat(content).contains("\"streamrune.security.trust-user-id-header\"");
  }

  /**
   * The crypto configuration is a record tree bound through its canonical constructors; every
   * component (and the per-backend {@code enabled} switches that gate the factory beans) must be
   * declared so IDE completion and Micronaut config validation see them.
   */
  @Test
  void configurationMetadataDeclaresEveryCryptoKey() throws Exception {
    InputStream is =
        getClass().getClassLoader().getResourceAsStream("META-INF/configuration-metadata.json");
    String content = new String(is.readAllBytes());
    var expected = new java.util.ArrayList<String>();
    for (var group :
        java.util.List.of(
            "cache:" + StreamRuneMicronautCryptoProperties.CacheConfig.class.getName(),
            "filesystem:" + StreamRuneMicronautCryptoProperties.FileSystemConfig.class.getName(),
            "postgres:" + StreamRuneMicronautCryptoProperties.PostgresConfig.class.getName(),
            "vault:" + StreamRuneMicronautCryptoProperties.VaultConfig.class.getName(),
            "aws:" + StreamRuneMicronautCryptoProperties.AwsConfig.class.getName())) {
      String prefix = "streamrune.crypto." + group.substring(0, group.indexOf(':')) + ".";
      Class<?> type = Class.forName(group.substring(group.indexOf(':') + 1));
      for (var component : type.getRecordComponents()) {
        expected.add(
            "\"" + prefix + component.getName().replaceAll("([A-Z])", "-$1").toLowerCase() + "\"");
      }
      expected.add("\"" + prefix.substring(0, prefix.length() - 1) + "\"");
    }
    expected.addAll(
        java.util.List.of(
            "\"streamrune.crypto\"",
            "\"streamrune.crypto.filesystem.enabled\"",
            "\"streamrune.crypto.postgres.enabled\"",
            "\"streamrune.crypto.vault.enabled\"",
            "\"streamrune.crypto.aws.enabled\""));
    assertThat(content).contains(expected.toArray(String[]::new));
  }
}
