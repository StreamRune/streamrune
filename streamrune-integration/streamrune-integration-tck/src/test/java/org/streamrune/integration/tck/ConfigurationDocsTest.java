package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.metrics.MetricNames;

class ConfigurationDocsTest {

  private static final Map<String, String> ORACLE =
      new TreeMap<>(
          Map.of(
              "streamrune.outbox.enabled", "false",
              "streamrune.outbox.retention-max-age", "7d",
              "streamrune.inbox.retention-max-age", "7d",
              "streamrune.retry-max-attempts", "3",
              "streamrune.projections.<name>.enabled", "true"));

  @Test
  void aKeyIsNamedInProseInAPropertiesListingOrNestedInYaml() {
    var docs =
        ConfigurationDocs.of(
            Map.of(
                "guide.md",
                String.join(
                    "\n",
                    "Set `streamrune.outbox.enabled` to switch it on.",
                    "```properties",
                    "streamrune.retry-max-attempts=3",
                    "```",
                    "```yaml",
                    "spring:",
                    "  outbox:",
                    "    retention-max-age: 1d",
                    "streamrune:",
                    "  # a comment",
                    "  outbox:",
                    "    retention-max-age: 7d",
                    "    tags:",
                    "      - a",
                    "  inbox:",
                    "    retention-max-age: 7d",
                    "```",
                    "Per projection: `streamrune.projections.<name>.enabled`.")));

    assertEquals(List.of(), docs.undocumented(ORACLE));
  }

  @Test
  void anUnnamedKeyIsReportedWithItsDefault() {
    var docs = ConfigurationDocs.of(Map.of("guide.md", "Only `streamrune.outbox.enabled`."));

    List<String> missing = docs.undocumented(ORACLE);

    assertTrue(
        missing.contains("streamrune.inbox.retention-max-age (default 7d)"), missing.toString());
    assertEquals(4, missing.size(), missing.toString());
  }

  @Test
  void aNameThatIsNoKeyMeterSpanAttributeOrNamespaceIsUnknown() {
    var docs =
        ConfigurationDocs.of(
            Map.of(
                "guide.md",
                String.join(
                    "\n",
                    "The meter `" + MetricNames.COMMANDS_DISPATCHED + "` counts.",
                    "The span carries `streamrune.command.type`.",
                    "Keys under `streamrune.outbox` and `streamrune.outbox.*` exist.",
                    "Logging `org.streamrune.core` and `-Dstreamrune.logsink.x` are not names.",
                    "The old `streamrune.outbox.renamed-key` is gone.",
                    "A wildcard `streamrune.nothing.*` names nothing.",
                    "A key `streamrune.outbox.enabled` exists.")));

    assertEquals(
        List.of("guide.md:5: streamrune.outbox.renamed-key", "guide.md:6: streamrune.nothing.*"),
        docs.unknown(ORACLE));
  }

  @Test
  void aDefaultColumnMustStateTheCanonicalDefault() {
    var docs =
        ConfigurationDocs.of(
            Map.of(
                "guide.md",
                String.join(
                    "\n",
                    "| Property | Default | Notes |",
                    "|---|---|---|",
                    "| `streamrune.outbox.retention-max-age` | **`7d`** | fine |",
                    "| `streamrune.inbox.retention-max-age` | 7 days | stated in words |",
                    "",
                    "| Framework | Property | Default |",
                    "|---|---|---|",
                    "| Quarkus | `streamrune.inbox.retention-max-age` (via `inbox()`) | `PT168H` |",
                    "",
                    "| Setting | Spring | Quarkus | Micronaut | Default |",
                    "|---|---|---|---|---|",
                    "| Retries | `streamrune.retry-max-attempts=3` | `streamrune.retry-max-attempts=3`"
                        + " | `streamrune.retry-max-attempts: 3` | 3 |",
                    "",
                    "| Builder method | Default | Notes |",
                    "|---|---|---|",
                    "| `streamrune.outbox.enabled` | 30 | not a key column |",
                    "",
                    "| Property | Meaning |",
                    "|---|---|",
                    "| `streamrune.outbox.enabled` | no default column |",
                    "",
                    "| Property | Default |",
                    "|---|---|",
                    "| `streamrune.not-a-key` | 1 |",
                    "| short row |")));

    assertEquals(
        List.of(
            "guide.md:4: streamrune.inbox.retention-max-age: the table says '7 days', the code"
                + " default is '7d'",
            "guide.md:8: streamrune.inbox.retention-max-age: the table says 'PT168H', the code"
                + " default is '7d'"),
        docs.wrongDefaults(ORACLE));
    assertEquals(5, docs.defaultRowCount());
  }

  @Test
  void metadataNamesTheKeysAndAPlaceholderKeyIsExempt() {
    String json =
        "{\"groups\":[],\"properties\":[{\"name\":\"streamrune.outbox.enabled\"},"
            + "{\"name\":\"streamrune.retry-max-attempts\"}]}";

    Set<String> declared =
        ConfigurationDocs.metadataKeys(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    assertEquals(Set.of("streamrune.outbox.enabled", "streamrune.retry-max-attempts"), declared);
    assertEquals(
        List.of(
            "streamrune.inbox.retention-max-age (default 7d)",
            "streamrune.outbox.retention-max-age (default 7d)"),
        ConfigurationDocs.missingFrom(declared, ORACLE));
  }

  @Test
  void brokenMetadataIsAnError() {
    assertThrowsUnchecked(
        () ->
            ConfigurationDocs.metadataKeys(
                new ByteArrayInputStream("{".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  void theMetersAndSpanAttributesAreNotConfigurationNames() {
    Set<String> names = ConfigurationDocs.nonConfigurationNames();

    assertTrue(names.contains(MetricNames.COMMANDS_DISPATCHED));
    assertTrue(names.contains("streamrune.command.type"));
  }

  @Test
  void theRepositoryGuidesAndAReadmeAreRead() {
    Path root = ConfigurationDocs.repositoryRoot();

    var docs = ConfigurationDocs.read(root, "streamrune-integration/streamrune-spring/README.md");

    assertEquals(root, docs.root());
    assertTrue(docs.fileNames().contains("docs/guide/production.md"), docs.fileNames().toString());
    assertTrue(docs.fileNames().contains("streamrune-integration/streamrune-spring/README.md"));
    assertTrue(docs.fileNames().size() > 10, docs.fileNames().toString());
  }

  @Test
  void aMissingFileIsAnError(@TempDir Path empty) throws Exception {
    Files.createDirectories(empty.resolve("docs/guide"));

    assertThrowsUnchecked(() -> ConfigurationDocs.read(empty, "no/such/README.md"));
  }

  @Test
  void aKeyReadInCodeMustBeKnown(@TempDir Path sources) throws Exception {
    Path type = sources.resolve("org/example/Reader.java");
    Files.createDirectories(type.getParent());
    Files.writeString(
        type,
        String.join(
            "\n",
            "package org.example;",
            "// \"streamrune.in-a-comment\" is not read",
            "/* \"streamrune.in-a-block\" neither */",
            " * \"streamrune.in-javadoc\" neither",
            "class Reader {",
            "  String a = env.get(\"streamrune.outbox.enabled\");",
            "  String b = env.get(\"streamrune.projections.\" + name + \".enabled\");",
            "  String c = \"streamrune.renamed must be set with \\\"streamrune.outbox.enabled\\\"\";",
            "  String d = \"" + MetricNames.COMMANDS_DISPATCHED + "\";",
            "}"));
    Files.writeString(sources.resolve("org/example/package-info.java"), "package org.example;");
    Files.writeString(sources.resolve("module-info.java"), "module org.example {}");

    assertEquals(
        List.of("org/example/Reader.java:8: streamrune.renamed"),
        ConfigurationDocs.unknownInJavaSources(sources, ORACLE));
    assertEquals(List.of("org.example.Reader"), ConfigurationDocs.typesDeclaredIn(sources));
  }

  @Test
  void anUnreadableSourceTreeIsAnError(@TempDir Path empty) {
    Path missing = empty.resolve("no-such-dir");

    assertThrowsUnchecked(() -> ConfigurationDocs.unknownInJavaSources(missing, ORACLE));
    assertThrowsUnchecked(() -> ConfigurationDocs.typesDeclaredIn(missing));
  }

  private static void assertThrowsUnchecked(Runnable action) {
    try {
      action.run();
    } catch (UncheckedIOException expected) {
      return;
    }
    throw new AssertionError("expected an UncheckedIOException");
  }
}
