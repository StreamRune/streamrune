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
  void aNameOnAnotherPageIsHeldToTheCodeAndDocumentsNoKey() {
    var docs =
        ConfigurationDocs.of(
            Map.of(
                "guide.md",
                "`streamrune.outbox.enabled`, `streamrune.outbox.retention-max-age`,"
                    + " `streamrune.retry-max-attempts`, `streamrune.projections.<name>.enabled`.",
                "docs/QUICKSTART.md",
                "Set `streamrune.inbox.retention-max-age`; `streamrune.outbox.time-out` is no key."),
            Set.of("guide.md"));

    assertEquals(
        List.of("streamrune.inbox.retention-max-age (default 7d)"), docs.undocumented(ORACLE));
    assertEquals(List.of("docs/QUICKSTART.md:1: streamrune.outbox.time-out"), docs.unknown(ORACLE));
  }

  @Test
  void theDomainOfAnEmailAddressIsNoName() {
    var docs = ConfigurationDocs.of(Map.of("README.md", "Write to security@streamrune.com."));

    assertEquals(List.of(), docs.unknown(ORACLE));
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

    Set<String> declared = metadata(json).keySet();

    assertEquals(Set.of("streamrune.outbox.enabled", "streamrune.retry-max-attempts"), declared);
    assertEquals(
        List.of(
            "streamrune.inbox.retention-max-age (default 7d)",
            "streamrune.outbox.retention-max-age (default 7d)"),
        ConfigurationDocs.missingFrom(declared, ORACLE));
  }

  @Test
  void metadataDefaultsAreRenderedAsTheGuidesWriteThem() {
    Map<String, String> stated =
        metadata(
            "{\"properties\":["
                + duration("a.days", "\"7d\"")
                + duration("a.hours", "\"24h\"")
                + duration("a.minutes", "\"60s\"")
                + duration("a.seconds", "\"PT30S\"")
                + duration("a.millis", "\"1500ms\"")
                + duration("a.micros", "\"500us\"")
                + duration("a.nanos", "\"500ns\"")
                + duration("a.minutes-short", "\"5m\"")
                + duration("a.zero", "\"0s\"")
                + duration("a.words", "\"5 minutes\"")
                + "{\"name\":\"a.flag\",\"type\":\"java.lang.Boolean\",\"defaultValue\":false},"
                + "{\"name\":\"a.ratio\",\"type\":\"java.lang.Double\",\"defaultValue\":2.0},"
                + "{\"name\":\"a.count\",\"type\":\"java.lang.Integer\",\"defaultValue\":100},"
                + "{\"name\":\"a.mode\",\"type\":\"java.lang.String\",\"defaultValue\":\"5m\"},"
                + "{\"name\":\"a.list\",\"defaultValue\":[\"x\",\"y\"]},"
                + "{\"name\":\"a.empty\",\"defaultValue\":\"\"},"
                + "{\"name\":\"a.null\",\"defaultValue\":null},"
                + "{\"name\":\"a.none\",\"type\":\"java.time.Duration\"}]}");

    Map<String, String> expected = new TreeMap<>();
    expected.putAll(
        Map.of(
            "a.days", "7d",
            "a.hours", "1d",
            "a.minutes", "1m",
            "a.seconds", "30s",
            "a.millis", "1500ms",
            "a.micros", "PT0.0005S",
            "a.nanos", "PT0.0000005S",
            "a.minutes-short", "5m",
            "a.zero", "0s",
            "a.words", "5 minutes"));
    expected.putAll(
        Map.of(
            "a.flag", "false",
            "a.ratio", "2.0",
            "a.count", "100",
            "a.mode", "5m",
            "a.list", "x, y",
            "a.empty", DefaultRendering.NONE,
            "a.null", DefaultRendering.NONE,
            "a.none", DefaultRendering.NONE));
    assertEquals(expected, stated);
  }

  @Test
  void metadataMustStateTheCodeDefaultOfAKeyTheCodeKnows() {
    Map<String, String> stated =
        metadata(
            "{\"properties\":["
                + duration("streamrune.outbox.retention-max-age", "\"PT168H\"")
                + duration("streamrune.inbox.retention-max-age", "\"6d\"")
                + "{\"name\":\"streamrune.outbox.enabled\",\"defaultValue\":false},"
                + "{\"name\":\"streamrune.retry-max-attempts\"},"
                + "{\"name\":\"streamrune.outbox.renamed-key\",\"defaultValue\":1}]}");

    assertEquals(
        List.of(
            "streamrune.inbox.retention-max-age: the metadata says '6d', the code default is"
                + " '7d'",
            "streamrune.outbox.renamed-key: declared, but the code neither binds nor reads it",
            "streamrune.retry-max-attempts: the metadata says '—', the code default is '3'"),
        ConfigurationDocs.wrongMetadata(stated, ORACLE));
  }

  @Test
  void brokenMetadataIsAnError() {
    assertThrowsUnchecked(() -> metadata("{"));
  }

  private static Map<String, String> metadata(String json) {
    return ConfigurationDocs.metadataDefaults(
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  }

  private static String duration(String name, String defaultValue) {
    return "{\"name\":\""
        + name
        + "\",\"type\":\"java.time.Duration\",\"defaultValue\":"
        + defaultValue
        + "},";
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
    List<String> otherPages = ConfigurationDocs.otherPages(root);
    assertTrue(!otherPages.isEmpty(), ConfigurationDocs.OTHER_PAGES_LIST + " lists no page");
    assertTrue(docs.fileNames().containsAll(otherPages), docs.fileNames().toString());
    assertTrue(docs.fileNames().size() > 10, docs.fileNames().toString());
  }

  @Test
  void theOtherPagesAreTheLinesOfTheirListThatAreNeitherBlankNorAComment(@TempDir Path root)
      throws Exception {
    Files.createDirectories(root.resolve(ConfigurationDocs.OTHER_PAGES_LIST).getParent());
    Files.writeString(
        root.resolve(ConfigurationDocs.OTHER_PAGES_LIST),
        String.join("\n", "# the pages", "README.md", "", "  docs/QUICKSTART.md  ", "#", ""));

    assertEquals(List.of("README.md", "docs/QUICKSTART.md"), ConfigurationDocs.otherPages(root));
    assertThrowsUnchecked(() -> ConfigurationDocs.otherPages(root.resolve("no-such-root")));
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
