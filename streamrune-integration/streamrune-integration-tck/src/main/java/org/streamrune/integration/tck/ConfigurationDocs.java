package org.streamrune.integration.tck;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.streamrune.core.metrics.MetricNames;

/**
 * The configuration keys a set of markdown files names, and the defaults its tables state: what
 * each integration's {@code ConfigurationDocLintTest} holds against its {@link
 * ConfigurationOracle}.
 *
 * <p>A key is named where its dotted form appears ({@code streamrune.outbox.enabled}, in prose, in
 * a table or in a properties listing) or where a YAML listing nests it ({@code streamrune:} then
 * {@code outbox:} then {@code enabled:}). A placeholder segment such as {@code <name>} is part of
 * the key. Meter names ({@link MetricNames}) and span attribute names ({@code TraceAttributes}),
 * which share the {@code streamrune.} prefix, are not keys.
 *
 * <p>A table states a default when its header has a {@code Default} column; the keys of a row are
 * the ones in its {@code Property}, {@code Setting}, {@code Spring}, {@code Quarkus} and {@code
 * Micronaut} cells. The cell is compared after its backticks and bold markers are removed, so it
 * must be written as {@link DefaultRendering} writes the default.
 */
public final class ConfigurationDocs {

  /**
   * A {@code streamrune.} name: dot-separated segments of lower-case letters, digits, {@code -} and
   * {@code _}, or a {@code <placeholder>}, optionally ending in {@code .*}. Not preceded by a word
   * character, a dot, a slash or a dash, so a package name ({@code org.streamrune.core}) or a
   * system property flag ({@code -Dstreamrune.x}) is not a name.
   */
  private static final Pattern NAME =
      Pattern.compile(
          "(?<![\\w.$/-])streamrune(?:\\.(?:[a-z0-9]+(?:[-_][a-z0-9]+)*|<[a-z-]+>))+(?:\\.\\*)?");

  /** A YAML mapping key at the start of a line, with its indentation. */
  private static final Pattern YAML_KEY =
      Pattern.compile("^(\\s*)([a-z0-9][a-z0-9_.-]*):(?:\\s|$)");

  private static final Set<String> KEY_COLUMNS =
      Set.of("Property", "Setting", "Spring", "Quarkus", "Micronaut");

  private final Path root;
  private final Map<String, List<String>> files;

  private ConfigurationDocs(Path root, Map<String, List<String>> files) {
    this.root = root;
    this.files = files;
  }

  /**
   * The markdown files under {@code docs/guide} of the repository and the given repository-relative
   * files (an integration's README), read once.
   */
  public static ConfigurationDocs read(Path root, String... moreFiles) {
    Map<String, List<String>> files = new LinkedHashMap<>();
    try (Stream<Path> guides = Files.walk(root.resolve("docs/guide"))) {
      for (Path guide : guides.filter(p -> p.toString().endsWith(".md")).sorted().toList()) {
        files.put(relative(root, guide), Files.readAllLines(guide, StandardCharsets.UTF_8));
      }
      for (String file : moreFiles) {
        files.put(file, Files.readAllLines(root.resolve(file), StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new ConfigurationDocs(root, files);
  }

  /** Markdown text given directly, by file name; for testing this class. */
  static ConfigurationDocs of(Map<String, String> markdownByFile) {
    Map<String, List<String>> files = new LinkedHashMap<>();
    markdownByFile.forEach((name, text) -> files.put(name, List.of(text.split("\n", -1))));
    return new ConfigurationDocs(Path.of("."), files);
  }

  /**
   * The repository root: the nearest directory, from the working directory up, that holds a {@code
   * settings.gradle.kts}.
   */
  public static Path repositoryRoot() {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null && !Files.exists(dir.resolve("settings.gradle.kts"))) {
      dir = dir.getParent();
    }
    if (dir == null) {
      throw new IllegalStateException(
          "no settings.gradle.kts above " + Path.of("").toAbsolutePath());
    }
    return dir;
  }

  /** The repository-relative names of the files read. */
  public Set<String> fileNames() {
    return new TreeSet<>(files.keySet());
  }

  /** The keys of {@code oracle} that no file names, with no placeholder exempt. */
  public List<String> undocumented(Map<String, String> oracle) {
    Set<String> named = new TreeSet<>();
    occurrences().forEach(o -> named.add(o.name()));
    List<String> missing = new ArrayList<>();
    for (String key : oracle.keySet()) {
      if (!named.contains(key)) {
        missing.add(key + " (default " + oracle.get(key) + ")");
      }
    }
    return missing;
  }

  /**
   * Every {@code streamrune.} name in the files that is neither a key of {@code oracle}, a meter, a
   * span attribute, nor a namespace of keys or meters (a name that a key or a meter continues after
   * a dot, or such a name followed by {@code .*}), as {@code file:line: name}.
   */
  public List<String> unknown(Map<String, String> oracle) {
    Set<String> known = new TreeSet<>(oracle.keySet());
    known.addAll(nonConfigurationNames());
    List<String> problems = new ArrayList<>();
    for (Occurrence occurrence : occurrences()) {
      String name = occurrence.name();
      String stem = name.endsWith(".*") ? name.substring(0, name.length() - 2) : name;
      boolean isKnown =
          (!name.endsWith(".*") && known.contains(name))
              || known.stream().anyMatch(k -> k.startsWith(stem + "."));
      if (!isKnown) {
        problems.add(occurrence.where() + ": " + name);
      }
    }
    return problems;
  }

  /**
   * Every row of a table with a {@code Default} column whose default differs from the default of a
   * key of {@code oracle} the row names, as {@code file:line: key: the table says X, the code
   * default is Y}.
   */
  public List<String> wrongDefaults(Map<String, String> oracle) {
    List<String> problems = new ArrayList<>();
    for (DefaultRow row : defaultRows()) {
      for (String key : row.keys()) {
        String expected = oracle.get(key);
        if (expected != null && !expected.equals(row.stated())) {
          problems.add(
              row.where()
                  + ": "
                  + key
                  + ": the table says '"
                  + row.stated()
                  + "', the code default is '"
                  + expected
                  + "'");
        }
      }
    }
    return problems;
  }

  /** The number of table rows that state the default of a key: proof the tables were found. */
  public int defaultRowCount() {
    return defaultRows().size();
  }

  /**
   * The keys of {@code oracle} missing from {@code declared}, the keys of an integration's
   * configuration metadata; a key with a placeholder segment is exempt, since metadata cannot
   * declare it.
   */
  public static List<String> missingFrom(Set<String> declared, Map<String, String> oracle) {
    List<String> missing = new ArrayList<>();
    for (String key : oracle.keySet()) {
      if (!key.contains("<") && !declared.contains(key)) {
        missing.add(key + " (default " + oracle.get(key) + ")");
      }
    }
    return missing;
  }

  /** The {@code properties[].name} values of a Spring or Micronaut configuration metadata file. */
  public static Set<String> metadataKeys(InputStream json) {
    Set<String> keys = new TreeSet<>();
    try (json) {
      JsonNode root = new ObjectMapper().readTree(json);
      for (JsonNode property : root.path("properties")) {
        keys.add(property.path("name").asText());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return keys;
  }

  /**
   * The {@code streamrune.} names that are not configuration keys: every meter of {@link
   * MetricNames} and every span attribute of {@code TraceAttributes}.
   */
  static Set<String> nonConfigurationNames() {
    Set<String> names = new TreeSet<>();
    for (Field field : MetricNames.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        names.add((String) staticValue(field));
      }
    }
    for (Field field : org.streamrune.runtime.TraceAttributes.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers())
          && field.getType() == io.opentelemetry.api.common.AttributeKey.class) {
        names.add(((io.opentelemetry.api.common.AttributeKey<?>) staticValue(field)).getKey());
      }
    }
    return names;
  }

  private static Object staticValue(Field field) {
    try {
      return field.get(null);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A name at a place: {@code file:line}. */
  record Occurrence(String name, String where) {}

  /** A table row that states a default for {@code keys}. */
  record DefaultRow(List<String> keys, String stated, String where) {}

  /** Every name in every file, dotted or nested in a YAML listing, in file order. */
  List<Occurrence> occurrences() {
    List<Occurrence> occurrences = new ArrayList<>();
    files.forEach(
        (file, lines) -> {
          boolean inYaml = false;
          Deque<Map.Entry<Integer, String>> path = new ArrayDeque<>();
          for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String where = file + ":" + (i + 1);
            String fence = line.strip();
            if (fence.startsWith("```") || fence.startsWith("~~~")) {
              inYaml = !inYaml && fence.substring(3).strip().matches("ya?ml");
              path.clear();
              continue;
            }
            Matcher name = NAME.matcher(line);
            while (name.find()) {
              occurrences.add(new Occurrence(name.group(), where));
            }
            if (inYaml) {
              yamlKey(line, path).ifPresent(key -> occurrences.add(new Occurrence(key, where)));
            }
          }
        });
    return occurrences;
  }

  /**
   * The dotted key a YAML line declares under the mapping keys above it, when it is a {@code
   * streamrune} key; {@code path} carries the enclosing keys and their indentation from line to
   * line.
   */
  private static java.util.Optional<String> yamlKey(
      String line, Deque<Map.Entry<Integer, String>> path) {
    Matcher key = YAML_KEY.matcher(line);
    if (!key.find()) {
      return java.util.Optional.empty();
    }
    int indent = key.group(1).length();
    while (!path.isEmpty() && path.peekLast().getKey() >= indent) {
      path.removeLast();
    }
    path.addLast(Map.entry(indent, key.group(2)));
    String dotted = String.join(".", path.stream().map(Map.Entry::getValue).toList());
    return dotted.startsWith("streamrune.")
        ? java.util.Optional.of(dotted)
        : java.util.Optional.empty();
  }

  /** Every row of every table that has a {@code Default} column and names a key in a key cell. */
  List<DefaultRow> defaultRows() {
    List<DefaultRow> rows = new ArrayList<>();
    files.forEach(
        (file, lines) -> {
          List<String> header = null;
          for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).strip();
            if (!line.startsWith("|")) {
              header = null;
              continue;
            }
            List<String> cells = cells(line);
            if (header == null) {
              header = cells;
              continue;
            }
            if (cells.stream().allMatch(c -> c.matches(":?-+:?"))) {
              continue;
            }
            int defaultColumn = header.indexOf("Default");
            if (defaultColumn < 0 || defaultColumn >= cells.size()) {
              continue;
            }
            List<String> keys = new ArrayList<>();
            for (int c = 0; c < Math.min(header.size(), cells.size()); c++) {
              if (KEY_COLUMNS.contains(header.get(c))) {
                Matcher name = NAME.matcher(cells.get(c));
                while (name.find()) {
                  if (!keys.contains(name.group())) {
                    keys.add(name.group());
                  }
                }
              }
            }
            if (!keys.isEmpty()) {
              String stated = cells.get(defaultColumn).replace("`", "").replace("**", "").strip();
              rows.add(new DefaultRow(keys, stated, file + ":" + (i + 1)));
            }
          }
        });
    return rows;
  }

  private static List<String> cells(String row) {
    String inner =
        row.substring(1, row.endsWith("|") && row.length() > 1 ? row.length() - 1 : row.length());
    List<String> cells = new ArrayList<>();
    for (String cell : inner.split("\\|", -1)) {
      cells.add(cell.strip());
    }
    return cells;
  }

  private static String relative(Path root, Path file) {
    return root.relativize(file).toString().replace('\\', '/');
  }

  /** The repository root the files were read from. */
  public Path root() {
    return root;
  }
}
