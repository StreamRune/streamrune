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
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
 *
 * <p>The files read are the guides under {@code docs/guide}, the integration's README, and the
 * {@link #OTHER_PAGES} that name keys too. Every name of every file is held to the code ({@link
 * #unknown}, {@link #wrongDefaults}); a key is documented ({@link #undocumented}) when a guide or
 * the README names it, where a reader looks a key up.
 */
public final class ConfigurationDocs {

  /**
   * The pages outside {@code docs/guide} that name configuration keys: the repository's README, the
   * quickstart and the API reference, repository-relative.
   */
  public static final List<String> OTHER_PAGES =
      List.of("README.md", "docs/QUICKSTART.md", "docs/reference/api.md");

  /**
   * The main sources of {@code streamrune-integration-api}, repository-relative: the keys the three
   * integrations read through its constants ({@code RequestIdentityPolicy}, {@code
   * SnapshotPurgeSetting} and the other settings) are declared there.
   */
  public static final String INTEGRATION_API_SOURCES =
      "streamrune-integration/streamrune-integration-api/src/main/java";

  /**
   * A {@code streamrune.} name: dot-separated segments of lower-case letters, digits, {@code -} and
   * {@code _}, or a {@code <placeholder>}, optionally ending in {@code .*}. Not preceded by a word
   * character, a dot, a slash, a dash or an at sign, so a package name ({@code
   * org.streamrune.core}), a system property flag ({@code -Dstreamrune.x}) or the domain of an
   * e-mail address is not a name.
   */
  private static final Pattern NAME =
      Pattern.compile(
          "(?<![\\w.$/@-])streamrune(?:\\.(?:[a-z0-9]+(?:[-_][a-z0-9]+)*|<[a-z-]+>))+(?:\\.\\*)?");

  /** A Java string literal on one line; group 1 is its content. */
  private static final Pattern STRING_LITERAL = Pattern.compile("\"((?:[^\"\\\\]|\\\\.)*)\"");

  /** A YAML mapping key at the start of a line, with its indentation. */
  private static final Pattern YAML_KEY =
      Pattern.compile("^(\\s*)([a-z0-9][a-z0-9_.-]*):(?:\\s|$)");

  private static final Set<String> KEY_COLUMNS =
      Set.of("Property", "Setting", "Spring", "Quarkus", "Micronaut");

  private final Path root;
  private final Map<String, List<String>> files;

  /** The files whose names document a key: the guides and the given README. */
  private final Set<String> referenceFiles;

  private ConfigurationDocs(
      Path root, Map<String, List<String>> files, Set<String> referenceFiles) {
    this.root = root;
    this.files = files;
    this.referenceFiles = referenceFiles;
  }

  /**
   * The markdown files under {@code docs/guide} of the repository, the given repository-relative
   * files (an integration's README) and the {@link #OTHER_PAGES}, read once.
   */
  public static ConfigurationDocs read(Path root, String... moreFiles) {
    Map<String, List<String>> files = new LinkedHashMap<>();
    Set<String> referenceFiles = new TreeSet<>();
    try (Stream<Path> guides = Files.walk(root.resolve("docs/guide"))) {
      for (Path guide : guides.filter(p -> p.toString().endsWith(".md")).sorted().toList()) {
        files.put(relative(root, guide), Files.readAllLines(guide, StandardCharsets.UTF_8));
        referenceFiles.add(relative(root, guide));
      }
      for (String file : moreFiles) {
        files.put(file, Files.readAllLines(root.resolve(file), StandardCharsets.UTF_8));
        referenceFiles.add(file);
      }
      for (String page : OTHER_PAGES) {
        files.put(page, Files.readAllLines(root.resolve(page), StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new ConfigurationDocs(root, files, referenceFiles);
  }

  /**
   * Markdown text given directly, by file name; for testing this class. Every file documents the
   * keys it names.
   */
  static ConfigurationDocs of(Map<String, String> markdownByFile) {
    return of(markdownByFile, markdownByFile.keySet());
  }

  /**
   * Markdown text given directly, by file name, of which {@code referenceFiles} document the keys
   * they name; for testing this class.
   */
  static ConfigurationDocs of(Map<String, String> markdownByFile, Set<String> referenceFiles) {
    Map<String, List<String>> files = new LinkedHashMap<>();
    markdownByFile.forEach((name, text) -> files.put(name, List.of(text.split("\n", -1))));
    return new ConfigurationDocs(Path.of("."), files, Set.copyOf(referenceFiles));
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

  /**
   * The keys of {@code oracle} that no guide and no given README names, with no placeholder exempt;
   * a key named on one of the {@link #OTHER_PAGES} only is not documented.
   */
  public List<String> undocumented(Map<String, String> oracle) {
    Set<String> named = new TreeSet<>();
    occurrences().stream()
        .filter(o -> referenceFiles.contains(o.file()))
        .forEach(o -> named.add(o.name()));
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
      if (!isKnown(occurrence.name(), known)) {
        problems.add(occurrence.where() + ": " + occurrence.name());
      }
    }
    return problems;
  }

  /**
   * Whether {@code name} is one of {@code known} or a namespace of one, with or without {@code .*}.
   */
  private static boolean isKnown(String name, Set<String> known) {
    boolean wildcard = name.endsWith(".*");
    String stem = wildcard ? name.substring(0, name.length() - 2) : name;
    return (!wildcard && known.contains(name))
        || known.stream().anyMatch(k -> k.startsWith(stem + "."));
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

  /**
   * Every {@code streamrune.} name inside a string literal of the Java sources under {@code
   * sourceRoot} that is not a key of {@code oracle}, a meter, a span attribute or a namespace of
   * them, as {@code file:line: name}: a key the integration reads in code that the oracle does not
   * know. Comment lines (starting with {@code //}, {@code /*} or {@code *}) are skipped.
   */
  public static List<String> unknownInJavaSources(Path sourceRoot, Map<String, String> oracle) {
    Set<String> known = new TreeSet<>(oracle.keySet());
    known.addAll(nonConfigurationNames());
    List<String> problems = new ArrayList<>();
    try (Stream<Path> sources = Files.walk(sourceRoot)) {
      for (Path source : sources.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
        List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
          String code = lines.get(i).strip();
          if (code.startsWith("//") || code.startsWith("/*") || code.startsWith("*")) {
            continue;
          }
          Matcher literal = STRING_LITERAL.matcher(code);
          while (literal.find()) {
            Matcher name = NAME.matcher(literal.group(1));
            while (name.find()) {
              if (!isKnown(name.group(), known)) {
                problems.add(relative(sourceRoot, source) + ":" + (i + 1) + ": " + name.group());
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return problems;
  }

  /**
   * The names of the top-level types declared in the Java sources under {@code sourceRoot}, a
   * {@code src/main/java} directory: what an integration's test loads, with their nested types, to
   * read the annotations its framework reads.
   */
  public static List<String> typesDeclaredIn(Path sourceRoot) {
    List<String> types = new ArrayList<>();
    try (Stream<Path> sources = Files.walk(sourceRoot)) {
      for (Path source : sources.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
        String name = relative(sourceRoot, source);
        if (!name.endsWith("module-info.java") && !name.endsWith("package-info.java")) {
          types.add(name.substring(0, name.length() - ".java".length()).replace('/', '.'));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return types;
  }

  /**
   * The keys a Spring or Micronaut configuration metadata file declares ({@code
   * properties[].name}), each with the {@code defaultValue} it states, rendered as {@link
   * DefaultRendering} writes it: for a {@code java.time.Duration} key the value is read in the
   * short form ({@code 5s}, {@code 7d}, {@code 500ms}) or in ISO-8601 ({@code PT5M}); any other
   * value is its JSON text, and a key that states none has {@link DefaultRendering#NONE}.
   */
  public static Map<String, String> metadataDefaults(InputStream json) {
    Map<String, String> defaults = new TreeMap<>();
    try (json) {
      JsonNode root = new ObjectMapper().readTree(json);
      for (JsonNode property : root.path("properties")) {
        defaults.put(property.path("name").asText(), statedDefault(property));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return defaults;
  }

  /**
   * Every key of {@code metadata}, an integration's configuration metadata as {@link
   * #metadataDefaults} reads it, that {@code oracle} does not know or whose stated default differs
   * from the oracle's, as {@code key: …}. The keys the metadata leaves out are {@link
   * #missingFrom}'s.
   */
  public static List<String> wrongMetadata(
      Map<String, String> metadata, Map<String, String> oracle) {
    List<String> problems = new ArrayList<>();
    metadata.forEach(
        (key, stated) -> {
          String expected = oracle.get(key);
          if (expected == null) {
            problems.add(key + ": declared, but the code neither binds nor reads it");
          } else if (!expected.equals(stated)) {
            problems.add(
                key
                    + ": the metadata says '"
                    + stated
                    + "', the code default is '"
                    + expected
                    + "'");
          }
        });
    return problems;
  }

  /** A duration in the short form: an integer and one of the units the integrations accept. */
  private static final Pattern SHORT_DURATION = Pattern.compile("(-?\\d+)(ns|us|ms|s|m|h|d)");

  private static String statedDefault(JsonNode property) {
    JsonNode value = property.path("defaultValue");
    if (value.isMissingNode() || value.isNull()) {
      return DefaultRendering.NONE;
    }
    if (value.isArray()) {
      List<String> elements = new ArrayList<>();
      value.forEach(element -> elements.add(element.asText()));
      return DefaultRendering.render(elements);
    }
    String text = value.asText();
    if ("java.time.Duration".equals(property.path("type").asText())) {
      Duration duration = duration(text);
      if (duration != null) {
        return DefaultRendering.render(duration);
      }
    }
    return DefaultRendering.render(text);
  }

  /** {@code text} read as a duration in the short form or in ISO-8601, or null if it is neither. */
  private static Duration duration(String text) {
    Matcher shortForm = SHORT_DURATION.matcher(text);
    if (shortForm.matches()) {
      long amount = Long.parseLong(shortForm.group(1));
      return switch (shortForm.group(2)) {
        case "ns" -> Duration.ofNanos(amount);
        case "us" -> Duration.ofNanos(amount * 1_000);
        case "ms" -> Duration.ofMillis(amount);
        case "s" -> Duration.ofSeconds(amount);
        case "m" -> Duration.ofMinutes(amount);
        case "h" -> Duration.ofHours(amount);
        default -> Duration.ofDays(amount);
      };
    }
    try {
      return Duration.parse(text);
    } catch (DateTimeParseException notIso) {
      return null;
    }
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

  /** A name at a place: {@code where} is {@code file:line}. */
  record Occurrence(String name, String file, String where) {}

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
              occurrences.add(new Occurrence(name.group(), file, where));
            }
            if (inYaml) {
              yamlKey(line, path)
                  .ifPresent(key -> occurrences.add(new Occurrence(key, file, where)));
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
