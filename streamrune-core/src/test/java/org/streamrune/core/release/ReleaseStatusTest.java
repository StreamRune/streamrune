package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The pages a user starts from say what is true about the release their coordinates name and about
 * the preview, before the release and in the commit the release tag freezes.
 *
 * <p>The version the README's Installation names is the release the pages are about. While its
 * {@code CHANGELOG.md} entry is undated ({@code ## [1.0.0] — unreleased}), the README, the
 * quickstart and the getting-started guide say that it is not on Maven Central yet. The pull
 * request that dates the entry is the commit that is released, so once it is dated, none of them
 * says so or calls it the release to come, the README's Status names it as released ({@code `1.0.0`
 * is the ...}), and the development version, the default {@code version} of the root build, has
 * moved above it: the tag then freezes a README whose preview is the next one, and Publish
 * Snapshot, which refuses a version below a tagged release, keeps publishing. A later undated entry
 * ({@code ## [1.0.1] — unreleased}) changes nothing while the Installation still names a released
 * version.
 *
 * <p>The README's "Preview builds" section names the development version, and no other page does:
 * the version moves in the build and the README only.
 */
class ReleaseStatusTest {

  private static final List<String> STATUS_PAGES =
      List.of("README.md", "docs/QUICKSTART.md", "docs/guide/getting-started.md");

  private static final Pattern DEFAULT_VERSION =
      Pattern.compile(
          "version = \\(project\\.findProperty\\(\"releaseVersion\"\\) as String\\?\\) \\?: \"([^\"]+)\"");

  private static final Pattern INSTALLED =
      Pattern.compile("org\\.streamrune:streamrune-core:([0-9]+\\.[0-9]+\\.[0-9]+[0-9A-Za-z.-]*)");

  private static final Pattern NOT_YET = Pattern.compile("is not on Maven Central yet");

  private static final Pattern TO_COME = Pattern.compile("will be the (?:first|next) release");

  @Test
  void theStatusPagesSayWhetherTheInstalledReleaseIsOnMavenCentral() {
    Map<String, String> pages = new LinkedHashMap<>();
    STATUS_PAGES.forEach(page -> pages.put(page, RepositoryScripts.read(page)));
    assertThat(statusProblems(RepositoryScripts.read("CHANGELOG.md"), pages, developmentVersion()))
        .isEmpty();
  }

  @Test
  void beforeTheReleaseThePagesMustSayItIsNotOutYet() {
    Map<String, String> pages =
        pages("`1.0.0` will be the first release. `1.0.0` is not on Maven Central yet.");
    assertThat(statusProblems(changelog("unreleased"), pages, "1.0.0-alpha-SNAPSHOT")).isEmpty();

    pages.put("docs/QUICKSTART.md", "Installation: org.streamrune:streamrune-core:1.0.0");
    assertThat(statusProblems(changelog("unreleased"), pages, "1.0.0-alpha-SNAPSHOT"))
        .singleElement()
        .asString()
        .contains("docs/QUICKSTART.md")
        .contains("`1.0.0` is not on Maven Central yet");
  }

  @Test
  void theReleasedCommitNamesTheReleaseAsOutAndThePreviewAboveIt() {
    Map<String, String> released = pages("`1.0.0` is the first release of StreamRune.");
    assertThat(statusProblems(changelog("2026-12-01"), released, "1.0.1-SNAPSHOT")).isEmpty();

    assertThat(
            statusProblems(
                changelog("2026-12-01"),
                pages("`1.0.0` will be the first release. `1.0.0` is not on Maven Central yet."),
                "1.0.1-SNAPSHOT"))
        .anyMatch(p -> p.contains("is not on Maven Central yet"))
        .anyMatch(p -> p.contains("will be the first release"))
        .anyMatch(p -> p.contains("does not say"));
    assertThat(statusProblems(changelog("2026-12-01"), released, "1.0.0-alpha-SNAPSHOT"))
        .singleElement()
        .asString()
        .contains("1.0.0-alpha-SNAPSHOT sorts below 1.0.0");
    assertThat(statusProblems(changelog("2026-12-01"), released, "1.0.0-SNAPSHOT")).hasSize(1);
  }

  @Test
  void aLaterUndatedEntryDemandsNothingWhileTheInstalledVersionIsReleased() {
    String changelog =
        "# Changelog\n\n## [1.0.1] — unreleased\n\nFixes.\n\n## [1.0.0] — 2026-12-01\n\nFirst.\n";
    assertThat(
            statusProblems(
                changelog, pages("`1.0.0` is the first release of StreamRune."), "1.0.1-SNAPSHOT"))
        .isEmpty();
  }

  @Test
  void thePreviewBuildsSectionNamesTheVersionTheBuildPublishes() {
    String readme = RepositoryScripts.read("README.md");
    int start = readme.indexOf("\n## Preview builds\n");
    int end = readme.indexOf("\n## ", start + 1);
    assertThat(start).as("README.md has a Preview builds section").isPositive();
    String section = readme.substring(start, end);

    List<String> named = new ArrayList<>();
    Matcher snapshot =
        Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+[0-9A-Za-z.-]*-SNAPSHOT").matcher(section);
    while (snapshot.find()) {
      named.add(snapshot.group());
    }
    assertThat(named).isNotEmpty().containsOnly(developmentVersion());
  }

  @Test
  void noOtherPageNamesTheDevelopmentVersion() throws IOException {
    String development = developmentVersion();
    Path root = RepositoryScripts.repositoryRoot();
    List<String> naming = new ArrayList<>();
    for (Path page : pages(root)) {
      String relative = root.relativize(page).toString().replace('\\', '/');
      if (!relative.equals("README.md")
          && Files.readString(page, StandardCharsets.UTF_8).contains(development)) {
        naming.add(relative);
      }
    }
    assertThat(naming).as("pages that name " + development + " besides the README").isEmpty();
  }

  /**
   * Why the status pages {@code pages} (path to text, the README first) disagree with {@code
   * changelog} and the development version {@code development}.
   */
  static List<String> statusProblems(
      String changelog, Map<String, String> pages, String development) {
    List<String> problems = new ArrayList<>();
    String readme = pages.get("README.md");
    Matcher installed = INSTALLED.matcher(readme);
    if (!installed.find()) {
      return List.of("README.md names no org.streamrune:streamrune-core:<version> to install");
    }
    String version = installed.group(1);
    Matcher entry =
        Pattern.compile("(?m)^## \\[" + Pattern.quote(version) + "\\] (?:—|-) (.+)$")
            .matcher(changelog);
    if (!entry.find()) {
      return List.of(
          "CHANGELOG.md has no `## [" + version + "]` entry, the version README.md installs");
    }
    boolean released = !entry.group(1).strip().equals("unreleased");
    pages.forEach(
        (page, raw) -> {
          String text = raw.replaceAll("\\s+", " ");
          if (!released && !text.contains("`" + version + "` is not on Maven Central yet")) {
            problems.add(
                page
                    + " must say \"`"
                    + version
                    + "` is not on Maven Central yet\" while its "
                    + "CHANGELOG entry is undated");
          }
          if (released && NOT_YET.matcher(text).find()) {
            problems.add(
                page + " says a version is not on Maven Central yet, but " + version + " is dated");
          }
          if (released && TO_COME.matcher(text).find()) {
            problems.add(
                page + " says \"will be the first release\", but " + version + " is dated");
          }
        });
    if (released) {
      int start = readme.indexOf("## Status");
      int end = start < 0 ? -1 : readme.indexOf("\n## ", start + 1);
      String status =
          start < 0
              ? ""
              : readme.substring(start, end < 0 ? readme.length() : end).replaceAll("\\s+", " ");
      if (!status.contains("`" + version + "` is the")) {
        problems.add(
            "the Status section of README.md does not say \"`"
                + version
                + "` is the ...\" release");
      }
      String base = development.replaceFirst("-.*", "");
      if (!numericallyAbove(base, version.replaceFirst("-.*", ""))) {
        problems.add(
            "the development version "
                + development
                + " sorts below "
                + version
                + ", whose entry is dated: move the default version in the root build.gradle.kts "
                + "above it, with the README's Preview builds, in the pull request that dates it");
      }
    }
    return problems;
  }

  private static boolean numericallyAbove(String a, String b) {
    String[] x = a.split("\\.");
    String[] y = b.split("\\.");
    for (int i = 0; i < 3; i++) {
      int c = Integer.compare(Integer.parseInt(x[i]), Integer.parseInt(y[i]));
      if (c != 0) {
        return c > 0;
      }
    }
    return false;
  }

  private static String developmentVersion() {
    Matcher declared = DEFAULT_VERSION.matcher(RepositoryScripts.read("build.gradle.kts"));
    assertThat(declared.find()).as("the default version in the root build.gradle.kts").isTrue();
    return declared.group(1);
  }

  private static String changelog(String date) {
    return "# Changelog\n\n## [1.0.0] — " + date + "\n\nThe first release.\n";
  }

  private static Map<String, String> pages(String status) {
    Map<String, String> pages = new LinkedHashMap<>();
    pages.put(
        "README.md",
        "# StreamRune\n\n## Status\n\n"
            + status
            + "\n\n## Installation\n\n"
            + "implementation(\"org.streamrune:streamrune-core:1.0.0\")\n");
    String rest =
        status.contains("not on Maven Central yet") ? "`1.0.0` is not on Maven Central yet." : "";
    pages.put("docs/QUICKSTART.md", rest);
    pages.put("docs/guide/getting-started.md", rest);
    return pages;
  }

  /** The markdown pages and issue templates of the repository, build output and tooling aside. */
  private static List<Path> pages(Path root) throws IOException {
    Set<String> pruned = Set.of("build", "out", "node_modules");
    try (Stream<Path> walk = Files.walk(root)) {
      return walk.filter(Files::isRegularFile)
          .filter(
              p -> {
                Path relative = root.relativize(p);
                for (Path part :
                    relative.getParent() == null ? List.<Path>of() : relative.getParent()) {
                  String name = part.toString();
                  if (pruned.contains(name) || (name.startsWith(".") && !name.equals(".github"))) {
                    return false;
                  }
                }
                String name = p.getFileName().toString();
                return name.endsWith(".md")
                    || relative.toString().replace('\\', '/').startsWith(".github/ISSUE_TEMPLATE/");
              })
          .sorted()
          .toList();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
