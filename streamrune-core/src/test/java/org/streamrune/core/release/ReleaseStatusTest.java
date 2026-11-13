package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The pages a user starts from say what is true about the release and the preview.
 *
 * <ul>
 *   <li>While the newest {@code CHANGELOG.md} entry is undated ({@code ## [x.y.z] — unreleased}),
 *       the README, the quickstart and the getting-started guide say that {@code x.y.z} is not on
 *       Maven Central yet, since their coordinates do not resolve; once the entry is dated, which
 *       the Release workflow requires, none of them says so. The pull request that dates the entry
 *       removes the sentences, and the released commit says the version is out.
 *   <li>The README's "Preview builds" section names the development version the build publishes
 *       (the default {@code version} of the root {@code build.gradle.kts}), and no other page names
 *       it: after a release the version moves in the build and the README only, and no page frozen
 *       at the release tag names a preview nobody publishes any more.
 * </ul>
 */
class ReleaseStatusTest {

  private static final List<String> STATUS_PAGES =
      List.of("README.md", "docs/QUICKSTART.md", "docs/guide/getting-started.md");

  private static final Pattern NEWEST_ENTRY =
      Pattern.compile("(?m)^## \\[([0-9]+\\.[0-9]+\\.[0-9]+[^\\]]*)\\] (?:—|-) (.+)$");

  private static final Pattern DEFAULT_VERSION =
      Pattern.compile(
          "version = \\(project\\.findProperty\\(\"releaseVersion\"\\) as String\\?\\) \\?: \"([^\"]+)\"");

  private static final Pattern NOT_YET = Pattern.compile("is not on Maven\\s+Central yet");

  @Test
  void theStatusPagesSayWhetherTheNewestEntryIsOnMavenCentral() {
    Matcher entry = NEWEST_ENTRY.matcher(RepositoryScripts.read("CHANGELOG.md"));
    assertThat(entry.find()).as("CHANGELOG.md has a `## [x.y.z] — …` entry").isTrue();
    String version = entry.group(1);
    boolean unreleased = entry.group(2).trim().equals("unreleased");
    for (String page : STATUS_PAGES) {
      String text = RepositoryScripts.read(page).replaceAll("\\s+", " ");
      if (unreleased) {
        assertThat(text)
            .as(page + " while the " + version + " entry of CHANGELOG.md is undated")
            .contains("`" + version + "` is not on Maven Central yet");
      } else {
        assertThat(NOT_YET.matcher(text).find())
            .as(page + " once the " + version + " entry of CHANGELOG.md is dated")
            .isFalse();
      }
    }
  }

  @Test
  void thePreviewBuildsSectionNamesTheVersionTheBuildPublishes() {
    Matcher declared = DEFAULT_VERSION.matcher(RepositoryScripts.read("build.gradle.kts"));
    assertThat(declared.find()).as("the default version in the root build.gradle.kts").isTrue();
    String development = declared.group(1);
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
    assertThat(named).isNotEmpty().containsOnly(development);
  }

  @Test
  void noOtherPageNamesTheDevelopmentVersion() throws IOException {
    Matcher declared = DEFAULT_VERSION.matcher(RepositoryScripts.read("build.gradle.kts"));
    assertThat(declared.find()).isTrue();
    String development = declared.group(1);
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
