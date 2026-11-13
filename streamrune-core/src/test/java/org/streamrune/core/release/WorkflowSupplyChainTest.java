package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The workflows run no code that a moved tag or another job's cache could change beside the signing
 * key and the Central Portal token.
 *
 * <p>Every action is pinned to a full commit SHA with the release it is in a trailing comment (a
 * tag can be moved to other code; a commit cannot). A job that has one of the release secrets in
 * its environment ({@code SIGNING_KEY}, {@code SIGNING_PASSWORD}, {@code MAVEN_CENTRAL_USERNAME},
 * {@code MAVEN_CENTRAL_PASSWORD}) neither reads nor writes the Gradle caches: a cached Gradle home
 * holds plugin jars, compiled build scripts and the Gradle distribution, which the build executes,
 * and every workflow on {@code main} writes such entries; so it has no {@code actions/cache} step,
 * no {@code setup-java} {@code cache:} and a {@code setup-gradle} with {@code cache-disabled:
 * true}. A secret named above the jobs reaches every job, and no workflow passes its secrets on
 * whole ({@code secrets: inherit}). The wrapper pins its distribution by checksum, since those jobs
 * download it.
 */
class WorkflowSupplyChainTest {

  private static final Pattern USES = Pattern.compile("^\\s*(?:-\\s+)?uses:\\s*(\\S+)(.*)$");

  private static final Pattern PINNED =
      Pattern.compile("[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_./-]+)?@[0-9a-f]{40}");

  private static final Pattern VERSION_COMMENT = Pattern.compile("\\s+# v[0-9]+(?:\\.[0-9]+)*\\s*");

  private static final Pattern RELEASE_SECRET =
      Pattern.compile(
          "secrets\\.(?:SIGNING_KEY|SIGNING_PASSWORD|MAVEN_CENTRAL_USERNAME|MAVEN_CENTRAL_PASSWORD)\\b");

  private static final Pattern JOB = Pattern.compile("^  ([A-Za-z0-9_-]+):\\s*$");

  @Test
  void everyActionIsPinnedToACommitWithItsReleaseInAComment() throws IOException {
    List<String> unpinned = new ArrayList<>();
    workflows()
        .forEach(
            (name, lines) -> {
              for (int i = 0; i < lines.size(); i++) {
                Matcher uses = USES.matcher(lines.get(i));
                if (!uses.matches() || uses.group(1).startsWith("./")) {
                  continue;
                }
                if (!PINNED.matcher(uses.group(1)).matches()
                    || !VERSION_COMMENT.matcher(uses.group(2)).matches()) {
                  unpinned.add(name + ":" + (i + 1) + ": " + lines.get(i).trim());
                }
              }
            });
    assertThat(unpinned).as("write `uses: owner/repo@<full commit SHA> # v<release>`").isEmpty();
  }

  @Test
  void aJobWithAReleaseSecretNeitherReadsNorWritesTheGradleCaches() throws IOException {
    Map<String, List<String>> secretJobs = new TreeMap<>();
    List<String> problems = cacheProblems(workflows(), secretJobs);
    // The rule holds for the jobs it is meant for: a parser that found none would pass vacuously.
    assertThat(secretJobs)
        .containsEntry("publish-snapshot.yml", List.of("publish-snapshot"))
        .containsEntry("release.yml", List.of("verify", "rehearse-signing", "publish"));
    assertThat(problems).isEmpty();
  }

  @Test
  void theCacheRuleRefusesEveryWayAJobCouldRestoreAGradleHome() {
    String job =
        """
        on: push
        jobs:
          publish:
            runs-on: ubuntu-latest
            steps:
              - uses: actions/setup-java@b843ebf9b913146b38da2eaa7aeddf76074ae1ad # v6.1.0
                with:
                  java-version: '25'
                  %s
              - uses: gradle/actions/setup-gradle@8da46b539cd8fe6ab27ca75128919a18d757604c # v6.4.1
                with:
                  %s
              - run: ./gradlew publish
                env:
                  TOKEN: ${{ secrets.MAVEN_CENTRAL_PASSWORD }}
        """;
    assertThat(problems(job.formatted("distribution: temurin", "cache-disabled: true"))).isEmpty();
    assertThat(problems(job.formatted("cache: 'gradle'", "cache-disabled: true")))
        .containsExactly("w.yml publish: setup-java with cache:");
    assertThat(problems(job.formatted("distribution: temurin", "cache-read-only: true")))
        .containsExactly("w.yml publish: setup-gradle without cache-disabled");
    assertThat(
            problems(
                job.formatted("distribution: temurin", "cache-disabled: true")
                    + "      - uses: actions/cache/restore@0123456789abcdef0123456789abcdef01234567 # v4.2.0\n"))
        .containsExactly("w.yml publish: actions/cache");
  }

  @Test
  void aReleaseSecretAtTheTopOfAWorkflowMakesEveryJobOneThatHoldsIt() {
    String workflow =
        """
        on: push
        env:
          SIGNING_KEY: ${{ secrets.SIGNING_KEY }}
        jobs:
          build:
            runs-on: ubuntu-latest
            steps:
              - uses: gradle/actions/setup-gradle@8da46b539cd8fe6ab27ca75128919a18d757604c # v6.4.1
        """;
    Map<String, List<String>> secretJobs = new TreeMap<>();
    assertThat(cacheProblems(Map.of("w.yml", workflow.lines().toList()), secretJobs))
        .containsExactly("w.yml build: setup-gradle without cache-disabled");
    assertThat(secretJobs).containsEntry("w.yml", List.of("build"));
  }

  @Test
  void noWorkflowHandsItsSecretsToAnotherOneWhole() throws IOException {
    List<String> inherit = new ArrayList<>();
    workflows()
        .forEach(
            (name, lines) -> {
              for (int i = 0; i < lines.size(); i++) {
                if (lines.get(i).matches("\\s*secrets:\\s*inherit\\s*(#.*)?")) {
                  inherit.add(name + ":" + (i + 1));
                }
              }
            });
    assertThat(inherit).as("pass a called workflow the secrets it needs by name").isEmpty();
  }

  private static List<String> problems(String workflow) {
    return cacheProblems(Map.of("w.yml", workflow.lines().toList()), new TreeMap<>());
  }

  /**
   * What lets a job that holds a release secret read or write a cache: {@code actions/cache} (its
   * restore and save actions included), {@code setup-java} with {@code cache:}, {@code
   * setup-gradle} without {@code cache-disabled: true}. A job holds a release secret when its body
   * names one, or when the workflow names one above its jobs (a workflow-level {@code env:}). The
   * jobs that hold one are added to {@code secretJobs}.
   */
  static List<String> cacheProblems(
      Map<String, List<String>> workflows, Map<String, List<String>> secretJobs) {
    List<String> problems = new ArrayList<>();
    workflows.forEach(
        (name, lines) -> {
          int jobsAt = lines.indexOf("jobs:");
          boolean workflowLevel =
              lines.subList(0, Math.max(jobsAt, 0)).stream()
                  .anyMatch(l -> RELEASE_SECRET.matcher(l).find());
          jobs(lines)
              .forEach(
                  (job, body) -> {
                    if (!workflowLevel
                        && body.stream().noneMatch(l -> RELEASE_SECRET.matcher(l).find())) {
                      return;
                    }
                    secretJobs.computeIfAbsent(name, k -> new ArrayList<>()).add(job);
                    for (int i = 0; i < body.size(); i++) {
                      String line = body.get(i);
                      if (line.contains("uses: actions/cache")) {
                        problems.add(name + " " + job + ": actions/cache");
                      }
                      if (line.contains("uses: actions/setup-java@")
                          && step(body, i).lines().anyMatch(l -> l.trim().startsWith("cache:"))) {
                        problems.add(name + " " + job + ": setup-java with cache:");
                      }
                      if (line.contains("uses: gradle/actions/setup-gradle@")
                          && !step(body, i).contains("cache-disabled: true")) {
                        problems.add(name + " " + job + ": setup-gradle without cache-disabled");
                      }
                    }
                  });
        });
    return problems;
  }

  @Test
  void theWrapperPinsItsDistributionByChecksum() {
    assertThat(RepositoryScripts.read("gradle/wrapper/gradle-wrapper.properties"))
        .containsPattern("(?m)^distributionSha256Sum=[0-9a-f]{64}$");
  }

  /** The workflow files, by file name, as lines. */
  static Map<String, List<String>> workflows() throws IOException {
    Map<String, List<String>> workflows = new TreeMap<>();
    try (Stream<Path> files =
        Files.list(RepositoryScripts.repositoryRoot().resolve(".github/workflows"))) {
      for (Path file : files.filter(f -> f.toString().endsWith(".yml")).sorted().toList()) {
        workflows.put(file.getFileName().toString(), Files.readAllLines(file));
      }
    }
    assertThat(workflows).isNotEmpty();
    return workflows;
  }

  /** The jobs of a workflow, by key, each as its lines, in file order. */
  static Map<String, List<String>> jobs(List<String> lines) {
    Map<String, List<String>> jobs = new LinkedHashMap<>();
    int start = lines.indexOf("jobs:");
    List<String> current = null;
    for (int i = start + 1; start >= 0 && i < lines.size(); i++) {
      String line = lines.get(i);
      Matcher job = JOB.matcher(line);
      if (job.matches()) {
        current = new ArrayList<>();
        jobs.put(job.group(1), current);
      } else if (!line.isEmpty() && !line.startsWith(" ") && !line.startsWith("#")) {
        break;
      } else if (current != null) {
        current.add(line);
      }
    }
    return jobs;
  }

  /** The text of the step whose {@code uses:} line is {@code body[index]}. */
  private static String step(List<String> body, int index) {
    String first = body.get(index);
    int indent = first.indexOf("uses:");
    StringBuilder step = new StringBuilder(first).append('\n');
    for (int i = index + 1; i < body.size(); i++) {
      String line = body.get(i);
      if (!line.isBlank() && (line.length() - line.stripLeading().length()) < indent) {
        break;
      }
      step.append(line).append('\n');
    }
    return step.toString();
  }
}
