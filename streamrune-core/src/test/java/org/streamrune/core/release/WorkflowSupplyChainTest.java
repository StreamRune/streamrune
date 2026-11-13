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
 * and every workflow on {@code main} writes such entries. The wrapper pins its distribution by
 * checksum, since those jobs download it.
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
    List<String> cached = new ArrayList<>();
    workflows()
        .forEach(
            (name, lines) ->
                jobs(lines)
                    .forEach(
                        (job, body) -> {
                          if (body.stream().noneMatch(l -> RELEASE_SECRET.matcher(l).find())) {
                            return;
                          }
                          secretJobs.computeIfAbsent(name, k -> new ArrayList<>()).add(job);
                          for (int i = 0; i < body.size(); i++) {
                            String line = body.get(i);
                            if (line.contains("uses: actions/cache")) {
                              cached.add(name + " " + job + ": actions/cache");
                            }
                            if (line.contains("uses: gradle/actions/setup-gradle@")
                                && !step(body, i).contains("cache-disabled: true")) {
                              cached.add(
                                  name + " " + job + ": setup-gradle without cache-disabled");
                            }
                          }
                        }));
    // The rule holds for the jobs it is meant for: a parser that found none would pass vacuously.
    assertThat(secretJobs)
        .containsEntry("publish-snapshot.yml", List.of("publish-snapshot"))
        .containsEntry("release.yml", List.of("verify", "rehearse-signing", "publish"));
    assertThat(cached).isEmpty();
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
