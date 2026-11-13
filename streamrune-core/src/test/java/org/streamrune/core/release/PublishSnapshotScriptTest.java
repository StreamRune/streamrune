package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code .github/scripts/publish-snapshot.sh}, the upload of the Publish Snapshot workflow, run
 * against a stand-in {@code gradlew}, {@code git} and {@code sleep}: nothing is uploaded and no
 * test waits.
 *
 * <p>The stand-in {@code gradlew} lists the modules of {@code FAKE_MODULES} in its dry run and
 * answers each module upload with the next line of {@code outcomes}: {@code ok}, {@code 429} (the
 * snapshot repository's rate limit, in the words Gradle prints), {@code 503}, {@code net} (a read
 * timeout), {@code dns} or {@code dnsmac} (a host that did not resolve, in Gradle's words on Linux
 * and on macOS) or {@code fail} (a failure that is neither the server's nor the network's); with no
 * line left, the upload succeeds. The stand-in {@code git} fails its first {@code
 * FAKE_LS_REMOTE_FAIL} reads of the tags. Every upload is appended to {@code uploads}, every wait
 * to {@code sleeps}.
 */
class PublishSnapshotScriptTest {

  private static final String SCRIPT = ".github/scripts/publish-snapshot.sh";

  @TempDir Path dir;

  private final Map<String, String> env = new HashMap<>();

  @BeforeEach
  void standIns() throws IOException {
    Path gradlew =
        RepositoryScripts.executable(
            dir,
            "gradlew",
            """
            cd "$FAKE_DIR"
            case "$*" in
              *':streamrune-core:properties'*) echo "version: $FAKE_VERSION"; exit 0 ;;
              *'--dry-run'*)
                echo "Calculating task graph"
                for m in $FAKE_MODULES; do
                  echo "$m:publishMavenPublicationToMavenCentralRepository SKIPPED"
                  echo "$m:publishToMavenCentral SKIPPED"
                done
                exit "${FAKE_DRY_RUN_EXIT:-0}" ;;
            esac
            module=${1%:publishToMavenCentral}
            echo "$module" >> uploads
            outcome=$(head -n 1 outcomes 2> /dev/null)
            [ -f outcomes ] && tail -n +2 outcomes > outcomes.next && mv outcomes.next outcomes
            case "$outcome" in
              429) echo "> Could not PUT 'https://central.sonatype.com/x.jar'. Received status code 429 from server: Too Many Requests"; exit 1 ;;
              503) echo "> Could not GET 'https://central.sonatype.com/x.xml'. Received status code 503 from server: Service Unavailable"; exit 1 ;;
              net) echo "> Could not PUT 'https://central.sonatype.com/x.jar'."; echo "   > Read timed out"; exit 1 ;;
              dns) echo "> Could not GET 'https://central.sonatype.com/x.xml'."; echo "   > central.sonatype.com: Temporary failure in name resolution"; exit 1 ;;
              dnsmac) echo "> Could not GET 'https://central.sonatype.com/x.xml'."; echo "   > central.sonatype.com: nodename nor servname provided, or not known"; exit 1 ;;
              fail) echo "> Compilation failed; see the compiler error output for details."; exit 1 ;;
            esac
            exit 0
            """);
    Path git =
        RepositoryScripts.executable(
            dir,
            "git",
            """
            case "$1" in
              rev-parse) echo 0123456789abcdef0123456789abcdef01234567 ;;
              ls-remote)
                failed=$(cat "$FAKE_DIR/ls-remote-failures" 2> /dev/null || echo 0)
                if [ "$failed" -lt "${FAKE_LS_REMOTE_FAIL:-0}" ]; then
                  echo $((failed + 1)) > "$FAKE_DIR/ls-remote-failures"; exit 128
                fi
                cat "$FAKE_DIR/tags" 2> /dev/null || true ;;
            esac
            """);
    Path sleep =
        RepositoryScripts.executable(dir, "sleep", "echo \"$1\" >> \"$FAKE_DIR/sleeps\"\n");
    env.put("GRADLEW", gradlew.toString());
    env.put("GIT", git.toString());
    env.put("SLEEP", sleep.toString());
    env.put("FAKE_DIR", dir.toString());
    env.put("FAKE_VERSION", "1.0.1-SNAPSHOT");
    env.put("FAKE_MODULES", ":core :runtime :spring :starter");
    env.put("SNAPSHOT_RETRY_DELAYS", "600 900 1200 1800");
    env.put("SNAPSHOT_NETWORK_DELAYS", "90 180");
    env.put("SNAPSHOT_WAIT_BUDGET", "5400");
  }

  @Test
  void everyModuleIsUploadedOnceWhenNothingIsRefused() throws IOException {
    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("uploads")).containsExactly(":core", ":runtime", ":spring", ":starter");
    assertThat(lines("sleeps")).isEmpty();
    assertThat(result.output()).contains("Published all 4 modules of 1.0.1-SNAPSHOT");
  }

  @Test
  void aRateLimitedUploadIsRepeatedForTheModuleItStoppedAtNotFromTheFirst() throws IOException {
    outcomes("ok", "ok", "429");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("uploads"))
        .containsExactly(":core", ":runtime", ":spring", ":spring", ":starter");
    assertThat(lines("sleeps")).containsExactly("600");
  }

  @Test
  void theWaitGrowsWithEveryRefusalInARowAndStartsAgainAfterAnUpload() throws IOException {
    outcomes("429", "429", "ok", "ok", "429", "ok");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("uploads"))
        .containsExactly(":core", ":core", ":core", ":runtime", ":spring", ":spring", ":starter");
    assertThat(lines("sleeps")).containsExactly("600", "900", "600");
  }

  @Test
  void aServerErrorIsRepeatedLikeARateLimit() throws IOException {
    outcomes("503");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("uploads"))
        .containsExactly(":core", ":core", ":runtime", ":spring", ":starter");
    assertThat(lines("sleeps")).containsExactly("600");
  }

  @Test
  void theRunGivesUpWhenTheWaitsInARowRunOutAndNamesTheModulesLeftOnAnEarlierCommit()
      throws IOException {
    env.put("SNAPSHOT_RETRY_DELAYS", "600 900");
    outcomes("ok", "ok", "429", "429", "429");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads"))
        .containsExactly(":core", ":runtime", ":spring", ":spring", ":spring");
    assertThat(lines("sleeps")).containsExactly("600", "900");
    assertThat(result.output())
        .contains("refused :spring 3 times in a row")
        .contains("Published 2 of 4 modules")
        .contains(":spring :starter");
  }

  @Test
  void theRunGivesUpWhenTheNextWaitWouldExceedItsBudget() throws IOException {
    env.put("SNAPSHOT_WAIT_BUDGET", "1000");
    outcomes("429", "429");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).containsExactly(":core", ":core");
    assertThat(lines("sleeps")).containsExactly("600");
    assertThat(result.output()).contains("has waited 600 of its 1000 seconds");
  }

  @Test
  void aFailureThatIsNotTheServersIsNotRepeated() throws IOException {
    outcomes("ok", "fail");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).containsExactly(":core", ":runtime");
    assertThat(lines("sleeps")).isEmpty();
    assertThat(result.output())
        .contains(
            "Publishing :runtime failed, neither on the snapshot repository's rate limit nor");
  }

  @Test
  void aDevelopmentVersionBelowATaggedReleaseIsRefusedBeforeAnyUpload() throws IOException {
    env.put("FAKE_VERSION", "1.0.0-alpha-SNAPSHOT");
    tags("v0.9.0", "v1.0.0");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).isEmpty();
    assertThat(result.output())
        .contains("1.0.0-alpha-SNAPSHOT sorts below 1.0.0 (tag v1.0.0)")
        .contains("Move the default version");
  }

  @Test
  void aDevelopmentVersionBelowAHigherReleaseIsRefusedToo() throws IOException {
    env.put("FAKE_VERSION", "1.0.1-SNAPSHOT");
    tags("v1.1.0");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).isEmpty();
  }

  @Test
  void aDevelopmentVersionAboveEveryReleaseIsPublished() throws IOException {
    env.put("FAKE_VERSION", "1.1.0-SNAPSHOT");
    tags("v1.0.0", "v1.0.1", "v1.1.0-rc.1");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("uploads")).hasSize(4);
  }

  @Test
  void aVersionThatIsNotASnapshotIsRefused() throws IOException {
    env.put("FAKE_VERSION", "1.0.0");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).isEmpty();
    assertThat(result.output()).contains("is not a -SNAPSHOT version");
  }

  @Test
  void aNetworkFailureIsRepeatedAfterAShortWait() throws IOException {
    outcomes("ok", "net", "net");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("uploads"))
        .containsExactly(":core", ":runtime", ":runtime", ":runtime", ":spring", ":starter");
    assertThat(lines("sleeps")).containsExactly("90", "180");
  }

  @Test
  void aHostThatDidNotResolveIsRepeatedLikeANetworkFailure() throws IOException {
    outcomes("dns", "dnsmac");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("uploads"))
        .containsExactly(":core", ":core", ":core", ":runtime", ":spring", ":starter");
    assertThat(lines("sleeps")).containsExactly("90", "180");
  }

  @Test
  void theRunGivesUpWhenTheNetworkKeepsBreakingTheSameUpload() throws IOException {
    outcomes("net", "net", "net");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).containsExactly(":core", ":core", ":core");
    assertThat(lines("sleeps")).containsExactly("90", "180");
    assertThat(result.output()).contains("The network broke the upload of :core 3 times in a row");
  }

  @Test
  void aFailedReadOfTheTagsIsTriedAgain() throws IOException {
    env.put("FAKE_LS_REMOTE_FAIL", "2");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(lines("sleeps")).containsExactly("30", "30");
    assertThat(lines("uploads")).hasSize(4);
  }

  @Test
  void tagsUnreadableThreeTimesRefuseTheRun() throws IOException {
    env.put("FAKE_LS_REMOTE_FAIL", "3");

    RepositoryScripts.Result result = run();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).isEmpty();
    assertThat(result.output()).contains("Could not read the tags of origin (three attempts)");
  }

  @Test
  void aPreReleaseTagOfTheSameVersionRefusesAPreviewItsQualifierDoesNotPass() throws IOException {
    env.put("FAKE_VERSION", "1.0.0-alpha-SNAPSHOT");
    tags("v1.0.0-rc.1");
    RepositoryScripts.Result alpha = run();
    assertThat(alpha.exitCode()).isEqualTo(1);
    assertThat(alpha.output()).contains("sorts below 1.0.0-rc.1");

    env.put("FAKE_VERSION", "1.0.0-rc.1-SNAPSHOT");
    assertThat(run().exitCode()).isEqualTo(1);
    assertThat(lines("uploads")).isEmpty();

    env.put("FAKE_VERSION", "1.0.0-rc.2-SNAPSHOT");
    assertThat(run().exitCode()).isZero();
    env.put("FAKE_VERSION", "1.0.0-SNAPSHOT");
    assertThat(run().exitCode()).isZero();
    assertThat(lines("uploads")).hasSize(8);
  }

  @Test
  void aDryRunThatListsNoModuleOrFailsRefusesTheRun() throws IOException {
    env.put("FAKE_MODULES", "");
    assertThat(run().output()).contains("lists no module to publish");

    env.put("FAKE_MODULES", ":core");
    env.put("FAKE_DRY_RUN_EXIT", "1");
    RepositoryScripts.Result failed = run();
    assertThat(failed.exitCode()).isEqualTo(1);
    assertThat(failed.output()).contains("Could not list the modules to publish");
    assertThat(lines("uploads")).isEmpty();
  }

  private RepositoryScripts.Result run() {
    return RepositoryScripts.runScript(SCRIPT, dir, env);
  }

  private void outcomes(String... outcomes) throws IOException {
    Files.write(dir.resolve("outcomes"), List.of(outcomes), StandardCharsets.UTF_8);
  }

  private void tags(String... tags) throws IOException {
    StringBuilder text = new StringBuilder();
    for (String tag : tags) {
      text.append("0123456789abcdef0123456789abcdef01234567\trefs/tags/").append(tag).append('\n');
    }
    Files.writeString(dir.resolve("tags"), text, StandardCharsets.UTF_8);
  }

  private List<String> lines(String file) throws IOException {
    Path path = dir.resolve(file);
    return Files.exists(path) ? Files.readAllLines(path, StandardCharsets.UTF_8) : List.of();
  }
}
