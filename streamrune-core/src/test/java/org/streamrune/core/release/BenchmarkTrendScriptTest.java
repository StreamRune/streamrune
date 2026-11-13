package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code .github/scripts/benchmark-trend.py}, which judges the newest point of the benchmark
 * history: a change of runner must raise no alert, a slowdown that stays must.
 *
 * <p>Each history has eight benchmarks. Every point multiplies all of them by the speed of the
 * machine the run drew ({@code machine}), and a benchmark named in {@code code} by its own factor
 * too. The machine factors mirror the recorded runs on unchanged code: draws up to 2.1 times apart,
 * every benchmark moving with them. Each case is chosen so that the rule it names decides it: the
 * threshold of 2.5, the median baseline, the confirmation by a second point, the sustained rule,
 * its division by the run's median and its three points.
 */
class BenchmarkTrendScriptTest {

  private static final String SCRIPT = ".github/scripts/benchmark-trend.py";

  private static final String NAME = "StreamRune JMH";

  private static final List<String> BENCHMARKS =
      List.of("append", "load", "loadSnapshot", "project", "serialize", "encrypt", "bus", "map");

  @TempDir Path dir;

  @Test
  void twoSlowDrawsAfterABaselineOfFastOnesRaiseNoAlert() throws IOException {
    // 1.0 / 0.48 = 2.08 times the baseline in both newest points: the spread between machines. A
    // threshold of 2.0 or of the comparison's former 1.3 would report it.
    RepositoryScripts.Result result =
        judge(history(new double[] {0.48, 0.48, 0.48, 0.48, 0.48, 1.0, 1.0}, Map.of()));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).doesNotContain("::warning::").doesNotContain("over once");
  }

  @Test
  void theBaselineIsTheMedianSoOneFastDrawBeforeTheNewestIsNoBaseline() throws IOException {
    // Against the point before alone, the newest would be 1.0 / 0.3 = 3.3 times slower.
    RepositoryScripts.Result result =
        judge(history(new double[] {1.0, 1.0, 1.0, 1.0, 0.3, 1.0}, Map.of()));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).doesNotContain("over once").doesNotContain("::warning::");
  }

  @Test
  void aLargeSlowdownInTheNewestPointAloneIsReportedButRaisesNoAlert() throws IOException {
    RepositoryScripts.Result result =
        judge(history(ones(5), Map.of("load", new double[] {1, 1, 1, 1, 3})));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output())
        .contains("| load |")
        .contains("over once")
        .doesNotContain("::warning::");
  }

  @Test
  void aLargeSlowdownThatStaysForTwoPointsRaisesTheAlert() throws IOException {
    RepositoryScripts.Result result =
        judge(history(ones(6), Map.of("load", new double[] {1, 1, 1, 1, 3, 3.2})));

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output())
        .contains("::warning::load takes 3.20x its baseline in the newest point and 3.00x")
        .doesNotContain("::warning::append");
  }

  @Test
  void aLargeSlowdownOfEveryBenchmarkStillRaisesTheAlert() throws IOException {
    RepositoryScripts.Result result = judge(history(new double[] {1, 1, 1, 1, 3, 3}, Map.of()));

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("::warning::load takes 3.00x");
  }

  @Test
  void aSmallerSlowdownOfOneBenchmarkThatStaysForThreePointsRaisesTheAlert() throws IOException {
    // 1.6 times in the last three points while the machine moves by up to 1.7 times between them.
    RepositoryScripts.Result result =
        judge(
            history(
                new double[] {1, 1, 1, 1, 1, 1.2, 0.7, 1.1},
                Map.of("load", new double[] {1, 1, 1, 1, 1, 1.6, 1.6, 1.6})));

    assertThat(result.exitCode()).as(result.output()).isEqualTo(1);
    assertThat(result.output())
        .contains("::warning::load takes more than 1.5x its baseline, relative to the other")
        .contains("sustained regression")
        .doesNotContain("::warning::append");
  }

  @Test
  void aSmallerSlowdownWithOneNoisyPointAmongItsFirstFourIsReported() throws IOException {
    // 1.6 times from point 5 on, its second slow point drawn 10% faster (1.44 relative to the run).
    // Over in three of the last four points, each against a baseline still at the level before.
    RepositoryScripts.Result result =
        judge(history(ones(9), Map.of("load", new double[] {1, 1, 1, 1, 1, 1.6, 1.44, 1.6, 1.6})));

    assertThat(result.exitCode()).as(result.output()).isEqualTo(1);
    assertThat(result.output())
        .contains("::warning::load takes more than 1.5x")
        .contains("in 3 of its last 4");
  }

  @Test
  void aSmallerSlowdownIsStillReportedInItsFifthSlowRun() throws IOException {
    // The baseline, a median of the previous nine points, is the level before the slowdown while
    // at most four of them are slow; a shorter window has moved to the new level by now.
    RepositoryScripts.Result result =
        judge(
            history(
                ones(10), Map.of("load", new double[] {1, 1, 1, 1, 1, 1.6, 1.44, 1.6, 1.6, 1.6})));

    assertThat(result.exitCode()).as(result.output()).isEqualTo(1);
    assertThat(result.output()).contains("::warning::load takes more than 1.5x");
  }

  @Test
  void aSmallerSlowdownForTwoPointsOnlyRaisesNoAlert() throws IOException {
    RepositoryScripts.Result result =
        judge(
            history(
                new double[] {1, 1, 1, 1, 1, 1, 1.2, 0.7},
                Map.of("load", new double[] {1, 1, 1, 1, 1, 1, 1.6, 1.6})));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).contains("over once").doesNotContain("::warning::");
  }

  @Test
  void aSlowerMachineForThreePointsIsTheRunsShareNotARegression() throws IOException {
    // Every benchmark 1.6 times slower: above the sustained threshold unless divided by the run.
    RepositoryScripts.Result result =
        judge(history(new double[] {1, 1, 1, 1, 1, 1.6, 1.6, 1.6}, Map.of()));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).doesNotContain("::warning::").doesNotContain("over once");
  }

  @Test
  void aBenchmarkJustAboveTheMeasuredNoiseRaisesNoAlert() throws IOException {
    // 1.35 times relative to the run in three points, just above the 1.33 measured on unchanged
    // code and below the sustained threshold of 1.5.
    RepositoryScripts.Result result =
        judge(
            history(
                new double[] {1, 1, 1, 1, 1, 0.6, 1.0, 0.8},
                Map.of("load", new double[] {1, 1, 1, 1, 1, 1.35, 1.35, 1.35})));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).doesNotContain("::warning::");
  }

  @Test
  void withTooFewPointsNothingIsJudgedYet() throws IOException {
    RepositoryScripts.Result result =
        judge(history(new double[] {1.0, 1.0, 5.0}, Map.of("load", new double[] {1, 1, 9})));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).contains("nothing to judge yet");
  }

  @Test
  void aBenchmarkMissingFromTheNewestPointIsNotJudged() throws IOException {
    String data = history(ones(6), Map.of("load", new double[] {1, 1, 1, 3, 3, 7}));
    data = data.replace(",{\"name\":\"load\",\"value\":7.0,\"unit\":\"us/op\"}", "");
    assertThat(data).doesNotContain("7.0");

    RepositoryScripts.Result result = judge(data);

    assertThat(result.output()).doesNotContain("| load |");
  }

  private static double[] ones(int points) {
    double[] machine = new double[points];
    java.util.Arrays.fill(machine, 1.0);
    return machine;
  }

  /** A data.js of {@code machine.length} points of {@link #BENCHMARKS}. */
  private static String history(double[] machine, Map<String, double[]> code) {
    List<String> entries = new ArrayList<>();
    for (int i = 0; i < machine.length; i++) {
      List<String> benches = new ArrayList<>();
      for (String name : BENCHMARKS) {
        double factor = code.containsKey(name) ? code.get(name)[i] : 1.0;
        benches.add(
            "{\"name\":\""
                + name
                + "\",\"value\":"
                + (machine[i] * factor)
                + ",\"unit\":\"us/op\"}");
      }
      entries.add(
          "{\"commit\":{\"id\":\"%040d\"},\"date\":%d,\"tool\":\"jmh\",\"benches\":[%s]}"
              .formatted(i, 1_700_000_000_000L + i, String.join(",", benches)));
    }
    return "window.BENCHMARK_DATA = {\"lastUpdate\":1,\"repoUrl\":\"x\",\"entries\":{\""
        + NAME
        + "\":["
        + String.join(",", entries)
        + "]}}\n";
  }

  private RepositoryScripts.Result judge(String data) throws IOException {
    Path file = dir.resolve("data.js");
    Files.writeString(file, data, StandardCharsets.UTF_8);
    return RepositoryScripts.runPython(SCRIPT, dir, file.toString(), NAME);
  }
}
