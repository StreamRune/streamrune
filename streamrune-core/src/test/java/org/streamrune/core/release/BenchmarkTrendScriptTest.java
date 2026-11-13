package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code .github/scripts/benchmark-trend.py}, which judges the newest point of the benchmark
 * history: a change of runner must raise no alert, a slowdown that stays must.
 *
 * <p>The histories mirror the nightly runs on unchanged code: every series moves with the machine
 * the run drew, by up to 2.1 times between machines.
 */
class BenchmarkTrendScriptTest {

  private static final String SCRIPT = ".github/scripts/benchmark-trend.py";

  private static final String NAME = "StreamRune JMH";

  @TempDir Path dir;

  @Test
  void aChangeOfMachineOnUnchangedCodeRaisesNoAlert() throws IOException {
    // The first recorded runs: one machine twice, a faster one, a slower one, then two of the
    // slowest kind in a row, 2.1 times the fastest.
    double[] machine = {1.0, 1.0, 0.48, 0.63, 1.0, 1.0};
    RepositoryScripts.Result result = judge(history(machine, Map.of()));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).doesNotContain("::warning::").doesNotContain("regression |");
  }

  @Test
  void aSlowdownInTheNewestPointAloneIsReportedButRaisesNoAlert() throws IOException {
    double[] machine = {1.0, 1.0, 1.0, 1.0, 1.0};
    RepositoryScripts.Result result =
        judge(history(machine, Map.of("load", new double[] {1, 1, 1, 1, 3})));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output())
        .contains("| load |")
        .contains("over once")
        .doesNotContain("::warning::");
  }

  @Test
  void aSlowdownThatStaysForTwoPointsRaisesTheAlert() throws IOException {
    double[] machine = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
    RepositoryScripts.Result result =
        judge(history(machine, Map.of("load", new double[] {1, 1, 1, 1, 3, 3.2})));

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output())
        .contains("::warning::load takes 3.20x its baseline in the newest point and 3.00x")
        .doesNotContain("::warning::append");
  }

  @Test
  void theBaselineIsTheMedianSoOneFastMachineDoesNotMakeASlowOneLookLikeARegression()
      throws IOException {
    // Against the previous point alone the newest would be 2.1/0.48 = 4.4 times slower.
    double[] machine = {1.0, 1.0, 1.0, 0.48, 1.0, 1.0};
    RepositoryScripts.Result result = judge(history(machine, Map.of()));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output()).doesNotContain("over once");
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
    Map<String, double[]> series = new LinkedHashMap<>();
    series.put("load", new double[] {1, 1, 1, 3, 3, 7});
    String data = history(new double[] {1, 1, 1, 1, 1, 1}, series);
    data = data.replace(",{\"name\":\"load\",\"value\":7.0,\"unit\":\"us/op\"}", "");
    assertThat(data).doesNotContain("7.0");

    RepositoryScripts.Result result = judge(data);

    assertThat(result.output()).doesNotContain("| load |");
  }

  /**
   * A data.js of {@code machine.length} points with the series {@code append} and {@code load},
   * each its machine factor times the code's factor ({@code code}, 1 where absent).
   */
  private static String history(double[] machine, Map<String, double[]> code) {
    List<String> entries = new ArrayList<>();
    for (int i = 0; i < machine.length; i++) {
      List<String> benches = new ArrayList<>();
      for (String name : List.of("append", "load")) {
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
