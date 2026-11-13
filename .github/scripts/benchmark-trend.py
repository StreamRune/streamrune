"""Reads the benchmark history the Benchmarks workflow records (dev/bench/data.js on gh-pages) and
reports a benchmark that has become slower, for benchmark.yml.

usage: python3 benchmark-trend.py <data.js> <benchmark set name>
        [--threshold 2.5] [--window 5] [--min-baseline 3]
        [--sustained-threshold 1.5] [--sustained-points 3] [--min-series 5]

Shared GitHub runners are not one machine. On unchanged code, two runs that drew the same kind of
machine agree within about 6%, but runs that drew different machines differ by up to 2.1 times, on
the CPU-only series (StreamRuneBenchmark.eventStoreLoad, batchSize=1: 0.131 and 0.064 us/op) as on
the PostgreSQL ones (PostgresEventStoreBenchmark.load, streamLength=10: 617 and 293 us/op). Every
series moves with the machine, though: divided by the median change of all the series of the same
run, the change of each series on unchanged code stayed between 0.81 and 1.33 in every comparison
of the recorded runs. A comparison with the previous run alone reports the machine, not the code.

Each benchmark of the newest recorded point has a baseline, the median of its previous --window
points (at least --min-baseline of them; with fewer, it is not judged yet), so one fast or slow
machine does not become the baseline. Its ratio is its time over that baseline, and its ratio to
the run is that ratio over the median ratio of every benchmark judged in the same point (the
machine's share; with fewer than --min-series benchmarks judged, there is none). Two rules report
it:

  - a large regression: its ratio exceeds --threshold (2.5, above the 2.1 measured between
    machines) in the newest point and in the point before, each against its own baseline: one
    extreme machine raises no alert, a large slowdown that stays does, even one that slows every
    benchmark alike;
  - a sustained regression: its ratio to the run exceeds --sustained-threshold (1.5, above the 1.33
    measured on unchanged code) in each of its last --sustained-points points (3): a smaller
    slowdown of some benchmarks that stays for three runs, whatever machines they drew. A slowdown
    of nearly every benchmark alike is the machine's share here, and only the first rule sees it.

It prints a table of every judged benchmark, writes it to the job summary when GITHUB_STEP_SUMMARY
is set, emits a ::warning:: for each regression and exits 1 when there is one, 0 otherwise. A
slowdown below both rules is a trend to read on the chart, over several points.
"""

import argparse
import json
import os
import statistics
import sys


def load(path, name):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    data = json.loads(text[text.index("{"):].rstrip().rstrip(";"))
    return data.get("entries", {}).get(name, [])


def series(entries):
    """Benchmark name -> [(entry index, value)], in the order the points were recorded."""
    out = {}
    for index, entry in enumerate(entries):
        for bench in entry.get("benches", []):
            out.setdefault(bench["name"], []).append((index, float(bench["value"])))
    return out


def ratio(values, i, window, min_baseline):
    """(values[i] over the median of the values before it, that median), or (None, None)."""
    if i < 0:
        return None, None
    earlier = values[max(0, i - window):i]
    if len(earlier) < min_baseline:
        return None, None
    baseline = statistics.median(earlier)
    if baseline <= 0:
        return None, None
    return values[i] / baseline, baseline


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("data")
    parser.add_argument("name")
    parser.add_argument("--threshold", type=float, default=2.5)
    parser.add_argument("--window", type=int, default=5)
    parser.add_argument("--min-baseline", type=int, default=3)
    parser.add_argument("--sustained-threshold", type=float, default=1.5)
    parser.add_argument("--sustained-points", type=int, default=3)
    parser.add_argument("--min-series", type=int, default=5)
    args = parser.parse_args(argv)

    entries = load(args.data, args.name)
    if not entries:
        print(f"No recorded point of '{args.name}' in {args.data}; nothing to judge.")
        return 0
    newest = len(entries) - 1
    commit = entries[newest].get("commit", {}).get("id", "?")[:7]
    by_name = series(entries)

    # ratios[name][entry index] = the benchmark's ratio to its baseline in that point.
    ratios = {}
    for bench, points in by_name.items():
        values = [value for _, value in points]
        ratios[bench] = {}
        for i, (index, _) in enumerate(points):
            r, _ = ratio(values, i, args.window, args.min_baseline)
            if r is not None:
                ratios[bench][index] = r
    # The machine's share of each point: the median ratio of the benchmarks judged in it.
    machine = {}
    for index in range(len(entries)):
        judged = [r[index] for r in ratios.values() if index in r]
        if len(judged) >= args.min_series:
            machine[index] = statistics.median(judged)

    rows = []
    regressions = []
    for bench, points in sorted(by_name.items()):
        if points[-1][0] != newest or newest not in ratios[bench]:
            continue  # not measured, or not judged yet, in the newest point
        values = [value for _, value in points]
        current, baseline = ratio(values, len(values) - 1, args.window, args.min_baseline)
        before, _ = ratio(values, len(values) - 2, args.window, args.min_baseline)
        to_run = current / machine[newest] if newest in machine else None
        large = current > args.threshold and before is not None and before > args.threshold
        last = [index for index, _ in points[-args.sustained_points:]]
        sustained = len(last) == args.sustained_points and all(
            index in ratios[bench]
            and index in machine
            and ratios[bench][index] / machine[index] > args.sustained_threshold
            for index in last
        )
        if large:
            status = "regression"
            regressions.append(
                f"{bench} takes {current:.2f}x its baseline in the newest point and "
                f"{before:.2f}x in the one before"
            )
        elif sustained:
            status = "sustained regression"
            regressions.append(
                f"{bench} takes more than {args.sustained_threshold:g}x its baseline, relative to "
                f"the other benchmarks of the run, in each of its last {args.sustained_points} points"
            )
        elif current > args.threshold or (to_run is not None and to_run > args.sustained_threshold):
            status = "over once"
        else:
            status = ""
        rows.append((bench, values[-1], baseline, current, to_run, status))

    if not rows:
        print(f"Fewer than {args.min_baseline + 1} recorded points per benchmark; nothing to judge yet.")
        return 0
    lines = [
        f"### Benchmarks of {commit} against the median of their previous {args.window} points",
        "",
        f"A regression: over {args.threshold:g}x in the two newest points, or over "
        f"{args.sustained_threshold:g}x relative to the run in the last {args.sustained_points}.",
        "",
        "| benchmark | newest | baseline | ratio | to the run | |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for bench, value, baseline, current, to_run, status in rows:
        run_text = f"{to_run:.2f}" if to_run is not None else "-"
        lines.append(f"| {bench} | {value:.4g} | {baseline:.4g} | {current:.2f} | {run_text} | {status} |")
    report = "\n".join(lines)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(report + "\n")
    for text in regressions:
        print(f"::warning::{text}; confirm it locally on both commits.")
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
