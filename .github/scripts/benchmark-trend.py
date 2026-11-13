"""Reads the benchmark history the Benchmarks workflow records (dev/bench/data.js on gh-pages) and
reports a benchmark that has become much slower, for benchmark.yml.

usage: python3 benchmark-trend.py <data.js> <benchmark set name>
        [--threshold 2.5] [--window 5] [--min-baseline 3]

Shared GitHub runners are not one machine. On unchanged code, two runs that drew the same kind of
machine agree within about 6%, but runs that drew different machines differ by up to 2.1 times, on
the CPU-only series (StreamRuneBenchmark.eventStoreLoad, batchSize=1: 0.131 and 0.064 us/op) as on
the PostgreSQL ones (PostgresEventStoreBenchmark.load, streamLength=10: 617 and 293 us/op), and
every series moves with the machine. A comparison with the previous run alone reports the machine,
not the code. So, for each benchmark of the newest recorded point:

  - its baseline is the median of its previous --window points (at least --min-baseline of them; with
    fewer, nothing is judged yet), so one fast or slow machine does not become the baseline;
  - it is over the threshold when it takes more than --threshold times its baseline: 2.5, above the
    2.1 measured between machines;
  - it is reported as a regression only when the point before it was over the threshold against its
    own baseline as well: one extreme machine does not raise an alert, a slowdown that stays does.

It prints a table of every judged benchmark, writes it to the job summary when GITHUB_STEP_SUMMARY
is set, emits a ::warning:: for each regression and exits 1 when there is one, 0 otherwise. A
smaller slowdown is a trend to read on the chart, over several points.
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
    """values[i] over the median of the values before it, or None while they are too few."""
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
    args = parser.parse_args(argv)

    entries = load(args.data, args.name)
    if not entries:
        print(f"No recorded point of '{args.name}' in {args.data}; nothing to judge.")
        return 0
    newest = len(entries) - 1
    commit = entries[newest].get("commit", {}).get("id", "?")[:7]
    rows = []
    regressions = []
    for bench, points in sorted(series(entries).items()):
        if points[-1][0] != newest:
            continue  # not measured in the newest point
        values = [value for _, value in points]
        current, baseline = ratio(values, len(values) - 1, args.window, args.min_baseline)
        if current is None:
            continue
        before, _ = ratio(values, len(values) - 2, args.window, args.min_baseline)
        over = current > args.threshold
        confirmed = over and before is not None and before > args.threshold
        status = "regression" if confirmed else ("over once" if over else "")
        rows.append((bench, values[-1], baseline, current, status))
        if confirmed:
            regressions.append((bench, current, before))

    if not rows:
        print(f"Fewer than {args.min_baseline + 1} recorded points per benchmark; nothing to judge yet.")
        return 0
    lines = [
        f"### Benchmarks of {commit} against the median of their previous {args.window} points",
        "",
        f"Threshold {args.threshold:g}x; a regression is over it in the two newest points.",
        "",
        "| benchmark | newest | baseline | ratio | |",
        "|---|---:|---:|---:|---|",
    ]
    for bench, value, baseline, current, status in rows:
        lines.append(f"| {bench} | {value:.4g} | {baseline:.4g} | {current:.2f} | {status} |")
    report = "\n".join(lines)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(report + "\n")
    for bench, current, before in regressions:
        print(
            f"::warning::{bench} takes {current:.2f}x its baseline in the newest point "
            f"and {before:.2f}x in the one before; confirm it locally on both commits."
        )
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
