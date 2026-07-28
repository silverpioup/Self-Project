#!/usr/bin/env python3
"""Run repeatable Q3 experiments and validate active Flink workers."""

import argparse
import csv
import platform
import re
import statistics
import subprocess
import time
from pathlib import Path

METRICS = re.compile(
    r"^METRICS\|(\d+)\|([\d.]+)\|(\d+)\|(\d+)\|(\d+)$"
)
PROCESSING = re.compile(r"^PROCESSING\|(\d+)\|([\d.]+)\|([\d.]+)$")
WORKERS = re.compile(r"^WORKERS\|(\d+)\|(\d+)(?:\|.*)?$")


def run_once(
    java: str,
    jar: Path,
    updates: Path,
    parallelism: int,
    max_heap: str,
) -> dict:
    command = [
        java,
        f"-Xmx{max_heap}",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "-jar",
        str(jar),
        str(updates),
        str(parallelism),
        "metrics",
    ]
    started = time.perf_counter()
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    wall_seconds = time.perf_counter() - started
    if completed.returncode:
        raise RuntimeError(completed.stdout + completed.stderr)

    parsed = {}
    for line in completed.stdout.splitlines():
        text = line.strip()
        match = METRICS.match(text)
        if match:
            parsed.update(
                updates=int(match.group(1)),
                mean_latency_us=float(match.group(2)),
                p50_latency_us=int(match.group(3)),
                p95_latency_us=int(match.group(4)),
                p99_latency_us=int(match.group(5)),
            )
            continue
        match = PROCESSING.match(text)
        if match:
            parsed.update(
                processing_seconds=float(match.group(2)),
                processing_throughput=float(match.group(3)),
            )
            continue
        match = WORKERS.match(text)
        if match:
            parsed.update(
                active_workers=int(match.group(1)),
                configured_workers=int(match.group(2)),
            )
    required = {
        "updates",
        "mean_latency_us",
        "p50_latency_us",
        "p95_latency_us",
        "p99_latency_us",
        "processing_seconds",
        "processing_throughput",
        "active_workers",
        "configured_workers",
    }
    missing = required - parsed.keys()
    if missing:
        raise RuntimeError(f"missing job metrics: {sorted(missing)}")
    if parsed["active_workers"] != parallelism:
        raise RuntimeError(
            f"parallelism {parallelism} used only "
            f"{parsed['active_workers']} active workers"
        )
    parsed["wall_clock_seconds"] = wall_seconds
    parsed["wall_clock_throughput"] = parsed["updates"] / wall_seconds
    return parsed


def mean(rows: list[dict], field: str) -> float:
    return statistics.fmean(row[field] for row in rows)


def standard_deviation(rows: list[dict], field: str) -> float:
    values = [row[field] for row in rows]
    return statistics.stdev(values) if len(values) > 1 else 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--java", required=True)
    parser.add_argument("--max-heap", default="4g")
    parser.add_argument("--parallelism", default="1,2,4,8")
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--raw-output", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, required=True)
    parser.add_argument("--environment-output", type=Path)
    args = parser.parse_args()

    levels = [int(value) for value in args.parallelism.split(",")]
    if args.repetitions < 1 or args.warmup_runs < 0:
        parser.error("repetitions must be positive and warmup-runs non-negative")

    raw_rows = []
    for parallelism in levels:
        for _ in range(args.warmup_runs):
            run_once(
                args.java, args.jar, args.input, parallelism, args.max_heap
            )
        for repetition in range(1, args.repetitions + 1):
            row = run_once(
                args.java, args.jar, args.input, parallelism, args.max_heap
            )
            row.update(
                dataset=args.dataset,
                parallelism=parallelism,
                repetition=repetition,
            )
            raw_rows.append(row)
            print(
                f"{args.dataset} p={parallelism} run={repetition}: "
                f"{row['processing_throughput']:.2f} updates/s, "
                f"active workers={row['active_workers']}"
            )

    raw_fields = [
        "dataset",
        "parallelism",
        "repetition",
        "updates",
        "active_workers",
        "configured_workers",
        "processing_seconds",
        "processing_throughput",
        "wall_clock_seconds",
        "wall_clock_throughput",
        "mean_latency_us",
        "p50_latency_us",
        "p95_latency_us",
        "p99_latency_us",
    ]
    args.raw_output.parent.mkdir(parents=True, exist_ok=True)
    with args.raw_output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=raw_fields)
        writer.writeheader()
        writer.writerows(raw_rows)

    summary_rows = []
    for parallelism in levels:
        rows = [row for row in raw_rows if row["parallelism"] == parallelism]
        summary_rows.append(
            {
                "dataset": args.dataset,
                "parallelism": parallelism,
                "repetitions": len(rows),
                "updates": rows[0]["updates"],
                "active_workers": rows[0]["active_workers"],
                "mean_processing_throughput": round(
                    mean(rows, "processing_throughput"), 2
                ),
                "stdev_processing_throughput": round(
                    standard_deviation(rows, "processing_throughput"), 2
                ),
                "mean_wall_clock_throughput": round(
                    mean(rows, "wall_clock_throughput"), 2
                ),
                "mean_latency_us": round(mean(rows, "mean_latency_us"), 2),
                "mean_p50_latency_us": round(mean(rows, "p50_latency_us"), 2),
                "mean_p95_latency_us": round(mean(rows, "p95_latency_us"), 2),
                "mean_p99_latency_us": round(mean(rows, "p99_latency_us"), 2),
            }
        )
    summary_fields = list(summary_rows[0])
    with args.summary_output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summary_rows)

    if args.environment_output:
        args.environment_output.write_text(
            "\n".join(
                [
                    f"platform={platform.platform()}",
                    f"processor={platform.processor()}",
                    f"logical_cpu_count={__import__('os').cpu_count()}",
                    f"python={platform.python_version()}",
                    f"java={args.java}",
                    f"warmup_runs={args.warmup_runs}",
                    f"repetitions={args.repetitions}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
