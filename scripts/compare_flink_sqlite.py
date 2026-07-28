#!/usr/bin/env python3
"""Compare exact Flink Q3 groups and emitted Top-10 snapshots with SQLite."""

import argparse
import csv
import re
import sqlite3
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from verify_q3_sqlite import (
    Q3_ALL_GROUPS_SQL,
    apply_update,
    create_schema,
    read_updates,
)

GroupKey = Tuple[int, str, int]
State = Dict[GroupKey, int]
TopRow = Tuple[GroupKey, int]

GROUP_PATTERN = re.compile(
    r"(?:^|\s)GROUP\|(\d+)\|(UPSERT_GROUP|DELETE_GROUP)\|(\d+)\|"
    r"(\d{4}-\d{2}-\d{2})\|(\d+)\|(-?\d+(?:\.\d+)?)\|"
    r"(-?\d+(?:\.\d+)?)\|([^\r\n]+)$"
)
TOP_PATTERN = re.compile(
    r"(?:^|\s)TOP10\|(\d+)\|(\d+)\|(\d+)\|"
    r"(\d{4}-\d{2}-\d{2})\|(\d+)\|(-?\d+(?:\.\d+)?)$"
)
TOP_EMPTY_PATTERN = re.compile(r"(?:^|\s)TOP10_EMPTY\|(\d+)$")


def revenue_units(value: str) -> int:
    return int((Decimal(value) * 10_000).to_integral_exact())


def snapshot_targets(update_count: int, every: int) -> List[int]:
    targets = list(range(every, update_count + 1, every))
    if update_count and (not targets or targets[-1] != update_count):
        targets.append(update_count)
    return targets


def top_ten(state: State) -> List[TopRow]:
    return sorted(
        state.items(),
        key=lambda item: (
            -item[1],
            item[0][1],
            item[0][0],
            item[0][2],
        ),
    )[:10]


def sqlite_snapshots(
    updates: List[List[str]], targets: Iterable[int]
) -> Dict[int, State]:
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    target_set = set(targets)
    snapshots: Dict[int, State] = {}
    for sequence, update in enumerate(updates, start=1):
        apply_update(conn, update)
        if sequence in target_set:
            state: State = {}
            for orderkey, revenue, orderdate, shippriority in conn.execute(
                Q3_ALL_GROUPS_SQL
            ):
                state[(int(orderkey), orderdate, int(shippriority))] = int(revenue)
            snapshots[sequence] = state
    return snapshots


def run_flink(
    project_root: Path,
    jar: Path,
    updates: Path,
    java: str,
    parallelism: int,
) -> Tuple[List[Tuple[int, GroupKey, int]], Dict[int, List[TopRow]]]:
    command = [
        java,
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "-jar",
        str(jar),
        str(updates),
        str(parallelism),
        "print",
    ]
    completed = subprocess.run(
        command,
        cwd=project_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise RuntimeError(f"Flink job failed with exit code {completed.returncode}")

    deltas: List[Tuple[int, GroupKey, int]] = []
    emitted_top: Dict[int, List[TopRow]] = {}
    for line in completed.stdout.splitlines():
        text = line.strip()
        group_match = GROUP_PATTERN.search(text)
        if group_match:
            sequence = int(group_match.group(1))
            key = (
                int(group_match.group(3)),
                group_match.group(4),
                int(group_match.group(5)),
            )
            deltas.append((sequence, key, revenue_units(group_match.group(7))))
            continue
        top_match = TOP_PATTERN.search(text)
        if top_match:
            sequence = int(top_match.group(1))
            rank = int(top_match.group(2))
            key = (
                int(top_match.group(3)),
                top_match.group(4),
                int(top_match.group(5)),
            )
            rows = emitted_top.setdefault(sequence, [])
            if rank != len(rows) + 1:
                raise RuntimeError(f"non-consecutive Top-10 rank at sequence {sequence}")
            rows.append((key, revenue_units(top_match.group(6))))
            continue
        empty_match = TOP_EMPTY_PATTERN.search(text)
        if empty_match:
            emitted_top[int(empty_match.group(1))] = []

    deltas.sort(key=lambda item: item[0])
    return deltas, emitted_top


def compare_states(expected: State, actual: State) -> List[str]:
    differences = []
    for key in sorted(expected.keys() | actual.keys()):
        expected_value = expected.get(key)
        actual_value = actual.get(key)
        if expected_value is None:
            differences.append(f"unexpected group {key}: {actual_value}")
        elif actual_value is None:
            differences.append(f"missing group {key}: expected {expected_value}")
        elif expected_value != actual_value:
            differences.append(
                f"group {key}: expected {expected_value}, actual {actual_value}"
            )
    return differences


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Compare exact Flink Q3 state and Top-10 with SQLite."
    )
    parser.add_argument("updates", type=Path)
    parser.add_argument(
        "--jar",
        type=Path,
        default=project_root
        / "target"
        / "flink-continuous-tpch-q3-1.0.0.jar",
    )
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--snapshot-every", type=int, default=1000)
    parser.add_argument("--java", default="java")
    parser.add_argument(
        "--output",
        type=Path,
        default=project_root / "results" / "correctness_comparison.csv",
    )
    args = parser.parse_args()

    if args.snapshot_every < 1:
        parser.error("--snapshot-every must be at least 1")
    if not args.updates.is_file():
        parser.error(f"update stream does not exist: {args.updates}")
    if not args.jar.is_file():
        parser.error(f"shaded jar does not exist: {args.jar}")

    updates = list(read_updates(args.updates))
    requested_targets = snapshot_targets(len(updates), args.snapshot_every)
    deltas, emitted_top = run_flink(
        project_root,
        args.jar.resolve(),
        args.updates.resolve(),
        args.java,
        args.parallelism,
    )
    all_targets = sorted(set(requested_targets) | set(emitted_top))
    expected = sqlite_snapshots(updates, all_targets)
    top10_validation = (
        "PASS"
        if all(
            emitted_top[target] == top_ten(expected[target])
            for target in emitted_top
        )
        else "FAIL"
    )

    actual: State = {}
    delta_index = 0
    rows = []
    failures = []
    for target in all_targets:
        while delta_index < len(deltas) and deltas[delta_index][0] <= target:
            _, key, current_revenue = deltas[delta_index]
            if current_revenue == 0:
                actual.pop(key, None)
            else:
                actual[key] = current_revenue
            delta_index += 1

        state_differences = compare_states(expected[target], actual)
        top_status = "NOT_EMITTED"
        if target in emitted_top:
            top_status = (
                "PASS"
                if emitted_top[target] == top_ten(expected[target])
                else "FAIL"
            )
            if top_status == "FAIL":
                failures.append((target, ["emitted Top-10 differs from SQLite"]))

        if target in requested_targets:
            rows.append(
                {
                    "snapshot": target,
                    "sqlite_groups": len(expected[target]),
                    "flink_groups": len(actual),
                    "group_mismatches": len(state_differences),
                    "top10_emitted_at_snapshot": top_status,
                    "emitted_top10_changes_verified": len(emitted_top),
                    "top10_validation": top10_validation,
                    "status": "PASS"
                    if not state_differences and top_status != "FAIL"
                    else "FAIL",
                }
            )
            if state_differences:
                failures.append((target, state_differences[:10]))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "snapshot",
            "sqlite_groups",
            "flink_groups",
            "group_mismatches",
            "top10_emitted_at_snapshot",
            "emitted_top10_changes_verified",
            "top10_validation",
            "status",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    if failures:
        for snapshot, differences in failures:
            print(f"snapshot {snapshot}: FAIL")
            for difference in differences:
                print(f"  {difference}")
        raise SystemExit(1)

    print(
        f"PASS: {len(requested_targets)} exact group snapshots, "
        f"{len(emitted_top)} emitted Top-10 changes, {len(updates)} updates, "
        f"parallelism {args.parallelism}; results: {args.output}"
    )


if __name__ == "__main__":
    main()
