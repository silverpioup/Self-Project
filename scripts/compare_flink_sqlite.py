#!/usr/bin/env python3
"""Replay one stream in Flink and SQLite and compare complete Q3 snapshots."""

import argparse
import csv
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from verify_q3_sqlite import apply_update, create_schema, read_updates

GroupKey = Tuple[int, str, int]
State = Dict[GroupKey, float]

Q3_ALL_GROUPS_SQL = """
SELECT
  l.l_orderkey,
  SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
  o.o_orderdate,
  o.o_shippriority
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE c.c_mktsegment = 'BUILDING'
  AND o.o_orderdate < '1995-03-15'
  AND l.l_shipdate > '1995-03-15'
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
"""

DELTA_PATTERN = re.compile(
    r"(?:^|\s)(\d+)\|(UPSERT_GROUP|DELETE_GROUP)\|(\d+)\|"
    r"(\d{4}-\d{2}-\d{2})\|(\d+)\|(-?\d+(?:\.\d+)?)\|"
    r"(-?\d+(?:\.\d+)?)\|([^\r\n]+)$"
)


def snapshot_targets(update_count: int, every: int) -> List[int]:
    targets = list(range(every, update_count + 1, every))
    if update_count and (not targets or targets[-1] != update_count):
        targets.append(update_count)
    return targets


def sqlite_snapshots(updates: List[List[str]], targets: Iterable[int]) -> Dict[int, State]:
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    target_set = set(targets)
    snapshots: Dict[int, State] = {}
    for sequence, update in enumerate(updates, start=1):
        apply_update(conn, update)
        if sequence in target_set:
            state: State = {}
            for orderkey, revenue, orderdate, shippriority in conn.execute(Q3_ALL_GROUPS_SQL):
                state[(int(orderkey), orderdate, int(shippriority))] = float(revenue)
            snapshots[sequence] = state
    return snapshots


def run_flink(
    project_root: Path,
    jar: Path,
    updates: Path,
    java: str,
    parallelism: int,
) -> List[Tuple[int, GroupKey, float]]:
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

    deltas: List[Tuple[int, GroupKey, float]] = []
    for line in completed.stdout.splitlines():
        match = DELTA_PATTERN.search(line.strip())
        if not match:
            continue
        sequence = int(match.group(1))
        key = (int(match.group(3)), match.group(4), int(match.group(5)))
        current_revenue = float(match.group(7))
        deltas.append((sequence, key, current_revenue))
    deltas.sort(key=lambda item: item[0])
    return deltas


def compare_states(expected: State, actual: State, tolerance: float) -> List[str]:
    differences = []
    for key in sorted(expected.keys() | actual.keys()):
        expected_value = expected.get(key)
        actual_value = actual.get(key)
        if expected_value is None:
            differences.append(f"unexpected group {key}: {actual_value:.2f}")
        elif actual_value is None:
            differences.append(f"missing group {key}: expected {expected_value:.2f}")
        elif abs(expected_value - actual_value) > tolerance:
            differences.append(
                f"group {key}: expected {expected_value:.2f}, actual {actual_value:.2f}"
            )
    return differences


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Compare complete Flink-maintained Q3 state with SQLite snapshots."
    )
    parser.add_argument("updates", type=Path)
    parser.add_argument("--jar", type=Path, default=project_root / "target" /
                        "flink-continuous-tpch-q3-1.0.0.jar")
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--snapshot-every", type=int, default=1000)
    parser.add_argument("--tolerance", type=float, default=0.011)
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
        parser.error(f"shaded jar does not exist: {args.jar}; run mvn clean package first")

    updates = list(read_updates(args.updates))
    targets = snapshot_targets(len(updates), args.snapshot_every)
    expected = sqlite_snapshots(updates, targets)
    deltas = run_flink(
        project_root, args.jar.resolve(), args.updates.resolve(), args.java, args.parallelism
    )

    actual: State = {}
    delta_index = 0
    rows = []
    failure_details = []
    for target in targets:
        while delta_index < len(deltas) and deltas[delta_index][0] <= target:
            _, key, current_revenue = deltas[delta_index]
            if abs(current_revenue) <= args.tolerance:
                actual.pop(key, None)
            else:
                actual[key] = current_revenue
            delta_index += 1

        differences = compare_states(expected[target], actual, args.tolerance)
        rows.append(
            {
                "snapshot": target,
                "sqlite_groups": len(expected[target]),
                "flink_groups": len(actual),
                "mismatches": len(differences),
                "status": "PASS" if not differences else "FAIL",
            }
        )
        if differences:
            failure_details.append((target, differences[:10]))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["snapshot", "sqlite_groups", "flink_groups", "mismatches", "status"],
        )
        writer.writeheader()
        writer.writerows(rows)

    if failure_details:
        for snapshot, differences in failure_details:
            print(f"snapshot {snapshot}: FAIL")
            for difference in differences:
                print(f"  {difference}")
        raise SystemExit(1)

    print(
        f"PASS: {len(targets)} complete snapshots, {len(updates)} updates, "
        f"parallelism {args.parallelism}; results: {args.output}"
    )


if __name__ == "__main__":
    main()
