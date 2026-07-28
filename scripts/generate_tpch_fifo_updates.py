#!/usr/bin/env python3
"""Build a valid FIFO insertion/deletion stream from TPC-H base tables."""

import argparse
import random
from collections import defaultdict, deque
from pathlib import Path
from typing import DefaultDict, Iterable, List

from convert_tpch_tbl_to_updates import require_fields, tbl_rows


def customer_update(row: List[str], insert: bool = True) -> str:
    return (
        f"+|customer|{row[0]}|{row[6]}"
        if insert
        else f"-|customer|{row[0]}"
    )


def order_update(row: List[str], insert: bool = True) -> str:
    return (
        f"+|orders|{row[0]}|{row[1]}|{row[4]}|{row[7]}"
        if insert
        else f"-|orders|{row[0]}"
    )


def lineitem_update(row: List[str], insert: bool = True) -> str:
    return (
        f"+|lineitem|{row[0]}|{row[3]}|{row[5]}|{row[6]}|{row[10]}"
        if insert
        else f"-|lineitem|{row[0]}|{row[3]}"
    )


def write_bundle(
    output,
    order: List[str],
    lineitems: Iterable[List[str]],
    insert: bool,
) -> int:
    count = 0
    if insert:
        output.write(order_update(order) + "\n")
        count += 1
        for item in lineitems:
            output.write(lineitem_update(item) + "\n")
            count += 1
    else:
        for item in lineitems:
            output.write(lineitem_update(item, insert=False) + "\n")
            count += 1
        output.write(order_update(order, insert=False) + "\n")
        count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the FIFO stream used by the AJU evaluation: load an "
            "initial window, then delete the oldest order bundle before "
            "inserting each new bundle."
        )
    )
    parser.add_argument("--customer", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--lineitem", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--warmup-fraction", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=6910)
    parser.add_argument("--max-orders", type=int, default=0)
    args = parser.parse_args()

    if not 0.0 < args.warmup_fraction < 1.0:
        parser.error("--warmup-fraction must be between zero and one")
    for source in (args.customer, args.orders, args.lineitem):
        if not source.is_file():
            parser.error(f"TPC-H table file does not exist: {source}")

    customers = list(tbl_rows(args.customer))
    orders = list(tbl_rows(args.orders))
    lineitems = list(tbl_rows(args.lineitem))
    for row in customers:
        require_fields(args.customer, row, 7)
    for row in orders:
        require_fields(args.orders, row, 8)
    for row in lineitems:
        require_fields(args.lineitem, row, 11)

    if args.max_orders:
        orders = orders[: args.max_orders]
    order_keys = {row[0] for row in orders}
    lineitems = [row for row in lineitems if row[0] in order_keys]
    customer_keys = {row[0] for row in customers}
    missing_customers = sorted({row[1] for row in orders} - customer_keys)
    if missing_customers:
        raise ValueError(f"orders reference missing customers: {missing_customers[:5]}")

    by_order: DefaultDict[str, List[List[str]]] = defaultdict(list)
    for row in lineitems:
        by_order[row[0]].append(row)
    missing_lineitems = [row[0] for row in orders if row[0] not in by_order]
    if missing_lineitems:
        raise ValueError(f"orders without lineitems: {missing_lineitems[:5]}")

    random.Random(args.seed).shuffle(orders)
    warmup_count = max(1, int(len(orders) * args.warmup_fraction))
    initial = orders[:warmup_count]
    arrivals = orders[warmup_count:]
    active = deque(initial)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    insertions = 0
    deletions = 0
    with args.output.open("w", encoding="utf-8", newline="\n") as output:
        output.write("# TPC-H-derived FIFO update stream\n")
        output.write(
            f"# seed={args.seed}|warmup_fraction={args.warmup_fraction}|"
            f"orders={len(orders)}\n"
        )
        for customer in customers:
            output.write(customer_update(customer) + "\n")
            count += 1
            insertions += 1

        for order in initial:
            bundle_count = write_bundle(
                output, order, by_order[order[0]], insert=True
            )
            count += bundle_count
            insertions += bundle_count

        for arriving in arrivals:
            oldest = active.popleft()
            bundle_count = write_bundle(
                output, oldest, by_order[oldest[0]], insert=False
            )
            count += bundle_count
            deletions += bundle_count

            bundle_count = write_bundle(
                output, arriving, by_order[arriving[0]], insert=True
            )
            count += bundle_count
            insertions += bundle_count
            active.append(arriving)

    print(
        f"wrote {count} updates ({insertions} insertions, "
        f"{deletions} deletions, {warmup_count} active orders) to {args.output}"
    )


if __name__ == "__main__":
    main()
