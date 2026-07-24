#!/usr/bin/env python3
"""Convert official TPC-H DBGEN base tables to the project's update format."""

import argparse
from pathlib import Path
from typing import Iterator, List


def tbl_rows(path: Path) -> Iterator[List[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        for line_number, line in enumerate(handle, start=1):
            fields = line.rstrip("\r\n").split("|")
            if fields and fields[-1] == "":
                fields.pop()
            if fields:
                yield fields
            else:
                raise ValueError(f"{path}:{line_number}: empty TPC-H row")


def limited(rows: Iterator[List[str]], maximum: int) -> Iterator[List[str]]:
    for index, row in enumerate(rows):
        if maximum and index >= maximum:
            break
        yield row


def require_fields(path: Path, row: List[str], count: int) -> None:
    if len(row) < count:
        raise ValueError(
            f"{path}: expected at least {count} pipe-separated fields, found {len(row)}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert customer.tbl, orders.tbl, and lineitem.tbl from TPC-H DBGEN."
    )
    parser.add_argument("--customer", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--lineitem", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-customers", type=int, default=0)
    parser.add_argument("--max-orders", type=int, default=0)
    parser.add_argument("--max-lineitems", type=int, default=0)
    args = parser.parse_args()

    for source in (args.customer, args.orders, args.lineitem):
        if not source.is_file():
            parser.error(f"TPC-H table file does not exist: {source}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    counts = {"customer": 0, "orders": 0, "lineitem": 0}

    with args.output.open("w", encoding="utf-8", newline="\n") as output:
        output.write("# Converted from TPC-H DBGEN .tbl files\n")
        output.write("# op|table|fields...\n")

        for row in limited(tbl_rows(args.customer), args.max_customers):
            require_fields(args.customer, row, 7)
            output.write(f"+|customer|{row[0]}|{row[6]}\n")
            counts["customer"] += 1

        for row in limited(tbl_rows(args.orders), args.max_orders):
            require_fields(args.orders, row, 8)
            output.write(f"+|orders|{row[0]}|{row[1]}|{row[4]}|{row[7]}\n")
            counts["orders"] += 1

        for row in limited(tbl_rows(args.lineitem), args.max_lineitems):
            require_fields(args.lineitem, row, 11)
            output.write(
                f"+|lineitem|{row[0]}|{row[3]}|{row[5]}|{row[6]}|{row[10]}\n"
            )
            counts["lineitem"] += 1

    total = sum(counts.values())
    print(
        f"wrote {total} updates "
        f"({counts['customer']} customer, {counts['orders']} orders, "
        f"{counts['lineitem']} lineitem) to {args.output}"
    )


if __name__ == "__main__":
    main()
