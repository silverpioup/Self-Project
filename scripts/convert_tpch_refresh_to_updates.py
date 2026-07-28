#!/usr/bin/env python3
"""Convert official DBGen -U refresh files to the project update format."""

import argparse
from collections import defaultdict
from pathlib import Path

from convert_tpch_tbl_to_updates import require_fields, tbl_rows
from generate_tpch_fifo_updates import lineitem_update, order_update


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Convert one TPC-H RF1/RF2 pair. DBGen -U produces the new "
            "orders/lineitems and the order keys to delete."
        )
    )
    parser.add_argument("--update-orders", type=Path, required=True)
    parser.add_argument("--update-lineitem", type=Path, required=True)
    parser.add_argument("--delete-keys", type=Path, required=True)
    parser.add_argument(
        "--current-lineitem",
        type=Path,
        required=True,
        help="Current lineitem.tbl used to expand RF2 order-key deletions.",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    update_orders = list(tbl_rows(args.update_orders))
    update_lineitems = list(tbl_rows(args.update_lineitem))
    current_lineitems = list(tbl_rows(args.current_lineitem))
    for row in update_orders:
        require_fields(args.update_orders, row, 8)
    for row in update_lineitems:
        require_fields(args.update_lineitem, row, 11)
    for row in current_lineitems:
        require_fields(args.current_lineitem, row, 11)

    delete_keys = []
    for row in tbl_rows(args.delete_keys):
        delete_keys.append(row[0])
    current_by_order = defaultdict(list)
    for row in current_lineitems:
        current_by_order[row[0]].append(row)

    missing = [key for key in delete_keys if key not in current_by_order]
    if missing:
        raise ValueError(
            "RF2 delete keys missing from current lineitem table: "
            + ", ".join(missing[:5])
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as output:
        output.write("# Official TPC-H DBGen RF1 followed by RF2\n")
        for row in update_orders:
            output.write(order_update(row) + "\n")
        for row in update_lineitems:
            output.write(lineitem_update(row) + "\n")
        for order_key in delete_keys:
            for row in current_by_order[order_key]:
                output.write(lineitem_update(row, insert=False) + "\n")
            output.write(f"-|orders|{order_key}\n")

    print(
        f"converted RF1 ({len(update_orders)} orders, "
        f"{len(update_lineitems)} lineitems) and RF2 "
        f"({len(delete_keys)} orders) to {args.output}"
    )


if __name__ == "__main__":
    main()
