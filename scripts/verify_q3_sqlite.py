#!/usr/bin/env python3
import argparse
import csv
import sqlite3
from decimal import Decimal
from pathlib import Path

Q3_ALL_GROUPS_SQL = """
SELECT
  l.l_orderkey,
  SUM(l.l_extendedprice_cents * (100 - l.l_discount_hundredths)) AS revenue_units,
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

Q3_TOP_TEN_SQL = Q3_ALL_GROUPS_SQL + """
ORDER BY revenue_units DESC, o.o_orderdate, l.l_orderkey
LIMIT 10
"""


def scaled_integer(value: str, places: int) -> int:
    scaled = Decimal(value) * (10 ** places)
    return int(scaled.to_integral_exact())


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE customer (
          c_custkey INTEGER PRIMARY KEY,
          c_mktsegment TEXT NOT NULL
        );
        CREATE TABLE orders (
          o_orderkey INTEGER PRIMARY KEY,
          o_custkey INTEGER NOT NULL,
          o_orderdate TEXT NOT NULL,
          o_shippriority INTEGER NOT NULL
        );
        CREATE TABLE lineitem (
          l_orderkey INTEGER NOT NULL,
          l_linenumber INTEGER NOT NULL,
          l_extendedprice_cents INTEGER NOT NULL,
          l_discount_hundredths INTEGER NOT NULL,
          l_shipdate TEXT NOT NULL,
          PRIMARY KEY (l_orderkey, l_linenumber)
        );
        """
    )


def require_changed(cursor: sqlite3.Cursor, table: str, key: str) -> None:
    if cursor.rowcount != 1:
        raise ValueError(f"cannot delete missing {table} tuple {key}")


def apply_update(conn: sqlite3.Connection, row: list[str]) -> None:
    op, table, *fields = row
    if op not in {"+", "-"}:
        raise ValueError(f"unknown operation {op!r}")
    insert = op == "+"

    if table == "customer":
        custkey = int(fields[0])
        if insert:
            conn.execute(
                "INSERT INTO customer VALUES (?, ?)", (custkey, fields[1])
            )
        else:
            cursor = conn.execute(
                "DELETE FROM customer WHERE c_custkey = ?", (custkey,)
            )
            require_changed(cursor, table, str(custkey))
    elif table == "orders":
        orderkey = int(fields[0])
        if insert:
            conn.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?)",
                (orderkey, int(fields[1]), fields[2], int(fields[3])),
            )
        else:
            cursor = conn.execute(
                "DELETE FROM orders WHERE o_orderkey = ?", (orderkey,)
            )
            require_changed(cursor, table, str(orderkey))
    elif table == "lineitem":
        orderkey = int(fields[0])
        linenumber = int(fields[1])
        key = f"{orderkey}/{linenumber}"
        if insert:
            discount = scaled_integer(fields[3], 2)
            if not 0 <= discount <= 100:
                raise ValueError(f"discount outside [0, 1] for lineitem {key}")
            conn.execute(
                "INSERT INTO lineitem VALUES (?, ?, ?, ?, ?)",
                (
                    orderkey,
                    linenumber,
                    scaled_integer(fields[2], 2),
                    discount,
                    fields[4],
                ),
            )
        else:
            cursor = conn.execute(
                "DELETE FROM lineitem "
                "WHERE l_orderkey = ? AND l_linenumber = ?",
                (orderkey, linenumber),
            )
            require_changed(cursor, table, key)
    else:
        raise ValueError(f"unknown table {table!r}")


def read_updates(path: Path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle, delimiter="|"):
            if not row or row[0].startswith("#"):
                continue
            yield row


def format_revenue(units: int) -> str:
    return f"{Decimal(units) / Decimal(10_000):.4f}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("updates", type=Path)
    parser.add_argument("--snapshot-every", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    rows = []
    for index, update in enumerate(read_updates(args.updates), start=1):
        apply_update(conn, update)
        if index % args.snapshot_every == 0:
            for orderkey, revenue, orderdate, shippriority in conn.execute(
                Q3_TOP_TEN_SQL
            ):
                rows.append(
                    {
                        "snapshot": index,
                        "l_orderkey": orderkey,
                        "revenue": format_revenue(revenue),
                        "o_orderdate": orderdate,
                        "o_shippriority": shippriority,
                    }
                )

    fieldnames = [
        "snapshot",
        "l_orderkey",
        "revenue",
        "o_orderdate",
        "o_shippriority",
    ]
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        writer = csv.DictWriter(__import__("sys").stdout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
