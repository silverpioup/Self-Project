#!/usr/bin/env python3
import argparse
import csv
import sqlite3
from pathlib import Path

Q3_SQL = """
SELECT
  l.l_orderkey,
  ROUND(SUM(l.l_extendedprice * (1 - l.l_discount)), 2) AS revenue,
  o.o_orderdate,
  o.o_shippriority
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE c.c_mktsegment = 'BUILDING'
  AND o.o_orderdate < '1995-03-15'
  AND l.l_shipdate > '1995-03-15'
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
ORDER BY revenue DESC, o.o_orderdate
LIMIT 10
"""


def create_schema(conn):
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
          l_extendedprice REAL NOT NULL,
          l_discount REAL NOT NULL,
          l_shipdate TEXT NOT NULL,
          PRIMARY KEY (l_orderkey, l_linenumber)
        );
        """
    )


def apply_update(conn, row):
    op, table, *fields = row
    insert = op == "+"
    if op not in {"+", "-"}:
        raise ValueError(f"Unknown operation {op!r}")

    if table == "customer":
        custkey = int(fields[0])
        if not insert:
            conn.execute("DELETE FROM customer WHERE c_custkey = ?", (custkey,))
        else:
            conn.execute(
                "INSERT OR REPLACE INTO customer VALUES (?, ?)",
                (custkey, fields[1]),
            )
    elif table == "orders":
        orderkey = int(fields[0])
        if not insert:
            conn.execute("DELETE FROM orders WHERE o_orderkey = ?", (orderkey,))
        else:
            conn.execute(
                "INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?)",
                (orderkey, int(fields[1]), fields[2], int(fields[3])),
            )
    elif table == "lineitem":
        orderkey = int(fields[0])
        linenumber = int(fields[1])
        if not insert:
            conn.execute(
                "DELETE FROM lineitem WHERE l_orderkey = ? AND l_linenumber = ?",
                (orderkey, linenumber),
            )
        else:
            conn.execute(
                "INSERT OR REPLACE INTO lineitem VALUES (?, ?, ?, ?, ?)",
                (orderkey, linenumber, float(fields[2]), float(fields[3]), fields[4]),
            )
    else:
        raise ValueError(f"Unknown table {table!r}")


def read_updates(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle, delimiter="|"):
            if not row or row[0].startswith("#"):
                continue
            yield row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("updates", help="Path to the update stream file")
    parser.add_argument("--snapshot-every", type=int, default=1)
    parser.add_argument("--output", default=None, help="Optional CSV output file")
    args = parser.parse_args()

    conn = sqlite3.connect(":memory:")
    create_schema(conn)

    rows = []
    for index, update in enumerate(read_updates(args.updates), start=1):
        apply_update(conn, update)
        if index % args.snapshot_every == 0:
            for orderkey, revenue, orderdate, shippriority in conn.execute(Q3_SQL):
                rows.append(
                    {
                        "snapshot": index,
                        "l_orderkey": orderkey,
                        "revenue": f"{revenue:.2f}",
                        "o_orderdate": orderdate,
                        "o_shippriority": shippriority,
                    }
                )

    fieldnames = ["snapshot", "l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
    if args.output:
        with Path(args.output).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        writer = csv.DictWriter(__import__("sys").stdout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
