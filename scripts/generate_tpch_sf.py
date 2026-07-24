#!/usr/bin/env python3
"""Generate TPC-H tables with DuckDB's dbgen extension for local experiments."""

import argparse
from pathlib import Path

import duckdb


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''").replace("\\", "/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate customer/orders/lineitem TPC-H tables at a chosen scale factor."
    )
    parser.add_argument("--scale-factor", type=float, default=0.01)
    parser.add_argument("--output-dir", type=Path, default=Path("data/tpch_sf001"))
    args = parser.parse_args()
    if args.scale_factor <= 0:
        parser.error("--scale-factor must be positive")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("INSTALL tpch")
    connection.execute("LOAD tpch")
    connection.execute("CALL dbgen(sf = ?)", [args.scale_factor])

    for table in ("customer", "orders", "lineitem"):
        destination = args.output_dir / f"{table}.tbl"
        connection.execute(
            f"COPY (SELECT * FROM {table}) TO '{sql_path(destination)}' "
            "(FORMAT CSV, DELIMITER '|', HEADER FALSE)"
        )
        row_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {row_count} rows -> {destination}")


if __name__ == "__main__":
    main()
