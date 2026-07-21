import argparse
from pathlib import Path


def generate_updates(output: Path, orders: int, lineitems_per_order: int) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", encoding="utf-8", newline="\n") as writer:
        writer.write("# op|table|fields...\n")
        writer.write("# deterministic TPC-H Q3-style update stream for benchmark runs\n")
        for i in range(1, orders + 1):
            custkey = i
            orderkey = 100000 + i
            segment = "BUILDING" if i % 5 != 0 else "AUTOMOBILE"
            order_date = "1995-03-01" if i % 7 != 0 else "1995-03-20"
            ship_priority = i % 3

            writer.write(f"+|customer|{custkey}|{segment}\n")
            writer.write(f"+|orders|{orderkey}|{custkey}|{order_date}|{ship_priority}\n")
            count += 2

            for line_number in range(1, lineitems_per_order + 1):
                price = 100.0 + ((i * line_number) % 900)
                discount = ((i + line_number) % 10) / 100.0
                ship_date = "1995-03-20" if line_number % 4 != 0 else "1995-03-10"
                writer.write(
                    f"+|lineitem|{orderkey}|{line_number}|{price:.2f}|{discount:.2f}|{ship_date}\n"
                )
                count += 1

            if i % 10 == 0:
                writer.write(f"-|lineitem|{orderkey}|1\n")
                count += 1

            if i % 25 == 0:
                writer.write(f"-|orders|{orderkey}\n")
                writer.write(f"+|orders|{orderkey}|{custkey}|{order_date}|{ship_priority}\n")
                count += 2
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic Q3 update streams.")
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--lineitems-per-order", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("data/benchmark/updates_1000.csv"))
    args = parser.parse_args()

    count = generate_updates(args.output, args.orders, args.lineitems_per_order)
    print(f"wrote {count} updates to {args.output}")


if __name__ == "__main__":
    main()
