# Presentation Outline

## Slide 1: Title

Query Processing over Streaming Data using Apache Flink  
Yun Hanxu, CSIT6910 Independent Project

## Slide 2: Motivation

Analytical queries are often needed over data that keeps changing. Rerunning a multi-table SQL query after every update is expensive. Continuous query processing maintains the answer incrementally and emits only the changes.

## Slide 3: Project Requirement

The project uses TPC-H, selects a query with at least three tables, implements the update algorithm in Flink, verifies correctness with a standard database, and evaluates basic performance.

## Slide 4: Selected Query

TPC-H Q3 joins `customer`, `orders`, and `lineitem`. It filters by market segment and dates, then groups by order key, order date, and ship priority to compute revenue.

## Slide 5: Why Q3

Q3 is the query highlighted in the Cquirrel demo and the project briefing. It includes three-table foreign-key joins and aggregation, while remaining manageable for a focused prototype.

## Slide 6: Incremental-Maintenance Idea

Maintain indexes over the three relations. For each insertion or deletion, find only the tuples that can join with the changed tuple. Update only the affected revenue group.

## Slide 7: System Design

Input update stream -> Flink DataStream -> process function -> maintained state maps -> delta output stream.

State maps:

```text
customers
orders
ordersByCustomer
lineItemsByOrder
revenueByGroup
```

## Slide 8: Demo Update Format

```text
+|customer|1|BUILDING
+|orders|100|1|1995-03-01|0
+|lineitem|100|1|1000.00|0.05|1995-03-20
-|lineitem|100|1
```

## Slide 9: Output Format

```text
sequence|change_type|l_orderkey|o_orderdate|shippriority|delta|current|reason
```

The output is delta enumeration: only changed groups are printed.

## Slide 10: Correctness Verification

Replay the same update stream into SQLite. Run the SQL version of Q3 at selected snapshots. Compare SQLite snapshots with the maintained Flink result.

## Slide 11: Benchmark Setup

Use deterministic TPC-H Q3-style update streams with inserts, deletes, and replace-style updates. Test three sizes: 5,180 updates, 51,800 updates, and 155,400 updates. Run each stream with Flink parallelism 1, 2, 4, and 8.

## Slide 12: Experimental Results

Sample stream is verified after every update. The large stream is checked by SQLite every 10,000 updates, producing 150 baseline top-10 rows. Best observed throughput is about 76,702 updates/second on the 155,400-update stream.

## Slide 13: Limitations

The prototype supports Q3 only. The Flink job is tested with parallelism 1/2/4/8, but the stateful maintenance operator remains centralized to preserve correctness. A partitioned keyed-state design would be needed for real multi-core speedup.

## Slide 14: Future Work

Add TPC-H `.tbl` conversion, periodically emit top-10 results, use Flink managed keyed state, support parallelism greater than 1, and extend to more TPC-H queries.

## Slide 15: Conclusion

The project implements continuous maintenance of a three-table analytical query in Flink. It demonstrates how incremental update processing can avoid full recomputation and produce real-time query-result changes.
