# Query Processing over Streaming Data using Apache Flink

Student: Yun Hanxu  
Supervisor: Prof. Ke Yi  
Industry collaborator: Alibaba  
Course: CSIT6910 Independent Project

## Abstract

This project studies continuous query processing over streaming relational updates using Apache Flink. Instead of evaluating a SQL query once over a static database, the system registers the query first, receives insertions and deletions later, and continuously maintains the query answer. The implemented prototype focuses on TPC-H Query 3, a representative analytical query over `customer`, `orders`, and `lineitem`. The query includes selection, primary-key-to-foreign-key joins, grouping, and aggregation, making it a suitable target for the Flink project described in the briefing meeting. The implementation maintains in-memory indexes over the three relations and emits only the changed query-result groups after each update. A SQLite replay script is provided as a correctness baseline.

## 1. Introduction

Many analytical applications operate on data that changes continuously. Examples include online sales monitoring, stock analytics, sensor dashboards, and network traffic monitoring. In these settings, repeatedly rerunning a complex query from scratch after every update is too expensive. A continuous query processing system addresses this problem by maintaining the query result incrementally.

The project is based mainly on two assigned references. Cquirrel is a continuous query processing system built on top of Flink for acyclic relational schemas. It supports selection-projection-join-aggregation queries where joins follow primary-key-to-foreign-key constraints. The SIGMOD 2020 AJU paper gives the fuller algorithmic foundation for maintaining acyclic foreign-key joins under updates. The briefing meeting emphasized that the project should pick one or two TPC-H queries, implement the relevant algorithm in Flink, verify correctness against a standard database, and then evaluate basic performance.

This project selects TPC-H Q3 because it is explicitly mentioned in the meeting as the typical three-table query used by previous students and by the Cquirrel demo. It is complex enough to cover the required features while still being manageable for a 3-credit independent project.

## 2. Problem Definition

Let `db` be the current database instance and let `Q(db)` be the result of evaluating a query `Q` over `db`. An update `u` is either the insertion or deletion of one tuple from one relation. After applying `u`, the new database is `db + u`. The goal is to maintain a data structure `D(db)` so that the system can efficiently compute the change between `Q(db)` and `Q(db + u)`.

This project uses the delta-enumeration model described by Cquirrel: after each update, the system outputs only the result groups whose values have changed. For TPC-H Q3, each group is identified by:

```text
(l_orderkey, o_orderdate, o_shippriority)
```

and the maintained aggregate is:

```text
SUM(l_extendedprice * (1 - l_discount))
```

## 3. Query Scope

The implemented query is the core of TPC-H Q3:

```sql
SELECT
  l_orderkey,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue,
  o_orderdate,
  o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
```

The query involves three TPC-H relations and two foreign-key joins:

```text
orders.o_custkey -> customer.c_custkey
lineitem.l_orderkey -> orders.o_orderkey
```

This forms an acyclic foreign-key schema, matching the assumptions of Cquirrel and AJU. The prototype maintains the grouped revenue values. The top-10 ordering can be derived from the maintained map of group revenues.

## 4. Algorithm

The key idea is to avoid recomputing the whole join after every update. The implementation maintains the following indexes:

```text
customers: custkey -> customer row
orders: orderkey -> order row
ordersByCustomer: custkey -> orderkeys
lineItemsByOrder: orderkey -> lineitem rows
revenueByGroup: group key -> current revenue
```

For a `lineitem` insertion, only the matching `orders` row and `customer` row can be affected. If the order date, ship date, and customer market segment satisfy the Q3 predicates, the lineitem contributes:

```text
l_extendedprice * (1 - l_discount)
```

to the corresponding group. A lineitem deletion subtracts the same value.

For an `orders` insertion, the system checks the referenced customer. If the customer satisfies the market-segment predicate and the order date predicate is true, all existing lineitems of that order are scanned and their qualifying revenues are added. An order deletion subtracts those contributions.

For a `customer` insertion, the system checks all current orders referencing that customer and all current lineitems under those orders. If the inserted customer belongs to the `BUILDING` segment, those contributions become valid and are added. A customer deletion subtracts them.

This is the direct incremental-maintenance version of the Cquirrel idea for the selected query. It is most efficient when updates are localized and the number of affected descendant tuples is small. This is also consistent with the briefing discussion, where the output after each update should contain only changed groups.

## 5. Flink Implementation

The prototype is implemented as a Java Flink streaming job:

```text
src/main/java/edu/hkust/ip/flink/TpchQ3ContinuousJob.java
```

The job reads a file as a Flink `DataStream`, treats each line as one update, and processes updates with a `ProcessFunction`. The project uses parallelism 1 by default. This keeps the state centralized and makes correctness checking straightforward. The process function maintains the relation indexes and emits delta records.

The update-stream format is:

```text
+|customer|custkey|mktsegment
-|customer|custkey
+|orders|orderkey|custkey|orderdate|shippriority
-|orders|orderkey
+|lineitem|orderkey|linenumber|extendedprice|discount|shipdate
-|lineitem|orderkey|linenumber
```

Each output line has the form:

```text
sequence|change_type|l_orderkey|o_orderdate|o_shippriority|delta_revenue|current_revenue|reason
```

For example, if a qualifying lineitem is inserted, the system emits an `UPSERT_GROUP` line for the affected order group. If a deletion brings a group's revenue to zero, it emits `DELETE_GROUP`. The implementation aggregates all contribution changes produced by one update before emitting output, so each affected group is reported once per update.

## 6. Correctness Verification

The project includes a SQLite verifier:

```text
scripts/verify_q3_sqlite.py
```

The verifier replays the same update stream into standard relational tables and runs the SQL version of Q3 at selected snapshots. This follows the supervisor's suggestion in the meeting: use a standard database such as SQLite, PostgreSQL, or MySQL to verify correctness, and for large inputs check a selected set of snapshots rather than every update.

For the sample stream, correctness can be checked after every update:

```bash
python scripts/verify_q3_sqlite.py data/sample_updates.csv --snapshot-every 1
```

For larger streams, a practical setting is:

```bash
python scripts/verify_q3_sqlite.py generated_updates.csv --snapshot-every 1000 --output expected.csv
```

The expected output can then be compared with the maintained Flink state at the same snapshot points.

## 7. Experiments

The experiments evaluate correctness, throughput, latency, and Flink parallelism settings. The hand-written sample stream checks corner cases such as late lineitem insertions, non-qualifying customers, lineitem deletion, and order deletion. Larger streams are produced by `scripts/generate_benchmark_updates.py`, which creates deterministic TPC-H Q3-style updates over `customer`, `orders`, and `lineitem`. The generated streams include insertions, deletions, and replace-style order updates.

The benchmark uses three data sizes:

```text
small:  1,000 orders, 3 lineitems/order,   5,180 updates
medium: 10,000 orders, 3 lineitems/order,  51,800 updates
large:  30,000 orders, 3 lineitems/order, 155,400 updates
```

The following table reports end-to-end wall-clock measurements on the local machine. The latency column is the average wall-clock processing time per input update. It includes local Flink startup overhead, so the larger streams are more representative than the smallest stream.

| Dataset | Updates | Flink parallelism | Wall-clock seconds | Throughput updates/s | Average ms/update |
|---|---:|---:|---:|---:|---:|
| small | 5,180 | 1 | 2.033 | 2,548.52 | 0.3924 |
| small | 5,180 | 2 | 2.007 | 2,581.49 | 0.3874 |
| small | 5,180 | 4 | 2.012 | 2,574.89 | 0.3884 |
| small | 5,180 | 8 | 2.016 | 2,569.46 | 0.3892 |
| medium | 51,800 | 1 | 2.015 | 25,708.94 | 0.0389 |
| medium | 51,800 | 2 | 1.999 | 25,918.15 | 0.0386 |
| medium | 51,800 | 4 | 2.030 | 25,516.41 | 0.0392 |
| medium | 51,800 | 8 | 2.037 | 25,429.86 | 0.0393 |
| large | 155,400 | 1 | 2.026 | 76,702.20 | 0.0130 |
| large | 155,400 | 2 | 2.040 | 76,169.97 | 0.0131 |
| large | 155,400 | 4 | 2.026 | 76,692.74 | 0.0130 |
| large | 155,400 | 8 | 2.038 | 76,263.43 | 0.0131 |

The results show that throughput increases with input size because the fixed cost of starting the local Flink job is amortized over more updates. Changing the Flink parallelism setting from 1 to 8 does not produce a meaningful speedup in this prototype. This is expected: the Q3 maintenance operator is intentionally kept at parallelism 1 to preserve one consistent in-memory state for the three joined tables. The experiment is still useful because it confirms that the program runs correctly under different Flink runtime parallelism settings, while also identifying the exact limitation that must be addressed by a future keyed-state or broadcast-state design.

For correctness on the generated large stream, SQLite was used as the standard SQL baseline every 10,000 updates:

```text
python scripts/verify_q3_sqlite.py data/benchmark/updates_large.csv --snapshot-every 10000 --output results/sqlite_q3_snapshots_large.csv
```

This produced 150 top-10 baseline rows over 15 sampled snapshots. Together with the sample-stream verification after every update, this checks both detailed update behavior and larger-stream SQL consistency.

The briefing also suggested trying different Flink parallelism settings after correctness is verified. The current implementation uses single parallelism to keep state simple. Extending the prototype to partition state by `orderkey` or `custkey` would be the next step toward multi-core experiments.

## 8. Discussion

The prototype demonstrates the central benefit of incremental continuous query processing. When a lineitem update arrives, only one order group may change. Rerunning Q3 over the entire database would repeat the full join and aggregation, while the incremental method touches only the relevant indexes. This matches the motivation of Cquirrel and AJU.

The current implementation is deliberately smaller than the full research system. It supports one representative query rather than arbitrary SPJA queries. It uses in-memory Java maps inside a Flink process function rather than a generalized query compiler or optimized distributed state backend. This is appropriate for the project scope because the main objective is to understand the algorithm and demonstrate it in Flink.

The main limitation is parallelism inside the stateful Q3 operator. The benchmark starts Flink with parallelism 1, 2, 4, and 8, but the maintenance operator remains centralized to preserve correctness. This makes the current design reliable and easy to verify, but it does not fully exploit Flink's distributed state model. A fully parallel design would partition state by join keys and use keyed or broadcast state to keep customer, order, and lineitem updates consistent across operator instances. Another limitation is that the benchmark generator produces TPC-H Q3-style update streams rather than official TPC-H DBGEN `.tbl` files; the schema and predicates match Q3, and an official DBGEN converter would be a natural extension.

## 9. Future Work

Future extensions include:

```text
1. Add a TPC-H DBGEN converter that turns official customer/orders/lineitem `.tbl` rows into update streams.
2. Periodically emit the current top-10 Q3 result, not only changed groups.
3. Replace in-memory maps with Flink managed keyed state.
4. Partition the stream to support parallelism greater than 1.
5. Add support for one more TPC-H query to show generality beyond Q3.
6. Implement more of the generic AJU/Cquirrel algorithm for acyclic schemas.
```

## 10. Meeting-Minute Summary

The briefing established the following requirements:

```text
1. Use the TPC-H benchmark and select one or two analytical queries.
2. The selected query should involve at least three tables; Q3 is the standard minimal example.
3. For the Flink project, implement the continuous update algorithm from the assigned papers.
4. Generate an update stream containing insertions and deletions.
5. Read the update stream in Flink and process each update with a stateful function.
6. Output changed result groups after updates.
7. Verify correctness against a standard database on selected snapshots.
8. After correctness, measure throughput and try larger datasets or parallelism settings.
9. The final report should include problem definition, algorithm, implementation, experiments, discussion, and future work.
```

## References

Qichen Wang, Chaoqi Zhang, Danish Alsayed, Ke Yi, Bin Wu, Feifei Li, and Chaoqun Zhan. "Cquirrel: Continuous Query Processing over Acyclic Relational Schemas." PVLDB 14(12), 2021.

Qichen Wang and Ke Yi. "Maintaining Acyclic Foreign-Key Joins under Updates." SIGMOD 2020.

TPC-H benchmark specification and data generator. Transaction Processing Performance Council.
