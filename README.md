# Continuous TPC-H Q3 Maintenance with Apache Flink

This repository contains the Flink job, verification scripts, experiment
records, and final report for my CSIT6910 project. The job applies the
live-tuple maintenance algorithm used by Cquirrel/AJU to TPC-H Query 3. It
processes insertions and deletions over `customer`, `orders`, and `lineitem`,
maintains grouped revenue and the official Q3 Top-10, and emits result changes.

Code: https://github.com/silverpioup/Self-Project

## Maintained Query

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

The foreign-key DAG is `lineitem -> orders -> customer`, with `lineitem` as
the root relation under the paper's edge convention. The implementation keeps
each tuple's live/non-live status and its child-match count. A root lineitem
contributes revenue exactly when it is live. A transition into or out of the
live set produces the corresponding aggregate delta; unrelated join results
are never materialized.

The implementation is limited to Q3 and does not compile arbitrary SQL.
Because Q3's join graph is a chain, each non-leaf tuple has one child relation
and its child-match count is either zero or one. The assertion-key machinery
needed by general DAG-shaped queries is therefore unnecessary.

## Design

- The source assigns a checkpointed global sequence number in input order.
- Orders and lineitems are routed by `orderkey mod parallelism`.
- Customer updates are replicated to all order shards.
- `RoutingPlan` selects one Flink key group for every requested subtask, so
  logical shards map one-to-one to physical workers.
- Each shard maintains customers, orders, reverse indexes, lineitems,
  live-state flags, and child-match counts in Flink managed `MapState`.
- A single ordered global stage combines all shard responses for one input
  sequence, maintains exact group revenue, and updates the Q3 Top-10.
- Prices are parsed as cents and discounts as hundredths. Revenue is stored as
  a `long` in units of 0.0001 dollars; no floating-point comparison is used.
- Duplicate insertions, deletion of a missing tuple, and malformed updates are
  rejected. Replacement is represented explicitly as delete followed by
  insert.

The keyed and operator state participates in Flink checkpoints when a positive
checkpoint interval is supplied. The console sink is intended for evaluation
and is not transactional; a production exactly-once deployment would use a
checkpoint-aware sink.

## Requirements

- JDK 17
- Maven 3.9 or newer
- Python 3.10 or newer
- DuckDB only when regenerating TPC-H tables:

```powershell
python -m pip install -r requirements-experiments.txt
```

Flink 1.19.1 should be run on JDK 17. JDK 25 is not used or supported by this
project.

## Build and Run

From the repository root in PowerShell:

```powershell
mvn clean verify
java --add-opens=java.base/java.util=ALL-UNNAMED `
  -jar target\flink-continuous-tpch-q3-1.0.0.jar `
  data\sample_updates.csv 4 print 0
```

The four positional arguments are:

```text
<input> <parallelism> <print|quiet|metrics|both> <checkpoint_interval_ms>
```

The portable helper performs the same build and sample run:

```powershell
.\scripts\run_sample.ps1
```

Run the shaded JAR shown above; the Maven exec plugin is not part of the run path.

## Input and Output

```text
+|customer|custkey|mktsegment
-|customer|custkey
+|orders|orderkey|custkey|orderdate|shippriority
-|orders|orderkey
+|lineitem|orderkey|linenumber|extendedprice|discount|shipdate
-|lineitem|orderkey|linenumber
```

Changed groups:

```text
GROUP|sequence|UPSERT_GROUP|orderkey|orderdate|shippriority|delta|current|reason
GROUP|sequence|DELETE_GROUP|orderkey|orderdate|shippriority|delta|0.0000|reason
```

Whenever the ordered result changes, the complete current ranking is emitted:

```text
TOP10|sequence|rank|orderkey|orderdate|shippriority|revenue
TOP10_EMPTY|sequence
```

Metrics mode emits:

```text
METRICS|count|mean_us|p50_us|p95_us|p99_us
PROCESSING|count|seconds|updates_per_second
WORKERS|active_workers|configured_workers|worker:records...
```

Latency is measured once per original input update, after every expected shard
response has been combined and the Top-10 has been updated.

## Correctness

`compare_flink_sqlite.py` replays the same update stream in SQLite using exact
integer arithmetic. It reconstructs every Flink group from emitted deltas and
checks the full state at selected snapshots. It also compares every emitted
Top-10 ranking, not only the final ranking.

```powershell
.\scripts\run_correctness.ps1
```

Recorded evidence in `results/`:

| Workload | Parallelism | Updates | Checked group snapshots | Verified Top-10 changes |
|---|---:|---:|---:|---:|
| Hand-written corner cases | 1, 2, 4, 8 | 13 | 13 per setting | 6 per setting |
| TPC-H fixture FIFO | 4 | 8 | 8 | 1 |
| Synthetic update stream | 8 | 156,600 | 16 | 351 |
| TPC-H SF0.1 FIFO | 8 | 1,140,816 | 12 | 141 |

Every recorded comparison has zero group mismatches and passes exact Top-10
validation.

## TPC-H Update Streams

Generate base tables and a deterministic FIFO stream:

```powershell
.\scripts\run_tpch_experiment.ps1 -ScaleFactor 0.1 `
  -WarmupRuns 1 -Repetitions 3
```

The SF0.1 base data contains 15,000 customers, 150,000 orders, and 600,572
lineitems. The generated stream first loads a 75,000-order window, then
repeatedly deletes the oldest order and its lineitems before inserting a new
order and its lineitems. It contains 1,140,816 updates: 765,572 insertions and
375,244 deletions.

For official DBGen refresh files, convert RF1/RF2 with:

```powershell
python scripts\convert_tpch_refresh_to_updates.py `
  --customer data\tpch\customer.tbl `
  --orders-base data\tpch\orders.tbl `
  --orders-insert data\tpch\orders.tbl.u1 `
  --lineitem-insert data\tpch\lineitem.tbl.u1 `
  --delete-keys data\tpch\delete.1 `
  --output data\tpch_refresh_updates.csv
```

## Parallel Experiment

The committed SF0.1 experiment used one warm-up and three measured runs for
each parallelism. `benchmark_q3.py` rejects a run unless every configured
worker processed records.

| Parallelism | Active workers | Processing updates/s (mean) | Std. dev. | Mean latency us | P50 us | P95 us | P99 us |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 654,465.24 | 68,597.79 | 7,547.01 | 6,340 | 14,736.67 | 27,103.33 |
| 2 | 2 | 895,123.76 | 138,713.22 | 6,543.48 | 5,233.33 | 14,846.67 | 31,353.33 |
| 4 | 4 | 828,483.05 | 35,911.26 | 5,275.98 | 3,196.67 | 16,320.00 | 44,766.67 |
| 8 | 8 | 756,174.26 | 12,483.74 | 10,567.00 | 6,680 | 34,033.33 | 73,646.67 |

Parallelism 2 gives the highest mean processing throughput, 36.8% above
parallelism 1. Parallelism 4 is 26.6% above parallelism 1, while parallelism 8
is 15.5% above it. Scaling is non-monotonic because customer updates are
replicated and the ordered global aggregation/Top-10 stage is intentionally
single-threaded. Wall-clock throughput is also stored in the raw result file;
it includes JVM and local Flink startup/shutdown and is therefore lower.

These measurements come from the committed local runs; they are not audited
TPC benchmark results. Raw runs, summaries, environment details, and
correctness tables are under `results/`.

## Project Layout

```text
src/main/java/edu/hkust/ip/flink/   Flink algorithm and state
src/test/java/                      JUnit tests
data/                               sample and small TPC-H fixture
scripts/                            generation, verification, and benchmarks
results/                            raw and summarized experimental evidence
final_report.md                     report source
YUN_Hanxu_21286712_Final_Report.pdf final submission report
```

## References

- Q. Wang and K. Yi, "Maintaining Acyclic Foreign-Key Joins under Updates,"
  SIGMOD 2020, https://doi.org/10.1145/3318464.3380586
- Q. Wang et al., "Cquirrel: Continuous Query Processing over Acyclic
  Relational Schemas," PVLDB 2021.
- TPC Benchmark H Standard Specification,
  https://www.tpc.org/tpch/
