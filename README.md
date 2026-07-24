# Continuous TPC-H Q3 Maintenance with Apache Flink

This project implements incremental maintenance of TPC-H Query 3 over
`customer`, `orders`, and `lineitem` updates. The Flink job keeps managed
keyed state and emits only result groups changed by each insertion or deletion.

Repository: https://github.com/silverpioup/Self-Project

## Query

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

The query contains two acyclic foreign-key joins, selections, grouping, and
aggregation. The job maintains every revenue group so that the ordered top ten
can be derived without rerunning the joins.

## Implementation

The ordered input receives a global sequence number and is then routed to
order shards:

- `orders` and `lineitem` updates go to `orderkey mod parallelism`.
- `customer` updates are copied to every shard because each customer may have
  orders in several shards.
- Each shard uses Flink `MapState` for customers, orders, reverse indexes,
  lineitems, and current group revenue.
- The stateful Q3 operator runs at the requested parallelism. It is no longer
  fixed at parallelism one.

This arrangement keeps all data for one order in the same keyed shard while
preserving the effect of customer updates across shards.

## Project Files

```text
pom.xml
src/main/java/edu/hkust/ip/flink/TpchQ3ContinuousJob.java
src/main/resources/simplelogger.properties
data/sample_updates.csv
data/expected_sample_snapshots.csv
data/tpch_fixture/*.tbl
scripts/convert_tpch_tbl_to_updates.py
scripts/generate_tpch_sf.py
scripts/generate_benchmark_updates.py
scripts/compare_flink_sqlite.py
scripts/verify_q3_sqlite.py
scripts/run_sample.ps1
scripts/run_correctness.ps1
scripts/run_benchmark.ps1
scripts/run_tpch_experiment.ps1
results/benchmark_results.csv
results/tpch_sf_results.csv
results/correctness_*.csv
final_report.md
YUN_Hanxu_21286712_Final_Report.pdf
```

## Requirements

- JDK 17
- Maven 3.9 or newer
- Python 3
- Optional TPC-H generation dependency:

```powershell
python -m pip install -r requirements-experiments.txt
```

## Build and Run

```powershell
mvn clean package
java --add-opens=java.base/java.util=ALL-UNNAMED `
  -jar target\flink-continuous-tpch-q3-1.0.0.jar `
  data\sample_updates.csv 4 print
```

The arguments are:

```text
<input> <parallelism> <print|quiet|metrics|both>
```

`metrics` prints one final line containing the number of measured updates,
mean latency, and P50/P95/P99 latency in microseconds:

```text
METRICS|count|mean_us|p50_us|p95_us|p99_us
```

The helper command builds and runs the sample:

```powershell
.\scripts\run_sample.ps1
```

## Update Format

```text
+|customer|custkey|mktsegment
-|customer|custkey
+|orders|orderkey|custkey|orderdate|shippriority
-|orders|orderkey
+|lineitem|orderkey|linenumber|extendedprice|discount|shipdate
-|lineitem|orderkey|linenumber
```

Delta output has this format:

```text
sequence|change_type|l_orderkey|o_orderdate|o_shippriority|delta_revenue|current_revenue|reason
```

## Automated Correctness Test

The comparison tool runs Flink, reconstructs its complete maintained state at
selected sequence numbers, replays the same stream in SQLite, and compares all
Q3 groups rather than only printed examples:

```powershell
python scripts\compare_flink_sqlite.py data\sample_updates.csv `
  --parallelism 4 --snapshot-every 1
```

Run the full checked suite:

```powershell
.\scripts\run_correctness.ps1
```

Checked results:

- Sample stream: 13 complete snapshots pass at parallelism 1, 2, 4, and 8.
- Synthetic large stream: 155,400 updates and 16 snapshots pass at
  parallelism 8.
- TPC-H SF 0.01 stream: 76,675 updates and 16 snapshots pass at
  parallelism 8.

## TPC-H Data

Convert official TPC-H DBGEN output:

```powershell
python scripts\convert_tpch_tbl_to_updates.py `
  --customer C:\tpch\customer.tbl `
  --orders C:\tpch\orders.tbl `
  --lineitem C:\tpch\lineitem.tbl `
  --output data\tpch_updates.csv
```

For a reproducible local TPC-H-compatible dataset, DuckDB's `dbgen` extension
can generate the tables and run the complete experiment:

```powershell
.\scripts\run_tpch_experiment.ps1 -ScaleFactor 0.01
```

SF 0.01 contains 1,500 customers, 15,000 orders, and 60,175 lineitems. The
converted input contains 76,675 updates.

## Experiments

`scripts\run_benchmark.ps1` generates update-heavy synthetic streams and runs
the keyed maintenance operator at parallelism 1, 2, 4, and 8. Results include
throughput and measured P50/P95/P99 end-to-operator latency.

The SF 0.01 TPC-H experiment produced:

| Parallelism | Updates/s | Mean us | P50 us | P95 us | P99 us |
|---:|---:|---:|---:|---:|---:|
| 1 | 37,788.28 | 14,632.15 | 14,514 | 22,875 | 23,374 |
| 2 | 37,935.63 | 17,795.53 | 16,015 | 29,085 | 29,857 |
| 4 | 37,715.33 | 5,963.91 | 2,961 | 18,259 | 21,590 |
| 8 | 37,663.54 | 6,338.67 | 3,969 | 19,328 | 27,359 |

These are local bounded-file results and include Flink startup and shutdown.
Throughput remains nearly constant because the single ordered file source and
customer replication limit scaling at this data size. The lower median latency
at parallelism 4 and 8 nevertheless shows that order-sharded maintenance is
executing concurrently. No linear speedup is claimed.
