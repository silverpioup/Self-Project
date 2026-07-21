# HKUST IP Flink Project

This package completes the project scope from the proposal and briefing notes: a small Apache Flink prototype for continuous query processing over TPC-H-style updates, plus a correctness checker and final write-up materials.

## Implemented Query

The prototype implements TPC-H Q3, the query highlighted in the project briefing and Cquirrel demo:

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

Q3 satisfies the minimum requirement because it joins three TPC-H tables and includes grouping and aggregation. It is also the query explicitly described in the Flink/Cquirrel part of the meeting transcript.

## Project Structure

```text
pom.xml
src/main/java/edu/hkust/ip/flink/TpchQ3ContinuousJob.java
src/main/resources/simplelogger.properties
data/sample_updates.csv
data/expected_sample_snapshots.csv
results/benchmark_results.csv
results/sqlite_q3_snapshots_large.csv
scripts/generate_benchmark_updates.py
scripts/run_benchmark.ps1
scripts/verify_q3_sqlite.py
scripts/run_sample.ps1
materials_summary.md
final_report.md
presentation_outline.md
```

## Update Stream Format

The input stream is pipe-separated:

```text
op|table|fields...
```

Supported records:

```text
+|customer|custkey|mktsegment
-|customer|custkey
+|orders|orderkey|custkey|orderdate|shippriority
-|orders|orderkey
+|lineitem|orderkey|linenumber|extendedprice|discount|shipdate
-|lineitem|orderkey|linenumber
```

## Build and Run

Prerequisites:

```text
JDK 17, Maven, Python 3
```

Build:

```bash
mvn clean package
```

Run on the sample stream with the shaded jar:

```powershell
java --add-opens=java.base/java.util=ALL-UNNAMED -jar target\flink-continuous-tpch-q3-1.0.0.jar data\sample_updates.csv
```

The jar also accepts optional benchmark arguments:

```text
java -jar target/flink-continuous-tpch-q3-1.0.0.jar <input> <parallelism> <print|quiet>
```

Example:

```powershell
java --add-opens=java.base/java.util=ALL-UNNAMED -jar target\flink-continuous-tpch-q3-1.0.0.jar data\sample_updates.csv 1 print
```

On JDK 17 or newer, Flink's serializer may need an explicit module-opening flag. The project includes a helper script for this:

```powershell
.\scripts\run_sample.ps1
```

Equivalent manual command:

```powershell
java --add-opens=java.base/java.util=ALL-UNNAMED -jar target\flink-continuous-tpch-q3-1.0.0.jar data\sample_updates.csv
```

Each output line has this format:

```text
sequence|change_type|l_orderkey|o_orderdate|o_shippriority|delta_revenue|current_revenue|reason
```

Only changed groups are emitted, matching the delta-enumeration mode described by Cquirrel. If a single update touches multiple tuples that contribute to the same group, the implementation combines them and emits one final delta for that group.

## Correctness Check

Use SQLite as the snapshot baseline:

```bash
python scripts/verify_q3_sqlite.py data/sample_updates.csv --snapshot-every 1
```

The checked-in `data/expected_sample_snapshots.csv` file is the expected SQLite snapshot output for the sample stream.

For a larger experiment, generate or convert TPC-H data into the same update format, run the Flink job, and compare selected snapshots against the SQLite output. The meeting notes suggested checking around 10 snapshots rather than every update for large data.

## Benchmark Experiment

The project includes a deterministic TPC-H Q3-style update generator and a benchmark runner:

```powershell
.\scripts\run_benchmark.ps1
```

The benchmark builds the jar, generates three update streams, and runs Flink with parallelism `1`, `2`, `4`, and `8`. The generated streams contain inserts, deletes, and replace-style updates over `customer`, `orders`, and `lineitem`.

Latest local results:

| Dataset | Updates | Parallelism | Wall-clock seconds | Throughput updates/s | Avg ms/update |
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

The full CSV output is saved at `results/benchmark_results.csv`.

For correctness on the largest generated stream, the SQLite checker was run every 10,000 updates:

```powershell
python .\scripts\verify_q3_sqlite.py data\benchmark\updates_large.csv --snapshot-every 10000 --output results\sqlite_q3_snapshots_large.csv
```

This produced 150 SQL baseline rows in `results/sqlite_q3_snapshots_large.csv`.

The current prototype keeps the Q3 maintenance operator at parallelism 1 so that the in-memory relational state remains correct and deterministic. The benchmark still starts Flink with parallelism 1/2/4/8 to measure the effect of the runtime setting on this centralized-state prototype. A fully partitioned version would require redesigning the three-table state with keyed or broadcast state.

## Notes on Scope

The implementation is intentionally focused on TPC-H Q3. It demonstrates the algorithmic idea from Cquirrel and the SIGMOD 2020 AJU paper: maintain indexes and update only the affected join/aggregation results after each tuple insertion or deletion. It is not a full reimplementation of the complete Cquirrel system.
