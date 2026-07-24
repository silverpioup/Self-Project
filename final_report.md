# Parallel Incremental Maintenance of TPC-H Query 3 with Apache Flink

**YUN Hanxu**  
**Student ID:** 21286712  
**Program:** MSc in Information Technology  
**Course:** CSIT6910 Independent Project  
**Supervisor:** Prof. Ke Yi  
**Email:** hyunac@connect.ust.hk  
**Code:** https://github.com/silverpioup/Self-Project

## Abstract

This project implements continuous incremental maintenance of TPC-H Query 3
using Apache Flink. The query joins `customer`, `orders`, and `lineitem`,
applies date and market-segment predicates, and aggregates revenue by order.
Instead of recomputing the SQL query after every update, the system stores
relation indexes and result-group revenue in Flink managed keyed state. Orders
and lineitems are partitioned by order key, while customer updates are
replicated to the order shards. The stateful operator therefore runs with
actual parallelism 1, 2, 4, or 8. Correctness is evaluated by reconstructing
the complete Flink result at selected snapshots and comparing it with an
independent SQLite execution. All checked snapshots pass on a corner-case
stream, a 155,400-update synthetic stream, and TPC-H scale-factor 0.01 data.

## 1. Background and Related Work

Continuous analytical applications evaluate registered queries while their
input relations change. Re-executing a multi-table query after every inserted
or deleted tuple repeats joins and aggregation work that is unrelated to the
changed record. Incremental view maintenance instead derives the change in the
answer from the update and the current indexed state.

The project follows the setting of Cquirrel, a continuous query processing
system for acyclic relational schemas. Cquirrel supports selection,
projection, joins, and aggregation under insertions and deletions, and uses
delta enumeration to report changed answers. The AJU work provides the fuller
algorithmic basis for maintaining acyclic foreign-key joins under updates.
These assumptions match TPC-H Query 3 because its joins form the acyclic chain
`customer <- orders <- lineitem`.

TPC-H supplies a standard decision-support schema, analytical queries, and a
scale-factor data generator. Query 3 was selected because it is the typical
three-table example identified in the project briefing. It contains two
foreign-key joins, selections, grouping, aggregation, and top-k ordering while
remaining suitable for a focused three-credit implementation.

## 2. Query and Incremental Algorithm

The maintained query is:

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

The maintained group key is
`(l_orderkey, o_orderdate, o_shippriority)`. A qualifying lineitem contributes
`l_extendedprice * (1 - l_discount)` to its group. The system retains every
non-zero group, allowing the ordered top ten to be derived without repeating
the joins.

For a lineitem update, the algorithm checks one order and its referenced
customer. If the predicates hold, the lineitem contribution is added or
subtracted. For an order update, only lineitems indexed under that order are
visited. For a customer update, only orders indexed under that customer and
their lineitems are visited. Contributions produced by one input update are
combined by group before output. A non-zero result emits `UPSERT_GROUP`; a
group reduced to zero is removed and emits `DELETE_GROUP`.

## 3. Implementation Details

The implementation uses Java, Apache Flink 1.19.1, Maven, and JDK 17. A
single-parallelism file source preserves input order and assigns a global
sequence number. The routing stage sends orders and lineitems to
`orderkey mod parallelism`. Customer changes are sent to every shard because a
customer may have orders in several partitions. The stream is keyed by shard
identifier and processed by a parallel keyed process function.

Each shard owns Flink `MapState` for customers, orders, orders indexed by
customer, lineitems indexed by order, and current revenue by result group. All
records belonging to one order therefore share one state partition, while
every shard has the customer state needed to evaluate its orders. The stateful
maintenance operator itself runs at the requested parallelism rather than
remaining centralized.

The input format represents insertion and deletion directly:

```text
+|customer|custkey|mktsegment
-|customer|custkey
+|orders|orderkey|custkey|orderdate|shippriority
-|orders|orderkey
+|lineitem|orderkey|linenumber|extendedprice|discount|shipdate
-|lineitem|orderkey|linenumber
```

The output contains the sequence number, change type, group key, revenue
delta, current revenue, and reason. The repository includes build and run
scripts, sample data, a TPC-H `.tbl` converter, deterministic workload
generation, automated correctness comparison, benchmark scripts, result
tables, and the final report.

## 4. Experimental Setup and Data Sets

Experiments were executed locally with Flink 1.19.1 and JDK 17. Every bounded
file run includes Flink startup and shutdown. Throughput is input updates
divided by wall-clock execution time. Latency is measured from sequence
assignment to completion of keyed-state maintenance. The program reports mean,
P50, P95, and P99 values in microseconds.

Three inputs were used:

| Data set | Description | Updates |
|---|---|---:|
| Sample | Hand-written insert/delete corner cases | 13 |
| Synthetic large | Update-heavy customer/order/lineitem stream | 155,400 |
| TPC-H SF 0.01 | 1,500 customers, 15,000 orders, 60,175 lineitems | 76,675 |

The TPC-H tables were generated by DuckDB's TPC-H-compatible `dbgen`
extension and converted into the project update format. The converter also
accepts official DBGEN `.tbl` column layouts. These measurements are local
project experiments, not audited TPC benchmark results.

Correctness is tested independently with SQLite. The comparison program runs
Flink, reconstructs the complete maintained state from emitted deltas, replays
the same updates into SQLite, executes the grouped Q3 SQL, and compares every
group and revenue value. The sample is checked after every update at
parallelism 1, 2, 4, and 8. Sixteen complete snapshots are checked for each
large data set at parallelism 8.

## 5. Experimental Results and Discussion

All correctness comparisons pass:

| Data set | Parallelism | Snapshots | Result |
|---|---:|---:|---|
| Sample | 1, 2, 4, 8 | 13 per setting | PASS |
| Synthetic large | 8 | 16 | PASS |
| TPC-H SF 0.01 | 8 | 16 | PASS |

The TPC-H SF 0.01 performance results are:

| Parallelism | Updates/s | Mean us | P50 us | P95 us | P99 us |
|---:|---:|---:|---:|---:|---:|
| 1 | 37,788.28 | 14,632.15 | 14,514 | 22,875 | 23,374 |
| 2 | 37,935.63 | 17,795.53 | 16,015 | 29,085 | 29,857 |
| 4 | 37,715.33 | 5,963.91 | 2,961 | 18,259 | 21,590 |
| 8 | 37,663.54 | 6,338.67 | 3,969 | 19,328 | 27,359 |

Throughput remains close to 37,700-37,900 updates per second. At this data
size, the ordered file source, fixed startup cost, and replication of customer
updates limit throughput scaling. P50 latency falls from 14,514 microseconds at
parallelism 1 to 2,961 microseconds at parallelism 4, which is consistent with
concurrent order-shard processing. Parallelism 8 does not improve on
parallelism 4 because additional routing and customer replication offset the
available concurrency.

The large synthetic stream reaches 74,925.82 updates per second at parallelism
1, but approximately 50,200 updates per second at parallelism 4 and 8. That
generator creates one customer update for every order, so broadcasting those
updates increases total operator work with the number of shards. The result
shows that the partitioning strategy is most appropriate when lineitem and
order updates dominate customer changes. No linear speedup is claimed.

The implementation meets the central functional objectives: three-table
TPC-H processing, insertions and deletions, incremental maintenance, managed
Flink state, changed-group output, complete SQL verification, larger data
sets, actual stateful-operator parallelism, and percentile latency
measurement.

## 6. Future Directions

The present system is query-specific rather than a general Cquirrel
reimplementation. A future version could compile more SPJA queries and derive
indexes automatically. Customer replication could be reduced by maintaining a
customer-to-shard subscription index or by introducing a separate enrichment
stage. A long-running Kafka experiment would separate steady-state throughput
from bounded-job startup cost. Further evaluation should cover checkpointing,
failure recovery, state-backend selection, cluster execution, and larger
official DBGEN scale factors.

## References

1. Q. Wang et al., "Cquirrel: Continuous Query Processing over Acyclic
   Relational Schemas," PVLDB, vol. 14, no. 12, 2021.
2. Q. Wang and K. Yi, "Maintaining Acyclic Foreign-Key Joins under Updates,"
   SIGMOD, 2020.
3. Transaction Processing Performance Council, *TPC Benchmark H Standard
   Specification*, Revision 3.0.1.
4. Apache Flink 1.19 Documentation.
5. Project code: https://github.com/silverpioup/Self-Project

# Appendix A: Meeting Minutes

The fourth meeting is scheduled for early August. Its entry is a draft agenda
and should be changed to completed minutes after the meeting.

## Minutes of the 1st Project Meeting

**Date:** Wednesday, June 10, 2026  
**Time:** 4:00 pm-4:22 pm  
**Place:** Online meeting  
**Present:** Prof. Ke Yi; YUN Hanxu; other CSIT6910 students  
**Apology:** None recorded  
**Note-taker:** YUN Hanxu

### 1. Approval of minutes

This was the first project meeting, so there were no previous minutes to
approve.

### 2. Discussion items

The supervisor introduced TPC-H and the two independent-project directions.
For the Flink project, the selected query should involve at least three tables;
Q3 was identified as the standard minimum example. The student should generate
an insertion/deletion stream, implement incremental processing with a Flink
process function and maintained state, output affected groups, verify selected
snapshots against a conventional database, and then evaluate throughput and
parallelism. The student was asked to read Cquirrel, with the AJU paper as the
fuller reference.

### 3. Meeting adjournment and next meeting

The briefing ended at approximately 4:22 pm. The next consultation would
review the selected query, update format, and implementation design.

## Minutes of the 2nd Project Meeting

**Date:** Wednesday, July 1, 2026  
**Time:** 4:00 pm-4:30 pm  
**Place:** Online meeting  
**Present:** Prof. Ke Yi; YUN Hanxu  
**Apology:** None recorded  
**Note-taker:** YUN Hanxu

### 1. Approval of minutes

The first meeting's requirements and the selection of TPC-H Q3 were reviewed.

### 2. Discussion items

The Q3 predicates, foreign-key chain, group key, and revenue expression were
confirmed. The update representation was defined for customer, orders, and
lineitem insertions and deletions. The planned state consisted of primary-key
maps, reverse indexes from customer to orders and order to lineitems, and a
revenue map. SQLite was selected as the independent correctness baseline.

### 3. Meeting adjournment and next meeting

The next meeting would review the runnable Flink implementation and sample
correctness results.

## Minutes of the 3rd Project Meeting

**Date:** Friday, July 24, 2026  
**Time:** 4:00 pm-4:30 pm  
**Place:** Online meeting  
**Present:** Prof. Ke Yi; YUN Hanxu  
**Apology:** None recorded  
**Note-taker:** YUN Hanxu

### 1. Approval of minutes

The query definition, update format, state indexes, and SQLite verification
plan from the second meeting were reviewed.

### 2. Discussion items

The working Java/Flink job and sample delta output were reviewed. Insertions,
deletions, replacement operations, and removal of zero-revenue groups were
tested. The correctness process was strengthened to reconstruct the complete
Flink state and compare every result group with SQLite. The next tasks were to
convert scale-factor data, move the state into Flink managed keyed state, and
test parallelism 1, 2, 4, and 8.

### 3. Meeting adjournment and next meeting

The next meeting would review the larger-data correctness checks,
parallel-performance measurements, and final report.

## Draft Minutes of the 4th Project Meeting (Scheduled)

**Date:** Monday, August 3, 2026  
**Time:** 4:00 pm-4:30 pm  
**Place:** Online meeting  
**Present:** Prof. Ke Yi; YUN Hanxu  
**Apology:** None recorded  
**Note-taker:** YUN Hanxu

### 1. Approval of minutes

The implementation and verification actions recorded in the third meeting
will be reviewed.

### 2. Discussion items

The final keyed-state design, TPC-H `.tbl` conversion, and experimental results
will be presented. The sample passed at parallelism 1, 2, 4, and 8. The
155,400-update synthetic stream and the 76,675-update TPC-H SF 0.01 stream each
passed 16 complete SQLite snapshot comparisons at parallelism 8. Throughput
and mean/P50/P95/P99 latency results will be discussed, including the scaling cost
of replicating customer updates. The final report structure, repository link,
limitations, and future directions will be reviewed.

### 3. Meeting adjournment and next meeting

The intended outcome is approval of the final report and code submission,
subject to any corrections requested during the meeting.
