# Materials Summary

## Files Used Directly

`Yun_Hanxu_IP_Proposal_Flink.pdf`

The proposal defines the project title, scope, timeline, grading components, and expected deliverables. The implemented package follows the proposal by providing a Java/Flink prototype, update-stream handling, representative SQL query support, correctness checking, report, and presentation outline.

`meeting_saved_closed_caption.txt`

The meeting transcript gives the most concrete project instructions. Important points used in this package:

```text
1. Use TPC-H.
2. Pick one or two queries.
3. The query must involve at least three tables.
4. Q3 is the typical minimum query and is acceptable.
5. For the Flink project, implement the update algorithm from the Cquirrel/AJU line of work.
6. Generate an update stream containing insertions and deletions.
7. Process the update stream in Flink, using a process function and maintained state.
8. Output changed groups after updates.
9. Verify correctness against a standard database such as SQLite, PostgreSQL, or MySQL.
10. Then evaluate throughput and possibly parallelism.
```

`Cquirrel.pdf`

Cquirrel is the primary paper for this Flink project. It describes a continuous
query processing engine built on Flink for acyclic relational schemas. It
supports SPJA queries with primary-key-to-foreign-key joins, arbitrary
insertions/deletions, and delta enumeration. The prototype follows this model
for TPC-H Q3 by maintaining live and non-live tuples, child-match counts,
reverse indexes, changed aggregates, and the ordered Top-10 in Flink managed
state.

`sigmod20.pdf`

The SIGMOD 2020 AJU paper is the fuller algorithmic reference. It studies
incremental maintenance of acyclic foreign-key joins under updates and explains
how TPC-H analytical queries can be maintained using the algorithmic
framework. The prototype implements the Q3 chain specialization rather than
the full general query compiler. Its evaluated FIFO stream is also motivated
by the paper's low-enclosureness update-sequence example.

## Files Treated as Background

`R2T.pdf`

This paper is about differentially private query evaluation with foreign keys. It appears to belong to the second project discussed in the meeting, not the Flink continuous query processing project. It is therefore not used in the implementation.

`ShiftedInverse.pdf`

This paper is also about differential privacy, specifically user-DP for monotonic functions. It is relevant to the privacy project, not the selected Flink project.

`21286712.json` and `URL.txt`

These are HKUST verification artifacts rather than technical project requirements. They were not needed for the implementation.

## Final Project Choice

The final project choice is:

```text
Implement continuous maintenance of TPC-H Q3 over update streams using Apache Flink.
```

This choice is directly supported by the proposal, the meeting transcript, Cquirrel, and the SIGMOD 2020 AJU paper.
