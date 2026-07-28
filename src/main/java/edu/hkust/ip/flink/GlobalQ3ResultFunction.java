package edu.hkust.ip.flink;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.accumulators.LongMinimum;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.time.LocalDate;

import static edu.hkust.ip.flink.Q3Types.GroupDelta;
import static edu.hkust.ip.flink.Q3Types.GroupKey;
import static edu.hkust.ip.flink.Q3Types.RankedGroup;
import static edu.hkust.ip.flink.Q3Types.SequenceBatch;
import static edu.hkust.ip.flink.Q3Types.ShardResult;

final class GlobalQ3ResultFunction
        extends KeyedProcessFunction<Integer, ShardResult, String> {
    private static final Comparator<RankedGroup> Q3_ORDER = Comparator
            .comparingLong((RankedGroup row) -> row.revenueUnits).reversed()
            .thenComparingInt(row -> row.group.orderDateEpochDay)
            .thenComparingLong(row -> row.group.orderKey)
            .thenComparingInt(row -> row.group.shipPriority);

    private final boolean printResults;
    private final boolean measureMetrics;

    private transient MapState<Long, SequenceBatch> pendingBatches;
    private transient ValueState<Long> nextSequence;
    private transient MapState<GroupKey, Long> revenueByGroup;
    private transient TreeSet<RankedGroup> ranking;
    private transient boolean rankingLoaded;

    private transient LatencyAccumulator latency;
    private transient LongCounter completedUpdates;
    private transient LongMinimum firstCreatedNanos;
    private transient LongMaximum lastCompletedNanos;

    GlobalQ3ResultFunction(boolean printResults, boolean measureMetrics) {
        this.printResults = printResults;
        this.measureMetrics = measureMetrics;
    }

    @Override
    public void open(OpenContext openContext) {
        pendingBatches = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "pending-sequence-batches", Long.class, SequenceBatch.class));
        nextSequence = getRuntimeContext().getState(
                new ValueStateDescriptor<>("next-input-sequence", Long.class));
        revenueByGroup = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "global-revenue-by-group", GroupKey.class, Long.class));
        ranking = new TreeSet<>(Q3_ORDER);

        if (measureMetrics) {
            latency = new LatencyAccumulator();
            completedUpdates = new LongCounter();
            firstCreatedNanos = new LongMinimum();
            lastCompletedNanos = new LongMaximum();
            getRuntimeContext().addAccumulator(TpchQ3ContinuousJob.LATENCY_ACCUMULATOR, latency);
            getRuntimeContext().addAccumulator(
                    TpchQ3ContinuousJob.COMPLETED_UPDATES_ACCUMULATOR, completedUpdates);
            getRuntimeContext().addAccumulator(
                    TpchQ3ContinuousJob.FIRST_CREATED_ACCUMULATOR, firstCreatedNanos);
            getRuntimeContext().addAccumulator(
                    TpchQ3ContinuousJob.LAST_COMPLETED_ACCUMULATOR, lastCompletedNanos);
        }
    }

    @Override
    public void processElement(
            ShardResult result,
            Context context,
            Collector<String> out
    ) throws Exception {
        SequenceBatch batch = pendingBatches.get(result.sequence);
        if (batch == null) {
            batch = new SequenceBatch();
            batch.expectedParts = result.expectedParts;
            batch.createdNanos = result.createdNanos;
        }
        if (batch.expectedParts != result.expectedParts) {
            throw new IllegalStateException(
                    "Inconsistent shard count for sequence " + result.sequence);
        }
        if (batch.deltasByShard.put(result.shard, result.deltas) != null) {
            throw new IllegalStateException(
                    "Duplicate shard result for sequence " + result.sequence
                            + ", shard " + result.shard);
        }
        pendingBatches.put(result.sequence, batch);
        drainCompletedSequences(out);
    }

    private void drainCompletedSequences(Collector<String> out) throws Exception {
        Long storedNext = nextSequence.value();
        long sequence = storedNext == null ? 1L : storedNext;
        while (true) {
            SequenceBatch batch = pendingBatches.get(sequence);
            if (batch == null || !batch.complete()) {
                break;
            }
            applyBatch(sequence, batch, out);
            pendingBatches.remove(sequence);
            sequence++;
            nextSequence.update(sequence);
        }
    }

    private void applyBatch(
            long sequence,
            SequenceBatch batch,
            Collector<String> out
    ) throws Exception {
        ensureRankingLoaded();
        List<RankedGroup> beforeTopTen = topTen();
        Map<GroupKey, GroupDelta> combined = combine(batch);

        for (GroupDelta delta : combined.values()) {
            Long current = revenueByGroup.get(delta.group);
            if (current != null) {
                ranking.remove(new RankedGroup(delta.group, current));
            }
            long next = Math.addExact(current == null ? 0L : current, delta.amount);
            if (next < 0L) {
                throw new IllegalStateException(
                        "Negative revenue for order " + delta.group.orderKey);
            }
            if (next == 0L) {
                revenueByGroup.remove(delta.group);
            } else {
                revenueByGroup.put(delta.group, next);
                ranking.add(new RankedGroup(delta.group, next));
            }
            if (printResults) {
                out.collect(formatGroup(sequence, delta, next));
            }
        }

        List<RankedGroup> afterTopTen = topTen();
        if (printResults && !sameRanking(beforeTopTen, afterTopTen)) {
            if (afterTopTen.isEmpty()) {
                out.collect("TOP10_EMPTY|" + sequence);
            } else {
                for (int index = 0; index < afterTopTen.size(); index++) {
                    RankedGroup row = afterTopTen.get(index);
                    out.collect(String.format(
                            Locale.US,
                            "TOP10|%d|%d|%d|%s|%d|%s",
                            sequence,
                            index + 1,
                            row.group.orderKey,
                            formatDate(row.group.orderDateEpochDay),
                            row.group.shipPriority,
                            ExactMoney.format(row.revenueUnits)
                    ));
                }
            }
        }

        if (measureMetrics) {
            long completed = System.nanoTime();
            latency.add((completed - batch.createdNanos) / 1_000L);
            completedUpdates.add(1L);
            firstCreatedNanos.add(batch.createdNanos);
            lastCompletedNanos.add(completed);
        }
    }

    private Map<GroupKey, GroupDelta> combine(SequenceBatch batch) {
        Map<GroupKey, GroupDelta> combined = new HashMap<>();
        for (List<GroupDelta> shardDeltas
                : new TreeMap<>(batch.deltasByShard).values()) {
            for (GroupDelta delta : shardDeltas) {
                GroupDelta current = combined.get(delta.group);
                if (current == null) {
                    combined.put(
                            delta.group,
                            new GroupDelta(delta.group, delta.amount, delta.reason));
                } else {
                    current.amount = Math.addExact(current.amount, delta.amount);
                    if (!current.reason.equals(delta.reason)) {
                        current.reason = "mixed-update";
                    }
                    if (current.amount == 0L) {
                        combined.remove(delta.group);
                    }
                }
            }
        }
        return combined;
    }

    private void ensureRankingLoaded() throws Exception {
        if (rankingLoaded) {
            return;
        }
        for (Map.Entry<GroupKey, Long> entry : revenueByGroup.entries()) {
            ranking.add(new RankedGroup(entry.getKey(), entry.getValue()));
        }
        rankingLoaded = true;
    }

    private List<RankedGroup> topTen() {
        List<RankedGroup> rows = new ArrayList<>(10);
        int count = 0;
        for (RankedGroup row : ranking) {
            rows.add(row);
            count++;
            if (count == 10) {
                break;
            }
        }
        return rows;
    }

    private static boolean sameRanking(List<RankedGroup> first, List<RankedGroup> second) {
        if (first.size() != second.size()) {
            return false;
        }
        for (int index = 0; index < first.size(); index++) {
            RankedGroup left = first.get(index);
            RankedGroup right = second.get(index);
            if (left.revenueUnits != right.revenueUnits
                    || !left.group.equals(right.group)) {
                return false;
            }
        }
        return true;
    }

    private static String formatGroup(long sequence, GroupDelta delta, long current) {
        return String.format(
                Locale.US,
                "GROUP|%d|%s|%d|%s|%d|%s|%s|%s",
                sequence,
                current == 0L ? "DELETE_GROUP" : "UPSERT_GROUP",
                delta.group.orderKey,
                formatDate(delta.group.orderDateEpochDay),
                delta.group.shipPriority,
                ExactMoney.format(delta.amount),
                ExactMoney.format(current),
                delta.reason
        );
    }

    private static String formatDate(int epochDay) {
        return LocalDate.ofEpochDay(epochDay).toString();
    }
}
