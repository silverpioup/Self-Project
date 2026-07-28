package edu.hkust.ip.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static edu.hkust.ip.flink.Q3Types.RoutedUpdate;
import static edu.hkust.ip.flink.Q3Types.SequencedUpdate;
import static edu.hkust.ip.flink.Q3Types.ShardResult;
import static edu.hkust.ip.flink.Q3Types.Update;

public final class TpchQ3ContinuousJob {
    static final String LATENCY_ACCUMULATOR = "completed-update-latency";
    static final String COMPLETED_UPDATES_ACCUMULATOR = "completed-input-updates";
    static final String FIRST_CREATED_ACCUMULATOR = "first-created-nanos";
    static final String LAST_COMPLETED_ACCUMULATOR = "last-completed-nanos";
    private static final String WORKER_PREFIX = "q3-worker-";

    private TpchQ3ContinuousJob() {
    }

    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : "data/sample_updates.csv";
        int parallelism = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String outputMode = args.length > 2
                ? args[2].toLowerCase(Locale.ROOT)
                : "print";
        long checkpointInterval = args.length > 3 ? Long.parseLong(args[3]) : 0L;
        if (parallelism < 1 || parallelism > RoutingPlan.MAX_PARALLELISM) {
            throw new IllegalArgumentException(
                    "Parallelism must be between 1 and "
                            + RoutingPlan.MAX_PARALLELISM + ".");
        }
        if (!Set.of("print", "quiet", "metrics", "both").contains(outputMode)) {
            throw new IllegalArgumentException(
                    "Output mode must be print, quiet, metrics, or both.");
        }
        if (checkpointInterval < 0L) {
            throw new IllegalArgumentException("Checkpoint interval cannot be negative.");
        }

        boolean printResults = "print".equals(outputMode) || "both".equals(outputMode);
        boolean measureMetrics = "metrics".equals(outputMode) || "both".equals(outputMode);
        RoutingPlan routingPlan = RoutingPlan.forParallelism(parallelism);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        if (checkpointInterval > 0L) {
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        }

        DataStream<String> source = env.readTextFile(input)
                .name("ordered-update-file-source")
                .setParallelism(1)
                .filter(line -> {
                    String trimmed = line.trim();
                    return !trimmed.isEmpty() && !trimmed.startsWith("#");
                })
                .name("remove-comments")
                .setParallelism(1);

        DataStream<SequencedUpdate> sequenced = source
                .process(new SequencingFunction())
                .name("assign-global-sequence")
                .setParallelism(1);

        DataStream<RoutedUpdate> routed = sequenced
                .flatMap(new ShardRoutingFunction(parallelism, routingPlan))
                .name("route-to-one-key-per-subtask")
                .setParallelism(1);

        DataStream<ShardResult> shardResults = routed
                .keyBy(update -> update.partitionKey)
                .process(new CquirrelQ3ShardFunction())
                .name("cquirrel-q3-live-tuple-maintenance")
                .setParallelism(parallelism)
                .setMaxParallelism(RoutingPlan.MAX_PARALLELISM);

        DataStream<String> results = shardResults
                .keyBy(ignored -> 0)
                .process(new GlobalQ3ResultFunction(printResults, measureMetrics))
                .name("ordered-global-aggregation-and-top10")
                .setParallelism(1);

        results.print().name("q3-result-output").setParallelism(1);

        JobExecutionResult execution = env.execute(
                "Cquirrel TPC-H Q3 Continuous Maintenance");
        if (measureMetrics) {
            printMetrics(execution, parallelism);
        }
    }

    static String workerAccumulatorName(int subtask) {
        return WORKER_PREFIX + subtask + "-records";
    }

    private static void printMetrics(JobExecutionResult result, int parallelism) {
        LatencyAccumulator.Summary latency =
                result.getAccumulatorResult(LATENCY_ACCUMULATOR);
        Long completed = result.getAccumulatorResult(COMPLETED_UPDATES_ACCUMULATOR);
        Long firstCreated = result.getAccumulatorResult(FIRST_CREATED_ACCUMULATOR);
        Long lastCompleted = result.getAccumulatorResult(LAST_COMPLETED_ACCUMULATOR);
        if (latency == null) {
            latency = new LatencyAccumulator.Summary();
            latency.buckets = new long[0];
        }
        long count = completed == null ? 0L : completed;
        long elapsedNanos = firstCreated == null || lastCompleted == null
                ? 0L
                : Math.max(0L, lastCompleted - firstCreated);
        System.out.printf(
                Locale.US,
                "METRICS|%d|%.2f|%d|%d|%d%n",
                count,
                latency.meanMicros(),
                latency.percentile(0.50),
                latency.percentile(0.95),
                latency.percentile(0.99)
        );
        System.out.printf(
                Locale.US,
                "PROCESSING|%d|%.6f|%s%n",
                count,
                elapsedNanos / 1_000_000_000.0,
                ExactMoney.formatPerSecond(count, elapsedNanos)
        );

        Map<String, Object> accumulators = result.getAllAccumulatorResults();
        List<Long> workerCounts = new ArrayList<>(parallelism);
        int active = 0;
        for (int subtask = 0; subtask < parallelism; subtask++) {
            Object value = accumulators.get(workerAccumulatorName(subtask));
            long workerCount = value instanceof Number ? ((Number) value).longValue() : 0L;
            workerCounts.add(workerCount);
            if (workerCount > 0L) {
                active++;
            }
        }
        StringBuilder line = new StringBuilder()
                .append("WORKERS|")
                .append(active)
                .append('|')
                .append(parallelism);
        for (int index = 0; index < workerCounts.size(); index++) {
            line.append('|').append(index).append(':').append(workerCounts.get(index));
        }
        System.out.println(line);
    }

    private static final class SequencingFunction
            extends ProcessFunction<String, SequencedUpdate>
            implements CheckpointedFunction {
        private long sequence;
        private transient ListState<Long> checkpointedSequence;

        @Override
        public void processElement(
                String value,
                Context context,
                Collector<SequencedUpdate> out
        ) {
            sequence++;
            out.collect(new SequencedUpdate(
                    sequence, System.nanoTime(), Update.parse(value)));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedSequence.clear();
            checkpointedSequence.add(sequence);
        }

        @Override
        public void initializeState(FunctionInitializationContext context)
                throws Exception {
            checkpointedSequence = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("global-input-sequence", Long.class));
            if (context.isRestored()) {
                for (Long restored : checkpointedSequence.get()) {
                    sequence = restored;
                }
            }
        }
    }

    private static final class ShardRoutingFunction
            extends RichFlatMapFunction<SequencedUpdate, RoutedUpdate> {
        private final int shardCount;
        private final RoutingPlan routingPlan;
        private transient LongCounter inputUpdates;

        private ShardRoutingFunction(int shardCount, RoutingPlan routingPlan) {
            this.shardCount = shardCount;
            this.routingPlan = routingPlan;
        }

        @Override
        public void open(OpenContext openContext) {
            inputUpdates = new LongCounter();
            getRuntimeContext().addAccumulator("input-update-count", inputUpdates);
        }

        @Override
        public void flatMap(SequencedUpdate value, Collector<RoutedUpdate> out) {
            inputUpdates.add(1L);
            if ("customer".equals(value.update.table)) {
                for (int shard = 0; shard < shardCount; shard++) {
                    out.collect(new RoutedUpdate(
                            shard,
                            routingPlan.partitionKey(shard),
                            shardCount,
                            value
                    ));
                }
                return;
            }
            long orderKey = Long.parseLong(value.update.fields[0]);
            int shard = (int) Math.floorMod(orderKey, (long) shardCount);
            out.collect(new RoutedUpdate(
                    shard,
                    routingPlan.partitionKey(shard),
                    1,
                    value
            ));
        }
    }
}
