package edu.hkust.ip.flink;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.io.Serializable;
import java.util.Arrays;

public final class LatencyAccumulator
        implements Accumulator<Long, LatencyAccumulator.Summary> {
    private static final long serialVersionUID = 1L;
    private static final int BUCKET_WIDTH_US = 10;
    private static final int REGULAR_BUCKETS = 10_000;

    private long count;
    private long sumMicros;
    private long maximumMicros;
    private long[] buckets = new long[REGULAR_BUCKETS + 1];

    @Override
    public void add(Long value) {
        long micros = Math.max(0L, value);
        count++;
        sumMicros += micros;
        maximumMicros = Math.max(maximumMicros, micros);
        int bucket = micros >= (long) REGULAR_BUCKETS * BUCKET_WIDTH_US
                ? REGULAR_BUCKETS
                : (int) (micros / BUCKET_WIDTH_US);
        buckets[bucket]++;
    }

    @Override
    public Summary getLocalValue() {
        return new Summary(count, sumMicros, maximumMicros, buckets.clone());
    }

    @Override
    public void resetLocal() {
        count = 0L;
        sumMicros = 0L;
        maximumMicros = 0L;
        Arrays.fill(buckets, 0L);
    }

    @Override
    public void merge(Accumulator<Long, Summary> other) {
        Summary summary = other.getLocalValue();
        count += summary.count;
        sumMicros += summary.sumMicros;
        maximumMicros = Math.max(maximumMicros, summary.maximumMicros);
        for (int index = 0; index < buckets.length; index++) {
            buckets[index] += summary.buckets[index];
        }
    }

    @Override
    public Accumulator<Long, Summary> clone() {
        LatencyAccumulator copy = new LatencyAccumulator();
        copy.count = count;
        copy.sumMicros = sumMicros;
        copy.maximumMicros = maximumMicros;
        copy.buckets = buckets.clone();
        return copy;
    }

    public static final class Summary implements Serializable {
        private static final long serialVersionUID = 1L;
        public long count;
        public long sumMicros;
        public long maximumMicros;
        public long[] buckets;

        public Summary() {
        }

        Summary(long count, long sumMicros, long maximumMicros, long[] buckets) {
            this.count = count;
            this.sumMicros = sumMicros;
            this.maximumMicros = maximumMicros;
            this.buckets = buckets;
        }

        public double meanMicros() {
            return count == 0L ? 0.0 : (double) sumMicros / count;
        }

        public long percentile(double fraction) {
            if (count == 0L) {
                return 0L;
            }
            long target = Math.max(1L, (long) Math.ceil(fraction * count));
            long cumulative = 0L;
            for (int index = 0; index < buckets.length; index++) {
                cumulative += buckets[index];
                if (cumulative >= target) {
                    return index == REGULAR_BUCKETS
                            ? maximumMicros
                            : (long) index * BUCKET_WIDTH_US;
                }
            }
            return maximumMicros;
        }
    }
}
