package edu.hkust.ip.flink;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;
import java.util.Arrays;

public final class RoutingPlan implements Serializable {
    static final int MAX_PARALLELISM = 128;
    private static final long serialVersionUID = 1L;

    private final int[] partitionKeys;

    private RoutingPlan(int[] partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public static RoutingPlan forParallelism(int parallelism) {
        if (parallelism < 1 || parallelism > MAX_PARALLELISM) {
            throw new IllegalArgumentException(
                    "Parallelism must be between 1 and " + MAX_PARALLELISM + ".");
        }
        int[] keys = new int[parallelism];
        Arrays.fill(keys, Integer.MIN_VALUE);
        int remaining = parallelism;
        for (int candidate = 0; remaining > 0; candidate++) {
            int target = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    candidate, MAX_PARALLELISM, parallelism);
            if (keys[target] == Integer.MIN_VALUE) {
                keys[target] = candidate;
                remaining--;
            }
        }
        return new RoutingPlan(keys);
    }

    int partitionKey(int shard) {
        return partitionKeys[shard];
    }

    public int targetSubtask(int shard, int parallelism) {
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(
                partitionKey(shard), MAX_PARALLELISM, parallelism);
    }
}
