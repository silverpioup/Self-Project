package edu.hkust.ip.flink.tests;

import org.junit.jupiter.api.Test;

import edu.hkust.ip.flink.ExactMoney;
import edu.hkust.ip.flink.Q3Types.Update;
import edu.hkust.ip.flink.RoutingPlan;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchQ3ContinuousJobTest {
    @Test
    void exactRevenueUsesFourDecimalPlaces() {
        assertEquals(9_500_000L, ExactMoney.revenueUnits("1000.00", "0.05"));
        assertEquals("950.0000", ExactMoney.format(9_500_000L));
        assertEquals(1_172_775L, ExactMoney.revenueUnits("123.45", "0.05"));
    }

    @Test
    void invalidDiscountIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ExactMoney.revenueUnits("10.00", "1.01"));
    }

    @Test
    void everyLogicalShardMapsToOneDistinctSubtask() {
        for (int parallelism : new int[]{1, 2, 4, 8, 16}) {
            RoutingPlan plan = RoutingPlan.forParallelism(parallelism);
            Set<Integer> targets = new HashSet<>();
            for (int shard = 0; shard < parallelism; shard++) {
                int target = plan.targetSubtask(shard, parallelism);
                targets.add(target);
                assertEquals(shard, target);
            }
            assertEquals(parallelism, targets.size());
        }
    }

    @Test
    void updateParserRequiresExactFieldCount() {
        Update update = Update.parse(
                "+|lineitem|10|1|100.00|0.05|1995-03-20");
        assertTrue(update.insert);
        assertEquals("lineitem", update.table);
        assertEquals(5, update.fields.length);
        assertThrows(
                IllegalArgumentException.class,
                () -> Update.parse("+|customer|1|BUILDING|extra"));
    }
}
