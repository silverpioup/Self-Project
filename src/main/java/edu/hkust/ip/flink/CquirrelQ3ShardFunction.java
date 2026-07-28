package edu.hkust.ip.flink;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static edu.hkust.ip.flink.Q3Types.CustomerState;
import static edu.hkust.ip.flink.Q3Types.GroupDelta;
import static edu.hkust.ip.flink.Q3Types.GroupKey;
import static edu.hkust.ip.flink.Q3Types.LineItemBucket;
import static edu.hkust.ip.flink.Q3Types.LineItemState;
import static edu.hkust.ip.flink.Q3Types.LongSet;
import static edu.hkust.ip.flink.Q3Types.OrderState;
import static edu.hkust.ip.flink.Q3Types.RoutedUpdate;
import static edu.hkust.ip.flink.Q3Types.ShardResult;
import static edu.hkust.ip.flink.Q3Types.Update;

/**
 * Q3 specialization of Cquirrel's bottom-up live-tuple maintenance.
 *
 * <p>The foreign-key DAG is lineitem -> orders -> customer. Because it is a
 * chain, each non-leaf tuple has one child relation and its Ic value is either
 * zero or one. A root lineitem contributes to the aggregate exactly while it
 * is alive.</p>
 */
final class CquirrelQ3ShardFunction
        extends KeyedProcessFunction<Integer, RoutedUpdate, ShardResult> {
    static final int Q3_DATE_EPOCH_DAY =
            (int) LocalDate.parse("1995-03-15").toEpochDay();

    private transient MapState<Long, CustomerState> customers;
    private transient MapState<Long, OrderState> orders;
    private transient MapState<Long, LongSet> ordersByCustomer;
    private transient MapState<Long, LineItemBucket> lineItemsByOrder;
    private transient LongCounter processedRecords;

    @Override
    public void open(OpenContext openContext) {
        customers = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("cquirrel-customers", Long.class, CustomerState.class));
        orders = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("cquirrel-orders", Long.class, OrderState.class));
        ordersByCustomer = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("orders-by-customer", Long.class, LongSet.class));
        lineItemsByOrder = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("lineitems-by-order", Long.class, LineItemBucket.class));

        processedRecords = new LongCounter();
        getRuntimeContext().addAccumulator(
                TpchQ3ContinuousJob.workerAccumulatorName(
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()),
                processedRecords);
    }

    @Override
    public void processElement(
            RoutedUpdate routed,
            Context context,
            Collector<ShardResult> out
    ) throws Exception {
        processedRecords.add(1L);
        Map<GroupKey, GroupDelta> pending = new LinkedHashMap<>();
        Update update = routed.value.update;
        switch (update.table) {
            case "customer":
                processCustomer(update, pending);
                break;
            case "orders":
                processOrder(update, pending);
                break;
            case "lineitem":
                processLineItem(update, pending);
                break;
            default:
                throw new IllegalArgumentException("Unknown table: " + update.table);
        }
        out.collect(new ShardResult(
                routed.value.sequence,
                routed.value.createdNanos,
                routed.shard,
                routed.expectedParts,
                new ArrayList<>(pending.values())
        ));
    }

    private void processCustomer(Update update, Map<GroupKey, GroupDelta> pending)
            throws Exception {
        long custKey = Long.parseLong(update.fields[0]);
        CustomerState existing = customers.get(custKey);
        if (!update.insert) {
            requireExisting(existing, "customer", Long.toString(custKey));
            transitionOrdersForCustomer(custKey, false, "customer-delete", pending);
            customers.remove(custKey);
            return;
        }
        requireAbsent(existing, "customer", Long.toString(custKey));
        CustomerState customer = new CustomerState(custKey, update.fields[1]);
        customers.put(custKey, customer);
        transitionOrdersForCustomer(
                custKey, customer.alive, "customer-insert", pending);
    }

    private void transitionOrdersForCustomer(
            long custKey,
            boolean customerAlive,
            String reason,
            Map<GroupKey, GroupDelta> pending
    ) throws Exception {
        LongSet orderKeys = ordersByCustomer.get(custKey);
        if (orderKeys == null) {
            return;
        }
        for (Long orderKey : orderKeys.values) {
            OrderState order = orders.get(orderKey);
            if (order == null) {
                continue;
            }
            int nextChildCount = customerAlive ? 1 : 0;
            boolean nextAlive = nextChildCount == 1 && qualifies(order);
            transitionOrder(order, nextChildCount, nextAlive, reason, pending);
        }
    }

    private void processOrder(Update update, Map<GroupKey, GroupDelta> pending)
            throws Exception {
        long orderKey = Long.parseLong(update.fields[0]);
        OrderState existing = orders.get(orderKey);
        if (!update.insert) {
            requireExisting(existing, "orders", Long.toString(orderKey));
            transitionOrder(existing, 0, false, "order-delete", pending);
            removeOrderIndex(existing);
            orders.remove(orderKey);
            return;
        }
        requireAbsent(existing, "orders", Long.toString(orderKey));
        OrderState order = new OrderState(
                orderKey,
                Long.parseLong(update.fields[1]),
                (int) LocalDate.parse(update.fields[2]).toEpochDay(),
                Integer.parseInt(update.fields[3])
        );
        CustomerState customer = customers.get(order.custKey);
        int childCount = customer != null && customer.alive ? 1 : 0;
        order.childMatchCount = childCount;
        order.alive = childCount == 1 && qualifies(order);
        orders.put(orderKey, order);
        addOrderIndex(order);
        transitionLineItems(
                order, order.alive, "order-insert", pending);
    }

    private void transitionOrder(
            OrderState order,
            int nextChildCount,
            boolean nextAlive,
            String reason,
            Map<GroupKey, GroupDelta> pending
    ) throws Exception {
        boolean changed = order.alive != nextAlive;
        order.childMatchCount = nextChildCount;
        order.alive = nextAlive;
        orders.put(order.orderKey, order);
        if (changed) {
            transitionLineItems(order, nextAlive, reason, pending);
        }
    }

    private void transitionLineItems(
            OrderState order,
            boolean orderAlive,
            String reason,
            Map<GroupKey, GroupDelta> pending
    ) throws Exception {
        LineItemBucket bucket = lineItemsByOrder.get(order.orderKey);
        if (bucket == null) {
            return;
        }
        for (LineItemState item : bucket.values.values()) {
            int nextChildCount = orderAlive ? 1 : 0;
            boolean nextAlive = nextChildCount == 1 && qualifies(item);
            if (item.alive != nextAlive) {
                addDelta(
                        pending,
                        groupFor(order),
                        nextAlive ? item.revenueUnits : -item.revenueUnits,
                        reason
                );
            }
            item.childMatchCount = nextChildCount;
            item.alive = nextAlive;
        }
        lineItemsByOrder.put(order.orderKey, bucket);
    }

    private void processLineItem(Update update, Map<GroupKey, GroupDelta> pending)
            throws Exception {
        long orderKey = Long.parseLong(update.fields[0]);
        int lineNumber = Integer.parseInt(update.fields[1]);
        LineItemBucket bucket = lineItemsByOrder.get(orderKey);
        LineItemState existing = bucket == null ? null : bucket.values.get(lineNumber);
        String key = orderKey + "/" + lineNumber;
        if (!update.insert) {
            requireExisting(existing, "lineitem", key);
            if (existing.alive) {
                OrderState order = orders.get(orderKey);
                if (order == null) {
                    throw new IllegalStateException(
                            "Alive lineitem has no order: " + key);
                }
                addDelta(
                        pending,
                        groupFor(order),
                        -existing.revenueUnits,
                        "lineitem-delete"
                );
            }
            bucket.values.remove(lineNumber);
            if (bucket.values.isEmpty()) {
                lineItemsByOrder.remove(orderKey);
            } else {
                lineItemsByOrder.put(orderKey, bucket);
            }
            return;
        }

        requireAbsent(existing, "lineitem", key);
        if (bucket == null) {
            bucket = new LineItemBucket();
        }
        LineItemState item = new LineItemState(
                orderKey,
                lineNumber,
                ExactMoney.revenueUnits(update.fields[2], update.fields[3]),
                (int) LocalDate.parse(update.fields[4]).toEpochDay()
        );
        OrderState order = orders.get(orderKey);
        item.childMatchCount = order != null && order.alive ? 1 : 0;
        item.alive = item.childMatchCount == 1 && qualifies(item);
        bucket.values.put(lineNumber, item);
        lineItemsByOrder.put(orderKey, bucket);
        if (item.alive) {
            addDelta(
                    pending,
                    groupFor(order),
                    item.revenueUnits,
                    "lineitem-insert"
            );
        }
    }

    private void addOrderIndex(OrderState order) throws Exception {
        LongSet orderKeys = ordersByCustomer.get(order.custKey);
        if (orderKeys == null) {
            orderKeys = new LongSet();
        }
        orderKeys.values.add(order.orderKey);
        ordersByCustomer.put(order.custKey, orderKeys);
    }

    private void removeOrderIndex(OrderState order) throws Exception {
        LongSet orderKeys = ordersByCustomer.get(order.custKey);
        if (orderKeys == null || !orderKeys.values.remove(order.orderKey)) {
            throw new IllegalStateException(
                    "Missing customer-to-order index for order " + order.orderKey);
        }
        if (orderKeys.values.isEmpty()) {
            ordersByCustomer.remove(order.custKey);
        } else {
            ordersByCustomer.put(order.custKey, orderKeys);
        }
    }

    private static boolean qualifies(OrderState order) {
        return order.orderDateEpochDay < Q3_DATE_EPOCH_DAY;
    }

    private static boolean qualifies(LineItemState item) {
        return item.shipDateEpochDay > Q3_DATE_EPOCH_DAY;
    }

    private static GroupKey groupFor(OrderState order) {
        return new GroupKey(
                order.orderKey, order.orderDateEpochDay, order.shipPriority);
    }

    private static void addDelta(
            Map<GroupKey, GroupDelta> pending,
            GroupKey group,
            long amount,
            String reason
    ) {
        if (amount == 0L) {
            return;
        }
        GroupDelta delta = pending.get(group);
        if (delta == null) {
            pending.put(group, new GroupDelta(group, amount, reason));
        } else {
            delta.amount = Math.addExact(delta.amount, amount);
            if (!delta.reason.equals(reason)) {
                delta.reason = "mixed-update";
            }
            if (delta.amount == 0L) {
                pending.remove(group);
            }
        }
    }

    private static void requireExisting(Object value, String table, String key) {
        if (value == null) {
            throw new IllegalStateException(
                    "Cannot delete missing " + table + " tuple " + key);
        }
    }

    private static void requireAbsent(Object value, String table, String key) {
        if (value != null) {
            throw new IllegalStateException(
                    "Cannot insert duplicate " + table + " key " + key
                            + "; represent a replacement as delete followed by insert.");
        }
    }
}
