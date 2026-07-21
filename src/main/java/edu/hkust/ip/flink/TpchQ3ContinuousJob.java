package edu.hkust.ip.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TpchQ3ContinuousJob {
    private static final LocalDate Q3_DATE = LocalDate.parse("1995-03-15");

    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : "data/sample_updates.csv";
        int parallelism = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String outputMode = args.length > 2 ? args[2].toLowerCase(Locale.ROOT) : "print";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataStream<String> updates = env.readTextFile(input)
                .filter(line -> {
                    String trimmed = line.trim();
                    return !trimmed.isEmpty() && !trimmed.startsWith("#");
                });

        DataStream<String> deltas = updates.process(new Q3ProcessFunction())
                .name("tpch-q3-continuous-maintenance")
                .setParallelism(1);

        if ("quiet".equals(outputMode)) {
            deltas.filter(line -> false)
                    .name("discard-delta-output")
                    .print()
                    .setParallelism(1);
        } else {
            deltas.print().setParallelism(1);
        }

        env.execute("TPC-H Q3 Continuous Maintenance");
    }

    public static final class Q3ProcessFunction extends ProcessFunction<String, String> {
        private final Map<Long, Customer> customers = new HashMap<>();
        private final Map<Long, OrderRow> orders = new HashMap<>();
        private final Map<Long, Set<Long>> ordersByCustomer = new HashMap<>();
        private final Map<Long, Map<Integer, LineItem>> lineItemsByOrder = new HashMap<>();
        private final Map<GroupKey, Double> revenueByGroup = new HashMap<>();
        private long sequence = 0L;

        @Override
        public void processElement(String value, Context context, Collector<String> out) {
            sequence++;
            Update update = Update.parse(value);
            Map<GroupKey, PendingDelta> pendingDeltas = new HashMap<>();
            switch (update.table) {
                case "customer":
                    processCustomer(update, pendingDeltas);
                    break;
                case "orders":
                    processOrder(update, pendingDeltas);
                    break;
                case "lineitem":
                    processLineItem(update, pendingDeltas);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown table: " + update.table);
            }
            flushRevenueDeltas(pendingDeltas, out);
        }

        private void processCustomer(Update update, Map<GroupKey, PendingDelta> pendingDeltas) {
            long custKey = Long.parseLong(update.fields[0]);
            Customer old = customers.get(custKey);
            if (old != null) {
                emitCustomerContribution(old, -1.0, "customer-delete-or-replace", pendingDeltas);
                customers.remove(custKey);
            }
            if (update.insert) {
                Customer current = new Customer(custKey, update.fields[1]);
                customers.put(custKey, current);
                emitCustomerContribution(current, 1.0, "customer-insert", pendingDeltas);
            }
        }

        private void emitCustomerContribution(
                Customer customer,
                double sign,
                String reason,
                Map<GroupKey, PendingDelta> pendingDeltas
        ) {
            if (!customer.isBuilding()) {
                return;
            }
            for (Long orderKey : ordersByCustomer.getOrDefault(customer.custKey, Set.of())) {
                OrderRow order = orders.get(orderKey);
                if (order != null && order.orderDate.isBefore(Q3_DATE)) {
                    emitOrderLineItemContributions(order, sign, reason, pendingDeltas);
                }
            }
        }

        private void processOrder(Update update, Map<GroupKey, PendingDelta> pendingDeltas) {
            long orderKey = Long.parseLong(update.fields[0]);
            OrderRow old = orders.get(orderKey);
            if (old != null) {
                emitOrderContributionIfMatched(old, -1.0, "order-delete-or-replace", pendingDeltas);
                removeOrderIndex(old);
                orders.remove(orderKey);
            }
            if (update.insert) {
                OrderRow current = new OrderRow(
                        orderKey,
                        Long.parseLong(update.fields[1]),
                        LocalDate.parse(update.fields[2]),
                        Integer.parseInt(update.fields[3])
                );
                orders.put(orderKey, current);
                ordersByCustomer.computeIfAbsent(current.custKey, ignored -> new HashSet<>()).add(orderKey);
                emitOrderContributionIfMatched(current, 1.0, "order-insert", pendingDeltas);
            }
        }

        private void removeOrderIndex(OrderRow order) {
            Set<Long> orderKeys = ordersByCustomer.get(order.custKey);
            if (orderKeys != null) {
                orderKeys.remove(order.orderKey);
                if (orderKeys.isEmpty()) {
                    ordersByCustomer.remove(order.custKey);
                }
            }
        }

        private void emitOrderContributionIfMatched(
                OrderRow order,
                double sign,
                String reason,
                Map<GroupKey, PendingDelta> pendingDeltas
        ) {
            Customer customer = customers.get(order.custKey);
            if (customer != null && customer.isBuilding() && order.orderDate.isBefore(Q3_DATE)) {
                emitOrderLineItemContributions(order, sign, reason, pendingDeltas);
            }
        }

        private void emitOrderLineItemContributions(
                OrderRow order,
                double sign,
                String reason,
                Map<GroupKey, PendingDelta> pendingDeltas
        ) {
            Map<Integer, LineItem> items = lineItemsByOrder.getOrDefault(order.orderKey, Map.of());
            for (LineItem item : items.values()) {
                if (item.shipDate.isAfter(Q3_DATE)) {
                    GroupKey group = new GroupKey(order.orderKey, order.orderDate, order.shipPriority);
                    addRevenueDelta(pendingDeltas, group, sign * item.revenue(), reason);
                }
            }
        }

        private void processLineItem(Update update, Map<GroupKey, PendingDelta> pendingDeltas) {
            long orderKey = Long.parseLong(update.fields[0]);
            int lineNumber = Integer.parseInt(update.fields[1]);
            Map<Integer, LineItem> items = lineItemsByOrder.computeIfAbsent(orderKey, ignored -> new HashMap<>());
            LineItem old = items.get(lineNumber);
            if (old != null) {
                emitLineItemContributionIfMatched(old, -1.0, "lineitem-delete-or-replace", pendingDeltas);
                items.remove(lineNumber);
            }
            if (update.insert) {
                LineItem current = new LineItem(
                        orderKey,
                        lineNumber,
                        Double.parseDouble(update.fields[2]),
                        Double.parseDouble(update.fields[3]),
                        LocalDate.parse(update.fields[4])
                );
                items.put(lineNumber, current);
                emitLineItemContributionIfMatched(current, 1.0, "lineitem-insert", pendingDeltas);
            }
            if (items.isEmpty()) {
                lineItemsByOrder.remove(orderKey);
            }
        }

        private void emitLineItemContributionIfMatched(
                LineItem item,
                double sign,
                String reason,
                Map<GroupKey, PendingDelta> pendingDeltas
        ) {
            OrderRow order = orders.get(item.orderKey);
            if (order == null || !order.orderDate.isBefore(Q3_DATE) || !item.shipDate.isAfter(Q3_DATE)) {
                return;
            }
            Customer customer = customers.get(order.custKey);
            if (customer == null || !customer.isBuilding()) {
                return;
            }
            GroupKey group = new GroupKey(order.orderKey, order.orderDate, order.shipPriority);
            addRevenueDelta(pendingDeltas, group, sign * item.revenue(), reason);
        }

        private void addRevenueDelta(
                Map<GroupKey, PendingDelta> pendingDeltas,
                GroupKey group,
                double delta,
                String reason
        ) {
            if (Math.abs(delta) < 1.0e-9) {
                return;
            }
            pendingDeltas.computeIfAbsent(group, ignored -> new PendingDelta()).add(delta, reason);
        }

        private void flushRevenueDeltas(Map<GroupKey, PendingDelta> pendingDeltas, Collector<String> out) {
            for (Map.Entry<GroupKey, PendingDelta> entry : pendingDeltas.entrySet()) {
                GroupKey group = entry.getKey();
                double delta = entry.getValue().amount;
                if (Math.abs(delta) < 1.0e-9) {
                    continue;
                }
                double next = revenueByGroup.getOrDefault(group, 0.0) + delta;
                if (Math.abs(next) < 1.0e-9) {
                    revenueByGroup.remove(group);
                    next = 0.0;
                } else {
                    revenueByGroup.put(group, next);
                }
                out.collect(String.format(
                        Locale.US,
                        "%d|%s|%d|%s|%d|%.2f|%.2f|%s",
                        sequence,
                        next == 0.0 ? "DELETE_GROUP" : "UPSERT_GROUP",
                        group.orderKey,
                        group.orderDate,
                        group.shipPriority,
                        delta,
                        next,
                        entry.getValue().reason
                ));
            }
        }

        private static final class PendingDelta {
            private double amount = 0.0;
            private String reason;

            private void add(double delta, String nextReason) {
                amount += delta;
                if (reason == null) {
                    reason = nextReason;
                } else if (!reason.equals(nextReason)) {
                    reason = "mixed-update";
                }
            }
        }
    }

    static final class Update {
        final boolean insert;
        final String table;
        final String[] fields;

        private Update(boolean insert, String table, String[] fields) {
            this.insert = insert;
            this.table = table;
            this.fields = fields;
        }

        static Update parse(String line) {
            String[] parts = line.split("\\|", -1);
            if (parts.length < 3) {
                throw new IllegalArgumentException("Bad update line: " + line);
            }
            boolean insert;
            if ("+".equals(parts[0])) {
                insert = true;
            } else if ("-".equals(parts[0])) {
                insert = false;
            } else {
                throw new IllegalArgumentException("Operation must be + or -: " + line);
            }
            String[] fields = new String[parts.length - 2];
            System.arraycopy(parts, 2, fields, 0, fields.length);
            return new Update(insert, parts[1].toLowerCase(Locale.ROOT), fields);
        }
    }

    static final class Customer implements Serializable {
        final long custKey;
        final String marketSegment;

        Customer(long custKey, String marketSegment) {
            this.custKey = custKey;
            this.marketSegment = marketSegment;
        }

        boolean isBuilding() {
            return "BUILDING".equalsIgnoreCase(marketSegment);
        }
    }

    static final class OrderRow implements Serializable {
        final long orderKey;
        final long custKey;
        final LocalDate orderDate;
        final int shipPriority;

        OrderRow(long orderKey, long custKey, LocalDate orderDate, int shipPriority) {
            this.orderKey = orderKey;
            this.custKey = custKey;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
        }
    }

    static final class LineItem implements Serializable {
        final long orderKey;
        final int lineNumber;
        final double extendedPrice;
        final double discount;
        final LocalDate shipDate;

        LineItem(long orderKey, int lineNumber, double extendedPrice, double discount, LocalDate shipDate) {
            this.orderKey = orderKey;
            this.lineNumber = lineNumber;
            this.extendedPrice = extendedPrice;
            this.discount = discount;
            this.shipDate = shipDate;
        }

        double revenue() {
            return extendedPrice * (1.0 - discount);
        }
    }

    static final class GroupKey implements Serializable {
        final long orderKey;
        final LocalDate orderDate;
        final int shipPriority;

        GroupKey(long orderKey, LocalDate orderDate, int shipPriority) {
            this.orderKey = orderKey;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GroupKey)) {
                return false;
            }
            GroupKey other = (GroupKey) o;
            return orderKey == other.orderKey
                    && shipPriority == other.shipPriority
                    && orderDate.equals(other.orderDate);
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(orderKey);
            result = 31 * result + orderDate.hashCode();
            result = 31 * result + Integer.hashCode(shipPriority);
            return result;
        }
    }

    public static List<Map.Entry<GroupKey, Double>> top10(Map<GroupKey, Double> revenueByGroup) {
        List<Map.Entry<GroupKey, Double>> entries = new ArrayList<>(revenueByGroup.entrySet());
        entries.sort(Comparator
                .<Map.Entry<GroupKey, Double>>comparingDouble(Map.Entry::getValue).reversed()
                .thenComparing(entry -> entry.getKey().orderDate));
        return entries.subList(0, Math.min(10, entries.size()));
    }
}
