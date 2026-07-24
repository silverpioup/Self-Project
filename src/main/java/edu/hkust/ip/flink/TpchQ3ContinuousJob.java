package edu.hkust.ip.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TpchQ3ContinuousJob {
    private static final LocalDate Q3_DATE = LocalDate.parse("1995-03-15");
    private static final String LATENCY_ACCUMULATOR = "latency-micros";

    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : "data/sample_updates.csv";
        int parallelism = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String outputMode = args.length > 2 ? args[2].toLowerCase(Locale.ROOT) : "print";
        if (parallelism < 1) {
            throw new IllegalArgumentException("Parallelism must be at least 1.");
        }
        if (!Set.of("print", "quiet", "metrics", "both").contains(outputMode)) {
            throw new IllegalArgumentException("Output mode must be print, quiet, metrics, or both.");
        }

        boolean printDeltas = "print".equals(outputMode) || "both".equals(outputMode);
        boolean measureLatency = "metrics".equals(outputMode) || "both".equals(outputMode);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

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
                .process(new SequencingFunction(measureLatency))
                .name("assign-input-sequence")
                .setParallelism(1);

        DataStream<RoutedUpdate> routed = sequenced
                .flatMap(new ShardRoutingFunction(parallelism))
                .name("route-updates-to-order-shards")
                .setParallelism(1);

        DataStream<String> deltas = routed
                .keyBy(update -> update.shard)
                .process(new Q3ShardProcessFunction(printDeltas, measureLatency))
                .name("parallel-tpch-q3-maintenance")
                .setParallelism(parallelism);

        deltas.print().name("delta-output").setParallelism(1);

        JobExecutionResult result = env.execute("TPC-H Q3 Continuous Maintenance");
        if (measureLatency) {
            List<Long> latencies = result.getAccumulatorResult(LATENCY_ACCUMULATOR);
            printLatencySummary(latencies == null ? List.of() : latencies);
        }
    }

    private static void printLatencySummary(List<Long> values) {
        if (values.isEmpty()) {
            System.out.println("METRICS|0|0.00|0|0|0");
            return;
        }
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        double mean = sorted.stream().mapToLong(Long::longValue).average().orElse(0.0);
        System.out.printf(
                Locale.US,
                "METRICS|%d|%.2f|%d|%d|%d%n",
                sorted.size(),
                mean,
                percentile(sorted, 0.50),
                percentile(sorted, 0.95),
                percentile(sorted, 0.99)
        );
    }

    private static long percentile(List<Long> sorted, double percentile) {
        int index = Math.max(0, (int) Math.ceil(percentile * sorted.size()) - 1);
        return sorted.get(Math.min(index, sorted.size() - 1));
    }

    private static final class SequencingFunction extends ProcessFunction<String, SequencedUpdate> {
        private final boolean measureLatency;
        private long sequence;

        private SequencingFunction(boolean measureLatency) {
            this.measureLatency = measureLatency;
        }

        @Override
        public void processElement(String value, Context context, Collector<SequencedUpdate> out) {
            sequence++;
            out.collect(new SequencedUpdate(
                    sequence,
                    measureLatency ? System.nanoTime() : 0L,
                    Update.parse(value)
            ));
        }
    }

    private static final class ShardRoutingFunction
            extends RichFlatMapFunction<SequencedUpdate, RoutedUpdate> {
        private final int shardCount;

        private ShardRoutingFunction(int shardCount) {
            this.shardCount = shardCount;
        }

        @Override
        public void flatMap(SequencedUpdate value, Collector<RoutedUpdate> out) {
            if ("customer".equals(value.update.table)) {
                for (int shard = 0; shard < shardCount; shard++) {
                    out.collect(new RoutedUpdate(shard, value));
                }
                return;
            }
            long orderKey = Long.parseLong(value.update.fields[0]);
            int shard = (int) Math.floorMod(orderKey, (long) shardCount);
            out.collect(new RoutedUpdate(shard, value));
        }
    }

    public static final class Q3ShardProcessFunction
            extends KeyedProcessFunction<Integer, RoutedUpdate, String> {
        private final boolean printDeltas;
        private final boolean measureLatency;

        private transient MapState<Long, Customer> customers;
        private transient MapState<Long, OrderRow> orders;
        private transient MapState<Long, OrderKeySet> ordersByCustomer;
        private transient MapState<Long, LineItemBucket> lineItemsByOrder;
        private transient MapState<GroupKey, Double> revenueByGroup;
        private transient ListAccumulator<Long> latencyMicros;

        public Q3ShardProcessFunction(boolean printDeltas, boolean measureLatency) {
            this.printDeltas = printDeltas;
            this.measureLatency = measureLatency;
        }

        @Override
        public void open(OpenContext openContext) {
            customers = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("customers", Long.class, Customer.class));
            orders = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("orders", Long.class, OrderRow.class));
            ordersByCustomer = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("orders-by-customer", Long.class, OrderKeySet.class));
            lineItemsByOrder = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("lineitems-by-order", Long.class, LineItemBucket.class));
            revenueByGroup = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("revenue-by-group", GroupKey.class, Double.class));

            if (measureLatency) {
                latencyMicros = new ListAccumulator<>();
                getRuntimeContext().addAccumulator(LATENCY_ACCUMULATOR, latencyMicros);
            }
        }

        @Override
        public void processElement(RoutedUpdate routed, Context context, Collector<String> out)
                throws Exception {
            Update update = routed.value.update;
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

            flushRevenueDeltas(routed.value.sequence, pendingDeltas, out);
            if (measureLatency && (!"customer".equals(update.table) || routed.shard == 0)) {
                long elapsed = Math.max(0L, System.nanoTime() - routed.value.createdNanos);
                latencyMicros.add(elapsed / 1_000L);
            }
        }

        private void processCustomer(Update update, Map<GroupKey, PendingDelta> pendingDeltas)
                throws Exception {
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
        ) throws Exception {
            if (!customer.isBuilding()) {
                return;
            }
            OrderKeySet indexedOrders = ordersByCustomer.get(customer.custKey);
            if (indexedOrders == null) {
                return;
            }
            for (Long orderKey : indexedOrders.values) {
                OrderRow order = orders.get(orderKey);
                if (order != null && order.orderDate.isBefore(Q3_DATE)) {
                    emitOrderLineItemContributions(order, sign, reason, pendingDeltas);
                }
            }
        }

        private void processOrder(Update update, Map<GroupKey, PendingDelta> pendingDeltas)
                throws Exception {
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
                OrderKeySet indexedOrders = ordersByCustomer.get(current.custKey);
                if (indexedOrders == null) {
                    indexedOrders = new OrderKeySet();
                }
                indexedOrders.values.add(orderKey);
                ordersByCustomer.put(current.custKey, indexedOrders);
                emitOrderContributionIfMatched(current, 1.0, "order-insert", pendingDeltas);
            }
        }

        private void removeOrderIndex(OrderRow order) throws Exception {
            OrderKeySet indexedOrders = ordersByCustomer.get(order.custKey);
            if (indexedOrders == null) {
                return;
            }
            indexedOrders.values.remove(order.orderKey);
            if (indexedOrders.values.isEmpty()) {
                ordersByCustomer.remove(order.custKey);
            } else {
                ordersByCustomer.put(order.custKey, indexedOrders);
            }
        }

        private void emitOrderContributionIfMatched(
                OrderRow order,
                double sign,
                String reason,
                Map<GroupKey, PendingDelta> pendingDeltas
        ) throws Exception {
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
        ) throws Exception {
            LineItemBucket bucket = lineItemsByOrder.get(order.orderKey);
            if (bucket == null) {
                return;
            }
            for (LineItem item : bucket.values.values()) {
                if (item.shipDate.isAfter(Q3_DATE)) {
                    GroupKey group = new GroupKey(
                            order.orderKey, order.orderDate, order.shipPriority);
                    addRevenueDelta(pendingDeltas, group, sign * item.revenue(), reason);
                }
            }
        }

        private void processLineItem(Update update, Map<GroupKey, PendingDelta> pendingDeltas)
                throws Exception {
            long orderKey = Long.parseLong(update.fields[0]);
            int lineNumber = Integer.parseInt(update.fields[1]);
            LineItemBucket bucket = lineItemsByOrder.get(orderKey);
            if (bucket == null) {
                bucket = new LineItemBucket();
            }
            LineItem old = bucket.values.get(lineNumber);
            if (old != null) {
                emitLineItemContributionIfMatched(
                        old, -1.0, "lineitem-delete-or-replace", pendingDeltas);
                bucket.values.remove(lineNumber);
            }
            if (update.insert) {
                LineItem current = new LineItem(
                        orderKey,
                        lineNumber,
                        Double.parseDouble(update.fields[2]),
                        Double.parseDouble(update.fields[3]),
                        LocalDate.parse(update.fields[4])
                );
                bucket.values.put(lineNumber, current);
                emitLineItemContributionIfMatched(
                        current, 1.0, "lineitem-insert", pendingDeltas);
            }
            if (bucket.values.isEmpty()) {
                lineItemsByOrder.remove(orderKey);
            } else {
                lineItemsByOrder.put(orderKey, bucket);
            }
        }

        private void emitLineItemContributionIfMatched(
                LineItem item,
                double sign,
                String reason,
                Map<GroupKey, PendingDelta> pendingDeltas
        ) throws Exception {
            OrderRow order = orders.get(item.orderKey);
            if (order == null
                    || !order.orderDate.isBefore(Q3_DATE)
                    || !item.shipDate.isAfter(Q3_DATE)) {
                return;
            }
            Customer customer = customers.get(order.custKey);
            if (customer == null || !customer.isBuilding()) {
                return;
            }
            GroupKey group = new GroupKey(
                    order.orderKey, order.orderDate, order.shipPriority);
            addRevenueDelta(pendingDeltas, group, sign * item.revenue(), reason);
        }

        private static void addRevenueDelta(
                Map<GroupKey, PendingDelta> pendingDeltas,
                GroupKey group,
                double delta,
                String reason
        ) {
            if (Math.abs(delta) < 1.0e-9) {
                return;
            }
            pendingDeltas.computeIfAbsent(group, ignored -> new PendingDelta())
                    .add(delta, reason);
        }

        private void flushRevenueDeltas(
                long sequence,
                Map<GroupKey, PendingDelta> pendingDeltas,
                Collector<String> out
        ) throws Exception {
            for (Map.Entry<GroupKey, PendingDelta> entry : pendingDeltas.entrySet()) {
                GroupKey group = entry.getKey();
                double delta = entry.getValue().amount;
                if (Math.abs(delta) < 1.0e-9) {
                    continue;
                }
                Double current = revenueByGroup.get(group);
                double next = (current == null ? 0.0 : current) + delta;
                if (Math.abs(next) < 1.0e-9) {
                    revenueByGroup.remove(group);
                    next = 0.0;
                } else {
                    revenueByGroup.put(group, next);
                }
                if (printDeltas) {
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
        }
    }

    private static final class PendingDelta {
        private double amount;
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

    static final class SequencedUpdate implements Serializable {
        private static final long serialVersionUID = 1L;
        final long sequence;
        final long createdNanos;
        final Update update;

        SequencedUpdate(long sequence, long createdNanos, Update update) {
            this.sequence = sequence;
            this.createdNanos = createdNanos;
            this.update = update;
        }
    }

    public static final class RoutedUpdate implements Serializable {
        private static final long serialVersionUID = 1L;
        public int shard;
        public SequencedUpdate value;

        public RoutedUpdate() {
        }

        RoutedUpdate(int shard, SequencedUpdate value) {
            this.shard = shard;
            this.value = value;
        }
    }

    static final class Update implements Serializable {
        private static final long serialVersionUID = 1L;
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
            String table = parts[1].toLowerCase(Locale.ROOT);
            int minimumFields;
            switch (table) {
                case "customer":
                    minimumFields = insert ? 2 : 1;
                    break;
                case "orders":
                    minimumFields = insert ? 4 : 1;
                    break;
                case "lineitem":
                    minimumFields = insert ? 5 : 2;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown table: " + table);
            }
            if (parts.length - 2 < minimumFields) {
                throw new IllegalArgumentException("Not enough fields: " + line);
            }
            String[] fields = new String[parts.length - 2];
            System.arraycopy(parts, 2, fields, 0, fields.length);
            return new Update(insert, table, fields);
        }
    }

    public static final class Customer implements Serializable {
        private static final long serialVersionUID = 1L;
        public long custKey;
        public String marketSegment;

        public Customer() {
        }

        Customer(long custKey, String marketSegment) {
            this.custKey = custKey;
            this.marketSegment = marketSegment;
        }

        boolean isBuilding() {
            return "BUILDING".equalsIgnoreCase(marketSegment);
        }
    }

    public static final class OrderRow implements Serializable {
        private static final long serialVersionUID = 1L;
        public long orderKey;
        public long custKey;
        public LocalDate orderDate;
        public int shipPriority;

        public OrderRow() {
        }

        OrderRow(long orderKey, long custKey, LocalDate orderDate, int shipPriority) {
            this.orderKey = orderKey;
            this.custKey = custKey;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
        }
    }

    public static final class LineItem implements Serializable {
        private static final long serialVersionUID = 1L;
        public long orderKey;
        public int lineNumber;
        public double extendedPrice;
        public double discount;
        public LocalDate shipDate;

        public LineItem() {
        }

        LineItem(
                long orderKey,
                int lineNumber,
                double extendedPrice,
                double discount,
                LocalDate shipDate
        ) {
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

    public static final class OrderKeySet implements Serializable {
        private static final long serialVersionUID = 1L;
        public Set<Long> values = new HashSet<>();

        public OrderKeySet() {
        }
    }

    public static final class LineItemBucket implements Serializable {
        private static final long serialVersionUID = 1L;
        public Map<Integer, LineItem> values = new HashMap<>();

        public LineItemBucket() {
        }
    }

    public static final class GroupKey implements Serializable {
        private static final long serialVersionUID = 1L;
        public long orderKey;
        public LocalDate orderDate;
        public int shipPriority;

        public GroupKey() {
        }

        GroupKey(long orderKey, LocalDate orderDate, int shipPriority) {
            this.orderKey = orderKey;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof GroupKey)) {
                return false;
            }
            GroupKey other = (GroupKey) object;
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
}
