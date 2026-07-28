package edu.hkust.ip.flink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class Q3Types {
    private Q3Types() {
    }

    public static final class Update implements Serializable {
        private static final long serialVersionUID = 1L;
        public boolean insert;
        public String table;
        public String[] fields;

        public Update() {
        }

        Update(boolean insert, String table, String[] fields) {
            this.insert = insert;
            this.table = table;
            this.fields = fields;
        }

        public static Update parse(String line) {
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
            String table = parts[1].toLowerCase(java.util.Locale.ROOT);
            int required;
            switch (table) {
                case "customer":
                    required = insert ? 2 : 1;
                    break;
                case "orders":
                    required = insert ? 4 : 1;
                    break;
                case "lineitem":
                    required = insert ? 5 : 2;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown table: " + table);
            }
            if (parts.length != required + 2) {
                throw new IllegalArgumentException(
                        "Expected " + required + " fields for " + table + ": " + line);
            }
            String[] fields = new String[required];
            System.arraycopy(parts, 2, fields, 0, required);
            return new Update(insert, table, fields);
        }
    }

    public static final class SequencedUpdate implements Serializable {
        private static final long serialVersionUID = 1L;
        public long sequence;
        public long createdNanos;
        public Update update;

        public SequencedUpdate() {
        }

        SequencedUpdate(long sequence, long createdNanos, Update update) {
            this.sequence = sequence;
            this.createdNanos = createdNanos;
            this.update = update;
        }
    }

    public static final class RoutedUpdate implements Serializable {
        private static final long serialVersionUID = 1L;
        public int shard;
        public int partitionKey;
        public int expectedParts;
        public SequencedUpdate value;

        public RoutedUpdate() {
        }

        RoutedUpdate(int shard, int partitionKey, int expectedParts, SequencedUpdate value) {
            this.shard = shard;
            this.partitionKey = partitionKey;
            this.expectedParts = expectedParts;
            this.value = value;
        }
    }

    public static final class CustomerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public long custKey;
        public String marketSegment;
        public boolean alive;

        public CustomerState() {
        }

        CustomerState(long custKey, String marketSegment) {
            this.custKey = custKey;
            this.marketSegment = marketSegment;
            this.alive = "BUILDING".equalsIgnoreCase(marketSegment);
        }
    }

    public static final class OrderState implements Serializable {
        private static final long serialVersionUID = 1L;
        public long orderKey;
        public long custKey;
        public int orderDateEpochDay;
        public int shipPriority;
        public int childMatchCount;
        public boolean alive;

        public OrderState() {
        }

        OrderState(long orderKey, long custKey, int orderDateEpochDay, int shipPriority) {
            this.orderKey = orderKey;
            this.custKey = custKey;
            this.orderDateEpochDay = orderDateEpochDay;
            this.shipPriority = shipPriority;
        }
    }

    public static final class LineItemState implements Serializable {
        private static final long serialVersionUID = 1L;
        public long orderKey;
        public int lineNumber;
        public long revenueUnits;
        public int shipDateEpochDay;
        public int childMatchCount;
        public boolean alive;

        public LineItemState() {
        }

        LineItemState(
                long orderKey,
                int lineNumber,
                long revenueUnits,
                int shipDateEpochDay
        ) {
            this.orderKey = orderKey;
            this.lineNumber = lineNumber;
            this.revenueUnits = revenueUnits;
            this.shipDateEpochDay = shipDateEpochDay;
        }
    }

    public static final class LongSet implements Serializable {
        private static final long serialVersionUID = 1L;
        public Set<Long> values = new HashSet<>();

        public LongSet() {
        }
    }

    public static final class LineItemBucket implements Serializable {
        private static final long serialVersionUID = 1L;
        public Map<Integer, LineItemState> values = new HashMap<>();

        public LineItemBucket() {
        }
    }

    public static final class GroupKey implements Serializable {
        private static final long serialVersionUID = 1L;
        public long orderKey;
        public int orderDateEpochDay;
        public int shipPriority;

        public GroupKey() {
        }

        GroupKey(long orderKey, int orderDateEpochDay, int shipPriority) {
            this.orderKey = orderKey;
            this.orderDateEpochDay = orderDateEpochDay;
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
                    && orderDateEpochDay == other.orderDateEpochDay;
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderKey, orderDateEpochDay, shipPriority);
        }
    }

    public static final class GroupDelta implements Serializable {
        private static final long serialVersionUID = 1L;
        public GroupKey group;
        public long amount;
        public String reason;

        public GroupDelta() {
        }

        GroupDelta(GroupKey group, long amount, String reason) {
            this.group = group;
            this.amount = amount;
            this.reason = reason;
        }
    }

    public static final class ShardResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public long sequence;
        public long createdNanos;
        public int shard;
        public int expectedParts;
        public List<GroupDelta> deltas = new ArrayList<>();

        public ShardResult() {
        }

        ShardResult(
                long sequence,
                long createdNanos,
                int shard,
                int expectedParts,
                List<GroupDelta> deltas
        ) {
            this.sequence = sequence;
            this.createdNanos = createdNanos;
            this.shard = shard;
            this.expectedParts = expectedParts;
            this.deltas = deltas;
        }
    }

    public static final class SequenceBatch implements Serializable {
        private static final long serialVersionUID = 1L;
        public int expectedParts;
        public long createdNanos;
        public Map<Integer, List<GroupDelta>> deltasByShard = new HashMap<>();

        public SequenceBatch() {
        }

        boolean complete() {
            return expectedParts > 0 && deltasByShard.size() == expectedParts;
        }
    }

    public static final class RankedGroup implements Serializable {
        private static final long serialVersionUID = 1L;
        public GroupKey group;
        public long revenueUnits;

        public RankedGroup() {
        }

        RankedGroup(GroupKey group, long revenueUnits) {
            this.group = group;
            this.revenueUnits = revenueUnits;
        }
    }
}
