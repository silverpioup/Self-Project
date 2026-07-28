package edu.hkust.ip.flink;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;

public final class ExactMoney {
    static final long REVENUE_SCALE = 10_000L;

    private ExactMoney() {
    }

    public static long revenueUnits(String extendedPrice, String discount) {
        long priceCents = new BigDecimal(extendedPrice)
                .movePointRight(2)
                .longValueExact();
        long discountHundredths = new BigDecimal(discount)
                .movePointRight(2)
                .longValueExact();
        if (discountHundredths < 0 || discountHundredths > 100) {
            throw new IllegalArgumentException("Discount must be between 0.00 and 1.00.");
        }
        return Math.multiplyExact(priceCents, 100L - discountHundredths);
    }

    public static String format(long revenueUnits) {
        return BigDecimal.valueOf(revenueUnits, 4)
                .setScale(4, RoundingMode.UNNECESSARY)
                .toPlainString();
    }

    static String formatPerSecond(long count, long elapsedNanos) {
        if (elapsedNanos <= 0L) {
            return "0.00";
        }
        return String.format(Locale.US, "%.2f", count * 1_000_000_000.0 / elapsedNanos);
    }
}
