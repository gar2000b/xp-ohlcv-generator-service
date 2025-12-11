package ca.digilogue.xp.generator;

import java.time.Instant;

/**
 * Represents an OHLCV (Open, High, Low, Close, Volume) candle.
 */
public class OhlcvCandle {
    private final String symbol;
    private final double open;
    private final double high;
    private final double low;
    private final double close;
    private final double volume;
    private final Instant timestamp;

    public OhlcvCandle(String symbol, double open, double high, double low, double close, double volume) {
        this.symbol = symbol;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.timestamp = Instant.now();
    }

    public String getSymbol() {
        return symbol;
    }

    public double getOpen() {
        return open;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getClose() {
        return close;
    }

    public double getVolume() {
        return volume;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format(
            "OhlcvCandle{symbol='%s', open=%.2f, high=%.2f, low=%.2f, close=%.2f, volume=%.2f, timestamp=%s}",
            symbol, open, high, low, close, volume, timestamp
        );
    }
}

