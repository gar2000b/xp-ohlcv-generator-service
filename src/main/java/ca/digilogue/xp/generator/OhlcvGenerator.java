package ca.digilogue.xp.generator;

import ca.digilogue.xp.service.InfluxDbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Generates OHLCV candle data every second for a given symbol.
 * Uses a random walk with volatility to simulate realistic price movements.
 */
public class OhlcvGenerator implements Runnable {
    
    private static final Logger log = LoggerFactory.getLogger(OhlcvGenerator.class);
    
    private final String symbol;
    private final double basePrice;
    private final double volatility;
    private final Random random;
    private final InfluxDbService influxDbService;
    
    private volatile boolean running = false;
    private double currentPrice;
    private volatile OhlcvCandle latestCandle; // Latest generated candle (thread-safe access)
    
    public OhlcvGenerator(String symbol, double basePrice, double volatility, InfluxDbService influxDbService) {
        this.symbol = symbol;
        this.basePrice = basePrice;
        this.volatility = volatility;
        this.random = new Random();
        this.currentPrice = basePrice;
        this.influxDbService = influxDbService;
    }
    
    @Override
    public void run() {
        running = true;
        log.info("OHLCV Generator started for symbol: {}", symbol);
        
        while (running) {
            try {
                OhlcvCandle candle = generateCandle();
                
                // Store the latest candle (thread-safe - volatile ensures visibility)
                latestCandle = candle;
                
                // Write to InfluxDB
                influxDbService.writeCandle(candle);
                
                // Also log to console for debugging
                System.out.println(candle);
                
                // Sleep for 1 second
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("OHLCV Generator interrupted");
                break;
            } catch (Exception e) {
                log.error("Error generating/writing OHLCV candle", e);
            }
        }
        
        log.info("OHLCV Generator stopped for symbol: {}", symbol);
    }
    
    /**
     * Generates a single OHLCV candle with realistic price movements.
     */
    private OhlcvCandle generateCandle() {
        // Open price is the previous close (or current price for first candle)
        double open = currentPrice;
        
        // Generate price change using random walk with volatility
        // Random walk: price change = volatility * random(-1 to 1)
        double priceChange = volatility * (random.nextDouble() * 2.0 - 1.0);
        
        // Apply some mean reversion (tendency to return to base price)
        double meanReversion = (basePrice - currentPrice) * 0.01; // 1% pull toward base
        priceChange += meanReversion;
        
        // Calculate close price
        double close = currentPrice + priceChange;
        
        // Ensure price doesn't go negative
        if (close < 0.01) {
            close = 0.01;
        }
        
        // Generate high and low within the candle
        // High is between open and close (or above if there's volatility)
        double candleRange = Math.abs(close - open) + (volatility * random.nextDouble() * 0.5);
        double high = Math.max(open, close) + (candleRange * random.nextDouble() * 0.3);
        double low = Math.min(open, close) - (candleRange * random.nextDouble() * 0.3);
        
        // Ensure low doesn't go negative
        if (low < 0.01) {
            low = 0.01;
        }
        
        // Ensure high >= all other prices
        high = Math.max(high, Math.max(open, close));
        
        // Ensure low <= all other prices
        low = Math.min(low, Math.min(open, close));
        
        // Generate volume (random between 1000 and 100000)
        double volume = 1000.0 + (random.nextDouble() * 99000.0);
        
        // Update current price for next candle
        currentPrice = close;
        
        return new OhlcvCandle(symbol, open, high, low, close, volume);
    }
    
    public void stop() {
        running = false;
    }
    
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Gets the latest generated candle for this symbol.
     * Returns null if no candle has been generated yet.
     * 
     * @return The latest OHLCV candle, or null if not yet generated
     */
    public OhlcvCandle getLatestCandle() {
        return latestCandle;
    }
    
    /**
     * Gets the symbol this generator is producing candles for.
     * 
     * @return The trading symbol (e.g., "MEGA-USD")
     */
    public String getSymbol() {
        return symbol;
    }
}

